"""Queue Status view - Pending jobs grouped by partition.

Each section shows one partition's pending jobs sorted by priority. The two
primary "when does this run" signals are surfaced as columns:

  Starts   - Slurm's own backfill estimate (start_time on the pending job)
  Reason   - why the job is waiting (Priority/Resources/Dependency/...)
"""

import urwid as u
import datetime
import re
from slop.utils import format_duration
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.widgets import rounded_box


_DUR_TOKEN_RE = re.compile(r'\d+[dhms]')


def _coarse_duration(seconds):
    """format_duration trimmed to its top 2 units (e.g. '4d3h28m31s' → '4d3h')."""
    s = format_duration(seconds)
    tokens = _DUR_TOKEN_RE.findall(s)
    return ''.join(tokens[:2]) if tokens else s


# ----- Field accessors ----------------------------------------------------

def _job_priority(job):
    p = getattr(job, 'priority', {})
    if isinstance(p, dict):
        return p.get('number', 0)
    return p if isinstance(p, int) else 0


def _job_partition(job):
    """First partition listed (jobs may target several comma-separated)."""
    part = getattr(job, 'partition', '') or ''
    return part.split(',', 1)[0].strip() or '(none)'


def _format_eta(start_time):
    if not isinstance(start_time, dict) or not start_time.get('set'):
        return EMPTY_PLACEHOLDER
    ts = start_time.get('number', 0)
    if ts == 0:
        return EMPTY_PLACEHOLDER
    diff = ts - datetime.datetime.now().timestamp()
    # Slurm uses far-future placeholder dates (~year 2106) when it has no
    # estimate; treat anything > 1y out as "Unknown" rather than a useful ETA.
    if diff > 365 * 24 * 3600:
        return "Unknown"
    if diff < -60:
        return "overdue"
    if diff < 60:
        return "now"
    return f"in {_coarse_duration(int(diff))}"


def _format_wait(submit_time):
    if not isinstance(submit_time, dict) or not submit_time.get('set'):
        return EMPTY_PLACEHOLDER
    submit = datetime.datetime.fromtimestamp(submit_time['number'])
    wait = int((datetime.datetime.now() - submit).total_seconds())
    return _coarse_duration(wait)


def _size_indicator(job):
    cpus_obj = getattr(job, 'cpus', {})
    if isinstance(cpus_obj, dict):
        cpus = cpus_obj.get('number', 1)
    else:
        cpus = cpus_obj if isinstance(cpus_obj, int) else 1
    tl = getattr(job, 'time_limit', {})
    minutes = tl.get('number', 60) if isinstance(tl, dict) and tl.get('set') else 60
    core_hours = (cpus * minutes) / 60
    if core_hours < 100:
        return "▪"
    if core_hours < 1000:
        return "▪▪"
    return "▪▪▪"


def _time_limit_str(job):
    tl = getattr(job, 'time_limit', {})
    if isinstance(tl, dict) and tl.get('set'):
        return _coarse_duration(tl.get('number', 0) * 60)
    return EMPTY_PLACEHOLDER


def _has_eta(job):
    st = getattr(job, 'start_time', {})
    if not (isinstance(st, dict) and st.get('set')):
        return False
    ts = st.get('number', 0)
    if ts <= 0:
        return False
    diff = ts - datetime.datetime.now().timestamp()
    return diff <= 365 * 24 * 3600


def _reason_attr(reason):
    if reason in ('Priority', 'Resources'):
        return 'normal'
    if reason in ('Dependency', 'JobHeldUser', 'JobHeldAdmin', 'BeginTime'):
        return 'warning'
    if 'NotAvail' in reason or 'Invalid' in reason:
        return 'error'
    return 'normal'


# ----- Column layouts -----------------------------------------------------
# Header and rows share one formatter so they can't drift.
def _format_row(width, *, rank, priority, eta, wait, reason, user, size, tlim, name):
    if width < 100:
        return f"{rank:>3} {priority:>7} {eta:<11} {reason:<14} {user:<10} {name:<20}"
    if width < 140:
        return (f"{rank:>3} {priority:>7} {eta:<11} {wait:>8} "
                f"{reason:<15} {user:<10} {size:<3} {tlim:>11} {name:<25}")
    return (f"{rank:>3} {priority:>7} {eta:<13} {wait:>8} "
            f"{reason:<18} {user:<10} {size:<3} {tlim:>11} {name:<40}")


def _format_header(width):
    return _format_row(
        width, rank='#', priority='Priority', eta='Starts',
        wait='Waiting', reason='Reason', user='User',
        size='Sz', tlim='Time', name='Job Name',
    )


# ----- Widgets ------------------------------------------------------------

class QueueJobWidget(u.WidgetWrap):
    """One pending job row."""

    def __init__(self, job, rank, width=120):
        self.job = job
        self.jobid = job.job_id
        self.rank = rank
        self.width = width
        super().__init__(self._build())

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build(self):
        job = self.job
        priority = _job_priority(job)
        eta = _format_eta(getattr(job, 'start_time', {}))
        wait = _format_wait(getattr(job, 'submit_time', {}))
        reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER
        user = getattr(job, 'user_name', EMPTY_PLACEHOLDER)
        size = _size_indicator(job)
        tlim = _time_limit_str(job)
        name = job.name or EMPTY_PLACEHOLDER

        text = _format_row(
            self.width, rank=str(self.rank), priority=str(priority),
            eta=eta[:13], wait=wait[:8], reason=reason[:18],
            user=user[:10], size=size, tlim=tlim[:11], name=name[:40],
        )
        return u.AttrMap(u.Text(text), _reason_attr(reason), 'normal_selected')


class QueueGroupWidget(u.WidgetWrap):
    """Collapsed bundle of consecutive same-(user, reason) jobs in one partition."""

    def __init__(self, group_key, start_rank, end_rank, group, width=120):
        self.group_key = group_key
        self.start_rank = start_rank
        self.end_rank = end_rank
        self.job_group = group
        self.width = width
        super().__init__(self._build())

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build(self):
        first = self.job_group[0]
        count = len(self.job_group)
        priority = max(_job_priority(j) for j in self.job_group)
        eta = _format_eta(getattr(first, 'start_time', {}))
        wait = _format_wait(getattr(first, 'submit_time', {}))
        reason = getattr(first, 'state_reason', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER
        user = getattr(first, 'user_name', EMPTY_PLACEHOLDER)

        sizes = [_size_indicator(j) for j in self.job_group]
        size = max(set(sizes), key=sizes.count)

        durations = []
        for j in self.job_group:
            tl = getattr(j, 'time_limit', {})
            if isinstance(tl, dict) and tl.get('set'):
                durations.append(tl.get('number', 0))
        if durations:
            mn, mx = min(durations), max(durations)
            tlim = (_coarse_duration(mn * 60) if mn == mx
                    else f"{_coarse_duration(mn * 60)}-{_coarse_duration(mx * 60)}")
        else:
            tlim = EMPTY_PLACEHOLDER

        name = (f"[{count} jobs]" if self.start_rank == self.end_rank
                else f"[{count} jobs #{self.start_rank}-{self.end_rank}]")

        text = _format_row(
            self.width, rank=str(self.start_rank), priority=str(priority),
            eta=eta[:13], wait=wait[:8], reason=reason[:18],
            user=user[:10], size=size, tlim=tlim[:11], name=name[:40],
        )
        return u.AttrMap(u.Text(text), _reason_attr(reason), 'normal_selected')


class PartitionHeaderWidget(u.WidgetWrap):
    """Non-selectable section header: partition name + pending counts."""

    def __init__(self, partition, total, with_eta, width=120):
        title = f"  {partition}  ({total} pending"
        if with_eta:
            title += f", {with_eta} with ETA"
        title += ")"
        line = title.ljust(max(width, len(title)))
        super().__init__(u.AttrMap(u.Text(line), 'jobheader'))

    def selectable(self):
        return False


# ----- Screen -------------------------------------------------------------

class ScreenViewQueue(u.WidgetWrap):
    """Pending jobs grouped by partition; sorted by priority within each."""

    def __init__(self, main_screen, jobs):
        self.main_screen = main_screen
        self.jobs = jobs
        self.expanded_groups = set()

        self.summary_text = u.Text("")
        self.col_header_text = u.AttrMap(u.Text(""), 'jobheader')
        self.job_walker = u.SimpleFocusListWalker([])
        self.job_listbox = u.ListBox(self.job_walker)

        pile = u.Pile([
            ('pack', self.summary_text),
            ('pack', u.Divider("─")),
            ('pack', self.col_header_text),
            ('pack', u.Divider("─")),
            u.ScrollBar(self.job_listbox),
        ])
        self.container = rounded_box(pile, title='Queue Status - Pending Jobs by Partition')

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, u.AttrMap(self.container, 'bg'))
        self.update()

    def on_jobs_update(self, *_a, **_kw):
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        self.update()

    def update(self):
        self.job_walker.clear()
        width = (self.main_screen.width - 3) if hasattr(self.main_screen, 'width') else 120

        self.col_header_text.original_widget.set_text(_format_header(width))

        # Bucket pending jobs by their first listed partition.
        by_part = {}
        for job in self.jobs.jobs:
            if not (hasattr(job, 'job_state') and 'PENDING' in job.job_state):
                continue
            by_part.setdefault(_job_partition(job), []).append(job)

        total_pending = sum(len(v) for v in by_part.values())
        total_eta = sum(1 for jobs in by_part.values() for j in jobs if _has_eta(j))
        self.summary_text.set_text([
            ('jobheader', '  '),
            ('jobheader', f'Pending: {total_pending}'),
            '   ',
            f'with ETA: {total_eta}',
            '   ',
            f'partitions: {len(by_part)}',
        ])

        widgets = []
        if not by_part:
            widgets.append(u.Text(("faded", "  No pending jobs in the queue")))
        else:
            # Partition order: highest top-priority job first ("which queue
            # is the next-to-run job sitting in").
            ordered = sorted(
                by_part.items(),
                key=lambda kv: max(_job_priority(j) for j in kv[1]),
                reverse=True,
            )
            for partition, jobs in ordered:
                jobs_sorted = sorted(jobs, key=_job_priority, reverse=True)
                with_eta = sum(1 for j in jobs_sorted if _has_eta(j))
                widgets.append(PartitionHeaderWidget(
                    partition, len(jobs_sorted), with_eta, width=width,
                ))

                # Group consecutive same-(user, reason) jobs within partition.
                groups, cur, cu, cr = [], [], None, None
                for job in jobs_sorted:
                    user = getattr(job, 'user_name', EMPTY_PLACEHOLDER)
                    reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER)
                    if user == cu and reason == cr:
                        cur.append(job)
                    else:
                        if cur:
                            groups.append(cur)
                        cur, cu, cr = [job], user, reason
                if cur:
                    groups.append(cur)

                rank = 1
                for group in groups:
                    n = len(group)
                    s, e = rank, rank + n - 1
                    # Namespace group keys by partition so identical rank
                    # ranges across partitions don't share collapse state.
                    key = f"{partition}:{s}-{e}"
                    if n == 1:
                        widgets.append(QueueJobWidget(group[0], rank, width=width))
                    elif key in self.expanded_groups:
                        for i, job in enumerate(group):
                            widgets.append(QueueJobWidget(job, s + i, width=width))
                    else:
                        widgets.append(QueueGroupWidget(key, s, e, group, width=width))
                    rank += n

        self.job_walker.extend(widgets)

        # Leave focus at index 0 (the first partition header). If we focused
        # the first job instead, the listbox would scroll the partition
        # header off the top. The header is non-selectable so arrow keys
        # still find the first job on first keypress.
        if len(self.job_walker):
            self.job_walker.set_focus(0)

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        focus_w, _ = self.job_listbox.get_focus()

        if key in ('e', 'enter', ' '):
            if hasattr(focus_w, 'group_key'):
                if focus_w.group_key in self.expanded_groups:
                    self.expanded_groups.remove(focus_w.group_key)
                else:
                    self.expanded_groups.add(focus_w.group_key)
                self.update()
                return None
            if hasattr(focus_w, 'jobid') and key in ('enter', ' '):
                from slop.ui.overlays import JobInfoOverlay
                job = self.jobs.job_index.get(focus_w.jobid)
                if job:
                    self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
                return None

        return super().keypress(size, key)
