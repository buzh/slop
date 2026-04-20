"""Queue Status view - the lifecycle "flow" of jobs.

Four sections, top to bottom, mirroring the upward flow of a job through its
states:

  1. Just ended       (jobs that vanished from scontrol since the last refresh)
  2. Finishing soon   (running jobs ranked by least time-remaining)
  3. Just started     (jobs that flipped PENDING → RUNNING in the last 15 min)
  4. Pending queue    (existing partition-grouped pending list)

Sections 1 and 3 are tracker-driven: entries arrive on a state transition and
linger until either an age cap (15 min for "started") or display capacity
forces eviction (FIFO). Section 2 is recomputed each refresh and excludes
anything currently in the "started" tracker — a job is never both "just
started" and "finishing soon".
"""

import urwid as u
import datetime
import re
import time
from slop.utils import format_duration
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.widgets import rounded_box


_DUR_TOKEN_RE = re.compile(r'\d+[dhms]')

# Age cap for the "Just started" tracker — entries older than this are evicted
# even if there's still room in the section.
STARTED_MAX_AGE = 15 * 60


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


def _ts(time_dict):
    """Pull a unix timestamp out of scontrol's `{set, number}` dicts."""
    if isinstance(time_dict, dict):
        return time_dict.get('number', 0) or 0
    return 0


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


# ----- Lifecycle helpers --------------------------------------------------

_STATE_SHORT = {
    'COMPLETED': 'CD', 'FAILED': 'F', 'CANCELLED': 'CA',
    'TIMEOUT': 'TO', 'OUT_OF_MEMORY': 'OOM', 'NODE_FAIL': 'NF',
    'PREEMPTED': 'PR', 'BOOT_FAIL': 'BF', 'DEADLINE': 'DL',
    'RUNNING': 'R', 'PENDING': 'PD', 'SUSPENDED': 'S',
    'COMPLETING': 'CG',
}


def _state_short(state):
    return _STATE_SHORT.get(state, (state or '?')[:3])


def _state_attr(state):
    if state == 'COMPLETED':
        return 'success'
    if state in ('FAILED', 'NODE_FAIL', 'OUT_OF_MEMORY', 'BOOT_FAIL'):
        return 'error'
    if state in ('CANCELLED', 'TIMEOUT', 'PREEMPTED', 'DEADLINE'):
        return 'warning'
    return 'normal'


def _resources_str(cpus, mem):
    parts = []
    if cpus:
        parts.append(f"{cpus}c")
    if mem:
        parts.append(mem)
    return ' '.join(parts) or EMPTY_PLACEHOLDER


def _snapshot_job(job):
    """Capture enough data to render a row even after the job leaves scontrol."""
    state = job.job_state[0] if getattr(job, 'job_state', None) else 'UNKNOWN'
    cpus_obj = getattr(job, 'cpus', {})
    if isinstance(cpus_obj, dict):
        cpus = cpus_obj.get('number', 0) if cpus_obj.get('set', True) else 0
    else:
        cpus = cpus_obj or 0

    # Memory: prefer per-node when set (gives the absolute number), else
    # multiply per-cpu by the cpu count to get total bytes-per-task.
    mem = ''
    mpn = getattr(job, 'memory_per_node', {})
    mpc = getattr(job, 'memory_per_cpu', {})
    if isinstance(mpn, dict) and mpn.get('set') and mpn.get('number'):
        mem = f"{int(mpn['number'])}M"
    elif isinstance(mpc, dict) and mpc.get('set') and mpc.get('number'):
        mem = f"{int(mpc['number']) * max(cpus, 1)}M"

    return {
        'jobid': job.job_id,
        'name': (getattr(job, 'name', None) or EMPTY_PLACEHOLDER),
        'user': getattr(job, 'user_name', EMPTY_PLACEHOLDER),
        'partition': _job_partition(job),
        'state': state,
        'submit_ts': _ts(getattr(job, 'submit_time', {})),
        'start_ts': _ts(getattr(job, 'start_time', {})),
        'end_ts': _ts(getattr(job, 'end_time', {})),
        'time_limit_min': (getattr(job, 'time_limit', {}).get('number', 0)
                           if isinstance(getattr(job, 'time_limit', {}), dict)
                           and getattr(job, 'time_limit', {}).get('set') else 0),
        'cpus': cpus,
        'mem': mem,
        'returncode': getattr(job, 'returncode', EMPTY_PLACEHOLDER),
    }


# ----- Pending-section formatters (existing, unchanged) -------------------

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


# ----- Lifecycle row formatters -------------------------------------------
# Each section has its own column set; sharing a single _format_row across
# sections would force every column into every section even when irrelevant.

def _format_ended_row(width, *, state, jobid, user, partition,
                      used, limit, exit_code, name):
    if width < 100:
        return f"{state:>3} {jobid:>9} {user:<10} {used:>8}/{limit:<8} {name:<20}"
    return (f"{state:>3} {jobid:>9} {user:<12} {partition:<14} "
            f"{used:>9}/{limit:<9} {exit_code:>8}  {name:<40}")


def _ended_header(width):
    return _format_ended_row(
        width, state='St', jobid='Job ID', user='User', partition='Partition',
        used='Used', limit='Limit', exit_code='Exit', name='Name',
    )


def _format_finishing_row(width, *, state, jobid, user, partition,
                          remaining, ran, name):
    if width < 100:
        return f"{state:>3} {jobid:>9} {user:<10} {remaining:>10} {name:<25}"
    return (f"{state:>3} {jobid:>9} {user:<12} {partition:<14} "
            f"{remaining:>11} {ran:>11}  {name:<40}")


def _finishing_header(width):
    return _format_finishing_row(
        width, state='St', jobid='Job ID', user='User', partition='Partition',
        remaining='Remaining', ran='Ran', name='Name',
    )


def _format_started_row(width, *, state, jobid, user, partition,
                        wait, ran, resources, name):
    if width < 100:
        return f"{state:>3} {jobid:>9} {user:<10} {wait:>10} {ran:>10} {name:<20}"
    return (f"{state:>3} {jobid:>9} {user:<12} {partition:<14} "
            f"{wait:>10} {ran:>10}  {resources:<18} {name:<35}")


def _started_header(width):
    return _format_started_row(
        width, state='St', jobid='Job ID', user='User', partition='Partition',
        wait='Waited', ran='Ran', resources='Resources', name='Name',
    )


# ----- Widgets ------------------------------------------------------------

class QueueJobWidget(u.WidgetWrap):
    """One pending job row. `parent_group_key` is set when this row is an
    expanded child of a QueueGroupWidget so 'e' from the child can still
    collapse the parent group."""

    def __init__(self, job, rank, width=120, parent_group_key=None):
        self.job = job
        self.jobid = job.job_id
        self.rank = rank
        self.width = width
        self.parent_group_key = parent_group_key
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
    """Header row for a bundle of consecutive same-(user, reason) jobs.
    Stays visible whether the group is expanded or collapsed; the marker
    (▶/▼) shows the current state."""

    def __init__(self, group_key, start_rank, end_rank, group, width=120,
                 expanded=False):
        self.group_key = group_key
        self.start_rank = start_rank
        self.end_rank = end_rank
        self.job_group = group
        self.width = width
        self.expanded = expanded
        super().__init__(self._build())

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build(self):
        count = len(self.job_group)
        rng = (f"#{self.start_rank}" if self.start_rank == self.end_rank
               else f"#{self.start_rank}-{self.end_rank}")

        if self.expanded:
            # Compact subheader — the children below carry per-row detail, so
            # repeating priority/user/eta on the header would just be noise.
            line = f"  ▼ [{count} jobs {rng}]".ljust(max(self.width, 1))
            return u.AttrMap(u.Text(line), 'faded', 'normal_selected')

        first = self.job_group[0]
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

        name = f"▶ [{count} jobs {rng}]"
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


class SectionTitleWidget(u.WidgetWrap):
    """Banner row for a top-of-screen lifecycle section."""

    def __init__(self, label, count, width=120):
        text = f"  {label}  ({count})".ljust(max(width, 1))
        super().__init__(u.AttrMap(u.Text(text), 'jobheader'))

    def selectable(self):
        return False


class _ReadOnlyRow(u.WidgetWrap):
    """Base for non-interactive rows in the upper sections."""

    def selectable(self):
        return False


class EndedJobWidget(_ReadOnlyRow):
    def __init__(self, snap, width=120):
        used_str = (_coarse_duration(int(snap['end_ts'] - snap['start_ts']))
                    if snap['end_ts'] and snap['start_ts']
                    and snap['end_ts'] >= snap['start_ts']
                    else EMPTY_PLACEHOLDER)
        limit_str = (_coarse_duration(snap['time_limit_min'] * 60)
                     if snap['time_limit_min'] else EMPTY_PLACEHOLDER)
        text = _format_ended_row(
            width,
            state=_state_short(snap['state']),
            jobid=str(snap['jobid'])[:9],
            user=str(snap['user'])[:12],
            partition=str(snap['partition'])[:14],
            used=used_str[:9],
            limit=limit_str[:9],
            exit_code=str(snap['returncode'])[:8],
            name=str(snap['name'])[:40],
        )
        super().__init__(u.AttrMap(u.Text(text), _state_attr(snap['state'])))


class FinishingJobWidget(_ReadOnlyRow):
    def __init__(self, job, width=120):
        end_ts = _ts(getattr(job, 'end_time', {}))
        start_ts = _ts(getattr(job, 'start_time', {}))
        now = time.time()
        remaining = (_coarse_duration(int(end_ts - now))
                     if end_ts > now else EMPTY_PLACEHOLDER)
        ran = (_coarse_duration(int(now - start_ts))
               if start_ts and now >= start_ts else EMPTY_PLACEHOLDER)
        state = job.job_state[0] if getattr(job, 'job_state', None) else 'R'
        text = _format_finishing_row(
            width,
            state=_state_short(state),
            jobid=str(job.job_id)[:9],
            user=str(getattr(job, 'user_name', EMPTY_PLACEHOLDER))[:12],
            partition=_job_partition(job)[:14],
            remaining=remaining[:11],
            ran=ran[:11],
            name=str(getattr(job, 'name', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER)[:40],
        )
        # Dim warning if <5 min remaining, else faded normal
        attr = 'warning' if (end_ts and end_ts - now < 300) else 'normal'
        super().__init__(u.AttrMap(u.Text(text), attr))


class StartedJobWidget(_ReadOnlyRow):
    def __init__(self, snap, width=120):
        now = time.time()
        wait_str = (_coarse_duration(int(snap['start_ts'] - snap['submit_ts']))
                    if snap['start_ts'] and snap['submit_ts']
                    and snap['start_ts'] >= snap['submit_ts']
                    else EMPTY_PLACEHOLDER)
        ran_str = (_coarse_duration(int(now - snap['start_ts']))
                   if snap['start_ts'] and now >= snap['start_ts']
                   else EMPTY_PLACEHOLDER)
        text = _format_started_row(
            width,
            state=_state_short(snap['state']),
            jobid=str(snap['jobid'])[:9],
            user=str(snap['user'])[:12],
            partition=str(snap['partition'])[:14],
            wait=wait_str[:10],
            ran=ran_str[:10],
            resources=_resources_str(snap['cpus'], snap['mem'])[:18],
            name=str(snap['name'])[:35],
        )
        super().__init__(u.AttrMap(u.Text(text), _state_attr(snap['state'])))


# ----- Screen -------------------------------------------------------------

class ScreenViewQueue(u.WidgetWrap):
    """Lifecycle flow: ended → finishing soon → just started → pending."""

    # Vertical weights for the four sections (ended, finishing, started, pending).
    SECTION_WEIGHTS = (15, 15, 15, 55)
    # Per-section overhead (title row + column-header row).
    TRACKER_OVERHEAD = 2

    def __init__(self, main_screen, jobs):
        self.main_screen = main_screen
        self.jobs = jobs

        # Pending-section state (existing).
        self.expanded_groups = set()
        self.selected_jobid = None
        self.selected_group_key = None

        # Lifecycle trackers.
        # started_tracker: {jobid: (snap, monotonic_ts_when_first_seen)}
        # ended_tracker:   {jobid: (snap, monotonic_ts_when_noticed_gone)}
        # prev_jobs_by_id: jobid -> Job from last refresh, used to detect
        #                  pending→running transitions and capture last-known
        #                  snapshots when jobs vanish.
        self.started_tracker = {}
        self.ended_tracker = {}
        self.prev_jobs_by_id = {}

        # Top three sections: re-built each render. Wrap each in a Filler so
        # the outer Pile can give them a weighted height (Pile-of-packs is a
        # flow widget; weight needs box).
        self.ended_section = u.Pile([u.Text("")])
        self.finishing_section = u.Pile([u.Text("")])
        self.started_section = u.Pile([u.Text("")])

        # Bottom section: existing pending queue with internal ListBox scroll.
        self.summary_text = u.Text("")
        self.col_header_text = u.AttrMap(u.Text(""), 'jobheader')
        self.job_walker = u.SimpleFocusListWalker([])
        self.job_listbox = u.ListBox(self.job_walker)
        pending_section = u.Pile([
            ('pack', self.summary_text),
            ('pack', u.Divider("─")),
            ('pack', self.col_header_text),
            ('pack', u.Divider("─")),
            u.ScrollBar(self.job_listbox),
        ])

        outer = u.Pile([
            ('weight', self.SECTION_WEIGHTS[0],
             u.Filler(self.ended_section, valign='top')),
            ('weight', self.SECTION_WEIGHTS[1],
             u.Filler(self.finishing_section, valign='top')),
            ('weight', self.SECTION_WEIGHTS[2],
             u.Filler(self.started_section, valign='top')),
            ('weight', self.SECTION_WEIGHTS[3], pending_section),
        ], focus_item=3)
        self.outer_pile = outer

        self.container = rounded_box(outer, title='Queue Status - Job Lifecycle Flow')

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, u.AttrMap(self.container, 'bg'))
        # Seed prev_jobs_by_id so the first transition check has something to
        # compare against (otherwise the first refresh after startup would
        # treat every running job as "newly started").
        self.prev_jobs_by_id = {j.job_id: j for j in self.jobs.jobs}
        self.update()

    # --- Signal / lifecycle -------------------------------------------------

    def on_jobs_update(self, *_a, **_kw):
        # Tracker bookkeeping has to run on every refresh, not just while
        # the view is active — otherwise switching to F7 after a stretch
        # away would reveal empty "ended/started" sections.
        self._update_trackers()
        if self.is_active():
            self._render()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        self.update()

    def update(self):
        """Called by ViewManager.show() and by the auto_refresh tick."""
        self._update_trackers()
        self._render()

    # --- Tracker bookkeeping -----------------------------------------------

    def _section_capacities(self):
        """Approximate row-count budget for each tracker section.

        Derived from the terminal height: the outer Pile splits its area by
        the configured weights, then each tracker section spends 2 rows on
        its title + column header.
        """
        total = max(0, getattr(self.main_screen, 'height', 30) - 2)
        weight_total = sum(self.SECTION_WEIGHTS)
        ended_h = int(total * self.SECTION_WEIGHTS[0] / weight_total)
        finish_h = int(total * self.SECTION_WEIGHTS[1] / weight_total)
        start_h = int(total * self.SECTION_WEIGHTS[2] / weight_total)
        return (
            max(0, ended_h - self.TRACKER_OVERHEAD),
            max(0, finish_h - self.TRACKER_OVERHEAD),
            max(0, start_h - self.TRACKER_OVERHEAD),
        )

    def _update_trackers(self):
        """Detect transitions and prune trackers; recomputed sections (like
        finishing-soon) happen inside _render."""
        now_mono = time.monotonic()
        current_jobs = list(self.jobs.jobs)
        current_by_id = {j.job_id: j for j in current_jobs}

        # 1) Pending → Running: a job that was PENDING last tick is now RUNNING.
        for jid, j in current_by_id.items():
            if jid in self.started_tracker:
                continue  # already tracked
            prev = self.prev_jobs_by_id.get(jid)
            if prev is None:
                continue  # never seen — don't count pre-existing jobs as "just started"
            was_pending = 'PENDING' in (getattr(prev, 'job_state', None) or [])
            now_running = 'RUNNING' in (getattr(j, 'job_state', None) or [])
            if was_pending and now_running:
                self.started_tracker[jid] = (_snapshot_job(j), now_mono)

        # 2) Vanished from scontrol: present last tick, gone now → ended.
        for jid, prev in self.prev_jobs_by_id.items():
            if jid in current_by_id:
                continue
            if jid in self.ended_tracker:
                continue
            self.ended_tracker[jid] = (_snapshot_job(prev), now_mono)
            # A job that just ended cannot also be "just started".
            self.started_tracker.pop(jid, None)

        # 3) Age out the started tracker.
        self.started_tracker = {
            jid: entry for jid, entry in self.started_tracker.items()
            if now_mono - entry[1] < STARTED_MAX_AGE
        }

        # 4) Capacity-driven eviction (FIFO — oldest entries leave first).
        ended_cap, _, started_cap = self._section_capacities()
        self.started_tracker = self._cap_dict(self.started_tracker, started_cap)
        self.ended_tracker = self._cap_dict(self.ended_tracker, ended_cap)

        # 5) Save current state for the next transition check.
        self.prev_jobs_by_id = current_by_id

    @staticmethod
    def _cap_dict(tracker, cap):
        """Trim a tracker dict to `cap` newest entries (drop oldest)."""
        if cap <= 0:
            return {}
        if len(tracker) <= cap:
            return tracker
        # Newest = highest monotonic timestamp; keep the last `cap` of those.
        sorted_items = sorted(tracker.items(), key=lambda kv: kv[1][1])
        return dict(sorted_items[-cap:])

    # --- Rendering ---------------------------------------------------------

    def _width(self):
        return (self.main_screen.width - 3) if hasattr(self.main_screen, 'width') else 120

    def _render(self):
        width = self._width()
        ended_cap, finish_cap, started_cap = self._section_capacities()

        self._render_ended_section(width, ended_cap)
        self._render_finishing_section(width, finish_cap)
        self._render_started_section(width, started_cap)
        self._render_pending_section(width)

    def _set_section_contents(self, section_pile, widgets):
        section_pile.contents = [(w, ('pack', None)) for w in widgets]

    def _render_ended_section(self, width, cap):
        # Newest first.
        items = sorted(self.ended_tracker.values(), key=lambda v: v[1], reverse=True)
        title = SectionTitleWidget("Just ended", len(items), width=width)
        col_header = u.AttrMap(u.Text(_ended_header(width)), 'faded')
        body = [title, col_header]
        if not items:
            body.append(u.Text(("faded", "  (no jobs ended since the view opened)")))
        else:
            for snap, _ts_seen in items[:cap] if cap else []:
                body.append(EndedJobWidget(snap, width=width))
        self._set_section_contents(self.ended_section, body)

    def _render_finishing_section(self, width, cap):
        # All currently-RUNNING jobs (excluding ones in started_tracker),
        # sorted by least time remaining.
        now = time.time()
        candidates = []
        for j in self.jobs.jobs:
            if 'RUNNING' not in (getattr(j, 'job_state', None) or []):
                continue
            if j.job_id in self.started_tracker:
                continue
            end_ts = _ts(getattr(j, 'end_time', {}))
            if end_ts <= 0:
                continue
            remaining = end_ts - now
            if remaining <= 0:
                continue
            candidates.append((remaining, j))
        candidates.sort(key=lambda x: x[0])

        title = SectionTitleWidget("Finishing soon", len(candidates), width=width)
        col_header = u.AttrMap(u.Text(_finishing_header(width)), 'faded')
        body = [title, col_header]
        if not candidates:
            body.append(u.Text(("faded", "  (no running jobs with a known end time)")))
        else:
            for _, job in candidates[:cap] if cap else []:
                body.append(FinishingJobWidget(job, width=width))
        self._set_section_contents(self.finishing_section, body)

    def _render_started_section(self, width, cap):
        items = sorted(self.started_tracker.values(), key=lambda v: v[1], reverse=True)
        title = SectionTitleWidget("Just started", len(items), width=width)
        col_header = u.AttrMap(u.Text(_started_header(width)), 'faded')
        body = [title, col_header]
        if not items:
            body.append(u.Text(("faded", "  (no jobs have started since the view opened)")))
        else:
            for snap, _ts_seen in items[:cap] if cap else []:
                body.append(StartedJobWidget(snap, width=width))
        self._set_section_contents(self.started_section, body)

    def _render_pending_section(self, width):
        self.job_walker.clear()
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
                    else:
                        expanded = key in self.expanded_groups
                        widgets.append(QueueGroupWidget(
                            key, s, e, group, width=width, expanded=expanded,
                        ))
                        if expanded:
                            for i, job in enumerate(group):
                                widgets.append(QueueJobWidget(
                                    job, s + i, width=width, parent_group_key=key,
                                ))
                    rank += n

        self.job_walker.extend(widgets)
        self._restore_focus()

    # --- Focus management (pending section only) ---------------------------

    def _restore_focus(self):
        """Re-anchor focus on the previously-selected job or group."""
        if not len(self.job_walker):
            return
        if self.selected_jobid is not None:
            for i, w in enumerate(self.job_walker):
                if getattr(w, 'jobid', None) == self.selected_jobid:
                    self.job_walker.set_focus(i)
                    return
        if self.selected_group_key is not None:
            for i, w in enumerate(self.job_walker):
                if getattr(w, 'group_key', None) == self.selected_group_key:
                    self.job_walker.set_focus(i)
                    return
        # First render or previously-focused row vanished: focus the first
        # partition header so the listbox doesn't scroll past it.
        self.job_walker.set_focus(0)

    def _capture_focus(self):
        """Snapshot current focus so the next rebuild can restore it."""
        focus_w, _ = self.job_listbox.get_focus()
        if focus_w is None:
            return
        self.selected_jobid = getattr(focus_w, 'jobid', None)
        self.selected_group_key = (
            getattr(focus_w, 'group_key', None)
            or getattr(focus_w, 'parent_group_key', None)
        )

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        focus_w, _ = self.job_listbox.get_focus()

        if key in ('e', 'enter', ' ') and focus_w is not None:
            # 'e' always toggles the containing group, even from a child row.
            if key == 'e':
                gkey = (getattr(focus_w, 'group_key', None)
                        or getattr(focus_w, 'parent_group_key', None))
                if gkey:
                    self._toggle_group(gkey)
                    return None

            # Enter/Space on a group header toggles it; on a job opens info.
            if hasattr(focus_w, 'group_key'):
                self._toggle_group(focus_w.group_key)
                return None
            if hasattr(focus_w, 'jobid'):
                from slop.ui.overlays import JobInfoOverlay
                job = self.jobs.job_index.get(focus_w.jobid)
                if job:
                    self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
                return None

        result = super().keypress(size, key)
        self._capture_focus()
        return result

    def _toggle_group(self, key):
        if key in self.expanded_groups:
            self.expanded_groups.remove(key)
        else:
            self.expanded_groups.add(key)
        # Anchor focus on the group header so the rebuilt list comes back to
        # it (whether we toggled from the header itself or from a child row).
        self.selected_group_key = key
        self.selected_jobid = None
        self.update()
