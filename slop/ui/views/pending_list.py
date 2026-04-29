"""Partition-grouped pending-jobs list — used by the Scheduler view (F8).

Encapsulates the entity that used to live at the bottom of the Queue view:
- One section per partition, ordered by the highest-priority job in each
- Consecutive same-(user, reason) pending jobs are bundled into expandable
  groups so the list stays digestible when one user submits a wave
- Per-partition rank, priority, ETA, wait, reason, size, time limit, name

Self-contained: subscribes to `jobs_updated`, owns its expand/collapse and
focus state across rebuilds, and intercepts e/Enter/Space when focused.
"""

import urwid as u
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.widgets import SafeListBox, SectionBanner
from slop.ui.views.queue_helpers import (
    coarse_duration,
    job_priority,
    job_partition,
    eta_seconds,
    format_eta_seconds,
    format_wait,
    time_limit_str,
    reason_attr,
)


def _format_eta(start_time):
    """Pending-list flavor of ETA formatting: uses EMPTY_PLACEHOLDER for unknown
    (the F8 column is narrow; the verbose 'Unknown' would push the layout)."""
    return format_eta_seconds(eta_seconds(start_time), unknown=EMPTY_PLACEHOLDER)


def _has_eta(job):
    return eta_seconds(getattr(job, 'start_time', {})) is not None


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
        priority = job_priority(job)
        eta = _format_eta(getattr(job, 'start_time', {}))
        wait = format_wait(getattr(job, 'submit_time', {}))
        reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER
        user = getattr(job, 'user_name', EMPTY_PLACEHOLDER)
        size = _size_indicator(job)
        tlim = time_limit_str(job)
        name = job.name or EMPTY_PLACEHOLDER

        text = _format_row(
            self.width, rank=str(self.rank), priority=str(priority),
            eta=eta[:13], wait=wait[:8], reason=reason[:18],
            user=user[:10], size=size, tlim=tlim[:11], name=name[:40],
        )
        return u.AttrMap(u.Text(text), reason_attr(reason), 'normal_selected')


class QueueGroupWidget(u.WidgetWrap):
    """Header row for a bundle of consecutive same-(user, reason) jobs."""

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
            # Compact subheader — children carry per-row detail, so repeating
            # priority/user/eta on the header would just be noise.
            line = f"  ▼ [{count} jobs {rng}]".ljust(max(self.width, 1))
            return u.AttrMap(u.Text(line), 'faded', 'normal_selected')

        first = self.job_group[0]
        priority = max(job_priority(j) for j in self.job_group)
        eta = _format_eta(getattr(first, 'start_time', {}))
        wait = format_wait(getattr(first, 'submit_time', {}))
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
            tlim = (coarse_duration(mn * 60) if mn == mx
                    else f"{coarse_duration(mn * 60)}-{coarse_duration(mx * 60)}")
        else:
            tlim = EMPTY_PLACEHOLDER

        name = f"▶ [{count} jobs {rng}]"
        text = _format_row(
            self.width, rank=str(self.start_rank), priority=str(priority),
            eta=eta[:13], wait=wait[:8], reason=reason[:18],
            user=user[:10], size=size, tlim=tlim[:11], name=name[:40],
        )
        return u.AttrMap(u.Text(text), reason_attr(reason), 'normal_selected')


def _partition_banner(partition, total, with_eta, width):
    label = f"{partition}  ({total} pending"
    if with_eta:
        label += f", {with_eta} with ETA"
    label += ")"
    return SectionBanner(label, width=width)


# ----- The widget ---------------------------------------------------------

class PendingListWidget(u.WidgetWrap):
    """Self-contained partition-grouped pending-jobs panel.

    Layout: summary header → divider → column header → divider → scrolling
    ListBox of partition headers + grouped/expanded rows.
    """

    def __init__(self, main_screen, jobs):
        self.main_screen = main_screen
        self.jobs = jobs

        self.expanded_groups = set()
        # Focus anchors restored across rebuilds (refresh, expand/collapse).
        self.selected_jobid = None
        self.selected_group_key = None

        self.summary_text = u.Text("")
        self.col_header_text = u.AttrMap(u.Text(""), 'jobheader')
        self.job_walker = u.SimpleFocusListWalker([])
        self.job_listbox = SafeListBox(self.job_walker)

        pile = u.Pile([
            ('pack', self.summary_text),
            ('pack', u.Divider("─")),
            ('pack', self.col_header_text),
            ('pack', u.Divider("─")),
            u.ScrollBar(self.job_listbox),
        ])

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        super().__init__(pile)
        self.update()

    # --- Lifecycle ---------------------------------------------------------

    def on_jobs_update(self, *_a, **_kw):
        # Cheap to rebuild; let the parent decide if it's visible.
        self.update()

    def update(self):
        width = (self.main_screen.width - 3) if hasattr(self.main_screen, 'width') else 120
        self.job_walker.clear()
        self.col_header_text.original_widget.set_text(_format_header(width))

        # Bucket pending jobs by their first listed partition.
        by_part = {}
        for job in self.jobs.jobs:
            if not (hasattr(job, 'job_state') and 'PENDING' in job.job_state):
                continue
            by_part.setdefault(job_partition(job), []).append(job)

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
                key=lambda kv: max(job_priority(j) for j in kv[1]),
                reverse=True,
            )
            for partition, jobs in ordered:
                jobs_sorted = sorted(jobs, key=job_priority, reverse=True)
                with_eta = sum(1 for j in jobs_sorted if _has_eta(j))
                widgets.append(_partition_banner(
                    partition, len(jobs_sorted), with_eta, width,
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

    # --- Focus & keypress --------------------------------------------------

    def _restore_focus(self):
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
