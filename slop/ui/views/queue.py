"""Queue Status view (F7) - the lifecycle "flow" of jobs.

Four sections, top to bottom, mirroring the upward flow of a job:

  1. Recently finished (jobs that vanished from scontrol since the last refresh)
  2. Finishing next    (RUNNING ranked by least time-remaining, plus any
                        COMPLETING/CG epilog jobs pinned to the top)
  3. Recently started  (RUNNING/COMPLETING jobs whose start_time is in the
                        last 15 min)
  4. Starting next     (cluster-wide pending jobs sorted by ETA)

Section 1 is tracker-driven: entries arrive when a job vanishes from scontrol
and linger until display capacity forces eviction (FIFO). Sections 2, 3, 4
are recomputed each refresh from the current job set. Section 3 uses
`start_time` as the source of truth — earlier versions tried to detect a
PENDING→RUNNING state transition across two refresh ticks, which silently
dropped jobs that scheduled fast enough to appear as RUNNING on the very
first tick we saw them.

Sections 2 and 3 are *not* mutually exclusive: a short, freshly-started job
(e.g. 5 min runtime) is both "recently started" and "finishing next" and
shows in both. They answer different questions — recent scheduling activity
vs imminent completion — and forcing exclusion would have hidden short jobs
from the finishing list entirely.

The partition-grouped pending list that used to sit in section 4 now lives in
the Scheduler view (F8); F7 is exclusively the "what's about to change" view.
"""

import urwid as u
import time
from slop.utils import compact_tres
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.widgets import rounded_box, SectionBanner
from slop.ui.state_style import state_attr, state_short
from slop.ui.views.queue_helpers import (
    coarse_duration,
    job_priority,
    job_partition,
    ts,
    eta_seconds,
    format_eta_seconds,
    format_wait,
    time_limit_str,
    reason_attr,
)


# How long after start_time a job still counts as "just started".
STARTED_MAX_AGE = 15 * 60
# Tolerance for start_time being slightly in the future relative to our local
# clock. Slurm sometimes reports a scheduler-side timestamp a few seconds
# ahead of the host's wall clock; without this tolerance the job is rejected
# from "Recently started" and silently falls into "Finishing next".
STARTED_FUTURE_TOLERANCE = 5 * 60


# ----- Lifecycle helpers --------------------------------------------------

def _is_just_started(job, now):
    """RUNNING/COMPLETING and started within the last STARTED_MAX_AGE seconds.

    Replaces the old PENDING→RUNNING transition tracker, which missed jobs
    that scheduled inside a single refresh interval (we never observed them
    in the PENDING state, so the transition check stayed false forever).
    """
    states = getattr(job, 'job_state', None) or []
    if 'RUNNING' not in states and 'COMPLETING' not in states:
        return False
    start_ts = ts(getattr(job, 'start_time', {}))
    if start_ts <= 0:
        return False
    age = now - start_ts
    return -STARTED_FUTURE_TOLERANCE <= age <= STARTED_MAX_AGE


def _snapshot_job(job):
    """Capture enough data to render a row even after the job leaves scontrol."""
    state = job.job_state[0] if getattr(job, 'job_state', None) else 'UNKNOWN'
    return {
        'jobid': job.job_id,
        'name': (getattr(job, 'name', None) or EMPTY_PLACEHOLDER),
        'user': getattr(job, 'user_name', EMPTY_PLACEHOLDER),
        'partition': job_partition(job),
        'state': state,
        'submit_ts': ts(getattr(job, 'submit_time', {})),
        'start_ts': ts(getattr(job, 'start_time', {})),
        'end_ts': ts(getattr(job, 'end_time', {})),
        'time_limit_min': (getattr(job, 'time_limit', {}).get('number', 0)
                           if isinstance(getattr(job, 'time_limit', {}), dict)
                           and getattr(job, 'time_limit', {}).get('set') else 0),
        'resources': compact_tres(job),
        'returncode': getattr(job, 'returncode', EMPTY_PLACEHOLDER),
    }


# ----- Lifecycle row formatters -------------------------------------------
#
# Each formatter builds a fixed-width prefix and lets `name` absorb whatever
# horizontal space is left over. Field widths are sized for the longest
# values we realistically see ("SUCCESS(0)" exit codes, 14-char usernames,
# etc.) so no truncation happens at typical terminal widths. The narrow
# (<100) fallback drops several columns and uses tight widths.


def _flex_row(prefix, name, width):
    """Append `name` to `prefix` so the total line fits `width`."""
    name_w = max(8, width - len(prefix))
    return prefix + str(name)[:name_w]


def _format_ended_row(width, *, state, jobid, user, partition,
                      used, limit, exit_code, resources, waited, name):
    if width < 100:
        return f"{state:>3} {jobid:>9} {user:<10} {used:>8}/{limit:<8} {name}"
    prefix = (f"{state:>3} {jobid:>9} {user:<14} {partition:<12} "
              f"{used:>9}/{limit:<9} {exit_code:<11} {waited:>9}  "
              f"{resources:<20} ")
    return _flex_row(prefix, name, width)


def _ended_header(width):
    return _format_ended_row(
        width, state='St', jobid='Job ID', user='User', partition='Partition',
        used='Used', limit='Limit', exit_code='Exit', waited='Waited',
        resources='Resources', name='Name',
    )


def _format_finishing_row(width, *, state, jobid, user, partition,
                          remaining, ran, resources, name):
    if width < 100:
        return f"{state:>3} {jobid:>9} {user:<10} {remaining:>10} {name}"
    prefix = (f"{state:>3} {jobid:>9} {user:<14} {partition:<12} "
              f"{remaining:>11} {ran:>11}  {resources:<20} ")
    return _flex_row(prefix, name, width)


def _finishing_header(width):
    return _format_finishing_row(
        width, state='St', jobid='Job ID', user='User', partition='Partition',
        remaining='Remaining', ran='Ran', resources='Resources', name='Name',
    )


def _format_started_row(width, *, state, jobid, user, partition,
                        wait, ran, tlim, resources, name):
    if width < 100:
        return f"{state:>3} {jobid:>9} {user:<10} {wait:>10} {ran:>10} {name}"
    prefix = (f"{state:>3} {jobid:>9} {user:<14} {partition:<12} "
              f"{wait:>10} {ran:>10} {tlim:>9}  {resources:<20} ")
    return _flex_row(prefix, name, width)


def _started_header(width):
    return _format_started_row(
        width, state='St', jobid='Job ID', user='User', partition='Partition',
        wait='Waited', ran='Ran', tlim='Limit',
        resources='Resources', name='Name',
    )


def _format_about_row(width, *, eta, jobid, user, partition, priority,
                      reason, resources, tlim, wait, name):
    if width < 100:
        return f"{eta:<13} {jobid:>9} {user:<10} {partition:<12} {reason:<14} {name:<20}"
    return (f"{eta:<14} {jobid:>9} {user:<12} {partition:<14} "
            f"{priority:>8} {reason:<14} {resources:<18} {tlim:>9} {wait:>9}  {name:<20}")


def _about_header(width):
    return _format_about_row(
        width, eta='ETA', jobid='Job ID', user='User', partition='Partition',
        priority='Priority', reason='Reason', resources='Resources',
        tlim='Time', wait='Waited', name='Name',
    )


# ----- Widgets ------------------------------------------------------------

class _ReadOnlyRow(u.WidgetWrap):
    """Base for non-interactive rows in the upper sections."""

    def selectable(self):
        return False


class EndedJobWidget(_ReadOnlyRow):
    def __init__(self, snap, width=120):
        used_str = (coarse_duration(int(snap['end_ts'] - snap['start_ts']))
                    if snap['end_ts'] and snap['start_ts']
                    and snap['end_ts'] >= snap['start_ts']
                    else EMPTY_PLACEHOLDER)
        limit_str = (coarse_duration(snap['time_limit_min'] * 60)
                     if snap['time_limit_min'] else EMPTY_PLACEHOLDER)
        waited_str = (coarse_duration(int(snap['start_ts'] - snap['submit_ts']))
                      if snap['start_ts'] and snap['submit_ts']
                      and snap['start_ts'] >= snap['submit_ts']
                      else EMPTY_PLACEHOLDER)
        text = _format_ended_row(
            width,
            state=state_short(snap['state']),
            jobid=str(snap['jobid'])[:9],
            user=str(snap['user'])[:14],
            partition=str(snap['partition'])[:12],
            used=used_str[:9],
            limit=limit_str[:9],
            exit_code=str(snap['returncode'])[:11],
            waited=waited_str[:9],
            resources=(snap.get('resources') or EMPTY_PLACEHOLDER)[:20],
            name=snap['name'],
        )
        super().__init__(u.AttrMap(u.Text(text), state_attr(snap['state'])))


class FinishingJobWidget(_ReadOnlyRow):
    def __init__(self, job, width=120):
        end_ts = ts(getattr(job, 'end_time', {}))
        start_ts = ts(getattr(job, 'start_time', {}))
        now = time.time()
        states = getattr(job, 'job_state', None) or []
        is_completing = 'COMPLETING' in states
        if is_completing:
            remaining = 'wrapping up'
        elif end_ts > now:
            remaining = coarse_duration(int(end_ts - now))
        else:
            remaining = EMPTY_PLACEHOLDER
        ran = (coarse_duration(int(now - start_ts))
               if start_ts and now >= start_ts else EMPTY_PLACEHOLDER)
        state = states[0] if states else 'R'
        text = _format_finishing_row(
            width,
            state=state_short(state),
            jobid=str(job.job_id)[:9],
            user=str(getattr(job, 'user_name', EMPTY_PLACEHOLDER))[:14],
            partition=job_partition(job)[:12],
            remaining=remaining[:11],
            ran=ran[:11],
            resources=(compact_tres(job) or EMPTY_PLACEHOLDER)[:20],
            name=str(getattr(job, 'name', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER),
        )
        # COMPLETING (epilog/cleanup) and <5 min remaining both get warning;
        # everything else stays neutral.
        if is_completing or (end_ts and end_ts - now < 300):
            attr = 'warning'
        else:
            attr = 'normal'
        super().__init__(u.AttrMap(u.Text(text), attr))


class StartedJobWidget(_ReadOnlyRow):
    def __init__(self, job, width=120):
        now = time.time()
        start_ts = ts(getattr(job, 'start_time', {}))
        submit_ts = ts(getattr(job, 'submit_time', {}))
        wait_str = (coarse_duration(int(start_ts - submit_ts))
                    if start_ts and submit_ts and start_ts >= submit_ts
                    else EMPTY_PLACEHOLDER)
        ran_str = (coarse_duration(int(now - start_ts))
                   if start_ts and now >= start_ts else EMPTY_PLACEHOLDER)
        state = job.job_state[0] if getattr(job, 'job_state', None) else 'R'
        text = _format_started_row(
            width,
            state=state_short(state),
            jobid=str(job.job_id)[:9],
            user=str(getattr(job, 'user_name', EMPTY_PLACEHOLDER))[:14],
            partition=job_partition(job)[:12],
            wait=wait_str[:10],
            ran=ran_str[:10],
            tlim=time_limit_str(job)[:9],
            resources=(compact_tres(job) or EMPTY_PLACEHOLDER)[:20],
            name=str(getattr(job, 'name', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER),
        )
        super().__init__(u.AttrMap(u.Text(text), state_attr(state)))


class AboutToStartJobWidget(u.WidgetWrap):
    """Selectable row for a pending job in the bottom (starting-next) section.
    Cluster-wide, sorted by ETA, partition column visible."""

    def __init__(self, job, width=120):
        self.job = job
        self.jobid = job.job_id
        self.width = width
        super().__init__(self._build())

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build(self):
        job = self.job
        diff = eta_seconds(getattr(job, 'start_time', {}))
        eta = format_eta_seconds(diff)
        priority = job_priority(job)
        reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER
        user = getattr(job, 'user_name', EMPTY_PLACEHOLDER)
        partition = job_partition(job)
        resources = compact_tres(job) or EMPTY_PLACEHOLDER
        tlim = time_limit_str(job)
        wait = format_wait(getattr(job, 'submit_time', {}))
        name = job.name or EMPTY_PLACEHOLDER

        text = _format_about_row(
            self.width,
            eta=eta[:14], jobid=str(job.job_id)[:9],
            user=user[:12], partition=partition[:14],
            priority=str(priority)[:8], reason=reason[:14],
            resources=resources[:18], tlim=tlim[:9], wait=wait[:9],
            name=name[:20],
        )
        # Soonest jobs (within 5 min or already overdue) get the success attr
        # so they pop visually; everything else stays neutral.
        if diff is not None and diff < 300:
            attr = 'success'
        else:
            attr = reason_attr(reason)
        return u.AttrMap(u.Text(text), attr, 'normal_selected')


# ----- Screen -------------------------------------------------------------

class ScreenViewQueue(u.WidgetWrap):
    """Lifecycle flow: recently finished → finishing next → recently started → starting next."""

    # Vertical weights for the four sections (ended, finishing, started, about).
    SECTION_WEIGHTS = (15, 15, 15, 55)
    # Per-section overhead (title row + column-header row).
    TRACKER_OVERHEAD = 2

    def __init__(self, main_screen, jobs):
        self.main_screen = main_screen
        self.jobs = jobs

        # Lifecycle trackers.
        # ended_tracker:   {jobid: (snap, monotonic_ts_when_noticed_gone)}
        # prev_jobs_by_id: jobid -> Job from last refresh, used to capture
        #                  last-known snapshots when jobs vanish from scontrol.
        self.ended_tracker = {}
        self.prev_jobs_by_id = {}

        # Bottom section: cluster-wide pending jobs sorted by ETA.
        self.selected_jobid = None

        # Top three sections: re-built each render. Wrap each in a Filler so
        # the outer Pile can give them a weighted height (Pile-of-packs is a
        # flow widget; weight needs box).
        self.ended_section = u.Pile([u.Text("")])
        self.finishing_section = u.Pile([u.Text("")])
        self.started_section = u.Pile([u.Text("")])

        # Bottom section uses a ListBox so the user can scroll past whatever
        # fits in the allotted height.
        self.about_summary = u.Text("")
        self.about_col_header = u.AttrMap(u.Text(""), 'jobheader')
        self.about_walker = u.SimpleFocusListWalker([])
        self.about_listbox = u.ListBox(self.about_walker)
        about_section = u.Pile([
            ('pack', self.about_summary),
            ('pack', u.Divider("─")),
            ('pack', self.about_col_header),
            ('pack', u.Divider("─")),
            u.ScrollBar(self.about_listbox),
        ])

        outer = u.Pile([
            ('weight', self.SECTION_WEIGHTS[0],
             u.Filler(self.ended_section, valign='top')),
            ('weight', self.SECTION_WEIGHTS[1],
             u.Filler(self.finishing_section, valign='top')),
            ('weight', self.SECTION_WEIGHTS[2],
             u.Filler(self.started_section, valign='top')),
            ('weight', self.SECTION_WEIGHTS[3], about_section),
        ], focus_item=3)
        self.outer_pile = outer

        self.container = rounded_box(outer, title='Queue Status - Job Lifecycle Flow')

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, u.AttrMap(self.container, 'bg'))
        # Seed prev_jobs_by_id so the first vanish-detection pass has
        # something to compare against.
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
        """Approximate row-count budget for each tracker section."""
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
        now_mono = time.monotonic()
        current_by_id = {j.job_id: j for j in self.jobs.jobs}

        # Vanished from scontrol: present last tick, gone now → ended.
        for jid, prev in self.prev_jobs_by_id.items():
            if jid in current_by_id or jid in self.ended_tracker:
                continue
            self.ended_tracker[jid] = (_snapshot_job(prev), now_mono)

        # Capacity-driven eviction (FIFO — oldest entries leave first).
        ended_cap, _, _ = self._section_capacities()
        self.ended_tracker = self._cap_dict(self.ended_tracker, ended_cap)

        self.prev_jobs_by_id = current_by_id

    @staticmethod
    def _cap_dict(tracker, cap):
        if cap <= 0:
            return {}
        if len(tracker) <= cap:
            return tracker
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
        self._render_about_section(width)

    def _set_section_contents(self, section_pile, widgets):
        section_pile.contents = [(w, ('pack', None)) for w in widgets]

    def _render_ended_section(self, width, cap):
        # Sort oldest-first, then keep the newest cap entries. Display order
        # is oldest-at-top / newest-at-bottom — the upward "flow" of the
        # view: a job vanishing from scontrol lands at the bottom of this
        # section and drifts up before being evicted off the top.
        items = sorted(self.ended_tracker.values(), key=lambda v: v[1])
        if cap:
            items = items[-cap:]

        # No count in the title: it's the "most recently finished N" where N
        # is whatever fits, same as the other dynamic sections.
        title = SectionBanner("Recently finished", width=width)
        col_header = u.AttrMap(u.Text(_ended_header(width)), 'faded')
        body = [title, col_header]
        if not items:
            body.append(u.Text(("faded", "  (no jobs have finished since the view opened)")))
        else:
            for snap, _ts_seen in items:
                body.append(EndedJobWidget(snap, width=width))
        self._set_section_contents(self.ended_section, body)

    def _render_finishing_section(self, width, cap):
        now = time.time()
        candidates = []
        for j in self.jobs.jobs:
            states = getattr(j, 'job_state', None) or []
            is_running = 'RUNNING' in states
            is_completing = 'COMPLETING' in states
            if not (is_running or is_completing):
                continue
            # No exclusion against "Recently started" — a short job (e.g. 5
            # min) would otherwise spend its entire life there and never
            # bubble up here. The two sections answer different questions
            # ("what just got scheduled?" vs "what's about to finish?") and
            # a fresh, soon-to-end job legitimately belongs in both.
            if is_completing:
                # Epilog/cleanup — sort to the very top of the section since
                # they're the closest to vanishing into "Recently finished".
                candidates.append((-1, j))
                continue
            end_ts = ts(getattr(j, 'end_time', {}))
            if end_ts <= 0:
                continue
            remaining = end_ts - now
            if remaining <= 0:
                continue
            candidates.append((remaining, j))
        candidates.sort(key=lambda x: x[0])

        # No count in the title: this section is always "the next N jobs to
        # finish" where N is whatever fits in the window — the total number
        # of running jobs would just be misleading.
        title = SectionBanner("Finishing next", width=width)
        col_header = u.AttrMap(u.Text(_finishing_header(width)), 'faded')
        body = [title, col_header]
        if not candidates:
            body.append(u.Text(("faded", "  (no running jobs with a known end time)")))
        else:
            for _, job in candidates[:cap] if cap else []:
                body.append(FinishingJobWidget(job, width=width))
        self._set_section_contents(self.finishing_section, body)

    def _render_started_section(self, width, cap):
        now = time.time()
        candidates = []
        for j in self.jobs.jobs:
            if not _is_just_started(j, now):
                continue
            start_ts = ts(getattr(j, 'start_time', {}))
            candidates.append((start_ts, j))
        # Sort oldest-first so the slice below keeps the newest. After
        # slicing, the visible block stays oldest-at-top / newest-at-bottom,
        # matching the upward "flow" of the view: a freshly-started job
        # appears at the bottom and drifts up as more jobs start behind it.
        candidates.sort(key=lambda x: x[0])
        if cap:
            candidates = candidates[-cap:]  # keep newest, drop oldest off the top

        # No count in the title: this section is always "the most recently
        # started N jobs" where N is whatever fits in the window.
        title = SectionBanner("Recently started", width=width)
        col_header = u.AttrMap(u.Text(_started_header(width)), 'faded')
        body = [title, col_header]
        if not candidates:
            body.append(u.Text(
                ("faded", "  (no jobs have started in the last 15 minutes)")))
        else:
            for _, job in candidates:
                body.append(StartedJobWidget(job, width=width))
        self._set_section_contents(self.started_section, body)

    def _render_about_section(self, width):
        """Cross-partition pending jobs sorted by ETA (soonest first)."""
        self.about_walker.clear()
        self.about_col_header.original_widget.set_text(_about_header(width))

        pending_with_eta = []
        pending_without = []
        for job in self.jobs.jobs:
            if 'PENDING' not in (getattr(job, 'job_state', None) or []):
                continue
            diff = eta_seconds(getattr(job, 'start_time', {}))
            if diff is None:
                pending_without.append(job)
            else:
                pending_with_eta.append((diff, job))
        pending_with_eta.sort(key=lambda x: x[0])

        total_pending = len(pending_with_eta) + len(pending_without)
        soonest = ''
        if pending_with_eta:
            soonest_diff = pending_with_eta[0][0]
            soonest = f'   soonest: {format_eta_seconds(soonest_diff)}'
        self.about_summary.set_text([
            ('jobheader', '  '),
            ('jobheader', f'Starting next: {len(pending_with_eta)}'),
            f'   no ETA: {len(pending_without)}',
            f'   total pending: {total_pending}',
            soonest,
        ])

        widgets = []
        if not pending_with_eta and not pending_without:
            widgets.append(u.Text(("faded", "  No pending jobs in the queue")))
        else:
            # ETA-known first (sorted by soonest), then unknowns at the end so
            # the user can still drill into them.
            for _, job in pending_with_eta:
                widgets.append(AboutToStartJobWidget(job, width=width))
            for job in pending_without:
                widgets.append(AboutToStartJobWidget(job, width=width))

        self.about_walker.extend(widgets)
        self._restore_about_focus()

    # --- Focus / keypress (starting-next section) --------------------------

    def _restore_about_focus(self):
        if not len(self.about_walker):
            return
        if self.selected_jobid is not None:
            for i, w in enumerate(self.about_walker):
                if getattr(w, 'jobid', None) == self.selected_jobid:
                    self.about_walker.set_focus(i)
                    return
        self.about_walker.set_focus(0)

    def _capture_about_focus(self):
        focus_w, _ = self.about_listbox.get_focus()
        if focus_w is None:
            return
        self.selected_jobid = getattr(focus_w, 'jobid', None)

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        focus_w, _ = self.about_listbox.get_focus()

        if key == 'enter' and focus_w is not None and hasattr(focus_w, 'jobid'):
            from slop.ui.overlays import JobInfoOverlay
            job = self.jobs.job_index.get(focus_w.jobid)
            if job:
                self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
            return None

        result = super().keypress(size, key)
        self._capture_about_focus()
        return result
