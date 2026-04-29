"""Queue Status view (F7) - the lifecycle "flow" of jobs.

Four sections, top to bottom, mirroring the upward flow of a job:

  1. Recently finished (jobs in a terminal state — CANCELLED/COMPLETED/
                        FAILED/TIMEOUT/OOM/DEADLINE — or jobs that vanished
                        from scontrol since the last refresh)
  2. Finishing next    (RUNNING ranked by least time-remaining, plus any
                        COMPLETING/CG epilog jobs pinned to the top)
  3. Recently started  (RUNNING/COMPLETING jobs whose start_time is in the
                        last 15 min)
  4. Starting next     (cluster-wide pending jobs sorted by ETA)

Section 1 is tracker-driven on two triggers: a job's state transitioning
from non-terminal to terminal (`job_state_ended`) during a refresh, OR a
job vanishing from scontrol entirely. Both triggers require us to have
seen the job in a non-terminal state at least once — we don't surface
jobs that were already terminal at first observation, since those finished
before we started watching and would clutter the conveyor belt with
ancient history. Slurm holds completed records for `MinJobAge` (default
5 min) before purging them; the state-transition trigger surfaces a job
immediately on completion rather than waiting for that purge. Once
tracked, entries linger until display capacity forces eviction (FIFO). Sections 2, 3, 4 are
recomputed each refresh from the current job set, with anything already
in the ended tracker excluded so it doesn't double-up. Section 3 uses
`start_time` as the source of truth — earlier versions tried to detect a
PENDING→RUNNING state transition across two refresh ticks, which silently
dropped jobs that scheduled fast enough to appear as RUNNING on the very
first tick we saw them.

Section 2 wins over section 3 when a short job qualifies for both: a job
already shown in "Finishing next" is suppressed from "Recently started"
even if that leaves the lower section under-filled. The duplicate row was
the more confusing failure mode; an empty slot in "Recently started" reads
fine because the job is still visible (just one row higher).

The partition-grouped pending list that used to sit in section 4 now lives in
the Scheduler view (F8); F7 is exclusively the "what's about to change" view.
"""

import urwid as u
import time
from slop.utils import compact_tres
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.widgets import rounded_box, SectionBanner
from slop.ui.state_style import state_attr, state_short
from slop.slurm.state import job_state_ended
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
# Window over which the "Recently finished" section accumulates summary
# stats. Independent of the display row count so we can summarize a longer
# span than fits on screen.
ENDED_STATS_WINDOW = 60 * 60


# ----- Lifecycle helpers --------------------------------------------------

def _tres_int(job, key):
    """Return the integer count for a TRES `key` on `job`, or 0 if absent.

    Used by the §2↔§3 gap footer to aggregate cpus/nodes across the
    steady-state running pool. We parse the raw alloc/req string rather
    than reusing `compact_tres` because we want raw integer counts, not
    the human-formatted compact rendering.
    """
    s = getattr(job, 'tres_alloc_str', '') or getattr(job, 'tres_req_str', '')
    if not s:
        return 0
    for entry in s.split(','):
        if '=' not in entry:
            continue
        k, v = entry.split('=', 1)
        if k == key:
            try:
                return int(v)
            except ValueError:
                return 0
    return 0


def _format_count(n):
    """Compact integer rendering: 999 → '999', 1234 → '1.2k', 12345 → '12k'."""
    if n < 1000:
        return str(n)
    if n < 10000:
        return f"{n / 1000:.1f}k"
    return f"{n // 1000}k"


def _gap_footer(text):
    """Faded one-line summary that sits at the bottom of a section to
    describe jobs which logically belong "between" this section and the
    one below it but don't qualify for any visible row. The leading
    ellipsis hints at continuation from the rows above."""
    return u.Text(("faded", f"  …{text}"))


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
        'account': getattr(job, 'account', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER,
        'partition': job_partition(job),
        'nodes': getattr(job, 'nodes', '') or EMPTY_PLACEHOLDER,
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


def _format_clock_ts(epoch_ts):
    """`HH:MM` if today, else `dd/mm@HH:MM`.

    The `@` glues the date to the time so the row reads cleanly when the
    next column is also `HH:MM` — a plain space looks like another column.
    """
    if not epoch_ts:
        return EMPTY_PLACEHOLDER
    import datetime as _dt
    t = _dt.datetime.fromtimestamp(int(epoch_ts))
    if t.date() == _dt.datetime.now().date():
        return t.strftime('%H:%M')
    return t.strftime('%d/%m@%H:%M')


# ----- Column layouts -----------------------------------------------------
#
# Each layout is a list of (label, align, kind, size, wrap) tuples shared by
# the row builder and the header builder so columns line up automatically.
#   label: header text
#   align: 'left' or 'right' (passed to the cell's Text widget)
#   kind:  'given' (fixed N chars) or 'weight' (proportional share)
#   size:  N (chars for 'given', relative weight for 'weight')
#   wrap:  'clip' or 'ellipsis' for overflow
#
# Weight columns absorb the leftover horizontal space — the row scales with
# the terminal instead of being truncated at hard-coded widths.

# Shared column widths so the same column lands at the same horizontal
# position in every section it appears in. Only `Name` is weighted — it
# soaks up the residual width so wider terminals show more of the job
# name. The User column doubles as account display: "username (project)"
# instead of two separate columns.
_JOBID_W     = 10
_USER_W      = 28
_PARTITION_W = 12
_RESOURCES_W = 22
_NODES_W     = 14

USER      = ('User',      'left', 'given', _USER_W,      'ellipsis')
PARTITION = ('Partition', 'left', 'given', _PARTITION_W, 'ellipsis')
RESOURCES = ('Resources', 'left', 'given', _RESOURCES_W, 'ellipsis')
NODES     = ('Nodes',     'left', 'given', _NODES_W,     'ellipsis')
NAME      = ('Name',      'left', 'weight', 1,           'ellipsis')


def _user_account(user, account):
    """Render a user with their account folded in as 'user (account)'."""
    if not account or account == EMPTY_PLACEHOLDER:
        return user or EMPTY_PLACEHOLDER
    return f"{user} ({account})"


ENDED_LAYOUT = [
    ('St',     'right', 'given', 3,         'clip'),
    ('Job ID', 'right', 'given', _JOBID_W,  'clip'),
    USER, PARTITION,
    ('Submitted',      'left',  'given', 11, 'clip'),
    ('Ended',          'left',  'given', 11, 'clip'),
    ('Requested/Used', 'right', 'given', 16, 'clip'),
    ('Exit',           'left',  'given', 11, 'clip'),
    ('Waited',         'right', 'given', 9,  'clip'),
    RESOURCES, NODES, NAME,
]

FINISHING_LAYOUT = [
    ('St',     'right', 'given', 3,         'clip'),
    ('Job ID', 'right', 'given', _JOBID_W,  'clip'),
    USER, PARTITION,
    ('Remaining', 'right', 'given', 12, 'clip'),
    ('Ran',       'right', 'given', 11, 'clip'),
    RESOURCES, NODES, NAME,
]

STARTED_LAYOUT = [
    ('St',     'right', 'given', 3,         'clip'),
    ('Job ID', 'right', 'given', _JOBID_W,  'clip'),
    USER, PARTITION,
    ('Waited', 'right', 'given', 10, 'clip'),
    ('Ran',    'right', 'given', 10, 'clip'),
    ('Limit',  'right', 'given',  9, 'clip'),
    RESOURCES, NODES, NAME,
]

# Pending jobs lead with ETA — that's the field a scheduler-watcher cares
# about most — and have no St / Nodes (they all share state PD and aren't
# allocated yet).
ABOUT_LAYOUT = [
    ('ETA',       'left',  'given', 11,        'clip'),
    ('Job ID',    'right', 'given', _JOBID_W,  'clip'),
    USER, PARTITION,
    ('Priority',  'right', 'given',  8, 'clip'),
    ('Reason',    'left',  'given', 14, 'clip'),
    ('Time',      'right', 'given',  9, 'clip'),
    ('Waited',    'right', 'given',  9, 'clip'),
    RESOURCES, NAME,
]


def _row(layout, values):
    """Build a `u.Columns` row from `values` paralleling `layout`."""
    cols = []
    for (_label, align, kind, size, wrap), value in zip(layout, values):
        t = u.Text(str(value), align=align)
        t.set_wrap_mode(wrap)
        cols.append((kind, size, t))
    return u.Columns(cols, dividechars=1)


def _header(layout):
    """Build the column-header row for `layout`. Each title inherits its
    column's data alignment so the label sits directly over the values it
    describes — header right-edge above number right-edge for numeric
    columns, left-aligned text above left-aligned text."""
    return _row(layout, [col[0] for col in layout])


# ----- Widgets ------------------------------------------------------------

class _JobRow(u.WidgetWrap):
    """Base for selectable job rows. Subclasses build the inner widget and
    pass the job id (or snapshot id for ended jobs) to ``__init__`` so the
    screen-level keypress handler can open JobInfoOverlay on Enter."""

    def __init__(self, w, jobid):
        self.jobid = jobid
        super().__init__(w)

    def selectable(self):
        return True

    def keypress(self, size, key):
        # Let the parent Pile / screen handler decide what to do with
        # everything (including 'enter'); rows themselves are dumb labels.
        return key


class EndedJobWidget(_JobRow):
    def __init__(self, snap):
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
        values = [
            state_short(snap['state']),
            snap['jobid'],
            _user_account(snap['user'], snap.get('account')),
            snap['partition'],
            _format_clock_ts(snap['submit_ts']),
            _format_clock_ts(snap['end_ts']),
            f"{limit_str}/{used_str}",
            snap['returncode'],
            waited_str,
            snap.get('resources') or EMPTY_PLACEHOLDER,
            snap.get('nodes') or EMPTY_PLACEHOLDER,
            snap['name'],
        ]
        super().__init__(
            u.AttrMap(_row(ENDED_LAYOUT, values),
                      state_attr(snap['state']), 'normal_selected'),
            snap['jobid'],
        )


class FinishingJobWidget(_JobRow):
    def __init__(self, job):
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
        values = [
            state_short(state),
            job.job_id,
            _user_account(getattr(job, 'user_name', EMPTY_PLACEHOLDER),
                          getattr(job, 'account', None)),
            job_partition(job),
            remaining,
            ran,
            compact_tres(job) or EMPTY_PLACEHOLDER,
            getattr(job, 'nodes', '') or EMPTY_PLACEHOLDER,
            getattr(job, 'name', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER,
        ]
        # COMPLETING (epilog/cleanup) and <5 min remaining both get warning;
        # everything else stays neutral.
        if is_completing or (end_ts and end_ts - now < 300):
            attr = 'warning'
        else:
            attr = 'normal'
        super().__init__(
            u.AttrMap(_row(FINISHING_LAYOUT, values), attr, 'normal_selected'),
            job.job_id,
        )


class StartedJobWidget(_JobRow):
    def __init__(self, job):
        now = time.time()
        start_ts = ts(getattr(job, 'start_time', {}))
        submit_ts = ts(getattr(job, 'submit_time', {}))
        wait_str = (coarse_duration(int(start_ts - submit_ts))
                    if start_ts and submit_ts and start_ts >= submit_ts
                    else EMPTY_PLACEHOLDER)
        ran_str = (coarse_duration(int(now - start_ts))
                   if start_ts and now >= start_ts else EMPTY_PLACEHOLDER)
        state = job.job_state[0] if getattr(job, 'job_state', None) else 'R'
        values = [
            state_short(state),
            job.job_id,
            _user_account(getattr(job, 'user_name', EMPTY_PLACEHOLDER),
                          getattr(job, 'account', None)),
            job_partition(job),
            wait_str,
            ran_str,
            time_limit_str(job),
            compact_tres(job) or EMPTY_PLACEHOLDER,
            getattr(job, 'nodes', '') or EMPTY_PLACEHOLDER,
            getattr(job, 'name', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER,
        ]
        super().__init__(
            u.AttrMap(_row(STARTED_LAYOUT, values),
                      state_attr(state), 'normal_selected'),
            job.job_id,
        )


class AboutToStartJobWidget(_JobRow):
    """Selectable row for a pending job in the bottom (starting-next)
    section. Cluster-wide, sorted by ETA. F8 has the full interactive
    pending list with grouping."""

    def __init__(self, job):
        diff = eta_seconds(getattr(job, 'start_time', {}))
        eta = format_eta_seconds(diff)
        priority = job_priority(job)
        reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER) or EMPTY_PLACEHOLDER
        user = _user_account(getattr(job, 'user_name', EMPTY_PLACEHOLDER),
                             getattr(job, 'account', None))
        partition = job_partition(job)
        resources = compact_tres(job) or EMPTY_PLACEHOLDER
        tlim = time_limit_str(job)
        wait = format_wait(getattr(job, 'submit_time', {}))
        name = job.name or EMPTY_PLACEHOLDER

        values = [eta, job.job_id, user, partition, priority, reason,
                  tlim, wait, resources, name]
        # Soonest jobs (within 5 min or already overdue) get the success attr
        # so they pop visually; everything else stays neutral.
        if diff is not None and diff < 300:
            attr = 'success'
        else:
            attr = reason_attr(reason)
        super().__init__(
            u.AttrMap(_row(ABOUT_LAYOUT, values), attr, 'normal_selected'),
            job.job_id,
        )


# ----- Screen -------------------------------------------------------------

class ScreenViewQueue(u.WidgetWrap):
    """Lifecycle flow: recently finished → finishing next → recently started → starting next."""

    # Vertical weights for the four sections (ended, finishing, started, about).
    # Equal split — the bottom section is now a static top-of-pending preview,
    # not a scrolling walker, so it doesn't need extra space. Use F8 for the
    # full interactive pending list.
    SECTION_WEIGHTS = (1, 1, 1, 1)
    # Per-section overhead (title row + column-header row).
    TRACKER_OVERHEAD = 2

    def __init__(self, main_screen, jobs):
        self.main_screen = main_screen
        self.jobs = jobs

        # Lifecycle trackers.
        # ended_tracker:   {jobid: (snap, monotonic_ts_when_noticed_gone)}
        #                  Bounded by display row count — drives the rows
        #                  shown in the "Recently finished" section.
        # ended_stats:     {jobid: snap}
        #                  Same snapshots, but pruned by age (ENDED_STATS_
        #                  WINDOW) instead of by row count. Drives the
        #                  one-line summary above the rows.
        # prev_jobs_by_id: jobid -> Job from last refresh, used to capture
        #                  last-known snapshots when jobs vanish from scontrol.
        self.ended_tracker = {}
        self.ended_stats = {}
        self.prev_jobs_by_id = {}

        # Focus persistence across re-renders. The 3-second refresh tick
        # rebuilds every row widget; without snapshotting the user's
        # selection the cursor would jump back to the default each tick.
        # Default to "Recently finished" (section 0) — the first interesting
        # row to look at. Until the user presses an arrow key, we leave
        # focus unset if that section is empty rather than auto-jumping
        # somewhere else.
        self.focused_section = 0
        self.focused_jobid_by_section = {}
        self.has_navigated = False

        # All four sections re-built each render. Each pile carries its own
        # weight spacer so it's always a box widget and the outer Pile can
        # give it a weighted height directly. The spacer also pushes job
        # rows down to the bottom of the section — conveyor-belt flow: a
        # newly-arrived row appears at the bottom and drifts up as more
        # rows accumulate behind it.
        def _empty_pile():
            return u.Pile([('weight', 1, u.SolidFill(' '))])

        self.ended_section = _empty_pile()
        self.finishing_section = _empty_pile()
        self.started_section = _empty_pile()
        self.about_section = _empty_pile()

        outer = u.Pile([
            ('weight', self.SECTION_WEIGHTS[0], self.ended_section),
            ('weight', self.SECTION_WEIGHTS[1], self.finishing_section),
            ('weight', self.SECTION_WEIGHTS[2], self.started_section),
            ('weight', self.SECTION_WEIGHTS[3], self.about_section),
        ])
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
        """Approximate row-count budget for each section (ended, finishing,
        started, about)."""
        total = max(0, getattr(self.main_screen, 'height', 30) - 2)
        weight_total = sum(self.SECTION_WEIGHTS)
        return tuple(
            max(0, int(total * w / weight_total) - self.TRACKER_OVERHEAD)
            for w in self.SECTION_WEIGHTS
        )

    def _update_trackers(self):
        now_mono = time.monotonic()
        now_wall = time.time()
        current_by_id = {j.job_id: j for j in self.jobs.jobs}

        def _is_terminal(job):
            states = getattr(job, 'job_state', None) or []
            return any(s in job_state_ended for s in states)

        def _record(jid, snap):
            """Add to both display tracker and stats dict; cheap to call
            twice for the same jid since both checks are dict membership."""
            if jid not in self.ended_tracker:
                self.ended_tracker[jid] = (snap, now_mono)
            if jid not in self.ended_stats:
                self.ended_stats[jid] = snap

        # Trigger 1: job's state transitioned from non-terminal to terminal
        # during this tick. We require the previous tick's snapshot to have
        # been non-terminal — otherwise we'd dump every long-finished CD/CA/F
        # job that's still lingering in scontrol (Slurm keeps records around
        # for MinJobAge, often hours) onto the conveyor belt the instant we
        # first see them, even though they finished long before this session
        # started. The conveyor belt is "things that just happened", so we
        # only put jobs on it when we actually witnessed the transition.
        for jid, job in current_by_id.items():
            if not _is_terminal(job):
                continue
            prev = self.prev_jobs_by_id.get(jid)
            if prev is None or _is_terminal(prev):
                continue
            _record(jid, _snapshot_job(job))

        # Trigger 2: job vanished from scontrol entirely without us first
        # catching it in a terminal state (e.g. very short-lived, or the
        # transition fell between refresh ticks). Same first-observation
        # caveat: if the previous tick already had it as terminal, it was
        # already done before we started watching — don't surface it now.
        for jid, prev in self.prev_jobs_by_id.items():
            if jid in current_by_id:
                continue
            if _is_terminal(prev):
                continue
            _record(jid, _snapshot_job(prev))

        # Capacity-driven eviction for the display tracker (FIFO — oldest
        # entries leave first when the section is full).
        ended_cap = self._section_capacities()[0]
        self.ended_tracker = self._cap_dict(self.ended_tracker, ended_cap)

        # Time-driven eviction for the stats dict — keep anything that ended
        # within the last ENDED_STATS_WINDOW seconds.
        cutoff = now_wall - ENDED_STATS_WINDOW
        self.ended_stats = {jid: snap for jid, snap in self.ended_stats.items()
                            if snap.get('end_ts', 0) >= cutoff}

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
        ended_cap, finish_cap, started_cap, about_cap = self._section_capacities()
        now = time.time()

        # Plan candidate sets for §2/§3/§4 up-front so the gap-summary
        # footers know exactly which jobs each adjacent section will show
        # — the footer for a gap describes jobs that exist in the lifecycle
        # but fall outside every visible window, so it needs the union of
        # visible jobids across both sections it sits between.
        finishing_cands = self._finishing_candidates(now)
        finishing_visible = ({j.job_id for _, j in finishing_cands[:finish_cap]}
                             if finish_cap else set())

        started_cands = self._started_candidates(now, skip=finishing_visible)
        # `_started_candidates` returns oldest-first; we keep the newest
        # `cap` entries so the slice below gives us the visible block.
        started_visible_pairs = (started_cands[-started_cap:]
                                 if started_cap else [])
        started_visible = {j.job_id for _, j in started_visible_pairs}

        about_plan = self._about_plan(now)

        steady_footer = self._steady_state_footer(
            now, finishing_visible, started_visible)
        pending_footer = self._pending_depth_footer(about_plan, about_cap)

        self._render_ended_section(width, ended_cap)
        self._render_finishing_section(
            width, finishing_cands, finish_cap, footer=steady_footer)
        self._render_started_section(
            width, started_cands, started_cap, footer=pending_footer)
        self._render_about_section(width, about_plan, about_cap)
        self._restore_focus()

    def _set_section_contents(self, section_pile, top_widgets, row_widgets,
                              footer=None):
        """Pack `top_widgets` (title + col header + any info text) at the
        top, then pin `row_widgets` (job rows) to the bottom via a weight
        spacer in between. Conveyor-belt flow: newest entries appear at
        the very bottom of the section and drift up as more accumulate.

        Optional `footer` is appended below the rows — used for the gap
        summary line that describes jobs not visible in this section but
        logically present in the lifecycle. The footer steals the bottom
        line from the row block, which is desirable: it visually anchors
        the gap summary to the section boundary it explains."""
        contents = [(w, ('pack', None)) for w in top_widgets]
        contents.append((u.SolidFill(' '), ('weight', 1)))
        contents.extend((w, ('pack', None)) for w in row_widgets)
        if footer is not None:
            contents.append((footer, ('pack', None)))
        section_pile.contents = contents

    def _ended_stats_text(self):
        """One-line summary of jobs in the stats window, or None if empty.

        The window length is computed from the actual data (oldest end_ts)
        so we don't lie about the time span before we've been running long
        enough to fill it.
        """
        snaps = [s for s in self.ended_stats.values() if s.get('end_ts', 0) > 0]
        if not snaps:
            return None
        span = int(time.time() - min(s['end_ts'] for s in snaps))
        # Floor at 1s so coarse_duration always returns something readable.
        span_str = coarse_duration(max(1, span))

        # Break out the headline terminal states. "Other" catches the long
        # tail (TIMEOUT, OUT_OF_MEMORY, DEADLINE, NODE_FAIL, BOOT_FAIL,
        # PREEMPTED, REQUEUED, ...) so the four counts always sum to total.
        # CA is intentionally separate from F: a user-cancelled job isn't
        # a failure of the job itself.
        completed = sum(1 for s in snaps if s.get('state') == 'COMPLETED')
        canceled  = sum(1 for s in snaps if s.get('state') == 'CANCELLED')
        failed    = sum(1 for s in snaps if s.get('state') == 'FAILED')
        other     = len(snaps) - completed - canceled - failed

        runtimes = [s['end_ts'] - s['start_ts'] for s in snaps
                    if s.get('start_ts', 0) > 0
                    and s['end_ts'] >= s['start_ts']]
        waits = [s['start_ts'] - s['submit_ts'] for s in snaps
                 if s.get('start_ts', 0) > 0 and s.get('submit_ts', 0) > 0
                 and s['start_ts'] >= s['submit_ts']]

        parts = [f"{n} {label}" for n, label in
                 ((completed, 'completed'), (canceled, 'canceled'),
                  (failed, 'failed'), (other, 'other')) if n]
        if runtimes:
            parts.append(f"avg runtime {coarse_duration(int(sum(runtimes) / len(runtimes)))}")
        if waits:
            parts.append(f"avg wait {coarse_duration(int(sum(waits) / len(waits)))}")
        return f"Last {span_str}: " + ", ".join(parts)

    def _render_ended_section(self, width, cap):
        # Sort by the job's own end_time (ascending), then keep the newest cap
        # entries. Display order is oldest-at-top / newest-at-bottom — the
        # upward "flow" of the view: a job that just ended lands at the
        # bottom and drifts up before being evicted off the top. Sorting by
        # end_time (rather than the monotonic "noticed gone" tick) keeps the
        # ordering correct even when a refresh notices several vanished jobs
        # at once and the tracker timestamps are tied.
        items = sorted(self.ended_tracker.values(), key=lambda v: v[0]['end_ts'])
        if cap:
            items = items[-cap:]

        # Stats summary lives in the banner alongside the title — same
        # pattern as "Starting next  (192 pending)". Promotes the summary
        # to the section header instead of an extra line below it.
        stats = self._ended_stats_text()
        title_text = "Recently finished"
        if stats:
            title_text = f"{title_text}  ({stats})"
        title = SectionBanner(title_text, width=width)
        col_header = u.AttrMap(_header(ENDED_LAYOUT), 'faded')
        top = [title, col_header]
        if not items:
            top.append(u.Text(("faded", "  (no jobs have finished since the view opened)")))
            rows = []
        else:
            rows = [EndedJobWidget(snap) for snap, _ts_seen in items]
        self._set_section_contents(self.ended_section, top, rows)

    def _finishing_candidates(self, now):
        """RUNNING/COMPLETING jobs ordered for the §2 (Finishing next) section.

        Sorted ascending by remaining seconds; COMPLETING jobs use a
        sentinel of -1 so they sort to the very top (closest to vanishing
        into Recently finished). Excludes anything already in
        `ended_tracker` — that job was both COMPLETING and terminal in
        the same tick and Recently finished has already captured it."""
        candidates = []
        for j in self.jobs.jobs:
            if j.job_id in self.ended_tracker:
                continue
            states = getattr(j, 'job_state', None) or []
            is_running = 'RUNNING' in states
            is_completing = 'COMPLETING' in states
            if not (is_running or is_completing):
                continue
            if is_completing:
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
        return candidates

    def _started_candidates(self, now, skip):
        """Jobs that recently entered RUNNING for the §3 section.

        `skip` is the set of jobids already visible in §2 (Finishing next)
        — short jobs that qualify for both sections are kept in §2 only,
        even at the cost of an under-filled §3, because a duplicate row
        is the more confusing failure mode."""
        candidates = []
        for j in self.jobs.jobs:
            if j.job_id in skip:
                continue
            if j.job_id in self.ended_tracker:
                continue
            if not _is_just_started(j, now):
                continue
            start_ts = ts(getattr(j, 'start_time', {}))
            candidates.append((start_ts, j))
        # Sort oldest-first so the caller's `[-cap:]` slice keeps the
        # newest. After slicing, the visible block stays oldest-at-top /
        # newest-at-bottom, matching the upward conveyor-belt flow.
        candidates.sort(key=lambda x: x[0])
        return candidates

    def _about_plan(self, now):
        """Group pending jobs by ETA-known/unknown plus headline stats.

        Returns a dict with the ordered visible-eligible list, the no-ETA
        list, total count, and the requested-time / current-wait samples
        used by the §4 banner header."""
        pending_with_eta = []
        pending_without = []
        time_limits = []
        waits = []
        for job in self.jobs.jobs:
            if 'PENDING' not in (getattr(job, 'job_state', None) or []):
                continue
            diff = eta_seconds(getattr(job, 'start_time', {}))
            if diff is None:
                pending_without.append(job)
            else:
                pending_with_eta.append((diff, job))
            tl = getattr(job, 'time_limit', {})
            if isinstance(tl, dict) and tl.get('set'):
                time_limits.append(tl.get('number', 0) * 60)
            submit_ts = ts(getattr(job, 'submit_time', {}))
            if submit_ts > 0:
                waits.append(max(0, now - submit_ts))
        pending_with_eta.sort(key=lambda x: x[0])
        ordered = [j for _, j in pending_with_eta] + pending_without
        return {
            'ordered': ordered,
            'no_eta_count': len(pending_without),
            'time_limits': time_limits,
            'waits': waits,
            'total': len(ordered),
        }

    def _steady_state_footer(self, now, finishing_visible, started_visible):
        """Footer for the §2↔§3 gap: the steady-state running pool.

        These are RUNNING jobs that don't qualify for either visible
        section — neither close enough to ending to land in §2 nor fresh
        enough to land in §3. By far the largest invisible bucket on a
        busy cluster, so summarizing aggregate footprint (cpus, nodes)
        and median runtime gives the operator a sense of "what's going on
        in the middle" without scrolling through hundreds of rows."""
        excluded = finishing_visible | started_visible | set(self.ended_tracker.keys())
        count = 0
        cpus = 0
        nodes = 0
        runtimes = []
        for j in self.jobs.jobs:
            if j.job_id in excluded:
                continue
            states = getattr(j, 'job_state', None) or []
            # COMPLETING is excluded deliberately: those jobs are
            # transitioning, not steady-state, and they're pinned to the
            # top of §2 so they're already accounted for.
            if 'RUNNING' not in states:
                continue
            count += 1
            cpus += _tres_int(j, 'cpu')
            nodes += _tres_int(j, 'node')
            start_ts = ts(getattr(j, 'start_time', {}))
            if start_ts and now >= start_ts:
                runtimes.append(now - start_ts)
        if count == 0:
            return None
        parts = [f"{count} more running steady-state"]
        if cpus:
            parts.append(f"{_format_count(cpus)} cpus")
        if nodes:
            parts.append(f"{nodes} nodes")
        if runtimes:
            runtimes.sort()
            median = runtimes[len(runtimes) // 2]
            parts.append(f"median ran {coarse_duration(int(median))}")
        return _gap_footer(" · ".join(parts))

    def _pending_depth_footer(self, about_plan, about_cap):
        """Footer for the §3↔§4 gap: pending jobs not visible in §4.

        §4 shows only the soonest-to-start `about_cap` rows; everything
        further down the queue (and every no-ETA job past the visible
        slice) sits "below the horizon" of the section. The total pending
        count is already in the §4 banner — this footer is specifically
        about the *invisible* tail."""
        total = about_plan['total']
        visible = min(total, about_cap) if about_cap else 0
        hidden = total - visible
        if hidden <= 0:
            return None
        # No-ETA jobs sit at the tail of `ordered`, so the visible slice
        # only consumes them once it has exhausted every ETA-known row.
        no_eta = about_plan['no_eta_count']
        with_eta = total - no_eta
        no_eta_visible = max(0, visible - with_eta)
        no_eta_hidden = no_eta - no_eta_visible
        parts = [f"{hidden} more pending in the queue"]
        if no_eta_hidden:
            parts.append(f"{no_eta_hidden} without start estimate")
        return _gap_footer(" · ".join(parts))

    def _render_finishing_section(self, width, candidates, cap, footer=None):
        # No count in the title: this section is always "the next N jobs to
        # finish" where N is whatever fits in the window — the total number
        # of running jobs would just be misleading.
        title = SectionBanner("Finishing next", width=width)
        col_header = u.AttrMap(_header(FINISHING_LAYOUT), 'faded')
        top = [title, col_header]
        visible = candidates[:cap] if cap else []
        if not candidates:
            top.append(u.Text(("faded", "  (no running jobs with a known end time)")))
            rows = []
        else:
            rows = [FinishingJobWidget(job) for _, job in visible]
        self._set_section_contents(self.finishing_section, top, rows, footer=footer)

    def _render_started_section(self, width, candidates, cap, footer=None):
        visible = candidates[-cap:] if cap else []
        # No count in the title: this section is always "the most recently
        # started N jobs" where N is whatever fits in the window.
        title = SectionBanner("Recently started", width=width)
        col_header = u.AttrMap(_header(STARTED_LAYOUT), 'faded')
        top = [title, col_header]
        if not candidates:
            top.append(u.Text(
                ("faded", "  (no jobs have started in the last 15 minutes)")))
            rows = []
        else:
            rows = [StartedJobWidget(job) for _, job in visible]
        self._set_section_contents(self.started_section, top, rows, footer=footer)

    def _render_about_section(self, width, about_plan, cap):
        """Top of the cluster-wide pending list, sorted by ETA (soonest first).

        Just a preview — the full interactive pending list lives in F8.
        """
        total_pending = about_plan['total']
        time_limits = about_plan['time_limits']
        waits = about_plan['waits']
        # Title carries the queue depth — unlike the upper sections, the
        # total count of pending jobs is genuine information (not just the
        # number of rows that fit). Append avg requested runtime and avg
        # current wait so the header doubles as a queue-health summary.
        parts = [f"{total_pending} pending"]
        if time_limits:
            parts.append(f"avg time {coarse_duration(int(sum(time_limits) / len(time_limits)))}")
        if waits:
            parts.append(f"avg waiting {coarse_duration(int(sum(waits) / len(waits)))}")
        title = SectionBanner(f"Starting next  ({', '.join(parts)})",
                              width=width)
        col_header = u.AttrMap(_header(ABOUT_LAYOUT), 'faded')
        top = [title, col_header]

        ordered = about_plan['ordered']
        if not ordered:
            top.append(u.Text(("faded", "  (no pending jobs in the queue)")))
            rows = []
        else:
            rows = [AboutToStartJobWidget(job)
                    for job in (ordered[:cap] if cap else [])]
        self._set_section_contents(self.about_section, top, rows)

    # --- Focus / keypress --------------------------------------------------
    #
    # Arrow navigation is handled explicitly here rather than delegating to
    # urwid's Pile-of-Piles. Each section pile has a non-selectable weight
    # spacer between its header rows and its job rows (used to pin rows to
    # the bottom — the conveyor belt effect). Letting urwid's cursor walker
    # cross that spacer is fragile: it tends to land on the spacer/header,
    # which we can't represent as a "focused job", so the next re-render
    # falls back to "last selectable" (the bottom row). Doing it ourselves:
    # always land on a real row, never confuse the saved-focus state.

    def _section_piles(self):
        """The four section piles in display order, mirroring SECTION_WEIGHTS."""
        return (self.ended_section, self.finishing_section,
                self.started_section, self.about_section)

    @staticmethod
    def _selectable_rows(section_pile):
        """List of (content_index, widget) pairs for selectable rows."""
        out = []
        for i, (w, _opts) in enumerate(section_pile.contents):
            try:
                if w.selectable():
                    out.append((i, w))
            except Exception:
                pass
        return out

    def _save_current(self, section_idx, section_pile):
        """Persist (section, jobid) for the next re-render. Skips when the
        currently focused widget isn't a row — never overwrite a real saved
        jobid with None."""
        self.focused_section = section_idx
        focused = section_pile.focus
        jid = getattr(focused, 'jobid', None) if focused is not None else None
        if jid is not None:
            self.focused_jobid_by_section[section_idx] = jid

    def _restore_focus(self):
        """After a re-render, put the cursor back on the previously focused
        job. If that job is gone, fall back to the top row in the same
        section. If the section itself is now empty and the user has
        already navigated, walk outward to the nearest non-empty section;
        if they haven't navigated yet, leave focus unset so an empty
        Recently-finished section doesn't auto-pull focus elsewhere."""
        sections = self._section_piles()
        n = len(sections)
        start = max(0, min(self.focused_section, n - 1))
        directions = (0, -1, 1) if self.has_navigated else (0,)
        for offset in range(n):
            for direction in directions:
                if direction == 0 and offset > 0:
                    continue
                idx = start + direction * offset
                if not (0 <= idx < n):
                    continue
                rows = self._selectable_rows(sections[idx])
                if not rows:
                    continue
                target_jid = self.focused_jobid_by_section.get(idx)
                pos = next((i for (i, w) in rows
                            if getattr(w, 'jobid', None) == target_jid),
                           rows[0][0])
                sections[idx].focus_position = pos
                try:
                    self.outer_pile.focus_position = idx
                except (IndexError, ValueError):
                    pass
                self.focused_section = idx
                return

    def _focused_jobid(self):
        sections = self._section_piles()
        sec = max(0, min(self.focused_section, len(sections) - 1))
        focused = sections[sec].focus
        return getattr(focused, 'jobid', None) if focused is not None else None

    def _open_job_info(self, jobid):
        from slop.ui.overlays import JobInfoOverlay
        job = self.jobs.job_index.get(jobid)
        if job is None:
            # Ended jobs have already vanished from scontrol; the snapshot
            # row stays selectable so the cursor doesn't trip over a dead
            # zone, but there's no live Job to hand to the overlay.
            return
        self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))

    def _move_focus(self, key):
        """Explicit row-by-row / section-by-section navigation."""
        self.has_navigated = True
        sections = self._section_piles()
        n = len(sections)
        sec = max(0, min(self.focused_section, n - 1))
        rows = self._selectable_rows(sections[sec])
        if not rows:
            # Current section has nothing to focus — fall back to the
            # nearest section that does.
            self._restore_focus()
            return
        try:
            cur_idx = sections[sec].focus_position
        except (IndexError, AttributeError):
            cur_idx = rows[0][0]
        cur_row = next((k for k, (i, _w) in enumerate(rows) if i == cur_idx), 0)

        if key == 'home':
            self._land(sec, sections[sec], rows[0][0])
            return
        if key == 'end':
            self._land(sec, sections[sec], rows[-1][0])
            return

        delta = -1 if key == 'up' else 1
        new_row = cur_row + delta

        if 0 <= new_row < len(rows):
            self._land(sec, sections[sec], rows[new_row][0])
            return

        # Crossed section boundary — find the next non-empty section in
        # that direction and land on its top (going down) or bottom (going
        # up). If there is none, clamp to the current section's edge.
        direction = 1 if delta > 0 else -1
        idx = sec + direction
        while 0 <= idx < n:
            adj_rows = self._selectable_rows(sections[idx])
            if adj_rows:
                target = adj_rows[0][0] if direction > 0 else adj_rows[-1][0]
                self._land(idx, sections[idx], target)
                return
            idx += direction
        # No adjacent section to receive focus — stay at the edge of this
        # section (top or bottom row).
        edge = rows[-1][0] if direction > 0 else rows[0][0]
        self._land(sec, sections[sec], edge)

    def _land(self, section_idx, section_pile, content_idx):
        """Move focus to (section_idx, content_idx) and persist it."""
        try:
            self.outer_pile.focus_position = section_idx
        except (IndexError, ValueError):
            pass
        try:
            section_pile.focus_position = content_idx
        except (IndexError, ValueError):
            pass
        self._save_current(section_idx, section_pile)

    def selectable(self):
        # Force-true regardless of the wrapped widget's opinion. The outer
        # Pile caches its `_selectable` flag from its children at init time,
        # and the four section piles all start out as non-selectable
        # spacer-only piles — so the cache says False forever, and arrow
        # keys never get delivered to this view. We always have at least
        # one row to focus once data arrives, and we handle Enter/Up/Down/
        # PageUp/PageDown/Home/End ourselves in keypress().
        return True

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key
        if key == 'enter':
            jid = self._focused_jobid()
            if jid is not None:
                self._open_job_info(jid)
            return None
        if key in ('up', 'down', 'home', 'end'):
            self._move_focus(key)
            return None
        return super().keypress(size, key)
