import urwid as u
import datetime
import time
from slop.utils import format_duration, nice_tres, compact_tres, compress_int_range
from slop.slurm import is_running, is_pending, is_ended, job_state_short
from slop import __version__
from slop.ui.style import get_display_attr
from slop.ui.state_style import state_icon, state_attr
from slop.ui.constants import EMPTY_PLACEHOLDER


def rounded_box(content, title=''):
    """LineBox with the project's rounded corners."""
    return u.LineBox(content, title=title,
                     tlcorner='╭', trcorner='╮',
                     blcorner='╰', brcorner='╯')


class SafeListBox(u.ListBox):
    """ListBox that drops a stale deferred-focus position before urwid
    can dereference it.

    `urwid.ListBox.set_focus()` stashes the previous focus position in
    `set_focus_pending` so the next render can animate scroll direction.
    When auto-refresh rebuilds a walker smaller than that stashed position
    (e.g. user has scrolled to row 124 of an entity list and the next
    refresh shrinks it under the cursor), `_set_focus_complete` raises
    `IndexError: focus index is out of range`. We drop the pending state
    when the stored row no longer exists — only the scroll-direction
    hint is lost, never the focus itself.
    """

    def _set_focus_complete(self, size, focus):
        pending = self.set_focus_pending
        if isinstance(pending, tuple) and len(pending) == 3:
            _, _, focus_pos = pending
            if isinstance(focus_pos, int) and (
                not len(self._body) or focus_pos >= len(self._body)
            ):
                self.set_focus_pending = None
        return super()._set_focus_complete(size, focus)


class ChildJobWidget(u.WidgetWrap):
    def __init__(self, job):
        self.job = job
        self.jobid = job.job_id

        # Build compact, informative display for array child
        task_id = job._task_id if hasattr(job, '_task_id') and job._task_id is not None else EMPTY_PLACEHOLDER

        primary_state = job.job_state[0] if job.job_state else ''
        symbol = state_icon(primary_state)

        # Build display based on state
        if is_running(job):
            # Running: Task [5]: ↻ 3h12m on c1-12 (100 cores, 100G mem)
            start_time = getattr(job, 'start_time', {})
            if isinstance(start_time, dict) and start_time.get('number'):
                elapsed = int(time.time()) - start_time['number']
                runtime = format_duration((elapsed // 60) * 60)
            else:
                runtime = EMPTY_PLACEHOLDER

            node = getattr(job, 'nodes', None)
            tres = nice_tres(job) if hasattr(job, 'tres_alloc_str') else ''

            text = f"  Task [{task_id}]: {symbol} {runtime}"
            if node:
                text += f" on {node}"
            if tres:
                text += f" ({tres})"

        elif "PENDING" in job.states:
            # Pending: Task [8]: ⋯ Priority (10h requested)
            reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER)
            time_limit = getattr(job, 'time_limit', {})
            if isinstance(time_limit, dict) and time_limit.get('number'):
                requested = format_duration(time_limit['number'] * 60)
            else:
                requested = EMPTY_PLACEHOLDER

            text = f"  Task [{task_id}]: {symbol} {reason} ({requested} requested)"

        else:
            # Ended: Task [5]: ✓ 3h42m (exit: 0) or ✗ 0h5m (exit: 1, OutOfMemory)
            wall_time_val = getattr(job, 'end_time', {}).get('number', 0) - getattr(job, 'start_time', {}).get('number', 0)
            wall_time = format_duration(wall_time_val) if wall_time_val > 0 else EMPTY_PLACEHOLDER
            exit_code = getattr(job, 'returncode', EMPTY_PLACEHOLDER)

            text = f"  Task [{task_id}]: {symbol} {wall_time} (exit: {exit_code}"

            # Add reason for failed/timed out/OOM jobs
            if {"FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT"} & job.states:
                reason = getattr(job, 'state_reason', '')
                if reason and reason != 'None':
                    text += f", {reason}"

            text += ")"

        text_widget = u.Text(text)
        widget = u.AttrMap(text_widget, 'normal', 'normal_selected')
        super().__init__(widget)

    def selectable(self):
        return True

class ArrayPendWidget(u.WidgetWrap):
    def __init__(self, pending):
        text = u.Text(f"  [+ {pending} more pending]")
        super().__init__(u.AttrMap(text, 'faded', 'normal_selected'))

    def selectable(self):
        return False

class UserJobListWidget(u.WidgetWrap):
    def __init__(self, job, width=None, view_type=None, force_array_tasks_col=False):
        self.job = job
        # Defensive field access - sacct jobs may not have all fields
        # For array parents with running children, use earliest child times
        if job.has_running_children and job.earliest_child_start_time:
            self.start_time = job.earliest_child_start_time
        else:
            self.start_time = getattr(job, 'start_time', {}).get("number", False) if hasattr(job, 'start_time') else False

        if job.has_running_children and job.earliest_child_end_time:
            self.end_time = job.earliest_child_end_time
        else:
            self.end_time = getattr(job, 'end_time', {}).get("number", 0) if hasattr(job, 'end_time') else 0

        self.time_limit = getattr(job, 'time_limit', {}).get("number", 0) if hasattr(job, 'time_limit') else 0
        self.jobid = job.job_id
        self.width = width
        self.view_type = view_type
        self.display_attr = get_display_attr(job, width, view_type, force_array_tasks_col)
        self.is_array = job.is_array
        self.widget = self.create_widget()
        super().__init__(self.widget)

    def create_widget(self):
        job = self.job

        # All jobs (including array children) use get_label() for consistent column display
        wrapped_text = self.get_label(job)

        col = u.Columns(wrapped_text, dividechars=1)
        w = u.AttrMap(col, 'normal', 'normal_selected')

        return w

                

    def get_label(self, job):
        w = []
        for col, (align, width, wrap_mode) in self.display_attr.items():
            value = getattr(job, col, None)
            now = datetime.datetime.now()
            text_attr = None  # Track if we need to apply a color attribute

            if col == "job_state":
                # For array parents with running children, show as running regardless of parent state
                if job.is_array_parent and job.has_running_children:
                    primary = "RUNNING"
                    short_states = "R"
                else:
                    primary = job.job_state[0] if job.job_state else ''
                    short_states = ",".join(job_state_short.get(s, f"?{s}") for s in job.job_state)
                symbol = state_icon(primary)
                text_attr = state_attr(primary)
                t = f"{symbol} {short_states}"
            elif col == "task_id":
                # For array children, show task ID
                if job.is_array_child and job._task_id is not None:
                    t = f"[{job._task_id}]"
                else:
                    t = EMPTY_PLACEHOLDER
            elif col == "job_id":
                if job.is_array_parent and job.array_children:
                    marker = "▼" if not job.array_collapsed_widget else "▶"

                    running = sum(1 for c in job.array_children if is_running(c))
                    pending = sum(1 for c in job.array_children if is_pending(c))
                    ended = sum(1 for c in job.array_children if is_ended(c))

                    parts = []
                    if running > 0:
                        parts.append(f"R:{running}")
                    if pending > 0:
                        parts.append(f"P:{pending}")
                    if ended > 0:
                        parts.append(f"E:{ended}")
                    status = f" ({' '.join(parts)})" if parts else ""

                    t = f"{marker} {job.job_id}{status}"
                else:
                    t = str(job.job_id)
            elif col == "array_tasks":
                if job.is_array_parent and job.array_children:
                    t = compress_int_range(job.array_task_ids) or EMPTY_PLACEHOLDER
                elif getattr(job, 'array_task_string', ''):
                    t = job.array_task_string
                else:
                    t = EMPTY_PLACEHOLDER
            elif col == "start_time": # epoch -> 1d2h3m
                if self.start_time:
                    ts = int(time.time()) - self.start_time
                    st = (ts // 60) * 60
                    t = format_duration(st)
                else:
                    t = EMPTY_PLACEHOLDER
            elif col == "end_time": # epoch -> 1d2h3m
                if self.end_time:
                    ts = self.end_time - int(time.time())
                    timestamp = (ts // 60) * 60
                    t = format_duration(timestamp)
                else:
                    t = EMPTY_PLACEHOLDER
            elif col == "submit_time": # epoch -> HH:MM (if today) or day/month HH:MM
                if value and isinstance(value, dict) and "number" in value:
                    timestamp = datetime.datetime.fromtimestamp(int(value["number"]))
                    if timestamp.date() == now.date():
                        t = timestamp.strftime('%H:%M')
                    else:
                        t = timestamp.strftime('%d/%m %H:%M')
                else:
                    t = EMPTY_PLACEHOLDER
            elif col == "wall_time": # if pending, show time requested. otherwise, show duration so far
                if 'PENDING' in job.states:
                    t = format_duration(self.time_limit * 60)
                else:
                    st = self.start_time
                    en = self.end_time
                    wt = en - st
                    t = format_duration(wt)
            elif col == "reason":
                t = job.state_reason
            elif col == "exit_code":
                t = job.returncode
            elif col == "tres":
                t = nice_tres(job)
            elif col == "resources":
                t = compact_tres(job) or EMPTY_PLACEHOLDER
            else:
                t = str(value)

            # Create Text widget with optional color attribute
            if text_attr:
                wrapped_text = u.Text((text_attr, t))
            else:
                wrapped_text = u.Text(t)

            # Always set a wrap mode to prevent overflow (default to 'clip' if None)
            wrapped_text.set_wrap_mode(wrap_mode if wrap_mode else 'clip')
            w.append((align, width, wrapped_text))

        return w


    def refresh(self):
        self.widget = self.create_widget()

    def selectable(self):
        return True


class UserItem(u.WidgetWrap):
    def __init__(self, user=None, njobs=None, running=None, pending=None):
        self.user = user
        i = str(njobs)
        i = f"[{i}]"
        status = f"(R:{running} P:{pending})" 
        label = u.Columns([('given', 6, u.Text(('buttons', i))), u.Text(user, align='left'), u.Text(status, align='right')])
        item = u.AttrMap(label, "normal", "normal_selected")
        u.WidgetWrap.__init__(self, item)

    def selectable(self):
            return True


class SectionHeader(u.WidgetWrap):
    """'═══ LABEL ═══...' header that auto-fills remaining width via urwid.Divider."""
    def __init__(self, label):
        cols = u.Columns([('pack', u.Text(f'═══ {label} ')), u.Divider('═')])
        super().__init__(u.AttrMap(cols, 'jobheader'))


class SectionBanner(u.WidgetWrap):
    """Non-selectable full-width banner row inside a scrolling panel.

    Variant of SectionHeader for the in-panel use case (queue/scheduler
    sub-sections); pads the label to `width` so the 'jobheader' attr fills
    the row even when the text is short.
    """
    def __init__(self, label, width=None):
        text = f"  {label}"
        if width:
            text = text.ljust(max(width, len(text)))
        super().__init__(u.AttrMap(u.Text(text), 'jobheader'))

    def selectable(self):
        return False


class ExpandableGroupMarker(u.WidgetWrap):
    """Selectable text marker for expandable job groups."""
    def __init__(self, text, group_key):
        self.group_key = group_key
        super().__init__(u.AttrMap(u.Text(text), 'faded', 'normal_selected'))

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key


class Header(u.WidgetWrap):
    def __init__(self, main_screen=None):
        self.main_screen = main_screen
        self.text_left = u.Text(f"Slurm Top {__version__}", wrap='clip')
        self.text_right = u.Text("", align='right', wrap='clip')
        header = u.AttrMap(u.Columns([self.text_left, self.text_right]), 'header')
        u.WidgetWrap.__init__(self, header)

    def update(self, view_name=None):
        """Update header with current view name."""
        if view_name:
            self.text_left.set_text(f"Slurm Top {__version__} - {view_name}")
        else:
            self.text_left.set_text(f"Slurm Top {__version__}")
        self.text_right.set_text("")  # Empty - shortcuts now in footer

class Footer(u.WidgetWrap):
    def __init__(self, main_screen=None):
        self.main_screen = main_screen
        self.text = u.Text("", wrap='clip')
        footer = u.AttrMap(self.text, 'footer')
        u.WidgetWrap.__init__(self, footer)

    def update(self, view_type=None):
        """Update footer with context-appropriate shortcuts.

        Args:
            view_type: Current view type ('dashboard', 'users', 'cluster', etc.)
        """
        screen_width = getattr(self.main_screen, 'width', 120)

        if screen_width < 90:
            text = "F1/2/3 5-8:Views ?:Help q:Quit"
        elif screen_width < 140:
            text = "F1: Dashboard | F2: Jobs | F3: My Jobs | F5-8: Cluster/History/Queue/Sched | ?: Help | q: Quit"
        else:
            text = ("F1: Dashboard | F2: Jobs (user/acct/partition/state) | F3: My Jobs | "
                    "F5: Cluster | F6: History | F7: Queue | F8: Scheduler | ?: Help | q: Quit")

        # Append a sub-line hint for the F2 cycling tabs.
        if view_type in ('users', 'accounts', 'partitions', 'states') and screen_width >= 100:
            text += "    [Tab/⇧Tab or 1-4: cycle tabs]"

        self.text.set_text(text)


class GenericOverlayText(u.WidgetWrap):
    def __init__(self, main_screen, text):
        if isinstance(text, str):
            num_lines = text.splitlines()
            num_lines = len(num_lines)
        elif isinstance(text, list):
            num_lines = len(text)
        else:
            num_lines = 1
        self.overlay_height = num_lines + 4 # pad with 4 extra lines

        t = u.Text(text, align='center')
        widget = u.AttrMap(rounded_box(u.Filler(t)), 'bg')
        u.WidgetWrap.__init__(self, widget)


class HelpOverlay(u.WidgetWrap):
    """Overlay for displaying scrollable text content (help, diagnostics, ...)."""
    def __init__(self, main_screen, text_lines, title='Help'):
        """
        Args:
            main_screen: Main screen instance
            text_lines: List of strings or (attr, string) tuples
            title: Title shown in the overlay's rounded border
        """
        self.overlay_height = len(text_lines) + 4  # pad with 4 extra lines

        # Convert each line to a Text widget
        widgets = []
        for line in text_lines:
            if isinstance(line, tuple):
                # (attr, text) tuple
                widgets.append(u.Text((line[0], line[1])))
            else:
                # Plain string
                widgets.append(u.Text(line))

        listbox = SafeListBox(u.SimpleFocusListWalker(widgets))
        widget = u.AttrMap(rounded_box(listbox, title=title), 'bg')
        u.WidgetWrap.__init__(self, widget)


class ProgressOverlay(u.WidgetWrap):
    """Overlay with updatable progress text."""
    def __init__(self, main_screen, initial_text):
        self.overlay_height = 10
        self.text_widget = u.Text(initial_text, align='center')
        widget = u.AttrMap(rounded_box(u.Filler(self.text_widget)), 'bg')
        u.WidgetWrap.__init__(self, widget)

    def update_text(self, text):
        """Update the displayed text."""
        self.text_widget.set_text(text)


class AccountUsageWidget(u.WidgetWrap):
    """One row of an account-usage table: name + CPU hours, color-coded by usage."""

    def __init__(self, account_data):
        self.account_data = account_data
        account = account_data.get('account', EMPTY_PLACEHOLDER)
        used_hours = account_data.get('used', 0)

        hours_str = f"{used_hours:,}" if used_hours >= 1000 else str(used_hours)
        text = f"  {account:20s} │ {hours_str:>12s}"

        if used_hours > 1000:
            attr = 'success'
        elif used_hours > 100:
            attr = 'bg'
        else:
            attr = 'faded'

        super().__init__(u.Text((attr, text)))
