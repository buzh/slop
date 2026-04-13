import urwid as u
import datetime
import time
from slop.utils import *
from slop.slurm import *
from slop import __version__
from slop.ui.style import get_display_attr

class IndentHeader(u.WidgetWrap):
    def __init__(self, header):
        indented_widget = u.Columns([
            ('fixed', 2, u.Text('  ')),
            header
        ])
        super().__init__(indented_widget)

    def selectable(self):
        return False


class ChildJobWidget(u.WidgetWrap):
    def __init__(self, job):
        self.job = job
        self.jobid = job.job_id
        self.display_attr = job.widget.display_attr
        indented_widget = u.Columns([
            ('fixed', 2, u.Text('  ')),
            job.widget
        ])
        super().__init__(indented_widget)

    def selectable(self):
        return True

class ArrayPendWidget(u.WidgetWrap):
    def __init__(self, pending):
        widget = u.Columns([u.Text(f"  [+ {pending} more pending]")], dividechars=1)
        widget = u.AttrMap(widget, 'faded', 'normal_selected')
        super().__init__(widget)

    def selectable(self):
        return False

class UserJobListHeader(u.WidgetWrap): # Dynamic header line item for job walker
    def __init__(self, parent, joblistwidget):
        display_attr = joblistwidget.display_attr
        job_states = joblistwidget.job.states
        is_array = bool(joblistwidget.job.is_array)
        is_array_parent = joblistwidget.job.is_array_parent
        is_running = bool(job_states & job_state_running)
        is_ended = bool(job_states & job_state_ended)
        is_pending = bool(job_states & job_state_pending)

        dynamic_labels = {
            'start_time': lambda: "Running" if is_running else "Started" if is_ended else "Array" if is_array else "Starting",
            'job_state': lambda: "Status" if is_ended else "S",
            'wall_time': lambda: "Duration" if not is_pending else "Time",
            'nodes': lambda: "Nodes" if not is_array_parent else "",
        }

        static_labels = {
            'end_time': "Deadline",
            'submit_time': "Submitted",
            'job_id': "Job ID",
            'account': "Acct",
            'exit_code': "Exit code",
            'array_tasks': "Array",
            'user_name': "User",
        }

        header = []
        for headeritem in display_attr:
            if headeritem in dynamic_labels:
                label = dynamic_labels[headeritem]()
            elif headeritem in static_labels:
                label = static_labels[headeritem]
            else:
                label = headeritem.capitalize()

            if parent.sort_col == headeritem:
                arrow = ">" if parent.sort_reverse else "<"
                label = f"{label}{arrow}"

            h = u.Text(label)
            header.append((display_attr[headeritem][0], display_attr[headeritem][1], h))
            
        widget = u.AttrMap(u.Columns(header, dividechars=1), 'jobheader')
        super().__init__(widget)


class UserJobListWidget(u.WidgetWrap):
    def __init__(self, job, width=None, view_type=None):
        self.job = job
        self.start_time = job.start_time.get("number", False)
        self.end_time = job.end_time["number"]
        self.time_limit = job.time_limit["number"]
        self.jobid = job.job_id
        self.width = width
        self.view_type = view_type
        self.display_attr = get_display_attr(job, width, view_type)
        self.is_array = job.is_array
        self.widget = self.create_widget()
        super().__init__(self.widget)

    def create_widget(self):
        job = self.job

        if job.is_array_child:
            if is_running(job):
                wrapped_text = self.get_label(job)
            else:
                tid = job._task_id
                state = ",".join(job.states)
                wrapped_text = [ u.Text(f"Array task: {tid} {state} {job.state_reason} ") ]
        else:
            wrapped_text = self.get_label(job)

        col = u.Columns(wrapped_text, dividechars=1)
        w = u.AttrMap(col, 'normal', 'normal_selected')

        return w

                

    def get_label(self, job):
        w = []
        for col, (align, width, wrap_mode) in self.display_attr.items():
            value = getattr(job, col, None)
            now = datetime.datetime.now()


            if col == "job_state":
                if { "RUNNING", "PENDING" } & job.states:
                    t = ",".join(slurm.status_shortnames.get(s, f"?{s}") for s in job.job_state)
                else:
                    t = job.job_state
            elif col == "job_id":
                if job.is_array_parent:
                    marker = "▼" if not job.array_collapsed_widget else "▶"
                    t = f"{marker} {job.job_id}_[*]"
                else:
                    t = str(job.job_id)
            elif col == "array_tasks":
                    
                    t = compress_int_range(job.array_task_ids) 
            elif col == "start_time": # epoch -> 1d2h3m
                if self.start_time:
                    ts = int(time.time()) - self.start_time
                    st = (ts // 60) * 60
                    t = format_duration(st)
                else:
                    t = "N/A"
            elif col == "end_time": # epoch -> 1d2h3m
                if self.end_time:
                    ts = self.end_time - int(time.time())
                    timestamp = (ts // 60) * 60
                    t = format_duration(timestamp)
                else:
                    t = "N/A"
            elif col == "submit_time": # epoch -> HH:MM (if today) or day/month HH:MM
                timestamp = datetime.datetime.fromtimestamp(int(value["number"]))
                if timestamp.date() == now.date():
                    t = timestamp.strftime('%H:%M')
                else:
                    t = timestamp.strftime('%d/%m %H:%M')
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
            else:
                t = str(value)

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


class JobListDivider(u.WidgetWrap):
    """Divider with optional centered text label."""
    def __init__(self, text=None):
        if text:
            w = u.Columns([u.Divider("-"), ('pack', u.AttrMap(u.Text(text, align='center'), 'jobheader')), u.Divider("-")], dividechars=2)
        else:
            w = u.Divider("-")
        super().__init__(w)


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
        header = u.AttrWrap(u.Columns([self.text_left, self.text_right]), 'header')
        u.WidgetWrap.__init__(self, header)

    def update(self, view_name=None):
        """Update header with current view name."""
        if view_name:
            self.text_left.set_text(f"Slurm Top {__version__} - {view_name}")
        else:
            self.text_left.set_text(f"Slurm Top {__version__}")
        self.text_right.set_text("q: Quit")

class Footer(u.WidgetWrap):
    def __init__(self, main_screen=None):
        self.main_screen = main_screen
        self.text_left = u.Text("", wrap='clip')
        self.text_center = u.Text("", align='center', wrap='clip')
        self.text_right = u.Text("", align='right', wrap='clip')
        # Three-column layout for better space utilization
        footer = u.AttrWrap(u.Columns([self.text_left, self.text_center, self.text_right]), 'footer')
        u.WidgetWrap.__init__(self, footer)

    def update(self, view_type=None, f1_label=None):
        """Update footer with context-appropriate shortcuts.

        Args:
            view_type: Current view type ('myjobs', 'users', 'cluster', etc.)
            f1_label: Label for F1 key (what will happen on next F1 press)
        """
        # Get screen width for responsive content
        screen_width = getattr(self.main_screen, 'width', 120)

        # Build shortcuts based on screen width and view type
        if screen_width < 90:
            # Narrow: Very compact
            shortcuts = self._build_narrow_shortcuts(view_type)
        elif screen_width < 140:
            # Medium: Balanced
            shortcuts = self._build_medium_shortcuts(view_type)
        else:
            # Wide: Full descriptions
            shortcuts = self._build_wide_shortcuts(view_type)

        self.text_left.set_text(shortcuts['left'])
        self.text_center.set_text(shortcuts.get('center', ''))
        self.text_right.set_text(shortcuts.get('right', ''))

    def _build_narrow_shortcuts(self, view_type):
        """Minimal shortcuts for narrow screens (<90 cols)."""
        left_parts = ["F1-5:Views", "/:Search"]

        if view_type == 'history':
            left_parts.append("Esc:Back")
        elif view_type in ['users', 'accounts', 'partitions', 'states']:
            left_parts.append("h:Hist")
            left_parts.append("e:Grp")
        elif view_type == 'myjobs':
            left_parts.append("e:Expand")

        left_parts.append("?:Help")

        right = ""
        if view_type == 'history':
            right = "0-7:Sort"
        elif view_type != 'cluster':
            right = "0-6:Sort"

        return {'left': ' '.join(left_parts), 'right': right}

    def _build_medium_shortcuts(self, view_type):
        """Moderate detail for medium screens (90-140 cols)."""
        left_parts = ["F1-F5: Views", "/: Search"]

        center = ""
        if view_type == 'history':
            center = "Enter: Details | Esc: Back"
        elif view_type in ['users', 'accounts', 'partitions', 'states']:
            center = "h: History | e: Groups | Enter: Details"
        elif view_type == 'myjobs':
            center = "e: Expand/Collapse | Enter: Details"
        elif view_type == 'cluster':
            center = "Arrows: Scroll"

        left_parts.append("?: Info")

        right = ""
        if view_type == 'history':
            right = "0-7: Sort"
        elif view_type != 'cluster':
            right = "0-6: Sort"

        return {'left': ' | '.join(left_parts), 'center': center, 'right': right}

    def _build_wide_shortcuts(self, view_type):
        """Full descriptions for wide screens (>140 cols)."""
        # Left: Navigation
        left_parts = [
            "F1: My Jobs/Users",
            "F2: Accounts",
            "F3: Partitions",
            "F4: States",
            "F5: Cluster"
        ]

        # Center: View-specific actions
        center_parts = []
        if view_type == 'history':
            center_parts = ["/: Search", "Enter: Job Details", "Esc: Back to Jobs", "?: App Info"]
        elif view_type in ['users', 'accounts']:
            center_parts = ["/: Search", "h: User History", "e: Expand Groups", "Enter: Job Details", "?: Info"]
        elif view_type in ['partitions', 'states']:
            center_parts = ["/: Search", "e: Expand Groups", "Enter: Job Details", "?: Info"]
        elif view_type == 'myjobs':
            center_parts = ["/: Search", "e: Expand/Collapse Sections", "Enter: Job Details", "?: Info"]
        elif view_type == 'cluster':
            center_parts = ["/: Search", "Arrows: Scroll", "?: App Info"]

        # Right: Sorting/Exit
        right_parts = []
        if view_type == 'history':
            right_parts.append("0-7: Sort Columns")
        elif view_type != 'cluster':
            right_parts.append("0-6: Sort Columns")
        right_parts.append("q: Quit")

        return {
            'left': ' | '.join(left_parts),
            'center': ' | '.join(center_parts),
            'right': ' | '.join(right_parts)
        }


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
        linebox = u.LineBox(
            u.Filler(t),
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        widget = u.AttrMap(linebox, 'bg')
        u.WidgetWrap.__init__(self, widget)


class ProgressOverlay(u.WidgetWrap):
    """Overlay with updatable progress text."""
    def __init__(self, main_screen, initial_text):
        self.overlay_height = 10
        self.text_widget = u.Text(initial_text, align='center')
        linebox = u.LineBox(
            u.Filler(self.text_widget),
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        widget = u.AttrMap(linebox, 'bg')
        u.WidgetWrap.__init__(self, widget)

    def update_text(self, text):
        """Update the displayed text."""
        self.text_widget.set_text(text)
