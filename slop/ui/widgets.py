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
        widget = u.Columns([u.Text(f"  [+ {pending} tasks pending]")], dividechars=1)
        widget = u.AttrMap(widget, 'faded', 'jobid_selected')
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
    def __init__(self, job):
        self.job = job
        self.start_time = job.start_time.get("number", False)
        self.end_time = job.end_time["number"]
        self.time_limit = job.time_limit["number"]
        self.jobid = job.job_id
        self.display_attr = get_display_attr(job)
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
        w = u.AttrMap(col, 'jobid', 'jobid_selected')

        return w

                

    def get_label(self, job): ### needs to be refactored
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

            if wrap_mode:
                wrapped_text.set_wrap_mode(wrap_mode)
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
        item = u.AttrMap(label, "jobid", "jobid_selected")
        u.WidgetWrap.__init__(self, item)

    def selectable(self):
            return True


class JobListDivider(u.WidgetWrap):
    def __init__(self, text=None):
        if text:
            w = u.Columns([u.Divider("-"), ('pack', u.AttrMap(u.Text(text, align='center'), 'jobheader')), u.Divider("-")], dividechars=2)
        else:
            w = u.Divider("-")
        super().__init__(w)


class Header(u.WidgetWrap):
    def __init__(self):
        header = u.AttrWrap(u.Columns([u.Text(f"Slurm Top {__version__}"), (u.Text("Q to quit", align='right'))]), 'header')
        u.WidgetWrap.__init__(self, header)

class Footer(u.WidgetWrap):
    def __init__(self):
        header = u.AttrWrap(u.Columns([u.Text("F1: Help"), (u.Text("F2: Switch view"), align='right'))]), 'header')
        u.WidgetWrap.__init__(self, header)


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
            tlcorner=u.LineBox.Symbols.LIGHT.TOP_LEFT_ROUNDED,
            trcorner=u.LineBox.Symbols.LIGHT.TOP_RIGHT_ROUNDED,
            blcorner=u.LineBox.Symbols.LIGHT.BOTTOM_LEFT_ROUNDED,
            brcorner=u.LineBox.Symbols.LIGHT.BOTTOM_RIGHT_ROUNDED
        )
        widget = u.AttrMap(linebox, 'bg')
        u.WidgetWrap.__init__(self, widget)
