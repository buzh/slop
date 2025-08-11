import urwid as u
from slop.models import Jobs
from slop.slurm import *
from slop.utils import *
from slop.ui.widgets import *

class JobInfoOverlay(u.WidgetWrap):
    def __init__(self, job):
        self.job = job
        self.text = self.fetch_info()
        self.height = len(self.text) if isinstance(self.text, list) else self.text.count('\n') + 1
        body = u.LineBox(u.Filler(u.Text(self.text)))
        body = u.AttrMap(body, 'jobid', 'jobid')
        super().__init__(body)

    def fetch_info(self):
        job = self.job
        def format_time(ts):
            try:
                if isinstance(ts, dict) and "number" in ts:
                    return datetime.datetime.fromtimestamp(int(ts["number"])).strftime("%Y-%m-%d %H:%M:%S")
                return "N/A"
            except Exception:
                return "N/A"
        if job.time_limit["set"]:
            time_limit = format_duration(job.time_limit["number"] * 60)
        if job.cpus["set"]:
            cpus = job.cpus["number"]
        req_tres = nice_tres(job)
        exit_code = f"{job.returncode}"

        lines = [
            f"Job ID      : {job.job_id}\n",
            f"Name        : {job.name}\n",
            f"User        : {job.user_name}\n",
            f"Account     : {job.account}\n",
            f"Partition   : {job.partition}\n",
            f"State       : {' '.join(job.job_state)}\n",
            f"Reason      : {getattr(job, 'reason', 'N/A')}\n",
            f"TRES        : {req_tres}\n",
            "",
            f"Submit Time : {format_time(job.submit_time)}\n",
            f"Start Time  : {format_time(job.start_time)}\n",
            f"End Time    : {format_time(job.end_time)}\n",
            f"Time Limit  : {time_limit}\n",
            "",
            f"Nodes       : {job.nodes}\n",
            f"CPUs        : {cpus}\n",
            f"Tasks       : {getattr(job, 'ntasks', 'N/A')}\n",
            f"Memory      : {getattr(job, 'min_memory_per_node', 'N/A')}\n",
            "",
            f"Work Dir    : {getattr(job, 'work_dir', 'N/A')}\n",
            f"Node List   : {getattr(job, 'node_list', 'N/A')}\n",
            f"Exit Code   : {exit_code}\n",
        ]
        return lines


class ScreenViewUsers(u.WidgetWrap): # List users on the left, user jobs on the right
    def __init__(self, main_screen, jobs):
        self.jobs = jobs
        self.selected_user = None
        self.selected_job = None
        self.sort_col = 'end_time'
        self.sort_reverse = False

        self.main_screen = main_screen
        self.userwalker = u.SimpleFocusListWalker([])
        self.jobwalker = u.SimpleFocusListWalker([])
        self.joblistbox = u.ListBox(self.jobwalker)

        uw = u.AttrMap(u.ScrollBar(u.ListBox(self.userwalker)), 'bg')
        self.jw = u.LineBox((u.ScrollBar(self.joblistbox)), title=f"User jobs")

        l = u.LineBox(u.Filler(uw, valign='top', height=self.main_screen.height), title="Users")
        self.joblist = u.Filler(self.jw, valign='top', height=self.main_screen.height)
        self.w = u.Columns([('weight', 25, l), ('weight', 75, self.joblist)])

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, self.w)

    def on_jobs_update(self, *_args, **_kwargs): # Only refresh if window is active
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def keypress(self, size, key):
        sort_keys = {
            '0': 'job_state',
            '1': 'job_id',
            '2': 'start_time',
            '3': 'end_time',
            '4': 'submit_time',
            '5': 'account',
            '6': 'partition',
        }

        if self.main_screen.overlay_showing:
            return key

        if (key == ' ' or key == 'enter') and self.w.get_focus_column() == 1:
            focus_w, _ = self.jobwalker.get_focus()
            if hasattr(focus_w, "jobid"):
                job = self.jobs.job_index.get(focus_w.jobid)
                if job and job.is_array_parent:
                    job.toggle_expand()
                    self.draw_jobs()
                elif job:
                    self.main_screen.open_overlay(JobInfoOverlay(job))
            return None

        if key in sort_keys and self.w.get_focus_column() == 1:
            selected_col = sort_keys[key]

            if self.sort_col == selected_col:
                self.sort_reverse = not self.sort_reverse
            else:
                self.sort_col = selected_col
                self.sort_reverse = True

            self.draw_jobs()
            return None

        if key == 'x':
            print("woop!")
            focus_w, _ = self.jobwalker.get_focus()
            if hasattr(focus_w, "jobid"):
                job = self.jobs.job_index.get(focus_w.jobid)
                job.widget.refresh()
                self.draw_jobs()
            return None

        return super().keypress(size, key)

    def modified(self):
        focus_w, _ = self.userwalker.get_focus()
        if hasattr(focus_w, "user"):
            self.selected_user = focus_w.user
            self.draw_jobs()

        focus_w, _ = self.jobwalker.get_focus()
        if hasattr(focus_w, "jobid"):
            self.selected_job = focus_w.jobid
            self.draw_jobs()

    def sort_jobs(self, jobtable):
        def get_sort_key(job):
            if self.sort_col in ['start_time', 'end_time', 'submit_time']:
                return job.__dict__[self.sort_col]['number']
            elif self.sort_col == 'job_state':
                return ','.join(job.__dict__[self.sort_col])
            else:
                return job.__dict__[self.sort_col]
        return sorted(jobtable, key=get_sort_key, reverse=self.sort_reverse)

    def update(self):
        self.draw_users()
        self.draw_jobs()

    def categorize_jobs(self, jobtable): # Organize jobs according to state
        job_sets = {
                "Array": [],
                "Running": [],
                "Ended": [],
                "Pending": [],
                "Other": []
        }


        for job in jobtable:
            # skip array children, rendered under parent
            if job.is_array_child:
                continue
            job_sets[job.get_state_category()].append(job)
        return job_sets
    """ Add labeled divider if requested, then widgets per job in joblist """
    def build_job_widgets(self, joblist, label=None):
        if not joblist:
            return []
        
        widgets = []

        if label:
            widgets.append(JobListDivider(f"{label}"))

        """ assume all jobs in joblist have the same fields, user first to create header """
        widgets.append(UserJobListHeader(self, joblist[0].widget))

        for job in joblist:
            if job.is_array_parent:
                widgets.append(job.widget)

                if not job.array_collapsed_widget:
                    children = sorted(job.array_children, key=lambda j: j.job_id)
                    numchildren = 0
                    runchildw = []
                    for child in children:
                        if is_running(child):
                            runchildw.append(ChildJobWidget(child))
                        else:
                            numchildren += 1
                    if runchildw:
                        hw = UserJobListHeader(self, runchildw[0])
                        widgets.append(IndentHeader(hw))
                        widgets.extend(runchildw)
                    widgets.append(ArrayPendWidget(numchildren))

            elif job.is_array_child:
                if job.array_parent:
                    continue
                continue

            else:
                widgets.append(job.widget)

        return widgets

    def restore_job_focus(self): # keep focus on selected job
        focused = False
        for index, item in enumerate(self.jobwalker):
            if hasattr(item, "jobid") and item.jobid == self.selected_job:
                self.jobwalker.set_focus(index)
                focused = True
                break
        if not focused: # if no selected item, or selected item has gone away
            for index, item in enumerate(self.jobwalker):
                if hasattr(item, "jobid"):
                    self.joblistbox.set_focus(0)
                    self.jobwalker.set_focus(index)
                    self.selected_job = item.jobid
                    break


    def draw_jobs(self): # Render the job list for selected user
        if not (hasattr(self.jobs, "usertable")): # usertable might not exist yet
            return
        if not self.selected_user in self.jobs.usertable: # selected user might no longer be in slurm job data
            return

        jobtable = self.jobs.usertable[self.selected_user]['jobs']
        jobtable = self.sort_jobs(jobtable)

        # keep track of selected job widget
        focus_w, _ = self.jobwalker.get_focus()
        if hasattr(focus_w, "jobid"):
            self.selected_job = focus_w.jobid
        else:
            self.selected_job = None

        u.disconnect_signal(self.jobwalker, 'modified', self.modified)
        self.jobwalker.clear()

        jobwalker_widgets = []

        # split jobs according to state to show only relevant attributes in list
        job_sets = self.categorize_jobs(jobtable)
        for job_category in ["Array", "Ended", "Running", "Pending", "Other"]:
            jobwalker_widgets.extend(self.build_job_widgets(job_sets[job_category], label=job_category))

        self.jobwalker.extend(jobwalker_widgets)
        self.jw.set_title(f"Jobs for {self.selected_user}")
        self.restore_job_focus()
        u.connect_signal(self.jobwalker, 'modified', self.modified)

    def draw_users(self):
        if hasattr(self.jobs, "usertable"): #usertable might not exist yet
            usertable = self.jobs.usertable
        else:
            return

        usertable = dict(sorted(usertable.items(), key=lambda item: item[1]['njobs'], reverse=True))

        u.disconnect_signal(self.userwalker, 'modified', self.modified)
        self.userwalker.clear()

        w = []
        for user in usertable: # move this logic out?
            n = usertable[user]["njobs"]
            r = usertable[user]["running"]
            p = usertable[user]["pending"]
            w.append(UserItem(user, n, r, p))

        self.userwalker.extend(w)

        if self.selected_user and self.selected_user in usertable:
            for i in self.userwalker:
                if hasattr(i, "user") and i.user == self.selected_user:
                    pos = self.userwalker.index(i)
                    self.userwalker.set_focus(pos) # put focus on selected user on update
        else:
            self.userwalker.set_focus(0) # default to first user in list if not selected, or no longer active
            focus_w, _ = self.userwalker.get_focus()
            if hasattr(focus_w, "user"):
                self.selected_user = focus_w.user

        u.connect_signal(self.userwalker, 'modified', self.modified)


class ConfirmExit(u.WidgetWrap):
    def __init__(self, main_screen):
        self.main_screen = main_screen
        y = u.AttrMap(u.Button("Yes", self.exit_program, align='center', wrap='clip'), 'buttons', 'buttons_selected')
        n = u.AttrMap(u.Button("No", self.cancel_exit, align='center', wrap='clip'), 'buttons', 'buttons_selected')
        b = [y, n]
        buttons = u.Columns(b)

        widget = u.AttrMap(u.LineBox(u.Filler(u.Pile([buttons])), title='Confirm exit?'), 'bg')
        u.WidgetWrap.__init__(self, widget)

    def keypress(self, size, key):
        return super().keypress(size, key)

    def exit_program(self, a=None) -> None:
        raise u.ExitMainLoop()

    def cancel_exit(self, a=None):
        self.main_screen.close_overlay()
