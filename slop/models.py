""" data models for slop """
import urwid as u
from slop.ui.widgets import UserJobListWidget
from slop.slurm import *
from slop.anonymize import (
    anonymize_user, anonymize_account, anonymize_node,
    anonymize_job_name, anonymize_path
)

""" a singular job """
class Job:
    def __init__(self, job_data):
        # populate attributes dynamically from json
        for key, value in job_data.items():
            setattr(self, key, value)

        # Anonymize sensitive fields if demo mode is enabled
        if hasattr(self, 'user_name') and self.user_name:
            self.user_name = anonymize_user(self.user_name)
        if hasattr(self, 'account') and self.account:
            self.account = anonymize_account(self.account)
        if hasattr(self, 'name') and self.name:
            self.name = anonymize_job_name(self.name)
        if hasattr(self, 'nodes') and self.nodes and isinstance(self.nodes, str):
            # Anonymize comma-separated node list
            nodes = [anonymize_node(n.strip()) for n in self.nodes.split(',')]
            self.nodes = ','.join(nodes)
        if hasattr(self, 'working_directory') and self.working_directory:
            self.working_directory = anonymize_path(self.working_directory)
        if hasattr(self, 'standard_output') and self.standard_output:
            self.standard_output = anonymize_path(self.standard_output)
        if hasattr(self, 'standard_error') and self.standard_error:
            self.standard_error = anonymize_path(self.standard_error)

        # store states as set to avoid enumerating repeatedly later
        self.states = set(self.job_state) 
        self._task_id = self._extract_number("array_task_id")
        e = ",".join(self.exit_code["status"])
        c = self.exit_code["return_code"]["number"]
        self.returncode = f"{e}({c})"

        if self.array_job_id["number"] == 0:
            self.is_array = False
            self.is_array_parent = False
            self.is_array_child = False
        else:
            self.is_array = True
            self.array_parent_id = self.array_job_id["number"]
            if self.array_parent_id == self.job_id:
                self.is_array_parent = True
                self.is_array_child = False
            else:
                self.is_array_child = True
                self.is_array_parent = False

        self.array_children = []
        self.array_parent = None
        self.array_collapsed_widget = True

    """ widgets are properties so they are created only when called """
    @property
    def widget(self):
        if not hasattr(self, '_widget'):
            self._widget = UserJobListWidget(self,
                                            width=getattr(self, '_widget_width', None),
                                            view_type=getattr(self, '_widget_view_type', None))
        return self._widget

    def set_widget_width(self, width, view_type=None):
        """Set width and view type for widget creation and clear cache to force recreation."""
        self._widget_width = width
        self._widget_view_type = view_type
        if hasattr(self, '_widget'):
            delattr(self, '_widget')

    @property
    def has_running_children(self):
        return self.is_array_parent and any(is_running(child) for child in self.array_children)

    @property
    def array_task_ids(self):
        return [child._task_id for child in self.array_children]

    def _extract_number(self, attr_name):
        attr = getattr(self, attr_name, {})
        if attr.get("set", False):
            return attr.get("number")
        return None

    def get_state_category(self):
        if self.is_array:
            return "Array"
        elif self.states & job_state_running:
            return "Running"
        elif self.states & job_state_ended:
            return "Ended"
        elif self.states & job_state_pending:
            return "Pending"
        else:
            return "Other"
    """ makes each top level slurm job attribute an attribute of this class object """
    def __repr__(self):
        attrs = ', '.join(f"{key}={value}" for key, value in self.__dict__.items())
        return f"Job({attrs})"

    """ helper function for right/down arrow in array job widgets """
    def toggle_expand(self):
        self.array_collapsed_widget = not self.array_collapsed_widget
        del self._widget

""" a collection of Job objects created from fetched scontrol json """
class Jobs:
    def __init__(self, slurm_json):
        self.jobs = []
        self.job_index = {}
        self.usertable = None
        self.accounttable = None
        self.partitiontable = None
        self.statetable = None
        self.update_slurmdata(slurm_json)
        u.register_signal(self.__class__, ['jobs_updated'])

    def __iter__(self):
        return iter(self.jobs)

    def update_slurmdata(self, slurm_json):
        previous_array_states = {}
        for job in self.jobs:
            if job.is_array_parent:
                previous_array_states[job.job_id] = job.array_collapsed_widget

        self.jobs.clear()
        self.jobs = [Job(job) for job in slurm_json['jobs']]
        self.job_index = {job.job_id: job for job in self.jobs}

        # Restore collapsed widget states
        for job in self.jobs:
            if job.is_array_parent and job.job_id in previous_array_states:
                job.array_collapsed_widget = previous_array_states[job.job_id]

        self.link_array_jobs()
        self.make_user_table()
        self.make_account_table()
        self.make_partition_table()
        self.make_state_table()
        u.emit_signal(self, 'jobs_updated')

    def link_array_jobs(self):
        for job in self.jobs:
            if job.is_array_child:
                parent = self.job_index.get(job.array_parent_id)
                if parent:
                    job.array_parent = parent
                    if job not in parent.array_children:
                        parent.array_children.append(job)

    def make_user_table(self):
        usertable = {}

        for job in self.jobs:
            user = job.user_name

            if user not in usertable:
                usertable[user] = {'njobs': 0, 'running': 0, 'pending': 0, 'jobs': []}

            usertable[user]['jobs'].append(job)

            usertable[user]['njobs'] += 1
            if is_running(job):
                usertable[user]['running'] += 1
            if is_pending(job):
                usertable[user]['pending'] += 1

        self.usertable = usertable

    def make_account_table(self):
        accounttable = {}

        for job in self.jobs:
            account = job.account

            if account not in accounttable:
                accounttable[account] = {'njobs': 0, 'running': 0, 'pending': 0, 'jobs': []}

            accounttable[account]['jobs'].append(job)

            accounttable[account]['njobs'] += 1
            if is_running(job):
                accounttable[account]['running'] += 1
            if is_pending(job):
                accounttable[account]['pending'] += 1

        self.accounttable = accounttable

    def make_partition_table(self):
        partitiontable = {}

        for job in self.jobs:
            partition = job.partition

            if partition not in partitiontable:
                partitiontable[partition] = {'njobs': 0, 'running': 0, 'pending': 0, 'jobs': []}

            partitiontable[partition]['jobs'].append(job)

            partitiontable[partition]['njobs'] += 1
            if is_running(job):
                partitiontable[partition]['running'] += 1
            if is_pending(job):
                partitiontable[partition]['pending'] += 1

        self.partitiontable = partitiontable

    def make_state_table(self):
        statetable = {}

        for job in self.jobs:
            # job_state is a list like ['RUNNING'] or ['PENDING']
            # Use the first state as the primary state for grouping
            state = job.job_state[0] if job.job_state else 'UNKNOWN'

            if state not in statetable:
                statetable[state] = {'njobs': 0, 'running': 0, 'pending': 0, 'jobs': []}

            statetable[state]['jobs'].append(job)

            statetable[state]['njobs'] += 1
            if is_running(job):
                statetable[state]['running'] += 1
            if is_pending(job):
                statetable[state]['pending'] += 1

        self.statetable = statetable

    def get_user_jobs(self, username):
        """Get all jobs for a specific user, grouped by state."""
        user_jobs = [job for job in self.jobs if job.user_name == username]

        if not user_jobs:
            return None

        # Group by state
        grouped = {
            'RUNNING': [],
            'PENDING': [],
            'COMPLETED': [],
            'FAILED': [],
            'OTHER': []
        }

        for job in user_jobs:
            state = job.job_state[0] if job.job_state else 'UNKNOWN'
            if state in ['RUNNING', 'COMPLETING']:
                grouped['RUNNING'].append(job)
            elif state in ['PENDING']:
                grouped['PENDING'].append(job)
            elif state in ['COMPLETED']:
                grouped['COMPLETED'].append(job)
            elif state in ['FAILED', 'TIMEOUT', 'CANCELLED', 'NODE_FAIL', 'OUT_OF_MEMORY']:
                grouped['FAILED'].append(job)
            else:
                grouped['OTHER'].append(job)

        return grouped
