""" data models for slop """
import urwid as u
from slop.ui.widgets import UserJobListWidget
from slop.slurm import (
    is_running,
    is_ended,
    is_pending,
    job_state_running,
    job_state_ended,
    job_state_pending,
)

""" a singular job """
class Job:
    def __init__(self, job_data):
        # populate attributes dynamically from json
        for key, value in job_data.items():
            setattr(self, key, value)

        # Normalize sacct data to scontrol format if needed
        self._normalize_sacct_data()

        # store states as set to avoid enumerating repeatedly later
        if hasattr(self, 'job_state'):
            self.states = set(self.job_state)
        else:
            self.states = set()

        self._task_id = self._extract_number("array_task_id") if hasattr(self, 'array_task_id') else None

        # Handle exit code (scontrol vs sacct format)
        if hasattr(self, 'exit_code') and isinstance(self.exit_code, dict):
            e = ",".join(self.exit_code.get("status", []))
            c = self.exit_code.get("return_code", {}).get("number", 0)
            self.returncode = f"{e}({c})" if e else str(c)
        else:
            self.returncode = "N/A"

        # Handle array job detection
        if hasattr(self, 'array_job_id') and isinstance(self.array_job_id, dict):
            array_num = self.array_job_id.get("number", 0)
            if array_num == 0:
                self.is_array = False
                self.is_array_parent = False
                self.is_array_child = False
            else:
                self.is_array = True
                self.array_parent_id = array_num
                if self.array_parent_id == self.job_id:
                    self.is_array_parent = True
                    self.is_array_child = False
                else:
                    self.is_array_child = True
                    self.is_array_parent = False
        else:
            # No array info available (sacct historical data)
            self.is_array = False
            self.is_array_parent = False
            self.is_array_child = False

        self.array_children = []
        self.array_parent = None
        self.array_collapsed_widget = True

    def _normalize_sacct_data(self):
        """Normalize sacct JSON format to match scontrol format."""
        # sacct uses 'state' with 'current' array, scontrol uses 'job_state'
        if hasattr(self, 'state') and not hasattr(self, 'job_state'):
            if isinstance(self.state, dict) and 'current' in self.state:
                self.job_state = self.state['current']
                # sacct has 'reason' in state dict, scontrol uses 'state_reason'
                if 'reason' in self.state and not hasattr(self, 'state_reason'):
                    self.state_reason = self.state['reason']
            else:
                self.job_state = []

        # Provide default state_reason if missing
        if not hasattr(self, 'state_reason'):
            self.state_reason = "None"

        # sacct uses 'user', scontrol uses 'user_name'
        if hasattr(self, 'user') and not hasattr(self, 'user_name'):
            self.user_name = self.user

        # sacct uses 'derived_exit_code', scontrol uses 'exit_code'
        if hasattr(self, 'derived_exit_code') and not hasattr(self, 'exit_code'):
            self.exit_code = self.derived_exit_code

        # sacct uses 'time' dict, scontrol uses separate time fields
        if hasattr(self, 'time') and isinstance(self.time, dict):
            if 'start' in self.time and not hasattr(self, 'start_time'):
                self.start_time = {"number": self.time['start']} if self.time['start'] else {"number": 0}
            if 'end' in self.time and not hasattr(self, 'end_time'):
                self.end_time = {"number": self.time['end']} if self.time['end'] else {"number": 0}
            if 'submission' in self.time and not hasattr(self, 'submit_time'):
                self.submit_time = {"number": self.time['submission']} if self.time['submission'] else {"number": 0}
            if 'limit' in self.time and not hasattr(self, 'time_limit'):
                # time.limit is already in dict format in sacct
                self.time_limit = self.time['limit']

        # sacct uses 'array' dict, scontrol uses separate array fields
        if hasattr(self, 'array') and isinstance(self.array, dict):
            if 'job_id' in self.array and not hasattr(self, 'array_job_id'):
                self.array_job_id = {"number": self.array['job_id']}
            if 'task_id' in self.array and not hasattr(self, 'array_task_id'):
                # sacct's task_id is already a dict with 'number' key
                if isinstance(self.array['task_id'], dict):
                    self.array_task_id = self.array['task_id']
                else:
                    self.array_task_id = {"number": self.array['task_id']}

        # sacct uses 'required' dict, scontrol uses top-level fields
        if hasattr(self, 'required') and isinstance(self.required, dict):
            if 'CPUs' in self.required and not hasattr(self, 'cpus'):
                self.cpus = {"set": True, "number": self.required['CPUs']}
            # memory_per_cpu and memory_per_node are already in correct format in sacct
            if 'memory_per_cpu' in self.required and not hasattr(self, 'memory_per_cpu'):
                self.memory_per_cpu = self.required['memory_per_cpu']
            if 'memory_per_node' in self.required and not hasattr(self, 'memory_per_node'):
                self.memory_per_node = self.required['memory_per_node']

        # Provide empty tres strings if not present (sacct has complex tres structure)
        if not hasattr(self, 'tres_alloc_str'):
            self.tres_alloc_str = ""
        if not hasattr(self, 'tres_req_str'):
            self.tres_req_str = ""

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
        # Categorize by actual state, not by array status
        # Array parents with running children should be in "Running"
        if self.is_array_parent and self.has_running_children:
            return "Running"
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
        # Case-insensitive, whitespace-stripped comparison for robustness
        username_normalized = username.strip().lower()
        user_jobs = [
            job for job in self.jobs
            if hasattr(job, 'user_name') and job.user_name.strip().lower() == username_normalized
        ]

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
