"""Jobs collection model."""
import urwid as u
from slop.models.job import Job
from slop.slurm import is_running, is_pending


class Jobs:
    """A collection of Job objects created from fetched scontrol json."""

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
        old_jobs_by_id = {}
        for job in self.jobs:
            if job.is_array_parent:
                previous_array_states[job.job_id] = job.array_collapsed_widget
            old_jobs_by_id[job.job_id] = job

        self.jobs.clear()
        self.jobs = [Job(job) for job in slurm_json['jobs']]
        self.job_index = {job.job_id: job for job in self.jobs}

        # Restore collapsed widget states
        for job in self.jobs:
            if job.is_array_parent and job.job_id in previous_array_states:
                job.array_collapsed_widget = previous_array_states[job.job_id]

        self.link_array_jobs()

        # Carry over cached widgets from the previous Job instances when the
        # rendered content is unchanged. Must happen after link_array_jobs so
        # array parents see their children when computing the signature.
        for job in self.jobs:
            old_job = old_jobs_by_id.get(job.job_id)
            if old_job is not None:
                job.transfer_widget_cache_from(old_job)

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

    def reset_array_collapse(self):
        """Collapse all array parent widgets and invalidate their cached widgets."""
        for job in self.jobs:
            if job.is_array_parent and not job.array_collapsed_widget:
                job.array_collapsed_widget = True
                if hasattr(job, '_widget'):
                    del job._widget

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
