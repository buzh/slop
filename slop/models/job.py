"""Job model representing a single Slurm job."""
import json
import time
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


class Job:
    """A singular job."""

    def __init__(self, job_data):
        # Stable hash of the raw JSON so refreshes can detect "no visible
        # change" and transfer the cached widget from the previous instance.
        try:
            self._raw_data_hash = hash(json.dumps(job_data, sort_keys=True, default=str))
        except (TypeError, ValueError):
            self._raw_data_hash = None

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
                # array_task_id.set=False marks the abstract parent that stands
                # in for unscheduled tasks; True marks a concrete scheduled task.
                # When job_id == array_job_id but task_id is set, it's a single
                # concrete task that happens to reuse the array id — render it
                # as a regular job rather than an empty expandable parent.
                task_id_set = isinstance(getattr(self, 'array_task_id', None), dict) \
                    and self.array_task_id.get('set', False)
                self.is_array_parent = (self.array_parent_id == self.job_id) and not task_id_set
                self.is_array_child = self.array_parent_id != self.job_id
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

    @property
    def widget(self):
        """Widgets are properties so they are created only when called."""
        if not hasattr(self, '_widget'):
            self._widget = UserJobListWidget(self,
                                            width=getattr(self, '_widget_width', None),
                                            view_type=getattr(self, '_widget_view_type', None),
                                            force_array_tasks_col=getattr(self, '_widget_force_array_tasks_col', False))
        return self._widget

    def set_widget_width(self, width, view_type=None, force_array_tasks_col=False):
        """Set width and view type for widget creation; invalidate cache only on change."""
        if (getattr(self, '_widget_width', None) == width
                and getattr(self, '_widget_view_type', None) == view_type
                and getattr(self, '_widget_force_array_tasks_col', False) == force_array_tasks_col):
            return
        self._widget_width = width
        self._widget_view_type = view_type
        self._widget_force_array_tasks_col = force_array_tasks_col
        if hasattr(self, '_widget'):
            delattr(self, '_widget')

    def widget_content_signature(self):
        """Identity for the rendered widget. Two jobs with equal signatures
        produce identical widget output and can share a cached widget."""
        # start_time/end_time columns render relative to now, rounded to the
        # minute, so the minute bucket gates cache reuse for time displays.
        minute_bucket = int(time.time()) // 60
        children_sig = (
            tuple(sorted(c._raw_data_hash for c in self.array_children if c._raw_data_hash is not None))
            if self.is_array_parent else ()
        )
        return (
            self._raw_data_hash,
            self.array_collapsed_widget,
            children_sig,
            minute_bucket,
        )

    def transfer_widget_cache_from(self, old_job):
        """Adopt old_job's cached widget if its signature still matches ours."""
        if not hasattr(old_job, '_widget'):
            return False
        if old_job.widget_content_signature() != self.widget_content_signature():
            return False
        self._widget = old_job._widget
        self._widget.job = self
        self._widget_width = getattr(old_job, '_widget_width', None)
        self._widget_view_type = getattr(old_job, '_widget_view_type', None)
        self._widget_force_array_tasks_col = getattr(old_job, '_widget_force_array_tasks_col', False)
        return True

    @property
    def has_running_children(self):
        return self.is_array_parent and any(is_running(child) for child in self.array_children)

    @property
    def earliest_child_start_time(self):
        """Get the earliest start time among running children."""
        if not self.is_array_parent:
            return None
        running_children = [child for child in self.array_children if is_running(child)]
        if not running_children:
            return None
        # Get start times, filtering out None/0 values
        start_times = []
        for child in running_children:
            start = child._extract_number('start_time')
            if start:
                start_times.append(start)
        return min(start_times) if start_times else None

    @property
    def earliest_child_end_time(self):
        """Get the earliest end time (deadline) among running children."""
        if not self.is_array_parent:
            return None
        running_children = [child for child in self.array_children if is_running(child)]
        if not running_children:
            return None
        # Get end times, filtering out None/0 values
        end_times = []
        for child in running_children:
            end = child._extract_number('end_time')
            if end:
                end_times.append(end)
        return min(end_times) if end_times else None

    @property
    def array_task_ids(self):
        return [child._task_id for child in self.array_children]

    def _extract_number(self, attr_name):
        attr = getattr(self, attr_name, {})
        if attr.get("set", False):
            return attr.get("number")
        return None

    def get_state_category(self):
        """Categorize by actual state, not by array status."""
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

    def toggle_expand(self):
        """Helper function for right/down arrow in array job widgets."""
        self.array_collapsed_widget = not self.array_collapsed_widget
        del self._widget

    def __repr__(self):
        """Makes each top level slurm job attribute an attribute of this class object."""
        attrs = ', '.join(f"{key}={value}" for key, value in self.__dict__.items())
        return f"Job({attrs})"
