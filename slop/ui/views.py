import urwid as u
import datetime
from slop.models import Jobs
from slop.models_cluster import ClusterResources
from slop.slurm import *
from slop.utils import *
from slop.ui.widgets import *

class JobInfoOverlay(u.WidgetWrap):
    def __init__(self, job, main_screen=None):
        self.job = job
        self.main_screen = main_screen

        # Calculate dimensions based on screen size
        if main_screen and hasattr(main_screen, 'height'):
            self.height = max(main_screen.height - 8, 15)
        else:
            self.height = 30  # Reasonable default

        # Calculate usable width for text content
        if main_screen and hasattr(main_screen, 'width'):
            self.content_width = max(main_screen.width - 20, 50)
        else:
            self.content_width = 60

        widgets = self.build_widgets()

        # Create scrollable listbox
        walker = u.SimpleFocusListWalker(widgets)
        listbox = u.ListBox(walker)

        # Wrap in LineBox with title showing job ID and state
        state = ' '.join(job.job_state)
        title = f"Job {job.job_id} - {state}"
        body = u.LineBox(
            listbox,
            title=title,
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        body = u.AttrMap(body, 'normal', 'normal')
        super().__init__(body)

    def build_widgets(self):
        """Build the overlay widgets with sections and computed fields."""
        job = self.job
        widgets = []

        # Determine job state category
        state = ' '.join(job.job_state)
        running = is_running(job)
        pending = is_pending(job)
        ended = is_ended(job)
        failed = state in ["FAILED", "TIMEOUT", "OUT_OF_MEMORY", "CANCELLED"]

        # === BASIC INFO ===
        widgets.append(u.AttrMap(u.Text("BASIC INFO"), 'jobheader'))
        widgets.append(u.Divider("─"))
        widgets.append(u.Text(f"Job ID      : {job.job_id}"))
        widgets.append(u.Text(f"Name        : {job.name}"))
        widgets.append(u.Text(f"User        : {job.user_name}"))
        widgets.append(u.Text(f"Account     : {job.account}"))
        widgets.append(u.Text(f"Partition   : {job.partition}"))

        # Array job info
        if hasattr(job, 'array_job_id') and job.array_job_id.get('set'):
            array_id = job.array_job_id['number']
            if hasattr(job, 'array_task_id') and job.array_task_id.get('set'):
                task_id = job.array_task_id['number']
                widgets.append(u.Text(f"Array Job   : {array_id} (Task {task_id})"))
            else:
                max_tasks = job.array_max_tasks['number'] if hasattr(job, 'array_max_tasks') else 'N/A'
                widgets.append(u.Text(f"Array Job   : {array_id} (Parent, {max_tasks} tasks)"))

        widgets.append(u.Divider())

        # === STATUS ===
        widgets.append(u.AttrMap(u.Text("STATUS"), 'jobheader'))
        widgets.append(u.Divider("─"))

        # Color-code state
        if failed:
            widgets.append(u.AttrMap(u.Text(f"State       : {state}"), 'state_failed'))
        elif running:
            widgets.append(u.AttrMap(u.Text(f"State       : {state}"), 'state_running'))
        elif pending:
            widgets.append(u.AttrMap(u.Text(f"State       : {state}"), 'state_pending'))
        else:
            widgets.append(u.Text(f"State       : {state}"))

        # State reason (important for pending jobs)
        state_reason = getattr(job, 'state_reason', 'None')
        if state_reason and state_reason != 'None':
            widgets.append(u.Text(f"Reason      : {state_reason}"))
            # Show explanation from reasons dict if available
            if state_reason in reasons:
                explanation = reasons[state_reason]
                # Wrap long explanations based on available width
                max_explanation_width = self.content_width - 14  # Account for indentation
                if len(explanation) > max_explanation_width:
                    explanation = explanation[:max_explanation_width-3] + "..."
                widgets.append(u.Text(f"              {explanation}"))

        # Exit code for ended jobs
        if ended:
            exit_code = self.format_exit_code(job)
            if failed:
                widgets.append(u.AttrMap(u.Text(f"Exit Code   : {exit_code}"), 'state_failed'))
            else:
                widgets.append(u.Text(f"Exit Code   : {exit_code}"))

        widgets.append(u.Divider())

        # === TIMELINE ===
        widgets.append(u.AttrMap(u.Text("TIMELINE"), 'jobheader'))
        widgets.append(u.Divider("─"))

        submit_time = self.format_time(job.submit_time)
        start_time = self.format_time(job.start_time)
        end_time = self.format_time(job.end_time)

        widgets.append(u.Text(f"Submit Time : {submit_time}"))

        if pending:
            # Show queue time for pending jobs
            queue_time = self.calculate_queue_time(job)
            widgets.append(u.Text(f"Queue Time  : {queue_time}"))
        else:
            widgets.append(u.Text(f"Start Time  : {start_time}"))

        if running:
            # Show runtime and time remaining
            runtime = self.calculate_runtime(job)
            time_remaining = self.calculate_time_remaining(job)
            widgets.append(u.Text(f"Runtime     : {runtime}"))
            if time_remaining:
                widgets.append(u.Text(f"Remaining   : {time_remaining}"))
        elif ended:
            # Show end time and total runtime
            widgets.append(u.Text(f"End Time    : {end_time}"))
            runtime = self.calculate_total_runtime(job)
            widgets.append(u.Text(f"Total Time  : {runtime}"))

        # Time limit
        time_limit = "N/A"
        if hasattr(job, 'time_limit') and job.time_limit.get("set"):
            time_limit = format_duration(job.time_limit["number"] * 60)
        widgets.append(u.Text(f"Time Limit  : {time_limit}"))

        widgets.append(u.Divider())

        # === RESOURCES ===
        widgets.append(u.AttrMap(u.Text("RESOURCES"), 'jobheader'))
        widgets.append(u.Divider("─"))

        # Parse TRES for better display
        tres_info = self.parse_tres(job)
        if job.nodes:
            widgets.append(u.Text(f"Nodes       : {job.nodes}"))
        widgets.append(u.Text(f"CPUs        : {tres_info['cpus']}"))
        widgets.append(u.Text(f"Memory      : {tres_info['memory']}"))
        if tres_info['gpus']:
            widgets.append(u.Text(f"GPUs        : {tres_info['gpus']}"))

        widgets.append(u.Divider())

        return widgets

    def format_time(self, ts):
        """Format timestamp to readable string."""
        try:
            if isinstance(ts, dict) and ts.get("set") and "number" in ts:
                return datetime.datetime.fromtimestamp(int(ts["number"])).strftime("%Y-%m-%d %H:%M:%S")
            return "Not set"
        except Exception:
            return "N/A"

    def format_exit_code(self, job):
        """Format exit code with status."""
        try:
            if hasattr(job, 'derived_exit_code') and job.derived_exit_code.get('set'):
                code = job.derived_exit_code['number']
                return f"{code}"
            elif hasattr(job, 'exit_code') and isinstance(job.exit_code, dict):
                if job.exit_code.get('return_code', {}).get('set'):
                    code = job.exit_code['return_code']['number']
                    status = job.exit_code.get('status', [])
                    if status:
                        return f"{code} ({', '.join(status)})"
                    return f"{code}"
            return "N/A"
        except Exception:
            return "N/A"

    def calculate_queue_time(self, job):
        """Calculate how long job has been waiting in queue."""
        try:
            submit_ts = job.submit_time.get('number')
            if not submit_ts:
                return "N/A"
            now = datetime.datetime.now().timestamp()
            elapsed = int(now - submit_ts)
            return format_duration(elapsed)
        except Exception:
            return "N/A"

    def calculate_runtime(self, job):
        """Calculate how long job has been running."""
        try:
            start_ts = job.start_time.get('number')
            if not start_ts:
                return "N/A"
            now = datetime.datetime.now().timestamp()
            elapsed = int(now - start_ts)
            return format_duration(elapsed)
        except Exception:
            return "N/A"

    def calculate_time_remaining(self, job):
        """Calculate time remaining until time limit."""
        try:
            start_ts = job.start_time.get('number')
            time_limit_min = job.time_limit.get('number')
            if not start_ts or not time_limit_min:
                return None
            now = datetime.datetime.now().timestamp()
            elapsed = int(now - start_ts)
            limit_sec = time_limit_min * 60
            remaining = limit_sec - elapsed
            if remaining < 0:
                return "EXCEEDED"
            return format_duration(remaining)
        except Exception:
            return None

    def calculate_total_runtime(self, job):
        """Calculate total runtime for ended job."""
        try:
            start_ts = job.start_time.get('number')
            end_ts = job.end_time.get('number')
            if not start_ts or not end_ts:
                return "N/A"
            elapsed = int(end_ts - start_ts)
            return format_duration(elapsed)
        except Exception:
            return "N/A"

    def parse_tres(self, job):
        """Parse TRES string into readable components."""
        info = {'cpus': 'N/A', 'memory': 'N/A', 'gpus': None}

        # CPUs
        if hasattr(job, 'cpus') and job.cpus.get('set'):
            info['cpus'] = str(job.cpus['number'])

        # Memory
        if hasattr(job, 'memory_per_cpu') and job.memory_per_cpu.get('set'):
            mem_mb = job.memory_per_cpu['number']
            cpus = job.cpus.get('number', 1) if hasattr(job, 'cpus') else 1
            total_mb = mem_mb * cpus
            if total_mb >= 1024:
                info['memory'] = f"{total_mb / 1024:.1f}GB ({mem_mb}MB/core)"
            else:
                info['memory'] = f"{total_mb}MB ({mem_mb}MB/core)"

        # GPUs from TRES string
        tres_str = getattr(job, 'tres_req_str', '') or getattr(job, 'tres_alloc_str', '')
        if tres_str and 'gpu' in tres_str.lower():
            # Extract GPU info from TRES
            gpu_info = nice_tres(job)
            if 'GPU' in gpu_info:
                info['gpus'] = gpu_info

        return info


class TwoColumnJobView(u.WidgetWrap):
    """Base class for two-column views: entity list on left, jobs on right.

    Subclasses must define:
        - entity_attr: str - attribute name on job (e.g., 'user_name', 'account')
        - left_title: str - title for left panel
        - right_title_template: str - template for right panel title (use {entity})
        - get_entity_table() - returns dict of {entity_name: {'njobs': int, 'running': int, 'pending': int, 'jobs': [Job]}}
        - create_entity_widget(entity_name, njobs, running, pending) - creates widget for left panel item
    """

    # Sort keys map to column positions (set dynamically based on visible columns)
    # This will be populated when the header is built
    SORT_KEYS = {}

    def __init__(self, main_screen, jobs):
        self.jobs = jobs
        self.selected_entity = None
        self.selected_job = None
        self.sort_col = 'job_id'  # Default sort by job ID
        self.sort_reverse = True  # Newest first (high job IDs first)
        self.main_screen = main_screen
        # Default view_type, can be overridden by subclasses
        if not hasattr(self, 'view_type'):
            self.view_type = 'users'

        # Collapse state for job groups
        self.collapsed_groups = {}  # {group_key: bool}
        self.calculate_jobs_per_group()

        # Prevent recursion in draw_jobs
        self._drawing = False

        # Create walkers
        self.entity_walker = u.SimpleFocusListWalker([])
        self.jobwalker = u.SimpleFocusListWalker([])
        self.joblistbox = u.ListBox(self.jobwalker)

        # Build UI
        entity_list = u.AttrMap(u.ScrollBar(u.ListBox(self.entity_walker)), 'bg')

        # Right panel: just the scrollable job list (category headers are inline)
        self.jw = u.LineBox(
            u.ScrollBar(self.joblistbox),
            title=self.right_title_template.format(entity=""),
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        left_panel = u.LineBox(
            entity_list,
            title=self.left_title,
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        right_panel = self.jw
        self.w = u.Columns([('weight', 25, left_panel), ('weight', 75, right_panel)])

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, self.w)

    def on_jobs_update(self, *_args, **_kwargs):
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        # 'e' key: toggle collapse/expand of similar job groups
        if key == 'e' and self.w.get_focus_column() == 1:
            focus_w, _ = self.jobwalker.get_focus()
            # Check if focused on an ellipsis widget
            if hasattr(focus_w, "group_key"):
                # Toggle this group
                group_key = focus_w.group_key
                self.collapsed_groups[group_key] = not self.collapsed_groups.get(group_key, True)
                self.draw_jobs()
            elif hasattr(focus_w, "jobid"):
                # Toggle the group containing this job
                job = self.jobs.job_index.get(focus_w.jobid)
                if job:
                    group_key = self.get_job_group_key(job)
                    self.collapsed_groups[group_key] = not self.collapsed_groups.get(group_key, True)
                    self.draw_jobs()
            return None

        # Space/Enter on job list: expand array parent or show job details
        if (key == ' ' or key == 'enter') and self.w.get_focus_column() == 1:
            focus_w, _ = self.jobwalker.get_focus()
            if hasattr(focus_w, "jobid"):
                job = self.jobs.job_index.get(focus_w.jobid)
                if job and job.is_array_parent:
                    # Array parent: expand to show children
                    job.toggle_expand()
                    self.draw_jobs()
                elif job:
                    # Regular job or array child: show details
                    self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
            return None

        # Number keys: sort by column
        if key in self.SORT_KEYS and self.w.get_focus_column() == 1:
            selected_col = self.SORT_KEYS[key]
            if self.sort_col == selected_col:
                self.sort_reverse = not self.sort_reverse
            else:
                self.sort_col = selected_col
                self.sort_reverse = True
            self.draw_jobs()
            return None

        # 'h' key: show history for selected user/account
        if key == 'h' and self.w.get_focus_column() == 0 and self.selected_entity:
            # Create a progress overlay
            from slop.ui.widgets import ProgressOverlay
            progress_overlay = ProgressOverlay(self.main_screen, f"Fetching history for {self.selected_entity}...")
            self.main_screen.open_overlay(progress_overlay)
            self.main_screen.loop.draw_screen()

            # Progress callback to update the overlay
            def update_progress(status):
                stage = status.get('stage', 'fetch')
                window = status.get('window', '')
                jobs = status.get('jobs_count', 0)
                cached = status.get('cached', 0)
                fresh = status.get('fresh', 0)
                fetch_time = status.get('fetch_duration', 0)
                new_jobs = status.get('new_in_window', 0)

                if stage == 'cache':
                    text = f"Fetching history for {self.selected_entity}...\n\nChecking cache for {window}...\n{jobs} jobs loaded ({cached} cached, {fresh} fresh)"
                elif stage == 'fetch':
                    text = f"Fetching history for {self.selected_entity}...\n\nFetching {window} from sacct...\n{jobs} jobs loaded"
                elif stage == 'complete':
                    if window == 'recent':
                        text = f"Fetching history for {self.selected_entity}...\n\nRecent jobs: {jobs} jobs ({fetch_time:.2f}s)"
                    else:
                        text = f"Fetching history for {self.selected_entity}...\n\n{window}: +{new_jobs} jobs ({fetch_time:.3f}s)\nTotal: {jobs} jobs ({cached} cached, {fresh} fresh)"

                progress_overlay.update_text(text)
                self.main_screen.loop.draw_screen()

            # Fetch history with adaptive fetching
            result = None
            if self.view_type == 'users':
                result = self.main_screen.sacct_fetcher.fetch_adaptive_sync('user', self.selected_entity, progress_callback=update_progress)
            elif self.view_type == 'accounts':
                result = self.main_screen.sacct_fetcher.fetch_adaptive_sync('account', self.selected_entity, progress_callback=update_progress)

            # Close the fetching overlay
            self.main_screen.close_overlay()

            # Show results or error
            if result and result.get('jobs'):
                self.main_screen.handle_search_result(result, self.view_type.rstrip('s'), self.selected_entity)
            else:
                # Show error message
                error_overlay = GenericOverlayText(self.main_screen, f"No history found for {self.selected_entity}\n\nPress Esc to close")
                self.main_screen.open_overlay(error_overlay)
            return None

        # Debug key
        if key == 'x':
            focus_w, _ = self.jobwalker.get_focus()
            if hasattr(focus_w, "jobid"):
                job = self.jobs.job_index.get(focus_w.jobid)
                job.widget.refresh()
                self.draw_jobs()
            return None

        return super().keypress(size, key)

    def modified(self):
        # Entity selection changed
        focus_w, _ = self.entity_walker.get_focus()
        entity_name = self._get_entity_from_widget(focus_w)
        if entity_name and entity_name != self.selected_entity:
            # Only redraw if entity actually changed
            self.selected_entity = entity_name
            self.draw_jobs()

        # Job selection changed - just track it, don't redraw
        focus_w, _ = self.jobwalker.get_focus()
        if hasattr(focus_w, "jobid"):
            self.selected_job = focus_w.jobid

    def _get_entity_from_widget(self, widget):
        """Extract entity name from widget. Override if needed."""
        if hasattr(widget, self.entity_attr):
            return getattr(widget, self.entity_attr)
        # Try common attribute names
        for attr in ['user', 'account', 'partition']:
            if hasattr(widget, attr):
                return getattr(widget, attr)
        return None

    def sort_jobs(self, jobtable):
        def get_sort_key(job):
            # Handle missing attributes gracefully
            if self.sort_col not in job.__dict__:
                return 0  # Put jobs without this field at the start/end

            value = job.__dict__[self.sort_col]

            # Handle time fields (dicts with 'number' key)
            if self.sort_col in ['start_time', 'end_time', 'submit_time']:
                if isinstance(value, dict) and 'number' in value:
                    return value['number']
                return 0
            # Handle job_state (list of strings)
            elif self.sort_col == 'job_state':
                if isinstance(value, list):
                    return ','.join(value)
                return str(value)
            # Handle other fields
            else:
                return value if value is not None else ''

        return sorted(jobtable, key=get_sort_key, reverse=self.sort_reverse)

    def categorize_jobs(self, jobtable):
        job_sets = {
            "Array": [],
            "Running": [],
            "Ended": [],
            "Pending": [],
            "Other": []
        }
        for job in jobtable:
            if job.is_array_child:
                continue
            job_sets[job.get_state_category()].append(job)
        return job_sets

    def get_job_group_key(self, job):
        """Generate a key for grouping similar jobs together."""
        # Group by: user (if not in users view) + partition + state
        parts = []
        if self.view_type != 'users' and hasattr(job, 'user_name'):
            parts.append(job.user_name)
        if hasattr(job, 'partition'):
            parts.append(job.partition)
        parts.append(job.get_state_category())
        return ':'.join(parts)

    def group_similar_jobs(self, joblist):
        """Group similar jobs together for collapsing."""
        from collections import OrderedDict
        groups = OrderedDict()

        for job in joblist:
            if job.is_array_child:
                continue

            key = self.get_job_group_key(job)
            if key not in groups:
                groups[key] = []
            groups[key].append(job)

        return groups

    def build_job_widgets(self, joblist, label=None):
        if not joblist:
            return []

        # Group similar jobs first to see if there's anything to display
        groups = self.group_similar_jobs(joblist)

        # If no groups after filtering (e.g., all array_children), return empty
        if not groups:
            return []

        widgets = []

        # Add divider with label
        if label:
            widgets.append(JobListDivider(f"{label}"))

        # Find a representative job for this category (prefer non-array-parent)
        representative_job = None
        for job in joblist:
            if not job.is_array_parent:
                representative_job = job
                break
        if not representative_job:
            representative_job = joblist[0]

        # Add inline header for this category
        widgets.append(self.build_category_header(representative_job))

        # Build job widgets for each group
        for group_key, group_jobs in groups.items():
            group_count = len(group_jobs)

            # Check if user has explicitly set collapse state for this group
            if group_key in self.collapsed_groups:
                is_collapsed = self.collapsed_groups[group_key]
            else:
                # Auto-collapse only very large groups (>20 jobs)
                is_collapsed = group_count > 20

            # Determine how many to show when collapsed
            if is_collapsed and group_count > self.jobs_per_group:
                jobs_to_show = group_jobs[:self.jobs_per_group]
                remaining = group_count - self.jobs_per_group
            else:
                jobs_to_show = group_jobs
                remaining = 0

            # Build widgets for visible jobs
            for job in jobs_to_show:
                if job.is_array_parent:
                    widgets.append(job.widget)
                    if not job.array_collapsed_widget:
                        children = sorted(job.array_children, key=lambda j: j.job_id)
                        running_children = []
                        pending_children = []

                        # Categorize children
                        for child in children:
                            if is_running(child):
                                running_children.append(ChildJobWidget(child))
                            else:
                                pending_children.append(child)

                        # Show all running children
                        if running_children:
                            widgets.extend(running_children)

                        # Show pending children
                        pending_count = len(pending_children)
                        if pending_count > 0:
                            # Show at least one pending child for inspection, or all if there are few
                            if pending_count <= 3:
                                # Show all pending if 3 or fewer
                                for child in pending_children:
                                    widgets.append(ChildJobWidget(child))
                            else:
                                # Show first pending child, then summary for the rest
                                widgets.append(ChildJobWidget(pending_children[0]))
                                widgets.append(ArrayPendWidget(pending_count - 1))
                else:
                    widgets.append(job.widget)

            # Add expand marker if collapsed
            if remaining > 0:
                expand_text = f"  ... and {remaining} more similar jobs (press 'e' to expand)"
                widgets.append(ExpandableGroupMarker(expand_text, group_key))

        return widgets

    def restore_job_focus(self):
        focused = False
        for index, item in enumerate(self.jobwalker):
            if hasattr(item, "jobid") and item.jobid == self.selected_job:
                self.jobwalker.set_focus(index)
                focused = True
                break
        if not focused:
            for index, item in enumerate(self.jobwalker):
                if hasattr(item, "jobid"):
                    self.joblistbox.set_focus(0)
                    self.jobwalker.set_focus(index)
                    self.selected_job = item.jobid
                    break

    def build_category_header(self, representative_job):
        """Build a header widget for a category of jobs."""
        display_attr = representative_job.widget.display_attr
        job_states = representative_job.states
        is_array = bool(representative_job.is_array)
        is_array_parent = representative_job.is_array_parent
        job_is_running = bool(job_states & job_state_running)
        is_ended = bool(job_states & job_state_ended)
        is_pending = bool(job_states & job_state_pending)

        dynamic_labels = {
            'start_time': lambda: "Running" if job_is_running else "Started" if is_ended else "Array" if is_array else "Starting",
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
            'partition': "Partition",
            'name': "Name",
            'reason': "Reason",
            'tres': "Resources",
        }

        # Build SORT_KEYS mapping based on current visible columns
        # (update it each time we build a header so it matches visible columns)
        self.SORT_KEYS = {}
        column_fields = list(display_attr.keys())
        for i, field in enumerate(column_fields):
            if i <= 9:  # Support keys 0-9
                self.SORT_KEYS[str(i)] = field

        header_columns = []
        for i, headeritem in enumerate(display_attr):
            if headeritem in dynamic_labels:
                label = dynamic_labels[headeritem]()
            elif headeritem in static_labels:
                label = static_labels[headeritem]
            else:
                label = headeritem.capitalize()

            # Add column number prefix
            if i <= 9:
                label = f"{i}:{label}"

            # Add sort indicator if this is the sorted column
            if self.sort_col == headeritem:
                arrow = "▼" if self.sort_reverse else "▲"
                label = f"{label}{arrow}"

            # Create column with same sizing as job widgets
            align, width, _ = display_attr[headeritem]
            h = u.Text(label)
            header_columns.append((align, width, h))

        # Return the header as a widget
        return u.AttrMap(u.Columns(header_columns, dividechars=1), 'jobheader')

    def calculate_jobs_per_group(self):
        """Calculate how many jobs to show per group when collapsed based on available height."""
        if hasattr(self.main_screen, 'height'):
            # Be more generous with space - show more jobs to fill the screen
            # Reserve space for headers and dividers, but use most of the screen
            available = max(self.main_screen.height - 8, 5)
            # Show more jobs per group to fill the screen better
            self.jobs_per_group = max(available // 2, 10)
        else:
            self.jobs_per_group = 10  # Default increased from 5

    def on_resize(self):
        """Handle resize events - redraw with new dimensions."""
        self.calculate_jobs_per_group()
        self.update()

    def update(self):
        self.draw_entities()
        self.draw_jobs()

    def draw_jobs(self):
        # Prevent recursion
        if self._drawing:
            return
        self._drawing = True

        try:
            entity_table = self.get_entity_table()
            if not entity_table:
                return
            if self.selected_entity not in entity_table:
                return

            jobtable = entity_table[self.selected_entity]['jobs']
            jobtable = self.sort_jobs(jobtable)

            # Set widget width for responsive display
            # Right panel is ~75% of screen width, minus borders (2-3 chars)
            available_width = int(self.main_screen.width * 0.75) - 3 if hasattr(self.main_screen, 'width') else None
            for job in jobtable:
                job.set_widget_width(available_width, view_type=self.view_type)

            # Track selected job
            focus_w, _ = self.jobwalker.get_focus()
            if hasattr(focus_w, "jobid"):
                self.selected_job = focus_w.jobid
            else:
                self.selected_job = None

            u.disconnect_signal(self.jobwalker, 'modified', self.modified)
            self.jobwalker.clear()

            # Build widgets
            job_sets = self.categorize_jobs(jobtable)

            jobwalker_widgets = []
            # Priority order: Running > Pending > Ended > Array > Other
            for job_category in ["Running", "Pending", "Ended", "Array", "Other"]:
                jobwalker_widgets.extend(self.build_job_widgets(job_sets[job_category], label=job_category))

            # If we have screen space left and there are collapsed groups, expand some to fill screen
            if hasattr(self.main_screen, 'height'):
                available_lines = self.main_screen.height - 5  # Reserve for header/footer
                current_widget_count = len(jobwalker_widgets)

                # If we have lots of empty space, try to expand collapsed groups
                if current_widget_count < available_lines * 0.7:
                    # Find collapsed groups that could be expanded
                    expandable = []
                    for widget in jobwalker_widgets:
                        if isinstance(widget, ExpandableGroupMarker):
                            expandable.append(widget.group_key)

                    # Auto-expand some groups if we have room
                    if expandable and current_widget_count < available_lines * 0.5:
                        # Expand the first few collapsed groups to fill more space
                        for group_key in expandable[:2]:  # Expand up to 2 groups
                            if group_key in self.collapsed_groups and self.collapsed_groups[group_key]:
                                self.collapsed_groups[group_key] = False

                        # Rebuild if we changed anything
                        if any(not self.collapsed_groups.get(gk, True) for gk in expandable[:2]):
                            jobwalker_widgets = []
                            for job_category in ["Running", "Pending", "Ended", "Array", "Other"]:
                                jobwalker_widgets.extend(self.build_job_widgets(job_sets[job_category], label=job_category))

            self.jobwalker.extend(jobwalker_widgets)
            self.jw.set_title(self.right_title_template.format(entity=self.selected_entity))
            self.restore_job_focus()
            u.connect_signal(self.jobwalker, 'modified', self.modified)
        finally:
            self._drawing = False

    def draw_entities(self):
        entity_table = self.get_entity_table()
        if not entity_table:
            return

        # Sort by job count
        entity_table = dict(sorted(entity_table.items(), key=lambda item: item[1]['njobs'], reverse=True))

        u.disconnect_signal(self.entity_walker, 'modified', self.modified)
        self.entity_walker.clear()

        # Build entity widgets
        widgets = []
        for entity_name in entity_table:
            stats = entity_table[entity_name]
            widget = self.create_entity_widget(entity_name, stats['njobs'], stats['running'], stats['pending'])
            widgets.append(widget)

        self.entity_walker.extend(widgets)

        # Restore or set focus
        if self.selected_entity and self.selected_entity in entity_table:
            for i in self.entity_walker:
                entity_name = self._get_entity_from_widget(i)
                if entity_name == self.selected_entity:
                    pos = self.entity_walker.index(i)
                    self.entity_walker.set_focus(pos)
                    break
        else:
            if len(self.entity_walker) > 0:
                self.entity_walker.set_focus(0)
                focus_w, _ = self.entity_walker.get_focus()
                self.selected_entity = self._get_entity_from_widget(focus_w)

        u.connect_signal(self.entity_walker, 'modified', self.modified)

    # Abstract methods - must be implemented by subclasses
    def get_entity_table(self):
        """Return the entity table dict. Must be overridden."""
        raise NotImplementedError("Subclass must implement get_entity_table()")

    def create_entity_widget(self, entity_name, njobs, running, pending):
        """Create a widget for an entity in the left panel. Must be overridden."""
        raise NotImplementedError("Subclass must implement create_entity_widget()")


class ScreenViewUsers(TwoColumnJobView):
    """View grouped by users."""
    entity_attr = 'user'
    left_title = "Users"
    right_title_template = "Jobs for {entity}"
    view_type = 'users'

    def get_entity_table(self):
        return self.jobs.usertable if hasattr(self.jobs, "usertable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)


class ScreenViewAccounts(TwoColumnJobView):
    """View grouped by accounts."""
    entity_attr = 'account'
    left_title = "Accounts"
    right_title_template = "Jobs for account {entity}"
    view_type = 'accounts'

    def get_entity_table(self):
        return self.jobs.accounttable if hasattr(self.jobs, "accounttable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)


class ScreenViewPartitions(TwoColumnJobView):
    """View grouped by partitions."""
    entity_attr = 'partition'
    left_title = "Partitions"
    right_title_template = "Jobs in partition {entity}"
    view_type = 'partitions'

    def get_entity_table(self):
        return self.jobs.partitiontable if hasattr(self.jobs, "partitiontable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)


class ScreenViewStates(TwoColumnJobView):
    """View grouped by job states."""
    entity_attr = 'state'
    left_title = "Job States"
    right_title_template = "Jobs in state {entity}"
    view_type = 'states'

    def get_entity_table(self):
        return self.jobs.statetable if hasattr(self.jobs, "statetable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)


class ScreenViewCluster(u.WidgetWrap):
    """Cluster resources view - similar to htop/btop."""

    def __init__(self, main_screen, cluster_fetcher):
        self.main_screen = main_screen
        self.cluster_fetcher = cluster_fetcher
        self.walker = u.SimpleFocusListWalker([])
        self.listbox = u.ListBox(self.walker)

        # Wrap in a LineBox
        widget = u.LineBox(
            u.ScrollBar(self.listbox),
            title="Cluster Resources",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        u.WidgetWrap.__init__(self, widget)

        # Build initial view
        self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        """Handle resize events."""
        self.update()

    def make_bar(self, used, total, width=40):
        """Create a visual progress bar."""
        if total == 0:
            filled = 0
        else:
            filled = int((used / total) * width)
        bar = "█" * filled + "░" * (width - filled)
        return bar

    def format_memory(self, mb):
        """Format memory in MB to human-readable form."""
        if mb < 1024:
            return f"{mb:.0f}MB"
        elif mb < 1024 * 1024:
            return f"{mb/1024:.1f}GB"
        else:
            return f"{mb/(1024*1024):.2f}TB"

    def update(self):
        """Rebuild the cluster view."""
        # Save current scroll position
        old_focus = None
        try:
            old_focus = self.walker.focus
        except (IndexError, AttributeError):
            pass

        # Get data
        nodes_data = self.cluster_fetcher.fetch_nodes_sync()
        partitions_data = self.cluster_fetcher.fetch_partitions_sync()

        cluster = ClusterResources(nodes_data, partitions_data)
        overall = cluster.get_overall_stats()
        gpu_stats = cluster.get_gpu_stats()

        # Calculate bar width and column widths based on available screen width
        available_width = self.main_screen.width - 10 if hasattr(self.main_screen, 'width') else 100
        bar_width = min(max(available_width - 50, 20), 60)

        # Adaptive column widths for GPU/node names (wider screens = more space for names)
        if available_width > 120:
            gpu_name_width = 20
            node_name_width = 16
        elif available_width > 100:
            gpu_name_width = 16
            node_name_width = 12
        else:
            gpu_name_width = 12
            node_name_width = 10

        # Build widgets
        widgets = []

        # Calculate separator width
        separator_width = available_width

        # === Overall cluster status ===
        summary_sep = "═" * max(separator_width - 20, 20)
        widgets.append(u.AttrMap(u.Text(f"═══ CLUSTER OVERVIEW {summary_sep}"), 'jobheader'))

        # Aligned resource display with consistent column widths
        # Label column: 8 chars, bar in brackets, then stats
        node_info = f"{overall['up_nodes']} UP, {overall['down_nodes']} DOWN"
        widgets.append(u.Text(f"Nodes   : {node_info} (Total: {overall['total_nodes']})"))

        cpu_bar = self.make_bar(overall['cpus_alloc'], overall['cpus_total'], bar_width)
        cpu_used = f"{overall['cpus_alloc']}/{overall['cpus_total']}".rjust(11)
        widgets.append(u.Text(f"CPU     : [{cpu_bar}] {cpu_used} cores ({overall['cpu_util']:5.1f}%)"))

        mem_bar = self.make_bar(overall['mem_alloc_mb'], overall['mem_total_mb'], bar_width)
        mem_alloc = self.format_memory(overall['mem_alloc_mb'])
        mem_total = self.format_memory(overall['mem_total_mb'])
        mem_used = f"{mem_alloc}/{mem_total}".rjust(17)
        widgets.append(u.Text(f"Memory  : [{mem_bar}] {mem_used} ({overall['mem_util']:5.1f}%)"))

        # === GPU resources ===
        if gpu_stats:
            widgets.append(u.Divider())
            gpu_sep = "═" * max(separator_width - 18, 20)
            widgets.append(u.AttrMap(u.Text(f"═══ GPU RESOURCES {gpu_sep}"), 'jobheader'))

            # Sort GPU types for consistent display
            for gpu_type in sorted(gpu_stats.keys()):
                stats = gpu_stats[gpu_type]
                gpu_bar = self.make_bar(stats['used'], stats['total'], bar_width)
                # Smart truncate GPU names - preserve both model and memory size
                gpu_name = smart_truncate(gpu_type.upper(), gpu_name_width, mode='middle')
                gpu_count = f"{stats['used']}/{stats['total']}".rjust(7)
                gpu_text = f"{gpu_name:{gpu_name_width}s} : [{gpu_bar}] {gpu_count} GPUs ({stats['util']:5.1f}%)"
                widgets.append(u.Text(gpu_text))

        # === GPU nodes detail ===
        gpu_nodes = [n for n in cluster.nodes if n.gpus and n.is_up]
        if gpu_nodes:
            widgets.append(u.Divider())
            gpu_nodes_sep = "═" * max(separator_width - 15, 20)
            widgets.append(u.AttrMap(u.Text(f"═══ GPU NODES {gpu_nodes_sep}"), 'jobheader'))

            # Column headers for GPU nodes
            node_col = "Node".ljust(node_name_width)
            gpu_col = "GPU Type".ljust(gpu_name_width)
            widgets.append(u.Text(('faded', f"{node_col} {gpu_col}   Usage                 State      Status")))
            widgets.append(u.Divider("─"))

            # Sort by node name
            gpu_nodes = sorted(gpu_nodes, key=lambda n: n.name)

            for node in gpu_nodes:
                for gpu in node.gpus:
                    node_bar = self.make_bar(gpu.used, gpu.total, 20)
                    state = "MIXED" if gpu.used > 0 and gpu.free > 0 else "IDLE" if gpu.used == 0 else "FULL"
                    state_str = (node.state[0] if node.state else "UNKNOWN")[:10]

                    # Smart truncate names - preserve identifying prefix and suffix
                    node_name_trunc = smart_truncate(node.name, node_name_width, mode='middle')
                    gpu_name = smart_truncate(gpu.gpu_type, gpu_name_width, mode='middle')
                    usage = f"{gpu.used}/{gpu.total}".center(5)

                    # Color-code based on usage
                    if state == "FULL":
                        attr = 'warning'
                    elif state == "IDLE":
                        attr = 'faded'
                    else:
                        attr = None

                    node_line = f"{node_name_trunc:{node_name_width}s} {gpu_name:{gpu_name_width}s} [{node_bar}] {usage}  {state:5s}      {state_str}"

                    if attr:
                        widgets.append(u.AttrMap(u.Text(node_line), attr))
                    else:
                        widgets.append(u.Text(node_line))

                    # Show indices if GPUs are in use
                    if gpu.used > 0 and gpu.indices != "N/A":
                        indent = " " * (node_name_width + gpu_name_width + 2)
                        widgets.append(u.Text(('faded', f"{indent}└─ Active GPUs: {gpu.indices}")))

        # === All nodes summary ===
        widgets.append(u.Divider())
        all_nodes_sep = "═" * max(separator_width - 19, 20)
        widgets.append(u.AttrMap(u.Text(f"═══ ALL NODES BY STATE {all_nodes_sep}"), 'jobheader'))

        # Group nodes by state with better formatting
        nodes_by_state = cluster.get_nodes_by_state()

        # Sort states by priority: ALLOCATED, MIXED, IDLE, DOWN, DRAIN, etc.
        state_priority = {
            'ALLOCATED': 0,
            'MIXED': 1,
            'IDLE': 2,
            'COMPLETING': 3,
            'DOWN': 4,
            'DRAIN': 5,
            'DRAINED': 6,
            'DRAINING': 7,
            'MAINT': 8,
            'RESERVED': 9,
        }

        sorted_states = sorted(nodes_by_state.keys(), key=lambda s: state_priority.get(s, 99))

        for state in sorted_states:
            node_list = nodes_by_state[state]
            count_str = f"({len(node_list):2d})".ljust(5)

            # Color-code state labels
            if state in ['ALLOCATED', 'MIXED']:
                state_attr = 'success'
            elif state in ['DOWN', 'DRAIN', 'DRAINED', 'DRAINING']:
                state_attr = 'error'
            elif state == 'IDLE':
                state_attr = 'faded'
            else:
                state_attr = None

            # Format node list - wrap at screen width
            node_names = sorted([n.name for n in node_list])

            # Build wrapped lines
            max_line_width = separator_width - 25  # Account for state label
            current_line = []
            current_width = 0
            lines = []

            for node_name in node_names:
                node_width = len(node_name) + 2  # +2 for ", "
                if current_width + node_width > max_line_width and current_line:
                    lines.append(", ".join(current_line))
                    current_line = [node_name]
                    current_width = len(node_name)
                else:
                    current_line.append(node_name)
                    current_width += node_width

            if current_line:
                lines.append(", ".join(current_line))

            # Display state and first line
            state_label = f"{state:12s} {count_str}"
            if lines:
                first_line = f"{state_label}: {lines[0]}"
                if state_attr:
                    widgets.append(u.AttrMap(u.Text(first_line), state_attr))
                else:
                    widgets.append(u.Text(first_line))

                # Additional lines indented
                for line in lines[1:]:
                    indent = " " * (len(state_label) + 2)
                    if state_attr:
                        widgets.append(u.AttrMap(u.Text(f"{indent}{line}"), state_attr))
                    else:
                        widgets.append(u.Text(f"{indent}{line}"))

        # Update walker
        self.walker.clear()
        self.walker.extend(widgets)

        # Restore scroll position
        if len(self.walker) > 0:
            if old_focus is not None and old_focus < len(self.walker):
                self.walker.set_focus(old_focus)
            else:
                self.walker.set_focus(0)


class ConfirmExit(u.WidgetWrap):
    def __init__(self, main_screen):
        self.main_screen = main_screen
        y = u.AttrMap(u.Button("Yes", self.exit_program), 'buttons', 'buttons_selected')
        n = u.AttrMap(u.Button("No", self.cancel_exit), 'buttons', 'buttons_selected')
        b = [y, n]
        buttons = u.Columns(b)

        widget = u.AttrMap(
            u.LineBox(
                u.Filler(u.Pile([buttons])),
                title='Confirm exit?',
                tlcorner='╭', trcorner='╮',
                blcorner='╰', brcorner='╯'
            ),
            'bg'
        )
        u.WidgetWrap.__init__(self, widget)

    def keypress(self, size, key):
        return super().keypress(size, key)

    def exit_program(self, a=None) -> None:
        raise u.ExitMainLoop()

    def cancel_exit(self, a=None):
        self.main_screen.close_overlay()
