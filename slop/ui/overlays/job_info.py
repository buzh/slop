"""Job information overlay."""
import urwid as u
import datetime
from slop.slurm import is_running, is_ended, is_pending, reasons
from slop.utils import format_duration, nice_tres
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.state_style import state_attr
from slop.ui.widgets import rounded_box, GenericOverlayText


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

        state = ' '.join(job.job_state)
        body = u.AttrMap(rounded_box(listbox, title=f"Job {job.job_id} - {state}"),
                         'normal', 'normal')
        super().__init__(body)

    def keypress(self, size, key):
        if key == 'h':
            self._open_user_history()
            return None
        return super().keypress(size, key)

    def _open_user_history(self):
        """Close this overlay and open the report view for the job's owner."""
        sc = self.main_screen
        if sc is None:
            return
        username = self.job.user_name
        sc.close_overlay()
        sc.open_overlay(GenericOverlayText(
            sc, f"Loading history for {username}...\n\nFetching account usage data..."
        ))
        sc.loop.draw_screen()
        result = sc.sreport_fetcher.fetch_user_utilization(username)
        sc.close_overlay()
        if result:
            sc.handle_search_result(result, 'user', username)
        else:
            sc.open_overlay(GenericOverlayText(
                sc, f"Failed to fetch data for {username}"
            ))

    def build_widgets(self):
        """Build the overlay widgets with sections and computed fields."""
        job = self.job
        widgets = []

        # Determine job state category
        state = ' '.join(job.job_state)
        primary_state = job.job_state[0] if job.job_state else ''
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

        # Array job info - only show if actually part of an array
        if hasattr(job, 'array_job_id') and job.array_job_id.get('set') and job.array_job_id['number'] != 0:
            array_id = job.array_job_id['number']
            if hasattr(job, 'array_task_id') and job.array_task_id.get('set'):
                task_id = job.array_task_id['number']
                widgets.append(u.Text(f"Array Job   : {array_id} (Task {task_id})"))
            elif hasattr(job, 'array_max_tasks') and job.array_max_tasks.get('set'):
                max_tasks = job.array_max_tasks['number']
                widgets.append(u.Text(f"Array Job   : {array_id} (Parent, {max_tasks} tasks)"))

        # QoS and Priority
        qos = getattr(job, 'qos', EMPTY_PLACEHOLDER)
        widgets.append(u.Text(f"QoS         : {qos}"))

        if hasattr(job, 'priority') and job.priority.get('set'):
            priority = job.priority['number']
            widgets.append(u.Text(f"Priority    : {priority:,}"))

        widgets.append(u.Divider())

        # === STATUS ===
        widgets.append(u.AttrMap(u.Text("STATUS"), 'jobheader'))
        widgets.append(u.Divider("─"))

        widgets.append(u.AttrMap(u.Text(f"State       : {state}"), state_attr(primary_state)))

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

            # Show eligible time (when job became eligible to run)
            if hasattr(job, 'eligible_time') and job.eligible_time.get('set'):
                eligible = self.format_time(job.eligible_time)
                widgets.append(u.Text(f"Eligible    : {eligible}"))

            # Show last scheduler evaluation
            if hasattr(job, 'last_sched_evaluation') and job.last_sched_evaluation.get('set'):
                last_eval = self.format_time(job.last_sched_evaluation)
                widgets.append(u.Text(f"Last Eval   : {last_eval}"))
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
        time_limit = EMPTY_PLACEHOLDER
        if hasattr(job, 'time_limit') and job.time_limit.get("set"):
            time_limit = format_duration(job.time_limit["number"] * 60)
        widgets.append(u.Text(f"Time Limit  : {time_limit}"))

        widgets.append(u.Divider())

        # === RESOURCES ===
        widgets.append(u.AttrMap(u.Text("RESOURCES"), 'jobheader'))
        widgets.append(u.Divider("─"))

        # Parse TRES for better display
        tres_info = self.parse_tres(job)

        # Node count
        if hasattr(job, 'node_count') and job.node_count.get('set'):
            node_count = job.node_count['number']
            widgets.append(u.Text(f"Node Count  : {node_count}"))

        # Actual nodes (if allocated)
        if job.nodes:
            # Truncate long node lists
            nodes_str = str(job.nodes)
            if len(nodes_str) > self.content_width - 14:
                nodes_str = nodes_str[:self.content_width - 17] + "..."
            widgets.append(u.Text(f"Nodes       : {nodes_str}"))

        widgets.append(u.Text(f"CPUs        : {tres_info['cpus']}"))

        # CPUs per task (threading info)
        if hasattr(job, 'cpus_per_task') and job.cpus_per_task.get('set'):
            cpus_per_task = job.cpus_per_task['number']
            widgets.append(u.Text(f"CPUs/Task   : {cpus_per_task}"))

        # Tasks
        if hasattr(job, 'tasks') and job.tasks.get('set'):
            tasks = job.tasks['number']
            widgets.append(u.Text(f"Tasks       : {tasks}"))

        widgets.append(u.Text(f"Memory      : {tres_info['memory']}"))

        if tres_info['gpus']:
            widgets.append(u.Text(f"GPUs        : {tres_info['gpus']}"))

        widgets.append(u.Divider())

        # === JOB EXECUTION ===
        widgets.append(u.AttrMap(u.Text("JOB EXECUTION"), 'jobheader'))
        widgets.append(u.Divider("─"))

        # Command
        command = getattr(job, 'command', None)
        if command:
            # Truncate long commands
            if len(command) > self.content_width - 14:
                command = command[:self.content_width - 17] + "..."
            widgets.append(u.Text(f"Command     : {command}"))

        # Working directory
        work_dir = getattr(job, 'current_working_directory', None)
        if work_dir:
            if len(work_dir) > self.content_width - 14:
                work_dir = work_dir[:self.content_width - 17] + "..."
            widgets.append(u.Text(f"Work Dir    : {work_dir}"))

        # Output file
        stdout = getattr(job, 'standard_output', None)
        if stdout:
            if len(stdout) > self.content_width - 14:
                stdout = stdout[:self.content_width - 17] + "..."
            widgets.append(u.Text(f"Output      : {stdout}"))

        # Error file
        stderr = getattr(job, 'standard_error', None)
        if stderr:
            if len(stderr) > self.content_width - 14:
                stderr = stderr[:self.content_width - 17] + "..."
            widgets.append(u.Text(f"Error       : {stderr}"))

        # Dependencies
        dependency = getattr(job, 'dependency', '')
        if dependency:
            if len(dependency) > self.content_width - 14:
                dependency = dependency[:self.content_width - 17] + "..."
            widgets.append(u.Text(f"Dependency  : {dependency}"))

        widgets.append(u.Divider())

        return widgets

    def format_time(self, ts):
        """Format timestamp to readable string."""
        try:
            if isinstance(ts, dict) and ts.get("set") and "number" in ts:
                return datetime.datetime.fromtimestamp(int(ts["number"])).strftime("%Y-%m-%d %H:%M:%S")
            return "Not set"
        except Exception:
            return EMPTY_PLACEHOLDER

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
            return EMPTY_PLACEHOLDER
        except Exception:
            return EMPTY_PLACEHOLDER

    def calculate_queue_time(self, job):
        """Calculate how long job has been waiting in queue."""
        try:
            submit_ts = job.submit_time.get('number')
            if not submit_ts:
                return EMPTY_PLACEHOLDER
            now = datetime.datetime.now().timestamp()
            elapsed = int(now - submit_ts)
            return format_duration(elapsed)
        except Exception:
            return EMPTY_PLACEHOLDER

    def calculate_runtime(self, job):
        """Calculate how long job has been running."""
        try:
            start_ts = job.start_time.get('number')
            if not start_ts:
                return EMPTY_PLACEHOLDER
            now = datetime.datetime.now().timestamp()
            elapsed = int(now - start_ts)
            return format_duration(elapsed)
        except Exception:
            return EMPTY_PLACEHOLDER

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
                return EMPTY_PLACEHOLDER
            elapsed = int(end_ts - start_ts)
            return format_duration(elapsed)
        except Exception:
            return EMPTY_PLACEHOLDER

    def parse_tres(self, job):
        """Parse TRES string into readable components."""
        info = {'cpus': EMPTY_PLACEHOLDER, 'memory': EMPTY_PLACEHOLDER, 'gpus': None}

        # CPUs
        if hasattr(job, 'cpus') and job.cpus.get('set'):
            info['cpus'] = str(job.cpus['number'])

        # Memory - try memory_per_cpu first, then memory_per_node
        if hasattr(job, 'memory_per_cpu') and job.memory_per_cpu.get('set'):
            mem_mb = job.memory_per_cpu['number']
            cpus = job.cpus.get('number', 1) if hasattr(job, 'cpus') else 1
            total_mb = mem_mb * cpus
            if total_mb >= 1024:
                info['memory'] = f"{total_mb / 1024:.1f}GB ({mem_mb}MB/core)"
            else:
                info['memory'] = f"{total_mb}MB ({mem_mb}MB/core)"
        elif hasattr(job, 'memory_per_node') and job.memory_per_node.get('set'):
            mem_mb = job.memory_per_node['number']
            if mem_mb >= 1024:
                info['memory'] = f"{mem_mb / 1024:.1f}GB/node"
            else:
                info['memory'] = f"{mem_mb}MB/node"

        # GPUs from TRES string
        tres_str = getattr(job, 'tres_req_str', '') or getattr(job, 'tres_alloc_str', '')
        if tres_str and 'gpu' in tres_str.lower():
            # Extract GPU info from TRES
            gpu_info = nice_tres(job)
            if 'GPU' in gpu_info:
                info['gpus'] = gpu_info

        return info
