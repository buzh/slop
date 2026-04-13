"""Enhanced job detail overlay using sacct data."""

import urwid as u
import datetime
from slop.utils import format_duration, smart_truncate


class JobDetailSacct(u.WidgetWrap):
    """Detailed job view using sacct accounting data with efficiency metrics."""

    def __init__(self, sacct_job, main_screen=None):
        """Initialize job detail overlay.

        Args:
            sacct_job: Job data from sacct JSON
            main_screen: Main screen instance (for height and width calculation)
        """
        self.sacct_job = sacct_job
        self.main_screen = main_screen

        # Calculate dimensions based on screen size
        if main_screen and hasattr(main_screen, 'height'):
            self.height = max(main_screen.height - 8, 20)
        else:
            self.height = 35

        # Calculate usable width for text content
        # Account for LineBox borders (2) + padding + label width (~14 chars)
        if main_screen and hasattr(main_screen, 'width'):
            self.content_width = max(main_screen.width - 20, 50)
        else:
            self.content_width = 70

        widgets = self.build_widgets()

        # Create scrollable listbox
        walker = u.SimpleFocusListWalker(widgets)
        listbox = u.ListBox(walker)

        # Wrap in LineBox with title
        state = ' '.join(sacct_job.get('state', {}).get('current', ['UNKNOWN']))
        title = f"Job {sacct_job.get('job_id', 'N/A')} - {state}"
        body = u.LineBox(
            listbox,
            title=title,
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        body = u.AttrMap(body, 'normal', 'normal')
        super().__init__(body)

    def build_widgets(self):
        """Build the overlay widgets with sections."""
        job = self.sacct_job
        widgets = []

        # Effective width for values (content_width minus label)
        value_width = self.content_width - 14

        # === BASIC INFO ===
        widgets.append(u.AttrMap(u.Text("BASIC INFO"), 'jobheader'))
        widgets.append(u.Divider("─"))
        widgets.append(u.Text(f"Job ID      : {job.get('job_id', 'N/A')}"))
        widgets.append(u.Text(f"Name        : {job.get('name', 'N/A')}"))
        widgets.append(u.Text(f"User        : {job.get('user', 'N/A')}"))
        widgets.append(u.Text(f"Account     : {job.get('account', 'N/A')}"))
        widgets.append(u.Text(f"Partition   : {job.get('partition', 'N/A')}"))
        widgets.append(u.Text(f"QoS         : {job.get('qos', 'N/A')}"))

        # Array job info
        array_info = job.get('array', {})
        if array_info.get('job_id'):
            task_id = array_info.get('task_id', {})
            if task_id.get('set'):
                widgets.append(u.Text(f"Array       : {array_info['job_id']} (Task {task_id['number']})"))

        widgets.append(u.Divider())

        # === STATUS ===
        widgets.append(u.AttrMap(u.Text("STATUS"), 'jobheader'))
        widgets.append(u.Divider("─"))

        state_info = job.get('state', {})
        state = ' '.join(state_info.get('current', ['UNKNOWN']))

        # Color-code state
        if state in ["FAILED", "TIMEOUT", "OUT_OF_MEMORY", "CANCELLED"]:
            widgets.append(u.AttrMap(u.Text(f"State       : {state}"), 'state_failed'))
        elif state in ["COMPLETED"]:
            widgets.append(u.AttrMap(u.Text(f"State       : {state}"), 'success'))
        else:
            widgets.append(u.Text(f"State       : {state}"))

        # Exit code
        exit_code_info = job.get('derived_exit_code', {})
        exit_code = exit_code_info.get('return_code', {}).get('number', 'N/A')
        exit_status = ', '.join(exit_code_info.get('status', []))
        if state in ["FAILED", "TIMEOUT", "OUT_OF_MEMORY"]:
            widgets.append(u.AttrMap(u.Text(f"Exit Code   : {exit_code} ({exit_status})"), 'error'))
        else:
            widgets.append(u.Text(f"Exit Code   : {exit_code} ({exit_status})"))

        # Priority
        priority = job.get('priority', {})
        if priority.get('set'):
            widgets.append(u.Text(f"Priority    : {priority['number']}"))

        widgets.append(u.Divider())

        # === TIMELINE ===
        widgets.append(u.AttrMap(u.Text("TIMELINE"), 'jobheader'))
        widgets.append(u.Divider("─"))

        time_info = job.get('time', {})
        widgets.append(u.Text(f"Submitted   : {self.format_timestamp(time_info.get('submission'))}"))
        widgets.append(u.Text(f"Eligible    : {self.format_timestamp(time_info.get('eligible'))}"))
        widgets.append(u.Text(f"Started     : {self.format_timestamp(time_info.get('start'))}"))
        widgets.append(u.Text(f"Ended       : {self.format_timestamp(time_info.get('end'))}"))

        # Queue time
        if time_info.get('start') and time_info.get('submission'):
            queue_time = time_info['start'] - time_info['submission']
            widgets.append(u.Text(f"Queue Time  : {format_duration(queue_time)}"))

        # Elapsed time
        elapsed = time_info.get('elapsed', 0)
        widgets.append(u.Text(f"Elapsed     : {format_duration(elapsed)}"))

        # Time limit
        limit = time_info.get('limit', {})
        if limit.get('set') and not limit.get('infinite'):
            limit_str = format_duration(limit['number'] * 60)
            widgets.append(u.Text(f"Time Limit  : {limit_str}"))

        widgets.append(u.Divider())

        # === RESOURCE EFFICIENCY ===
        widgets.append(u.AttrMap(u.Text("RESOURCE EFFICIENCY"), 'jobheader'))
        widgets.append(u.Divider("─"))

        # Get allocated resources
        tres_alloc = {item['type']: item['count'] for item in job.get('tres', {}).get('allocated', [])}
        cpus_alloc = tres_alloc.get('cpu', 0)
        mem_alloc_mb = tres_alloc.get('mem', 0)

        # CPU efficiency
        if cpus_alloc > 0 and elapsed > 0:
            total_time = time_info.get('total', {})
            total_cpu_sec = total_time.get('seconds', 0) + total_time.get('microseconds', 0) / 1000000
            cpu_hours_used = total_cpu_sec / 3600
            cpu_hours_alloc = (cpus_alloc * elapsed) / 3600
            cpu_efficiency = (cpu_hours_used / cpu_hours_alloc * 100) if cpu_hours_alloc > 0 else 0

            widgets.append(u.Text(f"CPU Allocated : {cpus_alloc} cores"))
            widgets.append(u.Text(f"CPU Used      : {cpu_hours_used:.2f} core-hours"))
            widgets.append(u.Text(f"CPU Allocated : {cpu_hours_alloc:.2f} core-hours"))

            if cpu_efficiency >= 80:
                widgets.append(u.AttrMap(u.Text(f"CPU Efficiency: {cpu_efficiency:.1f}%"), 'success'))
            elif cpu_efficiency >= 50:
                widgets.append(u.AttrMap(u.Text(f"CPU Efficiency: {cpu_efficiency:.1f}%"), 'warning'))
            else:
                widgets.append(u.AttrMap(u.Text(f"CPU Efficiency: {cpu_efficiency:.1f}% (LOW)"), 'error'))

        # Memory info
        if mem_alloc_mb > 0:
            mem_alloc_gb = mem_alloc_mb / 1024
            widgets.append(u.Text(f"Memory Alloc  : {mem_alloc_gb:.1f}GB"))

        # Time efficiency
        if limit.get('set') and not limit.get('infinite') and limit['number'] > 0:
            time_efficiency = (elapsed / (limit['number'] * 60)) * 100
            widgets.append(u.Text(f"Time Used     : {time_efficiency:.1f}% of limit"))
            if time_efficiency > 95:
                widgets.append(u.AttrMap(u.Text("  (Consider requesting more time)"), 'warning'))

        widgets.append(u.Divider())

        # === JOB STEPS ===
        steps = job.get('steps', [])
        if steps:
            widgets.append(u.AttrMap(u.Text(f"JOB STEPS ({len(steps)} steps)"), 'jobheader'))
            widgets.append(u.Divider("─"))

            # Calculate step name width (wider screens can show longer step names)
            step_name_width = min(max(self.content_width // 6, 10), 20)

            for step in steps:
                step_info = step.get('step', {})
                step_id = step_info.get('id', 'N/A')
                step_name_raw = step_info.get('name', 'N/A')
                # Smart truncate step name - preserve both start and end
                step_name = smart_truncate(step_name_raw, step_name_width, mode='middle')
                step_state = ' '.join(step.get('state', []))

                step_time = step.get('time', {})
                step_elapsed = step_time.get('elapsed', 0)

                step_exit = step.get('exit_code', {})
                step_exit_code = step_exit.get('return_code', {}).get('number', 'N/A')

                step_line = f"  {step_name:{step_name_width}s} | {step_state:10s} | {format_duration(step_elapsed):>10s} | Exit: {step_exit_code}"

                if step_state == 'FAILED':
                    widgets.append(u.AttrMap(u.Text(step_line), 'error'))
                elif step_state == 'COMPLETED':
                    widgets.append(u.Text(step_line))
                else:
                    widgets.append(u.AttrMap(u.Text(step_line), 'faded'))

            widgets.append(u.Divider())

        # === RESOURCES ===
        widgets.append(u.AttrMap(u.Text("RESOURCES"), 'jobheader'))
        widgets.append(u.Divider("─"))

        widgets.append(u.Text(f"Nodes       : {job.get('nodes', 'N/A')}"))
        widgets.append(u.Text(f"CPUs        : {cpus_alloc}"))

        mem_req = job.get('required', {})
        mem_per_node = mem_req.get('memory_per_node', {})
        if mem_per_node.get('set'):
            mem_mb = mem_per_node['number']
            widgets.append(u.Text(f"Memory      : {mem_mb / 1024:.1f}GB"))

        widgets.append(u.Divider())

        # === PATHS ===
        widgets.append(u.AttrMap(u.Text("PATHS"), 'jobheader'))
        widgets.append(u.Divider("─"))

        work_dir = job.get('working_directory', 'N/A')
        stdout = job.get('stdout', 'N/A')
        stderr = job.get('stderr', 'N/A')

        # Truncate long paths based on available width
        if len(work_dir) > value_width:
            work_dir = "..." + work_dir[-(value_width-3):]
        if len(stdout) > value_width:
            stdout = "..." + stdout[-(value_width-3):]
        if len(stderr) > value_width:
            stderr = "..." + stderr[-(value_width-3):]

        widgets.append(u.Text(f"Work Dir    : {work_dir}"))
        widgets.append(u.Text(f"Stdout      : {stdout}"))
        if stderr and stderr != stdout:
            widgets.append(u.Text(f"Stderr      : {stderr}"))

        # Submit command if available
        submit_line = job.get('submit_line', '')
        if submit_line:
            widgets.append(u.Divider())
            widgets.append(u.AttrMap(u.Text("SUBMIT COMMAND"), 'jobheader'))
            widgets.append(u.Divider("─"))
            # Wrap long command based on available width
            if len(submit_line) > self.content_width:
                parts = []
                for i in range(0, len(submit_line), self.content_width):
                    parts.append(submit_line[i:i+self.content_width])
                for part in parts:
                    widgets.append(u.Text(part))
            else:
                widgets.append(u.Text(submit_line))

        return widgets

    def format_timestamp(self, ts):
        """Format Unix timestamp to readable string."""
        if ts is None or ts == 0:
            return "N/A"
        try:
            return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except:
            return "N/A"
