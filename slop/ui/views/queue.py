"""Queue Status view - Shows pending jobs from the scheduler's perspective."""

import urwid as u
import datetime
from slop.utils import format_duration


class QueueJobWidget(u.WidgetWrap):
    """Widget for displaying a single pending job in the queue view."""

    def __init__(self, job, rank, width=None):
        self.job = job
        self.jobid = job.job_id
        self.rank = rank
        self.width = width or 120

        # Build the widget
        widget = self._build_widget()
        super().__init__(widget)

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build_widget(self):
        """Build the job display widget."""
        job = self.job

        # Priority
        priority = getattr(job, 'priority', {})
        if isinstance(priority, dict):
            priority_num = priority.get('number', 0)
        else:
            priority_num = priority if isinstance(priority, int) else 0

        # Job size indicator (resource footprint)
        size_indicator = self._get_size_indicator()

        # Time limit
        time_limit = getattr(job, 'time_limit', {})
        if isinstance(time_limit, dict) and time_limit.get('set'):
            duration_min = time_limit.get('number', 0)
            duration_str = format_duration(duration_min * 60)
        else:
            duration_str = "?"

        # Estimated start time
        start_time = getattr(job, 'start_time', {})
        eta_str = self._get_eta_string(start_time)

        # Wait time
        submit_time = getattr(job, 'submit_time', {})
        if isinstance(submit_time, dict) and submit_time.get('set'):
            submit = datetime.datetime.fromtimestamp(submit_time['number'])
            now = datetime.datetime.now()
            wait_sec = int((now - submit).total_seconds())
            wait_str = format_duration(wait_sec)
        else:
            wait_str = "?"

        # Reason
        reason = getattr(job, 'state_reason', 'Unknown')

        # Resource summary
        resource_str = self._get_resource_summary()

        # QOS
        qos = getattr(job, 'qos', '?')

        # Username
        username = getattr(job, 'user_name', '?')

        # Format based on width
        if self.width < 100:
            # Narrow
            line = f"{self.rank:>3} {priority_num:>7} {size_indicator:<3} {duration_str:>7} {username[:8]:<8} {job.name[:15]:<15}"
        elif self.width < 140:
            # Medium
            line = f"{self.rank:>3} {priority_num:>7} {size_indicator:<3} {duration_str:>7} {eta_str[:14]:<14} {username[:10]:<10} {reason[:12]:<12} {job.name[:20]:<20}"
        else:
            # Wide
            line = f"{self.rank:>3} {priority_num:>7} {size_indicator:<3} {duration_str:>7} {eta_str[:14]:<14} {wait_str[:11]:>11} {username[:10]:<10} {qos[:8]:<8} {reason[:15]:<15} {resource_str[:18]:<18} {job.name[:25]:<25}"

        # Color based on priority/reason
        attr = self._get_color_attr(reason)
        return u.AttrMap(u.Text(line), attr, 'normal_selected')

    def _get_size_indicator(self):
        """Get visual size indicator based on resource footprint."""
        job = self.job

        # Get node count
        node_count = getattr(job, 'node_count', {})
        if isinstance(node_count, dict):
            nodes = node_count.get('number', 1)
        else:
            nodes = node_count if isinstance(node_count, int) else 1

        # Get CPU count
        cpus_obj = getattr(job, 'cpus', {})
        if isinstance(cpus_obj, dict):
            cpus = cpus_obj.get('number', 1)
        else:
            cpus = cpus_obj if isinstance(cpus_obj, int) else 1

        # Get time limit (in minutes)
        time_limit = getattr(job, 'time_limit', {})
        if isinstance(time_limit, dict) and time_limit.get('set'):
            duration_min = time_limit.get('number', 60)
        else:
            duration_min = 60

        # Calculate "resource-hours" footprint
        # Small job: <100 core-hours
        # Medium job: 100-1000 core-hours
        # Large job: >1000 core-hours
        core_hours = (cpus * duration_min) / 60

        if core_hours < 100:
            return "▪"  # Small - can backfill easily
        elif core_hours < 1000:
            return "▪▪"  # Medium
        else:
            return "▪▪▪"  # Large - needs reserved slot

    def _get_eta_string(self, start_time):
        """Get estimated time to start as a human-readable string."""
        if not isinstance(start_time, dict) or not start_time.get('set'):
            return "unknown"

        start_timestamp = start_time.get('number', 0)
        if start_timestamp == 0:
            return "unknown"

        start_dt = datetime.datetime.fromtimestamp(start_timestamp)
        now = datetime.datetime.now()

        # If in the past or very soon, say "now"
        diff_sec = (start_dt - now).total_seconds()
        if diff_sec < 60:
            return "now"

        # Future time
        return f"in {format_duration(int(diff_sec))}"

    def _get_resource_summary(self):
        """Get compact resource summary."""
        job = self.job

        # Nodes
        node_count = getattr(job, 'node_count', {})
        if isinstance(node_count, dict):
            nodes = node_count.get('number', 0)
        else:
            nodes = node_count if isinstance(node_count, int) else 0

        # CPUs
        cpus_obj = getattr(job, 'cpus', {})
        if isinstance(cpus_obj, dict):
            cpus = cpus_obj.get('number', 0)
        else:
            cpus = cpus_obj if isinstance(cpus_obj, int) else 0

        # Memory (try memory_per_node first, then memory_per_cpu)
        mem_per_node = getattr(job, 'memory_per_node', {})
        mem_per_cpu = getattr(job, 'memory_per_cpu', {})

        if isinstance(mem_per_node, dict) and mem_per_node.get('set'):
            mem_mb = mem_per_node.get('number', 0)
            mem_str = f"{mem_mb // 1024}GB" if mem_mb > 1024 else f"{mem_mb}MB"
        elif isinstance(mem_per_cpu, dict) and mem_per_cpu.get('set'):
            mem_mb = mem_per_cpu.get('number', 0) * cpus
            mem_str = f"{mem_mb // 1024}GB" if mem_mb > 1024 else f"{mem_mb}MB"
        else:
            mem_str = "?"

        # Check for GPUs in tres_req_str
        tres_str = getattr(job, 'tres_req_str', '')
        gpu_count = 0
        if 'gres/gpu=' in tres_str:
            # Simple parsing - look for gres/gpu=N
            import re
            match = re.search(r'gres/gpu=(\d+)', tres_str)
            if match:
                gpu_count = int(match.group(1))

        parts = []
        if nodes > 0:
            parts.append(f"{nodes}n")
        if cpus > 0:
            parts.append(f"{cpus}c")
        if mem_str != "?":
            parts.append(mem_str)
        if gpu_count > 0:
            parts.append(f"{gpu_count}gpu")

        return " ".join(parts) if parts else "?"

    def _get_color_attr(self, reason):
        """Get color attribute based on job reason."""
        # Jobs that can backfill - normal
        if reason in ['Priority', 'Resources']:
            return 'normal'
        # Jobs with dependencies or holds - warning
        elif reason in ['Dependency', 'JobHeldUser', 'JobHeldAdmin']:
            return 'warning'
        # Jobs with issues - error
        elif 'NotAvail' in reason or 'Invalid' in reason:
            return 'error'
        else:
            return 'normal'


class QueueGroupWidget(u.WidgetWrap):
    """Widget for a collapsed group of jobs (same user + reason)."""

    def __init__(self, start_rank, end_rank, job_group, width=None):
        self.start_rank = start_rank
        self.end_rank = end_rank
        self.job_group = job_group  # List of jobs in this group
        self.group_key = f"{start_rank}-{end_rank}"  # Unique key for tracking expansion
        self.width = width or 120

        # Build the widget
        widget = self._build_widget()
        super().__init__(widget)

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build_widget(self):
        """Build the group summary widget."""
        # Get representative job (first in group)
        job = self.job_group[0]
        count = len(self.job_group)

        # Get shared attributes
        username = getattr(job, 'user_name', '?')
        reason = getattr(job, 'state_reason', 'Unknown')

        # Priority range
        priorities = []
        for j in self.job_group:
            priority = getattr(j, 'priority', {})
            if isinstance(priority, dict):
                priorities.append(priority.get('number', 0))
            else:
                priorities.append(priority if isinstance(priority, int) else 0)

        priority_min = min(priorities) if priorities else 0
        priority_max = max(priorities) if priorities else 0
        # Just show max priority to fit in column width
        priority_num = priority_max

        # Size indicator - use most common
        sizes = []
        for j in self.job_group:
            widget = QueueJobWidget(j, 0, width=self.width)
            sizes.append(widget._get_size_indicator())
        size_indicator = max(set(sizes), key=sizes.count) if sizes else "▪"

        # Time range
        durations = []
        for j in self.job_group:
            time_limit = getattr(j, 'time_limit', {})
            if isinstance(time_limit, dict) and time_limit.get('set'):
                durations.append(time_limit.get('number', 0))

        if durations:
            min_dur = min(durations)
            max_dur = max(durations)
            if min_dur == max_dur:
                duration_str = format_duration(min_dur * 60)
            else:
                duration_str = f"{format_duration(min_dur * 60)}-{format_duration(max_dur * 60)}"
        else:
            duration_str = "?"

        # Rank - show start rank in the column, add range info to name
        rank_num = self.start_rank

        # Add range indicator to name if group spans multiple ranks
        if self.start_rank == self.end_rank:
            name_col = f"[{count} jobs]"
        else:
            name_col = f"[{count} jobs #{self.start_rank}-{self.end_rank}]"

        # Format based on width - match individual job column widths exactly
        if self.width < 100:
            # Rank(3) Priority(7) Sz(3) Time(7) User(8) Name(15)
            line = f"{rank_num:>3} {priority_num:>7} {size_indicator:<3} {duration_str:>7} {username[:8]:<8} {name_col[:15]:<15}"
        elif self.width < 140:
            # Rank(3) Priority(7) Sz(3) Time(7) ETA(14) User(10) Reason(12) Name(20)
            line = f"{rank_num:>3} {priority_num:>7} {size_indicator:<3} {duration_str:>7} {'-':<14} {username[:10]:<10} {reason[:12]:<12} {name_col[:20]:<20}"
        else:
            # Rank(3) Priority(7) Sz(3) Time(7) ETA(14) Wait(11) User(10) QOS(8) Reason(15) Resources(18) Name(25)
            line = f"{rank_num:>3} {priority_num:>7} {size_indicator:<3} {duration_str:>7} {'-':<14} {'-':>11} {username[:10]:<10} {'-':<8} {reason[:15]:<15} {'-':<18} {name_col[:25]:<25}"

        return u.AttrMap(u.Text(line), 'normal', 'normal_selected')


class ScreenViewQueue(u.WidgetWrap):
    """Queue status view - shows pending jobs from scheduler's perspective."""

    def __init__(self, main_screen, jobs):
        self.main_screen = main_screen
        self.jobs = jobs

        # Group expansion tracking
        self.expanded_groups = set()  # Set of group_keys that are expanded

        # Build UI - separate header from scrollable content
        self.header_text = u.AttrMap(u.Text(""), 'jobheader')
        self.job_walker = u.SimpleFocusListWalker([])
        self.job_listbox = u.ListBox(self.job_walker)

        # Pile: header + divider + scrollable list (with scrollbar only on listbox)
        pile = u.Pile([
            ('pack', self.header_text),
            ('pack', u.Divider("─")),
            u.ScrollBar(self.job_listbox)
        ])

        self.container = u.LineBox(
            pile,
            title="Queue Status - Pending Jobs by Priority",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        body = u.AttrMap(self.container, 'bg')

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, body)

        # Initial update
        self.update()

    def on_jobs_update(self, *_args, **_kwargs):
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        """Handle terminal resize events."""
        self.update()

    def update(self):
        """Update the queue display."""
        self.job_walker.clear()
        widgets = []

        # Get available width
        available_width = self.main_screen.width - 3 if hasattr(self.main_screen, 'width') else 120

        # Update title
        self.container.set_title("Queue Status - Pending Jobs by Priority (grouped by user)")

        # Update header (separate from scrollable content)
        if available_width < 100:
            header_text = f"{'#':>3} {'Priority':>7} {'Sz':<3} {'Time':>7} {'User':<8} {'Job Name':<15}"
        elif available_width < 140:
            header_text = f"{'#':>3} {'Priority':>7} {'Sz':<3} {'Time':>7} {'ETA':<14} {'User':<10} {'Reason':<12} {'Job Name':<20}"
        else:
            header_text = f"{'#':>3} {'Priority':>7} {'Sz':<3} {'Time':>7} {'ETA':<14} {'Waiting':>11} {'User':<10} {'QOS':<8} {'Reason':<15} {'Resources':<18} {'Job Name':<25}"

        self.header_text.original_widget.set_text(header_text)

        # Get all pending jobs
        pending_jobs = []
        for job in self.jobs.jobs:
            if hasattr(job, 'job_state') and 'PENDING' in job.job_state:
                pending_jobs.append(job)

        if not pending_jobs:
            widgets.append(u.Text(("faded", "  No pending jobs in the queue")))
        else:
            # Sort by priority (descending - highest priority first)
            def get_priority(job):
                priority = getattr(job, 'priority', {})
                if isinstance(priority, dict):
                    return priority.get('number', 0)
                return priority if isinstance(priority, int) else 0

            sorted_jobs = sorted(pending_jobs, key=get_priority, reverse=True)

            # Group consecutive jobs by (user, reason)
            groups = []
            current_group = []
            current_user = None
            current_reason = None

            for job in sorted_jobs:
                user = getattr(job, 'user_name', '?')
                reason = getattr(job, 'state_reason', 'Unknown')

                if user == current_user and reason == current_reason:
                    # Same group - add to current
                    current_group.append(job)
                else:
                    # Different group - save current and start new
                    if current_group:
                        groups.append(current_group)
                    current_group = [job]
                    current_user = user
                    current_reason = reason

            # Don't forget the last group
            if current_group:
                groups.append(current_group)

            # Create widgets from groups
            rank = 1
            for group in groups:
                group_size = len(group)
                start_rank = rank
                end_rank = rank + group_size - 1
                group_key = f"{start_rank}-{end_rank}"

                if group_size == 1:
                    # Single job - always show individually
                    widgets.append(QueueJobWidget(group[0], rank, width=available_width))
                elif group_key in self.expanded_groups:
                    # Expanded group - show all jobs individually
                    for i, job in enumerate(group):
                        widgets.append(QueueJobWidget(job, start_rank + i, width=available_width))
                else:
                    # Collapsed group - show summary
                    widgets.append(QueueGroupWidget(start_rank, end_rank, group, width=available_width))

                rank += group_size

        self.job_walker.extend(widgets)

        # Set focus on first job, or first group if no individual jobs visible
        if len(self.job_walker) > 0:
            first_job_idx = None
            first_group_idx = None

            for i, widget in enumerate(self.job_walker):
                if hasattr(widget, 'jobid') and first_job_idx is None:
                    first_job_idx = i
                    break
                elif hasattr(widget, 'group_key') and first_group_idx is None:
                    first_group_idx = i

            # Prefer individual job, fall back to group
            if first_job_idx is not None:
                self.job_walker.set_focus(first_job_idx)
            elif first_group_idx is not None:
                self.job_walker.set_focus(first_group_idx)

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        focus_w, _ = self.job_listbox.get_focus()

        # 'e' or Enter/Space to toggle expand/collapse group
        if key in ('e', 'enter', ' '):
            if hasattr(focus_w, 'group_key'):
                # Focused on a group - toggle expansion
                group_key = focus_w.group_key
                if group_key in self.expanded_groups:
                    self.expanded_groups.remove(group_key)
                else:
                    self.expanded_groups.add(group_key)
                self.update()
                return None
            elif hasattr(focus_w, 'jobid') and key in ('enter', ' '):
                # Focused on individual job - show details
                from slop.ui.overlays import JobInfoOverlay
                job = self.jobs.job_index.get(focus_w.jobid)
                if job:
                    self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
                return None

        return super().keypress(size, key)
