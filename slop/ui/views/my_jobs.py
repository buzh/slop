"""My Jobs view - personalized view showing current user's jobs with detailed info."""
import urwid as u
import datetime
import os
from slop.models import Jobs
from slop.utils import format_duration, smart_truncate, compact_tres
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.overlays import JobInfoOverlay
from slop.ui.widgets import SafeListBox, SectionHeader, rounded_box
from slop.ui.state_style import state_icon, width_tier
from slop.slurm.history_fetcher import HistoryFetcher


class MyJobDetailWidget(u.WidgetWrap):
    """Compact widget for a single job in My Jobs view with responsive width."""

    def __init__(self, job, width=None):
        self.job = job
        self.jobid = job.job_id
        self.width = width or 80  # Default to 80 columns if not specified

        # Build aligned columns
        columns = self._build_columns()

        # Make selectable and add proper highlighting
        widget = u.AttrMap(columns, None, 'normal_selected')

        super().__init__(widget)

    def selectable(self):
        """Make widget selectable for arrow key navigation."""
        return True

    def keypress(self, size, key):
        """Allow navigation keys to pass through."""
        return key

    def _truncate(self, text, max_len):
        """Truncate text with ellipsis preserving both start and end."""
        return smart_truncate(text, max_len, mode='middle')

    def _build_columns(self):
        """Build aligned columns for the job."""
        job = self.job
        state = job.job_state[0] if job.job_state else 'UNKNOWN'
        icon = state_icon(state, style='detail')

        if state in ('RUNNING', 'COMPLETING'):
            return self._running_columns(icon)
        elif state == 'PENDING':
            return self._pending_columns(icon)
        else:
            return self._completed_columns(icon, state)

    def _running_columns(self, icon):
        """Single-line formatted text for running job."""
        job = self.job

        # Build parts
        job_id = str(job.job_id)

        # Time progress
        time_text = EMPTY_PLACEHOLDER
        elapsed_str = EMPTY_PLACEHOLDER
        if job.start_time and job.start_time.get('set') and job.time_limit and job.time_limit.get('set'):
            start = datetime.datetime.fromtimestamp(job.start_time['number'])
            now = datetime.datetime.now()
            elapsed = int((now - start).total_seconds())
            limit_sec = job.time_limit['number'] * 60

            elapsed_str = format_duration(elapsed).rjust(7)
            limit_str = format_duration(limit_sec).ljust(7)
            progress = min(100, int((elapsed / limit_sec) * 100))

            bar_width = 8
            filled = int((progress / 100) * bar_width)
            bar = '█' * filled + '░' * (bar_width - filled)
            time_text = f"{elapsed_str}/{limit_str}[{bar}]"

        # Resources
        resources = compact_tres(job) or EMPTY_PLACEHOLDER

        # Node
        node = job.nodes if job.nodes else EMPTY_PLACEHOLDER

        tier = width_tier(self.width)
        if tier == 'narrow':
            name = self._truncate(job.name, 15)
            line = f"{icon} {job_id:>7} {name:<17} {elapsed_str:>7}"
        elif tier == 'medium':
            name = self._truncate(job.name, 18)
            line = f"{icon} {job_id:>7} {name:<20} {time_text:<30}"
        else:
            name = self._truncate(job.name, 22)
            line = f"{icon} {job_id:>7} {name:<22} {time_text:<30} {resources:<12} {node}"

        return u.Text(line)

    def _pending_columns(self, icon):
        """Single-line formatted text for pending job."""
        job = self.job

        # Build parts
        job_id = str(job.job_id)

        # Wait time
        wait_text = EMPTY_PLACEHOLDER
        if job.submit_time and job.submit_time.get('set'):
            submit = datetime.datetime.fromtimestamp(job.submit_time['number'])
            now = datetime.datetime.now()
            wait_sec = int((now - submit).total_seconds())
            wait_str = format_duration(wait_sec)
            wait_text = f"{wait_str} wait"

        # Reason
        reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER)
        reason_short = self._abbreviate_reason(reason)

        # Resources
        resources = compact_tres(job) or EMPTY_PLACEHOLDER

        tier = width_tier(self.width)
        if tier == 'narrow':
            name = self._truncate(job.name, 15)
            line = f"{icon} {job_id:>7} {name:<17} {reason_short:<8}"
        elif tier == 'medium':
            name = self._truncate(job.name, 18)
            line = f"{icon} {job_id:>7} {name:<20} {wait_text:<14} {reason_short:<10}"
        else:
            name = self._truncate(job.name, 22)
            line = f"{icon} {job_id:>7} {name:<22} {wait_text:<14} {reason_short:<10} {resources:<12} {job.partition}"

        return u.Text(line)

    def _completed_columns(self, icon, state):
        """Single-line formatted text for completed/failed job."""
        job = self.job

        # Build parts
        job_id = str(job.job_id)

        # Runtime
        runtime_text = EMPTY_PLACEHOLDER
        if job.start_time and job.start_time.get('set') and job.end_time and job.end_time.get('set'):
            start = datetime.datetime.fromtimestamp(job.start_time['number'])
            end = datetime.datetime.fromtimestamp(job.end_time['number'])
            runtime = int((end - start).total_seconds())
            runtime_text = format_duration(runtime)

        # Exit code
        exit_text = EMPTY_PLACEHOLDER
        if hasattr(job, 'returncode'):
            exit_text = f"exit:{job.returncode}"

        # Resources
        resources = compact_tres(job) or EMPTY_PLACEHOLDER

        tier = width_tier(self.width)
        if tier == 'narrow':
            name = self._truncate(job.name, 15)
            line = f"{icon} {job_id:>7} {name:<17} {exit_text:<12}"
        elif tier == 'medium':
            name = self._truncate(job.name, 18)
            line = f"{icon} {job_id:>7} {name:<20} {runtime_text:<12} {exit_text:<15}"
        else:
            name = self._truncate(job.name, 22)
            line = f"{icon} {job_id:>7} {name:<22} {runtime_text:<12} {exit_text:<15} {resources}"

        return u.Text(line)

    def _abbreviate_reason(self, reason):
        """Abbreviate common reasons."""
        abbrev = {
            'Resources': 'Rsrc',
            'Dependency': 'Dep',
            'Priority': 'Prio',
            'PartitionTimeLimit': 'TimeLim',
            'QOSMaxJobsPerUserLimit': 'QOSLim',
            'AssocGrpCpuLimit': 'CPULim',
            'ReqNodeNotAvail': 'NodeN/A',
            'AssocMaxJobsLimit': 'JobLim',
        }
        return abbrev.get(reason, smart_truncate(reason, 10, mode='middle'))


class ScreenViewMyJobs(u.WidgetWrap):
    """View showing current user's jobs with detailed information."""

    def __init__(self, main_screen, jobs, username=None, adaptive_sacct=None):
        self.jobs = jobs
        self.main_screen = main_screen

        # Detect current user or use provided username
        self.username = username or os.getenv('USER') or os.getenv('USERNAME') or 'unknown'

        # Collapsed/expanded state per section
        self.collapsed_sections = {
            'RUNNING': True,
            'PENDING': True,
            'COMPLETED': True,
            'FAILED': True,
            'OTHER': True,
        }
        self.calculate_jobs_per_section()

        # History fetcher (common with report view)
        self.history_fetcher = HistoryFetcher(main_screen, adaptive_sacct) if adaptive_sacct else None
        self.history_status = None  # Status text for history section
        if self.history_fetcher:
            self.history_fetcher.on_progress = self._on_history_progress
            self.history_fetcher.on_complete = self._on_history_complete

        # === LEFT PANEL: Current Jobs ===
        self.current_jobs_walker = u.SimpleFocusListWalker([])
        self.current_jobs_listbox = SafeListBox(self.current_jobs_walker)

        left_panel = rounded_box(u.ScrollBar(self.current_jobs_listbox), title='Current Jobs')

        # === RIGHT PANEL: Job History ===
        self.history_walker = u.SimpleFocusListWalker([])
        self.history_listbox = SafeListBox(self.history_walker)

        right_panel = rounded_box(u.ScrollBar(self.history_listbox), title='Job History')

        # === TWO COLUMN LAYOUT ===
        self.columns = u.Columns([
            ('weight', 40, left_panel),
            ('weight', 60, right_panel),
        ], dividechars=1)

        # Set focus to left panel (current jobs) initially
        self.columns.set_focus_column(0)

        body = u.AttrMap(self.columns, 'bg')

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, body)

    def on_jobs_update(self, *_args, **_kwargs):
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def calculate_jobs_per_section(self):
        """Calculate how many jobs to show per section based on available height."""
        if hasattr(self.main_screen, 'height'):
            # Reserve space for section headers (up to 5 sections × 2 lines each)
            # Use remaining space for jobs, distributed across sections
            available = max(self.main_screen.height - 12, 5)
            # Distribute across typical 2-3 active sections (not all 5)
            self.jobs_per_section = max(available // 3, 5)
        else:
            self.jobs_per_section = 5  # Default fallback

    def on_resize(self):
        """Handle resize events - recalculate layout and redraw."""
        self.calculate_jobs_per_section()
        self.update()

    def has_jobs(self):
        """Check if current user has any jobs in the queue (any state)."""
        # Direct check - count any job belonging to this user
        # Use case-insensitive comparison and strip whitespace for robustness
        username_normalized = self.username.strip().lower()
        return any(
            hasattr(job, 'user_name') and
            job.user_name.strip().lower() == username_normalized
            for job in self.jobs.jobs
        )

    def update(self):
        """Update the job lists (both current and history).

        Also kicks an initial sacct fetch if one hasn't been started yet.
        Kicking belongs here (the orchestrator), not in `_update_history_jobs`,
        because that render runs from `_on_history_complete` too — and re-kicking
        from the failure-render path would tight-loop on users with no history
        or a sacct outage.
        """
        self._update_current_jobs()
        self._update_history_jobs()
        if self.history_fetcher and not self.history_fetcher.fetch_started and self.is_active():
            self.history_fetcher.start_fetch('user', self.username)

    def _update_current_jobs(self):
        """Update the left panel with current jobs."""
        user_jobs = self.jobs.get_user_jobs(self.username)

        self.current_jobs_walker.clear()
        widgets = []

        # Get available width (40% of screen for left panel)
        available_width = int(self.main_screen.width * 0.40) - 3 if hasattr(self.main_screen, 'width') else 50

        # 'FAILED' bucket also covers TIMEOUT; the header keeps the legacy label.
        section_labels = {'FAILED': 'FAILED/TIMEOUT'}

        # Show jobs grouped by state - most important first (only if user has current jobs)
        if user_jobs:
            for state in ['RUNNING', 'PENDING', 'FAILED', 'COMPLETED', 'OTHER']:
                state_jobs = user_jobs.get(state, [])
                if state_jobs:
                    total_count = len(state_jobs)
                    collapsed = self.collapsed_sections.get(state, True)

                    # Modern section header with expand/collapse indicator
                    expand_indicator = '▼' if not collapsed else '▶'
                    label = section_labels.get(state, state)
                    icon = state_icon(state, style='detail')
                    widgets.append(SectionHeader(f"{expand_indicator} {icon} {label} ({total_count})"))

                    # Sort jobs
                    sorted_jobs = sorted(state_jobs, key=lambda j: j.job_id, reverse=True)

                    if collapsed and total_count > self.jobs_per_section:
                        # Show first N jobs
                        for job in sorted_jobs[:self.jobs_per_section]:
                            widgets.append(MyJobDetailWidget(job, width=available_width))
                        # Show "... and X more" message
                        remaining = total_count - self.jobs_per_section
                        widgets.append(u.Text(('faded', f"  ... and {remaining} more (press 'e' to expand)")))
                    else:
                        # Show all jobs
                        for job in sorted_jobs:
                            widgets.append(MyJobDetailWidget(job, width=available_width))

                    # Add blank line between sections for visual separation
                    widgets.append(u.Divider())
        else:
            # No current jobs - show a message
            widgets.append(u.Text(("faded", "  No current jobs in the queue")))

        if widgets:
            self.current_jobs_walker.extend(widgets)
        else:
            self.current_jobs_walker.append(u.Text(("faded", "  No jobs to display")))

        # Set focus to top of list
        if len(self.current_jobs_walker) > 0:
            self.current_jobs_walker.set_focus(0)

    def _update_history_jobs(self):
        """Update the right panel with history jobs."""
        self.history_walker.clear()
        widgets = []

        # Get available width (60% of screen for right panel)
        available_width = int(self.main_screen.width * 0.60) - 3 if hasattr(self.main_screen, 'width') else 70

        if self.history_fetcher:
            if self.history_fetcher.loading:
                msg = self.history_status or "Loading job history..."
                widgets.append(u.Text(('faded', f"  {msg}")))
            elif self.history_fetcher.history_jobs:
                for job in self.history_fetcher.history_jobs:
                    widgets.append(MyJobDetailWidget(job, width=available_width))
            elif self.history_status:
                # Fetch completed without data — success-empty or failure.
                widgets.append(u.Text(('faded', f"  {self.history_status}")))
            else:
                widgets.append(u.Text(('faded', "  History will load when viewing this screen")))
        else:
            widgets.append(u.Text(('faded', "  History not available")))

        if widgets:
            self.history_walker.extend(widgets)

        # Set focus to top of list
        if len(self.history_walker) > 0:
            self.history_walker.set_focus(0)

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        # 'e' to toggle expand/collapse all sections (only works in left panel)
        if key == 'e':
            current_focus = self.columns.get_focus_column()
            if current_focus == 0:  # Only in left panel (current jobs)
                # Check if any section is collapsed
                any_collapsed = any(self.collapsed_sections.values())
                # Toggle all to opposite state
                new_state = not any_collapsed
                for section in self.collapsed_sections:
                    self.collapsed_sections[section] = new_state
                self._update_current_jobs()  # Only update current jobs panel
                return None

        # Enter to show full job info
        if key == 'enter' or key == ' ':
            # Check which panel is focused
            current_focus = self.columns.get_focus_column()

            if current_focus == 0:
                # Left panel - current jobs
                focus_w, _ = self.current_jobs_listbox.get_focus()
                if hasattr(focus_w, 'jobid'):
                    job = self.jobs.job_index.get(focus_w.jobid)
                    if job:
                        self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
            else:
                # Right panel - history jobs
                focus_w, _ = self.history_listbox.get_focus()
                if hasattr(focus_w, 'jobid') and self.history_fetcher:
                    job = next((j for j in self.history_fetcher.history_jobs if j.job_id == focus_w.jobid), None)
                    if job:
                        self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
            return None

        return super().keypress(size, key)

    def _on_history_progress(self, status):
        """Handle progress updates from history fetcher."""
        _, detail = self.history_fetcher.get_progress_text(status)
        self.history_status = detail

        # Redraw to show updated status
        if self.is_active():
            self._update_history_jobs()

    def _on_history_complete(self, history_jobs, meta):
        """Handle history fetch completion."""
        if history_jobs:
            self.history_status = None
        else:
            self.history_status = "No historical jobs found" if meta else "Failed to fetch history"

        # Redraw to show history
        if self.is_active():
            self._update_history_jobs()
