"""My Jobs view - personalized view showing current user's jobs with detailed info."""
import urwid as u
import datetime
import os
from slop.models import Jobs
from slop.slurm import *
from slop.utils import *
from slop.ui.widgets import *
from slop.ui.views import JobInfoOverlay


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
        """Truncate text with ellipsis if needed."""
        if len(text) <= max_len:
            return text
        return text[:max_len-1] + '…'

    def _build_columns(self):
        """Build aligned columns for the job."""
        job = self.job
        state = job.job_state[0] if job.job_state else 'UNKNOWN'

        # State icons
        icons = {
            'RUNNING': '▶',
            'COMPLETING': '▶',
            'PENDING': '⏸',
            'COMPLETED': '✓',
            'FAILED': '✗',
            'TIMEOUT': '⏱',
            'CANCELLED': '⊗',
            'OUT_OF_MEMORY': '⚠',
        }
        icon = icons.get(state, '•')

        if state in ['RUNNING', 'COMPLETING']:
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
        time_text = ''
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
        resources = self._get_compact_resources(job)

        # Node
        node = job.nodes if job.nodes else '?'

        # Format based on available width
        if self.width < 90:
            # Narrow: just essentials
            name = self._truncate(job.name, 15)
            line = f"{icon} {job_id:>7} {name:<17} {elapsed_str if elapsed_str else 'N/A':>7}"
        elif self.width < 120:
            # Medium: add time progress
            name = self._truncate(job.name, 18)
            line = f"{icon} {job_id:>7} {name:<20} {time_text:<30}"
        else:
            # Wide: full detail
            name = self._truncate(job.name, 22)
            line = f"{icon} {job_id:>7} {name:<22} {time_text:<30} {resources:<12} {node}"

        return u.Text(line)

    def _pending_columns(self, icon):
        """Single-line formatted text for pending job."""
        job = self.job

        # Build parts
        job_id = str(job.job_id)

        # Wait time
        wait_text = ''
        if job.submit_time and job.submit_time.get('set'):
            submit = datetime.datetime.fromtimestamp(job.submit_time['number'])
            now = datetime.datetime.now()
            wait_sec = int((now - submit).total_seconds())
            wait_str = format_duration(wait_sec)
            wait_text = f"{wait_str} wait"

        # Reason
        reason = getattr(job, 'state_reason', '?')
        reason_short = self._abbreviate_reason(reason)

        # Resources
        resources = self._get_compact_resources(job)

        # Format based on available width
        if self.width < 90:
            # Narrow: essentials only
            name = self._truncate(job.name, 15)
            line = f"{icon} {job_id:>7} {name:<17} {reason_short:<8}"
        elif self.width < 120:
            # Medium
            name = self._truncate(job.name, 18)
            line = f"{icon} {job_id:>7} {name:<20} {wait_text:<14} {reason_short:<10}"
        else:
            # Wide: full detail
            name = self._truncate(job.name, 22)
            line = f"{icon} {job_id:>7} {name:<22} {wait_text:<14} {reason_short:<10} {resources:<12} {job.partition}"

        return u.Text(line)

    def _completed_columns(self, icon, state):
        """Single-line formatted text for completed/failed job."""
        job = self.job

        # Build parts
        job_id = str(job.job_id)

        # Runtime
        runtime_text = ''
        if job.start_time and job.start_time.get('set') and job.end_time and job.end_time.get('set'):
            start = datetime.datetime.fromtimestamp(job.start_time['number'])
            end = datetime.datetime.fromtimestamp(job.end_time['number'])
            runtime = int((end - start).total_seconds())
            runtime_text = format_duration(runtime)

        # Exit code
        exit_text = ''
        if hasattr(job, 'returncode'):
            exit_text = f"exit:{job.returncode}"

        # Resources
        resources = self._get_compact_resources(job)

        # Format based on available width
        if self.width < 90:
            # Narrow: essentials only
            name = self._truncate(job.name, 15)
            line = f"{icon} {job_id:>7} {name:<17} {exit_text:<12}"
        elif self.width < 120:
            # Medium
            name = self._truncate(job.name, 18)
            line = f"{icon} {job_id:>7} {name:<20} {runtime_text:<12} {exit_text:<15}"
        else:
            # Wide: full detail
            name = self._truncate(job.name, 22)
            line = f"{icon} {job_id:>7} {name:<22} {runtime_text:<12} {exit_text:<15} {resources}"

        return u.Text(line)

    def _get_compact_resources(self, job):
        """Extract compact resource string: 16c 64G"""
        parts = []

        # Parse TRES
        tres_str = job.tres_alloc_str if job.tres_alloc_str else job.tres_req_str
        if tres_str:
            tres_dict = {}
            for item in tres_str.split(','):
                if '=' in item:
                    key, val = item.split('=', 1)
                    tres_dict[key] = val

            # CPU
            if 'cpu' in tres_dict:
                parts.append(f"{tres_dict['cpu']}c")

            # Memory (abbreviated)
            if 'mem' in tres_dict:
                mem = tres_dict['mem']
                # Simplify memory display
                if mem.endswith('G'):
                    parts.append(mem)
                elif mem.endswith('M'):
                    parts.append(mem)
                else:
                    parts.append(mem)

            # GPU
            if 'gres/gpu' in tres_dict:
                parts.append(f"{tres_dict['gres/gpu']}gpu")

        return ' '.join(parts) if parts else ''

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
        return abbrev.get(reason, reason[:10] if len(reason) > 10 else reason)


class ScreenViewMyJobs(u.WidgetWrap):
    """View showing current user's jobs with detailed information."""

    def __init__(self, main_screen, jobs, username=None):
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
            'OTHER': True
        }
        self.calculate_jobs_per_section()

        self.job_walker = u.SimpleFocusListWalker([])
        self.job_listbox = u.ListBox(self.job_walker)

        # Build UI
        self.container = u.LineBox(
            u.ScrollBar(self.job_listbox),
            title=f"My Jobs ({self.username}) - Press 'e' to expand/collapse sections",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        body = u.AttrMap(self.container, 'bg')

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
            # Available height, accounting for potential number of sections
            # Each section needs: header (1) + divider (1) = 2 overhead lines
            # With 5 potential sections, that's 10 overhead lines
            # Reserve some space for navigation
            available = max(self.main_screen.height - 10, 5)
            # Distribute evenly across sections, minimum 3
            self.jobs_per_section = max(available // 5, 3)
        else:
            self.jobs_per_section = 5  # Default fallback

    def on_resize(self):
        """Handle resize events - recalculate layout and redraw."""
        self.calculate_jobs_per_section()
        self.update()

    def has_jobs(self):
        """Check if current user has any jobs."""
        user_jobs = self.jobs.get_user_jobs(self.username)
        return user_jobs is not None and sum(len(jobs) for jobs in user_jobs.values()) > 0

    def update(self):
        """Update the job list."""
        user_jobs = self.jobs.get_user_jobs(self.username)

        if not user_jobs:
            # No jobs for this user
            self.job_walker.clear()
            self.job_walker.append(u.Text(("faded", "  No jobs found")))
            return

        self.job_walker.clear()
        widgets = []

        # Get available width (full screen minus LineBox borders)
        available_width = self.main_screen.width - 3 if hasattr(self.main_screen, 'width') else 80

        # Icons for state headers
        state_icons = {
            'RUNNING': '▶ RUNNING',
            'PENDING': '⏸ PENDING',
            'COMPLETED': '✓ COMPLETED',
            'FAILED': '✗ FAILED/TIMEOUT',
            'OTHER': '• OTHER'
        }

        # Show jobs grouped by state - most important first
        for state in ['RUNNING', 'PENDING', 'FAILED', 'COMPLETED', 'OTHER']:
            state_jobs = user_jobs.get(state, [])
            if state_jobs:
                total_count = len(state_jobs)
                collapsed = self.collapsed_sections.get(state, True)

                # Header with expand/collapse indicator
                expand_indicator = '▼' if not collapsed else '▶'
                header_text = f"{expand_indicator} {state_icons[state]} ({total_count})"
                widgets.append(u.Text(('jobheader', header_text)))

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

                # Only add divider between sections
                widgets.append(u.Divider())

        if widgets:
            self.job_walker.extend(widgets)
        else:
            self.job_walker.append(u.Text(("faded", "  No jobs to display")))

        # Set focus to top of list (urwid will skip to first selectable on arrow press)
        if len(self.job_walker) > 0:
            self.job_walker.set_focus(0)

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        # 'e' to toggle expand/collapse all sections
        if key == 'e':
            # Check if any section is collapsed
            any_collapsed = any(self.collapsed_sections.values())
            # Toggle all to opposite state
            new_state = not any_collapsed
            for section in self.collapsed_sections:
                self.collapsed_sections[section] = new_state
            self.update()
            return None

        # Enter to show full job info
        if key == 'enter' or key == ' ':
            focus_w, _ = self.job_walker.get_focus()
            if hasattr(focus_w, 'jobid'):
                job = self.jobs.job_index.get(focus_w.jobid)
                if job:
                    self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
            return None

        return super().keypress(size, key)
