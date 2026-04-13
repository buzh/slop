"""Job history view showing past jobs from sacct."""

import urwid as u
import datetime
from slop.utils import format_duration, smart_truncate
from slop.ui.job_detail_sacct import JobDetailSacct


class HistoryJobWidget(u.WidgetWrap):
    """Widget for displaying a single history job in the list."""

    def __init__(self, job_data, name_width=20):
        self.job_data = job_data
        self.jobid = job_data.get('job_id')
        self.name_width = name_width

        # Extract data
        job_id = job_data.get('job_id', 'N/A')
        name_raw = job_data.get('name', 'N/A')
        # Smart truncate - preserve both start and end of job name
        name = smart_truncate(name_raw, name_width, mode='middle')
        state_info = job_data.get('state', {})
        state = ' '.join(state_info.get('current', ['UNKNOWN']))[:10]

        time_info = job_data.get('time', {})
        submission = time_info.get('submission', 0)
        elapsed = time_info.get('elapsed', 0)

        # Calculate CPU efficiency
        cpu_eff = self._calculate_cpu_efficiency(job_data)

        # Format submission date/time
        if submission:
            submit_dt = datetime.datetime.fromtimestamp(submission)
            now = datetime.datetime.now()
            # Show time for recent jobs, date for older ones
            if submit_dt.date() == now.date():
                # Today: show time only
                date_str = submit_dt.strftime("%H:%M").rjust(16)
            elif submit_dt.year == now.year:
                # This year: show month-day and time
                date_str = submit_dt.strftime("%m-%d %H:%M")
            else:
                # Previous years: show full date
                date_str = submit_dt.strftime("%Y-%m-%d %H:%M")
        else:
            date_str = "N/A".rjust(16)

        # Format runtime
        runtime_str = format_duration(elapsed) if elapsed else "N/A"

        # Exit code
        exit_code_info = job_data.get('derived_exit_code', {})
        exit_code = exit_code_info.get('return_code', {}).get('number', '?')

        # Build display text
        text = f"{str(job_id):>8s} │ {name:<{self.name_width}s} │ {state:10s} │ {date_str:16s} │ {runtime_str:>10s} │ {cpu_eff:>5s} │ {str(exit_code):>3s}"

        # Color code based on state and efficiency
        if state in ["FAILED", "TIMEOUT", "OUT_OF_MEMORY"]:
            attr = 'state_failed'
        elif state == "CANCELLED":
            attr = 'faded'
        elif state == "COMPLETED":
            if cpu_eff != "N/A":
                eff_val = float(cpu_eff.rstrip('%'))
                if eff_val >= 80:
                    attr = 'success'  # Green for good efficiency
                elif eff_val >= 50:
                    attr = 'bg'  # Normal for medium
                else:
                    attr = 'warning'  # Yellow for low efficiency
            else:
                attr = 'bg'
        else:
            attr = 'bg'

        widget = u.AttrMap(u.Text(text), attr, 'normal_selected')
        super().__init__(widget)

    @staticmethod
    def _calculate_cpu_efficiency(job_data):
        """Calculate CPU efficiency percentage."""
        time_info = job_data.get('time', {})
        elapsed = time_info.get('elapsed', 0)

        tres_alloc = {item['type']: item['count'] for item in job_data.get('tres', {}).get('allocated', [])}
        cpus_alloc = tres_alloc.get('cpu', 0)

        if cpus_alloc > 0 and elapsed > 0:
            total_time = time_info.get('total', {})
            total_cpu_sec = total_time.get('seconds', 0) + total_time.get('microseconds', 0) / 1000000
            cpu_hours_used = total_cpu_sec / 3600
            cpu_hours_alloc = (cpus_alloc * elapsed) / 3600
            cpu_efficiency = (cpu_hours_used / cpu_hours_alloc * 100) if cpu_hours_alloc > 0 else 0
            return f"{cpu_efficiency:.0f}%"
        return "N/A"

    def selectable(self):
        return True


class JobHistoryView(u.WidgetWrap):
    """View showing job history from sacct data."""

    # Sort keys map number keys to sortable fields
    SORT_KEYS = {
        '0': 'job_id',
        '1': 'name',
        '2': 'state',
        '3': 'submission',
        '4': 'elapsed',
        '5': 'efficiency',
        '6': 'exit_code',
    }

    def __init__(self, main_screen, history_data, search_type, search_value, weeks_loaded=None):
        """Initialize history view.

        Args:
            main_screen: Main screen instance
            history_data: Parsed sacct JSON data (with optional 'meta' key)
            search_type: Type of search ('user', 'account', 'node')
            search_value: Search value (username, account name, or node name)
            weeks_loaded: Number of weeks of history currently loaded (optional, calculated from meta)
        """
        self.main_screen = main_screen
        self.history_data = history_data
        self.search_type = search_type
        self.search_value = search_value
        self.jobs = history_data.get('jobs', [])

        # Extract metadata if available
        self.meta = history_data.get('meta', {})
        self.cached_jobs = self.meta.get('cached_jobs', 0)
        self.fresh_jobs = self.meta.get('fresh_jobs', 0)

        # Calculate weeks_loaded from metadata or use provided value
        if weeks_loaded is None:
            # Estimate from job timestamps
            if self.jobs:
                oldest = min(j.get('time', {}).get('submission', float('inf')) for j in self.jobs)
                newest = max(j.get('time', {}).get('submission', 0) for j in self.jobs)
                if oldest != float('inf') and newest > 0:
                    days = (newest - oldest) / 86400
                    self.weeks_loaded = max(1, int(days / 7))
                else:
                    self.weeks_loaded = None
            else:
                self.weeks_loaded = None
        else:
            self.weeks_loaded = weeks_loaded

        # Sorting state
        self.sort_col = 'state'  # Default sort by state
        self.sort_reverse = False  # Ascending (COMPLETED before FAILED)

        # Calculate column widths based on screen size
        self.calculate_column_widths()

        # Create walker and listbox
        self.walker = u.SimpleFocusListWalker([])
        self.listbox = u.ListBox(self.walker)

        # Build title with weeks info
        if self.weeks_loaded:
            weeks_text = f"last {self.weeks_loaded} weeks"
        else:
            weeks_text = "adaptive fetch"

        if search_type == 'user':
            title = f"Job History - User: {search_value} ({weeks_text})"
        elif search_type == 'account':
            title = f"Job History - Account: {search_value} ({weeks_text})"
        elif search_type == 'node':
            title = f"Job History - Node: {search_value} ({weeks_text})"
        else:
            title = f"Job History ({weeks_text})"

        self.title_text = title

        # Wrap in linebox and scrollbar
        widget = u.LineBox(
            u.ScrollBar(self.listbox),
            title=title,
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        widget = u.AttrMap(widget, 'bg')

        u.WidgetWrap.__init__(self, widget)

        # Build the view
        self.update()

    def calculate_column_widths(self):
        """Calculate responsive column widths based on screen size."""
        # Get available width (total screen minus LineBox borders and scrollbar)
        available_width = getattr(self.main_screen, 'width', 120) - 5

        # Fixed columns that don't change size
        # Format: job_id(8) + sep(3) + state(10) + sep(3) + submitted(16) + sep(3) + runtime(10) + sep(3) + eff(5) + sep(3) + exit(3)
        # = 8 + 3 + 10 + 3 + 16 + 3 + 10 + 3 + 5 + 3 + 3 = 67
        # Plus "  " prefix = 69 total fixed
        fixed_width = 69

        # Remaining width goes to name column
        self.name_width = max(available_width - fixed_width, 15)  # Minimum 15 chars

        # Cap at reasonable maximum to avoid overly wide columns
        self.name_width = min(self.name_width, 40)

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        """Handle window resize - recalculate column widths and redraw."""
        self.calculate_column_widths()
        self.update()

    def update(self):
        """Build/rebuild the job list."""
        self.walker.clear()

        if not self.jobs:
            self.walker.append(u.Text("No jobs found"))
            return

        # Calculate summary stats
        total = len(self.jobs)
        completed = sum(1 for j in self.jobs if 'COMPLETED' in j.get('state', {}).get('current', []))
        failed = sum(1 for j in self.jobs if any(s in j.get('state', {}).get('current', [])
                     for s in ['FAILED', 'TIMEOUT', 'OUT_OF_MEMORY']))
        cancelled = sum(1 for j in self.jobs if 'CANCELLED' in j.get('state', {}).get('current', []))

        # Calculate average CPU efficiency for completed jobs
        efficiencies = []
        for job in self.jobs:
            if 'COMPLETED' in job.get('state', {}).get('current', []):
                eff_str = HistoryJobWidget._calculate_cpu_efficiency(job)
                if eff_str != "N/A":
                    efficiencies.append(float(eff_str.rstrip('%')))

        avg_eff = sum(efficiencies) / len(efficiencies) if efficiencies else 0

        # Calculate total CPU hours and efficiency metrics
        total_cpu_hours = 0
        total_cpu_hours_used = 0
        time_efficiencies = []
        jobs_hit_time_limit = 0
        jobs_low_time_use = 0

        for job in self.jobs:
            time_info = job.get('time', {})
            elapsed = time_info.get('elapsed', 0)
            tres_alloc = {item['type']: item['count'] for item in job.get('tres', {}).get('allocated', [])}
            cpus = tres_alloc.get('cpu', 0)

            # Total allocated CPU hours
            total_cpu_hours += (cpus * elapsed) / 3600

            # Calculate actual CPU hours used
            if 'COMPLETED' in job.get('state', {}).get('current', []):
                total_time = time_info.get('total', {})
                total_cpu_sec = total_time.get('seconds', 0) + total_time.get('microseconds', 0) / 1000000
                total_cpu_hours_used += total_cpu_sec / 3600

                # Time efficiency (elapsed vs time limit)
                limit = job.get('time', {}).get('limit', {})
                if limit.get('set') and not limit.get('infinite') and limit['number'] > 0 and elapsed > 0:
                    time_eff = (elapsed / (limit['number'] * 60)) * 100
                    time_efficiencies.append(time_eff)

                    # Track patterns
                    if time_eff > 95:
                        jobs_hit_time_limit += 1
                    elif time_eff < 20:
                        jobs_low_time_use += 1

        # Add summary header
        self.walker.append(u.AttrMap(u.Text("SUMMARY"), 'jobheader'))
        self.walker.append(u.Divider("─"))
        self.walker.append(u.Text(f"Total Jobs: {total}"))

        # Show cache stats if available
        if self.cached_jobs > 0 or self.fresh_jobs > 0:
            cache_pct = (self.cached_jobs * 100) // total if total > 0 else 0
            self.walker.append(u.AttrMap(u.Text(f"  Cached:   {self.cached_jobs} ({cache_pct}%)"), 'faded'))
            self.walker.append(u.AttrMap(u.Text(f"  Fresh:    {self.fresh_jobs}"), 'faded'))

        self.walker.append(u.Text(f"Completed:  {completed} ({completed*100//total if total > 0 else 0}%)"))
        self.walker.append(u.Text(f"Failed:     {failed} ({failed*100//total if total > 0 else 0}%)"))
        self.walker.append(u.Text(f"Cancelled:  {cancelled} ({cancelled*100//total if total > 0 else 0}%)"))

        # CPU efficiency
        if avg_eff > 0:
            if avg_eff >= 70:
                self.walker.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_eff:.1f}%"), 'success'))
            elif avg_eff >= 50:
                self.walker.append(u.Text(f"Avg CPU Eff: {avg_eff:.1f}%"))
            else:
                self.walker.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_eff:.1f}% (Consider requesting fewer cores)"), 'warning'))

        # CPU hours
        wasted_cpu_hours = total_cpu_hours - total_cpu_hours_used
        if wasted_cpu_hours > 0 and total_cpu_hours > 0:
            waste_pct = (wasted_cpu_hours / total_cpu_hours) * 100
            self.walker.append(u.Text(f"CPU Hours:  {total_cpu_hours:.1f}h allocated, {total_cpu_hours_used:.1f}h used"))
            if waste_pct > 30:
                self.walker.append(u.AttrMap(u.Text(f"  Wasted:   {wasted_cpu_hours:.1f}h ({waste_pct:.0f}%)"), 'warning'))
            else:
                self.walker.append(u.Text(f"  Wasted:   {wasted_cpu_hours:.1f}h ({waste_pct:.0f}%)"))
        else:
            self.walker.append(u.Text(f"CPU Hours:  {total_cpu_hours:.1f}h"))

        # Time efficiency and patterns
        if time_efficiencies:
            avg_time_eff = sum(time_efficiencies) / len(time_efficiencies)
            if avg_time_eff >= 60:
                self.walker.append(u.AttrMap(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"), 'success'))
            elif avg_time_eff < 30:
                self.walker.append(u.AttrMap(u.Text(f"Avg Time Use: {avg_time_eff:.0f}% (Consider shorter time limits)"), 'warning'))
            else:
                self.walker.append(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"))

        # Pattern warnings
        if jobs_hit_time_limit > 0:
            pct = (jobs_hit_time_limit / completed) * 100 if completed > 0 else 0
            if pct > 20:
                self.walker.append(u.AttrMap(u.Text(f"⚠ {jobs_hit_time_limit} jobs hit time limit ({pct:.0f}%)"), 'warning'))

        if jobs_low_time_use > 0:
            pct = (jobs_low_time_use / completed) * 100 if completed > 0 else 0
            if pct > 30:
                self.walker.append(u.AttrMap(u.Text(f"💡 {jobs_low_time_use} jobs used <20% of time limit ({pct:.0f}%)"), 'info'))

        self.walker.append(u.Divider())

        # Add column headers
        self.walker.append(u.AttrMap(u.Text("JOB LIST"), 'jobheader'))
        self.walker.append(u.Divider("─"))

        # Build header with sort indicators (use dynamic name width)
        columns = [
            ('0', 'job_id', 'Job ID', 8),
            ('1', 'name', 'Name', self.name_width),
            ('2', 'state', 'State', 10),
            ('3', 'submission', 'Submitted', 16),
            ('4', 'elapsed', 'Runtime', 10),
            ('5', 'efficiency', 'Eff %', 5),
            ('6', 'exit_code', 'Exit', 3),
        ]

        header_parts = []
        for key, field, label, width in columns:
            # Add number prefix
            col_label = f"{key}:{label}"
            # Add sort indicator if this is the sorted column
            if self.sort_col == field:
                arrow = "▼" if self.sort_reverse else "▲"
                col_label = f"{col_label}{arrow}"
            header_parts.append(col_label.ljust(width))

        header = "  " + " │ ".join(header_parts)
        self.walker.append(u.AttrMap(u.Text(header), 'jobheader'))
        self.walker.append(u.Divider("─"))

        # Sort jobs based on current sort column
        def get_sort_key(job):
            if self.sort_col == 'job_id':
                return job.get('job_id', 0)
            elif self.sort_col == 'name':
                return job.get('name', '').lower()
            elif self.sort_col == 'state':
                state = ' '.join(job.get('state', {}).get('current', ['UNKNOWN']))
                # Order: RUNNING, PENDING, COMPLETED, FAILED, CANCELLED, OTHER
                state_order = {'RUNNING': 0, 'PENDING': 1, 'COMPLETED': 2, 'FAILED': 3,
                              'TIMEOUT': 4, 'OUT_OF_MEMORY': 5, 'CANCELLED': 6}
                return state_order.get(state, 99)
            elif self.sort_col == 'submission':
                return job.get('time', {}).get('submission', 0)
            elif self.sort_col == 'elapsed':
                return job.get('time', {}).get('elapsed', 0)
            elif self.sort_col == 'efficiency':
                # Calculate efficiency for sorting
                eff_str = HistoryJobWidget._calculate_cpu_efficiency(job)
                if eff_str == "N/A":
                    return -1  # Sort N/A to end
                return float(eff_str.rstrip('%'))
            elif self.sort_col == 'exit_code':
                return job.get('derived_exit_code', {}).get('return_code', {}).get('number', 0)
            else:
                return 0

        sorted_jobs = sorted(self.jobs, key=get_sort_key, reverse=self.sort_reverse)

        # Add job widgets
        for job in sorted_jobs:
            self.walker.append(HistoryJobWidget(job, name_width=self.name_width))

        # Set focus to top of list (urwid will skip to first selectable on arrow press)
        if len(self.walker) > 0:
            self.walker.set_focus(0)

    def keypress(self, size, key):
        if key == 'esc' or key == 'q':
            # Go back to main view
            self.main_screen._show_screen(1, self.main_screen.screen_users, "Job Queue - Users", 'users')
            return None

        if key == 'enter' or key == ' ':
            # Show detail for selected job
            focus_w, _ = self.walker.get_focus()
            if hasattr(focus_w, 'jobid') and hasattr(focus_w, 'job_data'):
                detail_overlay = JobDetailSacct(focus_w.job_data, self.main_screen)
                self.main_screen.open_overlay(detail_overlay)
            return None

        # Number keys: sort by column
        if key in self.SORT_KEYS:
            selected_col = self.SORT_KEYS[key]
            if self.sort_col == selected_col:
                # Toggle sort direction if already sorting by this column
                self.sort_reverse = not self.sort_reverse
            else:
                # New column - set default direction
                self.sort_col = selected_col
                # Default directions for different columns
                if selected_col in ['submission', 'job_id']:
                    self.sort_reverse = True  # Newest/highest first
                elif selected_col in ['efficiency', 'elapsed']:
                    self.sort_reverse = True  # Highest first
                else:
                    self.sort_reverse = False  # Ascending
            self.update()
            return None

        return super().keypress(size, key)
