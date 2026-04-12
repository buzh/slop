"""Job history view showing past jobs from sacct."""

import urwid as u
import datetime
from slop.utils import format_duration
from slop.ui.job_detail_sacct import JobDetailSacct


class HistoryJobWidget(u.WidgetWrap):
    """Widget for displaying a single history job in the list."""

    def __init__(self, job_data):
        self.job_data = job_data
        self.jobid = job_data.get('job_id')

        # Extract data
        job_id = job_data.get('job_id', 'N/A')
        name = job_data.get('name', 'N/A')[:20]  # Truncate long names
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
        text = f"{str(job_id):>8s} │ {name:20s} │ {state:10s} │ {date_str:16s} │ {runtime_str:>10s} │ {cpu_eff:>5s} │ {str(exit_code):>3s}"

        # Color code based on state and efficiency
        if state in ["FAILED", "TIMEOUT", "OUT_OF_MEMORY"]:
            attr = 'failed'
        elif state == "CANCELLED":
            attr = 'faded'
        elif state == "COMPLETED":
            if cpu_eff != "N/A":
                eff_val = float(cpu_eff.rstrip('%'))
                if eff_val >= 80:
                    attr = 'running'  # Green for good efficiency
                elif eff_val >= 50:
                    attr = 'bg'  # Normal for medium
                else:
                    attr = 'pending'  # Yellow for low efficiency
            else:
                attr = 'bg'
        else:
            attr = 'bg'

        widget = u.AttrMap(u.Text(text), attr, 'jobid_selected')
        super().__init__(widget)

    def _calculate_cpu_efficiency(self, job_data):
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
        widget = u.LineBox(u.ScrollBar(self.listbox), title=title)
        widget = u.AttrMap(widget, 'bg')

        u.WidgetWrap.__init__(self, widget)

        # Build the view
        self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

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
                eff_str = HistoryJobWidget(job)._calculate_cpu_efficiency(job)
                if eff_str != "N/A":
                    efficiencies.append(float(eff_str.rstrip('%')))

        avg_eff = sum(efficiencies) / len(efficiencies) if efficiencies else 0

        # Calculate total CPU hours
        total_cpu_hours = 0
        for job in self.jobs:
            time_info = job.get('time', {})
            elapsed = time_info.get('elapsed', 0)
            tres_alloc = {item['type']: item['count'] for item in job.get('tres', {}).get('allocated', [])}
            cpus = tres_alloc.get('cpu', 0)
            total_cpu_hours += (cpus * elapsed) / 3600

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
        if avg_eff > 0:
            if avg_eff >= 70:
                self.walker.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_eff:.1f}%"), 'running'))
            elif avg_eff >= 50:
                self.walker.append(u.Text(f"Avg CPU Eff: {avg_eff:.1f}%"))
            else:
                self.walker.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_eff:.1f}% (Consider requesting fewer cores)"), 'pending'))
        self.walker.append(u.Text(f"CPU Hours:  {total_cpu_hours:.1f}h"))
        self.walker.append(u.Divider())

        # Add column headers
        self.walker.append(u.AttrMap(u.Text("JOB LIST"), 'jobheader'))
        self.walker.append(u.Divider("─"))
        header = "  Job ID │ Name                 │ State      │ Submitted        │    Runtime │ Eff % │ Exit"
        self.walker.append(u.AttrMap(u.Text(header), 'jobheader'))
        self.walker.append(u.Divider("─"))

        # Sort jobs by submission time (newest first)
        sorted_jobs = sorted(self.jobs, key=lambda j: j.get('time', {}).get('submission', 0), reverse=True)

        # Add job widgets
        for job in sorted_jobs:
            self.walker.append(HistoryJobWidget(job))

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

        return super().keypress(size, key)
