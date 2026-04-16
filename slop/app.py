import urwid as u
import asyncio
import os
from slop.models import Jobs
from slop.slurm.fetcher import SlurmJobFetcher
from slop.slurm.cluster_fetcher import SlurmClusterFetcher
from slop.slurm.sreport_fetcher import SreportFetcher
from slop.slurm.adaptive_sacct import AdaptiveSacctFetcher
from slop.ui.widgets import Header, Footer, GenericOverlayText
from slop.ui.views import (
    ScreenViewUsers,
    ScreenViewAccounts,
    ScreenViewPartitions,
    ScreenViewStates,
    ScreenViewCluster,
    ConfirmExit,
)
from slop.ui.myjobsview import ScreenViewMyJobs
from slop.ui.search_overlay import SearchOverlay
from slop.ui.report_view import ReportView
from slop.ui.queue_view import ScreenViewQueue
from slop import __version__

class SC(u.WidgetWrap):
    """Main screen controller for slop TUI."""

    def __init__(self):
        palette = {
            # UI Chrome
            ("header",           "white, bold", "dark blue"),
            ("footer",           "white, bold", "dark red"),
            ("jobheader",        "white, bold", "dark cyan"),
            ("buttons",          "yellow",      "black"),
            ("buttons_selected", "white",       "dark red"),

            # Default/Background
            ("bg",               "white",       "black"),
            ("normal",           "white",       "black"),
            ("normal_selected",  "black",       "yellow"),
            ("faded",            "light gray",  "black"),

            # Job States (explicit naming)
            ("state_running",    "light green", "black"),
            ("state_pending",    "yellow",      "black"),
            ("state_failed",     "light red",   "black"),

            # Performance/Health (separate from job states)
            ("success",          "light green", "black"),
            ("warning",          "yellow",      "black"),
            ("error",            "light red",   "black"),
            ("info",             "light cyan",  "black"),
        }

        # Event loop and fetchers
        self.asyncloop = u.AsyncioEventLoop()
        self.loop = u.MainLoop(self, palette, event_loop=self.asyncloop, unhandled_input=unhandled_input)
        self.jobfetcher = SlurmJobFetcher(loop=self.asyncloop._loop)
        self.cluster_fetcher = SlurmClusterFetcher(loop=self.asyncloop._loop)
        self.sreport_fetcher = SreportFetcher()
        self.adaptive_sacct = AdaptiveSacctFetcher()
        self.jobs = Jobs(self.jobfetcher.fetch_sync())
        self.refreshing = False

        # UI components
        self.header = Header(self)
        self.footer = Footer(self)
        self.confirmexit = ConfirmExit(self)

        # State tracking
        self.overlay_showing = False
        self.current_view = 1  # 0=my_jobs, 1=users, 2=accounts, 3=partitions, 4=states, 5=cluster, 6=report, 7=queue
        self.last_f1_view = None  # Last F1 view shown (0 or 1)

        col_rows = u.raw_display.Screen().get_cols_rows()
        self.width = col_rows[0]
        self.height = col_rows[1] - 4  # Subtract header/footer space

        # Create views
        self.current_username = os.getenv('USER') or os.getenv('USERNAME') or 'unknown'
        self.screen_my_jobs = ScreenViewMyJobs(self, self.jobs, self.current_username, self.adaptive_sacct)
        self.screen_users = ScreenViewUsers(self, self.jobs)
        self.screen_accounts = ScreenViewAccounts(self, self.jobs)
        self.screen_partitions = ScreenViewPartitions(self, self.jobs)
        self.screen_states = ScreenViewStates(self, self.jobs)
        self.screen_cluster = ScreenViewCluster(self, self.cluster_fetcher)
        self.screen_queue = ScreenViewQueue(self, self.jobs)
        self.screen_report = None  # Created on demand when searching
        self.body = u.AttrMap(self.screen_users, 'bg')
        self.frame = u.Frame(header=self.header, body=self.body, footer=self.footer)

        # Handle window resize (if supported by the event loop)
        try:
            u.connect_signal(self.loop.screen, 'resize', self.on_resize)
        except (NameError, AttributeError):
            pass  # Resize signal not available in this event loop

        super().__init__(self.frame)

        # Show initial screen: My Jobs if user has jobs, otherwise All Users
        if self.screen_my_jobs.has_jobs():
            self.show_screen_my_jobs()
        else:
            self.show_screen_users()

        self.show_splash_screen()

    async def auto_refresh(self, *_args):
        """Refresh job and cluster data, update current view."""
        if self.refreshing:
            return
        self.refreshing = True

        try:
            await self.jobfetcher.update_once()
            slurm_job_data = await self.jobfetcher.fetch()
            self.jobs.update_slurmdata(slurm_job_data)
            await self.cluster_fetcher.fetch()

            # Update current screen
            screens = [
                self.screen_my_jobs, self.screen_users, self.screen_accounts,
                self.screen_partitions, self.screen_states, self.screen_cluster
            ]
            if 0 <= self.current_view < len(screens):
                screens[self.current_view].update()

        finally:
            self.refreshing = False
            self.loop.set_alarm_in(3, lambda loop, user_data: asyncio.create_task(self.auto_refresh()))


    def show_app_info(self):
        """Display application information and keyboard shortcuts overlay."""
        fetch_duration = self.jobfetcher.last_fetch_duration.total_seconds()

        # Determine current view name for context-specific help
        view_names = {
            0: "My Jobs",
            1: "Users",
            2: "Accounts",
            3: "Partitions",
            4: "States",
            5: "Cluster",
            6: "History",
            7: "Queue"
        }
        current_view_name = view_names.get(self.current_view, "Unknown")

        help_text = [
            ("jobheader", f"  Slurm Top (slop) {__version__} - Keyboard Shortcuts  "),
            "",
            ("success", "NAVIGATION (F-Keys)"),
            "  F1       My Jobs / All Users (toggle)",
            "  F2       Accounts view",
            "  F3       Partitions view",
            "  F4       Job States view",
            "  F5       Cluster Resources",
            "  F6       History / User Report",
            "  F7       Queue Status",
            "",
            ("success", "COMMON ACTIONS"),
            "  /        Search (job ID, user, account, or node)",
            "  ?        Show this help",
            "  q        Quit (with confirmation)",
            "  Esc      Close overlay / Go back",
            "",
            ("success", "MY JOBS / USERS / ACCOUNTS / PARTITIONS / STATES"),
            "  h        View history for selected user (Users view only)",
            "  e        Expand/collapse job groups",
            "  Enter    Show job details",
            "  Space    Expand array job",
            "  Tab      Switch between left/right panels",
            "  0-6      Sort by column (0=state, 1=id, 2=start, etc.)",
            "",
            ("success", "HISTORY VIEW (F6)"),
            "  Enter    Show job details",
            "  Tab      Autocomplete usernames",
            "  Esc      Return to previous view",
            "",
            ("success", "QUEUE VIEW (F7)"),
            "  e        Expand/collapse grouped jobs",
            "  Enter    Show job details or expand group",
            "",
            ("success", "SEARCH (/)"),
            "  Tab      Autocomplete suggestions",
            "  Enter    Execute search",
            "",
            ("success", "CLUSTER VIEW (F5)"),
            "  Arrows   Scroll through GPU/node info",
            "",
            ("faded", "─" * 60),
            ("faded", f"Current view: {current_view_name}"),
            ("faded", "License: GNU GPL v3.0 | © 2025 Andreas Skau"),
            ("faded", "GitHub: https://github.com/buzh/slop"),
            ("faded", f"scontrol fetch time: {fetch_duration:.1f}s"),
        ]

        from slop.ui.widgets import HelpOverlay
        self.open_overlay(HelpOverlay(self, help_text))

    def open_search(self):
        """Open search overlay."""
        search_overlay = SearchOverlay(self, self.sreport_fetcher, self.adaptive_sacct, self.handle_search_result)
        self.open_overlay(search_overlay)

    def handle_search_result(self, result_data, search_type, search_value):
        """Handle search results.

        Args:
            result_data: sreport data (user/account) or sacct data (job/node)
            search_type: 'user', 'account', 'job', or 'node'
            search_value: Username, account name, job ID, or node name
        """
        # Save previous view if it was an F1 view (for Esc to return to)
        if self.current_view in [0, 1]:
            self.last_f1_view = self.current_view

        if search_type in ['user', 'account']:
            # Show comprehensive report view (sreport + adaptive sacct)
            self.screen_report = ReportView(
                self,
                search_type,
                search_value,
                result_data,
                self.adaptive_sacct
            )
            self.current_view = 6  # Report view
            self.frame.body = u.AttrMap(self.screen_report, 'bg')

            entity_label = "User" if search_type == 'user' else "Account"
            self.header.update(f"{entity_label} Report - {search_value}")
            self.footer.update('history', f1_label=self.get_f1_label())
            self.loop.draw_screen()

        elif search_type == 'job':
            # For job search, show job details overlay if single job found
            from slop.models import Job
            from slop.ui.views import JobInfoOverlay
            jobs_data = result_data.get('jobs', [])
            if jobs_data:
                job = Job(jobs_data[0])
                self.open_overlay(JobInfoOverlay(job, self))

        elif search_type == 'node':
            # For node search, show hardware info view
            # TODO: Create dedicated hardware info view
            # For now, just show a message
            from slop.ui.widgets import GenericOverlayText
            self.open_overlay(GenericOverlayText(self, f"Node search for '{search_value}' - Hardware info view coming soon!"))

    def show_splash_screen(self):
        """Display splash screen while initial data loads."""
        overlay = GenericOverlayText(self, "Welcome to slop\nPlease wait while fetching job data")
        self.open_overlay(overlay)

        def on_job_update(*_args):
            self.close_overlay()
            u.disconnect_signal(self.jobs, 'jobs_updated', on_job_update)

        u.connect_signal(self.jobs, 'jobs_updated', on_job_update)

    def on_resize(self, *args):
        """Handle terminal resize events."""
        col_rows = u.raw_display.Screen().get_cols_rows()
        self.width = col_rows[0]
        self.height = col_rows[1] - 4

        # Notify ALL screens of resize so they recalculate separator widths
        screens = [
            self.screen_my_jobs, self.screen_users, self.screen_accounts,
            self.screen_partitions, self.screen_states, self.screen_cluster,
            self.screen_queue
        ]
        for screen in screens:
            screen.on_resize()

        # Also update report view if it exists
        if self.screen_report:
            self.screen_report.on_resize()

        # Update footer to adapt to new width
        footer_types = ['myjobs', 'users', 'accounts', 'partitions', 'states', 'cluster']
        if 0 <= self.current_view < len(footer_types):
            self.footer.update(footer_types[self.current_view], f1_label=self.get_f1_label())

        self.loop.draw_screen()

    def get_f1_label(self):
        """Get the label for F1 key in footer."""
        if self.current_view in [0, 1]:
            self.last_f1_view = self.current_view
            return "All Users" if self.current_view == 0 else "My Jobs"
        else:
            return "My Jobs" if self.last_f1_view == 0 else "All Users"

    def _show_screen(self, view_id, screen, header_text, footer_type):
        """Common logic for switching screens."""
        self.current_view = view_id
        self.frame.body = u.AttrMap(screen, 'bg')
        screen.update()
        self.header.update(header_text)
        self.footer.update(footer_type, f1_label=self.get_f1_label())

    def show_screen_my_jobs(self):
        self._show_screen(0, self.screen_my_jobs, f"My Jobs ({self.current_username})", 'myjobs')

    def show_screen_users(self):
        self._show_screen(1, self.screen_users, "All Users", 'users')

    def show_screen_accounts(self):
        self._show_screen(2, self.screen_accounts, "Accounts", 'accounts')

    def show_screen_partitions(self):
        self._show_screen(3, self.screen_partitions, "Partitions", 'partitions')

    def show_screen_states(self):
        self._show_screen(4, self.screen_states, "Job States", 'states')

    def show_screen_cluster(self):
        self._show_screen(5, self.screen_cluster, "Cluster Resources", 'cluster')

    def show_screen_queue(self):
        self._show_screen(7, self.screen_queue, "Queue Status", 'queue')

    def show_screen_report(self):
        """Show report view, creating empty one if needed."""
        if self.screen_report is not None:
            # Show existing report
            entity_type = self.screen_report.entity_type
            entity_name = self.screen_report.entity_name
            entity_label = "User" if entity_type == 'user' else "Account"
            self._show_screen(6, self.screen_report, f"{entity_label} Report - {entity_name}", 'history')
        else:
            # Create empty report view for current user
            from slop.ui.report_view import ReportView
            empty_sreport = []
            self.screen_report = ReportView(
                self,
                'user',
                self.current_username,
                empty_sreport,
                self.adaptive_sacct
            )
            self._show_screen(6, self.screen_report, f"User Report - {self.current_username}", 'history')

        # Focus on search field (left column, which contains search box)
        if hasattr(self.screen_report, 'columns'):
            self.screen_report.columns.set_focus_column(0)

    def keypress(self, size, key):
        # Quit
        if key in ('q', 'f10'):
            self.open_overlay(self.confirmexit, height=3)
            return

        # F1: Toggle My Jobs / All Users
        if key == 'f1':
            self._handle_f1()
            return

        # Function keys: Switch views
        view_map = {'f2': self.show_screen_accounts, 'f3': self.show_screen_partitions,
                    'f4': self.show_screen_states, 'f5': self.show_screen_cluster,
                    'f6': self.show_screen_report, 'f7': self.show_screen_queue}
        if key in view_map:
            view_map[key]()
            return

        # Search
        if key == '/':
            self.open_search()
            return

        # Help
        if key == '?':
            self.show_app_info()
            return

        # Close overlay
        if key == 'esc' and self.overlay_showing:
            self.close_overlay()
            return

        return super().keypress(size, key)

    def _handle_f1(self):
        """Handle F1 key: toggle or return to last F1 view."""
        if self.current_view in [0, 1]:
            # On F1 view - toggle to the other
            self.show_screen_users() if self.current_view == 0 else self.show_screen_my_jobs()
        else:
            # On other view - return to last F1 view
            if self.last_f1_view == 0:
                self.show_screen_my_jobs()
            else:
                self.show_screen_users()

    def open_overlay(self, widget, height=None):
        """Display an overlay widget on top of current screen."""
        self.previous_body = self.frame.body

        if height is None:
            # Check for both 'height' and 'overlay_height' attributes
            height = getattr(widget, "height", getattr(widget, "overlay_height", int(self.height * 0.8)))

        # Wrap widget in a solid-colored frame to force it to expand to full height
        # Using Frame with just body ensures the widget gets the full height allocation
        framed = u.Frame(widget)

        # Create overlay
        overlay = u.Overlay(framed, self.frame.body,
                           align='center', width=('relative', 70),
                           valign='middle', height=height)
        self.frame.body = u.AttrMap(overlay, 'bg')
        self.overlay_showing = True

    def close_overlay(self):
        """Close the overlay and return to previous screen."""
        self.frame.body = self.previous_body
        self.overlay_showing = False

    def startloop(self):
        self.loop.set_alarm_in(1, lambda loop, user_data: asyncio.create_task(self.auto_refresh()))
        self.loop.run()

from slop.main import unhandled_input
