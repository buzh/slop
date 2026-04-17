import urwid as u
import asyncio
import os
from slop.models import Jobs
from slop.slurm import (
    SlurmJobFetcher,
    SlurmClusterFetcher,
    SreportFetcher,
    AdaptiveSacctFetcher,
)
from slop.ui.widgets import Header, Footer, GenericOverlayText, HelpOverlay
from slop.ui.views import ScreenViewReport
from slop.ui.overlays import ConfirmExit, SearchOverlay
from slop.ui.style import PALETTE
from slop.ui.help import build_help_text
from slop.ui.view_manager import ViewManager


def unhandled_input(key: str) -> None:
    """Handle unhandled input (global fallback)."""
    if key == 'q':
        raise u.ExitMainLoop()


class SC(u.WidgetWrap):
    """Main screen controller for slop TUI."""

    def __init__(self, offline_data_dir=None):
        self.palette = PALETTE

        # Event loop and fetchers
        self.asyncloop = u.AsyncioEventLoop()
        self.loop = u.MainLoop(self, self.palette, event_loop=self.asyncloop, unhandled_input=unhandled_input)
        self.offline_data_dir = offline_data_dir
        self.jobfetcher = SlurmJobFetcher(loop=self.asyncloop._loop, offline_data_dir=offline_data_dir)
        self.cluster_fetcher = SlurmClusterFetcher(loop=self.asyncloop._loop, offline_data_dir=offline_data_dir)
        self.sreport_fetcher = SreportFetcher(offline_data_dir=offline_data_dir)
        self.adaptive_sacct = AdaptiveSacctFetcher(offline_data_dir=offline_data_dir)
        self.jobs = Jobs(self.jobfetcher.fetch_sync())
        self.refreshing = False

        # UI components
        self.header = Header(self)
        self.footer = Footer(self)
        self.confirmexit = ConfirmExit(self)

        # State tracking
        self.overlay_showing = False
        self.overlay_stack = []  # Stack of previous bodies for nested overlays

        col_rows = u.raw_display.Screen().get_cols_rows()
        self.width = col_rows[0]
        self.height = col_rows[1] - 4  # Subtract header/footer space

        # Views (must come after fetchers and `self.height` are set)
        self.current_username = os.getenv('USER') or os.getenv('USERNAME') or 'unknown'
        self.views = ViewManager(self)
        self.body = u.AttrMap(self.views.users, 'bg')
        self.frame = u.Frame(header=self.header, body=self.body, footer=self.footer)

        # Handle window resize (if supported by the event loop)
        try:
            u.connect_signal(self.loop.screen, 'resize', self.on_resize)
        except (NameError, AttributeError):
            pass

        super().__init__(self.frame)

        # Show initial screen: My Jobs if user has jobs, otherwise All Users
        if self.views.my_jobs.has_jobs():
            self.show_screen_my_jobs()
        else:
            self.show_screen_users()

        self.show_splash_screen()

    # --- Compatibility shims --------------------------------------------------
    # External callers (e.g., overlays, sub-views) refer to `current_view`,
    # `last_f1_view`, and the `show_screen_*` methods. Forward them to ViewManager.

    @property
    def current_view(self):
        return self.views.current

    @property
    def last_f1_view(self):
        return self.views.last_f1_view

    def show_screen_my_jobs(self):  return self.views.show_my_jobs()
    def show_screen_users(self):    return self.views.show_users()
    def show_screen_accounts(self): return self.views.show_accounts()
    def show_screen_partitions(self): return self.views.show_partitions()
    def show_screen_states(self):   return self.views.show_states()
    def show_screen_cluster(self):  return self.views.show_cluster()
    def show_screen_queue(self):    return self.views.show_queue()
    def show_screen_report(self):   return self.views.show_report()

    def get_f1_label(self):
        return self.views.f1_label()

    # --- Refresh / resize ----------------------------------------------------

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

            target = self.views.auto_refresh_target()
            if target is not None:
                target.update()
        finally:
            self.refreshing = False
            self.loop.set_alarm_in(3, lambda loop, user_data: asyncio.create_task(self.auto_refresh()))

    def on_resize(self, *args):
        """Handle terminal resize events."""
        col_rows = u.raw_display.Screen().get_cols_rows()
        self.width = col_rows[0]
        self.height = col_rows[1] - 4

        for screen in self.views.all_resizable():
            screen.on_resize()

        footer_types = ['myjobs', 'users', 'accounts', 'partitions', 'states', 'cluster']
        if 0 <= self.views.current < len(footer_types):
            self.footer.update(footer_types[self.views.current], f1_label=self.get_f1_label())

        self.loop.draw_screen()

    # --- Overlays / search ---------------------------------------------------

    def show_app_info(self):
        """Display application information and keyboard shortcuts overlay."""
        fetch_duration = self.jobfetcher.last_fetch_duration.total_seconds()
        help_text = build_help_text(self.views.current, fetch_duration)
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
        # Save previous F1 view (for Esc to return to)
        if self.views.current in (0, 1):
            self.views.last_f1_view = self.views.current

        if search_type in ('user', 'account'):
            report = ScreenViewReport(self, search_type, search_value, result_data, self.adaptive_sacct)
            self.views.install_report(report, search_type, search_value)
            self.loop.draw_screen()

        elif search_type == 'job':
            from slop.models import Job
            from slop.ui.overlays import JobInfoOverlay
            jobs_data = result_data.get('jobs', [])
            if jobs_data:
                self.open_overlay(JobInfoOverlay(Job(jobs_data[0]), self))

        elif search_type == 'node':
            # TODO: dedicated hardware info view
            self.open_overlay(GenericOverlayText(self, f"Node search for '{search_value}' - Hardware info view coming soon!"))

    def show_splash_screen(self):
        """Display splash screen while initial data loads."""
        overlay = GenericOverlayText(self, "Welcome to slop\nPlease wait while fetching job data")
        self.open_overlay(overlay)

        def on_job_update(*_args):
            self.close_overlay()
            u.disconnect_signal(self.jobs, 'jobs_updated', on_job_update)

        u.connect_signal(self.jobs, 'jobs_updated', on_job_update)

    # --- Input ---------------------------------------------------------------

    def keypress(self, size, key):
        if key in ('q', 'f10'):
            self.open_overlay(self.confirmexit, height=3)
            return

        if key == 'f1':
            self.views.handle_f1()
            return

        view_map = {
            'f2': self.show_screen_accounts,
            'f3': self.show_screen_partitions,
            'f4': self.show_screen_states,
            'f5': self.show_screen_cluster,
            'f6': self.show_screen_report,
            'f7': self.show_screen_queue,
        }
        if key in view_map:
            view_map[key]()
            return

        if key == '/':
            self.open_search()
            return

        if key == '?':
            self.show_app_info()
            return

        if key == 'esc' and self.overlay_showing:
            self.close_overlay()
            return

        return super().keypress(size, key)

    # --- Overlay stack -------------------------------------------------------

    def open_overlay(self, widget, height=None):
        """Display an overlay widget on top of current screen."""
        MAX_OVERLAY_DEPTH = 3
        if len(self.overlay_stack) >= MAX_OVERLAY_DEPTH:
            return

        bottom = self.frame.body
        self.overlay_stack.append(bottom)

        depth = len(self.overlay_stack) - 1
        offset = depth * 3  # 3% offset per level for visual layering

        if depth > 0:
            dim_level = min(depth, 2)
            dim_attr = f'dim{dim_level}'
            attr_map = {attr[0]: dim_attr for attr in self.palette}
            bottom = u.AttrMap(bottom, attr_map=attr_map)

        if height is None:
            height = getattr(widget, "height", getattr(widget, "overlay_height", int(self.height * 0.8)))

        framed = u.Frame(widget)
        overlay = u.Overlay(framed, bottom,
                            align=('relative', 50 + offset),
                            width=('relative', 70),
                            valign=('relative', 50 + offset),
                            height=height)
        self.frame.body = u.AttrMap(overlay, 'bg')
        self.overlay_showing = True

    def close_overlay(self):
        """Close the overlay and return to previous screen."""
        if self.overlay_stack:
            self.frame.body = self.overlay_stack.pop()
            self.overlay_showing = len(self.overlay_stack) > 0
        else:
            self.overlay_showing = False

    def startloop(self):
        self.loop.set_alarm_in(1, lambda loop, user_data: asyncio.create_task(self.auto_refresh()))
        self.loop.run()
