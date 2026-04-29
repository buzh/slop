import urwid as u
import asyncio
import os
import time
from slop.models import Jobs
from slop.slurm import (
    SlurmJobFetcher,
    SlurmClusterFetcher,
    SlurmSdiagFetcher,
    SreportFetcher,
    AdaptiveSacctFetcher,
)
from slop.ui.widgets import Header, Footer, GenericOverlayText, HelpOverlay
from slop.ui.views import ScreenViewReport
from slop.ui.overlays import ConfirmExit, SearchOverlay
from slop.ui.style import PALETTE
from slop.ui.help import build_help_text, build_diagnostics_text
from slop.ui.view_manager import ViewManager


def unhandled_input(key: str) -> None:
    """Handle unhandled input (global fallback)."""
    if key == 'q':
        raise u.ExitMainLoop()


class _OverlayDim(u.AttrMap):
    """Marker AttrMap used to dim the bottom layer beneath an overlay.

    Distinguishing this from the plain `AttrMap(view, 'bg')` that wraps the
    bottom view lets `close_overlay` know which `AttrMap`s to peel.
    """
    pass


def _is_overlay_body(body):
    """True if `body` is a `frame.body` wrapper that holds an Overlay."""
    return isinstance(body, u.AttrMap) and isinstance(body.original_widget, u.Overlay)


def _peel_dim(widget):
    """Strip the `_OverlayDim` wrap if present, otherwise return `widget` unchanged."""
    return widget.original_widget if isinstance(widget, _OverlayDim) else widget


class Slop(u.WidgetWrap):
    """Main screen controller for the slop TUI."""

    def __init__(self, offline_data_dir=None):
        self.palette = PALETTE

        # Event loop and fetchers
        self.asyncloop = u.AsyncioEventLoop()
        self.loop = u.MainLoop(self, self.palette, event_loop=self.asyncloop, unhandled_input=unhandled_input)
        self.offline_data_dir = offline_data_dir
        self.jobfetcher = SlurmJobFetcher(loop=self.asyncloop._loop, offline_data_dir=offline_data_dir)
        self.cluster_fetcher = SlurmClusterFetcher(loop=self.asyncloop._loop, offline_data_dir=offline_data_dir)
        self.sdiag_fetcher = SlurmSdiagFetcher(loop=self.asyncloop._loop, offline_data_dir=offline_data_dir)
        self.sreport_fetcher = SreportFetcher(offline_data_dir=offline_data_dir)
        self.adaptive_sacct = AdaptiveSacctFetcher(offline_data_dir=offline_data_dir)
        self.jobs = Jobs(self.jobfetcher.fetch_sync())
        self.refreshing = False
        # Throttle slow-moving fetchers below the 3s job-refresh cadence.
        self._cluster_interval = 10
        self._cluster_next_fetch = 0.0
        self._sdiag_interval = 30
        self._sdiag_next_fetch = 0.0

        # UI components
        self.header = Header(self)
        self.footer = Footer(self)
        self.confirmexit = ConfirmExit(self)

        # State tracking
        self.overlay_showing = False
        self._splash_layer = None  # AttrMap holding the splash Overlay, for targeted removal

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
    def show_screen_scheduler(self): return self.views.show_scheduler()
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
            # Run all due fetchers in parallel; cluster/sdiag are throttled
            # below the 3s job cadence and only run when their interval has
            # elapsed. Wait for all before announcing the refresh, so views
            # that re-render on jobs_updated see fresh aux data too.
            now = time.monotonic()
            fetches = [self.jobfetcher.update_once()]
            if now >= self._cluster_next_fetch:
                fetches.append(self.cluster_fetcher.fetch())
                self._cluster_next_fetch = now + self._cluster_interval
            if now >= self._sdiag_next_fetch:
                fetches.append(self.sdiag_fetcher.fetch())
                self._sdiag_next_fetch = now + self._sdiag_interval
            await asyncio.gather(*fetches)
            slurm_job_data = await self.jobfetcher.fetch()
            self.jobs.update_slurmdata(slurm_job_data)

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

        footer_types = ['myjobs', 'users', 'accounts', 'partitions', 'states',
                        'cluster', 'history', 'queue', 'scheduler']
        if 0 <= self.views.current < len(footer_types):
            self.footer.update(footer_types[self.views.current], f1_label=self.get_f1_label())

        self.loop.draw_screen()

    # --- Overlays / search ---------------------------------------------------

    def schedule_main(self, fn, *args):
        """Schedule fn(*args) on the main event loop from a worker thread.

        Use this instead of `loop.set_alarm_in(0, ...)` from off-thread code:
        urwid's alarm heap and asyncio's `call_later` are not thread-safe.
        Triggers a redraw after the callback so widget updates show up
        immediately (urwid does that automatically for alarm-driven callbacks
        but not for ones scheduled directly via call_soon_threadsafe).
        """
        def run():
            fn(*args)
            self.loop.draw_screen()
        self.asyncloop._loop.call_soon_threadsafe(run)

    def show_app_info(self):
        """Display application information and keyboard shortcuts overlay."""
        help_text = build_help_text(self.views.current)
        self.open_overlay(HelpOverlay(self, help_text))

    def show_diagnostics(self):
        """Display per-fetcher timings and error state."""
        fetchers = [
            {'name': 'Jobs',      'command': 'scontrol --json show jobs',       'fetcher': self.jobfetcher},
            {'name': 'Cluster',   'command': 'scontrol --json show nodes/partitions', 'fetcher': self.cluster_fetcher},
            {'name': 'Scheduler', 'command': 'sdiag --json',                    'fetcher': self.sdiag_fetcher},
            {'name': 'sreport',   'command': 'sreport cluster AccountUtilizationByUser ...', 'fetcher': self.sreport_fetcher},
            {'name': 'sacct',     'command': 'sacct --json -u <user> -S <date>', 'fetcher': self.adaptive_sacct},
        ]
        text = build_diagnostics_text(fetchers)
        self.open_overlay(HelpOverlay(self, text, title='Diagnostics'))

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
        self._splash_layer = self.frame.body  # AttrMap wrapping the splash Overlay

        def on_job_update(*_args):
            self._dismiss_splash()
            u.disconnect_signal(self.jobs, 'jobs_updated', on_job_update)

        u.connect_signal(self.jobs, 'jobs_updated', on_job_update)

    def _dismiss_splash(self):
        """Remove the splash overlay wherever it sits in the overlay chain.

        The user may have opened other overlays on top of (or dismissed) the
        splash before initial data arrives. Walk the chain to find the splash
        layer and unlink it from its parent — if it's the topmost overlay this
        reduces to `close_overlay`.
        """
        splash_layer = self._splash_layer
        self._splash_layer = None
        if splash_layer is None:
            return
        if self.frame.body is splash_layer:
            self.close_overlay()
            return
        # Splash is buried under newer overlays. Find the parent overlay whose
        # bottom_w (after peeling its dim wrap) is the splash layer, then
        # re-link past it.
        parent_body = self.frame.body
        while _is_overlay_body(parent_body):
            ov = parent_body.original_widget
            bottom = ov.bottom_w
            inner = _peel_dim(bottom)
            if inner is splash_layer:
                splash_below = _peel_dim(splash_layer.original_widget.bottom_w)
                if isinstance(bottom, _OverlayDim):
                    ov.bottom_w = _OverlayDim(splash_below, attr_map=bottom.get_attr_map())
                else:
                    ov.bottom_w = splash_below
                return
            parent_body = inner

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
            'f8': self.show_screen_scheduler,
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

        if key == '!':
            self.show_diagnostics()
            return

        if key == 'esc' and self.overlay_showing:
            self.close_overlay()
            return

        return super().keypress(size, key)

    # --- Overlay chain -------------------------------------------------------
    # Overlays compose natively as nested `urwid.Overlay` widgets: each new
    # overlay wraps the current `frame.body` as its `bottom_w`. The chain is
    # the linked list rooted at `frame.body`, so there is no parallel stack
    # that can drift out of sync with the visible widget.

    def open_overlay(self, widget, height=None):
        """Display an overlay widget on top of the current frame body."""
        bottom = self.frame.body
        depth = self._overlay_depth(bottom)
        offset = depth * 3  # 3% offset per level for visual layering

        if depth > 0:
            dim_level = min(depth, 2)
            dim_attr = f'dim{dim_level}'
            dim_attrs = {attr[0]: dim_attr for attr in self.palette}
            bottom = _OverlayDim(bottom, attr_map=dim_attrs)

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
        """Peel the topmost overlay layer off `frame.body`."""
        if not _is_overlay_body(self.frame.body):
            self.overlay_showing = False
            return
        ov = self.frame.body.original_widget
        self.frame.body = _peel_dim(ov.bottom_w)
        self.overlay_showing = _is_overlay_body(self.frame.body)

    @staticmethod
    def _overlay_depth(body):
        """Count how many overlays sit above the bottom view in `body`."""
        depth = 0
        while _is_overlay_body(body):
            depth += 1
            body = _peel_dim(body.original_widget.bottom_w)
        return depth

    def replace_bottom_body(self, new_body):
        """Replace the bottom view in the overlay chain with `new_body`.

        With no overlay up this just rewrites `frame.body`. With overlays
        stacked it descends to the deepest `Overlay` and rewrites its
        `bottom_w` (preserving any dim wrap), so dismissing every overlay
        reveals the freshly chosen view rather than the one that was current
        when the overlay opened.
        """
        body = self.frame.body
        if not _is_overlay_body(body):
            self.frame.body = new_body
            return
        while True:
            ov = body.original_widget
            bottom = ov.bottom_w
            inner = _peel_dim(bottom)
            if _is_overlay_body(inner):
                body = inner
                continue
            if isinstance(bottom, _OverlayDim):
                ov.bottom_w = _OverlayDim(new_body, attr_map=bottom.get_attr_map())
            else:
                ov.bottom_w = new_body
            return

    def startloop(self):
        self.loop.set_alarm_in(1, lambda loop, user_data: asyncio.create_task(self.auto_refresh()))
        self.loop.run()
