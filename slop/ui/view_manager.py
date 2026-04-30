"""Owns the screen instances and handles view switching for the main app.

`Slop` keeps thin `show_screen_*` proxies so external callers (e.g., overlays
calling `main_screen.show_screen_dashboard()`) and the `keypress` dispatch
don't need to learn a new API.
"""
import urwid as u
from slop.ui.views import (
    ScreenViewDashboard,
    ScreenViewJobs,
    ScreenViewCluster,
    ScreenViewMyJobs,
    ScreenViewQueue,
    ScreenViewReport,
    ScreenViewScheduler,
)


# View IDs. The first three (DASHBOARD, JOBS, MY_JOBS) reflect the new
# F-key layout; the trailing IDs preserve the existing values so external
# callers using the same numeric constants still resolve correctly.
DASHBOARD, JOBS, MY_JOBS, CLUSTER, REPORT, QUEUE, SCHEDULER = range(7)


class ViewManager:
    """Holds every persistent view, tracks the active one, and switches between them."""

    def __init__(self, sc):
        self.sc = sc
        self.dashboard = ScreenViewDashboard(sc, sc.jobs, sc.cluster_fetcher)
        self.jobs_view = ScreenViewJobs(sc, sc.jobs)
        self.my_jobs = ScreenViewMyJobs(sc, sc.jobs, sc.current_username, sc.adaptive_sacct)
        self.cluster = ScreenViewCluster(sc, sc.cluster_fetcher)
        self.queue = ScreenViewQueue(sc, sc.jobs)
        self.scheduler = ScreenViewScheduler(sc, sc.sdiag_fetcher)
        self.report = None  # Created on demand

        self.current = DASHBOARD

    # --- Iteration helpers used by auto_refresh / on_resize ----------------

    def auto_refresh_target(self):
        """Return the screen whose `update()` should run after a data refresh, if any."""
        by_id = {
            DASHBOARD: self.dashboard, JOBS: self.jobs_view, MY_JOBS: self.my_jobs,
            CLUSTER: self.cluster, QUEUE: self.queue, SCHEDULER: self.scheduler,
        }
        return by_id.get(self.current)

    def all_resizable(self):
        """All screens that should receive `on_resize()`."""
        screens = [self.dashboard, self.jobs_view, self.my_jobs,
                   self.cluster, self.queue, self.scheduler]
        if self.report is not None:
            screens.append(self.report)
        return screens

    # --- Switching ---------------------------------------------------------

    def show(self, view_id, screen, header_text, footer_type):
        if view_id != self.current:
            self.sc.jobs.reset_array_collapse()
        self.current = view_id
        # `replace_bottom_body` rewrites the deepest layer of the overlay
        # chain when overlays are up, so dismissing them reveals the view the
        # user just picked rather than the one current when the overlay opened.
        self.sc.replace_bottom_body(u.AttrMap(screen, 'bg'))
        screen.update()
        self.sc.header.update(header_text)
        self.sc.footer.update(footer_type)

    def show_dashboard(self):
        self.show(DASHBOARD, self.dashboard, "Dashboard", 'dashboard')

    def show_jobs(self):
        self.show(JOBS, self.jobs_view, "Jobs", self.jobs_view.view_type)

    def show_my_jobs(self):
        self.show(MY_JOBS, self.my_jobs, f"My Jobs ({self.sc.current_username})", 'myjobs')

    def show_cluster(self):
        self.show(CLUSTER, self.cluster, "Cluster Resources", 'cluster')

    def show_queue(self):
        self.show(QUEUE, self.queue, "Queue Status", 'queue')

    def show_scheduler(self):
        self.show(SCHEDULER, self.scheduler, "Scheduler Health", 'scheduler')

    def show_report(self):
        """Show report view, creating an empty one for current user if needed."""
        if self.report is None:
            self.report = ScreenViewReport(
                self.sc, 'user', self.sc.current_username, [], self.sc.adaptive_sacct,
            )
        else:
            # If the previous fetch finished without data (failure or empty),
            # let re-opening the view kick off a retry.
            hf = getattr(self.report, 'history_fetcher', None)
            if hf is not None and not hf.loading and not hf.history_jobs:
                hf.start_fetch(self.report.entity_type, self.report.entity_name)
        entity_label = "User" if self.report.entity_type == 'user' else "Account"
        self.show(REPORT, self.report,
                  f"{entity_label} Report - {self.report.entity_name}", 'history')
        if hasattr(self.report, 'columns'):
            self.report.columns.set_focus_column(0)

    def install_report(self, report_screen, entity_type, entity_name):
        """Replace the current report with a freshly built one and focus it."""
        # Cancel the previous report's in-flight sacct fetch (if any) so its
        # eventual completion callback doesn't mutate orphaned widgets and the
        # accounting database isn't pestered with abandoned queries.
        if self.report is not None and self.report is not report_screen:
            prev_fetcher = getattr(self.report, 'history_fetcher', None)
            if prev_fetcher is not None:
                prev_fetcher.cancel()
        self.report = report_screen
        entity_label = "User" if entity_type == 'user' else "Account"
        self.current = REPORT
        self.sc.replace_bottom_body(u.AttrMap(report_screen, 'bg'))
        self.sc.header.update(f"{entity_label} Report - {entity_name}")
        self.sc.footer.update('history')
