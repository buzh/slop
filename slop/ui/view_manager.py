"""Owns the screen instances and handles view switching for the main app.

`Slop` keeps thin `show_screen_*` proxies so external callers (e.g., overlays
calling `main_screen.show_screen_users()`) and the `keypress` dispatch don't
need to learn a new API.
"""
import urwid as u
from slop.ui.views import (
    ScreenViewUsers,
    ScreenViewAccounts,
    ScreenViewPartitions,
    ScreenViewStates,
    ScreenViewCluster,
    ScreenViewMyJobs,
    ScreenViewQueue,
    ScreenViewReport,
    ScreenViewScheduler,
)


# View IDs (preserved from app.py for compatibility with main_screen.current_view)
MY_JOBS, USERS, ACCOUNTS, PARTITIONS, STATES, CLUSTER, REPORT, QUEUE, SCHEDULER = range(9)


class ViewManager:
    """Holds every persistent view, tracks the active one, and switches between them."""

    def __init__(self, sc):
        self.sc = sc
        self.my_jobs = ScreenViewMyJobs(sc, sc.jobs, sc.current_username, sc.adaptive_sacct)
        self.users = ScreenViewUsers(sc, sc.jobs)
        self.accounts = ScreenViewAccounts(sc, sc.jobs)
        self.partitions = ScreenViewPartitions(sc, sc.jobs)
        self.states = ScreenViewStates(sc, sc.jobs)
        self.cluster = ScreenViewCluster(sc, sc.cluster_fetcher)
        self.queue = ScreenViewQueue(sc, sc.jobs)
        self.scheduler = ScreenViewScheduler(sc, sc.sdiag_fetcher)
        self.report = None  # Created on demand

        self.current = USERS
        self.last_f1_view = None

    # --- Iteration helpers used by auto_refresh / on_resize ----------------

    def auto_refresh_target(self):
        """Return the screen whose `update()` should run after a data refresh, if any."""
        by_id = {
            MY_JOBS: self.my_jobs, USERS: self.users, ACCOUNTS: self.accounts,
            PARTITIONS: self.partitions, STATES: self.states, CLUSTER: self.cluster,
            QUEUE: self.queue, SCHEDULER: self.scheduler,
        }
        return by_id.get(self.current)

    def all_resizable(self):
        """All screens that should receive `on_resize()`."""
        screens = [self.my_jobs, self.users, self.accounts, self.partitions,
                   self.states, self.cluster, self.queue, self.scheduler]
        if self.report is not None:
            screens.append(self.report)
        return screens

    # --- Switching ---------------------------------------------------------

    def show(self, view_id, screen, header_text, footer_type):
        if view_id != self.current:
            self.sc.jobs.reset_array_collapse()
        self.current = view_id
        new_body = u.AttrMap(screen, 'bg')
        # If an overlay (e.g. the splash) is hiding the body, rewrite the
        # bottom of the stack so dismissing the overlay restores the view
        # the user just picked, not the one current when the overlay opened.
        if self.sc.overlay_stack:
            self.sc.overlay_stack[0] = new_body
        else:
            self.sc.frame.body = new_body
        screen.update()
        self.sc.header.update(header_text)
        self.sc.footer.update(footer_type, f1_label=self.f1_label())

    def show_my_jobs(self):
        self.show(MY_JOBS, self.my_jobs, f"My Jobs ({self.sc.current_username})", 'myjobs')

    def show_users(self):
        self.show(USERS, self.users, "All Users", 'users')

    def show_accounts(self):
        self.show(ACCOUNTS, self.accounts, "Accounts", 'accounts')

    def show_partitions(self):
        self.show(PARTITIONS, self.partitions, "Partitions", 'partitions')

    def show_states(self):
        self.show(STATES, self.states, "Job States", 'states')

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
        new_body = u.AttrMap(report_screen, 'bg')
        if self.sc.overlay_stack:
            self.sc.overlay_stack[0] = new_body
        else:
            self.sc.frame.body = new_body
        self.sc.header.update(f"{entity_label} Report - {entity_name}")
        self.sc.footer.update('history', f1_label=self.f1_label())

    # --- F1 toggle ---------------------------------------------------------

    def f1_label(self):
        """Label for the F1 hint in the footer."""
        if self.current in (MY_JOBS, USERS):
            self.last_f1_view = self.current
            return "All Users" if self.current == MY_JOBS else "My Jobs"
        return "My Jobs" if self.last_f1_view == MY_JOBS else "All Users"

    def handle_f1(self):
        """F1 toggles MY_JOBS↔USERS or returns to whichever was last."""
        if self.current == MY_JOBS:
            self.show_users()
        elif self.current == USERS:
            self.show_my_jobs()
        elif self.last_f1_view == MY_JOBS:
            self.show_my_jobs()
        else:
            self.show_users()
