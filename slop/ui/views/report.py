"""Comprehensive user/account report view combining sreport and sacct data."""

import subprocess
import threading

import urwid as u
from slop.ui.overlays import JobInfoOverlay
from slop.ui.tab_completion import TabCompletionMixin
from slop.ui.widgets import AccountUsageWidget, SafeListBox, rounded_box
from slop.ui.views.report_stats import calculate_user_stats, build_stats_widgets
from slop.slurm.history_fetcher import HistoryFetcher


class ScreenViewReport(TabCompletionMixin, u.WidgetWrap):
    """Comprehensive report view showing sreport and sacct data."""

    def __init__(self, main_screen, entity_type, entity_name, sreport_data, adaptive_sacct):
        """Initialize report view.

        Args:
            main_screen: Main screen instance
            entity_type: 'user' or 'account'
            entity_name: Username or account name
            sreport_data: List of dicts with 'account' and 'used' keys (from sreport)
            adaptive_sacct: AdaptiveSacctFetcher instance
        """
        self.main_screen = main_screen
        self.entity_type = entity_type
        self.entity_name = entity_name
        self.sreport_data = sreport_data

        self.status_text_widget = None
        self.selected_job = None
        self._user_search_in_flight = False

        self._init_completion()
        self._build_knowledge_base()

        self.history_fetcher = HistoryFetcher(main_screen, adaptive_sacct)
        self.history_fetcher.on_progress = self._on_history_progress
        self.history_fetcher.on_complete = self._on_history_complete

        self._build_ui()

        super().__init__(self.columns)

        self.columns.set_focus_column(1)

        self.history_fetcher.start_fetch(entity_type, entity_name)

    def _build_knowledge_base(self):
        """Build knowledge base from current cluster state for tab completion."""
        self.known_users = set()
        if hasattr(self.main_screen, 'jobs') and hasattr(self.main_screen.jobs, 'usertable'):
            self.known_users = set(self.main_screen.jobs.usertable.keys())

    def _on_search_change(self, edit_widget, new_text):
        """Update suggestions as user types."""
        if self.in_tab_completion:
            return

        query = new_text.strip().lower()
        if not query:
            self.search_suggestions.set_text("")
            self.current_matches = []
            return

        user_matches = sorted([u for u in self.known_users if query in u.lower()])[:10]
        self.current_matches = user_matches

        if user_matches:
            self.search_suggestions.set_text(("faded", f"{', '.join(user_matches)}"))
        else:
            self.search_suggestions.set_text(("faded", "No matches"))

    def _perform_user_search(self):
        """Search for a new user and reload the view (async)."""
        username = self.search_edit.get_edit_text().strip()
        if not username or self._user_search_in_flight:
            return
        if not hasattr(self.main_screen, 'open_search'):
            return

        self._user_search_in_flight = True
        self.search_suggestions.set_text(("faded", f"Looking up {username}..."))

        def worker():
            try:
                check = subprocess.run(
                    ['getent', 'passwd', username],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=2,
                )
                if check.returncode != 0:
                    payload = ('not_found', None)
                else:
                    from slop.slurm import SreportFetcher
                    sreport = SreportFetcher()
                    payload = ('ok', sreport.fetch_user_utilization(username))
            except Exception as e:
                payload = ('error', str(e))
            self.main_screen.schedule_main(self._on_user_search_done, username, payload)

        threading.Thread(target=worker, daemon=True).start()

    def _on_user_search_done(self, username, payload):
        self._user_search_in_flight = False
        status, data = payload
        if status == 'not_found':
            self.search_suggestions.set_text(("error", f"User '{username}' not found"))
        elif status == 'error':
            self.search_suggestions.set_text(("error", f"Search error: {data}"))
        elif data:
            self.main_screen.handle_search_result(data, 'user', username)
        else:
            self.search_suggestions.set_text(("error", f"No data for user '{username}'"))

    def _build_ui(self):
        """Build the four panels and assemble the two-column layout."""
        search_box = self._build_search_box()
        left_panel = self._build_account_panel()
        self.right_panel = self._build_history_panel()
        self.stats_panel = self._build_stats_panel()

        left_column = u.Pile([
            ('pack', search_box),
            ('weight', 40, left_panel),
            ('weight', 60, self.stats_panel),
        ])

        self.columns = u.Columns([
            ('weight', 25, left_column),
            ('weight', 75, self.right_panel),
        ], dividechars=1)

    def _build_search_box(self):
        self.search_edit = u.Edit("User: ")
        u.connect_signal(self.search_edit, 'change', self._on_search_change)
        self.search_suggestions = u.Text("", wrap='clip')

        pile = u.Pile([self.search_edit, self.search_suggestions])
        return rounded_box(u.AttrMap(u.Filler(pile, valign='top'), 'bg'), title='Search User')

    def _build_account_panel(self):
        total_hours = sum(row.get('used', 0) for row in self.sreport_data)
        total_str = f"{total_hours:,}" if total_hours >= 1000 else str(total_hours)

        entity_label = "User" if self.entity_type == 'user' else "Account"
        info = u.Text([
            ("normal", f"{entity_label}: {self.entity_name}\n"),
            ("normal", f"Total CPU hours: {total_str}"),
        ])

        usage_header = u.AttrMap(
            u.Text(f"  {'Account':<20s} │ {'CPU Hours':>12s}"),
            'jobheader',
        )

        widgets = [info, u.Divider("─"), usage_header, u.Divider("─")]
        for row in sorted(self.sreport_data, key=lambda x: x.get('used', 0), reverse=True):
            widgets.append(AccountUsageWidget(row))
        if len(widgets) == 4:
            widgets.append(u.Text(("faded", "  No usage data found")))

        return rounded_box(u.AttrMap(u.Filler(u.Pile(widgets), valign='top'), 'bg'), title='Account Usage')

    def _build_history_panel(self):
        self.header_pile = u.Pile([u.Text(("faded", "Fetching job history..."))])

        self.status_text_widget = u.AttrMap(
            u.Text(("faded", "Fetching job history...")),
            'bg', 'normal_selected',
        )
        self.job_listwalker = u.SimpleFocusListWalker([self.status_text_widget])
        self.job_listbox = SafeListBox(self.job_listwalker)

        right_content = u.Pile([
            ('pack', self.header_pile),
            ('weight', 1, u.AttrMap(self.job_listbox, 'bg')),
        ])

        return rounded_box(right_content, title='Job History')

    def _build_stats_panel(self):
        self.stats_pile = u.Pile([u.Text(("faded", "Loading statistics..."))])
        return rounded_box(u.AttrMap(u.Filler(self.stats_pile, valign='top'), 'bg'), title='Job Statistics')

    def _build_column_header(self, representative_job):
        """Build a column header widget based on a representative job's display attributes."""
        display_attr = representative_job.widget.display_attr

        static_labels = {
            'job_state': "Status",
            'job_id': "Job ID",
            'task_id': "Task",
            'start_time': "Started",
            'end_time': "Deadline",
            'submit_time': "Submitted",
            'wall_time': "Duration",
            'account': "Acct",
            'exit_code': "Exit code",
            'user_name': "User",
            'partition': "Partition",
            'name': "Name",
            'reason': "Reason",
            'tres': "Resources",
            'nodes': "Nodes",
        }

        header_columns = []
        for headeritem in display_attr:
            label = static_labels.get(headeritem, headeritem.capitalize())
            align, width, _ = display_attr[headeritem]
            header_columns.append((align, width, u.Text(('faded', label))))

        return u.Columns(header_columns, dividechars=1)

    def _on_history_progress(self, status):
        """Update progress display based on sacct fetcher status."""
        stage = status.get('stage')
        summary, _ = self.history_fetcher.get_progress_text(status)

        if stage == 'trying':
            window = status.get('window', '')
            attempt = status.get('attempt', 0)
            total = status.get('total_attempts', 0)
            header_text = u.Text(("faded", summary))
            detail_text = u.Text([
                ("info", "Fetching job history...\n\n"),
                ("normal", f"Trying {window} ({attempt}/{total})..."),
            ])

        elif stage == 'slow':
            window = status.get('window', '')
            duration = status.get('duration', 0)
            threshold = status.get('threshold', 5)
            header_text = u.Text(("faded", summary))
            detail_text = u.Text([
                ("info", "Fetching job history...\n\n"),
                ("warning", f"{window} too slow ({duration:.1f}s > {threshold}s)\n"),
                ("normal", "Trying shorter window..."),
            ])

        elif stage == 'empty':
            window = status.get('window', '')
            header_text = u.Text(("faded", summary))
            detail_text = u.Text([
                ("info", "Fetching job history...\n\n"),
                ("faded", f"{window} returned no data\n"),
                ("normal", "Trying shorter window..."),
            ])

        elif stage == 'success':
            return  # _on_history_complete handles this

        elif stage == 'failed':
            failures = status.get('failures', 0)
            backoff = status.get('backoff_minutes', 0)
            header_text = u.Text(("faded", summary))
            detail_text = u.Text([
                ("error", f"⚠ Query failed ({failures} attempts)\n\n"),
                ("warning", "Slurm accounting database may be overloaded.\n\n"),
                ("normal", f"Will retry in {backoff} minutes."),
            ])

        elif stage == 'backoff':
            wait = status.get('wait_seconds', 0)
            failures = status.get('failures', 0)
            header_text = u.Text(("faded", summary))
            detail_text = u.Text([
                ("warning", f"In retry backoff ({failures} prior failures)\n\n"),
                ("normal", f"Next retry in {wait} seconds"),
            ])
        else:
            return

        if len(self.header_pile.contents) > 0:
            self.header_pile.contents[0] = (header_text, ('pack', None))
        else:
            self.header_pile.contents = [(header_text, ('pack', None))]

        self.job_listwalker.clear()
        self.job_listwalker.append(u.AttrMap(detail_text, 'bg', 'normal_selected'))
        self.job_listwalker.set_focus(0)

    def _on_history_complete(self, history_jobs, meta):
        """Handle sacct fetch completion.

        Args:
            history_jobs: List of Job objects (already filtered and sorted by fetcher)
            meta: Metadata dict with 'window', 'hours', 'duration' keys
        """
        window_name = meta.get('window', 'unknown')
        hours = meta.get('hours', 0)
        duration = meta.get('duration', 0)

        if history_jobs:
            available_width = int(self.main_screen.width * 0.75) - 3 if hasattr(self.main_screen, 'width') else None
            for job in history_jobs:
                job.set_widget_width(available_width, view_type='history')

            summary_text = u.Text(("faded", f"Job History ({window_name}, {len(history_jobs)} jobs) - Query: {hours}h in {duration:.1f}s"))
            column_header = self._build_column_header(history_jobs[0])
            header_widgets = [summary_text, u.Divider("─"), column_header, u.Divider("─")]
            self.header_pile.contents = [(w, ('pack', None)) for w in header_widgets]

            self.job_listwalker.clear()
            self.job_listwalker.extend([job.widget for job in history_jobs])
            for index, item in enumerate(self.job_listwalker):
                if hasattr(item, "jobid"):
                    self.job_listwalker.set_focus(index)
                    break

            stats = calculate_user_stats(history_jobs)
            self.stats_pile.contents = [(w, ('pack', None)) for w in build_stats_widgets(stats)]

        else:
            header_text = u.Text(("faded", f"Job History ({window_name}, 0 jobs) - Query: {hours}h in {duration:.1f}s"))
            self.header_pile.contents = [(header_text, ('pack', None))]

            self.job_listwalker.clear()
            self.job_listwalker.append(u.AttrMap(u.Text(("faded", "No jobs found in query window")), 'bg', 'normal_selected'))
            self.job_listwalker.set_focus(0)

            self.stats_pile.contents = [(u.Text(("faded", "  No jobs to analyze")), ('pack', None))]

    def keypress(self, size, key):
        if self.columns.get_focus_column() == 0:
            if key == 'tab':
                self._cycle_completion()
                return None
            elif key == 'enter':
                self._perform_user_search()
                return None
            elif key not in ['esc', '?']:
                self._reset_completion()

        if key == 'esc':
            if self.main_screen.last_f1_view == 0:
                self.main_screen.show_screen_my_jobs()
            else:
                self.main_screen.show_screen_users()
            return None

        if key == '?':
            self.main_screen.show_app_info()
            return None

        if key == 'enter':
            focus_widget, _ = self.job_listbox.get_focus()
            if hasattr(focus_widget, "jobid"):
                job = next((j for j in self.history_fetcher.history_jobs if j.job_id == focus_widget.jobid), None)
                if job:
                    self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
            return None

        return super().keypress(size, key)

    def update(self):
        """Update view (no-op for static data)."""
        pass

    def on_resize(self):
        """Handle terminal resize."""
        pass
