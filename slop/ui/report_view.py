"""Comprehensive user/account report view combining sreport and sacct data."""

import urwid as u
import datetime
from slop.ui.views import JobInfoOverlay
from slop.ui.history_fetcher import HistoryFetcher


class AccountUsageWidget(u.WidgetWrap):
    """Widget for displaying a single account's usage."""

    def __init__(self, account_data):
        self.account_data = account_data

        account = account_data.get('account', 'N/A')
        used_hours = account_data.get('used', 0)

        # Format hours (right-aligned)
        if used_hours >= 1000:
            hours_str = f"{used_hours:,}"
        else:
            hours_str = str(used_hours)

        text = f"  {account:20s} │ {hours_str:>12s}"

        # Color code by usage (no focus map - this is static display)
        if used_hours > 1000:
            attr = 'success'
        elif used_hours > 100:
            attr = 'bg'
        else:
            attr = 'faded'

        widget = u.Text((attr, text))
        super().__init__(widget)


class ReportView(u.WidgetWrap):
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

        # State
        self.status_text_widget = None
        self.selected_job = None
        self._redraw_pending = False  # Debounce flag for screen redraws

        # Tab completion state for search
        self.current_matches = []
        self.completion_index = 0
        self.in_tab_completion = False
        self._build_knowledge_base()

        # History fetcher (common with my jobs view)
        self.history_fetcher = HistoryFetcher(main_screen, adaptive_sacct)
        self.history_fetcher.on_progress = self._on_history_progress
        self.history_fetcher.on_complete = self._on_history_complete

        # Build initial UI
        self._build_ui()

        super().__init__(self.columns)

        # Set focus to right panel (job list)
        self.columns.set_focus_column(1)

        # Connect signal for job list walker
        u.connect_signal(self.job_listwalker, 'modified', self.modified)

        # Start background sacct fetch
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

    def _handle_search_tab(self):
        """Handle tab completion for search."""
        if not self.current_matches:
            return

        if self.in_tab_completion and len(self.current_matches) > 1:
            self.completion_index = (self.completion_index + 1) % len(self.current_matches)
        else:
            self.completion_index = 0
            self.in_tab_completion = True

        completion = self.current_matches[self.completion_index]
        self.search_edit.set_edit_text(completion)
        self.search_edit.set_edit_pos(len(completion))

    def _perform_user_search(self):
        """Search for a new user and reload the view."""
        username = self.search_edit.get_edit_text().strip()
        if not username:
            return

        # Reload view for new user
        # Use the main screen's search handler to create a new ReportView
        if hasattr(self.main_screen, 'open_search'):
            # Directly fetch and show new report
            import subprocess
            try:
                result = subprocess.run(
                    ['getent', 'passwd', username],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=2
                )
                if result.returncode != 0:
                    self.search_suggestions.set_text(("error", f"User '{username}' not found"))
                    return
            except Exception:
                pass

            # Fetch new data and create new view
            from slop.slurm.sreport_fetcher import SreportFetcher
            sreport = SreportFetcher()
            result = sreport.fetch_user_utilization(username)
            if result:
                # Create new report view through main screen handler
                self.main_screen.handle_search_result(result, 'user', username)
            else:
                self.search_suggestions.set_text(("error", f"No data for user '{username}'"))

    def _build_ui(self):
        """Build the UI components."""
        # === USER SEARCH BOX (top of left column) ===
        self.search_edit = u.Edit("User: ")
        u.connect_signal(self.search_edit, 'change', self._on_search_change)
        self.search_suggestions = u.Text("", wrap='clip')

        search_widgets = [
            self.search_edit,
            self.search_suggestions,
        ]
        search_pile = u.Pile(search_widgets)
        search_filler = u.Filler(search_pile, valign='top')
        search_box = u.LineBox(
            u.AttrMap(search_filler, 'bg'),
            title="Search User",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        # Calculate sreport totals
        total_hours = sum(row.get('used', 0) for row in self.sreport_data)
        if total_hours >= 1000:
            total_str = f"{total_hours:,}"
        else:
            total_str = str(total_hours)

        # === ACCOUNT USAGE PANEL ===
        entity_label = "User" if self.entity_type == 'user' else "Account"
        left_info = u.Text([
            ("normal", f"{entity_label}: {self.entity_name}\n"),
            ("normal", f"Total CPU hours: {total_str}"),
        ])

        usage_header = u.Text(f"  {'Account':<20s} │ {'CPU Hours':>12s}")
        usage_header = u.AttrMap(usage_header, 'jobheader')

        account_widgets = [left_info, u.Divider("─"), usage_header, u.Divider("─")]
        for row in sorted(self.sreport_data, key=lambda x: x.get('used', 0), reverse=True):
            account_widgets.append(AccountUsageWidget(row))

        if len(account_widgets) == 4:  # Only headers, no data
            account_widgets.append(u.Text(("faded", "  No usage data found")))

        # Static display (no walker needed)
        left_pile = u.Pile(account_widgets)
        left_filler = u.Filler(left_pile, valign='top')
        left_panel = u.LineBox(
            u.AttrMap(left_filler, 'bg'),
            title="Account Usage",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        # === RIGHT PANEL: Job History ===
        # Header area (will be updated when jobs load)
        self.header_pile = u.Pile([
            u.Text(("faded", "Fetching job history..."))
        ])

        # Job list (placeholder, filled when sacct completes)
        self.status_text_widget = u.AttrMap(
            u.Text(("faded", "Fetching job history...")),
            'bg', 'normal_selected'
        )
        self.job_listwalker = u.SimpleFocusListWalker([self.status_text_widget])
        self.job_listbox = u.ListBox(self.job_listwalker)

        # Combine header and scrollable list
        right_content = u.Pile([
            ('pack', self.header_pile),
            ('weight', 1, u.AttrMap(self.job_listbox, 'bg')),
        ])

        self.right_panel = u.LineBox(
            right_content,
            title="Job History",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        # === STATISTICS PANEL (initially empty, filled when history loads) ===
        self.stats_widgets = [u.Text(("faded", "Loading statistics..."))]
        self.stats_pile = u.Pile(self.stats_widgets)
        stats_filler = u.Filler(self.stats_pile, valign='top')
        self.stats_panel = u.LineBox(
            u.AttrMap(stats_filler, 'bg'),
            title="Job Statistics",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        # === LEFT COLUMN: Stack search box + account usage + statistics ===
        left_column = u.Pile([
            ('pack', search_box),
            ('weight', 40, left_panel),
            ('weight', 60, self.stats_panel),
        ])

        # === TWO COLUMN LAYOUT ===
        self.columns = u.Columns([
            ('weight', 25, left_column),
            ('weight', 75, self.right_panel),
        ], dividechars=1)

    def modified(self):
        """Handle walker modification (focus change)."""
        # Just track focus, no need to redraw
        pass

    def _schedule_redraw(self):
        """Schedule a screen redraw with debouncing to avoid flicker."""
        if not self._redraw_pending:
            self._redraw_pending = True
            self.main_screen.loop.set_alarm_in(0.05, self._do_redraw)

    def _do_redraw(self, *_):
        """Execute the actual screen redraw."""
        self._redraw_pending = False
        self.main_screen.loop.draw_screen()

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

            # Create column with same sizing as job widgets
            align, width, _ = display_attr[headeritem]
            h = u.Text(('faded', label))
            header_columns.append((align, width, h))

        # Return the header as a widget (no AttrMap wrapper, just like other views)
        return u.Columns(header_columns, dividechars=1)

    def _on_history_progress(self, status):
        """Update progress display based on sacct fetcher status."""
        stage = status.get('stage')

        # Get formatted text from fetcher
        summary, _ = self.history_fetcher.get_progress_text(status)

        # Build detailed status message with color coding
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
            # Don't update here - _on_history_complete will handle it
            return

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

        # Update header (replace only the status widget, not the entire contents)
        if len(self.header_pile.contents) > 0:
            self.header_pile.contents[0] = (header_text, ('pack', None))
        else:
            self.header_pile.contents = [(header_text, ('pack', None))]

        # Update job list with status message (wrap in AttrMap to make selectable)
        u.disconnect_signal(self.job_listwalker, 'modified', self.modified)
        self.job_listwalker.clear()
        self.job_listwalker.append(u.AttrMap(detail_text, 'bg', 'normal_selected'))
        self.job_listwalker.set_focus(0)
        u.connect_signal(self.job_listwalker, 'modified', self.modified)
        self._schedule_redraw()

    def _calculate_statistics(self, jobs):
        """Calculate job statistics from history.

        Args:
            jobs: List of Job objects

        Returns:
            dict with statistics
        """
        if not jobs:
            return None

        stats = {
            'total': len(jobs),
            'completed': 0,
            'failed': 0,
            'cancelled': 0,
            'timeout': 0,
            'oom': 0,
            'cpu_efficiencies': [],
            'time_efficiencies': [],
            'total_cpu_hours_alloc': 0,
            'total_cpu_hours_used': 0,
            'jobs_hit_time_limit': 0,
            'jobs_low_time_use': 0,
            'most_used_partition': {},
            'most_used_account': {},
            'failed_reasons': {},
        }

        for job in jobs:
            # Job states
            states = set(job.job_state) if isinstance(job.job_state, list) else {job.job_state}

            if 'COMPLETED' in states:
                stats['completed'] += 1
            elif 'FAILED' in states:
                stats['failed'] += 1
                reason = getattr(job, 'state_reason', 'Unknown')
                stats['failed_reasons'][reason] = stats['failed_reasons'].get(reason, 0) + 1
            elif 'CANCELLED' in states:
                stats['cancelled'] += 1
            elif 'TIMEOUT' in states:
                stats['timeout'] += 1
            elif 'OUT_OF_MEMORY' in states:
                stats['oom'] += 1

            # Partition tracking
            if hasattr(job, 'partition'):
                stats['most_used_partition'][job.partition] = stats['most_used_partition'].get(job.partition, 0) + 1

            # Account tracking
            if hasattr(job, 'account'):
                stats['most_used_account'][job.account] = stats['most_used_account'].get(job.account, 0) + 1

            # CPU and time efficiency (only for completed jobs)
            if 'COMPLETED' in states:
                # Get time data from sacct (normalized by Job class)
                time_obj = getattr(job, 'time', {})
                if isinstance(time_obj, dict):
                    elapsed_sec = time_obj.get('elapsed', 0)

                    # Get actual CPU time used (from sacct)
                    total_cpu_time = time_obj.get('total', {})
                    if isinstance(total_cpu_time, dict):
                        cpu_sec_used = total_cpu_time.get('seconds', 0) + total_cpu_time.get('microseconds', 0) / 1000000
                    else:
                        cpu_sec_used = 0

                    # Get allocated CPUs from tres (sacct format)
                    cpus = 0
                    if hasattr(job, 'tres') and isinstance(job.tres, dict):
                        for tres_item in job.tres.get('allocated', []):
                            if isinstance(tres_item, dict) and tres_item.get('type') == 'cpu':
                                cpus = tres_item.get('count', 0)
                                break

                    # Fallback to cpus field if tres not available
                    if cpus == 0:
                        cpus_obj = getattr(job, 'cpus', {})
                        if isinstance(cpus_obj, dict):
                            cpus = cpus_obj.get('number', 0)
                        else:
                            cpus = cpus_obj if isinstance(cpus_obj, int) else 0

                    if cpus > 0 and elapsed_sec > 0:
                        # Calculate allocated CPU time
                        cpu_hours_alloc = (cpus * elapsed_sec) / 3600
                        stats['total_cpu_hours_alloc'] += cpu_hours_alloc

                        # Calculate actual CPU hours used
                        if cpu_sec_used > 0:
                            cpu_hours_used = cpu_sec_used / 3600
                            stats['total_cpu_hours_used'] += cpu_hours_used

                            # Calculate CPU efficiency
                            cpu_efficiency = (cpu_sec_used / (cpus * elapsed_sec)) * 100
                            stats['cpu_efficiencies'].append(cpu_efficiency)

                        # Time efficiency (elapsed vs time limit)
                        time_limit_obj = time_obj.get('limit', {})
                        if isinstance(time_limit_obj, dict) and time_limit_obj.get('set') and not time_limit_obj.get('infinite'):
                            limit_min = time_limit_obj.get('number', 0)
                            if limit_min > 0:
                                elapsed_min = elapsed_sec / 60
                                time_eff = (elapsed_min / limit_min) * 100
                                stats['time_efficiencies'].append(time_eff)

                                # Track patterns
                                if time_eff > 95:
                                    stats['jobs_hit_time_limit'] += 1
                                elif time_eff < 20:
                                    stats['jobs_low_time_use'] += 1

        return stats

    def _build_statistics_widgets(self, stats):
        """Build statistics display widgets.

        Args:
            stats: Statistics dict from _calculate_statistics

        Returns:
            list of urwid widgets
        """
        if not stats:
            return [u.Text(("faded", "  No statistics available"))]

        widgets = []
        total = stats['total']

        # Job counts with percentages
        widgets.append(u.Text(f"Total Jobs:  {total}"))
        if stats['completed'] > 0:
            pct = stats['completed'] * 100 // total
            widgets.append(u.Text(f"Completed:   {stats['completed']} ({pct}%)"))
        if stats['failed'] > 0:
            pct = stats['failed'] * 100 // total
            widgets.append(u.AttrMap(u.Text(f"Failed:      {stats['failed']} ({pct}%)"), 'error'))
        if stats['timeout'] > 0:
            widgets.append(u.AttrMap(u.Text(f"  Timeout:   {stats['timeout']}"), 'warning'))
        if stats['oom'] > 0:
            widgets.append(u.AttrMap(u.Text(f"  OOM:       {stats['oom']}"), 'warning'))
        if stats['cancelled'] > 0:
            pct = stats['cancelled'] * 100 // total
            widgets.append(u.Text(f"Cancelled:   {stats['cancelled']} ({pct}%)"))

        widgets.append(u.Divider())

        # CPU efficiency
        if stats['cpu_efficiencies']:
            avg_cpu_eff = sum(stats['cpu_efficiencies']) / len(stats['cpu_efficiencies'])
            if avg_cpu_eff >= 70:
                widgets.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_cpu_eff:.1f}%"), 'success'))
            elif avg_cpu_eff >= 40:
                widgets.append(u.Text(f"Avg CPU Eff: {avg_cpu_eff:.1f}%"))
            else:
                widgets.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_cpu_eff:.1f}%"), 'warning'))
                widgets.append(u.Text(("faded", "  💡 Consider requesting fewer cores")))

        # CPU hours
        if stats['total_cpu_hours_alloc'] > 0:
            wasted_cpu_hours = stats['total_cpu_hours_alloc'] - stats['total_cpu_hours_used']
            if stats['total_cpu_hours_used'] > 0:
                widgets.append(u.Text(f"CPU Hours:   {stats['total_cpu_hours_alloc']:.1f}h alloc, {stats['total_cpu_hours_used']:.1f}h used"))
                if wasted_cpu_hours > 0:
                    waste_pct = (wasted_cpu_hours / stats['total_cpu_hours_alloc']) * 100
                    if waste_pct > 30:
                        widgets.append(u.AttrMap(u.Text(f"  Wasted:    {wasted_cpu_hours:.1f}h ({waste_pct:.0f}%)"), 'warning'))
                    else:
                        widgets.append(u.Text(f"  Wasted:    {wasted_cpu_hours:.1f}h ({waste_pct:.0f}%)"))
            else:
                widgets.append(u.Text(f"CPU Hours:   {stats['total_cpu_hours_alloc']:.1f}h allocated"))

        # Time efficiency
        if stats['time_efficiencies']:
            avg_time_eff = sum(stats['time_efficiencies']) / len(stats['time_efficiencies'])
            if avg_time_eff >= 60:
                widgets.append(u.AttrMap(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"), 'success'))
            elif avg_time_eff < 30:
                widgets.append(u.AttrMap(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"), 'warning'))
                widgets.append(u.Text(("faded", "  💡 Consider shorter time limits")))
            else:
                widgets.append(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"))

        # Pattern warnings
        if stats['jobs_hit_time_limit'] > 0 and stats['completed'] > 0:
            pct = (stats['jobs_hit_time_limit'] / stats['completed']) * 100
            if pct > 20:
                widgets.append(u.AttrMap(u.Text(f"⚠ {stats['jobs_hit_time_limit']} jobs hit time limit ({pct:.0f}%)"), 'warning'))

        if stats['jobs_low_time_use'] > 0 and stats['completed'] > 0:
            pct = (stats['jobs_low_time_use'] / stats['completed']) * 100
            if pct > 30:
                widgets.append(u.Text(("info", f"💡 {stats['jobs_low_time_use']} jobs used <20% of time ({pct:.0f}%)")))

        # Most used resources
        if stats['most_used_partition']:
            top_partition = max(stats['most_used_partition'].items(), key=lambda x: x[1])
            widgets.append(u.Divider())
            widgets.append(u.Text(f"Top Partition: {top_partition[0]} ({top_partition[1]} jobs)"))

        # Failed job reasons (top 3)
        if stats['failed_reasons']:
            widgets.append(u.Divider())
            widgets.append(u.Text(("error", "Failed Job Reasons:")))
            sorted_reasons = sorted(stats['failed_reasons'].items(), key=lambda x: x[1], reverse=True)[:3]
            for reason, count in sorted_reasons:
                widgets.append(u.Text(f"  {reason}: {count}"))

        return widgets

    def _on_history_complete(self, history_jobs, meta):
        """Handle sacct fetch completion.

        Args:
            history_jobs: List of Job objects (already filtered and sorted by fetcher)
            meta: Metadata dict with 'window', 'hours', 'duration' keys
        """
        if history_jobs:
            # Build job list header
            window_name = meta.get('window', 'unknown')
            hours = meta.get('hours', 0)
            duration = meta.get('duration', 0)

            # Set widget width for responsive display
            available_width = int(self.main_screen.width * 0.75) - 3 if hasattr(self.main_screen, 'width') else None
            for job in history_jobs:
                job.set_widget_width(available_width, view_type='history')

            # Build fixed header (summary + column headers)
            summary_text = u.Text(("faded", f"Job History ({window_name}, {len(history_jobs)} jobs) - Query: {hours}h in {duration:.1f}s"))
            header_widgets = [summary_text, u.Divider("─")]

            # Add column headers if we have jobs (build AFTER setting width)
            column_header = self._build_column_header(history_jobs[0])
            header_widgets.append(column_header)
            header_widgets.append(u.Divider("─"))

            # Update header pile
            self.header_pile.contents = [(w, ('pack', None)) for w in header_widgets]

            # Create job widgets (only the actual jobs, not headers)
            job_widgets = []
            for job in history_jobs:
                job_widgets.append(job.widget)

            # Update job list
            u.disconnect_signal(self.job_listwalker, 'modified', self.modified)
            self.job_listwalker.clear()
            self.job_listwalker.extend(job_widgets)

            # Set focus on first selectable job
            for index, item in enumerate(self.job_listwalker):
                if hasattr(item, "jobid"):
                    self.job_listwalker.set_focus(index)
                    break

            u.connect_signal(self.job_listwalker, 'modified', self.modified)

            # Calculate and update statistics
            stats = self._calculate_statistics(history_jobs)
            stats_widgets = self._build_statistics_widgets(stats)
            self.stats_pile.contents = [(w, ('pack', None)) for w in stats_widgets]

        else:
            # No jobs found
            window_name = meta.get('window', 'unknown')
            hours = meta.get('hours', 0)
            duration = meta.get('duration', 0)

            # Update header
            header_text = u.Text(("faded", f"Job History ({window_name}, 0 jobs) - Query: {hours}h in {duration:.1f}s"))
            self.header_pile.contents = [(header_text, ('pack', None))]

            # Update job list
            u.disconnect_signal(self.job_listwalker, 'modified', self.modified)
            self.job_listwalker.clear()
            self.job_listwalker.append(u.AttrMap(u.Text(("faded", "No jobs found in query window")), 'bg', 'normal_selected'))
            self.job_listwalker.set_focus(0)
            u.connect_signal(self.job_listwalker, 'modified', self.modified)

            # Update statistics to show "no jobs"
            self.stats_pile.contents = [(u.Text(("faded", "  No jobs to analyze")), ('pack', None))]

        self._schedule_redraw()

    def keypress(self, size, key):
        # Check if search box is focused (left column focus)
        if self.columns.get_focus_column() == 0:
            # Left column focused - handle search box keys
            if key == 'tab':
                self._handle_search_tab()
                return None
            elif key == 'enter':
                self._perform_user_search()
                return None
            elif key not in ['esc', '?']:
                # Reset tab completion on any other key
                self.completion_index = 0
                self.in_tab_completion = False

        if key == 'esc':
            # Return to last F1 view (my jobs or users)
            if self.main_screen.last_f1_view == 0:
                self.main_screen.show_screen_my_jobs()
            else:
                self.main_screen.show_screen_users()
            return None

        if key == '?':
            self.main_screen.show_app_info()
            return None

        if key == 'enter':
            # Show job details overlay (right panel focused)
            focus_widget, _ = self.job_listbox.get_focus()
            if hasattr(focus_widget, "jobid"):
                # Find the Job object by jobid
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
