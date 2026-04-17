"""Search overlay for job lookup."""

import urwid as u
import subprocess
import re

from slop.ui.tab_completion import TabCompletionMixin


class SearchOverlay(TabCompletionMixin, u.WidgetWrap):
    """Overlay for searching jobs by various criteria."""

    def __init__(self, main_screen, sreport_fetcher, adaptive_sacct, on_result):
        """Initialize search overlay.

        Args:
            main_screen: Main screen instance
            sreport_fetcher: SreportFetcher instance
            adaptive_sacct: AdaptiveSacctFetcher instance
            on_result: Callback function(result_data, search_type, search_value)
        """
        self.main_screen = main_screen
        self.sreport_fetcher = sreport_fetcher
        self.adaptive_sacct = adaptive_sacct
        self.on_result = on_result
        self.overlay_height = 10

        # Build knowledge base from current cluster state
        self._build_knowledge_base()

        # Create single search input field with change callback
        self.search_edit = u.Edit("Search: ")
        u.connect_signal(self.search_edit, 'change', self._on_search_change)

        # Status text and suggestions
        self.status_text = u.Text("")
        self.suggestions_text = u.Text("", wrap='clip')

        self._init_completion()

        # Build widget list
        widgets = [
            u.AttrMap(u.Text("Search"), 'jobheader'),
            u.Divider("─"),
            u.Text("Enter job ID, username, account, or node:"),
            self.search_edit,
            u.Divider(),
            self.suggestions_text,
            self.status_text,
            u.Divider(),
            u.Text("Enter to search | Tab to complete | Esc to cancel"),
        ]

        # Create pile
        self.pile = pile = u.Pile(widgets)
        filler = u.Filler(pile, valign='top')

        # Wrap in linebox
        linebox = u.LineBox(
            filler,
            title="Search",
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        body = u.AttrMap(linebox, 'normal')

        super().__init__(body)

        # Focus on search field by default
        pile.focus_position = 3  # search_edit

    def _build_knowledge_base(self):
        """Build knowledge base from current cluster state."""
        self.known_users = set()
        self.known_accounts = set()
        self.known_nodes = set()

        # Get users and accounts from active jobs
        if hasattr(self.main_screen, 'jobs'):
            if hasattr(self.main_screen.jobs, 'usertable'):
                self.known_users = set(self.main_screen.jobs.usertable.keys())

            for job in self.main_screen.jobs.jobs:
                if hasattr(job, 'account'):
                    self.known_accounts.add(job.account)

        # Get complete node list from cluster fetcher
        if hasattr(self.main_screen, 'cluster_fetcher'):
            nodes_data = self.main_screen.cluster_fetcher.fetch_nodes_sync()
            if nodes_data and 'nodes' in nodes_data:
                for node_obj in nodes_data['nodes']:
                    if isinstance(node_obj, dict) and 'name' in node_obj:
                        self.known_nodes.add(node_obj['name'])

    def _on_search_change(self, edit_widget, new_text):
        """Update suggestions as user types."""
        # Skip recalculation if we're in the middle of tab completion
        if self.in_tab_completion:
            return

        query = new_text.strip().lower()
        if not query:
            self.suggestions_text.set_text("")
            self.current_matches = []
            return

        # Find matches in all categories (before type detection, so user can see all options)
        user_matches = sorted([u for u in self.known_users if query in u.lower()])[:5]
        account_matches = sorted([a for a in self.known_accounts if query in a.lower()])[:5]
        node_matches = sorted([n for n in self.known_nodes if query in n.lower()])[:5]

        # Check if it's a job ID (purely numeric)
        is_job_id = bool(re.match(r'^\d+(_\d+)?(\[\d+(-\d+)?\])?$', query))

        # Build combined matches for tab completion
        self.current_matches = user_matches + account_matches + node_matches

        # Build suggestions text
        suggestions = []
        if is_job_id:
            suggestions.append("Job ID")
        if user_matches:
            suggestions.append(f"Users: {', '.join(user_matches)}")
        if account_matches:
            suggestions.append(f"Accounts: {', '.join(account_matches)}")
        if node_matches:
            suggestions.append(f"Nodes: {', '.join(node_matches)}")

        if suggestions:
            self.suggestions_text.set_text(("faded", " | ".join(suggestions)))
        elif not is_job_id:
            self.suggestions_text.set_text(("faded", "No matches"))

    def keypress(self, size, key):
        if key == 'esc':
            self.main_screen.close_overlay()
            return None

        if key == 'enter':
            self.perform_search()
            return None

        if key == 'tab':
            self._cycle_completion()
            return None

        # Reset completion state when user types (not tab)
        if key not in ['tab', 'enter', 'esc']:
            self._reset_completion()

        return super().keypress(size, key)

    def _detect_search_type(self, query):
        """Detect what type of search this is.

        Returns:
            tuple: (search_type, confidence) where search_type is 'job', 'node', 'user', or 'account'
                   and confidence is 'high' or 'low'
        """
        # Job ID: purely numeric or numeric with underscore/brackets (array jobs)
        if re.match(r'^\d+(_\d+)?(\[\d+(-\d+)?\])?$', query):
            return ('job', 'high')

        # Check against known entities (exact match = high confidence)
        if query in self.known_nodes:
            return ('node', 'high')
        if query in self.known_accounts:
            return ('account', 'high')
        if query in self.known_users:
            return ('user', 'high')

        # Otherwise assume username (most common)
        return ('user', 'low')

    def _check_user_exists(self, username):
        """Check if a username exists on the system using getent.

        Args:
            username: Username to check

        Returns:
            bool: True if user exists, False otherwise
        """
        try:
            result = subprocess.run(
                ['getent', 'passwd', username],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=2
            )
            return result.returncode == 0
        except Exception:
            # If getent fails, assume user might exist (fail open)
            return True

    def perform_search(self):
        """Execute search based on input, auto-detecting the type."""
        query = self.search_edit.get_edit_text().strip()

        if not query:
            self.status_text.set_text(("failed", "Please enter a search term"))
            return

        # Detect search type
        search_type, confidence = self._detect_search_type(query)

        if search_type == 'job':
            # Search for specific job ID in current jobs first
            self.status_text.set_text(f"Searching for job {query}...")
            self.main_screen.loop.draw_screen()

            # Check current jobs
            job_found = False
            if hasattr(self.main_screen, 'jobs'):
                for job in self.main_screen.jobs.jobs:
                    if str(job.job_id) == query:
                        # Show job details overlay
                        from slop.ui.overlays import JobInfoOverlay
                        self.main_screen.close_overlay()
                        self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
                        job_found = True
                        break

            if not job_found:
                # Not in current jobs - search history via sacct
                self.status_text.set_text(f"Searching history for job {query}...")
                self.main_screen.loop.draw_screen()
                result = self.adaptive_sacct.fetch_job_sync(query)
                if result and result.get('jobs'):
                    # Show as history result
                    self.main_screen.close_overlay()
                    self.on_result(result, 'job', query)
                else:
                    self.status_text.set_text(("error", f"Job {query} not found"))

        elif search_type == 'node':
            # Search for jobs on a node
            self.status_text.set_text(f"Fetching history for node {query}...")
            self.main_screen.loop.draw_screen()
            result = self.adaptive_sacct.fetch_node_history_sync(query)
            if result and result.get('jobs'):
                self.main_screen.close_overlay()
                self.on_result(result, 'node', query)
            else:
                self.status_text.set_text(("error", f"No jobs found on node {query}"))

        elif search_type == 'account':
            # Account history view not yet implemented
            from slop.ui.widgets import GenericOverlayText
            self.main_screen.close_overlay()
            self.main_screen.open_overlay(GenericOverlayText(self.main_screen, f"Account history for '{query}'\n\nFull account report view coming soon.\nUse F2 (Accounts view) to browse jobs by account."))

        else:  # user
            # Check if user exists on the system
            self.status_text.set_text(f"Checking user {query}...")
            self.main_screen.loop.draw_screen()

            if not self._check_user_exists(query):
                self.status_text.set_text(("error", f"User '{query}' not found"))
                return

            # Fetch account utilization using sreport
            self.status_text.set_text(f"Fetching usage statistics for {query}...")
            self.main_screen.loop.draw_screen()

            result = self.sreport_fetcher.fetch_user_utilization(query)
            if result:
                self.main_screen.close_overlay()
                self.on_result(result, 'user', query)
            else:
                self.status_text.set_text(("error", f"Could not fetch usage data for user '{query}'"))
