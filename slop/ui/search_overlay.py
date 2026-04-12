"""Search overlay for job lookup."""

import urwid as u
import re
import subprocess
from slop.anonymize import deanonymize_user, deanonymize_account


class SearchOverlay(u.WidgetWrap):
    """Overlay for searching jobs by various criteria."""

    def __init__(self, main_screen, sacct_fetcher, on_result):
        """Initialize search overlay.

        Args:
            main_screen: Main screen instance
            sacct_fetcher: SacctFetcher instance
            on_result: Callback function(result_data, search_type, search_value)
        """
        self.main_screen = main_screen
        self.sacct_fetcher = sacct_fetcher
        self.on_result = on_result
        self.overlay_height = 16

        # Build knowledge base from current cluster state
        self._build_knowledge_base()

        # Create single search input field with change callback
        self.search_edit = u.Edit("Search: ")
        u.connect_signal(self.search_edit, 'change', self._on_search_change)

        # Status text and suggestions
        self.status_text = u.Text("")
        self.suggestions_text = u.Text("", wrap='clip')

        # Tab completion state
        self.current_matches = []
        self.completion_index = 0
        self.last_completion_query = ""
        self.in_tab_completion = False  # Flag to prevent match recalculation during tab cycling

        # Build widget list
        widgets = [
            u.Text(("jobheader", "Search Jobs")),
            u.Divider("─"),
            u.Divider(),
            u.Text("Enter job ID, username, account, or node name:"),
            u.Divider(),
            self.search_edit,
            u.Divider(),
            self.suggestions_text,
            u.Divider(),
            self.status_text,
            u.Divider(),
            u.Text("Enter to search | Esc to cancel"),
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
        pile.focus_position = 5  # search_edit

    def _build_knowledge_base(self):
        """Build knowledge base from current cluster state."""
        self.known_users = set()
        self.known_accounts = set()
        self.known_nodes = set()
        self.known_job_ids = set()

        # Get users and accounts from active jobs
        if hasattr(self.main_screen, 'jobs') and hasattr(self.main_screen.jobs, 'usertable'):
            self.known_users = set(self.main_screen.jobs.usertable.keys())

        if hasattr(self.main_screen, 'jobs') and hasattr(self.main_screen.jobs, '_jobs'):
            for job in self.main_screen.jobs._jobs.values():
                if hasattr(job, 'account') and job.account:
                    self.known_accounts.add(job.account)
                if hasattr(job, 'job_id'):
                    self.known_job_ids.add(str(job.job_id))
                # Get nodes from active jobs
                if hasattr(job, 'nodes') and job.nodes:
                    # Nodes can be a string like "node01,node02" or just "node01"
                    if isinstance(job.nodes, str):
                        # Split by comma for multiple nodes
                        for node in job.nodes.split(','):
                            node = node.strip()
                            # Only add if it looks like a node name (not "N/A", usernames, etc.)
                            if node and node.lower() not in ['n/a', 'none', ''] and len(node) > 0:
                                # Basic check: should contain a number or common node prefix
                                if any(c.isdigit() for c in node) or any(prefix in node.lower() for prefix in ['node', 'gpu', 'compute', 'cn-', 'cn_']):
                                    self.known_nodes.add(node)
                    elif isinstance(job.nodes, list):
                        for node in job.nodes:
                            if node and any(c.isdigit() for c in str(node)):
                                self.known_nodes.add(str(node))

        # Get nodes from cluster data
        if hasattr(self.main_screen, 'cluster_fetcher') and hasattr(self.main_screen.cluster_fetcher, 'cluster_resources'):
            cluster = self.main_screen.cluster_fetcher.cluster_resources
            if cluster and hasattr(cluster, 'nodes'):
                for node in cluster.nodes.values():
                    if hasattr(node, 'name') and node.name:
                        self.known_nodes.add(node.name)

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

        suggestions = []

        # Find matches - prioritize users, then accounts, then nodes, then jobs
        user_matches = sorted([u for u in self.known_users if query in u.lower()])[:5]
        account_matches = sorted([a for a in self.known_accounts if query in a.lower()])[:5]
        node_matches = sorted([n for n in self.known_nodes if query in n.lower()])[:5]
        job_matches = sorted([jid for jid in self.known_job_ids if query in jid.lower()])[:5]

        # Store all matches for tab completion
        self.current_matches = user_matches + account_matches + node_matches + job_matches

        if user_matches:
            suggestions.append(f"Users: {', '.join(user_matches)}")
        if account_matches:
            suggestions.append(f"Accounts: {', '.join(account_matches)}")
        if node_matches:
            suggestions.append(f"Nodes: {', '.join(node_matches)}")
        if job_matches:
            suggestions.append(f"Jobs: {', '.join(job_matches)}")

        if suggestions:
            self.suggestions_text.set_text(("faded", " | ".join(suggestions)))
        else:
            self.suggestions_text.set_text(("faded", "No matches in current data - will search history"))
            self.current_matches = []

    def keypress(self, size, key):
        if key == 'esc':
            self.main_screen.close_overlay()
            return None

        if key == 'enter':
            self.perform_search()
            return None

        if key == 'tab':
            self._handle_tab_completion()
            return None

        # Reset completion state when user types (not tab)
        if key not in ['tab', 'enter', 'esc']:
            self.completion_index = 0
            self.last_completion_query = ""
            self.in_tab_completion = False

        return super().keypress(size, key)

    def _handle_tab_completion(self):
        """Handle tab completion for search matches."""
        if not self.current_matches:
            return

        current_text = self.search_edit.get_edit_text().strip()

        # If we're cycling through completions (repeated tabs)
        if self.in_tab_completion and len(self.current_matches) > 1:
            # Move to next match
            self.completion_index = (self.completion_index + 1) % len(self.current_matches)
        else:
            # First tab - start from beginning
            self.completion_index = 0
            self.in_tab_completion = True

        # Set the completion (temporarily disable change callback)
        completion = self.current_matches[self.completion_index]
        self.search_edit.set_edit_text(completion)
        self.search_edit.set_edit_pos(len(completion))

        # Remember this for cycle detection
        self.last_completion_query = completion

    def _detect_search_type(self, query):
        """Detect what type of search this is using known cluster data.

        Returns:
            tuple: (search_type, is_active) where search_type is 'job', 'node', 'user', or 'account'
                   and is_active indicates if we have current data (don't need sacct)
        """
        # Check exact matches for jobs first (most specific)
        if query in self.known_job_ids:
            return ('job', True)  # Active job

        # Pattern matching for job IDs (purely numeric or array format)
        if re.match(r'^\d+(_\d+)?(\[\d+(-\d+)?\])?$', query):
            return ('job', False)  # Historical job

        # Check exact matches for users (prioritize over nodes/accounts)
        if query in self.known_users:
            return ('user', True)  # Active user

        # Try case-insensitive match for users
        query_lower = query.lower()
        for user in self.known_users:
            if user.lower() == query_lower:
                return ('user', True)

        # Check accounts (exact and case-insensitive)
        if query in self.known_accounts:
            return ('account', True)  # Active account

        for account in self.known_accounts:
            if account.lower() == query_lower:
                return ('account', True)

        # Check nodes last (exact and case-insensitive)
        if query in self.known_nodes:
            return ('node', True)  # Known node

        for node in self.known_nodes:
            if node.lower() == query_lower:
                return ('node', True)

        # Pattern matching for node names (very specific patterns)
        node_patterns = [
            r'^node[-_]?\d+',  # node001, node-001, node_001
            r'^gpu[-_]?\d+',   # gpu001, gpu-001
            r'^compute[-_]?\d+',  # compute001
            r'^cn[-_]?\d+',    # cn-001, cn001
        ]
        for pattern in node_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return ('node', False)

        # Default to user search (most common), will fall back to account
        return ('user', False)

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

    def _check_account_exists(self, account):
        """Check if an account exists by looking for it in current or recent sacct data.

        Args:
            account: Account name to check

        Returns:
            bool: True if account is known or might exist, False if definitely doesn't exist
        """
        # If we know about it, it exists
        if account in self.known_accounts or account.lower() in [a.lower() for a in self.known_accounts]:
            return True

        # Otherwise we don't know - let sacct try
        return True

    def _get_active_job_data(self, job_id):
        """Get data for an active job without using sacct.

        Args:
            job_id: Job ID string

        Returns:
            dict: Job data in sacct format, or None if not found
        """
        if not hasattr(self.main_screen, 'jobs') or not hasattr(self.main_screen.jobs, '_jobs'):
            return None

        # Try to find the job
        try:
            job_id_int = int(job_id.split('_')[0])  # Handle array jobs
            if job_id_int in self.main_screen.jobs._jobs:
                job = self.main_screen.jobs._jobs[job_id_int]
                # Job exists - but we need sacct format for the detail view
                # Just return None and let sacct handle it for now
                # TODO: Could convert Job object to sacct format if needed
                return None
        except:
            pass

        return None

    def perform_search(self):
        """Execute search based on input, auto-detecting the type."""
        query = self.search_edit.get_edit_text().strip()

        if not query:
            self.status_text.set_text(("failed", "Please enter a search term"))
            return

        # Detect search type
        search_type, is_active = self._detect_search_type(query)

        # Progress callback for history searches
        def update_progress(status):
            stage = status.get('stage', 'fetch')
            window = status.get('window', '')
            jobs = status.get('jobs_count', 0)
            new_jobs = status.get('new_in_window', 0)

            if stage == 'cache':
                self.status_text.set_text(f"Cache: {window}... ({jobs} jobs)")
            elif stage == 'fetch':
                self.status_text.set_text(f"Fetching {window}...")
            elif stage == 'complete':
                fetch_time = status.get('fetch_duration', 0)
                if window == 'recent':
                    self.status_text.set_text(f"Recent: {jobs} jobs ({fetch_time:.2f}s)")
                else:
                    self.status_text.set_text(f"{window}: +{new_jobs} ({fetch_time:.3f}s) | Total: {jobs}")

            self.main_screen.loop.draw_screen()

        if search_type == 'job':
            # Search for specific job ID
            # Always use sacct for jobs to get full detail
            self.status_text.set_text(f"Searching for job {query}...")
            self.main_screen.loop.draw_screen()
            result = self.sacct_fetcher.fetch_job_sync(query)
            if result and result.get('jobs'):
                self.main_screen.close_overlay()
                self.on_result(result, 'job', query)
            else:
                self.status_text.set_text(("failed", f"Job {query} not found"))

        elif search_type == 'node':
            # Search for jobs on a node (always need sacct for history)
            self.status_text.set_text(f"Fetching history for node {query}...")
            self.main_screen.loop.draw_screen()
            result = self.sacct_fetcher.fetch_adaptive_sync('node', query, progress_callback=update_progress)
            if result and result.get('jobs'):
                self.main_screen.close_overlay()
                self.on_result(result, 'node', query)
            else:
                self.status_text.set_text(("failed", f"No jobs found on node {query}"))

        elif search_type == 'user':
            # User search - validate username first if not already known
            # In demo mode, translate demo names back to real usernames
            real_username = deanonymize_user(query)

            if not is_active:
                # Check if user exists on the system
                self.status_text.set_text(f"Checking user {query}...")
                self.main_screen.loop.draw_screen()

                if not self._check_user_exists(real_username):
                    # User doesn't exist - try as account instead
                    self.status_text.set_text(f"User '{query}' not found. Trying as account...")
                    self.main_screen.loop.draw_screen()

                    result = self.sacct_fetcher.fetch_adaptive_sync('account', query, progress_callback=update_progress)
                    if result and result.get('jobs'):
                        self.main_screen.close_overlay()
                        self.on_result(result, 'account', query)
                    else:
                        self.status_text.set_text(("failed", f"'{query}' is not a valid user or account"))
                    return

            # User exists - fetch their job history
            if is_active:
                self.status_text.set_text(f"Fetching history for user {query} (known user)...")
            else:
                self.status_text.set_text(f"Fetching history for user {query}...")
            self.main_screen.loop.draw_screen()

            result = self.sacct_fetcher.fetch_adaptive_sync('user', real_username, progress_callback=update_progress)
            if result and result.get('jobs'):
                self.main_screen.close_overlay()
                self.on_result(result, 'user', real_username)
            else:
                # User exists but no jobs found - try as account anyway
                real_account = deanonymize_account(query)
                self.status_text.set_text(f"No jobs for user {query}. Trying account...")
                self.main_screen.loop.draw_screen()

                result = self.sacct_fetcher.fetch_adaptive_sync('account', real_account, progress_callback=update_progress)
                if result and result.get('jobs'):
                    self.main_screen.close_overlay()
                    self.on_result(result, 'account', real_account)
                else:
                    self.status_text.set_text(("failed", f"No jobs found for user or account '{query}'"))

        elif search_type == 'account':
            # Account search
            # In demo mode, translate demo names back to real account names
            real_account = deanonymize_account(query)

            if is_active:
                self.status_text.set_text(f"Fetching history for account {query} (known account)...")
            else:
                self.status_text.set_text(f"Searching for account {query}...")
            self.main_screen.loop.draw_screen()

            result = self.sacct_fetcher.fetch_adaptive_sync('account', real_account, progress_callback=update_progress)
            if result and result.get('jobs'):
                self.main_screen.close_overlay()
                self.on_result(result, 'account', real_account)
            else:
                self.status_text.set_text(("failed", f"No jobs found for account '{query}'"))
