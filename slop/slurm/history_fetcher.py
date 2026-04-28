"""Common job history fetcher for views that need sacct data."""
import threading
from slop.models import Job
from slop.slurm import is_running, is_pending


class HistoryFetcher:
    """Reusable component for fetching job history from sacct with adaptive strategy."""

    def __init__(self, main_screen, adaptive_sacct):
        """Initialize the history fetcher.

        Args:
            main_screen: Main screen instance for UI updates
            adaptive_sacct: AdaptiveSacctFetcher instance
        """
        self.main_screen = main_screen
        self.adaptive_sacct = adaptive_sacct

        # State
        self.history_jobs = []  # List of Job objects from sacct
        self.loading = False
        self.fetch_started = False
        self.sacct_thread = None
        self._cancelled = False

        # Callbacks - set by caller
        self.on_progress = None  # Called with progress status dict
        self.on_complete = None  # Called with filtered/sorted jobs list

    def start_fetch(self, entity_type, entity_name):
        """Start background sacct fetch.

        Args:
            entity_type: 'user' or 'account'
            entity_name: Username or account name
        """
        if not self.adaptive_sacct or self.loading or self.fetch_started:
            return

        self.fetch_started = True
        self.loading = True

        def fetch_worker():
            """Worker thread to fetch sacct data."""
            def progress_callback(status):
                """Handle progress updates from sacct fetcher."""
                self.main_screen.schedule_main(self._handle_progress, status)

            # Fetch data
            if entity_type == 'user':
                result = self.adaptive_sacct.fetch_user_jobs(entity_name, progress_callback)
            else:
                result = self.adaptive_sacct.fetch_account_jobs(entity_name, progress_callback)

            self.main_screen.schedule_main(self._handle_complete, result)

        # Start thread
        self.sacct_thread = threading.Thread(target=fetch_worker, daemon=True)
        self.sacct_thread.start()

    def cancel(self):
        """Mark the in-flight fetch as cancelled.

        The worker thread keeps running (sacct is non-cancellable), but its
        eventual progress / completion callbacks become no-ops, so an orphaned
        fetcher can't mutate widgets that have been replaced.
        """
        self._cancelled = True

    def _handle_progress(self, status):
        """Handle progress updates."""
        if self._cancelled:
            return
        if self.on_progress:
            self.on_progress(status)

    def _handle_complete(self, result):
        """Handle sacct fetch completion."""
        if self._cancelled:
            return
        self.loading = False

        if result and result.get('jobs'):
            job_dicts = result['jobs']
            meta = result.get('meta', {})

            # Create Job objects from sacct data
            all_jobs = [Job(job_dict) for job_dict in job_dicts]

            # Filter out running and pending jobs (they're already in current jobs view)
            history_jobs = [job for job in all_jobs if not is_running(job) and not is_pending(job)]

            # Sort by submission time (newest first)
            def get_submission_time(job):
                if hasattr(job, 'time') and isinstance(job.time, dict):
                    return job.time.get('submission', 0)
                elif hasattr(job, 'submit_time') and isinstance(job.submit_time, dict):
                    return job.submit_time.get('number', 0)
                return 0

            self.history_jobs = sorted(history_jobs, key=get_submission_time, reverse=True)

            # Call completion callback with jobs and metadata
            if self.on_complete:
                self.on_complete(self.history_jobs, meta)
        else:
            # Failed or no jobs — clear `fetch_started` so the caller can retry
            # (e.g. the user re-opens F6 after a transient sacct timeout).
            self.history_jobs = []
            self.fetch_started = False
            if self.on_complete:
                self.on_complete([], {})

    def get_progress_text(self, status):
        """Convert status dict to human-readable text.

        Args:
            status: Status dict from adaptive_sacct progress callback

        Returns:
            Tuple of (summary_text, detail_text) strings
        """
        stage = status.get('stage')

        if stage == 'trying':
            window = status.get('window', '')
            attempt = status.get('attempt', 0)
            total = status.get('total_attempts', 0)
            summary = f"Fetching job history... Trying {window} ({attempt}/{total})"
            detail = f"Trying {window} ({attempt}/{total})..."

        elif stage == 'slow':
            window = status.get('window', '')
            duration = status.get('duration', 0)
            threshold = status.get('threshold', 5)
            summary = f"Fetching job history... {window} too slow, trying shorter window"
            detail = f"{window} too slow ({duration:.1f}s > {threshold}s), trying shorter window..."

        elif stage == 'empty':
            window = status.get('window', '')
            summary = f"Fetching job history... {window} returned no data, trying shorter window"
            detail = f"{window} returned no data, trying shorter window..."

        elif stage == 'success':
            summary = "Job history loaded"
            detail = None

        elif stage == 'failed':
            failures = status.get('failures', 0)
            backoff = status.get('backoff_minutes', 0)
            summary = f"Query failed - will retry in {backoff} minutes"
            detail = f"Query failed ({failures} attempts), will retry in {backoff} minutes"

        elif stage == 'backoff':
            wait = status.get('wait_seconds', 0)
            failures = status.get('failures', 0)
            summary = f"In retry backoff - next retry in {wait}s"
            detail = f"In retry backoff ({failures} prior failures), next retry in {wait}s"

        else:
            summary = "Fetching job history..."
            detail = None

        return (summary, detail)
