"""Adaptive sacct fetcher with conservative resource usage."""

import subprocess
import json
import time
from datetime import timedelta

__all__ = ["AdaptiveSacctFetcher"]


class AdaptiveSacctFetcher:
    """Fetch sacct data adaptively to avoid overloading the accounting database.

    Strategy:
    - Try progressively smaller time windows if queries are too slow
    - Implement retry backoff if even small queries fail
    - Prioritize cluster health over comprehensive data
    """

    # Time window progression (in hours)
    TIME_WINDOWS = [
        ('1month', 720),    # ~30 days
        ('1week', 168),
        ('1day', 24),
        ('12hours', 12),
        ('6hours', 6),
    ]

    def __init__(self):
        self.timeout = 30  # Maximum time to wait for any query
        self.last_fetch_duration = timedelta(0)

        # Retry state (per entity)
        self.retry_state = {}  # {entity_key: {'failures': int, 'next_retry': timestamp}}

    def fetch_user_jobs(self, username, progress_callback=None):
        """Fetch job data for a user using adaptive time windows.

        Args:
            username: Username to query
            progress_callback: Optional callback(status_dict) for progress updates

        Returns:
            dict: {'jobs': [...], 'meta': {...}} or None on failure
        """
        return self._fetch_adaptive('user', username, progress_callback)

    def fetch_account_jobs(self, account, progress_callback=None):
        """Fetch job data for an account using adaptive time windows.

        Args:
            account: Account name to query
            progress_callback: Optional callback(status_dict) for progress updates

        Returns:
            dict: {'jobs': [...], 'meta': {...}} or None on failure
        """
        return self._fetch_adaptive('account', account, progress_callback)

    def fetch_job_sync(self, job_id):
        """Fetch a single job by ID (no time window needed).

        Args:
            job_id: Job ID to fetch

        Returns:
            dict: {'jobs': [...], 'meta': {...}} or None on failure
        """
        cmd = ['sacct', '--json', '-j', str(job_id)]
        return self._run_sacct_sync(cmd)

    def fetch_node_history_sync(self, node_name):
        """Fetch job history for a node (last 30 days).

        Args:
            node_name: Node name to query

        Returns:
            dict: {'jobs': [...], 'meta': {...}} or None on failure
        """
        cmd = ['sacct', '--json', '-N', node_name, '-S', 'now-30days']
        return self._run_sacct_sync(cmd)

    def _fetch_adaptive(self, entity_type, entity_name, progress_callback):
        """Adaptively fetch sacct data with progressive time window reduction.

        Strategy:
        1. Try each time window from largest to smallest
        2. If query takes >5 seconds, move to smaller window
        3. For 6-hour window, allow up to 15 seconds
        4. If all fail, implement retry backoff (5m, 10m, 15m)
        """
        entity_key = f"{entity_type}:{entity_name}"

        # Check if we're in retry backoff
        if entity_key in self.retry_state:
            state = self.retry_state[entity_key]
            if time.time() < state['next_retry']:
                # Still in backoff period
                wait_time = int(state['next_retry'] - time.time())
                if progress_callback:
                    progress_callback({
                        'stage': 'backoff',
                        'entity': entity_name,
                        'failures': state['failures'],
                        'wait_seconds': wait_time,
                    })
                return None

        all_jobs = []

        # Try each time window
        for idx, (window_name, hours) in enumerate(self.TIME_WINDOWS):
            is_final_window = (idx == len(self.TIME_WINDOWS) - 1)
            timeout_threshold = 15 if is_final_window else 5

            if progress_callback:
                progress_callback({
                    'stage': 'trying',
                    'window': window_name,
                    'hours': hours,
                    'attempt': idx + 1,
                    'total_attempts': len(self.TIME_WINDOWS),
                })

            # Build sacct command
            start_time_str = f"now-{hours}hours"
            if entity_type == 'user':
                cmd = ["sacct", "--json", "-u", entity_name, "-S", start_time_str]
            else:  # account
                cmd = ["sacct", "--json", "-A", entity_name, "-S", start_time_str]

            # Execute query and measure time
            query_start = time.time()
            result = self._run_sacct_sync(cmd)
            query_duration = time.time() - query_start

            if result and result.get('jobs'):
                jobs = result['jobs']
                all_jobs.extend(jobs)

                if progress_callback:
                    progress_callback({
                        'stage': 'success',
                        'window': window_name,
                        'hours': hours,
                        'jobs_count': len(jobs),
                        'duration': query_duration,
                    })

                # Success! Clear retry state
                if entity_key in self.retry_state:
                    del self.retry_state[entity_key]

                return {
                    'jobs': all_jobs,
                    'meta': {
                        'window': window_name,
                        'hours': hours,
                        'duration': query_duration,
                        'jobs_count': len(all_jobs),
                    }
                }

            # Query failed or was too slow
            if progress_callback:
                progress_callback({
                    'stage': 'slow' if query_duration >= timeout_threshold else 'empty',
                    'window': window_name,
                    'hours': hours,
                    'duration': query_duration,
                    'threshold': timeout_threshold,
                })

            # If query was fast enough but returned no data, that's OK - user has no jobs
            if query_duration < timeout_threshold and result is not None:
                if entity_key in self.retry_state:
                    del self.retry_state[entity_key]
                return {
                    'jobs': [],
                    'meta': {
                        'window': window_name,
                        'hours': hours,
                        'duration': query_duration,
                        'jobs_count': 0,
                    }
                }

            # Query was too slow - try next smaller window (unless this was the last one)
            if not is_final_window:
                continue

        # All attempts failed - implement retry backoff
        if entity_key not in self.retry_state:
            self.retry_state[entity_key] = {'failures': 0, 'next_retry': 0}

        state = self.retry_state[entity_key]
        state['failures'] += 1

        # Calculate backoff time: 5, 10, 15 minutes
        backoff_minutes = min(5 * state['failures'], 15)
        state['next_retry'] = time.time() + (backoff_minutes * 60)

        if progress_callback:
            progress_callback({
                'stage': 'failed',
                'entity': entity_name,
                'failures': state['failures'],
                'backoff_minutes': backoff_minutes,
                'retry_at': state['next_retry'],
            })

        return None

    def _run_sacct_sync(self, cmd):
        """Run sacct command synchronously.

        Args:
            cmd: Command list to execute

        Returns:
            dict: Parsed JSON data or None on error
        """
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout,
                text=True
            )

            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data
            else:
                return None

        except subprocess.TimeoutExpired:
            return None
        except Exception:
            return None

    def get_retry_status(self, entity_type, entity_name):
        """Get retry status for an entity.

        Args:
            entity_type: 'user' or 'account'
            entity_name: Username or account name

        Returns:
            dict: {'in_backoff': bool, 'failures': int, 'retry_in_seconds': int} or None
        """
        entity_key = f"{entity_type}:{entity_name}"

        if entity_key not in self.retry_state:
            return None

        state = self.retry_state[entity_key]
        retry_in = max(0, int(state['next_retry'] - time.time()))

        return {
            'in_backoff': retry_in > 0,
            'failures': state['failures'],
            'retry_in_seconds': retry_in,
        }
