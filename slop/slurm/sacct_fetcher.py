"""Async sacct fetcher for job accounting data."""

import asyncio
import subprocess
import json
import time
from datetime import timedelta
from slop.slurm.job_cache import JobCache
from slop.anonymize import (
    anonymize_user, anonymize_account, anonymize_node,
    anonymize_job_name, anonymize_path, is_demo_mode
)

__all__ = ["SacctFetcher"]


class SacctFetcher:
    """Fetch job accounting data using sacct command."""

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.timeout = 10
        self.max_timeout = 120
        self.last_fetch_duration = timedelta(0)
        self.cache = JobCache()

    def _anonymize_sacct_data(self, data):
        """Anonymize sacct JSON data if demo mode is enabled."""
        if not is_demo_mode() or not data or 'jobs' not in data:
            return data

        for job in data['jobs']:
            # Anonymize user and account
            if 'user' in job:
                job['user'] = anonymize_user(job['user'])
            if 'account' in job:
                job['account'] = anonymize_account(job['account'])
            if 'name' in job:
                job['name'] = anonymize_job_name(job['name'])

            # Anonymize nodes
            if 'nodes' in job and job['nodes']:
                job['nodes'] = anonymize_node(job['nodes'])

            # Anonymize paths
            if 'working_directory' in job and job['working_directory']:
                job['working_directory'] = anonymize_path(job['working_directory'])
            if 'stdout' in job and job['stdout']:
                job['stdout'] = anonymize_path(job['stdout'])
            if 'stderr' in job and job['stderr']:
                job['stderr'] = anonymize_path(job['stderr'])
            if 'stdout_expanded' in job and job['stdout_expanded']:
                job['stdout_expanded'] = anonymize_path(job['stdout_expanded'])
            if 'stderr_expanded' in job and job['stderr_expanded']:
                job['stderr_expanded'] = anonymize_path(job['stderr_expanded'])

            # Anonymize submit command if present
            if 'submit_line' in job and job['submit_line']:
                # Just anonymize paths in the command
                job['submit_line'] = anonymize_path(job['submit_line'])

        return data

    async def fetch_job(self, job_id):
        """Fetch accounting data for a specific job ID.

        Args:
            job_id: Job ID to query

        Returns:
            dict: Parsed JSON data or None on error
        """
        cmd = ["sacct", "--json", "-j", str(job_id)]
        return await self._run_sacct(cmd)

    async def fetch_user_history(self, username, weeks=52):
        """Fetch job history for a user.

        Args:
            username: Username to query
            weeks: Number of weeks of history (default 52)

        Returns:
            dict: Parsed JSON data or None on error
        """
        cmd = ["sacct", "--json", "-u", username, "-S", f"now-{weeks}weeks"]
        return await self._run_sacct(cmd)

    async def fetch_account_history(self, account, weeks=52):
        """Fetch job history for an account.

        Args:
            account: Account name to query
            weeks: Number of weeks of history (default 52)

        Returns:
            dict: Parsed JSON data or None on error
        """
        cmd = ["sacct", "--json", "-A", account, "-S", f"now-{weeks}weeks"]
        return await self._run_sacct(cmd)

    async def fetch_node_history(self, node, weeks=52):
        """Fetch job history for a node.

        Args:
            node: Node name to query
            weeks: Number of weeks of history (default 52)

        Returns:
            dict: Parsed JSON data or None on error
        """
        cmd = ["sacct", "--json", "-N", node, "-S", f"now-{weeks}weeks"]
        return await self._run_sacct(cmd)

    async def _run_sacct(self, cmd):
        """Run sacct command and return parsed JSON.

        Args:
            cmd: Command list to execute

        Returns:
            dict: Parsed JSON data or None on error
        """
        import time
        start_time = time.time()

        try:
            result = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                ),
                timeout=self.timeout
            )
            stdout, stderr = await result.communicate()

            self.last_fetch_duration = timedelta(seconds=time.time() - start_time)

            if result.returncode == 0:
                data = json.loads(stdout.decode('utf-8'))
                return self._anonymize_sacct_data(data)
            else:
                return None

        except asyncio.TimeoutError:
            # Increase timeout for next time
            self.timeout = min(self.timeout + 5, self.max_timeout)
            self.last_fetch_duration = timedelta(seconds=self.timeout)
            return None
        except Exception:
            return None

    def _run_sacct_sync(self, cmd):
        """Run sacct command synchronously (blocking).

        Args:
            cmd: Command list to execute

        Returns:
            dict: Parsed JSON data or None on error
        """
        import time
        start_time = time.time()

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.timeout
            )

            self.last_fetch_duration = timedelta(seconds=time.time() - start_time)

            if result.returncode == 0:
                data = json.loads(result.stdout.decode('utf-8'))
                return self._anonymize_sacct_data(data)
            else:
                return None

        except subprocess.TimeoutExpired:
            # Increase timeout for next time
            self.timeout = min(self.timeout + 5, self.max_timeout)
            self.last_fetch_duration = timedelta(seconds=self.timeout)
            return None
        except Exception:
            return None

    def fetch_job_sync(self, job_id):
        """Synchronously fetch accounting data for a specific job ID."""
        cmd = ["sacct", "--json", "-j", str(job_id)]
        return self._run_sacct_sync(cmd)

    def fetch_user_history_sync(self, username, weeks=52):
        """Synchronously fetch job history for a user."""
        cmd = ["sacct", "--json", "-u", username, "-S", f"now-{weeks}weeks"]
        return self._run_sacct_sync(cmd)

    def fetch_account_history_sync(self, account, weeks=52):
        """Synchronously fetch job history for an account."""
        cmd = ["sacct", "--json", "-A", account, "-S", f"now-{weeks}weeks"]
        return self._run_sacct_sync(cmd)

    def fetch_node_history_sync(self, node, weeks=52):
        """Synchronously fetch job history for a node."""
        cmd = ["sacct", "--json", "-N", node, "-S", f"now-{weeks}weeks"]
        return self._run_sacct_sync(cmd)

    def fetch_adaptive_sync(self, search_type, search_value, progress_callback=None, max_time=10.0, max_jobs=50000):
        """Adaptively fetch job history with caching and progressive time windows.

        Strategy:
        - Always fetch recent jobs (last 2 hours) from sacct to get new/updated jobs
        - For older time windows, use cache exclusively (much faster)
        - Only expand to older windows if needed and cache lookup is fast

        Args:
            search_type: 'user', 'account', or 'node'
            search_value: Username, account name, or node name
            progress_callback: Optional callback(status_dict) for progress updates
            max_time: Maximum time to spend fetching (seconds)
            max_jobs: Maximum number of jobs to fetch

        Returns:
            dict: Combined cached + fresh job data with metadata
        """
        start_time = time.time()
        all_jobs = {}  # De-duplicate by job_id
        total_cached = 0
        total_fresh = 0

        # STEP 1: Always fetch recent jobs from sacct (last 2 hours)
        # This is fast and gets new jobs + state updates for running/pending jobs
        if progress_callback:
            progress_callback({
                'stage': 'fetch',
                'window': 'recent',
                'hours': 2,
                'jobs_count': 0,
                'cached': 0,
                'fresh': 0
            })

        fetch_start = time.time()
        recent_data = None
        if search_type == 'user':
            cmd = ["sacct", "--json", "-u", search_value, "-S", "now-2hours"]
            recent_data = self._run_sacct_sync(cmd)
        elif search_type == 'account':
            cmd = ["sacct", "--json", "-A", search_value, "-S", "now-2hours"]
            recent_data = self._run_sacct_sync(cmd)
        elif search_type == 'node':
            cmd = ["sacct", "--json", "-N", search_value, "-S", "now-2hours"]
            recent_data = self._run_sacct_sync(cmd)

        fetch_duration = time.time() - fetch_start

        # Merge recent jobs
        if recent_data and recent_data.get('jobs'):
            for job in recent_data['jobs']:
                job_id = job.get('job_id')
                if job_id:
                    all_jobs[job_id] = job
                    total_fresh += 1

            # Cache the finished jobs
            self.cache.cache_jobs(recent_data)

        if progress_callback:
            progress_callback({
                'stage': 'complete',
                'window': 'recent',
                'hours': 2,
                'jobs_count': len(all_jobs),
                'cached': 0,
                'fresh': total_fresh,
                'fetch_duration': fetch_duration
            })

        # STEP 2: Expand to older time windows
        # Strategy: Try cache first, fall back to sacct if cache is empty/sparse
        # Progressive windows: 6h, 1d, 3d, 1w, 2w, 4w, 12w, 52w
        cache_windows = [
            ('6hours', 6),
            ('1day', 24),
            ('3days', 72),
            ('1week', 168),
            ('2weeks', 336),
            ('4weeks', 672),
            ('12weeks', 2016),
            ('52weeks', 8736),
        ]

        # Time thresholds for sacct fetches (more lenient than cache)
        sacct_time_thresholds = [0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 10.0]

        for idx, (window_name, hours) in enumerate(cache_windows):
            # Check if we've exceeded limits
            elapsed = time.time() - start_time
            if elapsed > max_time or len(all_jobs) >= max_jobs:
                break

            # Get cached jobs for this window
            window_start = int(time.time()) - (hours * 3600)

            if progress_callback:
                progress_callback({
                    'stage': 'cache',
                    'window': window_name,
                    'hours': hours,
                    'jobs_count': len(all_jobs),
                    'cached': total_cached,
                    'fresh': total_fresh
                })

            cache_start = time.time()
            cached_data = None
            if search_type == 'user':
                cached_data = self.cache.get_cached_jobs(window_start, user=search_value)
            elif search_type == 'account':
                cached_data = self.cache.get_cached_jobs(window_start, account=search_value)

            cache_duration = time.time() - cache_start

            # Merge cached jobs (skip ones we already have from recent fetch)
            new_cached = 0
            if cached_data and cached_data.get('jobs'):
                for job in cached_data['jobs']:
                    job_id = job.get('job_id')
                    if job_id and job_id not in all_jobs:
                        all_jobs[job_id] = job
                        total_cached += 1
                        new_cached += 1

            # If cache is empty or very sparse, AND we don't have many jobs yet, try sacct
            # This handles first-time searches for users with older jobs
            if new_cached < 5 and len(all_jobs) < 100:
                if progress_callback:
                    progress_callback({
                        'stage': 'fetch',
                        'window': window_name,
                        'hours': hours,
                        'jobs_count': len(all_jobs),
                        'cached': total_cached,
                        'fresh': total_fresh
                    })

                fetch_start = time.time()
                sacct_data = None
                if search_type == 'user':
                    cmd = ["sacct", "--json", "-u", search_value, "-S", f"now-{hours}hours"]
                    sacct_data = self._run_sacct_sync(cmd)
                elif search_type == 'account':
                    cmd = ["sacct", "--json", "-A", search_value, "-S", f"now-{hours}hours"]
                    sacct_data = self._run_sacct_sync(cmd)
                elif search_type == 'node':
                    cmd = ["sacct", "--json", "-N", search_value, "-S", f"now-{hours}hours"]
                    sacct_data = self._run_sacct_sync(cmd)

                fetch_duration = time.time() - fetch_start

                # Merge sacct jobs
                new_from_sacct = 0
                if sacct_data and sacct_data.get('jobs'):
                    for job in sacct_data['jobs']:
                        job_id = job.get('job_id')
                        if job_id and job_id not in all_jobs:
                            all_jobs[job_id] = job
                            total_fresh += 1
                            new_from_sacct += 1

                    # Cache the finished jobs for next time
                    self.cache.cache_jobs(sacct_data)

                if progress_callback:
                    progress_callback({
                        'stage': 'complete',
                        'window': window_name,
                        'hours': hours,
                        'jobs_count': len(all_jobs),
                        'cached': total_cached,
                        'fresh': total_fresh,
                        'fetch_duration': fetch_duration,
                        'new_in_window': new_from_sacct
                    })

                # Stop if this sacct fetch was slow
                if idx < len(sacct_time_thresholds) and fetch_duration > sacct_time_thresholds[idx]:
                    break

                # Stop if we found enough jobs
                if len(all_jobs) > 50:
                    break

            else:
                # Cache had data - report it
                if progress_callback:
                    progress_callback({
                        'stage': 'complete',
                        'window': window_name,
                        'hours': hours,
                        'jobs_count': len(all_jobs),
                        'cached': total_cached,
                        'fresh': total_fresh,
                        'fetch_duration': cache_duration,
                        'new_in_window': new_cached
                    })

                # Stop if we didn't find many new jobs in cache
                if new_cached < 10 and idx > 0:
                    break

            # Stop if we have a lot of jobs already
            if len(all_jobs) > 5000 * (idx + 1):
                break

        # Return combined result
        return {
            'jobs': list(all_jobs.values()),
            'meta': {
                'total_jobs': len(all_jobs),
                'cached_jobs': total_cached,
                'fresh_jobs': total_fresh,
                'total_duration': time.time() - start_time
            }
        }
