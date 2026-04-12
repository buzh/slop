"""SQLite-based job cache for sacct data."""

import sqlite3
import json
import os
import time
import fcntl
from pathlib import Path
from contextlib import contextmanager


class JobCache:
    """Cache for finished Slurm jobs to avoid repeated sacct queries."""

    # Terminal states that won't change
    FINISHED_STATES = {
        'BOOT_FAIL', 'CANCELLED', 'COMPLETED', 'DEADLINE', 'FAILED',
        'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED', 'TIMEOUT'
    }

    def __init__(self, cache_dir=None):
        """Initialize job cache.

        Args:
            cache_dir: Directory for cache file (default: ~/.cache/slop)
        """
        if cache_dir is None:
            cache_dir = Path.home() / '.cache' / 'slop'
        else:
            cache_dir = Path(cache_dir)

        cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = cache_dir / 'jobs.db'
        self.lock_path = cache_dir / 'jobs.db.lock'

        # Initialize database
        self._init_db()

    @contextmanager
    def _db_lock(self, timeout=10):
        """Acquire file lock for database access.

        This prevents corruption when multiple slop instances access
        the database on a shared filesystem.
        """
        lock_file = None
        try:
            # Create lock file
            lock_file = open(self.lock_path, 'w')

            # Try to acquire exclusive lock with timeout
            start_time = time.time()
            while True:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.time() - start_time > timeout:
                        raise TimeoutError("Could not acquire database lock")
                    time.sleep(0.1)

            yield

        finally:
            if lock_file:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                except:
                    pass

    def _get_connection(self):
        """Get database connection with proper settings."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        # Use WAL mode for better concurrent access
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize database schema, recreating if corrupted."""
        try:
            with self._db_lock():
                conn = self._get_connection()
                try:
                    conn.execute('''
                        CREATE TABLE IF NOT EXISTS jobs (
                            job_id TEXT NOT NULL,
                            cluster TEXT NOT NULL DEFAULT '',
                            user TEXT,
                            account TEXT,
                            state TEXT,
                            submit_time INTEGER,
                            end_time INTEGER,
                            data TEXT,
                            cached_at INTEGER,
                            PRIMARY KEY (job_id, cluster)
                        )
                    ''')

                    # Create indices for fast lookups
                    conn.execute('''
                        CREATE INDEX IF NOT EXISTS idx_user_submit
                        ON jobs(user, submit_time)
                    ''')
                    conn.execute('''
                        CREATE INDEX IF NOT EXISTS idx_account_submit
                        ON jobs(account, submit_time)
                    ''')
                    conn.execute('''
                        CREATE INDEX IF NOT EXISTS idx_state
                        ON jobs(state)
                    ''')
                    conn.execute('''
                        CREATE INDEX IF NOT EXISTS idx_end_time
                        ON jobs(end_time)
                    ''')

                    conn.commit()
                    conn.close()
                except sqlite3.DatabaseError:
                    # Database is corrupted - recreate it
                    conn.close()
                    self._recreate_db()
        except Exception:
            # If anything goes wrong, try to recreate
            self._recreate_db()

    def _recreate_db(self):
        """Recreate corrupted database."""
        try:
            if self.db_path.exists():
                # Move corrupted DB to backup
                backup_path = self.db_path.with_suffix('.db.corrupted')
                self.db_path.rename(backup_path)

            # Recreate fresh database
            conn = self._get_connection()
            conn.execute('''
                CREATE TABLE jobs (
                    job_id TEXT NOT NULL,
                    cluster TEXT NOT NULL DEFAULT '',
                    user TEXT,
                    account TEXT,
                    state TEXT,
                    submit_time INTEGER,
                    end_time INTEGER,
                    data TEXT,
                    cached_at INTEGER,
                    PRIMARY KEY (job_id, cluster)
                )
            ''')
            conn.execute('CREATE INDEX idx_user_submit ON jobs(user, submit_time)')
            conn.execute('CREATE INDEX idx_account_submit ON jobs(account, submit_time)')
            conn.execute('CREATE INDEX idx_state ON jobs(state)')
            conn.execute('CREATE INDEX idx_end_time ON jobs(end_time)')
            conn.commit()
            conn.close()
        except Exception:
            # Last resort - just delete and let next init recreate
            if self.db_path.exists():
                self.db_path.unlink()

    def cache_jobs(self, jobs_data, cluster='default'):
        """Cache finished jobs from sacct JSON data.

        Args:
            jobs_data: Parsed sacct JSON output (dict with 'jobs' key)
            cluster: Cluster identifier (default: 'default')

        Returns:
            int: Number of jobs cached
        """
        if not jobs_data or 'jobs' not in jobs_data:
            return 0

        jobs = jobs_data['jobs']
        cached_count = 0
        now = int(time.time())

        try:
            with self._db_lock():
                conn = self._get_connection()
                try:
                    for job in jobs:
                        # Only cache finished jobs
                        state_info = job.get('state', {})
                        states = state_info.get('current', [])
                        if not states:
                            continue

                        # Check if any state is terminal
                        is_finished = any(s in self.FINISHED_STATES for s in states)
                        if not is_finished:
                            continue

                        job_id = job.get('job_id', '')
                        if not job_id:
                            continue

                        # Extract metadata
                        user = job.get('user', '')
                        account = job.get('account', '')
                        state = ','.join(states)

                        time_info = job.get('time', {})
                        submit_time = time_info.get('submission', 0)
                        end_time = time_info.get('end', 0)

                        # Store complete job data as JSON
                        data_json = json.dumps(job)

                        # Insert or replace
                        conn.execute('''
                            INSERT OR REPLACE INTO jobs
                            (job_id, cluster, user, account, state, submit_time, end_time, data, cached_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (job_id, cluster, user, account, state, submit_time, end_time, data_json, now))

                        cached_count += 1

                    conn.commit()
                    conn.close()
                except Exception:
                    conn.close()
                    # Try to recreate if database is corrupted
                    self._recreate_db()
        except Exception:
            pass  # Fail silently - caching is optional

        return cached_count

    def get_cached_jobs(self, start_time_epoch, user=None, account=None, node=None, cluster='default'):
        """Get cached jobs from specified time window.

        Args:
            start_time_epoch: Unix timestamp for start of window
            user: Filter by username (optional)
            account: Filter by account (optional)
            node: Filter by node (optional - currently not indexed)
            cluster: Cluster identifier

        Returns:
            dict: Parsed jobs data in sacct JSON format, or None on error
        """
        try:
            with self._db_lock(timeout=5):
                conn = self._get_connection()
                try:
                    # Build query
                    query = '''
                        SELECT data FROM jobs
                        WHERE cluster = ? AND submit_time >= ?
                    '''
                    params = [cluster, start_time_epoch]

                    if user:
                        query += ' AND user = ?'
                        params.append(user)

                    if account:
                        query += ' AND account = ?'
                        params.append(account)

                    # Note: node filtering would require parsing JSON data
                    # We'll handle that in the caller if needed

                    cursor = conn.execute(query, params)
                    rows = cursor.fetchall()
                    conn.close()

                    # Parse job data
                    jobs = []
                    for row in rows:
                        try:
                            job_data = json.loads(row['data'])
                            jobs.append(job_data)
                        except:
                            continue

                    return {'jobs': jobs}

                except Exception:
                    conn.close()
                    return None
        except Exception:
            return None

    def get_stats(self):
        """Get cache statistics.

        Returns:
            dict: Statistics about cached jobs
        """
        try:
            with self._db_lock(timeout=5):
                conn = self._get_connection()
                try:
                    cursor = conn.execute('SELECT COUNT(*) as count FROM jobs')
                    total = cursor.fetchone()['count']

                    cursor = conn.execute('SELECT COUNT(DISTINCT user) as count FROM jobs')
                    users = cursor.fetchone()['count']

                    cursor = conn.execute('SELECT MIN(submit_time) as oldest FROM jobs')
                    oldest = cursor.fetchone()['oldest']

                    conn.close()

                    return {
                        'total_jobs': total,
                        'unique_users': users,
                        'oldest_job': oldest
                    }
                except Exception:
                    conn.close()
                    return None
        except Exception:
            return None
