"""Asynchronously fetch slurmctld diagnostics via `sdiag --json`."""
import asyncio
import datetime
import json
import os
import subprocess


def _n(value, default=0):
    """Read a sdiag field that may be a bare int or a Slurm 23+ wrapper.

    Newer Slurm versions report numeric fields as `{'set': bool, 'infinite':
    bool, 'number': N}`. Older ones (and some fields even on newer) are bare
    ints. This handles both shapes.
    """
    if isinstance(value, dict):
        return value.get('number', default)
    return value if value is not None else default


class SlurmSdiagFetcher:
    """Periodic wrapper around `sdiag --json`.

    The shape of the data exposed via `fetch_sync()` mirrors the JSON returned
    by sdiag, so consumers operate on `data['statistics']` directly.
    """

    def __init__(self, loop=None, offline_data_dir=None):
        self.data = {}
        self.loop = loop or asyncio.get_event_loop()
        self.timeout = 10
        self.last_fetch_duration = datetime.timedelta(0)
        self.last_error = None
        self.offline_data_dir = offline_data_dir

    async def fetch(self):
        if self.offline_data_dir:
            try:
                start = datetime.datetime.now()
                with open(os.path.join(self.offline_data_dir, 'sdiag.json')) as f:
                    self.data = json.load(f)
                self.last_fetch_duration = datetime.datetime.now() - start
                self.last_error = None
            except Exception as e:
                self.last_error = str(e)
            return self.data

        try:
            start = datetime.datetime.now()
            result = await self.loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    ["sdiag", "--json"],
                    check=True, capture_output=True, text=True, timeout=self.timeout,
                ),
            )
            self.last_fetch_duration = datetime.datetime.now() - start
            self.data = json.loads(result.stdout)
            self.last_error = None
        except Exception as e:
            self.last_error = str(e)
        return self.data

    def fetch_sync(self):
        return self.data

    async def update_once(self):
        await self.fetch()

    def compute_signals(self):
        """Extract load-gating signals from the latest sdiag snapshot.

        Returns None if no data is available yet, otherwise a dict with:
            pending_count, latency_us, rji_queued, rji_avg_us, rji_dropped
        REQUEST_JOB_INFO is the RPC slop drives via `scontrol show jobs`.
        """
        if not self.data:
            return None
        s = self.data.get('statistics') or {}
        if not s:
            return None
        rji = next(
            (r for r in s.get('rpcs_by_message_type', []) or []
             if r.get('message_type') == 'REQUEST_JOB_INFO'),
            {},
        )
        return {
            'pending_count': len(s.get('pending_rpcs') or []),
            'latency_us': _n(s.get('gettimeofday_latency')),
            'rji_queued': _n(rji.get('queued')),
            'rji_avg_us': _n(rji.get('average_time')),
            'rji_dropped': _n(rji.get('dropped')),
        }


__all__ = ["SlurmSdiagFetcher"]
