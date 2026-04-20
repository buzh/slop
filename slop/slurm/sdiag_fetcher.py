"""Asynchronously fetch slurmctld diagnostics via `sdiag --json`."""
import asyncio
import datetime
import json
import os
import subprocess


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


__all__ = ["SlurmSdiagFetcher"]
