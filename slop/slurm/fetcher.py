import subprocess
import json
import datetime
import asyncio

class SlurmJobFetcher:
    """Asynchronously fetch Slurm job data using scontrol."""

    def __init__(self, loop=None):
        self.jobs = {"jobs": []}
        self.loop = loop or asyncio.get_event_loop()
        self.timeout = 10
        self.timeout_max = 120
        self.last_fetch_duration = datetime.timedelta(0)

    async def get_json(self):
        """Fetch job JSON from scontrol with dynamic timeout handling."""
        if self.timeout > self.timeout_max:
            if not hasattr(self, '_max_timeout_warned'):
                self._max_timeout_warned = True
                print(f"Warning: scontrol timeout reached maximum ({self.timeout_max}s)")
            return

        try:
            start = datetime.datetime.now()
            result = await self.loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    ["scontrol", "--json", "show", "jobs"],
                    check=True, capture_output=True, text=True, timeout=self.timeout
                )
            )
            self.last_fetch_duration = datetime.datetime.now() - start
            self.jobs = json.loads(result.stdout)

        except subprocess.TimeoutExpired:
            self.timeout += 5  # Increase timeout for next attempt

        except Exception as e:
            print(f"Error fetching job data: {e}")

    def fetch_sync(self):
        return self.jobs.copy()

    async def fetch(self):
        return self.jobs.copy()

    async def update_once(self):
        await self.get_json()

__all__ = [
    "SlurmJobFetcher",
]
