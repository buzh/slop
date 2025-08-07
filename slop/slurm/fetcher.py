import subprocess
import json

class SlurmJobFetcher:
    def __init__(self, loop=None):
        self.jobs = {"jobs": []}
        self.interval = 3
        self.loop = loop or asyncio.get_event_loop()

    async def get_json(self):
        try:
            cmd = [ "scontrol", "--json", "show", "jobs" ]
            ret = await self.loop.run_in_executor(
                None,
                lambda: subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=10)
            )
            jobs = json.loads(ret.stdout)

            self.jobs = jobs

        except Exception as e:
            print(f"Error running scontrol: {e}")
            return None

    def fetch_sync(self):
        return self.jobs.copy()

    async def fetch(self):
        return self.jobs.copy()

    async def update_once(self):
        await self.get_json()

__all__ = [
    "SlurmJobFetcher",
]
