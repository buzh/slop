import subprocess
import json
import datetime

class SlurmJobFetcher:
    def __init__(self, loop=None):
        self.jobs = {"jobs": []}
        self.loop = loop or asyncio.get_event_loop()
        self.timeout_initial = 10
        self.timeout_max = 120
        self.timeout = 10
        self.last_fetch_duration = 0

    async def get_json(self):
        self.timeout = self.timeout_initial
        if self.timeout <= self.timeout_max:
            try:
                cmd = [ "scontrol", "--json", "show", "jobs" ]
                start = datetime.datetime.now()
                ret = await self.loop.run_in_executor(
                    None,
                    lambda: subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=self.timeout)
                )
                done = datetime.datetime.now()
                self.last_fetch_duration = done - start
                jobs = json.loads(ret.stdout)

                self.jobs = jobs

            except subprocess.TimeoutExpired:
                ### add ui feedback stuff here ###
                self.timeout += 5

            except Exception as e:
                print(f"Error running scontrol: {e}")
                return None
        else:
           print("Error: maximum scontrol timeout reached, unable to fetch job data")
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
