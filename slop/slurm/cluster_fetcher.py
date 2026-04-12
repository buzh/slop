import subprocess
import json
import datetime
import asyncio

class SlurmClusterFetcher:
    """Asynchronously fetch Slurm cluster resource data using scontrol."""

    def __init__(self, loop=None):
        self.nodes_data = {"nodes": []}
        self.partitions_data = {"partitions": []}
        self.loop = loop or asyncio.get_event_loop()
        self.timeout = 10
        self.last_fetch_duration = datetime.timedelta(0)

    async def fetch_nodes(self):
        """Fetch node information from scontrol."""
        try:
            start = datetime.datetime.now()
            result = await self.loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    ["scontrol", "--json", "show", "nodes"],
                    check=True, capture_output=True, text=True, timeout=self.timeout
                )
            )
            self.last_fetch_duration = datetime.datetime.now() - start
            self.nodes_data = json.loads(result.stdout)
        except Exception as e:
            print(f"Error fetching nodes: {e}")

    async def fetch_partitions(self):
        """Fetch partition information from scontrol."""
        try:
            result = await self.loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    ["scontrol", "--json", "show", "partitions"],
                    check=True, capture_output=True, text=True, timeout=self.timeout
                )
            )
            self.partitions_data = json.loads(result.stdout)
        except Exception as e:
            print(f"Error fetching partitions: {e}")

    def fetch_nodes_sync(self):
        return self.nodes_data.copy()

    def fetch_partitions_sync(self):
        return self.partitions_data.copy()

    async def fetch(self):
        await self.fetch_nodes()
        await self.fetch_partitions()
        return {
            'nodes': self.nodes_data,
            'partitions': self.partitions_data
        }

    async def update_once(self):
        await self.fetch()

__all__ = [
    "SlurmClusterFetcher",
]
