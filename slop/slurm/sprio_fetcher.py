"""Asynchronously fetch per-job priority components via `sprio`.

Output is keyed by job id so views can look up component breakdowns directly.
The collector script invokes sprio with a custom --format string; the fixed
field widths below match it so empty TRES columns don't collapse on whitespace.
"""
import asyncio
import datetime
import os
import subprocess


# Field widths match the sprio --format string used by debug/collect_offlinedata.sh:
#   %10i %.16u %.12o %.12r %.8n %10Y %8A %8F %8J %8P %8Q %6N %20T %8B %8S
SPRIO_FORMAT = "%10i %.16u %.12o %.12r %.8n %10Y %8A %8F %8J %8P %8Q %6N %20T %8B %8S"

_COL_SLICES = (
    ('jobid',         0,  10),
    ('user',         11,  27),
    ('account',      28,  40),
    ('partition',    41,  53),
    ('qos_name',     54,  62),
    ('priority',     63,  73),
    ('age',          74,  82),
    ('fairshare',    83,  91),
    ('jobsize',      92, 100),
    ('partition_pri',101, 109),
    ('qos_pri',      110, 118),
    ('nice',         119, 125),
    ('tres',         126, 146),
    ('sitefactor',   147, 155),
    ('site',         156, 164),
)

_INT_FIELDS = ('priority', 'age', 'fairshare', 'jobsize',
               'partition_pri', 'qos_pri', 'nice', 'tres', 'sitefactor')


def parse_sprio(text):
    """Parse sprio output (custom format) into {jobid: {component: value}}."""
    out = {}
    for raw in text.splitlines():
        # Right-pad short lines so slicing past the end is harmless.
        line = raw.ljust(_COL_SLICES[-1][2])
        row = {}
        for name, start, end in _COL_SLICES:
            row[name] = line[start:end].strip()
        if not row['jobid']:
            continue
        try:
            jobid = int(row['jobid'])
        except ValueError:
            continue
        for f in _INT_FIELDS:
            try:
                row[f] = int(row[f]) if row[f] else 0
            except ValueError:
                row[f] = 0
        row['jobid'] = jobid
        out[jobid] = row
    return out


class SprioFetcher:
    """Periodic wrapper around `sprio --noheader --format=...`.

    `data` maps job_id (str) → dict with keys:
        jobid user account partition qos_name
        priority age fairshare jobsize partition_pri qos_pri
        nice tres sitefactor site
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
                with open(os.path.join(self.offline_data_dir, 'sprio.out')) as f:
                    self.data = parse_sprio(f.read())
                self.last_fetch_duration = datetime.datetime.now() - start
                self.last_error = None
            except FileNotFoundError:
                self.data = {}
                self.last_error = None
            except Exception as e:
                self.last_error = str(e)
            return self.data

        try:
            start = datetime.datetime.now()
            result = await self.loop.run_in_executor(
                None,
                lambda: subprocess.run(
                    ["sprio", "--noheader", f"--format={SPRIO_FORMAT}"],
                    check=True, capture_output=True, text=True, timeout=self.timeout,
                ),
            )
            self.last_fetch_duration = datetime.datetime.now() - start
            self.data = parse_sprio(result.stdout)
            self.last_error = None
        except Exception as e:
            self.last_error = str(e)
        return self.data

    def fetch_sync(self):
        return self.data


__all__ = ["SprioFetcher", "parse_sprio", "SPRIO_FORMAT"]
