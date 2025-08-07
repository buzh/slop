import re
import time
import datetime
from slop.slurm import *

def compress_int_range(numbers):
    if not numbers:
        return ""
    sorted_num = sorted(set(numbers))
    ranges = []
    start = sorted_num[0]
    end = sorted_num[0]

    for i in range(1, len(sorted_num)):
        current = sorted_num[i]
        if current == end + 1:
            end = current
        else:
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")

            start = current
            end = current

    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")

    return ",".join(ranges)


def nice_tres(job):
    if job.tres_alloc_str:
        req = job.tres_alloc_str.split(',')
    else:
        req = job.tres_req_str.split(',')
    req_tres = {}
    req_tres['gputype'] = None
    for i in req:
        key, value = i.split('=')
        if re.match(r'gres\/gpu:', key):
            x = re.split(":", key, 1)
            req_tres['gputype'] = x[1]
        else:
            req_tres[key] = value
    parts = []
    if 'cpu' in req_tres:
        parts.append(f"{req_tres['cpu']} cores")
    if 'mem' in req_tres:
        parts.append(f"{req_tres['mem']} mem")
    if 'gres/gpu' in req_tres:
        parts.append(f"{req_tres['gres/gpu']} GPUs ({req_tres['gputype']})")
    if 'node' in req_tres:
        parts.append(f"{req_tres['node']} nodes")

    return ', '.join(parts)

def format_duration(seconds): # turns seconds into 1d2h3m4s
    try:
        seconds = int(seconds)
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if days: parts.append(f"{days}d")
        if hours: parts.append(f"{hours}h")
        if minutes: parts.append(f"{minutes}m")
        if seconds or not parts: parts.append(f"{seconds}s")

        return ''.join(parts)
    except Exception:
        return "N/A"

