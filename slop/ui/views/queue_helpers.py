"""Shared helpers for the F7 (queue) and F8 (scheduler/pending-list) views.

These two views both render pending-job rows and need the same set of small
field accessors and formatters. Before this module the helpers were
copy-pasted across `queue.py` and `pending_list.py`.
"""

import re
import datetime
from slop.utils import format_duration
from slop.ui.constants import EMPTY_PLACEHOLDER


_DUR_TOKEN_RE = re.compile(r'\d+[dhms]')


def coarse_duration(seconds):
    """`format_duration` trimmed to its top 2 units (e.g. '4d3h28m31s' → '4d3h')."""
    s = format_duration(seconds)
    tokens = _DUR_TOKEN_RE.findall(s)
    return ''.join(tokens[:2]) if tokens else s


def job_priority(job):
    p = getattr(job, 'priority', {})
    if isinstance(p, dict):
        return p.get('number', 0)
    return p if isinstance(p, int) else 0


def job_partition(job):
    """First partition listed (jobs may target several comma-separated)."""
    part = getattr(job, 'partition', '') or ''
    return part.split(',', 1)[0].strip() or '(none)'


def ts(time_dict):
    """Pull a unix timestamp out of scontrol's `{set, number}` dicts."""
    if isinstance(time_dict, dict):
        return time_dict.get('number', 0) or 0
    return 0


def eta_seconds(start_time):
    """Seconds-from-now of the ETA (negative = overdue), or None if no usable ETA.

    Slurm uses a far-future placeholder (~year 2106) when it has no estimate;
    treat anything > 1y out as None.
    """
    if not isinstance(start_time, dict) or not start_time.get('set'):
        return None
    t = start_time.get('number', 0)
    if t <= 0:
        return None
    diff = t - datetime.datetime.now().timestamp()
    if diff > 365 * 24 * 3600:
        return None
    return diff


def format_eta_seconds(diff, *, unknown="Unknown"):
    """Format the result of `eta_seconds()` as a human string.

    `unknown` lets callers pick between a verbose label (the F7 ETA column)
    and the universal `EMPTY_PLACEHOLDER` (the F8 pending list).
    """
    if diff is None:
        return unknown
    if diff < -60:
        return "overdue"
    if diff < 60:
        return "now"
    return f"in {coarse_duration(int(diff))}"


def format_wait(submit_time):
    if not isinstance(submit_time, dict) or not submit_time.get('set'):
        return EMPTY_PLACEHOLDER
    submit = datetime.datetime.fromtimestamp(submit_time['number'])
    wait = int((datetime.datetime.now() - submit).total_seconds())
    return coarse_duration(wait)


def time_limit_str(job):
    tl = getattr(job, 'time_limit', {})
    if isinstance(tl, dict) and tl.get('set'):
        return coarse_duration(tl.get('number', 0) * 60)
    return EMPTY_PLACEHOLDER


def reason_attr(reason):
    if reason in ('Priority', 'Resources'):
        return 'normal'
    if reason in ('Dependency', 'JobHeldUser', 'JobHeldAdmin', 'BeginTime'):
        return 'warning'
    if 'NotAvail' in reason or 'Invalid' in reason:
        return 'error'
    return 'normal'
