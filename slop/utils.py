import re
import time
import datetime


def compress_hostlist(names):
    """Compress hostnames to Slurm-style hostlist syntax.

    Splits each name at its trailing-digit suffix and groups by the alpha
    prefix; numeric suffixes within a group are run-length encoded.

        ['b1101','b1102','b1104','login-1','login-2','robinhood']
        -> 'b[1101-1102,1104],login-[1-2],robinhood'

    A single occurrence of a prefix renders without brackets ('b1101'),
    multiple occurrences always do ('b[1101]' would be silly even for n=1
    if it ever showed up). Names with no numeric suffix pass through.
    Width/zero-padding is not preserved — modern Slurm clusters usually
    don't use it, and the surrounding view treats this as a display string
    rather than something to round-trip back into Slurm.
    """
    if not names:
        return ""

    pat = re.compile(r'^(.*?)(\d+)$')
    grouped = {}     # prefix -> [int, ...]
    standalone = []  # names with no trailing digits
    order = []       # prefixes in first-seen order
    for name in names:
        m = pat.match(name)
        if m:
            prefix, num = m.group(1), int(m.group(2))
            if prefix not in grouped:
                grouped[prefix] = []
                order.append(prefix)
            grouped[prefix].append(num)
        else:
            standalone.append(name)

    parts = []
    for prefix in order:
        nums = grouped[prefix]
        if len(nums) == 1:
            parts.append(f"{prefix}{nums[0]}")
        else:
            parts.append(f"{prefix}[{compress_int_range(nums)}]")
    parts.extend(standalone)
    return ",".join(parts)


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


def _parse_tres(tres_str):
    """Parse a Slurm TRES string into {key: value}, plus a gputype side channel."""
    parsed = {}
    gputype = None
    for entry in tres_str.split(','):
        if '=' not in entry:
            continue
        key, value = entry.split('=', 1)
        if key.startswith('gres/gpu:'):
            gputype = key.split(':', 1)[1]
        else:
            parsed[key] = value
    return parsed, gputype


def _format_mem(mem_str):
    """'64000M' -> '64G', '512M' -> '512M', '2T' -> '2T'."""
    if not mem_str:
        return ''
    m = re.match(r'^(\d+(?:\.\d+)?)([KMGT]?)$', mem_str.strip())
    if not m:
        return mem_str
    val = float(m.group(1))
    unit = m.group(2) or 'M'
    if unit == 'M' and val >= 1024:
        val /= 1024
        unit = 'G'
    if unit == 'G' and val >= 1024:
        val /= 1024
        unit = 'T'
    if val == int(val):
        return f"{int(val)}{unit}"
    return f"{val:.1f}{unit}"


def nice_tres(job):
    """Verbose TRES summary, e.g. '16 cores, 64000M mem, 2 GPUs (a100), 1 nodes'."""
    tres_str = getattr(job, 'tres_alloc_str', '') or getattr(job, 'tres_req_str', '')
    if not tres_str or not tres_str.strip():
        return ''
    req_tres, gputype = _parse_tres(tres_str)

    parts = []
    if 'cpu' in req_tres:
        parts.append(f"{req_tres['cpu']} cores")
    if 'mem' in req_tres:
        parts.append(f"{req_tres['mem']} mem")
    if 'gres/gpu' in req_tres:
        parts.append(f"{req_tres['gres/gpu']} GPUs ({gputype})")
    if 'node' in req_tres:
        parts.append(f"{req_tres['node']} nodes")

    return ', '.join(parts)


def compact_tres(job):
    """Compact one-line TRES summary, e.g. '16c 64G 2×A100 4n'.

    Nodes column is dropped when the job uses a single node (the common case).
    """
    tres_str = getattr(job, 'tres_alloc_str', '') or getattr(job, 'tres_req_str', '')
    return compact_tres_str(tres_str)


def compact_tres_str(tres_str):
    """compact_tres counterpart that takes a raw TRES string (used for snapshots)."""
    if not tres_str or not tres_str.strip():
        return ''
    parsed, gputype = _parse_tres(tres_str)

    parts = []
    if 'cpu' in parsed:
        parts.append(f"{parsed['cpu']}c")
    if 'mem' in parsed:
        m = _format_mem(parsed['mem'])
        if m:
            parts.append(m)
    if 'gres/gpu' in parsed:
        n = parsed['gres/gpu']
        parts.append(f"{n}×{gputype.upper()}" if gputype else f"{n}gpu")
    nodes = parsed.get('node')
    if nodes and nodes != '1':
        parts.append(f"{nodes}n")
    return ' '.join(parts)

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


def smart_truncate(text, max_len, mode='middle', ellipsis='…'):
    """Intelligently truncate text preserving important parts.

    Args:
        text: String to truncate
        max_len: Maximum length including ellipsis
        mode: 'middle' (default), 'start', or 'end'
            - 'middle': "begin…end" - preserves both start and end (best for names with versions)
            - 'start': "…end" - preserves end (best for file paths)
            - 'end': "begin…" - preserves start (best for prefixes)
        ellipsis: Character(s) to use for truncation indicator

    Returns:
        Truncated string

    Examples:
        >>> smart_truncate("nvidia_a100_80gb", 12, 'middle')
        'nvid…80gb'
        >>> smart_truncate("/very/long/path/file.txt", 20, 'start')
        '…path/file.txt'
        >>> smart_truncate("long_prefix_name", 12, 'end')
        'long_prefix…'
    """
    if len(text) <= max_len:
        return text

    if max_len < len(ellipsis) + 2:
        # Too short to do anything meaningful
        return text[:max_len]

    if mode == 'start':
        # Keep the end (e.g., for file paths)
        keep_len = max_len - len(ellipsis)
        return ellipsis + text[-keep_len:]

    elif mode == 'end':
        # Keep the beginning
        keep_len = max_len - len(ellipsis)
        return text[:keep_len] + ellipsis

    else:  # mode == 'middle'
        # Keep both start and end
        # Reserve space for ellipsis and split remaining space
        available = max_len - len(ellipsis)
        # Favor the end slightly (often has version numbers)
        start_len = available // 2
        end_len = available - start_len
        return text[:start_len] + ellipsis + text[-end_len:]

