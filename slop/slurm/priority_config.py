"""Read PriorityWeight* settings from `scontrol show config`.

These weights set the ceiling for each priority component and rarely change,
so they're fetched once at startup.
"""
import os
import subprocess


_KEYS = (
    'PriorityWeightAge',
    'PriorityWeightAssoc',
    'PriorityWeightFairShare',
    'PriorityWeightJobSize',
    'PriorityWeightPartition',
    'PriorityWeightQOS',
    'PriorityWeightTRES',
)


def parse_config(text):
    """Pull PriorityWeight* keys out of `scontrol show config` text."""
    out = {}
    for line in text.splitlines():
        if '=' not in line:
            continue
        k, _, v = line.partition('=')
        k = k.strip()
        v = v.strip()
        if k in _KEYS:
            try:
                out[k] = int(v)
            except ValueError:
                out[k] = 0  # null / unparsable → treat as disabled
    return out


def fetch_priority_weights(offline_data_dir=None, timeout=10):
    """Return a dict like {'PriorityWeightAge': 10080, ...}.

    Returns an empty dict on failure rather than raising — callers degrade
    gracefully (no legend / no normalization).
    """
    if offline_data_dir:
        path = os.path.join(offline_data_dir, 'scontrol-show-config.out')
        try:
            with open(path) as f:
                return parse_config(f.read())
        except FileNotFoundError:
            return {}

    try:
        result = subprocess.run(
            ['scontrol', 'show', 'config'],
            check=True, capture_output=True, text=True, timeout=timeout,
        )
        return parse_config(result.stdout)
    except Exception:
        return {}


__all__ = ['fetch_priority_weights', 'parse_config']
