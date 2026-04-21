"""Shared display helpers for job-state symbols, colors, and width tiers.

Every job-row widget needs to map a Slurm state string to (icon, attr, short
code). Before this module those tables were copy-pasted across widgets.py,
my_jobs.py, queue.py and cluster.py with subtle drift between them.
"""
from slop.slurm import job_state_short


# Icon set used by the column-style row widgets (Users/Accounts/etc).
_COMPACT_ICONS = {
    'COMPLETED':     '✓',
    'FAILED':        '✗',
    'NODE_FAIL':     '✗',
    'OUT_OF_MEMORY': '✗',
    'BOOT_FAIL':     '✗',
    'TIMEOUT':       '⚠',
    'CANCELLED':     '⊗',
    'PREEMPTED':     '⊗',
    'DEADLINE':      '⊗',
    'RUNNING':       '↻',
    'COMPLETING':    '↻',
    'PENDING':       '⋯',
}

# Alternate icon set used by the My Jobs detail rows (designed to read well in
# the wider, single-line format with no other state text alongside).
_DETAIL_ICONS = {
    'RUNNING':       '▶',
    'COMPLETING':    '▶',
    'PENDING':       '⏸',
    'COMPLETED':     '✓',
    'FAILED':        '✗',
    'NODE_FAIL':     '✗',
    'BOOT_FAIL':     '✗',
    'TIMEOUT':       '⏱',
    'CANCELLED':     '⊗',
    'PREEMPTED':     '⊗',
    'OUT_OF_MEMORY': '⚠',
}

_STATE_ATTRS = {
    'COMPLETED':     'state_running',  # green for "good outcome"
    'RUNNING':       'state_running',
    'COMPLETING':    'state_running',
    'PENDING':       'state_pending',
    'FAILED':        'state_failed',
    'NODE_FAIL':     'state_failed',
    'OUT_OF_MEMORY': 'state_failed',
    'BOOT_FAIL':     'state_failed',
    'TIMEOUT':       'warning',
    'CANCELLED':     'faded',
    'PREEMPTED':     'faded',
    'DEADLINE':      'faded',
}


def state_icon(state, style='compact'):
    """Return a single-glyph indicator for `state`.

    `style='compact'` uses the icon set shared by the column-list widgets;
    `style='detail'` uses the alternate set tuned for the My Jobs view.
    """
    table = _DETAIL_ICONS if style == 'detail' else _COMPACT_ICONS
    if style == 'detail':
        return table.get(state, '•')
    return table.get(state, '·')


def state_attr(state):
    """Return the urwid palette attribute name for `state` (or 'faded')."""
    return _STATE_ATTRS.get(state, 'faded')


def state_short(state):
    """Two- or three-letter Slurm short code (`R`, `PD`, `OOM`, ...)."""
    return job_state_short.get(state, (state or '?')[:3])


def width_tier(width):
    """Bucket a column width into 'narrow' (<90), 'medium' (<120), or 'wide'."""
    if width is None:
        return 'wide'
    if width < 90:
        return 'narrow'
    if width < 120:
        return 'medium'
    return 'wide'
