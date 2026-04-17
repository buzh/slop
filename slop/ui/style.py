"""Declarative column layouts for job display.

A layout is an ordered list of column entries: (field, sizing, weight, wrap).
`get_display_attr` selects one based on job state + width category, then
inserts the user_name and array_tasks columns at the right positions.
"""
from slop.slurm import is_running, is_ended, is_pending


PALETTE = [
    # UI Chrome
    ("header",           "white, bold", "dark blue"),
    ("footer",           "white, bold", "dark red"),
    ("jobheader",        "white, bold", "dark cyan"),
    ("buttons",          "yellow",      "black"),
    ("buttons_selected", "white",       "dark red"),

    # Default/Background
    ("bg",               "white",       "black"),
    ("normal",           "white",       "black"),
    ("normal_selected",  "black",       "yellow"),
    ("faded",            "light gray",  "black"),

    # Job States (explicit naming)
    ("state_running",    "light green", "black"),
    ("state_pending",    "yellow",      "black"),
    ("state_failed",     "light red",   "black"),

    # Performance/Health (separate from job states)
    ("success",          "light green", "black"),
    ("warning",          "yellow",      "black"),
    ("error",            "light red",   "black"),
    ("info",             "light cyan",  "black"),

    # Overlay dimming (applied to lower layers in overlay stack)
    ("dim1",             "dark gray",   "black"),
    ("dim2",             "black",       "black"),
]


W = 'weight'
G = 'given'
CLIP = 'clip'
ELL = 'ellipsis'

# Base layouts indexed by (state_category, size). 'user_name' and 'array_tasks'
# are inserted as modifiers — never put them in here directly.
LAYOUTS = {
    'running': {
        'narrow': [('job_id',     W, 4, CLIP),
                   ('start_time', W, 4, CLIP),
                   ('name',       W, 5, ELL)],
        'medium': [('job_id',     W, 5, CLIP),
                   ('start_time', W, 4, CLIP),
                   ('end_time',   W, 3, CLIP),
                   ('partition',  W, 4, CLIP),
                   ('name',       W, 5, ELL)],
        'wide':   [('job_id',     W, 5, CLIP),
                   ('start_time', W, 4, CLIP),
                   ('end_time',   W, 3, CLIP),
                   ('account',    W, 3, CLIP),
                   ('partition',  W, 4, CLIP),
                   ('name',       W, 5, ELL),
                   ('nodes',      W, 5, CLIP)],
    },
    'pending': {
        'narrow': [('job_id',     W, 3, CLIP),
                   ('wall_time',  W, 2, CLIP),
                   ('name',       W, 5, ELL),
                   ('reason',     W, 4, ELL)],
        'medium': [('job_id',     W, 3, CLIP),
                   ('submit_time',W, 3, CLIP),
                   ('wall_time',  W, 2, CLIP),
                   ('partition',  W, 3, CLIP),
                   ('name',       W, 5, ELL),
                   ('reason',     W, 4, ELL)],
        'wide':   [('job_id',     W, 3, CLIP),
                   ('submit_time',W, 3, CLIP),
                   ('wall_time',  W, 2, CLIP),
                   ('account',    W, 3, CLIP),
                   ('partition',  W, 3, CLIP),
                   ('name',       W, 5, ELL),
                   ('reason',     W, 5, ELL)],
    },
    'ended': {
        'narrow': [('job_state',  W, 2, CLIP),
                   ('job_id',     W, 4, CLIP),
                   ('name',       W, 5, ELL),
                   ('exit_code',  W, 6, CLIP)],
        'medium': [('job_state',  W, 3, CLIP),
                   ('job_id',     W, 4, CLIP),
                   ('wall_time',  W, 4, CLIP),
                   ('partition',  W, 4, CLIP),
                   ('name',       W, 5, ELL),
                   ('exit_code',  W, 6, CLIP)],
        'wide':   [('job_state',  W, 3, CLIP),
                   ('job_id',     W, 4, CLIP),
                   ('wall_time',  W, 4, CLIP),
                   ('account',    W, 4, CLIP),
                   ('partition',  W, 4, CLIP),
                   ('name',       W, 5, ELL),
                   ('exit_code',  W, 8, CLIP)],
    },
    'other': {
        'narrow': [('job_state',  G, 3, CLIP),
                   ('job_id',     W, 3, CLIP),
                   ('name',       W, 5, ELL)],
        'medium': [('job_state',  G, 3, CLIP),
                   ('job_id',     W, 3, CLIP),
                   ('wall_time',  W, 3, CLIP),
                   ('partition',  W, 3, CLIP),
                   ('name',       W, 5, ELL)],
        'wide':   [('job_state',  G, 3, CLIP),
                   ('job_id',     W, 3, CLIP),
                   ('submit_time',W, 3, CLIP),
                   ('wall_time',  W, 3, CLIP),
                   ('account',    W, 3, CLIP),
                   ('partition',  W, 3, CLIP),
                   ('name',       W, 5, ELL),
                   ('reason',     W, 5, ELL)],
    },
}

# Array children only have narrow vs wide variants (medium folds into wide).
CHILD_LAYOUTS = {
    'running': {
        'narrow': [('job_state',  W, 2, CLIP),
                   ('task_id',    W, 2, CLIP),
                   ('start_time', W, 3, CLIP),
                   ('nodes',      W, 3, CLIP)],
        'wide':   [('job_state',  W, 2, CLIP),
                   ('task_id',    W, 2, CLIP),
                   ('start_time', W, 3, CLIP),
                   ('end_time',   W, 3, CLIP),
                   ('nodes',      W, 3, CLIP),
                   ('tres',       W, 4, CLIP)],
    },
    'ended': {
        'narrow': [('job_state',  W, 2, CLIP),
                   ('job_id',     W, 3, CLIP),
                   ('task_id',    W, 2, CLIP),
                   ('wall_time',  W, 3, CLIP),
                   ('exit_code',  W, 4, CLIP)],
        'wide':   [('job_state',  W, 2, CLIP),
                   ('job_id',     W, 3, CLIP),
                   ('task_id',    W, 2, CLIP),
                   ('wall_time',  W, 3, CLIP),
                   ('exit_code',  W, 4, CLIP),
                   ('reason',     W, 4, ELL)],
    },
    'pending': {
        'narrow': [('job_state',  W, 2, CLIP),
                   ('task_id',    W, 2, CLIP),
                   ('wall_time',  W, 3, CLIP),
                   ('reason',     W, 4, ELL)],
        'wide':   [('job_state',  W, 2, CLIP),
                   ('task_id',    W, 2, CLIP),
                   ('wall_time',  W, 3, CLIP),
                   ('partition',  W, 3, CLIP),
                   ('reason',     W, 4, ELL)],
    },
}

USER_COL = ('user_name', W, 3, CLIP)
ARRAY_TASKS_COL = ('array_tasks', W, 2, CLIP)


def _size(width):
    if width is None:
        return 'wide'
    if width < 90:
        return 'narrow'
    if width < 130:
        return 'medium'
    return 'wide'


def _category(job):
    if is_running(job) or job.has_running_children:
        return 'running'
    if is_pending(job):
        return 'pending'
    if is_ended(job):
        return 'ended'
    return 'other'


def _child_category(job):
    if is_running(job):
        return 'running'
    if is_ended(job):
        return 'ended'
    return 'pending'  # children fold pending + other into pending


def _insert_after(layout, anchor, entry):
    out = []
    for col in layout:
        out.append(col)
        if col[0] == anchor:
            out.append(entry)
    return out


def get_display_attr(job, width=None, view_type=None):
    """Get column layout for a job.

    Returns dict {field: (sizing, weight, wrap_mode)} suitable for urwid.Columns.
    """
    size = _size(width)
    show_user = bool(view_type and view_type != 'users')

    if job.is_array_child:
        child_size = 'narrow' if size == 'narrow' else 'wide'
        layout = CHILD_LAYOUTS[_child_category(job)][child_size]
        return {f: (s, w, m) for f, s, w, m in layout}

    category = _category(job)
    # Array parents collapse 'other' into 'ended' layout (preserves prior behavior)
    if job.is_array_parent and category == 'other':
        category = 'ended'

    layout = LAYOUTS[category][size]

    if show_user:
        layout = _insert_after(layout, 'job_id', USER_COL)

    # Array parents get an array_tasks column except in running state, where
    # the layout deliberately matches non-array running jobs for alignment.
    if job.is_array_parent and category != 'running':
        anchor = 'user_name' if show_user else 'job_id'
        layout = _insert_after(layout, anchor, ARRAY_TASKS_COL)

    return {f: (s, w, m) for f, s, w, m in layout}
