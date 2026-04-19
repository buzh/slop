"""Shared UI tunables that affect layout and grouping behavior."""

# Auto-collapse a similar-job group when it grows past this size, unless the
# user has explicitly toggled it.
LARGE_GROUP_AUTO_COLLAPSE = 30

# When an array parent is expanded, show every pending child up to this count;
# beyond that, render the first child plus a summary of the rest.
MAX_PENDING_CHILDREN_INLINE = 10

# Target fraction of vertical screen space the job list should fill before we
# stop opportunistically expanding collapsed groups.
SCREEN_FILL_RATIO = 0.9

# Single placeholder rendered wherever a field has no value. Use this in any
# display path so missing data looks the same everywhere; never branch on it
# (use None as the data-missing sentinel and substitute this only at render).
EMPTY_PLACEHOLDER = '-'
