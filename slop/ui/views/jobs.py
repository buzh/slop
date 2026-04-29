"""Unified Jobs view (F2) - cycling tab strip over Users/Accounts/Partitions/States.

Wraps the four `TwoColumnJobView`-derived sub-views in a single screen with an
underline-style tab strip header. `Tab` / `Shift-Tab` cycle forward / back, and
`1`-`4` jump directly. Each sub-view keeps its own selection and sort state,
since the wrapper just swaps which one is visible — it doesn't recreate them.

State is intentionally not persisted across sessions.
"""
import urwid as u

from slop.ui.views import (
    ScreenViewUsers,
    ScreenViewAccounts,
    ScreenViewPartitions,
    ScreenViewStates,
)


TABS = [
    ('Users',      'users',      ScreenViewUsers),
    ('Accounts',   'accounts',   ScreenViewAccounts),
    ('Partitions', 'partitions', ScreenViewPartitions),
    ('States',     'states',     ScreenViewStates),
]


def _tab_strip(active_index):
    """Build the underline-style tab strip as a 2-row Pile.

    Each cell is `" N Label "` with N being the 1-based digit shortcut. The
    active cell uses bold white + a coloured bar; inactive cells fade out.
    """
    label_row = []
    bar_row = []
    for i, (name, _key, _cls) in enumerate(TABS):
        cell = f" {i + 1} {name} "
        if i == active_index:
            label_row.append(('tab_active', cell))
            bar_row.append(('info', '─' * len(cell)))
        else:
            label_row.append(('faded', cell))
            bar_row.append(('faded', '─' * len(cell)))
        if i < len(TABS) - 1:
            label_row.append(('normal', '  '))
            bar_row.append(('normal', '  '))
    return u.Pile([u.Text(label_row, wrap='clip'),
                   u.Text(bar_row, wrap='clip')])


class ScreenViewJobs(u.WidgetWrap):
    """Single F2 screen that hosts the four grouped views with cycling tabs."""

    def __init__(self, main_screen, jobs):
        self.main_screen = main_screen
        self.jobs = jobs

        # Instantiate every sub-view up-front; switching is just swapping
        # which one occupies the body. This preserves per-tab selection and
        # sort state for the duration of the session.
        self.subviews = [cls(main_screen, jobs) for _name, _key, cls in TABS]
        self.current_index = 0

        # The frame's body is replaced when the user cycles tabs. Building
        # the frame once and rewriting `body` (rather than rebuilding) keeps
        # urwid from losing focus during the swap.
        self._header_holder = u.WidgetPlaceholder(_tab_strip(self.current_index))
        self.frame = u.Frame(
            header=self._header_holder,
            body=self.subviews[self.current_index],
        )

        # Auto-refresh hook: only the active sub-view needs an update on
        # jobs_updated. The sub-views also subscribe individually but their
        # `is_active` checks return False (they're behind the wrapper), so
        # forwarding from here is the canonical refresh path.
        u.connect_signal(self.jobs, 'jobs_updated', self._on_jobs_update)

        u.WidgetWrap.__init__(self, u.AttrMap(self.frame, 'bg'))

    # --- Lifecycle ----------------------------------------------------------

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def _on_jobs_update(self, *_a, **_kw):
        if self.is_active():
            self.subviews[self.current_index].update()

    def update(self):
        """Called by ViewManager.show()."""
        self.subviews[self.current_index].update()

    def on_resize(self):
        for sv in self.subviews:
            sv.on_resize()

    @property
    def view_type(self):
        """Footer hint key — matches the active sub-view's view_type."""
        return TABS[self.current_index][1]

    # --- Tab cycling --------------------------------------------------------

    def _switch(self, new_index):
        if new_index == self.current_index or not (0 <= new_index < len(TABS)):
            return
        self.current_index = new_index
        self._header_holder.original_widget = _tab_strip(new_index)
        self.frame.body = self.subviews[new_index]
        self.subviews[new_index].update()

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        # Direct selection by digit
        if key in ('1', '2', '3', '4'):
            self._switch(int(key) - 1)
            return None

        # Cycling
        if key == 'tab':
            self._switch((self.current_index + 1) % len(TABS))
            return None
        if key == 'shift tab':
            self._switch((self.current_index - 1) % len(TABS))
            return None

        return super().keypress(size, key)
