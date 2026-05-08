"""Modal shown when the load governor halts refresh after sustained backoff."""
import urwid as u

from slop.ui.widgets import rounded_box


class HaltedModal(u.WidgetWrap):
    """Suspended-refresh dialog with a single OK button.

    Pressing OK calls `main_screen.resume_after_halt()`, which resets the
    governor and kicks the refresh loop again.
    """

    overlay_height = 9

    def __init__(self, main_screen):
        self.main_screen = main_screen
        msg = u.Text(
            "slurmctld has been overloaded for 30 minutes.\n"
            "Refreshing has been suspended to reduce load.\n",
            align='center',
        )
        ok_button = u.Button("OK to retry", self._on_ok)
        ok = u.AttrMap(ok_button, 'buttons', 'buttons_selected')
        pile = u.Pile([msg, ok])
        pile.focus_position = 1
        widget = u.AttrMap(
            rounded_box(u.Filler(pile), title='Refreshing paused'),
            'bg',
        )
        u.WidgetWrap.__init__(self, widget)

    def _on_ok(self, _btn=None):
        self.main_screen.resume_after_halt()


__all__ = ["HaltedModal"]
