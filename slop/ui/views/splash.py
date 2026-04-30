"""Loading splash shown while the first scontrol fetch completes.

Standalone screen — not an overlay — so the user doesn't see a half-rendered
dashboard behind a "please wait" box. Future: drop ASCII art in here.
"""
import urwid as u


class ScreenViewSplash(u.WidgetWrap):
    def __init__(self):
        message = u.Text(
            [('jobheader', "  Welcome to slop  "),
             ('normal', "\n\n"),
             ('faded', "Please wait while fetching job data...")],
            align='center',
        )
        super().__init__(u.AttrMap(u.Filler(message, valign='middle'), 'bg'))

    def update(self):
        pass

    def on_resize(self):
        pass
