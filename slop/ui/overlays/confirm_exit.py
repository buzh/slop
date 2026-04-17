"""Confirm exit overlay."""
import urwid as u


class ConfirmExit(u.WidgetWrap):
    def __init__(self, main_screen):
        self.main_screen = main_screen
        y = u.AttrMap(u.Button("Yes", self.exit_program), 'buttons', 'buttons_selected')
        n = u.AttrMap(u.Button("No", self.cancel_exit), 'buttons', 'buttons_selected')
        b = [y, n]
        buttons = u.Columns(b)

        widget = u.AttrMap(
            u.LineBox(
                u.Filler(u.Pile([buttons])),
                title='Confirm exit?',
                tlcorner='╭', trcorner='╮',
                blcorner='╰', brcorner='╯'
            ),
            'bg'
        )
        u.WidgetWrap.__init__(self, widget)

    def keypress(self, size, key):
        return super().keypress(size, key)

    def exit_program(self, a=None) -> None:
        raise u.ExitMainLoop()

    def cancel_exit(self, a=None):
        self.main_screen.close_overlay()
