"""Tab-completion state machine for search/edit widgets."""


class TabCompletionMixin:
    """Cycle through `self.current_matches` when Tab is pressed.

    Consumer responsibilities:
      - call `self._init_completion()` in `__init__`
      - hold an `urwid.Edit` instance at `self.search_edit`
      - keep `self.current_matches` updated as input changes
      - in the Edit's 'change' handler, return early when `self.in_tab_completion`
        is set (it is true while we're writing the completion back into the edit)
      - on Tab keypress, call `self._cycle_completion()`
      - on any other text-modifying keypress, call `self._reset_completion()`
    """

    def _init_completion(self):
        self.current_matches = []
        self.completion_index = 0
        self.in_tab_completion = False

    def _reset_completion(self):
        self.completion_index = 0
        self.in_tab_completion = False

    def _cycle_completion(self):
        if not self.current_matches:
            return
        if self.in_tab_completion and len(self.current_matches) > 1:
            self.completion_index = (self.completion_index + 1) % len(self.current_matches)
        else:
            self.completion_index = 0
            self.in_tab_completion = True
        completion = self.current_matches[self.completion_index]
        self.search_edit.set_edit_text(completion)
        self.search_edit.set_edit_pos(len(completion))
