"""Job states view."""
from slop.ui.views.base import TwoColumnJobView
from slop.ui.widgets import UserItem


class ScreenViewStates(TwoColumnJobView):
    """View grouped by job states."""
    entity_attr = 'state'
    left_title = "Job States"
    right_title_template = "Jobs in state {entity}"
    view_type = 'states'

    def get_entity_table(self):
        return self.jobs.statetable if hasattr(self.jobs, "statetable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)
