"""Users view."""
from slop.ui.views.base import TwoColumnJobView
from slop.ui.widgets import UserItem


class ScreenViewUsers(TwoColumnJobView):
    """View grouped by users."""
    entity_attr = 'user'
    left_title = "Users"
    right_title_template = "Jobs for {entity}"
    view_type = 'users'

    def get_entity_table(self):
        return self.jobs.usertable if hasattr(self.jobs, "usertable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)
