"""Accounts view."""
from slop.ui.views.base import TwoColumnJobView
from slop.ui.widgets import UserItem


class ScreenViewAccounts(TwoColumnJobView):
    """View grouped by accounts."""
    entity_attr = 'account'
    left_title = "Accounts"
    right_title_template = "Jobs for account {entity}"
    view_type = 'accounts'

    def get_entity_table(self):
        return self.jobs.accounttable if hasattr(self.jobs, "accounttable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)
