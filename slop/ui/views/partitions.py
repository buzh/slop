"""Partitions view."""
from slop.ui.views.base import TwoColumnJobView
from slop.ui.widgets import UserItem


class ScreenViewPartitions(TwoColumnJobView):
    """View grouped by partitions."""
    entity_attr = 'partition'
    left_title = "Partitions"
    right_title_template = "Jobs in partition {entity}"
    view_type = 'partitions'

    def get_entity_table(self):
        return self.jobs.partitiontable if hasattr(self.jobs, "partitiontable") else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        return UserItem(entity_name, njobs, running, pending)
