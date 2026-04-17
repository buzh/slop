"""Screen view components."""
from slop.ui.views.base import TwoColumnJobView, make_view
from slop.ui.views.my_jobs import ScreenViewMyJobs
from slop.ui.views.cluster import ScreenViewCluster
from slop.ui.views.queue import ScreenViewQueue
from slop.ui.views.report import ScreenViewReport


ScreenViewUsers = make_view(
    "ScreenViewUsers",
    entity_attr='user',
    left_title="Users",
    right_title_template="Jobs for {entity}",
    view_type='users',
    table_attr='usertable',
)

ScreenViewAccounts = make_view(
    "ScreenViewAccounts",
    entity_attr='account',
    left_title="Accounts",
    right_title_template="Jobs for account {entity}",
    view_type='accounts',
    table_attr='accounttable',
)

ScreenViewPartitions = make_view(
    "ScreenViewPartitions",
    entity_attr='partition',
    left_title="Partitions",
    right_title_template="Jobs in partition {entity}",
    view_type='partitions',
    table_attr='partitiontable',
)

ScreenViewStates = make_view(
    "ScreenViewStates",
    entity_attr='state',
    left_title="Job States",
    right_title_template="Jobs in state {entity}",
    view_type='states',
    table_attr='statetable',
)


__all__ = [
    "TwoColumnJobView",
    "make_view",
    "ScreenViewMyJobs",
    "ScreenViewUsers",
    "ScreenViewAccounts",
    "ScreenViewPartitions",
    "ScreenViewStates",
    "ScreenViewCluster",
    "ScreenViewQueue",
    "ScreenViewReport",
]
