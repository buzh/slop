"""Screen view components."""
from slop.ui.views.base import TwoColumnJobView
from slop.ui.views.my_jobs import ScreenViewMyJobs
from slop.ui.views.users import ScreenViewUsers
from slop.ui.views.accounts import ScreenViewAccounts
from slop.ui.views.partitions import ScreenViewPartitions
from slop.ui.views.states import ScreenViewStates
from slop.ui.views.cluster import ScreenViewCluster
from slop.ui.views.queue import ScreenViewQueue
from slop.ui.views.report import ScreenViewReport

__all__ = [
    "TwoColumnJobView",
    "ScreenViewMyJobs",
    "ScreenViewUsers",
    "ScreenViewAccounts",
    "ScreenViewPartitions",
    "ScreenViewStates",
    "ScreenViewCluster",
    "ScreenViewQueue",
    "ScreenViewReport",
]
