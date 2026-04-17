"""Data models for slop."""
from slop.models.job import Job
from slop.models.jobs import Jobs
from slop.models.cluster import GPUInfo, Node, ClusterResources

__all__ = ["Job", "Jobs", "GPUInfo", "Node", "ClusterResources"]
