import re
from collections import defaultdict

class GPUInfo:
    """GPU allocation information for a node."""

    def __init__(self, gpu_type, total, used, indices="N/A"):
        self.gpu_type = gpu_type
        self.total = total
        self.used = used
        self.indices = indices

    @property
    def free(self):
        """Number of free GPUs."""
        return self.total - self.used

    @property
    def utilization(self):
        """GPU utilization percentage."""
        return (self.used / self.total * 100.0) if self.total > 0 else 0.0

class Node:
    """Slurm compute node with CPU, memory, and GPU resources."""

    def __init__(self, node_data):
        self.name = node_data.get('name', 'unknown')
        self.state = node_data.get('state', [])
        self.partitions = node_data.get('partitions', [])

        # CPU
        self.cpus_total = node_data.get('cpus', 0)
        self.cpus_alloc = node_data.get('alloc_cpus', 0)
        self.cpus_idle = self.cpus_total - self.cpus_alloc
        self.cpu_load = node_data.get('cpu_load', 0) / 100.0

        # Memory (MB)
        self.mem_total = node_data.get('real_memory', 0)
        self.mem_alloc = node_data.get('alloc_memory', 0)
        self.mem_free = node_data.get('free_mem', {}).get('number', 0)

        # GPUs
        self.gpus = self._parse_gpus(node_data.get('gres', ''), node_data.get('gres_used', ''))

    def _parse_gpus(self, gres, gres_used):
        """Parse GPU info from GRES strings.

        Args:
            gres: Total available, e.g. "gpu:a100:4"
            gres_used: Currently allocated, e.g. "gpu:a100:2(IDX:0-1)"

        Returns:
            List of GPUInfo objects
        """
        if not gres or 'gpu' not in gres:
            return []

        # Total: gpu:type:count
        match = re.search(r'gpu:(\w+):(\d+)', gres)
        if not match:
            return []

        gpu_type, total = match.group(1), int(match.group(2))

        # Used: gpu:type:count(IDX:...)
        used, indices = 0, "N/A"
        if gres_used and 'gpu' in gres_used:
            used_match = re.search(r'gpu:\w+:(\d+)\(IDX:([^)]+)\)', gres_used)
            if used_match:
                used, indices = int(used_match.group(1)), used_match.group(2)

        return [GPUInfo(gpu_type, total, used, indices)]

    @property
    def is_up(self):
        """Check if node is operational."""
        down_states = {'DOWN', 'DRAIN', 'DRAINING', 'FAIL', 'FAILING', 'NOT_RESPONDING'}
        return not any(s in down_states for s in self.state)

    @property
    def cpu_utilization(self):
        """CPU allocation percentage."""
        if self.cpus_total == 0:
            return 0.0
        return (self.cpus_alloc / self.cpus_total) * 100.0

    @property
    def mem_utilization(self):
        """Memory allocation percentage."""
        if self.mem_total == 0:
            return 0.0
        return (self.mem_alloc / self.mem_total) * 100.0

class ClusterResources:
    """Aggregated cluster resource statistics."""

    def __init__(self, nodes_data, partitions_data=None):
        self.nodes = [Node(n) for n in nodes_data.get('nodes', [])]
        self.partitions_data = partitions_data

    def get_overall_stats(self):
        """Get cluster-wide CPU and memory statistics."""
        up_nodes = [n for n in self.nodes if n.is_up]

        stats = {
            'total_nodes': len(self.nodes),
            'up_nodes': len(up_nodes),
            'down_nodes': len(self.nodes) - len(up_nodes),
            'cpus_total': sum(n.cpus_total for n in up_nodes),
            'cpus_alloc': sum(n.cpus_alloc for n in up_nodes),
            'cpus_idle': sum(n.cpus_idle for n in up_nodes),
            'mem_total_mb': sum(n.mem_total for n in up_nodes),
            'mem_alloc_mb': sum(n.mem_alloc for n in up_nodes),
            'mem_free_mb': sum(n.mem_free for n in up_nodes),
        }

        stats['cpu_util'] = (stats['cpus_alloc'] / stats['cpus_total'] * 100.0) if stats['cpus_total'] > 0 else 0.0
        stats['mem_util'] = (stats['mem_alloc_mb'] / stats['mem_total_mb'] * 100.0) if stats['mem_total_mb'] > 0 else 0.0

        return stats

    def get_gpu_stats(self):
        """Get GPU statistics aggregated by type."""
        gpu_stats = defaultdict(lambda: {'total': 0, 'used': 0, 'free': 0, 'nodes': []})

        for node in (n for n in self.nodes if n.is_up):
            for gpu in node.gpus:
                stats = gpu_stats[gpu.gpu_type]
                stats['total'] += gpu.total
                stats['used'] += gpu.used
                stats['free'] += gpu.free
                stats['nodes'].append(node.name)

        # Add utilization percentage
        for stats in gpu_stats.values():
            stats['util'] = (stats['used'] / stats['total'] * 100.0) if stats['total'] > 0 else 0.0

        return dict(gpu_stats)

    def get_nodes_by_state(self):
        """Group nodes by state."""
        by_state = defaultdict(list)
        for node in self.nodes:
            # Use primary state
            primary_state = node.state[0] if node.state else 'UNKNOWN'
            by_state[primary_state].append(node)
        return dict(by_state)

__all__ = [
    "Node",
    "GPUInfo",
    "ClusterResources",
]
