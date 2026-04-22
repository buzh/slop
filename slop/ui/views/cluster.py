"""Cluster resources view."""
import urwid as u
from slop.models import ClusterResources
from slop.utils import compress_hostlist, smart_truncate
from slop.ui.widgets import SafeListBox, SectionHeader, rounded_box


class ScreenViewCluster(u.WidgetWrap):
    """Cluster resources view - similar to htop/btop."""

    def __init__(self, main_screen, cluster_fetcher):
        self.main_screen = main_screen
        self.cluster_fetcher = cluster_fetcher
        self.walker = u.SimpleFocusListWalker([])
        self.listbox = SafeListBox(self.walker)
        # `e` toggles between Slurm-style ranges (default — fits a 1000-node
        # cluster on one line per state) and the full comma-separated list.
        self.expanded_hostlist = False

        widget = rounded_box(u.ScrollBar(self.listbox), title='Cluster Resources')
        u.WidgetWrap.__init__(self, widget)

        # Build initial view
        self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        """Handle resize events."""
        self.update()

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key
        if key == 'e':
            self.expanded_hostlist = not self.expanded_hostlist
            self.update()
            return None
        return super().keypress(size, key)

    def make_bar(self, used, total, width=40):
        """Create a visual progress bar."""
        if total == 0:
            filled = 0
        else:
            filled = int((used / total) * width)
        bar = "█" * filled + "░" * (width - filled)
        return bar

    def format_memory(self, mb):
        """Format memory in MB to human-readable form."""
        if mb < 1024:
            return f"{mb:.0f}MB"
        elif mb < 1024 * 1024:
            return f"{mb/1024:.1f}GB"
        else:
            return f"{mb/(1024*1024):.2f}TB"

    def update(self):
        """Rebuild the cluster view."""
        # Save current scroll position
        old_focus = None
        try:
            old_focus = self.walker.focus
        except (IndexError, AttributeError):
            pass

        # Get data
        nodes_data = self.cluster_fetcher.fetch_nodes_sync()
        partitions_data = self.cluster_fetcher.fetch_partitions_sync()

        cluster = ClusterResources(nodes_data, partitions_data)
        overall = cluster.get_overall_stats()
        gpu_stats = cluster.get_gpu_stats()

        # Calculate bar width and column widths based on available screen width
        available_width = self.main_screen.width - 10 if hasattr(self.main_screen, 'width') else 100
        bar_width = min(max(available_width - 50, 20), 60)

        # Adaptive column widths for GPU/node names (wider screens = more space for names)
        if available_width > 120:
            gpu_name_width = 20
            node_name_width = 16
        elif available_width > 100:
            gpu_name_width = 16
            node_name_width = 12
        else:
            gpu_name_width = 12
            node_name_width = 10

        # Build widgets
        widgets = []

        # === Overall cluster status ===
        widgets.append(SectionHeader('CLUSTER OVERVIEW'))

        # Aligned resource display with consistent column widths
        # Label column: 8 chars, bar in brackets, then stats
        node_info = f"{overall['up_nodes']} UP, {overall['down_nodes']} DOWN"
        widgets.append(u.Text(f"Nodes   : {node_info} (Total: {overall['total_nodes']})"))

        cpu_bar = self.make_bar(overall['cpus_alloc'], overall['cpus_total'], bar_width)
        cpu_used = f"{overall['cpus_alloc']}/{overall['cpus_total']}".rjust(11)
        widgets.append(u.Text(f"CPU     : [{cpu_bar}] {cpu_used} cores ({overall['cpu_util']:5.1f}%)"))

        mem_bar = self.make_bar(overall['mem_alloc_mb'], overall['mem_total_mb'], bar_width)
        mem_alloc = self.format_memory(overall['mem_alloc_mb'])
        mem_total = self.format_memory(overall['mem_total_mb'])
        mem_used = f"{mem_alloc}/{mem_total}".rjust(17)
        widgets.append(u.Text(f"Memory  : [{mem_bar}] {mem_used} ({overall['mem_util']:5.1f}%)"))

        # === GPU resources ===
        if gpu_stats:
            widgets.append(u.Divider())
            widgets.append(SectionHeader('GPU RESOURCES'))

            # Sort GPU types for consistent display
            for gpu_type in sorted(gpu_stats.keys()):
                stats = gpu_stats[gpu_type]
                gpu_bar = self.make_bar(stats['used'], stats['total'], bar_width)
                # Smart truncate GPU names - preserve both model and memory size
                gpu_name = smart_truncate(gpu_type.upper(), gpu_name_width, mode='middle')
                gpu_count = f"{stats['used']}/{stats['total']}".rjust(7)
                gpu_text = f"{gpu_name:{gpu_name_width}s} : [{gpu_bar}] {gpu_count} GPUs ({stats['util']:5.1f}%)"
                widgets.append(u.Text(gpu_text))

        # === GPU nodes detail ===
        gpu_nodes = [n for n in cluster.nodes if n.gpus and n.is_up]
        if gpu_nodes:
            widgets.append(u.Divider())
            widgets.append(SectionHeader('GPU NODES'))

            # Column headers for GPU nodes
            node_col = "Node".ljust(node_name_width)
            gpu_col = "GPU Type".ljust(gpu_name_width)
            widgets.append(u.Text(('faded', f"{node_col} {gpu_col}   Usage                 State      Status")))
            widgets.append(u.Divider("─"))

            # Sort by node name
            gpu_nodes = sorted(gpu_nodes, key=lambda n: n.name)

            for node in gpu_nodes:
                for gpu in node.gpus:
                    node_bar = self.make_bar(gpu.used, gpu.total, 20)
                    state = "MIXED" if gpu.used > 0 and gpu.free > 0 else "IDLE" if gpu.used == 0 else "FULL"
                    state_str = (node.state[0] if node.state else "UNKNOWN")[:10]

                    # Smart truncate names - preserve identifying prefix and suffix
                    node_name_trunc = smart_truncate(node.name, node_name_width, mode='middle')
                    gpu_name = smart_truncate(gpu.gpu_type, gpu_name_width, mode='middle')
                    usage = f"{gpu.used}/{gpu.total}".center(5)

                    # Color-code based on usage
                    if state == "FULL":
                        attr = 'warning'
                    elif state == "IDLE":
                        attr = 'faded'
                    else:
                        attr = None

                    node_line = f"{node_name_trunc:{node_name_width}s} {gpu_name:{gpu_name_width}s} [{node_bar}] {usage}  {state:5s}      {state_str}"

                    if attr:
                        widgets.append(u.AttrMap(u.Text(node_line), attr))
                    else:
                        widgets.append(u.Text(node_line))

                    # Show indices if GPUs are in use
                    if gpu.used > 0 and gpu.indices != "N/A":
                        indent = " " * (node_name_width + gpu_name_width + 2)
                        widgets.append(u.Text(('faded', f"{indent}└─ Active GPUs: {gpu.indices}")))

        # === All nodes summary ===
        widgets.append(u.Divider())
        widgets.append(SectionHeader('ALL NODES BY STATE'))

        # Group nodes by state with better formatting
        nodes_by_state = cluster.get_nodes_by_state()

        # Sort states by priority: ALLOCATED, MIXED, IDLE, DOWN, DRAIN, etc.
        state_priority = {
            'ALLOCATED': 0,
            'MIXED': 1,
            'IDLE': 2,
            'COMPLETING': 3,
            'DOWN': 4,
            'DRAIN': 5,
            'DRAINED': 6,
            'DRAINING': 7,
            'MAINT': 8,
            'RESERVED': 9,
        }

        sorted_states = sorted(nodes_by_state.keys(), key=lambda s: state_priority.get(s, 99))

        for state in sorted_states:
            node_list = nodes_by_state[state]
            count_str = f"({len(node_list):2d})".ljust(5)

            # Color-code state labels
            if state in ['ALLOCATED', 'MIXED']:
                state_attr = 'success'
            elif state in ['DOWN', 'DRAIN', 'DRAINED', 'DRAINING']:
                state_attr = 'error'
            elif state == 'IDLE':
                state_attr = 'faded'
            else:
                state_attr = None

            # Default: Slurm-style ranges so a 900-node ALLOCATED block
            # fits on one line. `e` swaps in the full comma-separated list.
            node_names = sorted(n.name for n in node_list)
            if self.expanded_hostlist:
                body = ", ".join(node_names)
                wrap = 'space'
            else:
                body = compress_hostlist(node_names)
                wrap = 'any'

            state_label = f"{state:12s} {count_str}"
            line = u.Text(f"{state_label}: {body}", wrap=wrap)
            if state_attr:
                widgets.append(u.AttrMap(line, state_attr))
            else:
                widgets.append(line)

        # Update walker
        self.walker.clear()
        self.walker.extend(widgets)

        # Restore scroll position
        if len(self.walker) > 0:
            if old_focus is not None and old_focus < len(self.walker):
                self.walker.set_focus(old_focus)
            else:
                self.walker.set_focus(0)


