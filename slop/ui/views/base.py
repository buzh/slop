"""Base class for two-column job views."""
import urwid as u
import datetime
from slop.slurm import (
    is_running,
    is_ended,
    is_pending,
    job_state_running,
    job_state_ended,
    job_state_pending,
)
from slop.utils import format_duration, nice_tres, smart_truncate
from slop.ui.constants import (
    LARGE_GROUP_AUTO_COLLAPSE,
    MAX_PENDING_CHILDREN_INLINE,
    SCREEN_FILL_RATIO,
)
from slop.ui.widgets import (
    ChildJobWidget,
    ArrayPendWidget,
    ExpandableGroupMarker,
    SectionHeader,
    UserItem,
    GenericOverlayText,
)
from slop.ui.overlays import JobInfoOverlay


class TwoColumnJobView(u.WidgetWrap):
    """Base class for two-column views: entity list on left, jobs on right.

    Subclasses (or `make_view()`) configure these class attributes:
        - entity_attr: str - attribute name on job (e.g., 'user', 'account')
        - left_title: str - title for left panel
        - right_title_template: str - template for right panel title (use {entity})
        - view_type: str - identifier for layout selection ('users', 'accounts', ...)
        - table_attr: str - attribute on `self.jobs` holding the entity table

    Override `get_entity_table()` or `create_entity_widget()` only if the
    default attribute-driven behavior isn't enough.
    """

    # Defaults used by the factory-generated views; subclasses may override.
    entity_attr = None
    left_title = None
    right_title_template = None
    view_type = 'users'
    table_attr = None

    # Sort keys map to column positions (set dynamically based on visible columns)
    # This will be populated when the header is built
    SORT_KEYS = {}

    def __init__(self, main_screen, jobs):
        self.jobs = jobs
        self.selected_entity = None
        self.selected_job = None
        self.sort_col = 'job_id'  # Default sort by job ID
        self.sort_reverse = True  # Newest first (high job IDs first)
        self.main_screen = main_screen
        # Default view_type, can be overridden by subclasses
        if not hasattr(self, 'view_type'):
            self.view_type = 'users'

        # Collapse state for job groups
        self.collapsed_groups = {}  # {group_key: bool}
        self.calculate_jobs_per_group()

        # Prevent recursion in draw_jobs
        self._drawing = False

        # Create walkers
        self.entity_walker = u.SimpleFocusListWalker([])
        self.jobwalker = u.SimpleFocusListWalker([])
        self.joblistbox = u.ListBox(self.jobwalker)

        # Build UI
        entity_list = u.AttrMap(u.ScrollBar(u.ListBox(self.entity_walker)), 'bg')

        # Right panel: just the scrollable job list (category headers are inline)
        self.jw = u.LineBox(
            u.ScrollBar(self.joblistbox),
            title=self.right_title_template.format(entity=""),
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )

        left_panel = u.LineBox(
            entity_list,
            title=self.left_title,
            tlcorner='╭', trcorner='╮',
            blcorner='╰', brcorner='╯'
        )
        right_panel = self.jw
        self.w = u.Columns([('weight', 25, left_panel), ('weight', 75, right_panel)])

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, self.w)

    def on_jobs_update(self, *_args, **_kwargs):
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        in_job_col = self.w.get_focus_column() == 1
        if key == 'h' and self.entity_attr == 'user' and self.selected_entity:
            return self._key_history()
        if key == 'e' and in_job_col:
            return self._key_toggle_group()
        if (key == ' ' or key == 'enter') and in_job_col:
            return self._key_expand_or_details()
        if key in self.SORT_KEYS and in_job_col:
            return self._key_sort(key)
        return super().keypress(size, key)

    def _key_history(self):
        entity_name = self.selected_entity
        self.main_screen.open_overlay(GenericOverlayText(
            self.main_screen,
            f"Loading history for {entity_name}...\n\nFetching account usage data..."
        ))
        self.main_screen.loop.draw_screen()
        result = self.main_screen.sreport_fetcher.fetch_user_utilization(entity_name)
        self.main_screen.close_overlay()
        if result:
            self.main_screen.handle_search_result(result, 'user', entity_name)
        else:
            self.main_screen.open_overlay(GenericOverlayText(
                self.main_screen,
                f"Failed to fetch data for {entity_name}"
            ))
        return None

    def _key_toggle_group(self):
        focus_w, _ = self.jobwalker.get_focus()
        if hasattr(focus_w, "group_key"):
            group_key = focus_w.group_key
        elif hasattr(focus_w, "jobid"):
            job = self.jobs.job_index.get(focus_w.jobid)
            group_key = self.get_job_group_key(job) if job else None
        else:
            group_key = None
        if group_key is not None:
            self.collapsed_groups[group_key] = not self.collapsed_groups.get(group_key, True)
            self.draw_jobs()
        return None

    def _key_expand_or_details(self):
        focus_w, _ = self.jobwalker.get_focus()
        if not hasattr(focus_w, "jobid"):
            return None
        job = self.jobs.job_index.get(focus_w.jobid)
        if not job:
            return None
        if job.is_array_parent:
            job.toggle_expand()
            self.draw_jobs()
        else:
            self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
        return None

    def _key_sort(self, key):
        selected_col = self.SORT_KEYS[key]
        if self.sort_col == selected_col:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_col = selected_col
            self.sort_reverse = True
        self.draw_jobs()
        return None

    def modified(self):
        # Entity selection changed
        focus_w, _ = self.entity_walker.get_focus()
        entity_name = self._get_entity_from_widget(focus_w)
        if entity_name and entity_name != self.selected_entity:
            # Only redraw if entity actually changed
            self.selected_entity = entity_name
            self.draw_jobs()

        # Job selection changed - just track it, don't redraw
        focus_w, _ = self.jobwalker.get_focus()
        if hasattr(focus_w, "jobid"):
            self.selected_job = focus_w.jobid

    def _get_entity_from_widget(self, widget):
        """Extract entity name from widget. Override if needed."""
        if hasattr(widget, self.entity_attr):
            return getattr(widget, self.entity_attr)
        # Try common attribute names
        for attr in ['user', 'account', 'partition']:
            if hasattr(widget, attr):
                return getattr(widget, attr)
        return None

    def sort_jobs(self, jobtable):
        def get_sort_key(job):
            # Handle missing attributes gracefully
            if self.sort_col not in job.__dict__:
                return 0  # Put jobs without this field at the start/end

            value = job.__dict__[self.sort_col]

            # Handle time fields (dicts with 'number' key)
            if self.sort_col in ['start_time', 'end_time', 'submit_time']:
                if isinstance(value, dict) and 'number' in value:
                    return value['number']
                return 0
            # Handle job_state (list of strings)
            elif self.sort_col == 'job_state':
                if isinstance(value, list):
                    return ','.join(value)
                return str(value)
            # Handle other fields
            else:
                return value if value is not None else ''

        return sorted(jobtable, key=get_sort_key, reverse=self.sort_reverse)

    def categorize_jobs(self, jobtable):
        job_sets = {
            "Running": [],
            "Pending": [],
            "Ended": [],
            "Other": []
        }
        for job in jobtable:
            if job.is_array_child:
                continue
            job_sets[job.get_state_category()].append(job)
        return job_sets

    def get_job_group_key(self, job):
        """Generate a key for grouping similar jobs together."""
        # Group by: user (if not in users view) + partition + state
        parts = []
        if self.view_type != 'users' and hasattr(job, 'user_name'):
            parts.append(job.user_name)
        if hasattr(job, 'partition'):
            parts.append(job.partition)
        parts.append(job.get_state_category())
        return ':'.join(parts)

    def group_similar_jobs(self, joblist):
        """Group similar jobs together for collapsing."""
        from collections import OrderedDict
        groups = OrderedDict()

        for job in joblist:
            if job.is_array_child:
                continue

            key = self.get_job_group_key(job)
            if key not in groups:
                groups[key] = []
            groups[key].append(job)

        return groups

    def build_job_widgets(self, joblist, label=None):
        if not joblist:
            return []
        groups = self.group_similar_jobs(joblist)
        if not groups:
            return []

        widgets = []
        if label:
            widgets.append(self._build_section_header(label))

        representative_job = next(
            (j for j in joblist if not j.is_array_parent),
            joblist[0],
        )
        widgets.append(self.build_category_header(representative_job))

        for group_key, group_jobs in groups.items():
            widgets.extend(self._build_group(group_key, group_jobs))
        return widgets

    def _build_section_header(self, label):
        return SectionHeader(label.upper())

    def _build_group(self, group_key, group_jobs):
        group_count = len(group_jobs)
        if group_key in self.collapsed_groups:
            is_collapsed = self.collapsed_groups[group_key]
        else:
            is_collapsed = group_count > LARGE_GROUP_AUTO_COLLAPSE

        if is_collapsed and group_count > self.jobs_per_group:
            jobs_to_show = group_jobs[:self.jobs_per_group]
            remaining = group_count - self.jobs_per_group
        else:
            jobs_to_show = group_jobs
            remaining = 0

        widgets = []
        for job in jobs_to_show:
            widgets.append(job.widget)
            if job.is_array_parent and not job.array_collapsed_widget:
                widgets.extend(self._build_array_children(job))

        if remaining > 0:
            expand_text = f"  ... and {remaining} more similar jobs (press 'e' to expand)"
            widgets.append(ExpandableGroupMarker(expand_text, group_key))
        return widgets

    def _build_array_children(self, parent_job):
        children = sorted(parent_job.array_children, key=lambda j: j.job_id)
        running_widgets = []
        pending_children = []
        for child in children:
            if is_running(child):
                running_widgets.append(ChildJobWidget(child))
            else:
                pending_children.append(child)

        widgets = list(running_widgets)
        pending_count = len(pending_children)
        if pending_count == 0:
            return widgets
        if pending_count <= MAX_PENDING_CHILDREN_INLINE:
            widgets.extend(ChildJobWidget(c) for c in pending_children)
        else:
            widgets.append(ChildJobWidget(pending_children[0]))
            widgets.append(ArrayPendWidget(pending_count - 1))
        return widgets

    def restore_job_focus(self):
        focused = False
        for index, item in enumerate(self.jobwalker):
            if hasattr(item, "jobid") and item.jobid == self.selected_job:
                self.jobwalker.set_focus(index)
                focused = True
                break
        if not focused:
            for index, item in enumerate(self.jobwalker):
                if hasattr(item, "jobid"):
                    self.joblistbox.set_focus(0)
                    self.jobwalker.set_focus(index)
                    self.selected_job = item.jobid
                    break

    def build_category_header(self, representative_job):
        """Build a header widget for a category of jobs."""
        display_attr = representative_job.widget.display_attr
        job_states = representative_job.states
        is_array = bool(representative_job.is_array)
        is_array_parent = representative_job.is_array_parent
        # Treat array parents with running children as "running" for column headers
        job_is_running = bool(job_states & job_state_running) or representative_job.has_running_children
        is_ended = bool(job_states & job_state_ended)
        is_pending = bool(job_states & job_state_pending)

        dynamic_labels = {
            'start_time': lambda: "Running" if job_is_running else "Started" if is_ended else "Starting",
            'job_state': lambda: "Status" if is_ended else "S",
            'wall_time': lambda: "Duration" if not is_pending else "Time",
            'nodes': lambda: "Nodes",
        }

        static_labels = {
            'end_time': "Deadline",
            'submit_time': "Submitted",
            'job_id': "Job ID",
            'account': "Acct",
            'exit_code': "Exit code",
            'array_tasks': "Array",
            'user_name': "User",
            'partition': "Partition",
            'name': "Name",
            'reason': "Reason",
            'tres': "Resources",
        }

        # Build SORT_KEYS mapping based on current visible columns
        # (update it each time we build a header so it matches visible columns)
        self.SORT_KEYS = {}
        column_fields = list(display_attr.keys())
        for i, field in enumerate(column_fields):
            if i <= 9:  # Support keys 0-9
                self.SORT_KEYS[str(i)] = field

        header_columns = []
        for i, headeritem in enumerate(display_attr):
            if headeritem in dynamic_labels:
                label = dynamic_labels[headeritem]()
            elif headeritem in static_labels:
                label = static_labels[headeritem]
            else:
                label = headeritem.capitalize()

            # Add column number prefix
            if i <= 9:
                label = f"{i}:{label}"

            # Add sort indicator if this is the sorted column
            if self.sort_col == headeritem:
                arrow = "↓" if self.sort_reverse else "↑"
                label = f"{label} {arrow}"

            # Create column with same sizing as job widgets
            align, width, _ = display_attr[headeritem]
            h = u.Text(('faded', label))
            header_columns.append((align, width, h))

        # Return the header as a widget
        return u.Columns(header_columns, dividechars=1)

    def calculate_jobs_per_group(self):
        """Calculate how many jobs to show per group when collapsed based on available height."""
        if hasattr(self.main_screen, 'height'):
            # Reserve minimal space for headers/dividers, use rest for jobs
            # This ensures large screens are filled properly
            available = max(self.main_screen.height - 8, 5)
            # Use most of available height, reserving ~5 lines for section headers
            self.jobs_per_group = max(available - 5, 10)
        else:
            self.jobs_per_group = 10  # Default increased from 5

    def on_resize(self):
        """Handle resize events - redraw with new dimensions."""
        self.calculate_jobs_per_group()
        self.update()

    def update(self):
        self.draw_entities()
        self.draw_jobs()

    def draw_jobs(self):
        # Prevent recursion
        if self._drawing:
            return
        self._drawing = True

        try:
            entity_table = self.get_entity_table()
            if not entity_table:
                return
            if self.selected_entity not in entity_table:
                return

            jobtable = entity_table[self.selected_entity]['jobs']
            jobtable = self.sort_jobs(jobtable)

            # Set widget width for responsive display
            # Right panel is ~75% of screen width, minus borders (2-3 chars)
            available_width = int(self.main_screen.width * 0.75) - 3 if hasattr(self.main_screen, 'width') else None

            # Build widgets (Running > Pending > Ended > Other). Categorize first so
            # we can give every row in a category the same column layout — when any
            # row is an array parent, all rows in that category include array_tasks.
            job_sets = self.categorize_jobs(jobtable)
            for cat_jobs in job_sets.values():
                force_col = any(j.is_array_parent for j in cat_jobs)
                for job in cat_jobs:
                    job.set_widget_width(available_width, view_type=self.view_type,
                                         force_array_tasks_col=force_col)

            # Track selected job
            focus_w, _ = self.jobwalker.get_focus()
            if hasattr(focus_w, "jobid"):
                self.selected_job = focus_w.jobid
            else:
                self.selected_job = None

            u.disconnect_signal(self.jobwalker, 'modified', self.modified)
            self.jobwalker.clear()

            jobwalker_widgets = self._build_all_categories(job_sets)
            jobwalker_widgets = self._auto_expand_to_fill(jobwalker_widgets, job_sets)

            self.jobwalker.extend(jobwalker_widgets)
            self.jw.set_title(self.right_title_template.format(entity=self.selected_entity))
            self.restore_job_focus()
            u.connect_signal(self.jobwalker, 'modified', self.modified)
        finally:
            self._drawing = False

    def _build_all_categories(self, job_sets):
        widgets = []
        for category in ["Running", "Pending", "Ended", "Other"]:
            widgets.extend(self.build_job_widgets(job_sets[category], label=category))
        return widgets

    def _auto_expand_to_fill(self, widgets, job_sets):
        """If there's empty screen space, force-expand collapsed groups to fill it."""
        if not hasattr(self.main_screen, 'height'):
            return widgets
        available_lines = self.main_screen.height - 5
        if len(widgets) >= available_lines * SCREEN_FILL_RATIO:
            return widgets

        expandable = [w.group_key for w in widgets if isinstance(w, ExpandableGroupMarker)]
        if not expandable:
            return widgets

        for group_key in expandable:
            if group_key in self.collapsed_groups:
                self.collapsed_groups[group_key] = False
        return self._build_all_categories(job_sets)

    def draw_entities(self):
        entity_table = self.get_entity_table()
        if not entity_table:
            return

        # Sort by job count
        entity_table = dict(sorted(entity_table.items(), key=lambda item: item[1]['njobs'], reverse=True))

        u.disconnect_signal(self.entity_walker, 'modified', self.modified)
        self.entity_walker.clear()

        # Build entity widgets
        widgets = []
        for entity_name in entity_table:
            stats = entity_table[entity_name]
            widget = self.create_entity_widget(entity_name, stats['njobs'], stats['running'], stats['pending'])
            widgets.append(widget)

        self.entity_walker.extend(widgets)

        # Restore or set focus
        if self.selected_entity and self.selected_entity in entity_table:
            for i in self.entity_walker:
                entity_name = self._get_entity_from_widget(i)
                if entity_name == self.selected_entity:
                    pos = self.entity_walker.index(i)
                    self.entity_walker.set_focus(pos)
                    break
        else:
            if len(self.entity_walker) > 0:
                self.entity_walker.set_focus(0)
                focus_w, _ = self.entity_walker.get_focus()
                self.selected_entity = self._get_entity_from_widget(focus_w)

        u.connect_signal(self.entity_walker, 'modified', self.modified)

    def get_entity_table(self):
        """Return the entity table dict from `self.jobs.<table_attr>`."""
        return getattr(self.jobs, self.table_attr, None) if self.table_attr else None

    def create_entity_widget(self, entity_name, njobs, running, pending):
        """Create a left-panel widget for an entity row."""
        return UserItem(entity_name, njobs, running, pending)


def make_view(class_name, **config):
    """Build a `TwoColumnJobView` subclass from a config dict.

    Use for the standard built-in views; subclass directly only when behavior
    diverges beyond the configurable attributes.
    """
    return type(class_name, (TwoColumnJobView,), config)


