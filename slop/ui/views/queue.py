"""Queue Status view - Shows pending jobs from the scheduler's perspective.

Each row carries a stacked priority-component bar built from sprio data, and
the panel header summarizes pending counts plus the cluster's priority weights.
"""

import urwid as u
import datetime
from slop.utils import format_duration
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.widgets import rounded_box


# Component → (label, urwid attr, weight-config key). Order is left→right
# in the bar. Components whose PriorityWeight* is 0/null are skipped.
PRIORITY_COMPONENTS = (
    ('age',           'A', 'info',          'PriorityWeightAge'),
    ('fairshare',     'F', 'warning',       'PriorityWeightFairShare'),
    ('qos_pri',       'Q', 'success',       'PriorityWeightQOS'),
    ('jobsize',       'J', 'state_failed',  'PriorityWeightJobSize'),
    ('partition_pri', 'P', 'state_pending', 'PriorityWeightPartition'),
    ('tres',          'T', 'normal',        'PriorityWeightTRES'),
)

# 9 fill levels for a single cell: empty → full.
_FILL_LEVELS = ' ▁▂▃▄▅▆▇█'


def _enabled_components(weights):
    """List of (key, label, attr, weight) for components with nonzero weight."""
    out = []
    for key, label, attr, wkey in PRIORITY_COMPONENTS:
        w = (weights.get(wkey) or 0) if weights else 0
        if w > 0:
            out.append((key, label, attr, w))
    return out


def _fill_cell(value, ceiling):
    """Pick a block character whose fill height = value / ceiling."""
    if ceiling <= 0 or value <= 0:
        return ' '
    frac = min(1.0, value / ceiling)
    idx = max(1, min(8, int(round(frac * 8))))
    return _FILL_LEVELS[idx]


def _build_priority_bar(components, enabled):
    """Return urwid markup for a per-component utilization bar.

    Each enabled component gets one cell; the cell's fill height shows what
    fraction of that component's weight the job has earned. Color identifies
    the component (matching the legend in the panel header).
    """
    if not enabled:
        return [('faded', '·')]
    if not components:
        return [('faded', '·' * len(enabled))]

    segments = []
    for key, _label, attr, weight in enabled:
        ch = _fill_cell(components.get(key, 0) or 0, weight)
        if ch == ' ':
            segments.append(('faded', '·'))
        else:
            segments.append((attr, ch))
    return segments


def _bar_width_for(enabled):
    """Bar width = one cell per enabled priority component."""
    return max(1, len(enabled))


class QueueJobWidget(u.WidgetWrap):
    """Widget for displaying a single pending job in the queue view."""

    def __init__(self, job, rank, width=None, sprio_row=None, enabled_components=None):
        self.job = job
        self.jobid = job.job_id
        self.rank = rank
        self.width = width or 120
        self.sprio_row = sprio_row
        self.enabled_components = enabled_components or []

        widget = self._build_widget()
        super().__init__(widget)

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build_widget(self):
        job = self.job

        priority = getattr(job, 'priority', {})
        if isinstance(priority, dict):
            priority_num = priority.get('number', 0)
        else:
            priority_num = priority if isinstance(priority, int) else 0

        size_indicator = self._get_size_indicator()

        time_limit = getattr(job, 'time_limit', {})
        if isinstance(time_limit, dict) and time_limit.get('set'):
            duration_min = time_limit.get('number', 0)
            duration_str = format_duration(duration_min * 60)
        else:
            duration_str = EMPTY_PLACEHOLDER

        start_time = getattr(job, 'start_time', {})
        eta_str = self._get_eta_string(start_time)

        submit_time = getattr(job, 'submit_time', {})
        if isinstance(submit_time, dict) and submit_time.get('set'):
            submit = datetime.datetime.fromtimestamp(submit_time['number'])
            now = datetime.datetime.now()
            wait_sec = int((now - submit).total_seconds())
            wait_str = format_duration(wait_sec)
        else:
            wait_str = EMPTY_PLACEHOLDER

        reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER)
        resource_str = self._get_resource_summary()
        qos = getattr(job, 'qos', EMPTY_PLACEHOLDER)
        username = getattr(job, 'user_name', EMPTY_PLACEHOLDER)

        bar_segments = _build_priority_bar(self.sprio_row, self.enabled_components)
        row_attr = self._get_color_attr(reason)

        # Layout: rank(3) priority(6) bar [columns...]
        prefix = [f"{self.rank:>3} {priority_num:>6} "]
        if self.width < 100:
            tail = f"  {size_indicator:<3} {duration_str:>7} {username[:8]:<8} {job.name[:15]:<15}"
        elif self.width < 140:
            tail = (f"  {size_indicator:<3} {duration_str:>7} {eta_str[:14]:<14} "
                    f"{username[:10]:<10} {reason[:12]:<12} {job.name[:20]:<20}")
        else:
            tail = (f"  {size_indicator:<3} {duration_str:>7} {eta_str[:14]:<14} "
                    f"{wait_str[:11]:>11} {username[:10]:<10} {qos[:8]:<8} "
                    f"{reason[:15]:<15} {resource_str[:18]:<18} {job.name[:25]:<25}")

        markup = prefix + list(bar_segments) + [tail]
        return u.AttrMap(u.Text(markup), row_attr, 'normal_selected')

    def _get_size_indicator(self):
        """Get visual size indicator based on resource footprint."""
        job = self.job

        node_count = getattr(job, 'node_count', {})
        if isinstance(node_count, dict):
            nodes = node_count.get('number', 1)
        else:
            nodes = node_count if isinstance(node_count, int) else 1

        cpus_obj = getattr(job, 'cpus', {})
        if isinstance(cpus_obj, dict):
            cpus = cpus_obj.get('number', 1)
        else:
            cpus = cpus_obj if isinstance(cpus_obj, int) else 1

        time_limit = getattr(job, 'time_limit', {})
        if isinstance(time_limit, dict) and time_limit.get('set'):
            duration_min = time_limit.get('number', 60)
        else:
            duration_min = 60

        core_hours = (cpus * duration_min) / 60

        if core_hours < 100:
            return "▪"
        elif core_hours < 1000:
            return "▪▪"
        else:
            return "▪▪▪"

    def _get_eta_string(self, start_time):
        if not isinstance(start_time, dict) or not start_time.get('set'):
            return EMPTY_PLACEHOLDER

        start_timestamp = start_time.get('number', 0)
        if start_timestamp == 0:
            return EMPTY_PLACEHOLDER

        start_dt = datetime.datetime.fromtimestamp(start_timestamp)
        now = datetime.datetime.now()

        diff_sec = (start_dt - now).total_seconds()
        if diff_sec < 60:
            return "now"

        return f"in {format_duration(int(diff_sec))}"

    def _get_resource_summary(self):
        job = self.job

        node_count = getattr(job, 'node_count', {})
        if isinstance(node_count, dict):
            nodes = node_count.get('number', 0)
        else:
            nodes = node_count if isinstance(node_count, int) else 0

        cpus_obj = getattr(job, 'cpus', {})
        if isinstance(cpus_obj, dict):
            cpus = cpus_obj.get('number', 0)
        else:
            cpus = cpus_obj if isinstance(cpus_obj, int) else 0

        mem_per_node = getattr(job, 'memory_per_node', {})
        mem_per_cpu = getattr(job, 'memory_per_cpu', {})

        if isinstance(mem_per_node, dict) and mem_per_node.get('set'):
            mem_mb = mem_per_node.get('number', 0)
            mem_str = f"{mem_mb // 1024}GB" if mem_mb > 1024 else f"{mem_mb}MB"
        elif isinstance(mem_per_cpu, dict) and mem_per_cpu.get('set'):
            mem_mb = mem_per_cpu.get('number', 0) * cpus
            mem_str = f"{mem_mb // 1024}GB" if mem_mb > 1024 else f"{mem_mb}MB"
        else:
            mem_str = None

        tres_str = getattr(job, 'tres_req_str', '')
        gpu_count = 0
        if 'gres/gpu=' in tres_str:
            import re
            match = re.search(r'gres/gpu=(\d+)', tres_str)
            if match:
                gpu_count = int(match.group(1))

        parts = []
        if nodes > 0:
            parts.append(f"{nodes}n")
        if cpus > 0:
            parts.append(f"{cpus}c")
        if mem_str:
            parts.append(mem_str)
        if gpu_count > 0:
            parts.append(f"{gpu_count}gpu")

        return " ".join(parts) if parts else EMPTY_PLACEHOLDER

    def _get_color_attr(self, reason):
        if reason in ['Priority', 'Resources']:
            return 'normal'
        elif reason in ['Dependency', 'JobHeldUser', 'JobHeldAdmin']:
            return 'warning'
        elif 'NotAvail' in reason or 'Invalid' in reason:
            return 'error'
        else:
            return 'normal'


class QueueGroupWidget(u.WidgetWrap):
    """Widget for a collapsed group of jobs (same user + reason)."""

    def __init__(self, start_rank, end_rank, job_group, width=None,
                 sprio=None, enabled_components=None):
        self.start_rank = start_rank
        self.end_rank = end_rank
        self.job_group = job_group
        self.group_key = f"{start_rank}-{end_rank}"
        self.width = width or 120
        self.sprio = sprio or {}
        self.enabled_components = enabled_components or []

        widget = self._build_widget()
        super().__init__(widget)

    def selectable(self):
        return True

    def keypress(self, size, key):
        return key

    def _build_widget(self):
        job = self.job_group[0]
        count = len(self.job_group)

        username = getattr(job, 'user_name', EMPTY_PLACEHOLDER)
        reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER)

        priorities = []
        for j in self.job_group:
            priority = getattr(j, 'priority', {})
            if isinstance(priority, dict):
                priorities.append(priority.get('number', 0))
            else:
                priorities.append(priority if isinstance(priority, int) else 0)
        priority_num = max(priorities) if priorities else 0

        sizes = []
        for j in self.job_group:
            widget = QueueJobWidget(j, 0, width=self.width)
            sizes.append(widget._get_size_indicator())
        size_indicator = max(set(sizes), key=sizes.count) if sizes else "▪"

        durations = []
        for j in self.job_group:
            time_limit = getattr(j, 'time_limit', {})
            if isinstance(time_limit, dict) and time_limit.get('set'):
                durations.append(time_limit.get('number', 0))

        if durations:
            min_dur = min(durations)
            max_dur = max(durations)
            if min_dur == max_dur:
                duration_str = format_duration(min_dur * 60)
            else:
                duration_str = f"{format_duration(min_dur * 60)}-{format_duration(max_dur * 60)}"
        else:
            duration_str = EMPTY_PLACEHOLDER

        rank_num = self.start_rank
        if self.start_rank == self.end_rank:
            name_col = f"[{count} jobs]"
        else:
            name_col = f"[{count} jobs #{self.start_rank}-{self.end_rank}]"

        # Aggregate bar: average each component across the group so the bar
        # represents the group as a whole.
        aggregated = {}
        sampled = 0
        for j in self.job_group:
            row = self.sprio.get(j.job_id)
            if row:
                sampled += 1
                for key, _, _, _ in PRIORITY_COMPONENTS:
                    aggregated[key] = aggregated.get(key, 0) + (row.get(key, 0) or 0)
        if sampled:
            for k in aggregated:
                aggregated[k] //= max(sampled, 1)
        bar = list(_build_priority_bar(aggregated if sampled else None,
                                       self.enabled_components))

        ph = EMPTY_PLACEHOLDER
        prefix = [f"{rank_num:>3} {priority_num:>6} "]
        if self.width < 100:
            tail = f"  {size_indicator:<3} {duration_str:>7} {username[:8]:<8} {name_col[:15]:<15}"
        elif self.width < 140:
            tail = (f"  {size_indicator:<3} {duration_str:>7} {ph:<14} "
                    f"{username[:10]:<10} {reason[:12]:<12} {name_col[:20]:<20}")
        else:
            tail = (f"  {size_indicator:<3} {duration_str:>7} {ph:<14} "
                    f"{ph:>11} {username[:10]:<10} {ph:<8} "
                    f"{reason[:15]:<15} {ph:<18} {name_col[:25]:<25}")

        markup = prefix + bar + [tail]
        return u.AttrMap(u.Text(markup), 'normal', 'normal_selected')


class ScreenViewQueue(u.WidgetWrap):
    """Queue status view - shows pending jobs from scheduler's perspective."""

    def __init__(self, main_screen, jobs, sprio_fetcher=None, priority_weights=None):
        self.main_screen = main_screen
        self.jobs = jobs
        self.sprio_fetcher = sprio_fetcher
        self.priority_weights = priority_weights or {}

        self.expanded_groups = set()

        # Two-line summary block above the column header.
        self.summary_text = u.Text("")
        self.col_header_text = u.AttrMap(u.Text(""), 'jobheader')
        self.job_walker = u.SimpleFocusListWalker([])
        self.job_listbox = u.ListBox(self.job_walker)

        pile = u.Pile([
            ('pack', self.summary_text),
            ('pack', u.Divider("─")),
            ('pack', self.col_header_text),
            ('pack', u.Divider("─")),
            u.ScrollBar(self.job_listbox)
        ])

        self.container = rounded_box(pile, title='Queue Status - Pending Jobs by Priority')

        body = u.AttrMap(self.container, 'bg')

        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)
        u.WidgetWrap.__init__(self, body)

        self.update()

    def on_jobs_update(self, *_args, **_kwargs):
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        self.update()

    def _sprio_data(self):
        return self.sprio_fetcher.fetch_sync() if self.sprio_fetcher else {}

    def _summary_markup(self, pending_jobs, with_eta, sprio_count, enabled):
        """Top summary line + priority-weight legend line."""
        total = len(pending_jobs)

        line1 = [
            ('jobheader', '  '),
            ('jobheader', f'Pending: {total}'),
            '   ',
            f'with ETA: {with_eta}',
            '   ',
            f'sprio rows: {sprio_count}',
        ]
        if not enabled:
            return [line1, [('faded', '  (no PriorityWeight* configured)')]]

        # Legend: each component shows its letter (colored) and weight ceiling.
        # Trailing hint explains the cell fill levels.
        legend = ['  Components (cell = % of weight earned):  ']
        for _key, label, attr, weight in enabled:
            legend.append((attr, label))
            legend.append(f':{weight}  ')
        legend.append(('faded', f'fill: {_FILL_LEVELS[1]}…{_FILL_LEVELS[8]} (12%→100%)'))
        return [line1, legend]

    def _refresh_summary(self, pending_jobs, with_eta, enabled):
        sprio = self._sprio_data()
        sprio_count = sum(1 for j in pending_jobs if j.job_id in sprio)
        line1, line2 = self._summary_markup(pending_jobs, with_eta, sprio_count, enabled)
        markup = list(line1) + ['\n'] + list(line2)
        self.summary_text.set_text(markup)

    def update(self):
        self.job_walker.clear()
        widgets = []

        available_width = self.main_screen.width - 3 if hasattr(self.main_screen, 'width') else 120
        sprio = self._sprio_data()
        enabled = _enabled_components(self.priority_weights)
        bar_width = _bar_width_for(enabled)

        self.container.set_title("Queue Status - Pending Jobs by Priority (grouped by user)")

        # Column header — bar cells are labeled with their component letters
        # (e.g. "AFQ" for Age/FairShare/QOS).
        if enabled:
            bar_label = ''.join(lbl for _, lbl, _, _ in enabled)
        else:
            bar_label = '·' * bar_width
        if available_width < 100:
            header_text = (f"{'#':>3} {'Priority':>6} {bar_label} "
                           f"{'Sz':<3} {'Time':>7} {'User':<8} {'Job Name':<15}")
        elif available_width < 140:
            header_text = (f"{'#':>3} {'Priority':>6} {bar_label} "
                           f"{'Sz':<3} {'Time':>7} {'ETA':<14} "
                           f"{'User':<10} {'Reason':<12} {'Job Name':<20}")
        else:
            header_text = (f"{'#':>3} {'Priority':>6} {bar_label} "
                           f"{'Sz':<3} {'Time':>7} {'ETA':<14} {'Waiting':>11} "
                           f"{'User':<10} {'QOS':<8} {'Reason':<15} "
                           f"{'Resources':<18} {'Job Name':<25}")
        self.col_header_text.original_widget.set_text(header_text)

        pending_jobs = []
        for job in self.jobs.jobs:
            if hasattr(job, 'job_state') and 'PENDING' in job.job_state:
                pending_jobs.append(job)

        # Count jobs with a usable ETA for the summary.
        with_eta = 0
        for job in pending_jobs:
            st = getattr(job, 'start_time', {})
            if isinstance(st, dict) and st.get('set') and st.get('number', 0) > 0:
                with_eta += 1
        self._refresh_summary(pending_jobs, with_eta, enabled)

        if not pending_jobs:
            widgets.append(u.Text(("faded", "  No pending jobs in the queue")))
        else:
            def get_priority(job):
                priority = getattr(job, 'priority', {})
                if isinstance(priority, dict):
                    return priority.get('number', 0)
                return priority if isinstance(priority, int) else 0

            sorted_jobs = sorted(pending_jobs, key=get_priority, reverse=True)

            # Group consecutive jobs by (user, reason)
            groups = []
            current_group = []
            current_user = None
            current_reason = None

            for job in sorted_jobs:
                user = getattr(job, 'user_name', EMPTY_PLACEHOLDER)
                reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER)

                if user == current_user and reason == current_reason:
                    current_group.append(job)
                else:
                    if current_group:
                        groups.append(current_group)
                    current_group = [job]
                    current_user = user
                    current_reason = reason

            if current_group:
                groups.append(current_group)

            rank = 1
            for group in groups:
                group_size = len(group)
                start_rank = rank
                end_rank = rank + group_size - 1
                group_key = f"{start_rank}-{end_rank}"

                if group_size == 1:
                    sprio_row = sprio.get(group[0].job_id)
                    widgets.append(QueueJobWidget(
                        group[0], rank, width=available_width,
                        sprio_row=sprio_row, enabled_components=enabled,
                    ))
                elif group_key in self.expanded_groups:
                    for i, job in enumerate(group):
                        sprio_row = sprio.get(job.job_id)
                        widgets.append(QueueJobWidget(
                            job, start_rank + i, width=available_width,
                            sprio_row=sprio_row, enabled_components=enabled,
                        ))
                else:
                    widgets.append(QueueGroupWidget(
                        start_rank, end_rank, group, width=available_width,
                        sprio=sprio, enabled_components=enabled,
                    ))

                rank += group_size

        self.job_walker.extend(widgets)

        if len(self.job_walker) > 0:
            first_job_idx = None
            first_group_idx = None

            for i, widget in enumerate(self.job_walker):
                if hasattr(widget, 'jobid') and first_job_idx is None:
                    first_job_idx = i
                    break
                elif hasattr(widget, 'group_key') and first_group_idx is None:
                    first_group_idx = i

            if first_job_idx is not None:
                self.job_walker.set_focus(first_job_idx)
            elif first_group_idx is not None:
                self.job_walker.set_focus(first_group_idx)

    def keypress(self, size, key):
        if self.main_screen.overlay_showing:
            return key

        focus_w, _ = self.job_listbox.get_focus()

        if key in ('e', 'enter', ' '):
            if hasattr(focus_w, 'group_key'):
                group_key = focus_w.group_key
                if group_key in self.expanded_groups:
                    self.expanded_groups.remove(group_key)
                else:
                    self.expanded_groups.add(group_key)
                self.update()
                return None
            elif hasattr(focus_w, 'jobid') and key in ('enter', ' '):
                from slop.ui.overlays import JobInfoOverlay
                job = self.jobs.job_index.get(focus_w.jobid)
                if job:
                    self.main_screen.open_overlay(JobInfoOverlay(job, self.main_screen))
                return None

        return super().keypress(size, key)
