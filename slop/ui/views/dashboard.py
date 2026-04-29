"""Dashboard view (F1) - first impression landing screen.

Three stripes:
  * full-width  CLUSTER PULSE     (nodes / CPU / memory / GPU bars per type)
  * side-by-side YOU and LIVE QUEUE   (adaptive YOU panel based on user activity)
  * one-line activity ticker         (jobs started / finished in the last 10 min)

Reads from the same Jobs / ClusterResources / sdiag fetchers used by the
specialized views; refresh is signal-driven via `jobs_updated`.
"""
import time
from collections import Counter

import urwid as u

from slop.models import ClusterResources
from slop.ui.constants import EMPTY_PLACEHOLDER
from slop.ui.widgets import rounded_box
from slop.utils import format_duration
from slop.ui.views.queue_helpers import coarse_duration


# Activity ticker window — same horizon as the mockup.
ACTIVITY_WINDOW = 10 * 60
LONG_PENDING_THRESHOLD = 24 * 3600


def _fmt_bytes_mb(mb):
    if mb < 1024:
        return f"{mb:.0f}MB"
    if mb < 1024 * 1024:
        return f"{mb / 1024:.1f}GB"
    return f"{mb / (1024 * 1024):.2f}TB"


def _bar_markup(used, total, width):
    """Return a urwid markup list for a fixed-width progress bar.

    Colour is picked from utilisation: red ≥90%, yellow ≥75%, cyan <25%,
    otherwise green. Empty cells stay faded so the bar end is visible even
    when nothing is allocated. Skips zero-length chunks — urwid's clipped
    text renderer can leave the cell after an empty markup chunk holding
    stale content from prior frames.
    """
    if width <= 0:
        return []
    if total <= 0:
        return [('faded', '░' * width)]
    pct = used / total
    filled = max(0, min(width, int(round(pct * width))))
    if pct >= 0.90:
        attr = 'error'
    elif pct >= 0.75:
        attr = 'warning'
    elif pct < 0.25:
        attr = 'info'
    else:
        attr = 'success'
    parts = []
    if filled:
        parts.append((attr, '█' * filled))
    if width - filled:
        parts.append(('faded', '░' * (width - filled)))
    return parts


def _tres_int(job, key):
    """Integer count for `key` from a job's TRES alloc/req string, 0 if absent."""
    s = getattr(job, 'tres_alloc_str', '') or getattr(job, 'tres_req_str', '')
    if not s:
        return 0
    for entry in s.split(','):
        if '=' not in entry:
            continue
        k, v = entry.split('=', 1)
        if k == key:
            try:
                return int(v)
            except ValueError:
                return 0
    return 0


def _tres_mem_mb(job):
    """Memory request in MB (parses G/M/T suffixes)."""
    s = getattr(job, 'tres_alloc_str', '') or getattr(job, 'tres_req_str', '')
    if not s:
        return 0
    for entry in s.split(','):
        if '=' not in entry:
            continue
        k, v = entry.split('=', 1)
        if k != 'mem':
            continue
        v = v.strip()
        try:
            if v.endswith('G'):
                return int(v[:-1]) * 1024
            if v.endswith('T'):
                return int(v[:-1]) * 1024 * 1024
            if v.endswith('M'):
                return int(v[:-1])
            return int(v)
        except ValueError:
            return 0
    return 0


def _ts(time_dict):
    if isinstance(time_dict, dict):
        return time_dict.get('number', 0) or 0
    return 0


# ----- Section builders ---------------------------------------------------


def _pulse_section(stats, gpu_stats, width):
    """CLUSTER PULSE box: node summary + CPU/Memory bars + GPU per type."""
    rows = []
    inner = max(20, width - 4)
    bar_w = max(20, inner - 18 - 30)

    # Nodes row
    rows.append(_labeled(
        'Nodes',
        [
            ('success', f"{stats['up_nodes']:>3} UP"),
            ('normal', '  '),
            ('error', f"{stats['down_nodes']:>2} DOWN"),
            ('normal', '  '),
            ('warning', f"{stats.get('drain_nodes', 0):>2} DRAIN"),
            ('normal', '  '),
            ('faded', f"(total {stats['total_nodes']})"),
        ],
    ))

    # CPU row
    cpu_pct = stats['cpu_util']
    rows.append(_labeled('CPU', _bar_markup(stats['cpus_alloc'], stats['cpus_total'], bar_w)
                         + [('normal', f"  {stats['cpus_alloc']}/{stats['cpus_total']} cores  {cpu_pct:5.1f}%")]))

    # Memory row
    mem_pct = stats['mem_util']
    mem_meta = (f"  {_fmt_bytes_mb(stats['mem_alloc_mb'])} / "
                f"{_fmt_bytes_mb(stats['mem_total_mb'])}  {mem_pct:5.1f}%")
    rows.append(_labeled('Memory', _bar_markup(stats['mem_alloc_mb'], stats['mem_total_mb'], bar_w)
                         + [('normal', mem_meta)]))

    # GPU rows (sorted by utilisation descending so saturated types pop)
    if gpu_stats:
        rows.append(u.Text(""))
        # Pad every typ name to the widest one so bars line up across rows.
        typ_w = max(14, max(len(t) for t in gpu_stats))
        gpu_bar_w = max(12, bar_w - (typ_w + 2) - 18)
        items = sorted(gpu_stats.items(),
                       key=lambda kv: -(kv[1]['used'] / kv[1]['total']
                                        if kv[1]['total'] else 0))
        for i, (typ, s) in enumerate(items):
            label = 'GPUs' if i == 0 else ''
            pct = (s['used'] / s['total'] * 100) if s['total'] else 0
            tag = []
            if pct == 0:
                tag = [('normal', '  '), ('info', '← available')]
            elif pct >= 100:
                tag = [('normal', '  '), ('error', 'saturated')]
            content = (
                [('normal', f"{typ:<{typ_w}s} ")]
                + _bar_markup(s['used'], s['total'], gpu_bar_w)
                + [('normal', f"  {s['used']}/{s['total']}  {pct:5.1f}%")]
                + tag
            )
            rows.append(_labeled(label, content))

    pile = u.Pile(rows)
    return rounded_box(pile, title='CLUSTER PULSE')


def _labeled(label, markup, gutter=18):
    """Row with a fixed-width label gutter on the left + markup payload."""
    text = ([(None, label.ljust(gutter))] if label else [(None, ' ' * gutter)])
    text += markup if isinstance(markup, list) else [(None, str(markup))]
    return u.Text(text, wrap='clip')


# ----- YOU and LIVE QUEUE -------------------------------------------------


def _you_section(user, jobs, now, free_cpu, free_gpu_types):
    """Adaptive YOU panel — running/pending detail or idle exploration prompts."""
    rows = []
    mine = [j for j in jobs if getattr(j, 'user_name', None) == user]
    running = [j for j in mine if 'RUNNING' in (getattr(j, 'job_state', None) or [])]
    pending = [j for j in mine if 'PENDING' in (getattr(j, 'job_state', None) or [])]
    completed = [j for j in mine if (getattr(j, 'job_state', None) or [None])[0] == 'COMPLETED']

    if running or pending:
        bits = []
        if running:
            bits += [('success', f"▶ {len(running)} running")]
        if pending:
            if bits:
                bits += [('normal', '   ')]
            bits += [('warning', f"⏸ {len(pending)} pending")]
        if completed:
            if bits:
                bits += [('normal', '   ')]
            bits += [('info', f"✓ {len(completed)} just done")]
        rows.append(_labeled('', bits))
        rows.append(u.Text(""))

        # Next to finish
        running_with_end = [(_ts(getattr(j, 'end_time', {})), j)
                            for j in running if _ts(getattr(j, 'end_time', {})) > 0]
        running_with_end.sort(key=lambda x: (x[0], getattr(x[1], 'job_id', 0)))
        if running_with_end:
            et, j = running_with_end[0]
            remaining = format_duration(max(0, et - now))
            elapsed = format_duration(max(0, now - _ts(getattr(j, 'start_time', {}))))
            rows.append(_labeled(
                'Next to finish',
                [('normal', f"job {j.job_id}  in "),
                 ('success', remaining),
                 ('faded', f"  (ran {elapsed} on {getattr(j, 'nodes', '—') or '—'})")],
            ))
            name = (getattr(j, 'name', '') or '')[:60]
            if name:
                rows.append(_labeled('', [('faded', name)]))

        # Next to start
        with_eta = [(_ts(getattr(j, 'start_time', {})), j)
                    for j in pending if _ts(getattr(j, 'start_time', {})) > 0]
        with_eta.sort(key=lambda x: (x[0], getattr(x[1], 'job_id', 0)))
        if with_eta:
            st, j = with_eta[0]
            eta = format_duration(max(0, st - now))
            all_eta = sorted(
                _ts(getattr(p, 'start_time', {})) for p in jobs
                if 'PENDING' in (getattr(p, 'job_state', None) or [])
                and _ts(getattr(p, 'start_time', {})) > 0
            )
            try:
                pos = all_eta.index(st) + 1
                pos_str = f"#{pos}"
            except ValueError:
                pos_str = '?'
            rows.append(_labeled(
                'Next to start',
                [('normal', f"job {j.job_id}  ETA "),
                 ('info', eta),
                 ('faded', f"  (cluster queue position {pos_str})")],
            ))

        # Long-pending warning
        long_pending = [j for j in pending
                        if (now - _ts(getattr(j, 'submit_time', {}))) > LONG_PENDING_THRESHOLD]
        if long_pending:
            top_reason = Counter(getattr(j, 'state_reason', '') for j in long_pending).most_common(1)[0]
            rows.append(_labeled('', [
                ('warning',
                 f"⚠ {len(long_pending)} pending >24h "
                 f"(top reason: {top_reason[0]} ×{top_reason[1]})"),
            ]))
    else:
        rows.append(_labeled('', [('faded', 'You have no jobs running or pending right now.')]))
        rows.append(u.Text(""))
        gpu_avail = ', '.join(free_gpu_types) or 'none'
        rows.append(_labeled(
            'Available now',
            [('success', f"{free_cpu} free CPUs"),
             ('normal', ' · '),
             ('success', f"{len(free_gpu_types)} GPU types"),
             ('normal', ' have capacity')],
        ))
        rows.append(_labeled('', [('faded', f"({gpu_avail})")]))
        rows.append(u.Text(""))
        rows.append(_labeled('Try', [('info', '/'),
                                     ('normal', ' look up any user, account, or job id')]))
        rows.append(_labeled('', [('info', 'F2'),
                                  ('normal', ' browse jobs by user/acct/partition/state')]))
        rows.append(_labeled('', [('info', 'F7'),
                                  ('normal', ' watch live queue flow')]))

    pile = u.Pile(rows)
    return rounded_box(pile, title=f'YOU ({user})')


def _queue_section(jobs, now):
    """LIVE QUEUE box: counts, soonest-to-start preview, top wait reasons."""
    rows = []

    running = [j for j in jobs if 'RUNNING' in (getattr(j, 'job_state', None) or [])]
    pending = [j for j in jobs if 'PENDING' in (getattr(j, 'job_state', None) or [])]

    cpus_running = sum(_tres_int(j, 'cpu') for j in running)
    nodes_running = sum(_tres_int(j, 'node') for j in running)
    rows.append(_labeled(
        '',
        [('success', 'RUNNING'),
         ('normal', f"   {len(running)} jobs · {cpus_running:,} cpus · {nodes_running} nodes")],
        gutter=0,
    ))

    waits = [now - _ts(getattr(j, 'submit_time', {})) for j in pending
             if _ts(getattr(j, 'submit_time', {})) > 0]
    tlims = []
    for j in pending:
        tl = getattr(j, 'time_limit', {})
        if isinstance(tl, dict) and tl.get('number'):
            tlims.append(tl['number'] * 60)
    avg_wait = sum(waits) / len(waits) if waits else 0
    avg_tlim = sum(tlims) / len(tlims) if tlims else 0
    rows.append(_labeled(
        '',
        [('warning', 'PENDING'),
         ('normal',
          f"   {len(pending)} jobs · avg requested {coarse_duration(avg_tlim)}"
          f" · avg wait {coarse_duration(avg_wait)}")],
        gutter=0,
    ))
    rows.append(u.Text(""))

    # Soonest to start
    with_eta = sorted(
        [(_ts(getattr(j, 'start_time', {})), j) for j in pending
         if _ts(getattr(j, 'start_time', {})) > 0],
        key=lambda x: (x[0], getattr(x[1], 'job_id', 0)),
    )
    rows.append(_labeled('Soonest to start', [], gutter=2))
    for st, j in with_eta[:3]:
        diff = st - now
        eta = format_duration(max(0, diff))
        if diff < 3600:
            eta_attr = 'success'
        elif diff < 86400:
            eta_attr = 'warning'
        else:
            eta_attr = 'faded'
        cpus = _tres_int(j, 'cpu')
        mem_gb = _tres_mem_mb(j) // 1024
        user = getattr(j, 'user_name', '?') or '?'
        acct = getattr(j, 'account', '?') or '?'
        name = (getattr(j, 'name', '') or '')[:18]
        rows.append(_labeled('', [
            (eta_attr, f"{eta:>8}"),
            ('normal', '  '),
            ('normal', f"{user:<14}"),
            ('faded', f" ({acct}) "),
            ('normal', f"{cpus:>3}c {mem_gb:>4}G  "),
            ('faded', name),
        ], gutter=2))
    rows.append(u.Text(""))

    # Top wait reasons (mini bar chart)
    reasons = Counter(getattr(j, 'state_reason', '') for j in pending)
    top = reasons.most_common(3)
    if top:
        rows.append(_labeled('Top wait reasons', [], gutter=2))
        max_n = top[0][1] or 1
        for r, n in top:
            barlen = max(1, int(round(n / max_n * 14)))
            bar_attr = 'warning' if r == 'Resources' else 'info'
            rows.append(_labeled('', [
                ('normal', f"{r:<22} {n:>4}  "),
                (bar_attr, '█' * barlen),
            ], gutter=2))

    pile = u.Pile(rows)
    return rounded_box(pile, title='LIVE QUEUE')


def _activity_line(jobs, now):
    started_recent = [
        j for j in jobs
        if _ts(getattr(j, 'start_time', {})) > 0
        and (now - _ts(getattr(j, 'start_time', {}))) < ACTIVITY_WINDOW
        and 'RUNNING' in (getattr(j, 'job_state', None) or [])
    ]
    ended_recent = [
        j for j in jobs
        if _ts(getattr(j, 'end_time', {})) > 0
        and 0 < (now - _ts(getattr(j, 'end_time', {}))) < ACTIVITY_WINDOW
        and (getattr(j, 'job_state', None) or [None])[0] in ('COMPLETED', 'FAILED', 'CANCELLED')
    ]
    completed = sum(1 for j in ended_recent
                    if (getattr(j, 'job_state', None) or [None])[0] == 'COMPLETED')
    cancelled = sum(1 for j in ended_recent
                    if (getattr(j, 'job_state', None) or [None])[0] == 'CANCELLED')
    failed = sum(1 for j in ended_recent
                 if (getattr(j, 'job_state', None) or [None])[0] == 'FAILED')
    return u.Text([
        ('faded', '  Last 10 min:  '),
        ('success', f"▶ {len(started_recent)} started"),
        ('normal', '  ·  '),
        ('info', f"✓ {completed} finished"),
        ('normal', '  ·  '),
        ('faded', f"⊗ {cancelled} cancelled  ·  ✗ {failed} failed"),
    ])


# ----- Screen -------------------------------------------------------------


class ScreenViewDashboard(u.WidgetWrap):
    """First-impression dashboard. F1 lands here."""

    def __init__(self, main_screen, jobs, cluster_fetcher):
        self.main_screen = main_screen
        self.jobs = jobs
        self.cluster_fetcher = cluster_fetcher

        # Plain placeholder over a top-anchored Pile — the dashboard isn't a
        # scrollable list, so a ListBox would only add focus/scroll state that
        # gets reset on every refresh (jumping the viewport to the bottom).
        # The Pile ends with a weighted SolidFill that explicitly repaints the
        # area below the content; without it, stale cells from a prior render
        # (e.g. the pre-data startup layout) bleed through.
        self.placeholder = u.WidgetPlaceholder(u.SolidFill(' '))
        u.connect_signal(self.jobs, 'jobs_updated', self.on_jobs_update)

        u.WidgetWrap.__init__(self, u.AttrMap(self.placeholder, 'bg'))
        self.update()

    def on_jobs_update(self, *_a, **_kw):
        if self.is_active():
            self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        self.update()

    def update(self):
        width = getattr(self.main_screen, 'width', 120)
        now = time.time()

        nodes_data = self.cluster_fetcher.fetch_nodes_sync() or {'nodes': []}
        partitions_data = self.cluster_fetcher.fetch_partitions_sync()
        cluster = ClusterResources(nodes_data, partitions_data)
        stats = cluster.get_overall_stats()

        # ClusterResources doesn't surface a DRAIN count separately, so add it
        # here from the raw node list (DRAINING counts as DRAIN for this view).
        drain_states = {'DRAIN', 'DRAINING'}
        stats['drain_nodes'] = sum(
            1 for n in cluster.nodes
            if any(s in drain_states for s in n.state) and n.is_up
        )
        gpu_stats = cluster.get_gpu_stats()

        free_cpu = stats['cpus_total'] - stats['cpus_alloc']
        free_gpu_types = sorted(
            [t for t, s in gpu_stats.items() if s['used'] < s['total']],
            key=lambda t: -(gpu_stats[t]['total'] - gpu_stats[t]['used']),
        )

        user = getattr(self.main_screen, 'current_username', 'unknown')

        pulse = _pulse_section(stats, gpu_stats, width)
        you = _you_section(user, self.jobs.jobs, now, free_cpu, free_gpu_types)
        queue = _queue_section(self.jobs.jobs, now)
        activity = _activity_line(self.jobs.jobs, now)

        # Stack: pulse (full) → mid (you | queue) → activity ticker.
        mid = u.Columns([('weight', 50, you), ('weight', 50, queue)], dividechars=2)

        pile = u.Pile([
            ('pack', pulse),
            ('pack', u.Divider()),
            ('pack', mid),
            ('pack', u.Divider()),
            ('pack', activity),
            ('weight', 1, u.SolidFill(' ')),
        ])
        self.placeholder.original_widget = pile
