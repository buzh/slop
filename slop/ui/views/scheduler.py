"""Scheduler health view (F8) — slurmctld diagnostics + partition-grouped pending queue.

Top half: backfill / main scheduler stats and slowest hot RPCs from sdiag.
Bottom half: PendingListWidget (the partition-grouped pending list that used
to sit at the bottom of F7). The two halves refresh independently — sdiag on
its 30s timer, the pending list on every jobs_updated signal.
"""
import urwid as u
from slop.ui.widgets import SafeListBox, SectionHeader, rounded_box
from slop.ui.views.pending_list import PendingListWidget


def _num(d):
    return d.get('number', 0) if isinstance(d, dict) else (d or 0)


def _fmt_us(microseconds):
    if microseconds < 1000:
        return f'{microseconds}µs'
    if microseconds < 1_000_000:
        return f'{microseconds/1000:.1f}ms'
    return f'{microseconds/1e6:.2f}s'


def _fmt_dur(seconds):
    seconds = max(0, int(seconds))
    d, r = divmod(seconds, 86400)
    h, r = divmod(r, 3600)
    m, _ = divmod(r, 60)
    if d: return f'{d}d {h}h'
    if h: return f'{h}h {m}m'
    return f'{m}m'


def _fmt_int(n):
    return f'{n:,}'.replace(',', ' ')


class ScreenViewScheduler(u.WidgetWrap):
    """Scheduler health view — backfill stats, queue summary, RPC pressure."""

    # Vertical split between scheduler stats (top) and pending-queue (bottom).
    # The user dropped RPC pressure to free up room for the pending list, so
    # the pending list gets the larger share.
    SECTION_WEIGHTS = (40, 60)

    def __init__(self, main_screen, sdiag_fetcher):
        self.main_screen = main_screen
        self.sdiag_fetcher = sdiag_fetcher
        self.walker = u.SimpleFocusListWalker([])
        self.listbox = SafeListBox(self.walker)

        self.pending_list = PendingListWidget(main_screen, main_screen.jobs)

        outer = u.Pile([
            ('weight', self.SECTION_WEIGHTS[0], u.ScrollBar(self.listbox)),
            ('weight', self.SECTION_WEIGHTS[1], self.pending_list),
        ], focus_item=1)

        widget = rounded_box(outer, title='Scheduler Health & Pending Queue')
        u.WidgetWrap.__init__(self, widget)
        self.update()

    def is_active(self):
        return self.main_screen.frame.body.base_widget is self

    def on_resize(self):
        self.update()

    # ------------------------------------------------------------------
    # render helpers
    # ------------------------------------------------------------------

    def _kv_row(self, pairs, indent=4):
        """Build a `Text` from (label, value, attr_or_None) tuples."""
        markup = [' ' * indent]
        for i, item in enumerate(pairs):
            if i:
                markup.append('   ')
            label, value, attr = item
            markup.append(('faded', f'{label} '))
            markup.append((attr, str(value)) if attr else str(value))
        return u.Text(markup, wrap='clip')

    def _exit_reasons(self, reasons, indent=4):
        """Render scheduler exit-reason counters with color emphasis."""
        markup = [' ' * indent, ('faded', 'exit reasons  ')]
        nz = [(k, v) for k, v in reasons.items() if v]
        if not nz:
            markup.append(('faded', 'none'))
            return u.Text(markup, wrap='clip')
        for i, (k, v) in enumerate(sorted(nz, key=lambda x: -x[1])):
            if i:
                markup.append('  ')
            attr = 'success' if k == 'end_job_queue' else 'warning'
            markup.append((attr, f'{k}'))
            markup.append(('faded', f'={v}'))
        return u.Text(markup, wrap='clip')

    def _rpc_msg_row(self, msg_type, count, avg_us):
        col = 'error' if avg_us > 10000 else 'warning' if avg_us > 1000 else 'normal'
        return u.Text([
            '    ',
            f'{msg_type:<35} ',
            f'{_fmt_int(count):>10}  ',
            (col, f'{_fmt_us(avg_us):>10}'),
        ], wrap='clip')

    # ------------------------------------------------------------------
    # update — rebuild the list
    # ------------------------------------------------------------------

    def update(self):
        try:
            old_focus = self.walker.focus
        except (IndexError, AttributeError):
            old_focus = None

        data = self.sdiag_fetcher.fetch_sync() or {}
        s = data.get('statistics', {})

        widgets = []

        if not s:
            err = self.sdiag_fetcher.last_error or 'no data yet'
            widgets.append(u.Text([('warning', f'No sdiag data available: {err}')]))
            self.walker.clear()
            self.walker.extend(widgets)
            return

        now = _num(s.get('req_time'))
        boot = _num(s.get('req_time_start'))
        last_bf = _num(s.get('bf_when_last_cycle'))

        # --- Snapshot summary -----------------------------------------
        widgets.append(u.Text([
            'slurmctld snapshot  ',
            ('faded',
             f'uptime {_fmt_dur(now - boot)}  ·  '
             f'last backfill cycle {_fmt_dur(now - last_bf)} ago')
        ], wrap='clip'))
        widgets.append(u.Divider())

        # --- Backfill scheduler ---------------------------------------
        widgets.append(SectionHeader('BACKFILL SCHEDULER'))
        widgets.append(self._kv_row([
            ('last/mean/max cycle',
             f'{_fmt_us(s.get("bf_cycle_last", 0))} / '
             f'{_fmt_us(s.get("bf_cycle_mean", 0))} / '
             f'{_fmt_us(s.get("bf_cycle_max", 0))}', None),
            ('cycles since boot', _fmt_int(s.get('bf_cycle_counter', 0)), None),
        ]))
        widgets.append(self._kv_row([
            ('queue / table size',
             f'{s.get("bf_queue_len", 0)} / {s.get("bf_table_size", 0)}', None),
            ('depth (last/try)',
             f'{s.get("bf_last_depth", 0)} / {s.get("bf_last_depth_try", 0)}', None),
        ]))
        widgets.append(self._kv_row([
            ('backfilled (last cycle / total)',
             f'{s.get("bf_last_backfilled_jobs", 0)} / '
             f'{_fmt_int(s.get("bf_backfilled_jobs", 0))}',
             'success' if s.get('bf_last_backfilled_jobs', 0) > 0 else None),
        ]))
        widgets.append(self._exit_reasons(s.get('bf_exit') or {}))
        widgets.append(u.Divider())

        # --- Main scheduler -------------------------------------------
        widgets.append(SectionHeader('MAIN SCHEDULER'))
        widgets.append(self._kv_row([
            ('last/mean/max cycle',
             f'{_fmt_us(s.get("schedule_cycle_last", 0))} / '
             f'{_fmt_us(s.get("schedule_cycle_mean", 0))} / '
             f'{_fmt_us(s.get("schedule_cycle_max", 0))}', None),
            ('per minute', s.get('schedule_cycle_per_minute', 0), None),
            ('queue len', s.get('schedule_queue_length', 0), None),
        ]))
        widgets.append(self._exit_reasons(s.get('schedule_exit') or {}))
        widgets.append(u.Divider())

        # --- Job state counters ---------------------------------------
        widgets.append(SectionHeader('JOB STATE COUNTERS (since boot)'))
        cells = [
            ('pending',   s.get('jobs_pending', 0),   'warning'),
            ('running',   s.get('jobs_running', 0),   'success'),
            ('started',   s.get('jobs_started', 0),   None),
            ('completed', s.get('jobs_completed', 0), None),
            ('canceled',  s.get('jobs_canceled', 0),  'faded'),
            ('failed',    s.get('jobs_failed', 0),
                'error' if s.get('jobs_failed', 0) else 'faded'),
            ('submitted', s.get('jobs_submitted', 0), None),
        ]
        markup = ['    ']
        for label, value, attr in cells:
            markup.append((attr, f'{value:>6}') if attr else f'{value:>6}')
            markup.append(('faded', f' {label:<10}'))
        widgets.append(u.Text(markup, wrap='clip'))
        widgets.append(u.Divider())

        # --- Slowest hot RPCs -----------------------------------------
        rpcs = [r for r in s.get('rpcs_by_message_type', []) if r.get('count', 0) > 10000]
        rpcs.sort(key=lambda r: -_num(r.get('average_time')))
        if rpcs:
            widgets.append(SectionHeader('SLOWEST HOT RPCs (avg time, count > 10k)'))
            widgets.append(u.Text([
                '    ', ('faded', f'{"message_type":<35} {"count":>10}  {"avg":>10}'),
            ], wrap='clip'))
            for r in rpcs[:8]:
                widgets.append(self._rpc_msg_row(
                    r.get('message_type', '?'), r.get('count', 0),
                    _num(r.get('average_time'))))

        # --- Footer note: fetch metadata ------------------------------
        dur = self.sdiag_fetcher.last_fetch_duration.total_seconds()
        widgets.append(u.Divider())
        widgets.append(u.Text(('faded', f'  sdiag fetch: {dur*1000:.0f}ms'), wrap='clip'))

        self.walker.clear()
        self.walker.extend(widgets)

        if len(self.walker) > 0:
            if old_focus is not None and old_focus < len(self.walker):
                self.walker.set_focus(old_focus)
            else:
                self.walker.set_focus(0)
