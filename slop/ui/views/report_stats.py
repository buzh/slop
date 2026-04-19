"""Statistics calculation and rendering for the user report view.

Pure functions: `calculate_user_stats(jobs)` returns a dict; `build_stats_widgets(stats)`
turns that dict into urwid widgets. No view state involved.
"""
import urwid as u

from slop.ui.constants import EMPTY_PLACEHOLDER


def calculate_user_stats(jobs):
    """Aggregate sacct job records into summary stats.

    Returns None if the job list is empty.
    """
    if not jobs:
        return None

    stats = {
        'total': len(jobs),
        'completed': 0,
        'failed': 0,
        'cancelled': 0,
        'timeout': 0,
        'oom': 0,
        'cpu_efficiencies': [],
        'time_efficiencies': [],
        'total_cpu_hours_alloc': 0,
        'total_cpu_hours_used': 0,
        'jobs_hit_time_limit': 0,
        'jobs_low_time_use': 0,
        'most_used_partition': {},
        'most_used_account': {},
        'failed_reasons': {},
    }

    for job in jobs:
        states = set(job.job_state) if isinstance(job.job_state, list) else {job.job_state}

        if 'COMPLETED' in states:
            stats['completed'] += 1
        elif 'FAILED' in states:
            stats['failed'] += 1
            reason = getattr(job, 'state_reason', EMPTY_PLACEHOLDER)
            stats['failed_reasons'][reason] = stats['failed_reasons'].get(reason, 0) + 1
        elif 'CANCELLED' in states:
            stats['cancelled'] += 1
        elif 'TIMEOUT' in states:
            stats['timeout'] += 1
        elif 'OUT_OF_MEMORY' in states:
            stats['oom'] += 1

        if hasattr(job, 'partition'):
            stats['most_used_partition'][job.partition] = stats['most_used_partition'].get(job.partition, 0) + 1
        if hasattr(job, 'account'):
            stats['most_used_account'][job.account] = stats['most_used_account'].get(job.account, 0) + 1

        if 'COMPLETED' in states:
            _accumulate_efficiency(job, stats)

    return stats


def _accumulate_efficiency(job, stats):
    """Add this completed job's CPU/time efficiency contributions to `stats`."""
    time_obj = getattr(job, 'time', {})
    if not isinstance(time_obj, dict):
        return
    elapsed_sec = time_obj.get('elapsed', 0)

    total_cpu_time = time_obj.get('total', {})
    if isinstance(total_cpu_time, dict):
        cpu_sec_used = total_cpu_time.get('seconds', 0) + total_cpu_time.get('microseconds', 0) / 1000000
    else:
        cpu_sec_used = 0

    cpus = 0
    if hasattr(job, 'tres') and isinstance(job.tres, dict):
        for tres_item in job.tres.get('allocated', []):
            if isinstance(tres_item, dict) and tres_item.get('type') == 'cpu':
                cpus = tres_item.get('count', 0)
                break

    if cpus == 0:
        cpus_obj = getattr(job, 'cpus', {})
        if isinstance(cpus_obj, dict):
            cpus = cpus_obj.get('number', 0)
        else:
            cpus = cpus_obj if isinstance(cpus_obj, int) else 0

    if cpus <= 0 or elapsed_sec <= 0:
        return

    cpu_hours_alloc = (cpus * elapsed_sec) / 3600
    stats['total_cpu_hours_alloc'] += cpu_hours_alloc

    if cpu_sec_used > 0:
        stats['total_cpu_hours_used'] += cpu_sec_used / 3600
        stats['cpu_efficiencies'].append((cpu_sec_used / (cpus * elapsed_sec)) * 100)

    time_limit_obj = time_obj.get('limit', {})
    if isinstance(time_limit_obj, dict) and time_limit_obj.get('set') and not time_limit_obj.get('infinite'):
        limit_min = time_limit_obj.get('number', 0)
        if limit_min > 0:
            elapsed_min = elapsed_sec / 60
            time_eff = (elapsed_min / limit_min) * 100
            stats['time_efficiencies'].append(time_eff)
            if time_eff > 95:
                stats['jobs_hit_time_limit'] += 1
            elif time_eff < 20:
                stats['jobs_low_time_use'] += 1


def build_stats_widgets(stats):
    """Render a stats dict into a list of urwid widgets for the report view."""
    if not stats:
        return [u.Text(("faded", "  No statistics available"))]

    widgets = []
    total = stats['total']

    widgets.append(u.Text(f"Total Jobs:  {total}"))
    if stats['completed'] > 0:
        pct = stats['completed'] * 100 // total
        widgets.append(u.Text(f"Completed:   {stats['completed']} ({pct}%)"))
    if stats['failed'] > 0:
        pct = stats['failed'] * 100 // total
        widgets.append(u.AttrMap(u.Text(f"Failed:      {stats['failed']} ({pct}%)"), 'error'))
    if stats['timeout'] > 0:
        widgets.append(u.AttrMap(u.Text(f"  Timeout:   {stats['timeout']}"), 'warning'))
    if stats['oom'] > 0:
        widgets.append(u.AttrMap(u.Text(f"  OOM:       {stats['oom']}"), 'warning'))
    if stats['cancelled'] > 0:
        pct = stats['cancelled'] * 100 // total
        widgets.append(u.Text(f"Cancelled:   {stats['cancelled']} ({pct}%)"))

    widgets.append(u.Divider())

    if stats['cpu_efficiencies']:
        avg_cpu_eff = sum(stats['cpu_efficiencies']) / len(stats['cpu_efficiencies'])
        if avg_cpu_eff >= 70:
            widgets.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_cpu_eff:.1f}%"), 'success'))
        elif avg_cpu_eff >= 40:
            widgets.append(u.Text(f"Avg CPU Eff: {avg_cpu_eff:.1f}%"))
        else:
            widgets.append(u.AttrMap(u.Text(f"Avg CPU Eff: {avg_cpu_eff:.1f}%"), 'warning'))
            widgets.append(u.Text(("faded", "  💡 Consider requesting fewer cores")))

    if stats['total_cpu_hours_alloc'] > 0:
        wasted_cpu_hours = stats['total_cpu_hours_alloc'] - stats['total_cpu_hours_used']
        if stats['total_cpu_hours_used'] > 0:
            widgets.append(u.Text(f"CPU Hours:   {stats['total_cpu_hours_alloc']:.1f}h alloc, {stats['total_cpu_hours_used']:.1f}h used"))
            if wasted_cpu_hours > 0:
                waste_pct = (wasted_cpu_hours / stats['total_cpu_hours_alloc']) * 100
                if waste_pct > 30:
                    widgets.append(u.AttrMap(u.Text(f"  Wasted:    {wasted_cpu_hours:.1f}h ({waste_pct:.0f}%)"), 'warning'))
                else:
                    widgets.append(u.Text(f"  Wasted:    {wasted_cpu_hours:.1f}h ({waste_pct:.0f}%)"))
        else:
            widgets.append(u.Text(f"CPU Hours:   {stats['total_cpu_hours_alloc']:.1f}h allocated"))

    if stats['time_efficiencies']:
        avg_time_eff = sum(stats['time_efficiencies']) / len(stats['time_efficiencies'])
        if avg_time_eff >= 60:
            widgets.append(u.AttrMap(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"), 'success'))
        elif avg_time_eff < 30:
            widgets.append(u.AttrMap(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"), 'warning'))
            widgets.append(u.Text(("faded", "  💡 Consider shorter time limits")))
        else:
            widgets.append(u.Text(f"Avg Time Use: {avg_time_eff:.0f}%"))

    if stats['jobs_hit_time_limit'] > 0 and stats['completed'] > 0:
        pct = (stats['jobs_hit_time_limit'] / stats['completed']) * 100
        if pct > 20:
            widgets.append(u.AttrMap(u.Text(f"⚠ {stats['jobs_hit_time_limit']} jobs hit time limit ({pct:.0f}%)"), 'warning'))

    if stats['jobs_low_time_use'] > 0 and stats['completed'] > 0:
        pct = (stats['jobs_low_time_use'] / stats['completed']) * 100
        if pct > 30:
            widgets.append(u.Text(("info", f"💡 {stats['jobs_low_time_use']} jobs used <20% of time ({pct:.0f}%)")))

    if stats['most_used_partition']:
        top_partition = max(stats['most_used_partition'].items(), key=lambda x: x[1])
        widgets.append(u.Divider())
        widgets.append(u.Text(f"Top Partition: {top_partition[0]} ({top_partition[1]} jobs)"))

    if stats['failed_reasons']:
        widgets.append(u.Divider())
        widgets.append(u.Text(("error", "Failed Job Reasons:")))
        sorted_reasons = sorted(stats['failed_reasons'].items(), key=lambda x: x[1], reverse=True)[:3]
        for reason, count in sorted_reasons:
            widgets.append(u.Text(f"  {reason}: {count}"))

    return widgets
