"""Help-overlay text. Pure data — kept out of `app.py` to keep that file lean."""
from slop import __version__


VIEW_NAMES = {
    0: "Dashboard",
    1: "Jobs",
    2: "My Jobs",
    3: "Cluster",
    4: "History",
    5: "Queue",
    6: "Scheduler",
}


def build_help_text(current_view):
    """Return the urwid markup list shown in the help overlay."""
    current_view_name = VIEW_NAMES.get(current_view, "Unknown")
    return [
        ("jobheader", f"  Slurm Top (slop) {__version__} - Keyboard Shortcuts  "),
        "",
        ("success", "NAVIGATION (F-Keys)"),
        "  F1       Dashboard",
        "  F2       Jobs (user / account / partition / state)",
        "  F3       My Jobs",
        "  F5       Cluster Resources",
        "  F6       History / User Report",
        "  F7       Queue Status",
        "  F8       Scheduler Health (sdiag)",
        "",
        ("success", "COMMON ACTIONS"),
        "  /        Search (job ID, user, account, or node)",
        "  ?        Show this help",
        "  !        Show fetcher diagnostics (timings, errors)",
        "  q        Quit (with confirmation)",
        "  Esc      Close overlay / Go back",
        "",
        ("success", "JOBS VIEW (F2)"),
        "  Tab/⇧Tab Cycle between user / account / partition / state tabs",
        "  1-4      Jump directly to a tab",
        "  h        View history for selected user (Users tab only)",
        "  e        Expand/collapse job groups",
        "  Enter    Show job details",
        "  Space    Expand array job",
        "  0-6      Sort by column (0=state, 1=id, 2=start, ...)",
        "",
        ("success", "MY JOBS (F3)"),
        "  Enter    Show job details",
        "  Space    Expand array job",
        "",
        ("success", "HISTORY VIEW (F6)"),
        "  Enter    Show job details",
        "  Tab      Autocomplete usernames",
        "  Esc      Return to dashboard",
        "",
        ("success", "QUEUE VIEW (F7)"),
        "  Enter    Show job details",
        "",
        ("success", "JOB DETAILS OVERLAY"),
        "  h        Open history view for this job's user",
        "",
        ("success", "SCHEDULER VIEW (F8)"),
        "  e        Expand/collapse grouped jobs",
        "  Enter    Show job details or expand group",
        "",
        ("success", "SEARCH (/)"),
        "  Tab      Autocomplete suggestions",
        "  Enter    Execute search",
        "",
        ("success", "CLUSTER VIEW (F5)"),
        "  Arrows   Scroll through GPU/node info",
        "  e        Expand/compress node hostlists",
        "",
        ("faded", "─" * 60),
        ("faded", f"Current view: {current_view_name}"),
        ("faded", "License: GNU GPL v3.0 | © 2025 Andreas Skau"),
        ("faded", "GitHub: https://github.com/buzh/slop"),
    ]


def _fmt_duration(td):
    """Render a timedelta as 'NNN ms' / 'X.Xs', or '—' if zero/None."""
    if td is None:
        return "—"
    secs = td.total_seconds()
    if secs <= 0:
        return "—"
    if secs < 1:
        return f"{int(secs * 1000)} ms"
    return f"{secs:.1f}s"


def build_diagnostics_text(fetchers):
    """Return urwid markup for the diagnostics overlay.

    `fetchers` is a list of dicts with keys: name, command, fetcher.
    """
    lines = [
        ("jobheader", f"  slop {__version__} - Fetcher Diagnostics  "),
        "",
        ("success", "Last fetch timings, configured timeout, and most recent error per command."),
        "",
    ]
    for entry in fetchers:
        name = entry['name']
        cmd = entry['command']
        f = entry['fetcher']
        duration = _fmt_duration(getattr(f, 'last_fetch_duration', None))
        timeout = getattr(f, 'timeout', None)
        timeout_str = f"{timeout}s" if timeout is not None else "—"
        err = getattr(f, 'last_error', None)

        lines.append(("success", f"  {name}"))
        lines.append(("faded", f"    $ {cmd}"))
        lines.append(f"    last: {duration}    timeout: {timeout_str}")
        if err:
            lines.append(("error", f"    error: {err}"))
        lines.append("")
    return lines
