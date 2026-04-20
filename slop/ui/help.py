"""Help-overlay text. Pure data — kept out of `app.py` to keep that file lean."""
from slop import __version__


VIEW_NAMES = {
    0: "My Jobs",
    1: "Users",
    2: "Accounts",
    3: "Partitions",
    4: "States",
    5: "Cluster",
    6: "History",
    7: "Queue",
    8: "Scheduler",
}


def build_help_text(current_view, fetch_duration):
    """Return the urwid markup list shown in the help overlay."""
    current_view_name = VIEW_NAMES.get(current_view, "Unknown")
    return [
        ("jobheader", f"  Slurm Top (slop) {__version__} - Keyboard Shortcuts  "),
        "",
        ("success", "NAVIGATION (F-Keys)"),
        "  F1       My Jobs / All Users (toggle)",
        "  F2       Accounts view",
        "  F3       Partitions view",
        "  F4       Job States view",
        "  F5       Cluster Resources",
        "  F6       History / User Report",
        "  F7       Queue Status",
        "  F8       Scheduler Health (sdiag)",
        "",
        ("success", "COMMON ACTIONS"),
        "  /        Search (job ID, user, account, or node)",
        "  ?        Show this help",
        "  q        Quit (with confirmation)",
        "  Esc      Close overlay / Go back",
        "",
        ("success", "MY JOBS / USERS / ACCOUNTS / PARTITIONS / STATES"),
        "  h        View history for selected user (Users view only)",
        "  e        Expand/collapse job groups",
        "  Enter    Show job details",
        "  Space    Expand array job",
        "  Tab      Switch between left/right panels",
        "  0-6      Sort by column (0=state, 1=id, 2=start, etc.)",
        "",
        ("success", "HISTORY VIEW (F6)"),
        "  Enter    Show job details",
        "  Tab      Autocomplete usernames",
        "  Esc      Return to previous view",
        "",
        ("success", "QUEUE VIEW (F7)"),
        "  e        Expand/collapse grouped jobs",
        "  Enter    Show job details or expand group",
        "",
        ("success", "SEARCH (/)"),
        "  Tab      Autocomplete suggestions",
        "  Enter    Execute search",
        "",
        ("success", "CLUSTER VIEW (F5)"),
        "  Arrows   Scroll through GPU/node info",
        "",
        ("faded", "─" * 60),
        ("faded", f"Current view: {current_view_name}"),
        ("faded", "License: GNU GPL v3.0 | © 2025 Andreas Skau"),
        ("faded", "GitHub: https://github.com/buzh/slop"),
        ("faded", f"scontrol fetch time: {fetch_duration:.1f}s"),
    ]
