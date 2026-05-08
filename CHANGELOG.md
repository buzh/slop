# Changelog

## May 8th, 2026: v1.1.9

**Backoff strategy.** New load governor reads `sdiag` and tiers refresh cadence (NORMAL/SLOW/BACKOFF/HALTED) so slop eases off the controller under pressure and resumes when signals clear. After 30 min of sustained pressure with no user activity the session halts and waits for an explicit acknowledgement.

- Improved queue screen (F7): renamed pending section, moved summary footer to where it belongs, and reserved a row so it can't slip off-screen.
- Fixed bug where hitting `q` (or other hotkeys) multiple times would stack duplicate overlays.

## April 30th, 2026: v1.1.8

Adds a new dashboard as a landing screen, and includes numerous minor improvements.

## April 29th, 2026: v1.1.7a

Fixes loop when a user has no job history.

## April 29th, 2026: v1.1.7

Numerous minor bug fixes, as well as a code overhaul that should improve TUI performance by handing off more of the legwork to Urwid itself.

## April 21st, 2026: v1.1.6

Introduces a brand new "job flow" screen which gives a live view of how jobs are starting and ending. It shows an ETA for the highest priority pending jobs, which jobs have just started, which are about to end, and the most recent jobs that did end.

Also adds a new screen that displays statistics from the scheduler and backfiller, along with a view of the pending jobs per partition. These two are still work in progress, so consider them a preview.
