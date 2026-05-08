"""Load-aware refresh governor based on slurmctld diagnostics.

Reads `sdiag` signals to decide how aggressively `scontrol` should be polled.
Four tiers (NORMAL/SLOW/BACKOFF/HALTED) with sub-tier escalation inside BACKOFF.
"""
import time
from enum import Enum


NORMAL_CADENCE = 3
SLOW_CADENCE = 15
BACKOFF_1_CADENCE = 60
BACKOFF_2_CADENCE = 5 * 60
BACKOFF_3_CADENCE = 10 * 60
NORMAL_CLUSTER_CADENCE = 10
NORMAL_SDIAG_CADENCE = 30

BACKOFF_2_AFTER = 5 * 60
BACKOFF_3_AFTER = 15 * 60
HALTED_AFTER = 30 * 60

LATENCY_US_THRESHOLD = 1000
RJI_AVG_US_THRESHOLD = 100_000
PENDING_BACKOFF_THRESHOLD = 5
CLEAN_SAMPLES_TO_RECOVER = 2


class Tier(Enum):
    NORMAL = 'normal'
    SLOW = 'slow'
    BACKOFF = 'backoff'
    HALTED = 'halted'


class LoadGovernor:
    """Tracks scontrol refresh tier based on sdiag signals."""

    def __init__(self):
        self.tier = Tier.NORMAL
        self.last_dropped = None
        self.clean_samples = 0
        self.backoff_entered_at = None
        self.last_keypress_at = time.monotonic()

    def reset(self):
        """Fresh start — called after the user clicks OK on the HALTED modal."""
        self.tier = Tier.NORMAL
        self.last_dropped = None
        self.clean_samples = 0
        self.backoff_entered_at = None
        self.last_keypress_at = time.monotonic()

    def note_keypress(self):
        self.last_keypress_at = time.monotonic()

    def update_from_signals(self, signals):
        """Process a fresh sdiag sample. Returns True if the tier changed.

        No-op when HALTED — only `reset()` exits HALTED.
        """
        if self.tier == Tier.HALTED or signals is None:
            return False

        prev_tier = self.tier
        cur_dropped = signals['rji_dropped']
        dropped_inc = (
            self.last_dropped is not None and cur_dropped > self.last_dropped
        )
        self.last_dropped = cur_dropped

        pending = signals['pending_count']
        latency = signals['latency_us']
        queued = signals['rji_queued']
        avg_us = signals['rji_avg_us']

        if pending > PENDING_BACKOFF_THRESHOLD or dropped_inc:
            candidate = Tier.BACKOFF
        elif pending >= 1 or latency > LATENCY_US_THRESHOLD:
            candidate = Tier.SLOW
        elif (queued == 0 and avg_us < RJI_AVG_US_THRESHOLD):
            candidate = Tier.NORMAL
        else:
            candidate = Tier.SLOW

        if candidate == Tier.NORMAL and prev_tier == Tier.SLOW:
            self.clean_samples += 1
            if self.clean_samples < CLEAN_SAMPLES_TO_RECOVER:
                candidate = Tier.SLOW
        else:
            self.clean_samples = 0

        if candidate == Tier.BACKOFF and prev_tier != Tier.BACKOFF:
            self.backoff_entered_at = time.monotonic()
        elif candidate != Tier.BACKOFF:
            self.backoff_entered_at = None

        self.tier = candidate
        return self.tier != prev_tier

    def check_halted(self):
        """Time-based HALTED transition. Returns True iff just transitioned."""
        if self.tier != Tier.BACKOFF:
            return False
        backoff_age = self._time_in_backoff()
        idle_age = time.monotonic() - self.last_keypress_at
        if backoff_age >= HALTED_AFTER and idle_age >= HALTED_AFTER:
            self.tier = Tier.HALTED
            return True
        return False

    def _time_in_backoff(self):
        if self.backoff_entered_at is None:
            return 0.0
        return time.monotonic() - self.backoff_entered_at

    def jobs_cadence(self):
        if self.tier == Tier.NORMAL:
            return NORMAL_CADENCE
        if self.tier == Tier.SLOW:
            return SLOW_CADENCE
        if self.tier == Tier.BACKOFF:
            age = self._time_in_backoff()
            if age >= BACKOFF_3_AFTER:
                return BACKOFF_3_CADENCE
            if age >= BACKOFF_2_AFTER:
                return BACKOFF_2_CADENCE
            return BACKOFF_1_CADENCE
        return NORMAL_CADENCE

    def cluster_cadence(self):
        if self.tier == Tier.NORMAL:
            return NORMAL_CLUSTER_CADENCE
        return self.jobs_cadence()

    def sdiag_cadence(self):
        if self.tier in (Tier.NORMAL, Tier.SLOW):
            return NORMAL_SDIAG_CADENCE
        return self.jobs_cadence()

    def indicator(self):
        """(palette_attr, dot_char, label_text) for the header indicator."""
        if self.tier == Tier.HALTED:
            return ('header_err', '✕', '')
        if self.tier == Tier.NORMAL:
            attr = 'header_ok'
        elif self.tier == Tier.SLOW:
            attr = 'header_warn'
        else:
            attr = 'header_err'
        sec = self.jobs_cadence()
        if sec >= 60 and sec % 60 == 0:
            label = f'({sec // 60}m)'
        else:
            label = f'({sec}s)'
        return (attr, '●', label)


__all__ = ["LoadGovernor", "Tier"]
