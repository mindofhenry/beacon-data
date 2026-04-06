"""
generators/narrative_arcs.py
Defines rep performance curves, sequence event dates, and account story beats.

Each arc modifies a rep's baseline metrics over time to create realistic
longitudinal patterns (ramp-ups, plateaus, PIP recoveries, etc.).

Arc modifiers are centralized here. Individual generators import and apply them.
"""

import random
from .config import DATA_START, GLOBAL_SEED


# -- Arc Event Constants -----------------------------------------------------

DEGRADING_SEQUENCE_ID = 1001   # the sequence that degrades and gets rewritten
REWRITE_STEP_ORDER = 3         # which step gets rewritten
REWRITE_WEEK = 26              # when Jordan rewrites it (October 2025)
PIP_START_WEEK = 22            # when Samantha's PIP begins (September 2025)
NEW_HIRE_WEEK = 23             # when Aisha starts (September 2025)

OLD_VERSION_MULTIPLIER = 0.48  # reply rate ~1.4% for reps still on old step 3


# -- Helpers -----------------------------------------------------------------

def date_to_week_index(dt):
    """Convert a date to its week index (0-51) relative to DATA_START."""
    delta = (dt - DATA_START).days
    return min(max(delta // 7, 0), 51)


def _deterministic_noise(rep_id, week_index, amplitude):
    """Return deterministic noise in [-amplitude, +amplitude]."""
    seed = GLOBAL_SEED + hash(rep_id) + week_index * 997
    rng = random.Random(seed)
    return rng.uniform(-amplitude, amplitude)


def _lerp(week_index, start_week, end_week, start_val, end_val):
    """Linear interpolation between two values over a week range."""
    t = (week_index - start_week) / (end_week - start_week)
    t = max(0.0, min(1.0, t))
    return start_val + t * (end_val - start_val)


# -- SDR Performance Curves --------------------------------------------------
# Each returns a float multiplier for the given week_index.
# 1.0 = team average. Below = underperforming. Above = outperforming.

def _curve_plateau_rep(week_index, rep_id="sdr_1"):
    """sdr_1 (Marcus Webb): flat at 0.75, noise +/-0.03."""
    return 0.75 + _deterministic_noise(rep_id, week_index, 0.03)


def _curve_top_performer_ramp(week_index, rep_id="sdr_2"):
    """sdr_2 (Priya Nair): 1.0 -> ramp to 1.8 -> 2.2."""
    if week_index <= 12:
        base = 1.0
        noise = _deterministic_noise(rep_id, week_index, 0.05)
    elif week_index <= 25:
        base = _lerp(week_index, 13, 25, 1.0, 1.8)
        noise = _deterministic_noise(rep_id, week_index, 0.08)
    else:
        base = 2.2
        noise = _deterministic_noise(rep_id, week_index, 0.15)
    return base + noise


def _curve_rewrite_adopter_default(week_index, rep_id="sdr_3"):
    """sdr_3 (Jordan Chase): default behavior — flat 1.0, noise +/-0.05."""
    return 1.0 + _deterministic_noise(rep_id, week_index, 0.05)


def _curve_rewrite_adopter_step3(week_index, rep_id="sdr_3"):
    """sdr_3 (Jordan Chase): on degrading sequence step 3.
    Stable -> decay -> rewrite recovery."""
    if week_index <= 17:
        base = 1.0
        noise = _deterministic_noise(rep_id, week_index, 0.05)
    elif week_index <= 25:
        base = _lerp(week_index, 18, 25, 1.0, 0.50)
        noise = _deterministic_noise(rep_id, week_index, 0.03)
    else:
        base = 2.1
        noise = _deterministic_noise(rep_id, week_index, 0.10)
    return base + noise


def _curve_pip_recovery(week_index, rep_id="sdr_4"):
    """sdr_4 (Samantha Reyes): underperform -> decay -> PIP recovery."""
    if week_index <= 12:
        base = 0.55
        noise = _deterministic_noise(rep_id, week_index, 0.05)
    elif week_index <= 21:
        base = _lerp(week_index, 13, 21, 0.55, 0.28)
        noise = _deterministic_noise(rep_id, week_index, 0.03)
    elif week_index <= 34:
        base = _lerp(week_index, 22, 34, 0.28, 0.95)
        noise = _deterministic_noise(rep_id, week_index, 0.04)
    else:
        base = 0.98
        noise = _deterministic_noise(rep_id, week_index, 0.05)
    return base + noise


def _curve_steady_performer(week_index, rep_id="sdr_5"):
    """sdr_5 (Tyler Brooks): flat 1.0, noise +/-0.05."""
    return 1.0 + _deterministic_noise(rep_id, week_index, 0.05)


def _curve_new_hire_ramp(week_index, rep_id="sdr_6"):
    """sdr_6 (Aisha Okonkwo): not hired until week 23, then ramp."""
    if week_index < NEW_HIRE_WEEK:
        return 0.0
    elif week_index <= 35:
        base = _lerp(week_index, 23, 35, 0.30, 0.85)
        noise = _deterministic_noise(rep_id, week_index, 0.03)
        return base + noise
    else:
        return 0.90 + _deterministic_noise(rep_id, week_index, 0.05)


# -- Curve Dispatch -----------------------------------------------------------

_SDR_DEFAULT_CURVES = {
    "sdr_1": _curve_plateau_rep,
    "sdr_2": _curve_top_performer_ramp,
    "sdr_3": _curve_rewrite_adopter_default,
    "sdr_4": _curve_pip_recovery,
    "sdr_5": _curve_steady_performer,
    "sdr_6": _curve_new_hire_ramp,
}

# SDRs that stay on the OLD version of step 3 after the rewrite
_OLD_VERSION_SDRS = {"sdr_1", "sdr_2", "sdr_4", "sdr_5"}


# -- Public API ---------------------------------------------------------------

def get_rep_multiplier(rep_id, week_index, step_order=None, sequence_id=None):
    """Return the performance multiplier for a rep at a given week.

    Args:
        rep_id: e.g. "sdr_1", "ae_3", "mgr_2"
        week_index: 0-51 (week 0 = DATA_START)
        step_order: optional step number in a sequence
        sequence_id: optional sequence identifier

    Returns:
        float multiplier (1.0 = team average)
    """
    # AEs and managers: passthrough (Phase 3 will add AE curves)
    if not rep_id.startswith("sdr_"):
        return 1.0

    is_rewrite_step = (
        sequence_id == DEGRADING_SEQUENCE_ID
        and step_order == REWRITE_STEP_ORDER
    )

    # sdr_3 on the rewrite step: uses special curve (stable -> decay -> recovery)
    if rep_id == "sdr_3" and is_rewrite_step:
        return _curve_rewrite_adopter_step3(week_index, rep_id)

    # Other SDRs on the rewrite step after the rewrite week
    if is_rewrite_step and week_index >= REWRITE_WEEK:
        # sdr_6 uses new version (her normal curve)
        if rep_id == "sdr_6":
            return _curve_new_hire_ramp(week_index, rep_id)
        # sdr_1, sdr_2, sdr_4, sdr_5 stuck on old version
        if rep_id in _OLD_VERSION_SDRS:
            return OLD_VERSION_MULTIPLIER + _deterministic_noise(
                rep_id, week_index, 0.03
            )

    # Default: use the rep's standard curve
    curve_fn = _SDR_DEFAULT_CURVES.get(rep_id)
    if curve_fn is None:
        return 1.0
    return curve_fn(week_index, rep_id)


# -- Validation ---------------------------------------------------------------

if __name__ == "__main__":
    CHECKPOINT_WEEKS = [0, 12, 18, 22, 23, 26, 35, 51]
    WEEK_LABELS = [
        "Wk0 Apr", "Wk12 Jun", "Wk18 Aug", "Wk22 Sep",
        "Wk23 Sep", "Wk26 Oct", "Wk35 Dec", "Wk51 Mar",
    ]
    SDR_IDS = ["sdr_1", "sdr_2", "sdr_3", "sdr_4", "sdr_5", "sdr_6"]

    header = f"{'Rep':<8} {'Context':<16}" + "".join(
        f"{lbl:>10}" for lbl in WEEK_LABELS
    )
    sep = "-" * len(header)

    print("Phase 2A Validation: SDR Performance Multipliers")
    print(sep)
    print(header)
    print(sep)

    for sdr_id in SDR_IDS:
        # Row 1: default context (no sequence/step)
        vals_default = [
            get_rep_multiplier(sdr_id, w) for w in CHECKPOINT_WEEKS
        ]
        row = f"{sdr_id:<8} {'default':<16}"
        row += "".join(f"{v:>10.3f}" for v in vals_default)
        print(row)

        # Row 2: rewrite step context (step 3, seq 1001)
        vals_rewrite = [
            get_rep_multiplier(sdr_id, w, step_order=3, sequence_id=1001)
            for w in CHECKPOINT_WEEKS
        ]
        row = f"{'':<8} {'step3 seq1001':<16}"
        row += "".join(f"{v:>10.3f}" for v in vals_rewrite)
        print(row)
        print()

    print(sep)
    print("Multiplier 1.0 = team average. 0.0 = not hired / no activity.")
    print(f"Rewrite happens at week {REWRITE_WEEK}. PIP starts week {PIP_START_WEEK}.")
    print(f"New hire (sdr_6) starts week {NEW_HIRE_WEEK}.")
