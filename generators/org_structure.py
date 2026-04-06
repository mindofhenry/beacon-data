"""
generators/org_structure.py
Defines all 19 reps + 3 managers, SDR-to-AE pairings, and rep lifecycle events.
"""

import json
import os

# ── Managers ────────────────────────────────────────────────────────────────

MANAGERS = [
    {
        "id": "mgr_1",
        "name": "Christine Park",
        "role": "Manager",
        "segment": "All",
        "manager_id": None,
        "hired_date": "2023-11-01",
        "narrative_arc": None,
    },
    {
        "id": "mgr_2",
        "name": "James Holloway",
        "role": "Manager",
        "segment": "SMB",
        "manager_id": None,
        "hired_date": "2023-09-15",
        "narrative_arc": None,
    },
    {
        "id": "mgr_3",
        "name": "Amara Okafor",
        "role": "Manager",
        "segment": "ENT",
        "manager_id": None,
        "hired_date": "2023-10-01",
        "narrative_arc": None,
    },
]

# ── SDR Team (reports to mgr_1) ────────────────────────────────────────────

SDRS = [
    {
        "id": "sdr_1",
        "name": "Marcus Webb",
        "role": "SDR",
        "segment": "All",
        "manager_id": "mgr_1",
        "hired_date": "2024-06-15",
        "narrative_arc": "plateau_rep",
    },
    {
        "id": "sdr_2",
        "name": "Priya Nair",
        "role": "SDR",
        "segment": "All",
        "manager_id": "mgr_1",
        "hired_date": "2024-08-01",
        "narrative_arc": "top_performer_ramp",
    },
    {
        "id": "sdr_3",
        "name": "Jordan Chase",
        "role": "SDR",
        "segment": "All",
        "manager_id": "mgr_1",
        "hired_date": "2024-07-10",
        "narrative_arc": "rewrite_adopter",
    },
    {
        "id": "sdr_4",
        "name": "Samantha Reyes",
        "role": "SDR",
        "segment": "All",
        "manager_id": "mgr_1",
        "hired_date": "2024-05-20",
        "narrative_arc": "pip_recovery",
    },
    {
        "id": "sdr_5",
        "name": "Tyler Brooks",
        "role": "SDR",
        "segment": "All",
        "manager_id": "mgr_1",
        "hired_date": "2024-09-01",
        "narrative_arc": "steady_performer",
    },
    {
        "id": "sdr_6",
        "name": "Aisha Okonkwo",
        "role": "SDR",
        "segment": "All",
        "manager_id": "mgr_1",
        "hired_date": "2025-09-08",
        "narrative_arc": "new_hire_ramp",
    },
]

# ── AE Team SMB/MM (reports to mgr_2) ──────────────────────────────────────

AES_SMB_MM = [
    {
        "id": "ae_1",
        "name": "David Kowalski",
        "role": "AE",
        "segment": "SMB",
        "manager_id": "mgr_2",
        "hired_date": "2024-03-15",
        "narrative_arc": "multi_threader",
    },
    {
        "id": "ae_2",
        "name": "Elena Vasquez",
        "role": "AE",
        "segment": "SMB",
        "manager_id": "mgr_2",
        "hired_date": "2024-06-01",
        "narrative_arc": "sandbagging_forecaster",
    },
    {
        "id": "ae_3",
        "name": "Ryan Obi",
        "role": "AE",
        "segment": "SMB",
        "manager_id": "mgr_2",
        "hired_date": "2024-04-20",
        "narrative_arc": "steady_performer",
    },
    {
        "id": "ae_4",
        "name": "Hannah Liu",
        "role": "AE",
        "segment": "SMB",
        "manager_id": "mgr_2",
        "hired_date": "2024-07-15",
        "narrative_arc": "steady_performer",
    },
    {
        "id": "ae_5",
        "name": "Nate Johansson",
        "role": "AE",
        "segment": "MM",
        "manager_id": "mgr_2",
        "hired_date": "2024-05-10",
        "narrative_arc": "deal_stalls_dies",
    },
    {
        "id": "ae_6",
        "name": "Keiko Tanaka",
        "role": "AE",
        "segment": "MM",
        "manager_id": "mgr_2",
        "hired_date": "2024-08-20",
        "narrative_arc": "competitive_displacement",
    },
]

# ── AE Team ENT/STRAT (reports to mgr_3) ───────────────────────────────────

AES_ENT_STRAT = [
    {
        "id": "ae_7",
        "name": "Marcus Adeyemi",
        "role": "AE",
        "segment": "ENT",
        "manager_id": "mgr_3",
        "hired_date": "2024-02-01",
        "narrative_arc": "cross_segment_divergence",
    },
    {
        "id": "ae_8",
        "name": "Laura Chen",
        "role": "AE",
        "segment": "ENT",
        "manager_id": "mgr_3",
        "hired_date": "2024-04-15",
        "narrative_arc": "pipeline_lifecycle",
    },
    {
        "id": "ae_9",
        "name": "Daniel Osei",
        "role": "AE",
        "segment": "STRAT",
        "manager_id": "mgr_3",
        "hired_date": "2024-01-10",
        "narrative_arc": "multi_threaded_close",
    },
    {
        "id": "ae_10",
        "name": "Sofia Petrov",
        "role": "AE",
        "segment": "STRAT",
        "manager_id": "mgr_3",
        "hired_date": "2024-06-20",
        "narrative_arc": "steady_performer",
    },
]

# ── Combined roster ─────────────────────────────────────────────────────────

ALL_REPS = SDRS + AES_SMB_MM + AES_ENT_STRAT + MANAGERS

# ── SDR-to-AE Pairings ─────────────────────────────────────────────────────

SDR_AE_PAIRS = [
    {"sdr_id": "sdr_1", "ae_id": "ae_1"},
    {"sdr_id": "sdr_2", "ae_id": "ae_5"},
    {"sdr_id": "sdr_3", "ae_id": "ae_6"},
    {"sdr_id": "sdr_4", "ae_id": "ae_3"},
    {"sdr_id": "sdr_5", "ae_id": "ae_7"},
    {"sdr_id": "sdr_6", "ae_id": "ae_9"},
]

# ── Rep Lifecycle Events ───────────────────────────────────────────────────

REP_EVENTS = [
    {
        "rep_id": "sdr_6",
        "event_type": "hire",
        "event_date": "2025-09-08",
        "details": "New SDR hire, reports to mgr_1",
    },
    {
        "rep_id": "sdr_4",
        "event_type": "performance_intervention",
        "event_date": "2025-09-01",
        "details": "PIP initiated for underperformance",
    },
]


def generate_org_files(output_dir):
    """Write org structure files to output_dir."""
    os.makedirs(output_dir, exist_ok=True)

    reps_path = os.path.join(output_dir, "reps.json")
    with open(reps_path, "w") as f:
        json.dump(ALL_REPS, f, indent=2)

    pairs_path = os.path.join(output_dir, "rep_pairs.json")
    with open(pairs_path, "w") as f:
        json.dump(SDR_AE_PAIRS, f, indent=2)

    events_path = os.path.join(output_dir, "rep_events.json")
    with open(events_path, "w") as f:
        json.dump(REP_EVENTS, f, indent=2)

    return {
        "reps.json": len(ALL_REPS),
        "rep_pairs.json": len(SDR_AE_PAIRS),
        "rep_events.json": len(REP_EVENTS),
    }
