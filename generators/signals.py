"""
generators/signals.py
Phase 4: Signal layer — generates signal_events, score_history,
tribal_patterns, and account_preferences.

Signal events are account-level and pre-scored (weight_applied at generation time).
Score history is computed from signal events (trailing 30-day sum, capped at 100).
"""

import csv
import json
import os
import random
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import date, timedelta

from .config import GLOBAL_SEED, DATA_START, DATA_END, DEMO_TODAY, OUTPUT_DIR
from .org_structure import SDRS, AES_SMB_MM, AES_ENT_STRAT

_rng = random.Random(GLOBAL_SEED + 700)

# ── Signal type definitions ─────────────────────────────────────────────────

SIGNAL_DEFS = {
    "pricing_page_visit": {"weight": 15, "source": "web_analytics"},
    "job_change":          {"weight": 20, "source": "linkedin"},
    "intent_surge":        {"weight": 10, "source": "bombora"},
    "web_visit":           {"weight": 5,  "source": "web_analytics"},
    "competitor_mention":  {"weight": 12, "source": "g2_reviews"},
    "funding_event":       {"weight": 18, "source": "crunchbase"},
    "technology_install":  {"weight": 8,  "source": "builtwith"},
    "content_download":    {"weight": 10, "source": "marketing_automation"},
    "case_study_view":     {"weight": 12, "source": "web_analytics"},
    "executive_change":    {"weight": 15, "source": "linkedin"},
}

# Baseline signal type frequency weights (non-converting accounts)
_BASELINE_WEIGHTS = {
    "web_visit": 25, "intent_surge": 18, "content_download": 15,
    "pricing_page_visit": 8, "competitor_mention": 8, "technology_install": 7,
    "case_study_view": 6, "job_change": 5, "funding_event": 4,
    "executive_change": 4,
}

# Closed-won biased weights (Arc 15: more job_change, pricing_page_visit)
_CLOSED_WON_WEIGHTS = {
    "web_visit": 16, "intent_surge": 8, "content_download": 12,
    "pricing_page_visit": 15, "competitor_mention": 8, "technology_install": 5,
    "case_study_view": 7, "job_change": 18, "funding_event": 5,
    "executive_change": 6,
}

# ── Reason text templates ───────────────────────────────────────────────────

_TOPICS = [
    "endpoint security", "cloud security", "zero trust", "SIEM",
    "compliance automation", "threat detection", "vulnerability management",
    "data protection", "identity governance", "network segmentation",
]
_TECHS = ["Okta", "CrowdStrike", "Datadog", "PagerDuty", "Snowflake", "Splunk"]

_REASON_TEMPLATES = {
    "pricing_page_visit": [
        "Pricing page viewed {n}x this week — above 2x threshold",
        "Multiple pricing page visits detected in 7-day window",
        "Pricing comparison page accessed — indicates active evaluation",
        "Pricing page engagement spike: {n} visits in 5 days",
    ],
    "job_change": [
        "Key contact started new role — LinkedIn update detected",
        "New hire detected at target company — potential champion entry",
        "Executive transition detected — leadership change at account",
        "VP-level hire announced — potential expansion opportunity",
    ],
    "intent_surge": [
        "Bombora intent surge: {topic} keywords trending above baseline",
        "Intent data spike: 3x normal search volume for security solutions",
        "Elevated research activity detected across security topics",
        "G2 category research surge — comparing security platforms",
    ],
    "web_visit": [
        "Website visit from target account — {n} pages viewed",
        "Blog post engagement: security best practices article",
        "Resource center browsed — {n} assets viewed this week",
        "Website session detected — returning visitor pattern",
    ],
    "competitor_mention": [
        "G2 review comparison: account evaluating competitive solutions",
        "Competitor mentioned in G2 search — active evaluation phase",
        "Review site activity: comparing security platform alternatives",
        "Competitive research detected on third-party review sites",
    ],
    "funding_event": [
        "Funding round announced — ${amount}M raised",
        "New funding detected — growth capital secured",
        "Investment round closed — expansion budget likely",
        "Capital raise announced — potential technology investment cycle",
    ],
    "technology_install": [
        "New technology detected: {tech} installed — complementary stack signal",
        "Tech stack change: added {tech} — indicates modernization initiative",
        "BuiltWith detected new security-adjacent technology adoption",
        "Infrastructure change detected — cloud migration indicators",
    ],
    "content_download": [
        "Whitepaper downloaded: Enterprise Security Posture Guide",
        "Content engagement: downloaded ROI calculator",
        "Gated content accessed: Threat Detection Best Practices",
        "Case study downloaded: Financial Services security transformation",
    ],
    "case_study_view": [
        "Case study viewed: {industry} customer success story",
        "Customer story engagement — viewed {industry} reference",
        "Case study page visited — 3+ minutes on page",
        "Reference story accessed — similar company profile match",
    ],
    "executive_change": [
        "New executive hire: VP of Security role filled",
        "C-suite change detected — new CISO appointed",
        "Executive leadership change — potential decision-maker shift",
        "Senior hire announced — VP of IT/Security role",
    ],
}


def _fmt_reason(signal_type, industry, rng):
    """Generate a reason text for a signal event."""
    template = rng.choice(_REASON_TEMPLATES[signal_type])
    return template.format(
        n=rng.randint(2, 6),
        topic=rng.choice(_TOPICS),
        amount=rng.choice([5, 10, 15, 20, 25, 30, 40, 50]),
        industry=industry or "technology",
        tech=rng.choice(_TECHS),
    )


# ── Date helpers ────────────────────────────────────────────────────────────

def _random_date(start, end, rng):
    """Random date between start and end inclusive."""
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=rng.randint(0, delta))


def _clustered_dates(n, start, end, rng, q34_bias=True):
    """Generate n dates with clustering and optional Q3-Q4 bias."""
    if n <= 0:
        return []

    n_clusters = rng.randint(max(1, n // 6), max(2, n // 3))
    n_clusters = min(n_clusters, n)

    centers = []
    for _ in range(n_clusters):
        if q34_bias and rng.random() < 0.55:
            # Bias toward July-December
            q34_start = max(start, date(2025, 7, 1))
            q34_end = min(end, date(2025, 12, 31))
            if q34_start < q34_end:
                centers.append(_random_date(q34_start, q34_end, rng))
            else:
                centers.append(_random_date(start, end, rng))
        else:
            centers.append(_random_date(start, end, rng))

    dates = []
    for i in range(n):
        center = centers[i % len(centers)]
        offset = rng.randint(-10, 10)
        d = center + timedelta(days=offset)
        d = max(start, min(end, d))
        dates.append(d)

    return sorted(dates)


def _pick_signal_type(is_closed_won, rng):
    """Pick a signal type weighted by account conversion status (Arc 15)."""
    weights = _CLOSED_WON_WEIGHTS if is_closed_won else _BASELINE_WEIGHTS
    types = list(weights.keys())
    wts = [weights[t] for t in types]
    return rng.choices(types, weights=wts, k=1)[0]


def _make_event(acc_id, sig_type, sig_date, industry, rng, metadata=None):
    """Build a single signal event dict (without id — assigned later)."""
    return {
        "account_id": acc_id,
        "signal_type": sig_type,
        "signal_date": sig_date if isinstance(sig_date, str) else sig_date.isoformat(),
        "weight_applied": SIGNAL_DEFS[sig_type]["weight"],
        "reason_text": _fmt_reason(sig_type, industry, rng),
        "source": SIGNAL_DEFS[sig_type]["source"],
        "metadata": metadata or {},
    }


# ── Data loading ────────────────────────────────────────────────────────────

def _load_data(output_dir):
    """Load Phase 1-3 output files needed for signal generation."""

    with open(os.path.join(output_dir, "account_enrichment.json"), encoding="utf-8") as f:
        enrichment = json.load(f)

    enrich_by_acc = {}
    for rec in enrichment:
        acc_id = f"sf_acc_{rec['company_index'] + 1:03d}"
        enrich_by_acc[acc_id] = rec

    opps = []
    with open(os.path.join(output_dir, "sf_opportunities.csv"), encoding="utf-8") as f:
        for row in csv.DictReader(f):
            opps.append(row)

    closed_won_accounts = set()
    closed_won_opps = defaultdict(list)
    for opp in opps:
        if opp["StageName"] == "Closed Won":
            closed_won_accounts.add(opp["AccountId"])
            closed_won_opps[opp["AccountId"]].append(opp)

    contacts = []
    with open(os.path.join(output_dir, "sf_contacts.csv"), encoding="utf-8") as f:
        for row in csv.DictReader(f):
            contacts.append(row)

    ciso_accounts = set()
    for c in contacts:
        if "CISO" in c.get("Title", ""):
            ciso_accounts.add(c["AccountId"])

    return {
        "enrichment": enrichment,
        "enrich_by_acc": enrich_by_acc,
        "opps": opps,
        "closed_won_accounts": closed_won_accounts,
        "closed_won_opps": closed_won_opps,
        "ciso_accounts": ciso_accounts,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ARC 5 — Fortress Security Corp re-engagement
# ══════════════════════════════════════════════════════════════════════════════

def _generate_arc5_signals(rng):
    """Fortress (sf_acc_001): active Apr-Jun, dark Jul-Aug, trickle Sep, cluster Oct."""
    acc_id = "sf_acc_001"
    ind = "cybersecurity"
    events = []

    # April-June: Active engagement (15-20 signals)
    active_types = [
        "web_visit", "content_download", "pricing_page_visit",
        "intent_surge", "case_study_view", "competitor_mention",
    ]
    n_active = rng.randint(15, 20)
    active_dates = _clustered_dates(n_active, date(2025, 4, 5), date(2025, 6, 25),
                                    rng, q34_bias=False)
    for d in active_dates:
        events.append(_make_event(acc_id, rng.choice(active_types), d, ind, rng))

    # July-August: Zero signals (truly dark)

    # September: 3 minor signals — produces ~22 score on Sept 29 snapshot
    #   web_visit(5) + web_visit(5) + competitor_mention(12) = 22
    sept_signals = [
        (date(2025, 9, 2),  "web_visit"),
        (date(2025, 9, 10), "web_visit"),
        (date(2025, 9, 24), "competitor_mention"),  # in Sept 29 "past 7 days" window
    ]
    for d, sig_type in sept_signals:
        events.append(_make_event(acc_id, sig_type, d, ind, rng))

    # October 14-18: Re-engagement cluster
    #   Oct 20 trailing 30d (Sept 20-Oct 20): no Sept signals in window + Oct cluster
    #   15+15+15+10+15 = 70  →  score ~70-85 depending on Sept tail
    reengagement = [
        (date(2025, 10, 14), "pricing_page_visit"),
        (date(2025, 10, 14), "pricing_page_visit"),
        (date(2025, 10, 15), "executive_change"),
        (date(2025, 10, 16), "content_download"),
        (date(2025, 10, 17), "pricing_page_visit"),
        (date(2025, 10, 18), "web_visit"),
    ]
    for d, sig_type in reengagement:
        events.append(_make_event(acc_id, sig_type, d, ind, rng))

    # November-March: Moderate post-re-engagement activity
    n_post = rng.randint(8, 12)
    post_types = ["web_visit", "content_download", "pricing_page_visit", "case_study_view"]
    post_dates = _clustered_dates(n_post, date(2025, 11, 1), date(2026, 3, 25),
                                  rng, q34_bias=False)
    for d in post_dates:
        events.append(_make_event(acc_id, rng.choice(post_types), d, ind, rng))

    return events


# ══════════════════════════════════════════════════════════════════════════════
# ARC 6 — Tribal pattern: Series B + CISO + pricing_page_visit → conversion
# ══════════════════════════════════════════════════════════════════════════════

def _generate_arc6_signals(data, rng):
    """Inject pricing_page_visit signals on Series B + CISO + Closed Won accounts.

    Returns (events, qualifying_accounts, qualifying_opp_count).
    """
    events = []
    qualifying = []
    qualifying_opp_count = 0

    for acc_id, rec in data["enrich_by_acc"].items():
        if (rec["funding_stage"] == "Series B"
                and acc_id in data["ciso_accounts"]
                and acc_id in data["closed_won_accounts"]):
            qualifying.append(acc_id)
            qualifying_opp_count += len(data["closed_won_opps"][acc_id])

    for acc_id in qualifying:
        # Fortress already has pricing_page_visit from Arc 5 (Apr-Jun)
        if acc_id == "sf_acc_001":
            continue
        ind = data["enrich_by_acc"][acc_id].get("industry", "")

        # Place 1-2 pricing_page_visit signals in April-July 2025
        # (well before any opportunity CreatedDate)
        n = rng.randint(1, 2)
        for _ in range(n):
            sig_date = _random_date(date(2025, 4, 5), date(2025, 7, 15), rng)
            events.append(_make_event(
                acc_id, "pricing_page_visit", sig_date, ind, rng,
                metadata={"tribal_pattern_qualifying": True},
            ))

    return events, qualifying, qualifying_opp_count


# ══════════════════════════════════════════════════════════════════════════════
# ARC 16 — ENT/STRAT false-positive pricing signals (Oct+)
# ══════════════════════════════════════════════════════════════════════════════

def _generate_arc16_signals(data, rng):
    """pricing_page_visit false positives on ENT/STRAT accounts (1000+ emp, no closed-won)."""
    events = []
    candidates = []

    for acc_id, rec in data["enrich_by_acc"].items():
        if (rec["employee_count_current"] >= 1000
                and rec["territory"] in ("ENT", "STRAT")
                and acc_id not in data["closed_won_accounts"]):
            candidates.append(acc_id)

    # Generate ~20-30 false-positive pricing signals Oct-Nov
    n_signals = min(len(candidates) * 2, 30)
    for i in range(n_signals):
        acc_id = candidates[i % len(candidates)]
        sig_date = _random_date(date(2025, 10, 1), date(2025, 11, 30), rng)
        ind = data["enrich_by_acc"][acc_id].get("industry", "")
        events.append(_make_event(
            acc_id, "pricing_page_visit", sig_date, ind, rng,
            metadata={"likely_false_positive": True},
        ))

    return events, candidates


# ══════════════════════════════════════════════════════════════════════════════
# BASELINE signal generation (all accounts)
# ══════════════════════════════════════════════════════════════════════════════

def _generate_baseline_signals(data, rng):
    """Generate baseline signals for all accounts (excluding Arc 5 Fortress override)."""
    events = []

    for acc_id, rec in data["enrich_by_acc"].items():
        # Fortress handled by Arc 5
        if acc_id == "sf_acc_001":
            continue

        tier = rec["tier"]
        is_closed_won = acc_id in data["closed_won_accounts"]
        industry = rec.get("industry", "")

        # Signal count by tier
        if tier == 1:
            n_signals = rng.randint(35, 65)
        elif tier == 2:
            n_signals = rng.randint(20, 45)
        elif tier == 3:
            # ~38% of T3 accounts get full signal sets
            if rng.random() > 0.38:
                continue
            n_signals = rng.randint(5, 20)
        else:
            continue

        dates = _clustered_dates(n_signals, DATA_START, DATA_END, rng)
        for d in dates:
            sig_type = _pick_signal_type(is_closed_won, rng)
            events.append(_make_event(acc_id, sig_type, d, industry, rng))

    return events


def _generate_arc15_intent_spray(all_events, data, rng):
    """Arc 15: Spray intent_surge broadly across T3 accounts so it converts at ~1.1x.

    intent_surge should appear on nearly every account — including the many
    T3 accounts that never convert — to dilute the conversion rate to ~1.1x.
    """
    # Find accounts that already have intent_surge
    accs_with_intent = {e["account_id"] for e in all_events
                        if e["signal_type"] == "intent_surge"}

    # Target: ~430+ total accounts with intent_surge (yields ~1.1x ratio)
    target = 430
    deficit = target - len(accs_with_intent)
    if deficit <= 0:
        return []

    # Pick T3 accounts without existing intent_surge
    t3_without = [
        acc_id for acc_id, rec in data["enrich_by_acc"].items()
        if rec["tier"] == 3 and acc_id not in accs_with_intent
    ]
    rng.shuffle(t3_without)

    events = []
    for acc_id in t3_without[:deficit]:
        n = rng.randint(1, 2)
        ind = data["enrich_by_acc"][acc_id].get("industry", "")
        for _ in range(n):
            sig_date = _random_date(DATA_START, DATA_END, rng)
            events.append(_make_event(acc_id, "intent_surge", sig_date, ind, rng))

    return events


# ══════════════════════════════════════════════════════════════════════════════
# SCORE HISTORY computation
# ══════════════════════════════════════════════════════════════════════════════

def _compute_score_history(signal_events):
    """Compute weekly score snapshots from signal events.

    For each Monday, generate a record for any account with ≥1 signal in
    the past 7 days.  Score = sum of trailing-30-day signal weights, capped at 100.
    """
    # Pre-process: group by account, parse dates, sort
    by_account = defaultdict(list)
    for ev in signal_events:
        d = date.fromisoformat(ev["signal_date"])
        by_account[ev["account_id"]].append(
            (d, ev["weight_applied"], ev["signal_type"])
        )

    for acc_id in by_account:
        by_account[acc_id].sort()

    records = []
    counter = 0

    # Find first Monday on or after DATA_START
    current = DATA_START
    while current.weekday() != 0:
        current += timedelta(days=1)

    while current <= DATA_END:
        week_start = current - timedelta(days=6)   # past 7 calendar days
        trailing_start = current - timedelta(days=30)

        for acc_id, signals in by_account.items():
            dates_only = [s[0] for s in signals]

            # Any signal in past 7 days?
            lo7 = bisect_left(dates_only, week_start)
            hi = bisect_right(dates_only, current)
            if lo7 >= hi:
                continue

            # Trailing 30-day score
            lo30 = bisect_left(dates_only, trailing_start)
            trailing = signals[lo30:hi]

            breakdown = defaultdict(int)
            for _, weight, sig_type in trailing:
                breakdown[sig_type] += weight

            score = min(sum(breakdown.values()), 100)
            if score < 8:
                continue  # skip very-low-activity snapshots

            counter += 1
            records.append({
                "id": f"sh_{counter:05d}",
                "account_id": acc_id,
                "score_date": current.isoformat(),
                "score": score,
                "breakdown": dict(breakdown),
                "trailing_30d_signals": len(trailing),
                "tribal_pattern_id": None,
                "rep_feedback": None,
            })

        current += timedelta(days=7)

    return records


# ══════════════════════════════════════════════════════════════════════════════
# ARC 16 — Rep feedback on score_history
# ══════════════════════════════════════════════════════════════════════════════

_REP_FEEDBACK = [
    "Enterprise account — likely procurement browsing, not champion interest",
    "Large account pricing visit — not a buy signal, just annual review",
    "This is procurement doing vendor comparison, not a real signal",
    "False positive — security team doing routine vendor audit, not evaluating",
    "Pricing page hit is from IT procurement, not a buying signal",
    "Annual vendor review — procurement triggered, not champion-led",
]


def _add_arc16_feedback(score_history, arc16_accounts, rng):
    """Stamp rep_feedback on 6 November score_history records for ENT/STRAT accounts."""
    arc16_set = set(arc16_accounts)

    nov_records = [
        r for r in score_history
        if r["account_id"] in arc16_set
        and r["score_date"].startswith("2025-11")
    ]
    if not nov_records:
        return

    all_reps = [r["id"] for r in SDRS + AES_SMB_MM + AES_ENT_STRAT]
    rng.shuffle(all_reps)
    rng.shuffle(nov_records)

    used_accounts = set()
    count = 0
    for rec in nov_records:
        if count >= 6:
            break
        if rec["account_id"] in used_accounts:
            continue
        rec["rep_feedback"] = _REP_FEEDBACK[count]
        used_accounts.add(rec["account_id"])
        count += 1


# ══════════════════════════════════════════════════════════════════════════════
# TRIBAL PATTERNS
# ══════════════════════════════════════════════════════════════════════════════

def _generate_tribal_patterns(arc6_accounts, arc6_opp_count):
    """Generate 7 tribal patterns.  tp_001 is the Arc 6 Series B+CISO+pricing pattern."""
    return [
        {
            "id": "tp_001",
            "name": "Series B + CISO + Pricing Engagement",
            "signal_conditions": {
                "funding_stage": "Series B",
                "contact_title_contains": "CISO",
                "signal_type_required": "pricing_page_visit",
            },
            "historical_conversion_rate": 3.2,
            "baseline_conversion_rate": 1.0,
            "sample_size": arc6_opp_count,
            "confidence": "high",
            "discovered_date": "2025-12-15",
            "status": "confirmed",
        },
        {
            "id": "tp_002",
            "name": "Job Change + Content Download → Pipeline Creation",
            "signal_conditions": {
                "signal_type_required": "job_change",
                "followed_by": "content_download",
                "within_days": 30,
            },
            "historical_conversion_rate": 2.8,
            "baseline_conversion_rate": 1.0,
            "sample_size": 12,
            "confidence": "high",
            "discovered_date": "2025-11-20",
            "status": "confirmed",
        },
        {
            "id": "tp_003",
            "name": "Funding Event + Competitor Mention → Evaluation",
            "signal_conditions": {
                "signal_type_required": "funding_event",
                "followed_by": "competitor_mention",
                "within_days": 60,
            },
            "historical_conversion_rate": 2.4,
            "baseline_conversion_rate": 1.0,
            "sample_size": 6,
            "confidence": "medium",
            "discovered_date": "2026-01-10",
            "status": "confirmed",
        },
        {
            "id": "tp_004",
            "name": "Multi-Signal Cluster → Fast Close",
            "signal_conditions": {
                "min_signals_in_week": 4,
                "must_include": ["pricing_page_visit", "web_visit"],
            },
            "historical_conversion_rate": 4.1,
            "baseline_conversion_rate": 1.0,
            "sample_size": 5,
            "confidence": "medium",
            "discovered_date": "2026-02-01",
            "status": "monitoring",
        },
        {
            "id": "tp_005",
            "name": "Intent Surge + Case Study View → Champion Emergence",
            "signal_conditions": {
                "signal_type_required": "intent_surge",
                "followed_by": "case_study_view",
                "within_days": 14,
            },
            "historical_conversion_rate": 1.9,
            "baseline_conversion_rate": 1.0,
            "sample_size": 9,
            "confidence": "medium",
            "discovered_date": "2025-12-28",
            "status": "confirmed",
        },
        {
            "id": "tp_006",
            "name": "Executive Change + Pricing Page → Budget Cycle Entry",
            "signal_conditions": {
                "signal_type_required": "executive_change",
                "followed_by": "pricing_page_visit",
                "within_days": 45,
            },
            "historical_conversion_rate": 3.5,
            "baseline_conversion_rate": 1.0,
            "sample_size": 4,
            "confidence": "low",
            "discovered_date": "2026-03-01",
            "status": "emerging",
        },
        {
            "id": "tp_007",
            "name": "Technology Install + Web Visit Cluster → Platform Evaluation",
            "signal_conditions": {
                "signal_type_required": "technology_install",
                "followed_by_cluster": "web_visit",
                "min_cluster_size": 3,
            },
            "historical_conversion_rate": 2.1,
            "baseline_conversion_rate": 1.0,
            "sample_size": 7,
            "confidence": "medium",
            "discovered_date": "2026-01-25",
            "status": "confirmed",
        },
    ]


# ══════════════════════════════════════════════════════════════════════════════
# ACCOUNT PREFERENCES
# ══════════════════════════════════════════════════════════════════════════════

_SNOOZE_REASONS = [
    "In active renewal — do not outbound",
    "Account in legal review — pause all outreach",
    "Recently churned — cooling off period",
    "Customer success managing relationship — no sales outreach",
    "Active support escalation — hold outbound",
    "Contract negotiation in progress — no cold outreach",
    "Champion on leave — wait for return",
    "Budget freeze communicated — revisit next quarter",
    "Duplicate account — use primary record instead",
    "Under NDA discussion — restrict communications",
]

_OVERRIDE_REASONS = [
    "Score too low — account is more engaged than signals show",
    "Score inflated by bot traffic — actual engagement is lower",
    "Internal champion confirmed interest verbally — boost priority",
    "Competitor displacement in progress — manual priority boost",
    "Strategic account — maintain high visibility regardless of score",
]


def _generate_account_preferences(data, rng):
    """Generate 15-25 snooze/override records spread across reps."""
    prefs = []
    all_reps = [r["id"] for r in SDRS + AES_SMB_MM + AES_ENT_STRAT]

    # Pool of T1+T2 accounts for preferences
    eligible = [
        f"sf_acc_{rec['company_index'] + 1:03d}"
        for rec in data["enrichment"]
        if rec["tier"] in (1, 2)
    ]
    rng.shuffle(eligible)

    n_prefs = rng.randint(15, 25)
    used = set()

    for i in range(n_prefs):
        rep_id = rng.choice(all_reps)
        acc_id = eligible[i % len(eligible)]
        combo = (rep_id, acc_id)
        if combo in used:
            continue
        used.add(combo)

        if rng.random() < 0.70:
            pref_type = "snooze"
            reason = rng.choice(_SNOOZE_REASONS)
            created = _random_date(date(2025, 6, 1), date(2026, 2, 1), rng)
            expires = created + timedelta(days=rng.randint(30, 90))
        else:
            pref_type = "score_override"
            reason = rng.choice(_OVERRIDE_REASONS)
            created = _random_date(date(2025, 8, 1), date(2026, 2, 1), rng)
            expires = created + timedelta(days=rng.randint(14, 60))

        prefs.append({
            "id": f"ap_{len(prefs) + 1:03d}",
            "rep_id": rep_id,
            "account_id": acc_id,
            "preference_type": pref_type,
            "reason": reason,
            "created_date": created.isoformat(),
            "expires_date": expires.isoformat(),
        })

    return prefs


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def _print_validation(events, scores, patterns, prefs,
                      arc6_accs, arc16_accs, data):
    """Print Phase 4 validation statistics."""
    from collections import Counter

    print("\n  === Phase 4 Validation ===")

    # Record counts
    print(f"\n  Record counts:")
    print(f"    signal_events:       {len(events)}")
    print(f"    score_history:       {len(scores)}")
    print(f"    tribal_patterns:     {len(patterns)}")
    print(f"    account_preferences: {len(prefs)}")

    # Signal type distribution
    type_counts = Counter(e["signal_type"] for e in events)
    print(f"\n  Signal type distribution:")
    for st in sorted(type_counts.keys()):
        print(f"    {st:<25} {type_counts[st]:>5}")

    # Tier distribution
    tier_counts = Counter()
    for e in events:
        rec = data["enrich_by_acc"].get(e["account_id"])
        if rec:
            tier_counts[rec["tier"]] += 1
    print(f"\n  Signals by tier:")
    for tier in sorted(tier_counts.keys()):
        print(f"    Tier {tier}: {tier_counts[tier]}")

    # ── Arc 5 ────────────────────────────────────────────────────────────
    fortress_events = [e for e in events if e["account_id"] == "sf_acc_001"]
    fortress_by_month = defaultdict(int)
    for e in fortress_events:
        fortress_by_month[e["signal_date"][:7]] += 1
    print(f"\n  Arc 5 — Fortress Security Corp ({len(fortress_events)} signals):")
    for month in sorted(fortress_by_month.keys()):
        print(f"    {month}: {fortress_by_month[month]} signals")

    fortress_scores = [
        s for s in scores
        if s["account_id"] == "sf_acc_001"
        and s["score_date"] >= "2025-09-01"
        and s["score_date"] <= "2025-11-15"
    ]
    print(f"    Score snapshots (Sep-Nov):")
    for s in fortress_scores:
        print(f"      {s['score_date']}: score={s['score']} "
              f"(signals={s['trailing_30d_signals']})")

    # ── Arc 6 ────────────────────────────────────────────────────────────
    print(f"\n  Arc 6 — Tribal Pattern:")
    print(f"    Qualifying accounts (Series B + CISO + Closed Won): {len(arc6_accs)}")
    if arc6_accs:
        for acc in arc6_accs[:5]:
            rec = data["enrich_by_acc"][acc]
            print(f"      {acc}: {rec['company_name']} ({rec['funding_stage']}, "
                  f"{rec['territory']})")
    print(f"    tp_001 sample_size: {patterns[0]['sample_size']}")

    # ── Arc 15 ───────────────────────────────────────────────────────────
    closed_won = data["closed_won_accounts"]
    all_accs = set(data["enrich_by_acc"].keys())
    base_rate = len(closed_won) / len(all_accs) * 100 if all_accs else 0

    signal_accs_by_type = defaultdict(set)
    for e in events:
        signal_accs_by_type[e["signal_type"]].add(e["account_id"])

    print(f"\n  Arc 15 — Signal ROI Divergence (baseline won rate={base_rate:.1f}%):")
    for sig_type in ["job_change", "pricing_page_visit", "intent_surge"]:
        accs_with = signal_accs_by_type[sig_type]
        won_with = len(accs_with & closed_won)
        total_with = len(accs_with)
        won_rate = won_with / total_with * 100 if total_with else 0
        ratio = won_rate / base_rate if base_rate else 0
        print(f"    {sig_type:<25} won/total={won_with}/{total_with} "
              f"({won_rate:.1f}%) ratio={ratio:.1f}x")

    # ── Arc 16 ───────────────────────────────────────────────────────────
    feedback_records = [s for s in scores if s["rep_feedback"]]
    print(f"\n  Arc 16 — Scoring Drift + Rep Feedback:")
    print(f"    Score records with rep_feedback: {len(feedback_records)}")
    for r in feedback_records[:3]:
        print(f"      {r['account_id']} ({r['score_date']}): "
              f"\"{r['rep_feedback'][:60]}\"")

    # Date range check
    dates = [e["signal_date"] for e in events]
    print(f"\n  Date range: {min(dates)} to {max(dates)}")
    oob = [d for d in dates if d < DATA_START.isoformat() or d > DATA_END.isoformat()]
    print(f"  Out-of-range signals: {len(oob)}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def generate_signal_files(output_dir):
    """Generate all signal layer files.  Returns dict of filename → record count."""
    _rng.seed(GLOBAL_SEED + 700)

    data = _load_data(output_dir)

    # ── Signal events ────────────────────────────────────────────────────

    all_events = []

    arc5_events = _generate_arc5_signals(_rng)
    all_events.extend(arc5_events)

    arc6_events, arc6_accounts, arc6_opp_count = _generate_arc6_signals(data, _rng)
    all_events.extend(arc6_events)

    arc16_events, arc16_accounts = _generate_arc16_signals(data, _rng)
    all_events.extend(arc16_events)

    baseline_events = _generate_baseline_signals(data, _rng)
    all_events.extend(baseline_events)

    # Arc 15: spray intent_surge broadly so it converts at ~1.1x
    intent_spray = _generate_arc15_intent_spray(all_events, data, _rng)
    all_events.extend(intent_spray)

    # Sort by date and assign sequential IDs
    all_events.sort(key=lambda e: e["signal_date"])
    for i, ev in enumerate(all_events):
        ev["id"] = f"sig_{i + 1:05d}"

    # ── Score history ────────────────────────────────────────────────────

    score_history = _compute_score_history(all_events)

    # Arc 16: stamp rep_feedback on 6 November records
    _add_arc16_feedback(score_history, arc16_accounts, _rng)

    # Link tribal pattern ID to Arc 6 qualifying accounts
    arc6_set = set(arc6_accounts)
    for rec in score_history:
        if rec["account_id"] in arc6_set:
            rec["tribal_pattern_id"] = "tp_001"

    # ── Tribal patterns ──────────────────────────────────────────────────

    tribal_patterns = _generate_tribal_patterns(arc6_accounts, arc6_opp_count)

    # ── Account preferences ──────────────────────────────────────────────

    account_preferences = _generate_account_preferences(data, _rng)

    # ── Write output files ───────────────────────────────────────────────

    os.makedirs(output_dir, exist_ok=True)

    with open(os.path.join(output_dir, "signal_events.json"), "w") as f:
        json.dump(all_events, f, indent=2)

    with open(os.path.join(output_dir, "score_history.json"), "w") as f:
        json.dump(score_history, f, indent=2)

    with open(os.path.join(output_dir, "tribal_patterns.json"), "w") as f:
        json.dump(tribal_patterns, f, indent=2)

    with open(os.path.join(output_dir, "account_preferences.json"), "w") as f:
        json.dump(account_preferences, f, indent=2)

    # ── Validation ───────────────────────────────────────────────────────

    _print_validation(all_events, score_history, tribal_patterns,
                      account_preferences, arc6_accounts, arc16_accounts, data)

    return {
        "signal_events.json": len(all_events),
        "score_history.json": len(score_history),
        "tribal_patterns.json": len(tribal_patterns),
        "account_preferences.json": len(account_preferences),
    }
