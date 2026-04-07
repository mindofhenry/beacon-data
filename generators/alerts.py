"""
generators/alerts.py
Phase 5: Alert log generation — generates alert_log.json.

Alerts fire when signal scores cross thresholds or specific score changes occur.
Each alert is assigned to an SDR based on account territory mapping.

Arc 13: Coverage gap pattern — Oct 20-24 cluster of unresponded alerts.
"""

import json
import os
import random
from collections import defaultdict
from datetime import date, datetime, timedelta

from .config import GLOBAL_SEED, DATA_START, DATA_END, OUTPUT_DIR
from .org_structure import SDRS

_rng = random.Random(GLOBAL_SEED + 900)

# ── Alert tier definitions ─────────────────────────────────────────────────

ALERT_TIERS = {
    "CRITICAL": {"response_avg_hrs": 1.5, "response_std_hrs": 0.5},
    "HIGH":     {"response_avg_hrs": 4.0, "response_std_hrs": 1.5},
    "STANDARD": {"response_avg_hrs": 12.0, "response_std_hrs": 4.0},
}

RESPONSE_ACTIONS = [
    "sequence_enrolled", "manual_email_sent", "call_scheduled",
    "account_researched", "dismissed", "escalated_to_ae",
]

# Weighted: sequence_enrolled and call_scheduled more common than dismissed
_ACTION_WEIGHTS = [25, 20, 20, 15, 10, 10]

# ── Alert title/body templates ─────────────────────────────────────────────

_TITLE_TEMPLATES = {
    "score_spike": "Score spike: {company} ({prev} → {curr})",
    "score_threshold_80": "Score crossed 80: {company} (now {curr})",
    "score_threshold_60": "Score crossed 60: {company} (now {curr})",
    "re_engagement": "Re-engagement detected: {company} (score {curr} after {silence_days}d silence)",
    "score_change": "Score change: {company} ({prev} → {curr})",
}

_BODY_TEMPLATES = {
    "score_spike": [
        "Significant score jump detected. {signals_summary}. Review account for outbound timing.",
        "Sharp increase in engagement signals. {signals_summary}. Prioritize outreach.",
        "Account activity surged this week. {signals_summary}. Consider immediate follow-up.",
    ],
    "score_threshold_80": [
        "Account has crossed the critical engagement threshold. {signals_summary}.",
        "High-priority account now scoring above 80. {signals_summary}. Immediate outreach recommended.",
        "Critical score reached. {signals_summary}. This account is in active evaluation.",
    ],
    "score_threshold_60": [
        "Account engagement is increasing. {signals_summary}. Consider adding to active sequence.",
        "Moderate engagement threshold crossed. {signals_summary}. Good candidate for personalized outreach.",
        "Score moving into actionable range. {signals_summary}. Review account history before outreach.",
    ],
    "re_engagement": [
        "Account re-emerged after {silence_days} days of silence. {signals_summary}.",
        "Previously dark account showing new activity. {signals_summary}. Re-engagement window open.",
        "Dormant account reactivated. {signals_summary}. Champion may have returned or new stakeholder emerged.",
    ],
    "score_change": [
        "Notable score movement this week. {signals_summary}.",
        "Account engagement shifted. {signals_summary}. Monitor for continued trend.",
        "Score change detected. {signals_summary}. Consider adjusting outreach cadence.",
    ],
}


def _signals_summary(breakdown):
    """Format a breakdown dict into a readable summary string."""
    parts = []
    for sig_type, weight in sorted(breakdown.items(), key=lambda x: -x[1]):
        label = sig_type.replace("_", " ")
        parts.append(f"{label} ({weight}pts)")
    return ", ".join(parts[:4]) if parts else "mixed signals"


# ── SDR assignment ─────────────────────────────────────────────────────────

SDR_IDS = [s["id"] for s in SDRS]  # sdr_1 through sdr_6


def _build_account_sdr_map(enrichment):
    """Map each account_id to an SDR for alert assignment.

    T1 accounts (25): distributed ~4 per SDR by index.
    T2/T3: round-robin by territory.
    """
    acc_sdr = {}
    territory_robin = defaultdict(int)

    for rec in enrichment:
        acc_id = f"sf_acc_{rec['company_index'] + 1:03d}"
        tier = rec["tier"]

        if tier == 1:
            # Distribute T1 accounts evenly across 6 SDRs
            sdr_idx = rec["company_index"] % 6
            acc_sdr[acc_id] = SDR_IDS[sdr_idx]
        else:
            # Round-robin by territory
            territory = rec.get("territory", "SMB")
            idx = territory_robin[territory] % 6
            territory_robin[territory] += 1
            acc_sdr[acc_id] = SDR_IDS[idx]

    return acc_sdr


# ── Core alert generation ──────────────────────────────────────────────────

def _generate_alerts(score_history, enrichment, rng):
    """Walk score_history chronologically, detect threshold crossings, generate alerts."""

    # Build enrichment lookup
    enrich_by_acc = {}
    company_names = {}
    for rec in enrichment:
        acc_id = f"sf_acc_{rec['company_index'] + 1:03d}"
        enrich_by_acc[acc_id] = rec
        company_names[acc_id] = rec["company_name"]

    acc_sdr = _build_account_sdr_map(enrichment)

    # Group scores by account, sorted by date
    scores_by_account = defaultdict(list)
    for rec in score_history:
        scores_by_account[rec["account_id"]].append(rec)
    for acc_id in scores_by_account:
        scores_by_account[acc_id].sort(key=lambda r: r["score_date"])

    # Track last score date per account (for re-engagement detection)
    alerts = []

    for acc_id, score_recs in scores_by_account.items():
        company = company_names.get(acc_id, acc_id)
        sdr_id = acc_sdr.get(acc_id, rng.choice(SDR_IDS))

        prev_score = 0
        prev_date = None

        for rec in score_recs:
            curr_score = rec["score"]
            curr_date = date.fromisoformat(rec["score_date"])
            breakdown = rec.get("breakdown", {})
            signals_text = _signals_summary(breakdown)

            delta = curr_score - prev_score

            # Calculate silence gap
            silence_days = (curr_date - prev_date).days if prev_date else 0

            alert_type = None
            tier = None
            title_key = None
            extra = {}

            # CRITICAL: score jump >30 points
            if delta > 30:
                alert_type = "score_spike"
                tier = "CRITICAL"
                title_key = "score_spike"

            # CRITICAL: score crosses 80 (was below, now at or above)
            elif curr_score >= 80 and prev_score < 80:
                alert_type = "score_threshold"
                tier = "CRITICAL"
                title_key = "score_threshold_80"

            # HIGH: score crosses 60
            elif curr_score >= 60 and prev_score < 60:
                alert_type = "score_threshold"
                tier = "HIGH"
                title_key = "score_threshold_60"

            # HIGH: re-engagement after >60 days silence
            elif silence_days > 60 and curr_score >= 20:
                alert_type = "re_engagement"
                tier = "HIGH"
                title_key = "re_engagement"
                extra["silence_days"] = silence_days

            # STANDARD: score change >10 points
            elif delta > 10:
                alert_type = "score_change"
                tier = "STANDARD"
                title_key = "score_change"

            if alert_type and tier:
                # Generate timestamp (business hours on the score_date)
                hour = rng.randint(8, 17)
                minute = rng.randint(0, 59)
                ts = datetime(curr_date.year, curr_date.month, curr_date.day,
                              hour, minute, 0)

                title = _TITLE_TEMPLATES[title_key].format(
                    company=company, prev=prev_score, curr=curr_score,
                    silence_days=extra.get("silence_days", 0),
                )
                body = rng.choice(_BODY_TEMPLATES[title_key]).format(
                    signals_summary=signals_text,
                    silence_days=extra.get("silence_days", 0),
                )

                alerts.append({
                    "alert_type": alert_type,
                    "tier": tier,
                    "account_id": acc_id,
                    "rep_id": sdr_id,
                    "timestamp": ts.isoformat(),
                    "title": title,
                    "body": body,
                    "score": curr_score,
                    "prev_score": prev_score,
                    "_score_date": curr_date.isoformat(),
                })

            prev_score = curr_score
            prev_date = curr_date

    return alerts


# ── Response behavior ──────────────────────────────────────────────────────

def _add_response_behavior(alerts, rng):
    """Add responded/response_time_hours/response_action to each alert."""
    for alert in alerts:
        tier_params = ALERT_TIERS[alert["tier"]]
        # 80% respond normally
        responded = rng.random() < 0.80
        if responded:
            avg = tier_params["response_avg_hrs"]
            std = tier_params["response_std_hrs"]
            response_time = max(0.1, rng.gauss(avg, std))
            action = rng.choices(RESPONSE_ACTIONS, weights=_ACTION_WEIGHTS, k=1)[0]
        else:
            response_time = None
            action = None

        alert["responded"] = responded
        alert["response_time_hours"] = round(response_time, 1) if response_time else None
        alert["response_action"] = action


# ── Arc 13: Coverage gap (Oct 20-24, 2025) ─────────────────────────────────

ARC_13_START = date(2025, 10, 20)
ARC_13_END = date(2025, 10, 24)
ARC_13_SDRS = ["sdr_1", "sdr_2", "sdr_4", "sdr_5"]
ARC_13_COUNT = 12


def _inject_arc13(alerts, score_history, enrichment, rng):
    """Ensure exactly 12 HIGH/CRITICAL alerts in Oct 20-24 are unresponded/late.

    Spread across sdr_1, sdr_2, sdr_4, sdr_5 (~3 each).
    Surrounding weeks maintain normal ~80% response rate.
    """
    enrich_by_acc = {}
    company_names = {}
    for rec in enrichment:
        acc_id = f"sf_acc_{rec['company_index'] + 1:03d}"
        enrich_by_acc[acc_id] = rec
        company_names[acc_id] = rec["company_name"]

    # Find accounts with score >60 near Oct 20
    high_score_accounts = set()
    for rec in score_history:
        sd = rec["score_date"]
        if "2025-10-13" <= sd <= "2025-10-27" and rec["score"] > 60:
            high_score_accounts.add(rec["account_id"])

    target_sdr_set = set(ARC_13_SDRS)

    # Split alerts into window vs non-window
    arc13_window = []
    other_alerts = []
    for alert in alerts:
        alert_date = date.fromisoformat(alert["_score_date"])
        if ARC_13_START <= alert_date <= ARC_13_END:
            arc13_window.append(alert)
        else:
            other_alerts.append(alert)

    # First, reset all window alerts from target SDRs to normal responded state
    # (they got response behavior earlier, we'll override selectively)

    # Group existing HIGH/CRITICAL window alerts by target SDR
    by_sdr = defaultdict(list)
    for a in arc13_window:
        if a["rep_id"] in target_sdr_set and a["tier"] in ("CRITICAL", "HIGH"):
            by_sdr[a["rep_id"]].append(a)

    # Ensure each of the 4 target SDRs has at least 3 HIGH/CRITICAL alerts
    candidate_accounts = list(high_score_accounts)
    rng.shuffle(candidate_accounts)
    ca_idx = 0

    for sdr_id in ARC_13_SDRS:
        deficit = 3 - len(by_sdr[sdr_id])
        for _ in range(deficit):
            acc_id = candidate_accounts[ca_idx % len(candidate_accounts)]
            ca_idx += 1
            company = company_names.get(acc_id, acc_id)
            score_val = 65 + rng.randint(0, 20)

            alert_date = ARC_13_START + timedelta(days=rng.randint(0, 4))
            hour = rng.randint(8, 17)
            minute = rng.randint(0, 59)
            ts = datetime(alert_date.year, alert_date.month, alert_date.day,
                          hour, minute, 0)

            tier = rng.choice(["CRITICAL", "HIGH"])
            title = (f"Score spike: {company} (score {score_val})" if tier == "CRITICAL"
                     else f"Score crossed 60: {company} (now {score_val})")
            body = ("Engagement surge during product launch week. "
                    "Alert queued but not actioned due to enablement sessions.")

            new_alert = {
                "alert_type": "score_threshold" if tier == "HIGH" else "score_spike",
                "tier": tier,
                "account_id": acc_id,
                "rep_id": sdr_id,
                "timestamp": ts.isoformat(),
                "title": title,
                "body": body,
                "score": score_val,
                "prev_score": score_val - rng.randint(15, 35),
                "_score_date": alert_date.isoformat(),
                "responded": False,
                "response_time_hours": None,
                "response_action": None,
            }
            arc13_window.append(new_alert)
            by_sdr[sdr_id].append(new_alert)

    # Mark exactly 3 per SDR (12 total) as unresponded or very late
    marked_set = set()
    for sdr_id in ARC_13_SDRS:
        for a in by_sdr[sdr_id][:3]:
            # Mix: ~60% fully unresponded, ~40% responded but very late (>24hr)
            if rng.random() < 0.6:
                a["responded"] = False
                a["response_time_hours"] = None
                a["response_action"] = None
            else:
                a["responded"] = True
                a["response_time_hours"] = round(24 + rng.uniform(2, 48), 1)
                a["response_action"] = rng.choice(["account_researched", "dismissed"])
            marked_set.add(id(a))

    # Ensure non-targeted window alerts are responded normally
    # (Arc 13 gap is specifically about the 4 target SDRs being overwhelmed)
    for a in arc13_window:
        if id(a) not in marked_set:
            tier_params = ALERT_TIERS[a["tier"]]
            avg = tier_params["response_avg_hrs"]
            std = tier_params["response_std_hrs"]
            a["responded"] = True
            a["response_time_hours"] = round(max(0.1, rng.gauss(avg, std)), 1)
            a["response_action"] = rng.choices(RESPONSE_ACTIONS, weights=_ACTION_WEIGHTS, k=1)[0]

    return other_alerts + arc13_window


# ── Volume control ─────────────────────────────────────────────────────────

def _control_volume(alerts, target_min=1000, target_max=2000, rng=None):
    """Ensure alert count falls within target range.

    If too many: sample down, preserving Arc 13 alerts.
    If too few: unlikely given the thresholds, but we'd lower the bar.
    """
    if len(alerts) <= target_max:
        return alerts

    # Preserve Arc 13 window alerts
    arc13 = [a for a in alerts if ARC_13_START.isoformat() <= a["_score_date"] <= ARC_13_END.isoformat()]
    non_arc13 = [a for a in alerts if a not in arc13]

    # Sample non-arc13 down
    needed = target_max - len(arc13)
    if needed < len(non_arc13):
        rng.shuffle(non_arc13)
        non_arc13 = non_arc13[:needed]

    return arc13 + non_arc13


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def generate_alert_files(output_dir):
    """Generate alert_log.json. Returns dict of filename → record count."""
    _rng.seed(GLOBAL_SEED + 900)

    # Load dependencies
    with open(os.path.join(output_dir, "score_history.json"), encoding="utf-8") as f:
        score_history = json.load(f)

    with open(os.path.join(output_dir, "account_enrichment.json"), encoding="utf-8") as f:
        enrichment = json.load(f)

    # Generate base alerts from score history
    alerts = _generate_alerts(score_history, enrichment, _rng)

    # Add response behavior
    _add_response_behavior(alerts, _rng)

    # Inject Arc 13 coverage gap
    alerts = _inject_arc13(alerts, score_history, enrichment, _rng)

    # Control volume
    alerts = _control_volume(alerts, rng=_rng)

    # Sort by timestamp and assign sequential IDs
    alerts.sort(key=lambda a: a["timestamp"])
    for i, alert in enumerate(alerts):
        alert["id"] = f"alert_{i + 1:04d}"

    # Remove internal fields before output
    for alert in alerts:
        alert.pop("_score_date", None)
        alert.pop("score", None)
        alert.pop("prev_score", None)

    # Write output
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "alert_log.json"), "w") as f:
        json.dump(alerts, f, indent=2)

    # Print validation
    _print_validation(alerts)

    return {"alert_log.json": len(alerts)}


def _print_validation(alerts):
    """Print Phase 5 alert validation statistics."""
    from collections import Counter

    print("\n  === Phase 5 Alert Validation ===")
    print(f"\n  Total alerts: {len(alerts)}")

    # Tier distribution
    tier_counts = Counter(a["tier"] for a in alerts)
    print(f"\n  Tier distribution:")
    for tier in ["CRITICAL", "HIGH", "STANDARD"]:
        print(f"    {tier}: {tier_counts.get(tier, 0)}")

    # Response rate
    responded = sum(1 for a in alerts if a["responded"])
    print(f"\n  Response rate: {responded}/{len(alerts)} ({responded/len(alerts)*100:.1f}%)")

    # Average response times by tier
    print(f"  Avg response time by tier:")
    for tier in ["CRITICAL", "HIGH", "STANDARD"]:
        times = [a["response_time_hours"] for a in alerts
                 if a["tier"] == tier and a["response_time_hours"] is not None]
        if times:
            print(f"    {tier}: {sum(times)/len(times):.1f}hr (n={len(times)})")

    # Arc 13 check
    oct_alerts = [a for a in alerts
                  if a["timestamp"][:10] >= "2025-10-20"
                  and a["timestamp"][:10] <= "2025-10-24"]
    arc13_unresponded = [a for a in oct_alerts
                         if not a["responded"] or
                         (a["response_time_hours"] and a["response_time_hours"] > 24)]
    arc13_sdrs = Counter(a["rep_id"] for a in arc13_unresponded)
    print(f"\n  Arc 13 — Coverage Gap (Oct 20-24):")
    print(f"    Total alerts in window: {len(oct_alerts)}")
    print(f"    Unresponded/late: {len(arc13_unresponded)}")
    print(f"    By SDR: {dict(arc13_sdrs)}")

    # Monthly distribution
    monthly = Counter()
    for a in alerts:
        monthly[a["timestamp"][:7]] += 1
    print(f"\n  Monthly distribution:")
    for month in sorted(monthly.keys()):
        print(f"    {month}: {monthly[month]}")
