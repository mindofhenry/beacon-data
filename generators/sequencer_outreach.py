"""
generators/sequencer_outreach.py
Longitudinal Outreach sequencer data with weekly cohort model.

Generates:
  - sequences.json      (13 sequence definitions with aggregate counts)
  - sequence_steps.json (step definitions per sequence)
  - email_activity.json (per-prospect mailing records across 52 weeks)

Engagement rates are modulated per-rep, per-week via narrative_arcs.get_rep_multiplier().
"""

import json
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from .config import DATA_START, DATA_END, GLOBAL_SEED, OUTPUT_DIR
from .contact_pool import CONTACT_POOL
from .org_structure import SDRS
from .narrative_arcs import (
    get_rep_multiplier,
    DEGRADING_SEQUENCE_ID,
    REWRITE_STEP_ORDER,
    REWRITE_WEEK,
    NEW_HIRE_WEEK,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_WEEKS = 52  # week 0 = Apr 1-7, 2025 ... week 51 = Mar 25-31, 2026

SDR_LIST = [s for s in SDRS]  # 6 SDRs
SDR_IDS = [s["id"] for s in SDR_LIST]
SDR_MAP = {s["id"]: s for s in SDR_LIST}

POOL_PROSPECTS = [
    {
        "id": 10001 + i,
        "first_name": c["first_name"],
        "last_name": c["last_name"],
        "email": c["email"],
        "title": c["title"],
        "company": c["company"],
    }
    for i, c in enumerate(CONTACT_POOL)
]

# ---------------------------------------------------------------------------
# Email templates — static, no faker
# ---------------------------------------------------------------------------

TOPICS = ["security", "compliance", "identity", "cloud infrastructure", "developer tooling"]
PERSONAS = ["CISO", "VP of Engineering", "IT Director", "CTO", "Security Engineer"]
PAIN_POINTS = ["alert fatigue", "compliance gaps", "shadow IT exposure",
               "slow incident response", "developer security debt"]
TRIGGERS = ["expanded your engineering team", "announced a new product",
            "closed a funding round", "posted a compliance job", "moved to the cloud"]
SIMILAR_COS = ["Stripe", "Notion", "Linear", "Vercel", "Retool", "Figma"]
ASSET_TYPES = ["benchmark report", "case study", "risk calculator", "ROI framework"]

EMAIL_TEMPLATES = [
    {"subject": "Quick question on your {topic} stack",
     "body": "Hi {first_name},\n\nNoticed {company_name} recently {trigger}. Most {persona}s I talk to are dealing with {pain_point} -- is that on your radar?\n\nWorth a 15-minute call?\n\n{rep_name}"},
    {"subject": "Re: {topic} at {company_name}",
     "body": "Hi {first_name},\n\nFollowing up on my last note. We helped {similar_company} reduce {pain_point} meaningfully in under 90 days.\n\nDoes that resonate?\n\n{rep_name}"},
    {"subject": "One thing I missed in my last email",
     "body": "Hi {first_name},\n\nForgot to mention -- we have a {asset_type} on {topic} specifically for {persona}s at companies your size. No pitch, just benchmarks.\n\nWant me to send it over?\n\n{rep_name}"},
    {"subject": "Should I close your file, {first_name}?",
     "body": "Hi {first_name},\n\nI've reached out a few times without a response. Should I close your file, or is there a better time to reconnect?\n\n{rep_name}"},
    {"subject": "Last note -- {topic} for {company_name}",
     "body": "Hi {first_name},\n\nThis is my last note. If {pain_point} ever becomes a priority, my calendar is at calendly.com/beacon-demo.\n\n{rep_name}"},
]

VALUE_ADD_TEMPLATE = {
    "subject": "Q3 {topic} data -- worth 5 minutes?",
    "body": "Hi {first_name},\n\nJust published our {topic} research report -- data from 200+ companies on {pain_point}. The insights for {persona}s at companies your size are specific and actionable.\n\nWant me to send the report over?\n\n{rep_name}",
}

SOCIAL_PROOF_TEMPLATE = {
    "subject": "How {similar_company} tackled {pain_point}",
    "body": "Hi {first_name},\n\nA customer similar to {company_name} -- same stage, same {pain_point} challenge -- reduced their exposure by 60% in 90 days. Happy to share the case study and customer results.\n\n15 minutes this week?\n\n{rep_name}",
}

MULTICHANNEL_EMAIL_TEMPLATE = {
    "subject": "Also sent you a LinkedIn message, {first_name}",
    "body": "Hi {first_name},\n\nI left you a voicemail earlier and sent a LinkedIn connection request too -- wanted to make sure this didn't fall through the cracks.\n\n{pain_point} keeps coming up with {persona}s right now. Is it on your radar?\n\n{rep_name}",
}

# Rewrite v2 template for sequence 1001 step 3 (Jordan's rewrite)
REWRITE_V2_TEMPLATE = {
    "subject": "Your {topic} blind spots -- 3 data points from our research",
    "body": "Hi {first_name},\n\nWe analyzed {pain_point} patterns across 150+ companies in {company_name}'s segment. Three findings stood out:\n\n1. Teams your size typically miss 40% of exposures in the first 72 hours\n2. The median detection-to-response gap is 11 days (vs. 3 days for top quartile)\n3. Companies that consolidated tooling cut that gap by 60%\n\nI can walk you through the full dataset in 15 minutes -- the benchmarks are specific to your industry.\n\n{rep_name}",
}

CALL_TEMPLATES = [
    {"body": "Call attempt -- introduce value prop and ask for 15 minutes."},
    {"body": "Call attempt -- reference last email open, ask about {pain_point}."},
    {"body": "Call attempt -- breakup call, confirm fit or close file."},
]

# Overflow prospect name pools (no faker)
_EXTRA_FIRST_NAMES = [
    "Alex", "Jordan", "Morgan", "Taylor", "Casey", "Riley", "Jamie",
    "Quinn", "Avery", "Blake", "Drew", "Emery", "Finley", "Harley",
    "Kendall", "Logan", "Parker", "Reese", "Sage", "Skyler",
]
_EXTRA_LAST_NAMES = [
    "Chen", "Kim", "Patel", "Nguyen", "Rodriguez", "Williams", "Johnson",
    "Smith", "Brown", "Davis", "Wilson", "Anderson", "Thomas", "Jackson",
    "Martinez", "Garcia", "Lee", "Harris", "Clark", "Lewis",
]
_EXTRA_COMPANIES = [
    "Apex Corp", "Summit Group", "Vertex Inc", "Pinnacle Solutions",
    "Meridian Tech", "Horizon Systems", "Zenith Group", "Atlas Corp",
    "Crestview Inc", "Northgate Solutions", "Ridgemont Group", "Oakfield Corp",
    "Starfield Inc", "Ironwood Group", "BlueSky Systems", "RedRock Corp",
]
_EXTRA_TITLES = [
    "VP of Sales", "VP of Marketing", "CFO", "CIO", "Director of Revenue",
    "Head of Finance", "VP of Revenue Operations", "Chief Financial Officer",
    "VP of Growth", "Director of Marketing", "VP of Finance",
]


# ---------------------------------------------------------------------------
# Sequence configurations (13 sequences, IDs 1001-1013)
# ---------------------------------------------------------------------------

SEQUENCE_CONFIGS = [
    # -- Existing 5 (IDs 1001-1005) -- preserved names/tags/tiers exactly --
    {
        "name": "CISO Outbound -- Q2 Pipeline Push",
        "tags": ["ciso", "enterprise", "q2"],
        "description": "Targeting CISOs at 500-5000 employee companies with a security posture angle.",
        "tier": "green",
        "num_steps": 5,
        "prospect_count": 100,
        "cohort_size": 30,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "VP Eng -- Developer Security Cold",
        "tags": ["vp-eng", "developer-security", "cold"],
        "description": "Cold outreach to VP Engineering at Series B-D SaaS on developer security pain.",
        "tier": "yellow",
        "num_steps": 6,
        "prospect_count": 100,
        "cohort_size": 28,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "IT Director -- Compliance Renewal",
        "tags": ["it-director", "compliance", "renewal"],
        "description": "Re-engagement sequence for IT Directors approaching compliance renewal windows.",
        "tier": "yellow",
        "num_steps": 5,
        "prospect_count": 100,
        "cohort_size": 28,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "CTO Sequence -- Post-Funding Outreach",
        "tags": ["cto", "post-funding", "enterprise"],
        "description": "Targeting CTOs at companies that closed a Series B or C in the last 90 days.",
        "tier": "green",
        "num_steps": 6,
        "prospect_count": 100,
        "cohort_size": 28,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "Security Engineer -- Inbound Follow-Up",
        "tags": ["security-engineer", "inbound"],
        "description": "Follow-up sequence for inbound leads from security engineering personas.",
        "tier": "red",
        "num_steps": 5,
        "prospect_count": 100,
        "cohort_size": 25,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    # -- New 8 (IDs 1006-1013) --
    {
        "name": "CISO -- Threat Detection ROI",
        "tags": ["ciso", "threat-detection", "roi"],
        "description": "High-volume CISO sequence. Quantifies detection gap costs and ROI of faster response times. Value-add research hook at step 3.",
        "tier": "green",
        "num_steps": 5,
        "prospect_count": 400,
        "value_add_step": 3,
        "cohort_size": 58,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "VP Marketing -- Pipeline Attribution",
        "tags": ["vp-marketing", "attribution", "pipeline"],
        "description": "Outbound targeting VP Marketing on pipeline attribution and marketing ROI measurement.",
        "tier": "green",
        "num_steps": 6,
        "value_add_step": 3,
        "prospect_count": 100,
        "cohort_size": 28,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "CFO -- Security ROI Framework",
        "tags": ["cfo", "security-roi", "finance"],
        "description": "Finance-angle outreach to CFOs quantifying security investment returns and risk reduction.",
        "tier": "yellow",
        "num_steps": 5,
        "social_proof_step": 3,
        "prospect_count": 100,
        "cohort_size": 28,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "IT Director -- Cloud Security Posture",
        "tags": ["it-director", "cloud-security", "posture"],
        "description": "Cold outreach to IT Directors managing cloud security posture and tool consolidation.",
        "tier": "yellow",
        "num_steps": 5,
        "prospect_count": 100,
        "cohort_size": 28,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "IT Director -- Patch Cycle Acceleration",
        "tags": ["it-director", "patch-management", "vulnerability"],
        "description": "7-step sequence targeting IT Directors struggling with patch cycle backlog and vulnerability exposure windows.",
        "tier": "yellow",
        "num_steps": 7,
        "prospect_count": 400,
        "cohort_size": 55,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "CFO -- Budget Freeze Outbound",
        "tags": ["cfo", "budget", "cost-reduction"],
        "description": "CFO-targeted outreach during budget cycles. High reply signal, zero meeting conversion (tests health gate).",
        "tier": "yellow",
        "num_steps": 5,
        "prospect_count": 350,
        "synthetic_only": True,
        "cohort_size": 48,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "VP Marketing -- Brand Risk Outbound",
        "tags": ["vp-marketing", "brand-risk", "compliance"],
        "description": "Brand safety and compliance risk angle for VP Marketing at SaaS and media companies.",
        "tier": "yellow",
        "num_steps": 4,
        "prospect_count": 100,
        "cohort_size": 28,
        "active_start_week": 0,
        "active_end_week": 51,
    },
    {
        "name": "CISO -- Board Risk Reporting",
        "tags": ["ciso", "board-reporting", "risk"],
        "description": "High-volume CISO sequence focused on board-level risk reporting and executive communication.",
        "tier": "green",
        "num_steps": 6,
        "prospect_count": 400,
        "social_proof_step": 3,
        "cohort_size": 55,
        "active_start_week": 0,
        "active_end_week": 51,
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso_dt(dt):
    """Format a datetime as ISO 8601 with Z suffix."""
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _week_start_date(week_index):
    """Return the Monday of the given week index as a date."""
    return DATA_START + timedelta(weeks=week_index)


def _render_template(tmpl, prospect, rep_name, seed):
    """Render an email template deterministically using seed for choices."""
    rng = random.Random(seed)
    return {
        "subject": tmpl["subject"].format(
            topic=rng.choice(TOPICS),
            company_name=prospect["company"],
            first_name=prospect["first_name"],
            pain_point=rng.choice(PAIN_POINTS),
            similar_company=rng.choice(SIMILAR_COS),
            persona=rng.choice(PERSONAS),
            asset_type=rng.choice(ASSET_TYPES),
            rep_name=rep_name,
        ),
        "body": tmpl["body"].format(
            topic=rng.choice(TOPICS),
            company_name=prospect["company"],
            trigger=rng.choice(TRIGGERS),
            first_name=prospect["first_name"],
            persona=rng.choice(PERSONAS),
            pain_point=rng.choice(PAIN_POINTS),
            similar_company=rng.choice(SIMILAR_COS),
            asset_type=rng.choice(ASSET_TYPES),
            rep_name=rep_name,
        ),
    }


def _generate_extra_prospects(start_id, count, tag):
    """Synthetic prospects not in CONTACT_POOL -- no Salesforce match."""
    local_rng = random.Random(start_id)
    prospects = []
    for i in range(count):
        fn = local_rng.choice(_EXTRA_FIRST_NAMES)
        ln = local_rng.choice(_EXTRA_LAST_NAMES)
        prospects.append({
            "id": start_id + i,
            "first_name": fn,
            "last_name": ln,
            "email": f"{fn.lower()}.{ln.lower()}.{tag}.{i:04d}@synth.mock",
            "title": local_rng.choice(_EXTRA_TITLES),
            "company": local_rng.choice(_EXTRA_COMPANIES),
        })
    return prospects


def _mailing_state(opened, clicked, replied, bounced):
    if bounced:
        return "bounced"
    if replied:
        return "replied"
    if clicked or opened:
        return "opened"
    return "delivered"


def _get_step_type(order, num_steps, cfg, seq_id):
    """Determine step type. First, last, and intent-override steps are email."""
    is_intent = (
        cfg.get("value_add_step") == order
        or cfg.get("social_proof_step") == order
        or cfg.get("multichannel_step") == order
    )
    if order == 1 or order == num_steps or is_intent:
        return "auto_email"
    # Deterministic type assignment based on sequence ID + order (no hash())
    rng = random.Random(GLOBAL_SEED + seq_id * 131 + order * 31)
    return rng.choices(
        ["auto_email", "manual_email", "call"], weights=[0.50, 0.20, 0.30]
    )[0]


def _get_step_template(order, cfg):
    """Select the email template for a step."""
    va_step = cfg.get("value_add_step")
    sp_step = cfg.get("social_proof_step")
    mc_step = cfg.get("multichannel_step")
    if va_step and order == va_step:
        return VALUE_ADD_TEMPLATE
    if sp_step and order == sp_step:
        return SOCIAL_PROOF_TEMPLATE
    if mc_step and order == mc_step:
        return MULTICHANNEL_EMAIL_TEMPLATE
    return EMAIL_TEMPLATES[min(order - 1, len(EMAIL_TEMPLATES) - 1)]


def _base_rates(step_order, tier, seed):
    """Compute base open/click/reply rates for a step, deterministically."""
    rng = random.Random(seed)
    decay = max(0.6, 1.0 - (step_order - 1) * 0.06)
    open_r = rng.uniform(0.20, 0.42) * decay
    click_r = rng.uniform(0.010, 0.035) * decay

    if tier == "green":
        reply_r = rng.uniform(0.11, 0.17) * decay
    elif tier == "red":
        reply_r = rng.uniform(0.020, 0.045) * decay
    else:  # yellow
        reply_r = rng.uniform(0.075, 0.115) * decay

    return {
        "open_rate": round(open_r, 4),
        "click_rate": round(click_r, 4),
        "reply_rate": round(reply_r, 4),
    }


# ---------------------------------------------------------------------------
# Core generation
# ---------------------------------------------------------------------------

def _build_step_definitions(seq_id, cfg):
    """Build the list of step definitions for a sequence."""
    num_steps = cfg["num_steps"]
    steps = []
    for order in range(1, num_steps + 1):
        stype = _get_step_type(order, num_steps, cfg, seq_id)
        is_email = stype in ("auto_email", "manual_email")

        # Pick a representative rep for the step record (schema compat)
        rep_rng = random.Random(GLOBAL_SEED + seq_id * 100 + order)
        rep = rep_rng.choice(SDR_LIST)

        if is_email:
            tmpl = _get_step_template(order, cfg)
            rendered = _render_template(
                tmpl, POOL_PROSPECTS[0], rep["name"],
                seed=GLOBAL_SEED + seq_id * 1000 + order
            )
            subject = rendered["subject"]
            body = rendered["body"]
        else:
            call_tmpl = CALL_TEMPLATES[min(order - 1, len(CALL_TEMPLATES) - 1)]
            call_rng = random.Random(GLOBAL_SEED + seq_id * 1000 + order)
            subject = None
            body = call_tmpl["body"].format(
                pain_point=call_rng.choice(PAIN_POINTS),
            )

        steps.append({
            "order": order,
            "step_type": stype,
            "is_email": is_email,
            "subject": subject,
            "body_text": body,
            "body_html": f"<p>{body}</p>" if body else None,
            "rep_id": rep["id"],
            "rep_name": rep["name"],
            "tier": cfg["tier"],
            "interval_days": order * 3,
        })
    return steps


def _select_cohort(seq_id, week_index, cfg, pool_prospects, overflow_prospects):
    """Select a cohort of prospects for this sequence + week."""
    cohort_size = cfg["cohort_size"]
    synthetic_only = cfg.get("synthetic_only", False)
    rng = random.Random(GLOBAL_SEED + seq_id * 10000 + week_index)

    if synthetic_only:
        return rng.sample(overflow_prospects, min(cohort_size, len(overflow_prospects)))

    prospect_count = cfg.get("prospect_count", 100)
    if prospect_count > 100:
        # High-volume: ~40% from pool, ~60% overflow
        pool_count = max(1, round(cohort_size * 0.40))
        overflow_count = cohort_size - pool_count
        pool_pick = rng.sample(pool_prospects, min(pool_count, len(pool_prospects)))
        overflow_pick = rng.sample(
            overflow_prospects, min(overflow_count, len(overflow_prospects))
        )
        return pool_pick + overflow_pick
    else:
        return rng.sample(pool_prospects, min(cohort_size, len(pool_prospects)))


def _assign_sdr(week_index, prospect_index):
    """Assign an SDR to a mailing. sdr_6 only from week 23 onward."""
    if week_index < NEW_HIRE_WEEK:
        active_sdrs = SDR_IDS[:5]  # sdr_1 through sdr_5
    else:
        active_sdrs = SDR_IDS[:6]  # all 6
    idx = (week_index + prospect_index) % len(active_sdrs)
    return active_sdrs[idx]


def generate_outreach_data():
    """Generate all Outreach data: mailings, sequences, steps."""
    all_mailings = []
    mailing_id = 200000

    # Pre-build step definitions and prospect pools per sequence
    seq_step_defs = {}
    seq_prospects = {}

    for i, cfg in enumerate(SEQUENCE_CONFIGS):
        seq_id = 1001 + i
        step_defs = _build_step_definitions(seq_id, cfg)
        seq_step_defs[seq_id] = step_defs

        prospect_count = cfg.get("prospect_count", 100)
        synthetic_only = cfg.get("synthetic_only", False)
        tag = f"or{seq_id}"
        extra_start_id = 50000 + (seq_id - 1001) * 500

        if synthetic_only:
            pool = []
            overflow = _generate_extra_prospects(extra_start_id, prospect_count, tag)
        elif prospect_count > 100:
            pool = list(POOL_PROSPECTS)
            overflow = _generate_extra_prospects(
                extra_start_id, prospect_count - 100, tag
            )
        else:
            pool = list(POOL_PROSPECTS)
            overflow = []

        seq_prospects[seq_id] = (pool, overflow)

    # Aggregate counters for sequences and steps
    seq_agg = {}
    step_agg = {}

    # Assign step IDs
    step_id_counter = 5000
    step_id_map = {}  # (seq_id, order) -> step_id
    for i, cfg in enumerate(SEQUENCE_CONFIGS):
        seq_id = 1001 + i
        seq_agg[seq_id] = {"deliver": 0, "open": 0, "click": 0, "reply": 0, "bounce": 0}
        for step_def in seq_step_defs[seq_id]:
            step_id_counter += 1
            sid = step_id_counter
            step_id_map[(seq_id, step_def["order"])] = sid
            step_agg[sid] = {
                "deliver": 0, "open": 0, "click": 0, "reply": 0,
                "bounce": 0, "opt_out": 0,
            }

    # Generate mailings: for each sequence, for each week, for each cohort prospect
    for i, cfg in enumerate(SEQUENCE_CONFIGS):
        seq_id = 1001 + i
        start_week = cfg["active_start_week"]
        end_week = cfg["active_end_week"]
        step_defs = seq_step_defs[seq_id]
        email_steps = [s for s in step_defs if s["is_email"]]
        pool, overflow = seq_prospects[seq_id]

        for week_index in range(start_week, end_week + 1):
            cohort = _select_cohort(seq_id, week_index, cfg, pool, overflow)
            week_start = _week_start_date(week_index)

            for p_idx, prospect in enumerate(cohort):
                sdr_id = _assign_sdr(week_index, p_idx)
                sdr = SDR_MAP[sdr_id]

                for step_def in email_steps:
                    order = step_def["order"]
                    step_sid = step_id_map[(seq_id, order)]

                    # Attrition: not every prospect reaches every step
                    attrition_factor = max(0.40, 1.0 - (order - 1) * 0.12)
                    attrition_rng = random.Random(
                        GLOBAL_SEED + seq_id * 100000 + week_index * 1000
                        + prospect["id"] * 10 + order
                    )
                    if attrition_rng.random() > attrition_factor:
                        continue

                    # Get rep multiplier
                    mult = get_rep_multiplier(sdr_id, week_index, order, seq_id)

                    # Skip if multiplier is 0 (sdr_6 before hire)
                    if mult == 0.0:
                        continue

                    # Base rates (seeded per sequence+step+week for consistency)
                    rate_seed = GLOBAL_SEED + seq_id * 1000 + order * 100 + week_index
                    rates = _base_rates(order, cfg["tier"], rate_seed)

                    # Apply multiplier to reply rate only
                    effective_reply = max(0.0, min(0.5, rates["reply_rate"] * mult))
                    open_rate = rates["open_rate"]
                    click_rate = rates["click_rate"]

                    # Deterministic outcome per prospect
                    outcome_seed = (
                        GLOBAL_SEED + seq_id * 100000 + step_sid * 1000
                        + week_index * 100 + prospect["id"]
                    )
                    outcome_rng = random.Random(outcome_seed)
                    outcome_val = outcome_rng.random()

                    # Bounce rate ~1.8%
                    bounced = outcome_val < 0.018
                    opened = False
                    clicked = False
                    replied = False

                    if not bounced:
                        opened = outcome_val < (0.018 + open_rate)
                        if opened:
                            # Click and reply drawn from separate thresholds
                            click_val = outcome_rng.random()
                            clicked = click_val < click_rate
                            reply_val = outcome_rng.random()
                            replied = reply_val < effective_reply

                    # Timestamps
                    ts_rng = random.Random(outcome_seed + 7)
                    day_offset = ts_rng.randint(0, 6)  # within the week
                    hour = ts_rng.randint(7, 17)
                    minute = ts_rng.randint(0, 59)
                    step_day_offset = (order - 1) * ts_rng.randint(2, 4)

                    scheduled_dt = datetime(
                        week_start.year, week_start.month, week_start.day,
                        hour, minute, 0
                    ) + timedelta(days=day_offset + step_day_offset)

                    # Clamp to DATA_END
                    data_end_dt = datetime(
                        DATA_END.year, DATA_END.month, DATA_END.day, 23, 59, 59
                    )
                    if scheduled_dt > data_end_dt:
                        continue

                    delivered_at = (
                        scheduled_dt + timedelta(minutes=ts_rng.randint(1, 5))
                        if not bounced else None
                    )
                    opened_at = (
                        delivered_at + timedelta(hours=ts_rng.randint(1, 48))
                        if opened and delivered_at else None
                    )
                    clicked_at = (
                        opened_at + timedelta(minutes=ts_rng.randint(2, 120))
                        if clicked and opened_at else None
                    )
                    replied_at = (
                        opened_at + timedelta(hours=ts_rng.randint(1, 24))
                        if replied and opened_at else None
                    )
                    bounced_at = (
                        scheduled_dt + timedelta(minutes=ts_rng.randint(5, 60))
                        if bounced else None
                    )
                    last_event = (
                        replied_at or clicked_at or opened_at
                        or bounced_at or delivered_at or scheduled_dt
                    )

                    # Template version for rewrite split
                    template_version = None
                    if seq_id == DEGRADING_SEQUENCE_ID and order == REWRITE_STEP_ORDER:
                        if week_index < REWRITE_WEEK:
                            template_version = "v1"
                        elif sdr_id in ("sdr_3", "sdr_6"):
                            template_version = "v2"
                        else:
                            template_version = "v1"

                    # Select template content
                    if template_version == "v2":
                        tmpl = REWRITE_V2_TEMPLATE
                    else:
                        tmpl = _get_step_template(order, cfg)

                    rendered = _render_template(
                        tmpl, prospect, sdr["name"],
                        seed=GLOBAL_SEED + seq_id * 1000 + order * 50 + week_index
                    )

                    mailing_id += 1

                    # Update aggregates
                    seq_agg[seq_id]["deliver"] += 1 if delivered_at else 0
                    seq_agg[seq_id]["open"] += 1 if opened else 0
                    seq_agg[seq_id]["click"] += 1 if clicked else 0
                    seq_agg[seq_id]["reply"] += 1 if replied else 0
                    seq_agg[seq_id]["bounce"] += 1 if bounced else 0

                    step_agg[step_sid]["deliver"] += 1 if delivered_at else 0
                    step_agg[step_sid]["open"] += 1 if opened else 0
                    step_agg[step_sid]["click"] += 1 if clicked else 0
                    step_agg[step_sid]["reply"] += 1 if replied else 0
                    step_agg[step_sid]["bounce"] += 1 if bounced else 0

                    mailing = {
                        "id": mailing_id,
                        "type": "mailing",
                        "attributes": {
                            "subject": rendered["subject"],
                            "bodyHtml": f"<p>{rendered['body']}</p>",
                            "bodyText": rendered["body"],
                            "state": _mailing_state(opened, clicked, replied, bounced),
                            "trackOpens": True,
                            "trackLinks": True,
                            "openCount": 1 if opened else 0,
                            "clickCount": 1 if clicked else 0,
                            "scheduledAt": _iso_dt(scheduled_dt),
                            "deliveredAt": _iso_dt(delivered_at) if delivered_at else None,
                            "openedAt": _iso_dt(opened_at) if opened_at else None,
                            "clickedAt": _iso_dt(clicked_at) if clicked_at else None,
                            "repliedAt": _iso_dt(replied_at) if replied_at else None,
                            "bouncedAt": _iso_dt(bounced_at) if bounced_at else None,
                            "stateChangedAt": _iso_dt(last_event),
                            "createdAt": _iso_dt(scheduled_dt),
                            "updatedAt": _iso_dt(last_event),
                            "errorReason": None,
                            "errorBacktrace": None,
                            "retryCount": 0,
                            "retryAt": None,
                            "retryInterval": None,
                            "followUpTaskType": (
                                "follow_up" if not replied and not bounced else None
                            ),
                            "followUpTaskScheduledAt": None,
                            "overrideSafetySettings": False,
                            "references": [],
                            "unsubscribedAt": None,
                            "_prospect_id": prospect["id"],
                            "_prospect_email": prospect["email"],
                            "_prospect_first_name": prospect["first_name"],
                            "_prospect_last_name": prospect["last_name"],
                            "_prospect_title": prospect["title"],
                            "_prospect_company": prospect["company"],
                            "_rep_id": sdr_id,
                            "_week_index": week_index,
                        },
                        "relationships": {
                            "sequenceStep": {
                                "data": {"id": step_sid, "type": "sequenceStep"}
                            },
                            "sequence": {
                                "data": {"id": seq_id, "type": "sequence"}
                            },
                            "prospect": {
                                "data": {"id": prospect["id"], "type": "prospect"}
                            },
                        },
                    }

                    # Add template version for rewrite step
                    if template_version is not None:
                        mailing["attributes"]["_template_version"] = template_version

                    all_mailings.append(mailing)

    # Build sequence records
    sequences = []
    for i, cfg in enumerate(SEQUENCE_CONFIGS):
        seq_id = 1001 + i
        agg = seq_agg[seq_id]
        # Use a deterministic created/updated timestamp
        created_rng = random.Random(GLOBAL_SEED + seq_id)
        created_days_back = created_rng.randint(60, 365)
        created = datetime(2026, 3, 31, 12, 0, 0) - timedelta(days=created_days_back)
        updated = created + timedelta(days=created_rng.randint(1, 30))

        sequences.append({
            "id": seq_id,
            "type": "sequence",
            "_tier": cfg["tier"],
            "_prospect_count": cfg.get("prospect_count", 100),
            "_synthetic_only": cfg.get("synthetic_only", False),
            "_value_add_step": cfg.get("value_add_step"),
            "_social_proof_step": cfg.get("social_proof_step"),
            "_multichannel_step": cfg.get("multichannel_step"),
            "attributes": {
                "name": cfg["name"],
                "description": cfg["description"],
                "enabled": True,
                "enabledAt": _iso_dt(created + timedelta(days=1)),
                "sequenceType": "interval",
                "scheduleIntervalType": "schedule",
                "shareType": "shared",
                "tags": cfg["tags"],
                "sequenceStepCount": cfg["num_steps"],
                "durationInDays": cfg["num_steps"] * 3,
                "automationPercentage": round(
                    random.Random(GLOBAL_SEED + seq_id + 99).uniform(0.6, 1.0), 2
                ),
                "deliverCount": agg["deliver"],
                "openCount": agg["open"],
                "clickCount": agg["click"],
                "replyCount": agg["reply"],
                "bounceCount": agg["bounce"],
                "optOutCount": None,
                "scheduleCount": 0,
                "primaryReplyAction": "finish",
                "secondaryReplyAction": "continue",
                "stepOverridesEnabled": False,
                "createdAt": _iso_dt(created),
                "updatedAt": _iso_dt(updated),
            },
        })

    # Build step records
    step_records = []
    for i, cfg in enumerate(SEQUENCE_CONFIGS):
        seq_id = 1001 + i
        step_defs = seq_step_defs[seq_id]
        # Use sequence created time as base for step created times
        seq_created = datetime.fromisoformat(
            sequences[i]["attributes"]["createdAt"].replace("Z", "+00:00")
        ).replace(tzinfo=None)

        for step_def in step_defs:
            order = step_def["order"]
            sid = step_id_map[(seq_id, order)]
            agg = step_agg[sid]
            is_email = step_def["is_email"]
            step_created = seq_created + timedelta(days=order)

            dc = agg["deliver"] if is_email else None
            oc = agg["open"] if is_email else None
            cc = agg["click"] if is_email else None
            rc = agg["reply"] if is_email else None
            bc = agg["bounce"] if is_email else None

            step_records.append({
                "id": sid,
                "type": "sequenceStep",
                "attributes": {
                    "order": order,
                    "displayName": f"Step {order} -- {step_def['step_type'].replace('_', ' ').title()}",
                    "stepType": step_def["step_type"],
                    "interval": step_def["interval_days"] * 86400,
                    "date": None,
                    "taskAutoskipDelay": None,
                    "deliverCount": dc,
                    "openCount": oc,
                    "clickCount": cc,
                    "replyCount": rc,
                    "bounceCount": bc,
                    "negativeReplyCount": int(rc * 0.10) if rc else None,
                    "neutralReplyCount": int(rc * 0.60) if rc else None,
                    "positiveReplyCount": int(rc * 0.30) if rc else None,
                    "optOutCount": int(dc * 0.005) if dc else None,
                    "failureCount": 0,
                    "scheduleCount": 0,
                    "subject": step_def["subject"],
                    "bodyHtml": step_def["body_html"],
                    "bodyText": step_def["body_text"],
                    "createdAt": _iso_dt(step_created),
                    "updatedAt": _iso_dt(step_created + timedelta(days=1)),
                    "_rep_id": step_def["rep_id"],
                    "_rep_name": step_def["rep_name"],
                    "_tier": step_def["tier"],
                },
                "relationships": {
                    "sequence": {"data": {"id": seq_id, "type": "sequence"}}
                },
            })

    return sequences, step_records, all_mailings


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------

def generate_outreach_files(output_dir=None):
    """Generate and write all Outreach files. Returns record counts."""
    if output_dir is None:
        output_dir = OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    sequences, steps, mailings = generate_outreach_data()

    files = {
        "sequences.json": sequences,
        "sequence_steps.json": steps,
        "email_activity.json": mailings,
    }
    for fname, data in files.items():
        path = os.path.join(output_dir, fname)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    _print_validation(sequences, steps, mailings)

    return {fname: len(data) for fname, data in files.items()}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _print_validation(sequences, steps, mailings):
    """Print comprehensive validation stats."""
    print("\n" + "=" * 70)
    print("Outreach Validation Summary")
    print("=" * 70)

    # 1. Record counts
    print(f"\nRecord counts:")
    print(f"  Sequences:     {len(sequences)}")
    print(f"  Steps:         {len(steps)}")
    print(f"  Mailings:      {len(mailings)}")

    email_steps = [s for s in steps if s["attributes"]["stepType"] in ("auto_email", "manual_email")]
    call_steps = [s for s in steps if s["attributes"]["stepType"] == "call"]
    print(f"  Email steps:   {len(email_steps)}")
    print(f"  Call steps:    {len(call_steps)}")

    # 2. Overall rates
    total_sends = len(mailings)
    total_opens = sum(1 for m in mailings if m["attributes"]["openedAt"])
    total_replies = sum(1 for m in mailings if m["attributes"]["repliedAt"])
    total_clicks = sum(1 for m in mailings if m["attributes"]["clickedAt"])
    total_bounces = sum(1 for m in mailings if m["attributes"]["bouncedAt"])

    print(f"\nOverall rates:")
    print(f"  Open rate:   {total_opens/total_sends:.1%}  (target ~27%)")
    print(f"  Reply rate:  {total_replies/total_sends:.1%}  (target ~2.9%)")
    print(f"  Click rate:  {total_clicks/total_sends:.1%}")
    print(f"  Bounce rate: {total_bounces/total_sends:.1%}")

    # 3. Per-rep annual reply rates
    rep_stats = defaultdict(lambda: {"sends": 0, "replies": 0, "opens": 0})
    for m in mailings:
        rid = m["attributes"]["_rep_id"]
        rep_stats[rid]["sends"] += 1
        if m["attributes"]["repliedAt"]:
            rep_stats[rid]["replies"] += 1
        if m["attributes"]["openedAt"]:
            rep_stats[rid]["opens"] += 1

    print(f"\nPer-rep annual reply rates:")
    sdr_names = {s["id"]: s["name"] for s in SDR_LIST}
    for sdr_id in SDR_IDS:
        st = rep_stats[sdr_id]
        if st["sends"] > 0:
            rr = st["replies"] / st["sends"]
            print(f"  {sdr_id} ({sdr_names[sdr_id]:<20s}): {rr:.1%}  ({st['sends']:>6} sends, {st['replies']:>4} replies)")
        else:
            print(f"  {sdr_id} ({sdr_names[sdr_id]:<20s}): no sends")

    # 4. Per-quarter reply rates by rep
    # Q2'25 = weeks 0-12, Q3'25 = weeks 13-25, Q4'25 = weeks 26-38, Q1'26 = weeks 39-51
    quarters = [
        ("Q2'25", 0, 12),
        ("Q3'25", 13, 25),
        ("Q4'25", 26, 38),
        ("Q1'26", 39, 51),
    ]
    q_stats = {
        sdr_id: {q[0]: {"sends": 0, "replies": 0} for q in quarters}
        for sdr_id in SDR_IDS
    }
    for m in mailings:
        rid = m["attributes"]["_rep_id"]
        wk = m["attributes"]["_week_index"]
        for qname, qstart, qend in quarters:
            if qstart <= wk <= qend:
                q_stats[rid][qname]["sends"] += 1
                if m["attributes"]["repliedAt"]:
                    q_stats[rid][qname]["replies"] += 1
                break

    print(f"\nPer-quarter reply rates:")
    header = f"  {'Rep':<28s}" + "".join(f"{q[0]:>10s}" for q in quarters)
    print(header)
    print("  " + "-" * (28 + 10 * len(quarters)))
    for sdr_id in SDR_IDS:
        row = f"  {sdr_id} ({sdr_names[sdr_id]:<20s})"
        for qname, _, _ in quarters:
            qs = q_stats[sdr_id][qname]
            if qs["sends"] > 0:
                rr = qs["replies"] / qs["sends"]
                row += f"{rr:>9.1%} "
            else:
                row += f"{'--':>9s} "
        print(row)

    # 5. Rewrite split validation
    print(f"\nRewrite split (seq 1001, step 3, after week {REWRITE_WEEK}):")
    rewrite_mailings = [
        m for m in mailings
        if m["relationships"]["sequence"]["data"]["id"] == DEGRADING_SEQUENCE_ID
        and m["attributes"].get("_template_version") is not None
        and m["attributes"]["_week_index"] >= REWRITE_WEEK
    ]
    v2_mailings = [m for m in rewrite_mailings if m["attributes"].get("_template_version") == "v2"]
    v1_mailings = [m for m in rewrite_mailings if m["attributes"].get("_template_version") == "v1"]

    if v2_mailings:
        v2_replies = sum(1 for m in v2_mailings if m["attributes"]["repliedAt"])
        print(f"  sdr_3/sdr_6 (v2): {v2_replies}/{len(v2_mailings)} = {v2_replies/len(v2_mailings):.1%} reply rate")
    if v1_mailings:
        v1_replies = sum(1 for m in v1_mailings if m["attributes"]["repliedAt"])
        print(f"  Other SDRs  (v1): {v1_replies}/{len(v1_mailings)} = {v1_replies/len(v1_mailings):.1%} reply rate")

    # 6. sdr_6 volume check
    sdr6_mailings = [m for m in mailings if m["attributes"]["_rep_id"] == "sdr_6"]
    sdr6_before_23 = [m for m in sdr6_mailings if m["attributes"]["_week_index"] < NEW_HIRE_WEEK]
    print(f"\nsdr_6 (Aisha) volume check:")
    print(f"  Total mailings: {len(sdr6_mailings)}")
    print(f"  Before week 23: {len(sdr6_before_23)} (should be 0)")

    # Per-sequence summary
    print(f"\nPer-sequence summary:")
    print(f"  {'SeqID':>6} {'Tier':>6} {'Sends':>7} {'RepR':>7}  Name")
    print("  " + "-" * 65)
    seq_mail_stats = defaultdict(lambda: {"sends": 0, "replies": 0})
    for m in mailings:
        sid = m["relationships"]["sequence"]["data"]["id"]
        seq_mail_stats[sid]["sends"] += 1
        if m["attributes"]["repliedAt"]:
            seq_mail_stats[sid]["replies"] += 1
    for seq in sequences:
        sid = seq["id"]
        ss = seq_mail_stats[sid]
        rr = ss["replies"] / ss["sends"] if ss["sends"] else 0
        print(f"  {sid:>6} {seq['_tier']:>6} {ss['sends']:>7} {rr:>6.1%}  {seq['attributes']['name']}")

    print("=" * 70)
