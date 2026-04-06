"""
generators/salesforce.py
Generates expanded Salesforce CRM data with realistic deal timelines,
narrative arcs, and coaching-quality task detail.

Outputs 5 CSV files to output/:
  sf_accounts.csv, sf_contacts.csv, sf_opportunities.csv,
  sf_opportunity_contact_roles.csv, sf_tasks.csv

Attribution chain: contact_pool email -> sequencer reply -> SF contact ->
OpportunityContactRole ->opportunity (AE-owned).
"""

import csv
import json
import os
import random
from collections import defaultdict
from datetime import date, timedelta

from .config import DATA_START, DATA_END, DEMO_TODAY, GLOBAL_SEED
from .contact_pool import CONTACT_POOL, _COMPANIES
from .account_enrichment import _ENRICHMENT
from .org_structure import (
    AES_SMB_MM, AES_ENT_STRAT, SDR_AE_PAIRS, SDRS,
)
from .narrative_arcs import (
    HANDOFF_OPP_DELAY_DAYS,
    ARC_9_DEAL, ARC_10_DEALS, ARC_11_COMPETITIVE,
    ARC_12_SANDBAGGING, ARC_17_ENT_STALL,
)

# ── Seed ──────────────────────────────────────────────────────────────────────

_rng = random.Random(GLOBAL_SEED)

# ── Roster helpers ────────────────────────────────────────────────────────────

ALL_AES = AES_SMB_MM + AES_ENT_STRAT
AE_MAP = {ae["id"]: ae for ae in ALL_AES}
SDR_MAP = {s["id"]: s for s in SDRS}

# Map AE segment to deal parameters: (min_opps, max_opps, min_amt, max_amt, min_cycle, max_cycle)
# Per-AE opp targets by segment. Arc deals fill some of these, baseline fills the rest.
# SMB 4 AEs * ~17 = 68, MM 2 * ~15 = 30, ENT 2 * ~10 = 20, STRAT 2 * ~7 = 14
# Total: ~132 baseline + ~30 arc deals >= 160
SEGMENT_PARAMS = {
    "SMB":  (22, 26, 15_000, 80_000, 30, 60),
    "MM":   (16, 20, 50_000, 150_000, 60, 90),
    "ENT":  (10, 14, 100_000, 300_000, 90, 150),
    "STRAT": (7, 9, 200_000, 500_000, 120, 180),
}

STAGES_ORDERED = [
    "Prospecting", "Qualification", "Needs Analysis",
    "Proposal/Price Quote", "Negotiation", "Closed Won", "Closed Lost",
]
OPEN_STAGES = set(STAGES_ORDERED[:5])

# Typical days spent in each stage (min, max) for normal deals
STAGE_DURATIONS = {
    "Prospecting":          (7, 14),
    "Qualification":        (10, 20),
    "Needs Analysis":       (14, 25),
    "Proposal/Price Quote": (10, 20),
    "Negotiation":          (14, 30),
}

OCR_ROLES = ["Decision Maker", "Economic Buyer", "Influencer", "End User",
             "Technical Evaluator", "Executive Sponsor", "Champion"]

INBOUND_SOURCES = ["Inbound", "Web", "Partner Referral", "Event"]

US_CITIES = [
    "San Francisco", "New York", "Austin", "Chicago", "Seattle",
    "Boston", "Denver", "Atlanta", "Dallas", "Los Angeles",
    "Washington DC", "Minneapolis", "Phoenix", "Charlotte", "Portland",
]

# ── Company-to-territory mapping ─────────────────────────────────────────────

COMPANY_TERRITORY = {}
COMPANY_BY_TERRITORY = defaultdict(list)
for idx, co in enumerate(_COMPANIES):
    territory = _ENRICHMENT[idx]["territory"]
    COMPANY_TERRITORY[co["name"]] = territory
    COMPANY_BY_TERRITORY[territory].append(idx)

# Build company index ->account enrichment data
COMPANY_ENRICHMENT = {idx: _ENRICHMENT[idx] for idx in range(len(_COMPANIES))}

# Contacts grouped by company index
CONTACTS_BY_COMPANY = defaultdict(list)
for c in CONTACT_POOL:
    for idx, co in enumerate(_COMPANIES):
        if co["name"] == c["company"]:
            CONTACTS_BY_COMPANY[idx].append(c)
            break

# ── Use-case fragments for deal naming ───────────────────────────────────────

_USE_CASES = [
    "Enterprise Security Suite", "Cloud Posture Starter", "DevSecOps Platform",
    "Threat Detection Module", "Compliance Automation", "Endpoint Protection",
    "Security Posture Assessment", "Vulnerability Management", "Zero Trust Rollout",
    "API Security Gateway", "SIEM Integration", "Identity Governance",
    "Incident Response Retainer", "Pen Test Engagement", "Risk Dashboard",
    "Container Security", "Data Loss Prevention", "Cloud Workload Protection",
    "Security Awareness Training", "Managed Detection & Response",
    "SOC-as-a-Service", "Privileged Access Management", "Network Segmentation",
    "Threat Intelligence Feed", "Security Operations Center",
]

# ── Task subject/description templates ───────────────────────────────────────

_DISCOVERY_SUBJECTS = [
    "Discovery call with {contact}",
    "Intro call --{company} security posture",
    "Initial discovery --{contact} at {company}",
]
_DISCOVERY_DESCS = [
    "Discussed current security stack and pain points. {contact} confirmed budget cycle starts {quarter}.",
    "Explored compliance requirements. {contact} mentioned upcoming audit deadline.",
    "Walked through DOOM platform overview. Strong interest in {use_case}.",
]

_DEMO_SUBJECTS = [
    "Demo completed --{company}",
    "Platform demo for {contact} and team",
    "Technical demo --{use_case}",
]
_DEMO_DESCS = [
    "Ran full platform demo. {contact} engaged, asked about integration timeline.",
    "Demo well received. Follow-up requested on pricing and deployment model.",
    "Showed {use_case} capabilities. Team had detailed technical questions.",
]

_PROPOSAL_SUBJECTS = [
    "Sent pricing deck to {company}",
    "Proposal submitted --{company}",
    "Pricing discussion with {contact}",
]
_PROPOSAL_DESCS = [
    "Sent formal proposal at ${amount}. {contact} reviewing with procurement.",
    "Pricing deck delivered. Expecting feedback within 2 weeks.",
    "Walked through ROI framework. {contact} to present to leadership.",
]

_NEGOTIATION_SUBJECTS = [
    "Contract redline review --{company}",
    "Negotiation kickoff --pricing discussion",
    "Legal review --{company} MSA",
    "Security questionnaire follow-up",
]
_NEGOTIATION_DESCS = [
    "Received redlined MSA. Legal reviewing data residency clauses.",
    "Pricing discussion --{contact} pushing for volume discount.",
    "Addressed security questionnaire items. Awaiting final legal sign-off.",
    "Negotiating payment terms. Champion {contact} aligned on value.",
]

_CLOSE_SUBJECTS = [
    "Contract signed --{company}",
    "Deal closed won --{company}",
    "Implementation kickoff scheduled --{company}",
]
_CLOSE_WON_DESCS = [
    "Contract signed. Implementation kickoff scheduled for next week.",
    "Deal closed. {contact} confirmed as primary point of contact for onboarding.",
    "Won deal. {contact} excited about {use_case} rollout.",
]
_CLOSE_LOST_DESCS = [
    "Deal closed lost --no response from champion.",
    "Lost to competitor. {contact} cited pricing as primary factor.",
    "Deal stalled and closed lost. Budget redirected to other priorities.",
    "Champion left the company. New leadership chose different direction.",
]

_FOLLOWUP_SUBJECTS = [
    "Follow-up email to {contact}",
    "Left voicemail for {contact}",
    "Check-in with {contact} --{company}",
    "Sent case study to {contact}",
]
_FOLLOWUP_DESCS = [
    "Sent follow-up email. No response to last two outreach attempts.",
    "Left voicemail. {contact} has been unresponsive for {gap_days} days.",
    "Quick check-in. {contact} mentioned team is still evaluating options.",
    "Shared {industry} case study. {contact} forwarded to internal stakeholders.",
]

_MEETING_SUBJECTS = [
    "Executive sponsor intro --CTO alignment",
    "Multi-stakeholder review --{company}",
    "QBR prep meeting with {contact}",
    "Technical validation session --{company}",
    "Procurement alignment call",
]
_MEETING_DESCS = [
    "Met with CTO and {contact}. Executive buy-in confirmed.",
    "Multi-stakeholder session with {n_attendees} attendees. Good alignment on requirements.",
    "Reviewed success metrics. {contact} confirmed path to expansion.",
    "Technical validation complete. No blockers identified.",
    "Procurement call. Discussed payment terms and contract timeline.",
]

_COMPETITIVE_SUBJECTS = [
    "Competitive comparison --{competitor} vs DOOM",
    "Champion mentioned evaluating {competitor}",
    "Competitive intel --{company} shortlist",
    "Win/loss debrief --{competitor} factor",
]
_COMPETITIVE_DESCS = [
    "Competitive eval in progress --prospect comparing us to {competitor} on response time SLA.",
    "{contact} shared that {competitor} is also in evaluation. We differentiate on platform breadth.",
    "Created battle card positioning vs {competitor}. Sent to {contact} for internal circulation.",
    "Debrief with {contact}. {competitor} undercut on price but lacked integration depth.",
]


def _fmt(template, **kwargs):
    """Safe format --missing keys become empty strings."""
    for k in ["contact", "company", "use_case", "amount", "quarter",
              "gap_days", "industry", "competitor", "n_attendees"]:
        kwargs.setdefault(k, "")
    return template.format(**kwargs)


# ── Date helpers ─────────────────────────────────────────────────────────────

def _add_biz_days(start, n):
    """Add n business days to a date."""
    current = start
    added = 0
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def _random_date_in_range(start, end):
    """Random date between start and end (inclusive)."""
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=_rng.randint(0, delta))


def _date_from_str(s):
    """Parse YYYY-MM-DD string to date."""
    parts = s.split("-")
    return date(int(parts[0]), int(parts[1]), int(parts[2]))


def _stage_at_date(created_date, cycle_days, as_of=DEMO_TODAY,
                   close_won=None, close_lost=None):
    """Determine what stage a deal is in as of as_of date.

    Returns (stage_name, close_date_if_closed).
    """
    if close_won:
        if as_of >= close_won:
            return "Closed Won", close_won
    if close_lost:
        if as_of >= close_lost:
            return "Closed Lost", close_lost

    elapsed = (as_of - created_date).days
    if elapsed < 0:
        return "Prospecting", None

    # Divide cycle into 5 open stages proportionally
    stage_fractions = [0.15, 0.20, 0.25, 0.20, 0.20]
    cumulative = 0
    for i, frac in enumerate(stage_fractions):
        cumulative += frac * cycle_days
        if elapsed < cumulative:
            return STAGES_ORDERED[i], None

    # Past the full cycle --still in Negotiation (deal hasn't closed yet)
    return "Negotiation", None


# ── ID counters ──────────────────────────────────────────────────────────────

class _IdGen:
    def __init__(self):
        self.acc = 0
        self.opp = 0
        self.ocr = 0
        self.tsk = 0

    def next_acc(self):
        self.acc += 1
        return f"sf_acc_{self.acc:03d}"

    def next_opp(self):
        self.opp += 1
        return f"sf_opp_{self.opp:03d}"

    def next_ocr(self):
        self.ocr += 1
        return f"sf_ocr_{self.ocr:04d}"

    def next_tsk(self):
        self.tsk += 1
        return f"sf_tsk_{self.tsk:04d}"


# ══════════════════════════════════════════════════════════════════════════════
# MAIN GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def generate_salesforce_files(output_dir):
    """Generate all Salesforce CSV files. Returns dict of filename->count."""
    _rng.seed(GLOBAL_SEED)
    ids = _IdGen()

    # ── Load email_activity.json for Arc 8 handoff ────────────────────────
    ea_path = os.path.join(output_dir, "email_activity.json")
    seq_path = os.path.join(output_dir, "sequences.json")

    replies_by_sdr = defaultdict(list)
    if os.path.exists(ea_path):
        with open(ea_path, encoding="utf-8") as f:
            for m in json.load(f):
                a = m["attributes"]
                if a.get("repliedAt") and not a["_prospect_email"].endswith("@synth.mock"):
                    replies_by_sdr[a["_rep_id"]].append({
                        "email": a["_prospect_email"],
                        "company": a["_prospect_company"],
                        "replied_at": a["repliedAt"][:10],
                        "seq_id": m["relationships"]["sequence"]["data"]["id"],
                    })

    seq_names = {}
    if os.path.exists(seq_path):
        with open(seq_path, encoding="utf-8") as f:
            for s in json.load(f):
                seq_names[s["id"]] = s["attributes"]["name"]

    # ── Build accounts ────────────────────────────────────────────────────

    accounts = []
    acc_id_map = {}  # company_name ->sf_acc_id
    for idx, co in enumerate(_COMPANIES):
        acc_id = ids.next_acc()
        enrichment = _ENRICHMENT[idx]
        accounts.append({
            "Id": acc_id,
            "Name": co["name"],
            "Industry": co["industry"],
            "NumberOfEmployees": enrichment["employee_count_current"],
            "BillingCity": _rng.choice(US_CITIES),
        })
        acc_id_map[co["name"]] = acc_id

    # ── Build contacts ────────────────────────────────────────────────────

    contacts = []
    contact_id_map = {}  # email ->sf_con_id
    for i, c in enumerate(CONTACT_POOL):
        con_id = f"sf_con_{i + 1:03d}"
        contacts.append({
            "Id": con_id,
            "FirstName": c["first_name"],
            "LastName": c["last_name"],
            "Email": c["email"],
            "Title": c["title"],
            "AccountId": acc_id_map[c["company"]],
            "Industry": c["industry"],
        })
        contact_id_map[c["email"]] = con_id

    # ── Build opportunities, OCRs, and tasks ─────────────────────────────

    opportunities = []
    ocrs = []
    tasks = []
    use_case_idx = 0

    def _next_use_case():
        nonlocal use_case_idx
        uc = _USE_CASES[use_case_idx % len(_USE_CASES)]
        use_case_idx += 1
        return uc

    def _pick_contacts_for_company(company_idx, n=1):
        """Pick n contacts from a company."""
        pool = CONTACTS_BY_COMPANY.get(company_idx, [])
        if not pool:
            return []
        return _rng.sample(pool, min(n, len(pool)))

    def _get_contact_id(email):
        return contact_id_map.get(email)

    def _get_account_id(company_name):
        return acc_id_map.get(company_name)

    # Helper: generate tasks for a deal lifecycle
    def _generate_deal_tasks(opp_id, ae_id, contact_list, company_name,
                             use_case, amount, stages_with_dates,
                             competitor=None, activity_gap=None):
        """Generate realistic tasks for a deal.

        stages_with_dates: list of (stage_name, entry_date) tuples
        activity_gap: optional (gap_start_date, gap_end_date) tuple
        competitor: optional competitor name for competitive arc
        """
        deal_tasks = []
        industry = COMPANY_TERRITORY.get(company_name, "technology")

        primary_contact = contact_list[0] if contact_list else None
        primary_name = f"{primary_contact['first_name']} {primary_contact['last_name']}" if primary_contact else "Contact"
        primary_email = primary_contact["email"] if primary_contact else None

        quarters = {1: "Q1", 2: "Q1", 3: "Q1", 4: "Q2", 5: "Q2", 6: "Q2",
                    7: "Q3", 8: "Q3", 9: "Q3", 10: "Q4", 11: "Q4", 12: "Q4"}

        for stage_idx, (stage, entry_date_str) in enumerate(stages_with_dates):
            entry_date = _date_from_str(entry_date_str) if isinstance(entry_date_str, str) else entry_date_str

            # Pick which contact for this task (multi-threaded = rotate)
            if len(contact_list) > 1:
                task_contact = contact_list[stage_idx % len(contact_list)]
            else:
                task_contact = primary_contact

            if not task_contact:
                continue

            task_contact_name = f"{task_contact['first_name']} {task_contact['last_name']}"
            task_contact_id = _get_contact_id(task_contact["email"])
            if not task_contact_id:
                continue

            q = quarters.get(entry_date.month, "Q3")

            # Check if this date falls in the activity gap
            if activity_gap:
                gap_start, gap_end = activity_gap
                if gap_start <= entry_date <= gap_end:
                    continue

            if stage == "Prospecting":
                subj = _rng.choice(_DISCOVERY_SUBJECTS)
                desc = _rng.choice(_DISCOVERY_DESCS)
                task_type = _rng.choice(["Call", "Meeting"])
            elif stage == "Qualification":
                subj = _rng.choice(_DISCOVERY_SUBJECTS + _DEMO_SUBJECTS[:1])
                desc = _rng.choice(_DISCOVERY_DESCS + _DEMO_DESCS[:1])
                task_type = "Call"
            elif stage == "Needs Analysis":
                subj = _rng.choice(_DEMO_SUBJECTS)
                desc = _rng.choice(_DEMO_DESCS)
                task_type = "Meeting"
            elif stage == "Proposal/Price Quote":
                subj = _rng.choice(_PROPOSAL_SUBJECTS)
                desc = _rng.choice(_PROPOSAL_DESCS)
                task_type = "Email"
            elif stage == "Negotiation":
                if competitor:
                    subj = _rng.choice(_COMPETITIVE_SUBJECTS + _NEGOTIATION_SUBJECTS[:1])
                    desc = _rng.choice(_COMPETITIVE_DESCS + _NEGOTIATION_DESCS[:1])
                else:
                    subj = _rng.choice(_NEGOTIATION_SUBJECTS)
                    desc = _rng.choice(_NEGOTIATION_DESCS)
                task_type = _rng.choice(["Call", "Email", "Meeting"])
            elif stage == "Closed Won":
                subj = _rng.choice(_CLOSE_SUBJECTS)
                desc = _rng.choice(_CLOSE_WON_DESCS)
                task_type = "Email"
            elif stage == "Closed Lost":
                subj = _rng.choice(["Deal closed lost --{company}", "Close-out --{company} deal lost"])
                desc = _rng.choice(_CLOSE_LOST_DESCS)
                task_type = "Email"
            else:
                subj = _rng.choice(_FOLLOWUP_SUBJECTS)
                desc = _rng.choice(_FOLLOWUP_DESCS)
                task_type = "Email"

            subj = _fmt(subj, contact=task_contact_name, company=company_name,
                        use_case=use_case, amount=str(amount), quarter=q,
                        competitor=competitor or "", industry=industry)
            desc = _fmt(desc, contact=task_contact_name, company=company_name,
                        use_case=use_case, amount=str(amount), quarter=q,
                        competitor=competitor or "", industry=industry,
                        gap_days="14", n_attendees=str(len(contact_list)))

            # Offset the activity date slightly from stage entry
            offset = _rng.randint(1, 5)
            activity_date = entry_date + timedelta(days=offset)
            if activity_date > DEMO_TODAY:
                activity_date = DEMO_TODAY

            deal_tasks.append({
                "Id": ids.next_tsk(),
                "WhoId": task_contact_id,
                "WhatId": opp_id,
                "Subject": subj,
                "Status": "Completed",
                "ActivityDate": activity_date.isoformat(),
                "Type": task_type,
                "Description": desc,
                "OwnerId": ae_id,
            })

        # Add extra follow-up tasks for longer deals
        if len(stages_with_dates) >= 2 and len(contact_list) > 0:
            n_extra = _rng.randint(2, 4)
            for _ in range(n_extra):
                first_date = _date_from_str(stages_with_dates[0][1]) if isinstance(stages_with_dates[0][1], str) else stages_with_dates[0][1]
                last_date = _date_from_str(stages_with_dates[-1][1]) if isinstance(stages_with_dates[-1][1], str) else stages_with_dates[-1][1]
                rand_date = _random_date_in_range(first_date, last_date)

                if activity_gap:
                    gap_start, gap_end = activity_gap
                    if gap_start <= rand_date <= gap_end:
                        continue

                tc = _rng.choice(contact_list)
                tc_name = f"{tc['first_name']} {tc['last_name']}"
                tc_id = _get_contact_id(tc["email"])
                if not tc_id:
                    continue

                if competitor and _rng.random() < 0.4:
                    subj = _fmt(_rng.choice(_COMPETITIVE_SUBJECTS),
                                contact=tc_name, company=company_name,
                                competitor=competitor)
                    desc = _fmt(_rng.choice(_COMPETITIVE_DESCS),
                                contact=tc_name, company=company_name,
                                competitor=competitor)
                else:
                    subj = _fmt(_rng.choice(_FOLLOWUP_SUBJECTS),
                                contact=tc_name, company=company_name,
                                use_case=use_case, industry=industry,
                                gap_days=str(_rng.randint(5, 21)))
                    desc = _fmt(_rng.choice(_FOLLOWUP_DESCS),
                                contact=tc_name, company=company_name,
                                use_case=use_case, industry=industry,
                                gap_days=str(_rng.randint(5, 21)))

                if rand_date > DEMO_TODAY:
                    rand_date = DEMO_TODAY

                deal_tasks.append({
                    "Id": ids.next_tsk(),
                    "WhoId": tc_id,
                    "WhatId": opp_id,
                    "Subject": subj,
                    "Status": "Completed",
                    "ActivityDate": rand_date.isoformat(),
                    "Type": _rng.choice(["Call", "Email", "Meeting"]),
                    "Description": desc,
                    "OwnerId": ae_id,
                })

        return deal_tasks

    # Helper: compute stage dates from created_date and cycle length
    def _compute_stage_dates(created_date, cycle_days, final_stage, close_date=None,
                             ent_stall=False):
        """Return list of (stage_name, date_str) through the deal lifecycle."""
        result = []
        stage_fracs = [0.15, 0.20, 0.25, 0.20, 0.20]
        current = created_date

        for i, stage in enumerate(STAGES_ORDERED[:5]):
            result.append((stage, current.isoformat()))
            if stage == final_stage and final_stage in OPEN_STAGES:
                break
            dur = int(stage_fracs[i] * cycle_days)
            # Arc 17: ENT stall at Negotiation
            if ent_stall and stage == "Negotiation":
                dur = _rng.randint(60, 90)
            current = current + timedelta(days=max(dur, 3))

        if final_stage in ("Closed Won", "Closed Lost") and close_date:
            result.append((final_stage, close_date.isoformat()))

        return result

    # ── Helper to create one opportunity + OCRs + tasks ───────────────────

    def _create_opp(ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case, final_stage=None, close_date=None,
                    contact_list=None, ocr_roles=None, competitor=None,
                    activity_gap=None, ent_stall=False,
                    original_close_date=None):
        """Create an opportunity with OCRs and tasks. Returns (opp, ocr_list, task_list)."""
        company_name = _COMPANIES[company_idx]["name"]
        account_id = acc_id_map[company_name]
        opp_id = ids.next_opp()

        # Determine final stage and close date if not specified
        if final_stage is None:
            final_stage, computed_close = _stage_at_date(created_date, cycle_days)
            if computed_close:
                close_date = computed_close
        if close_date is None and final_stage in ("Closed Won", "Closed Lost"):
            close_date = created_date + timedelta(days=cycle_days)
        if close_date is None:
            # Open deal --estimated close date
            close_date = created_date + timedelta(days=cycle_days)

        opp_name = f"{company_name} --{use_case}"

        opp = {
            "Id": opp_id,
            "Name": opp_name,
            "AccountId": account_id,
            "StageName": final_stage,
            "Amount": amount,
            "CloseDate": close_date.isoformat(),
            "LeadSource": lead_source,
            "OwnerId": ae_id,
        }
        if original_close_date:
            opp["_original_close_date"] = original_close_date.isoformat()

        # Get contacts for this company if not provided
        if contact_list is None:
            contact_list = _pick_contacts_for_company(company_idx, _rng.randint(2, 4))

        # OCRs
        opp_ocrs = []
        roles_to_use = ocr_roles or _rng.sample(OCR_ROLES, min(len(contact_list), len(OCR_ROLES)))
        for j, contact in enumerate(contact_list):
            con_id = _get_contact_id(contact["email"])
            if not con_id:
                continue
            role = roles_to_use[j] if j < len(roles_to_use) else "Influencer"
            opp_ocrs.append({
                "Id": ids.next_ocr(),
                "OpportunityId": opp_id,
                "ContactId": con_id,
                "Role": role,
                "IsPrimary": str(j == 0),
            })

        # Stage dates for task generation
        stage_dates = _compute_stage_dates(
            created_date, cycle_days, final_stage, close_date,
            ent_stall=ent_stall,
        )

        # Tasks
        opp_tasks = _generate_deal_tasks(
            opp_id, ae_id, contact_list, company_name, use_case, amount,
            stage_dates, competitor=competitor, activity_gap=activity_gap,
        )

        return opp, opp_ocrs, opp_tasks

    # ══════════════════════════════════════════════════════════════════════
    # ARC 8: SDR-to-AE Handoff Opportunities
    # ══════════════════════════════════════════════════════════════════════

    arc8_opp_count = 0
    for pair in SDR_AE_PAIRS:
        sdr_id = pair["sdr_id"]
        ae_id = pair["ae_id"]
        ae = AE_MAP[ae_id]
        segment = ae["segment"]

        # Get pool-contact replies for this SDR
        sdr_replies = replies_by_sdr.get(sdr_id, [])
        if not sdr_replies:
            continue

        # Deduplicate by email, keep earliest reply
        seen_emails = {}
        for r in sdr_replies:
            if r["email"] not in seen_emails:
                seen_emails[r["email"]] = r

        # Pick 2-4 contacts who replied
        unique_replies = list(seen_emails.values())
        _rng.shuffle(unique_replies)
        n_handoffs = _rng.randint(2, 4)
        handoff_contacts = unique_replies[:n_handoffs]

        for reply_info in handoff_contacts:
            email = reply_info["email"]
            company_name = reply_info["company"]
            reply_date = _date_from_str(reply_info["replied_at"])
            seq_id = reply_info["seq_id"]
            lead_source = seq_names.get(seq_id, "Outbound Sequence")

            # Find company index
            company_idx = None
            for idx, co in enumerate(_COMPANIES):
                if co["name"] == company_name:
                    company_idx = idx
                    break
            if company_idx is None:
                continue

            # Created date = 1-3 biz days after reply
            delay = _rng.randint(*HANDOFF_OPP_DELAY_DAYS)
            created_date = _add_biz_days(reply_date, delay)
            if created_date > DEMO_TODAY:
                continue

            # Deal params by segment
            params = SEGMENT_PARAMS[segment]
            amount = _rng.randrange(params[2], params[3] + 1, 5_000)
            cycle_days = _rng.randint(params[4], params[5])

            # Find the replied contact in pool
            contact = next((c for c in CONTACT_POOL if c["email"] == email), None)
            if not contact:
                continue

            use_case = _next_use_case()

            # Assign explicit outcome for handoff deals
            elapsed = (DEMO_TODAY - created_date).days
            handoff_roll = _rng.random()
            if elapsed > cycle_days and handoff_roll < 0.30:
                # Some handoff deals close won
                close_date = created_date + timedelta(days=cycle_days + _rng.randint(0, 10))
                if close_date > DEMO_TODAY:
                    close_date = DEMO_TODAY - timedelta(days=_rng.randint(1, 14))
                opp, opp_ocrs, opp_tasks = _create_opp(
                    ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case, contact_list=[contact],
                    final_stage="Closed Won", close_date=close_date,
                )
            elif elapsed > cycle_days and handoff_roll < 0.45:
                close_date = created_date + timedelta(days=_rng.randint(cycle_days // 2, cycle_days))
                if close_date > DEMO_TODAY:
                    close_date = DEMO_TODAY - timedelta(days=_rng.randint(1, 14))
                opp, opp_ocrs, opp_tasks = _create_opp(
                    ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case, contact_list=[contact],
                    final_stage="Closed Lost", close_date=close_date,
                )
            else:
                # Still open at appropriate stage
                opp, opp_ocrs, opp_tasks = _create_opp(
                    ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case, contact_list=[contact],
                )
            opportunities.append(opp)
            ocrs.extend(opp_ocrs)
            tasks.extend(opp_tasks)
            arc8_opp_count += 1

    # ══════════════════════════════════════════════════════════════════════
    # ARC 9: Deal Stalls and Dies --Nate Johansson (ae_5)
    # ══════════════════════════════════════════════════════════════════════

    # Pick an MM company for Arc 9
    mm_companies = COMPANY_BY_TERRITORY["MM"]
    arc9_company_idx = mm_companies[0]  # IronShield Technologies (idx 1)
    arc9_contacts = _pick_contacts_for_company(arc9_company_idx, 1)

    arc9_stages = ARC_9_DEAL["stages"]
    arc9_opp_id = ids.next_opp()
    arc9_company_name = _COMPANIES[arc9_company_idx]["name"]
    arc9_close_date = _date_from_str(arc9_stages[-1][1])

    opp_arc9 = {
        "Id": arc9_opp_id,
        "Name": f"{arc9_company_name} --Enterprise Platform Expansion",
        "AccountId": acc_id_map[arc9_company_name],
        "StageName": "Closed Lost",
        "Amount": ARC_9_DEAL["amount"],
        "CloseDate": arc9_close_date.isoformat(),
        "LeadSource": seq_names.get(1001, "CISO Outbound -- Q2 Pipeline Push"),
        "OwnerId": "ae_5",
    }
    opportunities.append(opp_arc9)

    # Single OCR
    if arc9_contacts:
        c9 = arc9_contacts[0]
        con_id = _get_contact_id(c9["email"])
        if con_id:
            ocrs.append({
                "Id": ids.next_ocr(),
                "OpportunityId": arc9_opp_id,
                "ContactId": con_id,
                "Role": "Champion",
                "IsPrimary": "True",
            })

    # Arc 9 tasks --detailed story progression
    arc9_gap_start = _date_from_str(ARC_9_DEAL["activity_gap_start"])
    arc9_gap_end = _date_from_str(ARC_9_DEAL["activity_gap_end"])
    c9_name = f"{arc9_contacts[0]['first_name']} {arc9_contacts[0]['last_name']}" if arc9_contacts else "Champion"
    c9_id = _get_contact_id(arc9_contacts[0]["email"]) if arc9_contacts else None

    arc9_task_defs = [
        # Pre-gap tasks (active engagement)
        ("2025-07-12", "Discovery call with " + c9_name,
         f"Discovery call --strong interest in platform. {c9_name} confirmed budget for H2.",
         "Call"),
        ("2025-07-22", f"Sent DOOM overview deck to {arc9_company_name}",
         f"Shared product overview. {c9_name} reviewing with team.",
         "Email"),
        ("2025-08-07", f"Qualification call --{c9_name} budget confirmed",
         f"Qualification complete. {c9_name} confirmed $180K budget allocated. Moving to needs analysis.",
         "Call"),
        ("2025-08-20", f"Follow-up email to {c9_name}",
         f"Sent competitive positioning doc. {c9_name} forwarded to VP.",
         "Email"),
        ("2025-09-15", f"Demo completed --{arc9_company_name}",
         f"Demo well received. {c9_name} engaged, asked detailed integration questions.",
         "Meeting"),
        ("2025-09-25", f"Technical deep dive with {c9_name}",
         f"Reviewed API integration requirements. {c9_name} satisfied with technical approach.",
         "Meeting"),
        ("2025-10-10", f"Proposal sent --{arc9_company_name}",
         f"Sent formal proposal at $180,000. {c9_name} reviewing with procurement.",
         "Email"),
        ("2025-10-22", f"Pricing discussion with {c9_name}",
         f"Walked through pricing tiers. {c9_name} pushing for multi-year discount.",
         "Call"),
        ("2025-11-05", f"Negotiation kickoff --{arc9_company_name}",
         f"Contract redline received. Negotiating MSA terms with legal.",
         "Call"),
        ("2025-11-08", f"Left voicemail for {c9_name}",
         f"Attempted follow-up on contract redlines. {c9_name} did not answer.",
         "Call"),
        # GAP: Nov 10 - Jan 10 --NO TASKS
        # Post-gap close-out
        ("2026-01-12", f"Deal closed lost --{arc9_company_name}",
         f"Deal closed lost --no response from champion. {c9_name} unresponsive since early November. 42-day activity gap.",
         "Email"),
    ]

    if c9_id:
        for dt_str, subj, desc, task_type in arc9_task_defs:
            tasks.append({
                "Id": ids.next_tsk(),
                "WhoId": c9_id,
                "WhatId": arc9_opp_id,
                "Subject": subj,
                "Status": "Completed",
                "ActivityDate": dt_str,
                "Type": task_type,
                "Description": desc,
                "OwnerId": "ae_5",
            })

    # ══════════════════════════════════════════════════════════════════════
    # ARC 10: Multi-Threaded Deals Close
    # ══════════════════════════════════════════════════════════════════════

    # --- David (ae_1) SMB multi-threader: 2-3 deals with 4 OCRs each ---
    smb_companies = COMPANY_BY_TERRITORY["SMB"]
    for deal_num in range(3):
        company_idx = smb_companies[deal_num % len(smb_companies)]
        contact_list = _pick_contacts_for_company(company_idx, 4)
        amount = _rng.randrange(40_000, 75_001, 5_000)
        created_date = DATA_START + timedelta(days=_rng.randint(30, 200))
        cycle_days = _rng.randint(30, 60)
        use_case = _next_use_case()

        # These deals should mostly close won
        close_date = created_date + timedelta(days=cycle_days + _rng.randint(0, 14))
        if close_date > DEMO_TODAY:
            final_stage = "Negotiation"
            close_date_val = created_date + timedelta(days=cycle_days)
        else:
            final_stage = "Closed Won"
            close_date_val = close_date

        opp, opp_ocrs, opp_tasks = _create_opp(
            "ae_1", company_idx, amount, created_date, cycle_days,
            seq_names.get(1001, "CISO Outbound -- Q2 Pipeline Push"), use_case,
            final_stage=final_stage, close_date=close_date_val,
            contact_list=contact_list,
            ocr_roles=["Decision Maker", "Economic Buyer", "Technical Evaluator", "End User"],
        )
        opportunities.append(opp)
        ocrs.extend(opp_ocrs)
        tasks.extend(opp_tasks)

    # --- Daniel (ae_9) STRAT signature $350K deal with 5 OCRs ---
    strat_companies = COMPANY_BY_TERRITORY["STRAT"]
    arc10_strat_idx = strat_companies[0]  # Meridian Financial Group (idx 2)
    arc10_strat_contacts = _pick_contacts_for_company(arc10_strat_idx, 4)

    # We need 5 OCR roles but only have 4 contacts per company.
    # Use all 4 contacts and assign the 5 roles (one contact gets two roles via the OCR).
    # Actually, we have exactly 4 contacts per company. The spec asks for 5 OCRs with
    # specific roles. We'll use the 4 available contacts and create the 5th from another
    # STRAT company contact.
    strat_company2_idx = strat_companies[1]  # Hartwell Industries (idx 5)
    extra_contact = _pick_contacts_for_company(strat_company2_idx, 1)
    arc10_5_contacts = arc10_strat_contacts + extra_contact

    arc10_strat_roles = ARC_10_DEALS["ae_9"]["contact_roles"]
    arc10_created = date(2025, 5, 15)
    arc10_cycle = 160
    arc10_close = date(2025, 11, 20)

    opp_d, opp_d_ocrs, opp_d_tasks = _create_opp(
        "ae_9", arc10_strat_idx, 350_000, arc10_created, arc10_cycle,
        seq_names.get(1001, "CISO Outbound -- Q2 Pipeline Push"),
        "Enterprise Security Transformation",
        final_stage="Closed Won", close_date=arc10_close,
        contact_list=arc10_5_contacts,
        ocr_roles=arc10_strat_roles,
    )
    opportunities.append(opp_d)
    ocrs.extend(opp_d_ocrs)
    tasks.extend(opp_d_tasks)

    # Add extra multi-stakeholder tasks for Daniel's deal referencing different contacts
    daniel_opp_id = opp_d["Id"]
    strat_co_name = _COMPANIES[arc10_strat_idx]["name"]
    for ci, contact in enumerate(arc10_5_contacts):
        con_id = _get_contact_id(contact["email"])
        if not con_id:
            continue
        role = arc10_strat_roles[ci] if ci < len(arc10_strat_roles) else "Stakeholder"
        cn = f"{contact['first_name']} {contact['last_name']}"

        extra_date = arc10_created + timedelta(days=20 + ci * 18)
        if extra_date > DEMO_TODAY:
            extra_date = DEMO_TODAY

        meeting_subjs = [
            f"Stakeholder alignment --{cn} ({role})",
            f"Technical review with {cn} --{strat_co_name}",
            f"Executive briefing --{cn}",
        ]
        meeting_descs = [
            f"Met with {cn} ({role}) to align on security requirements and deployment timeline.",
            f"{cn} validated technical architecture. No blockers from {role} perspective.",
            f"Executive session with {cn}. Confirmed organizational priority for security transformation.",
        ]
        tasks.append({
            "Id": ids.next_tsk(),
            "WhoId": con_id,
            "WhatId": daniel_opp_id,
            "Subject": _rng.choice(meeting_subjs),
            "Status": "Completed",
            "ActivityDate": extra_date.isoformat(),
            "Type": "Meeting",
            "Description": _rng.choice(meeting_descs),
            "OwnerId": "ae_9",
        })

    # ══════════════════════════════════════════════════════════════════════
    # ARC 11: Competitive Displacement --Keiko (ae_6)
    # ══════════════════════════════════════════════════════════════════════

    competitor = ARC_11_COMPETITIVE["competitor_name"]
    mm_for_keiko = [idx for idx in mm_companies if idx not in (arc9_company_idx,)]

    # Deal A: Won ($95K)
    arc11_co_a = mm_for_keiko[0]  # Apex Health Systems (idx 3)
    arc11_contacts_a = _pick_contacts_for_company(arc11_co_a, 2)
    arc11a_created = date(2025, 6, 10)
    arc11a_close = date(2025, 9, 15)
    opp_a, ocrs_a, tasks_a = _create_opp(
        "ae_6", arc11_co_a, 95_000, arc11a_created, 90,
        "Partner Referral", "Security Posture Assessment",
        final_stage="Closed Won", close_date=arc11a_close,
        contact_list=arc11_contacts_a, competitor=competitor,
    )
    opportunities.append(opp_a)
    ocrs.extend(ocrs_a)
    tasks.extend(tasks_a)

    # Deal B: Lost ($110K)
    arc11_co_b = mm_for_keiko[1]  # Nexus SaaS Labs (idx 4)
    arc11_contacts_b = _pick_contacts_for_company(arc11_co_b, 2)
    arc11b_created = date(2025, 7, 1)
    arc11b_close = date(2025, 10, 20)
    opp_b, ocrs_b, tasks_b = _create_opp(
        "ae_6", arc11_co_b, 110_000, arc11b_created, 100,
        "Web", "Threat Detection Module",
        final_stage="Closed Lost", close_date=arc11b_close,
        contact_list=arc11_contacts_b, competitor=competitor,
    )
    opportunities.append(opp_b)
    ocrs.extend(ocrs_b)
    tasks.extend(tasks_b)

    # ══════════════════════════════════════════════════════════════════════
    # ARC 12: Forecast Sandbagging --Elena (ae_2)
    # ══════════════════════════════════════════════════════════════════════

    elena_opps_created = 0
    for deal_i in range(8):
        company_idx = smb_companies[deal_i % len(smb_companies)]
        amount = _rng.randrange(15_000, 80_001, 5_000)
        created_date = DATA_START + timedelta(days=30 + deal_i * 40)
        cycle_days = _rng.randint(30, 60)
        use_case = _next_use_case()

        actual_close = created_date + timedelta(days=cycle_days)
        if actual_close > DEMO_TODAY:
            # Still open
            opp, opp_ocrs, opp_tasks = _create_opp(
                "ae_2", company_idx, amount, created_date, cycle_days,
                _rng.choice(INBOUND_SOURCES), use_case,
            )
        elif deal_i in (0, 2, 4):
            # Sandbagged: original forecast was 14-21 days early (3 deals)
            # This gives ~62% accuracy: 5 on-time / 8 total
            slippage = _rng.randint(*ARC_12_SANDBAGGING["slippage_days_range"])
            original_close = actual_close - timedelta(days=slippage)
            opp, opp_ocrs, opp_tasks = _create_opp(
                "ae_2", company_idx, amount, created_date, cycle_days,
                _rng.choice(INBOUND_SOURCES), use_case,
                final_stage="Closed Won", close_date=actual_close,
                original_close_date=original_close,
            )
        else:
            # On-time deal (closes near forecast)
            opp, opp_ocrs, opp_tasks = _create_opp(
                "ae_2", company_idx, amount, created_date, cycle_days,
                _rng.choice(INBOUND_SOURCES), use_case,
                final_stage="Closed Won", close_date=actual_close,
                original_close_date=actual_close - timedelta(days=_rng.randint(0, 5)),
            )

        opportunities.append(opp)
        ocrs.extend(opp_ocrs)
        tasks.extend(opp_tasks)
        elena_opps_created += 1

    # ══════════════════════════════════════════════════════════════════════
    # ARC 17: Cross-Segment ENT Stall --ae_7 and ae_8
    # ══════════════════════════════════════════════════════════════════════

    ent_companies = COMPANY_BY_TERRITORY["ENT"]
    ent_stall_start = date(2025, 9, 1)

    for ae_id in ARC_17_ENT_STALL["affected_aes"]:
        ae = AE_MAP[ae_id]
        params = SEGMENT_PARAMS["ENT"]
        n_opps = _rng.randint(params[0], params[1])

        for deal_i in range(n_opps):
            company_idx = ent_companies[deal_i % len(ent_companies)]
            amount = _rng.randrange(params[2], params[3] + 1, 5_000)
            created_date = DATA_START + timedelta(days=_rng.randint(0, 300))
            cycle_days = _rng.randint(params[4], params[5])
            use_case = _next_use_case()

            # Determine if this deal hits the ENT stall
            is_stalled = created_date >= ent_stall_start

            if is_stalled:
                # Stalled deals: longer cycle, mostly stuck in Negotiation
                cycle_days = _rng.randint(150, 220)
                # Low close rate: ~14%
                if _rng.random() < ARC_17_ENT_STALL["stalled_neg_to_close_rate"]:
                    final_stage = "Closed Won"
                    close_date = created_date + timedelta(days=cycle_days)
                    if close_date > DEMO_TODAY:
                        final_stage = "Negotiation"
                        close_date = None
                else:
                    # Most stall at Negotiation
                    final_stage = "Negotiation"
                    close_date = None

                lead_source = _rng.choice(INBOUND_SOURCES) if ae_id in ("ae_8", "ae_10") else seq_names.get(1001, "CISO Outbound -- Q2 Pipeline Push")
                opp, opp_ocrs, opp_tasks = _create_opp(
                    ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case,
                    final_stage=final_stage, close_date=close_date,
                    ent_stall=True,
                )
            else:
                # Normal pre-stall deals -- assign outcome explicitly
                lead_source = _rng.choice(INBOUND_SOURCES) if ae_id in ("ae_8", "ae_10") else seq_names.get(1001, "CISO Outbound -- Q2 Pipeline Push")
                pre_roll = _rng.random()
                if pre_roll < 0.38:
                    # Normal close rate ~38%
                    close_date = created_date + timedelta(days=cycle_days + _rng.randint(0, 14))
                    if close_date > DEMO_TODAY:
                        opp, opp_ocrs, opp_tasks = _create_opp(
                            ae_id, company_idx, amount, created_date, cycle_days,
                            lead_source, use_case,
                        )
                    else:
                        opp, opp_ocrs, opp_tasks = _create_opp(
                            ae_id, company_idx, amount, created_date, cycle_days,
                            lead_source, use_case,
                            final_stage="Closed Won", close_date=close_date,
                        )
                elif pre_roll < 0.55:
                    close_date = created_date + timedelta(days=_rng.randint(cycle_days // 2, cycle_days))
                    if close_date > DEMO_TODAY:
                        close_date = DEMO_TODAY - timedelta(days=_rng.randint(1, 30))
                    opp, opp_ocrs, opp_tasks = _create_opp(
                        ae_id, company_idx, amount, created_date, cycle_days,
                        lead_source, use_case,
                        final_stage="Closed Lost", close_date=close_date,
                    )
                else:
                    opp, opp_ocrs, opp_tasks = _create_opp(
                        ae_id, company_idx, amount, created_date, cycle_days,
                        lead_source, use_case,
                    )

            opportunities.append(opp)
            ocrs.extend(opp_ocrs)
            tasks.extend(opp_tasks)

    # ══════════════════════════════════════════════════════════════════════
    # BASELINE OPPORTUNITIES for remaining AEs
    # ══════════════════════════════════════════════════════════════════════

    # Track how many opps each AE has so far
    ae_opp_counts = defaultdict(int)
    for opp in opportunities:
        ae_opp_counts[opp["OwnerId"]] += 1

    for ae in ALL_AES:
        ae_id = ae["id"]
        segment = ae["segment"]
        params = SEGMENT_PARAMS[segment]
        target_min, target_max = params[0], params[1]
        current_count = ae_opp_counts[ae_id]

        # ae_2 (Elena) gets exactly 8 deals from Arc 12 — no baseline
        if ae_id == "ae_2":
            continue

        # Skip if already at target from arc-specific generation
        if current_count >= target_min:
            continue

        needed = _rng.randint(target_min, target_max) - current_count
        if needed <= 0:
            continue

        # Get companies for this segment
        segment_companies = COMPANY_BY_TERRITORY[segment]
        # For paired AEs, also draw from MM pool for variety
        all_eligible = segment_companies[:]
        if not all_eligible:
            all_eligible = COMPANY_BY_TERRITORY["MM"][:]

        # Determine lead source strategy
        is_unpaired = ae_id in ("ae_2", "ae_4", "ae_8", "ae_10")

        for deal_i in range(needed):
            company_idx = all_eligible[deal_i % len(all_eligible)]
            amount = _rng.randrange(params[2], params[3] + 1, 5_000)
            cycle_days = _rng.randint(params[4], params[5])
            use_case = _next_use_case()

            if is_unpaired:
                lead_source = _rng.choice(INBOUND_SOURCES)
            else:
                lead_source = seq_names.get(
                    _rng.choice(list(seq_names.keys())),
                    "Outbound Sequence"
                )

            # Explicitly assign deal outcome for baseline deals.
            # Targeting ~20% Won, ~15% Lost, ~65% open overall.
            # Arc deals skew closed, so baseline uses lower close rates:
            # ~12% Won, ~10% Lost, ~78% open.
            outcome_roll = _rng.random()
            if outcome_roll < 0.08:
                # Closed Won: created early enough to have completed its cycle
                created_date = DATA_START + timedelta(days=_rng.randint(0, 250))
                close_date = created_date + timedelta(days=cycle_days + _rng.randint(0, 14))
                if close_date > DEMO_TODAY:
                    close_date = DEMO_TODAY - timedelta(days=_rng.randint(1, 30))
                opp, opp_ocrs, opp_tasks = _create_opp(
                    ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case,
                    final_stage="Closed Won", close_date=close_date,
                )
            elif outcome_roll < 0.18:
                # Closed Lost: created early, died during cycle
                created_date = DATA_START + timedelta(days=_rng.randint(0, 250))
                close_date = created_date + timedelta(days=_rng.randint(cycle_days // 2, cycle_days))
                if close_date > DEMO_TODAY:
                    close_date = DEMO_TODAY - timedelta(days=_rng.randint(1, 30))
                opp, opp_ocrs, opp_tasks = _create_opp(
                    ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case,
                    final_stage="Closed Lost", close_date=close_date,
                )
            else:
                # Open deal: pick a target stage and create recently enough to be there
                target_stage_roll = _rng.random()
                if target_stage_roll < 0.20:
                    target_stage = "Prospecting"
                    days_ago = _rng.randint(3, int(cycle_days * 0.15))
                elif target_stage_roll < 0.45:
                    target_stage = "Qualification"
                    days_ago = _rng.randint(int(cycle_days * 0.15), int(cycle_days * 0.35))
                elif target_stage_roll < 0.65:
                    target_stage = "Needs Analysis"
                    days_ago = _rng.randint(int(cycle_days * 0.35), int(cycle_days * 0.60))
                elif target_stage_roll < 0.82:
                    target_stage = "Proposal/Price Quote"
                    days_ago = _rng.randint(int(cycle_days * 0.60), int(cycle_days * 0.80))
                else:
                    target_stage = "Negotiation"
                    days_ago = _rng.randint(int(cycle_days * 0.80), cycle_days)
                created_date = DEMO_TODAY - timedelta(days=days_ago)
                if created_date < DATA_START:
                    created_date = DATA_START + timedelta(days=_rng.randint(0, 30))
                opp, opp_ocrs, opp_tasks = _create_opp(
                    ae_id, company_idx, amount, created_date, cycle_days,
                    lead_source, use_case,
                    final_stage=target_stage,
                )

            opportunities.append(opp)
            ocrs.extend(opp_ocrs)
            tasks.extend(opp_tasks)

    # ══════════════════════════════════════════════════════════════════════
    # WRITE CSV FILES
    # ══════════════════════════════════════════════════════════════════════

    os.makedirs(output_dir, exist_ok=True)

    def _write_csv(filename, rows, fieldnames):
        path = os.path.join(output_dir, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        return len(rows)

    counts = {}
    counts["sf_accounts.csv"] = _write_csv(
        "sf_accounts.csv", accounts,
        ["Id", "Name", "Industry", "NumberOfEmployees", "BillingCity"],
    )
    counts["sf_contacts.csv"] = _write_csv(
        "sf_contacts.csv", contacts,
        ["Id", "FirstName", "LastName", "Email", "Title", "AccountId", "Industry"],
    )
    counts["sf_opportunities.csv"] = _write_csv(
        "sf_opportunities.csv", opportunities,
        ["Id", "Name", "AccountId", "StageName", "Amount", "CloseDate", "LeadSource", "OwnerId"],
    )
    counts["sf_opportunity_contact_roles.csv"] = _write_csv(
        "sf_opportunity_contact_roles.csv", ocrs,
        ["Id", "OpportunityId", "ContactId", "Role", "IsPrimary"],
    )
    counts["sf_tasks.csv"] = _write_csv(
        "sf_tasks.csv", tasks,
        ["Id", "WhoId", "WhatId", "Subject", "Status", "ActivityDate", "Type", "Description", "OwnerId"],
    )

    # ══════════════════════════════════════════════════════════════════════
    # VALIDATION
    # ══════════════════════════════════════════════════════════════════════

    print("\n  === Phase 3 Validation ===")

    # 1. Record counts
    print(f"\n  Record counts:")
    print(f"    Accounts:      {len(accounts)}")
    print(f"    Contacts:      {len(contacts)}")
    print(f"    Opportunities: {len(opportunities)}")
    print(f"    OCRs:          {len(ocrs)}")
    print(f"    Tasks:         {len(tasks)}")

    # 2. Opportunities by AE
    print(f"\n  Opportunities by AE:")
    ae_counts = defaultdict(int)
    for opp in opportunities:
        ae_counts[opp["OwnerId"]] += 1
    for ae in ALL_AES:
        seg = ae["segment"]
        cnt = ae_counts.get(ae["id"], 0)
        print(f"    {ae['id']} ({ae['name']}, {seg}): {cnt} opps")

    # 3. Stage distribution
    print(f"\n  Stage distribution:")
    from collections import Counter
    stage_counts = Counter(o["StageName"] for o in opportunities)
    total_opps = len(opportunities)
    for stage in STAGES_ORDERED:
        cnt = stage_counts.get(stage, 0)
        pct = cnt / total_opps * 100 if total_opps else 0
        print(f"    {stage:<25} {cnt:>4} ({pct:.1f}%)")

    # 4. Arc 8 validation
    print(f"\n  Arc 8 --SDR-to-AE Handoff:")
    print(f"    Handoff-attributed opps: {arc8_opp_count}")
    arc8_opps = [o for o in opportunities if o["LeadSource"] in seq_names.values()
                 and any(o["OwnerId"] == p["ae_id"] for p in SDR_AE_PAIRS)]
    for opp in arc8_opps[:3]:
        # Find the OCR ->contact ->email
        opp_ocr = next((r for r in ocrs if r["OpportunityId"] == opp["Id"] and r["IsPrimary"] == "True"), None)
        if opp_ocr:
            con = next((c for c in contacts if c["Id"] == opp_ocr["ContactId"]), None)
            if con:
                sdr = next((p["sdr_id"] for p in SDR_AE_PAIRS if p["ae_id"] == opp["OwnerId"]), "?")
                print(f"    Spot check: {sdr} ->{con['Email']} ->opp {opp['Id']} ({opp['StageName']}) ->{opp['OwnerId']}")

    # 5. Arc 9 validation
    print(f"\n  Arc 9 --Deal Stalls and Dies:")
    arc9 = next((o for o in opportunities if o["OwnerId"] == "ae_5" and o["Amount"] == 180_000), None)
    if arc9:
        arc9_ocr_count = sum(1 for r in ocrs if r["OpportunityId"] == arc9["Id"])
        arc9_tasks_list = [t for t in tasks if t["WhatId"] == arc9["Id"]]
        dates = sorted([t["ActivityDate"] for t in arc9_tasks_list])
        if len(dates) >= 2:
            # Find the biggest gap
            max_gap = 0
            for i in range(1, len(dates)):
                d1 = _date_from_str(dates[i - 1])
                d2 = _date_from_str(dates[i])
                gap = (d2 - d1).days
                if gap > max_gap:
                    max_gap = gap
            print(f"    Nate's $180K deal: {arc9['StageName']}, {arc9_ocr_count} OCR(s), max task gap: {max_gap} days")
        else:
            print(f"    Nate's $180K deal: {arc9['StageName']}, {arc9_ocr_count} OCR(s)")

    # 6. Arc 10 validation
    print(f"\n  Arc 10 --Multi-Threaded Deals:")
    for ae_id_check in ["ae_1", "ae_9"]:
        ae_opps = [o for o in opportunities if o["OwnerId"] == ae_id_check]
        for opp in ae_opps:
            ocr_count = sum(1 for r in ocrs if r["OpportunityId"] == opp["Id"])
            if ocr_count >= 4:
                print(f"    {ae_id_check}: {opp['Name'][:50]} --{ocr_count} OCRs, {opp['StageName']}")

    # 7. Arc 11 validation
    print(f"\n  Arc 11 --Competitive Displacement:")
    keiko_opps = [o for o in opportunities if o["OwnerId"] == "ae_6"]
    for opp in keiko_opps:
        opp_tasks_list = [t for t in tasks if t["WhatId"] == opp["Id"]]
        comp_mentions = sum(1 for t in opp_tasks_list if "ShieldStack" in (t.get("Subject", "") + t.get("Description", "")))
        if comp_mentions > 0:
            print(f"    {opp['Name'][:50]} --{opp['StageName']}, {comp_mentions} competitor mentions")

    # 8. Arc 12 validation
    print(f"\n  Arc 12 --Forecast Sandbagging:")
    elena_opps = [o for o in opportunities if o["OwnerId"] == "ae_2"]
    sandbagged = [o for o in elena_opps if "_original_close_date" in o]
    if sandbagged:
        total_elena_closed = [o for o in elena_opps if o["StageName"] == "Closed Won"]
        on_time = 0
        for o in total_elena_closed:
            if "_original_close_date" in o:
                orig = _date_from_str(o["_original_close_date"])
                actual = _date_from_str(o["CloseDate"])
                if abs((actual - orig).days) <= 7:
                    on_time += 1
            else:
                on_time += 1
        accuracy = on_time / len(total_elena_closed) * 100 if total_elena_closed else 0
        print(f"    Elena's closed deals: {len(total_elena_closed)}, on-time: {on_time}, accuracy: {accuracy:.0f}%")

    # 9. Arc 17 validation
    print(f"\n  Arc 17 --ENT Stall:")
    for ae_id_check in ARC_17_ENT_STALL["affected_aes"]:
        ae_neg = [o for o in opportunities if o["OwnerId"] == ae_id_check and o["StageName"] == "Negotiation"]
        print(f"    {ae_id_check}: {len(ae_neg)} deals stuck in Negotiation")

    # Cross-segment comparison
    smb_neg_dwell = []
    ent_neg_dwell = []
    for opp in opportunities:
        if opp["StageName"] == "Negotiation":
            ae = AE_MAP.get(opp["OwnerId"])
            if not ae:
                continue
            # Estimate dwell by looking at task dates
            opp_tasks_list = sorted(
                [t for t in tasks if t["WhatId"] == opp["Id"]],
                key=lambda t: t["ActivityDate"],
            )
            if len(opp_tasks_list) >= 2:
                first = _date_from_str(opp_tasks_list[0]["ActivityDate"])
                last = _date_from_str(opp_tasks_list[-1]["ActivityDate"])
                dwell = (DEMO_TODAY - first).days
                if ae["segment"] == "SMB":
                    smb_neg_dwell.append(dwell)
                elif ae["segment"] == "ENT":
                    ent_neg_dwell.append(dwell)

    if smb_neg_dwell and ent_neg_dwell:
        avg_smb = sum(smb_neg_dwell) / len(smb_neg_dwell)
        avg_ent = sum(ent_neg_dwell) / len(ent_neg_dwell)
        print(f"    Avg Negotiation dwell --SMB: {avg_smb:.0f} days, ENT: {avg_ent:.0f} days")

    # 10. Attribution chain spot check
    print(f"\n  Attribution Chain Spot Checks:")
    arc8_sample = [o for o in opportunities if o["LeadSource"] in seq_names.values()][:3]
    for opp in arc8_sample:
        opp_ocr = next((r for r in ocrs if r["OpportunityId"] == opp["Id"] and r["IsPrimary"] == "True"), None)
        if opp_ocr:
            con = next((c for c in contacts if c["Id"] == opp_ocr["ContactId"]), None)
            if con:
                sdr = next((p["sdr_id"] for p in SDR_AE_PAIRS if p["ae_id"] == opp["OwnerId"]), "N/A")
                # Find reply date from email_activity
                reply_date = "?"
                for r in replies_by_sdr.get(sdr, []):
                    if r["email"] == con["Email"]:
                        reply_date = r["replied_at"]
                        break
                print(f"    {sdr} sequenced {con['Email']}")
                print(f"      ->replied {reply_date} ->opp created {opp['CloseDate']}")
                print(f"      ->AE {opp['OwnerId']} owns ->{opp['StageName']}")

    return counts
