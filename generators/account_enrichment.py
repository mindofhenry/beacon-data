"""
generators/account_enrichment.py
Enriches all 500 accounts (25 Tier 1 + 75 Tier 2 + 400 Tier 3) with
funding, ICP fit, territory, employee counts, growth rates, and tech stack.

Tier 1: Hand-curated enrichment data (unchanged from Phase 3).
Tier 2: Generated enrichment consistent with size_band and industry.
Tier 3: Minimal enrichment (territory + icp_fit + basic tech stack).
"""

import json
import os
import random

from generators.config import GLOBAL_SEED
from generators.contact_pool import _COMPANIES
from generators.account_universe import get_tier2_accounts, get_tier3_accounts

# ── Tech stack tag pool ─────────────────────────────────────────────────────

_TECH_TAGS = [
    "aws", "gcp", "azure", "okta", "datadog", "snowflake", "salesforce",
    "hubspot", "slack", "jira", "github", "pagerduty", "splunk",
    "crowdstrike", "zscaler",
]

# ── Per-company enrichment data ─────────────────────────────────────────────
# Fields: funding_stage, employee_count_current, employee_growth_6mo,
#          tech_stack_tags, icp_fit_score, territory
#
# Territory rules:
#   <200 = SMB, 200-1000 = MM, 1001-5000 = ENT, 5000+ = STRAT
#   No companies are 5000+, so 2-3 of the largest ENT companies are
#   promoted to STRAT based on Series C/D/Public + 3000+ employees.
#
# Funding distribution: Seed(3), A(5), B(8), C(5), D(2), Public(2) = 25
# ICP fit distribution: Strong(8), Moderate(10), Weak(7) = 25

_ENRICHMENT = [
    # idx 0: Fortress Security Corp — 1001-5000, cybersecurity
    # MUST BE: Series B, Strong ICP, ENT territory, has CISO contact
    {
        "funding_stage": "Series B",
        "employee_count_current": 2800,
        "employee_growth_6mo": 0.12,
        "tech_stack_tags": ["aws", "crowdstrike", "splunk", "okta", "jira"],
        "icp_fit_score": "Strong",
        "territory": "ENT",
    },
    # idx 1: IronShield Technologies — 501-1000, cybersecurity
    {
        "funding_stage": "Series B",
        "employee_count_current": 720,
        "employee_growth_6mo": 0.08,
        "tech_stack_tags": ["aws", "datadog", "github", "slack"],
        "icp_fit_score": "Strong",
        "territory": "MM",
    },
    # idx 2: Meridian Financial Group — 1001-5000, financial services
    {
        "funding_stage": "Series C",
        "employee_count_current": 3500,
        "employee_growth_6mo": 0.06,
        "tech_stack_tags": ["azure", "salesforce", "splunk", "okta", "snowflake"],
        "icp_fit_score": "Strong",
        "territory": "STRAT",
    },
    # idx 3: Apex Health Systems — 501-1000, healthcare
    {
        "funding_stage": "Series B",
        "employee_count_current": 650,
        "employee_growth_6mo": 0.10,
        "tech_stack_tags": ["aws", "okta", "pagerduty", "jira"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 4: Nexus SaaS Labs — 201-500, SaaS
    {
        "funding_stage": "Series A",
        "employee_count_current": 340,
        "employee_growth_6mo": 0.18,
        "tech_stack_tags": ["gcp", "datadog", "github", "slack"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 5: Hartwell Industries — 1001-5000, manufacturing
    {
        "funding_stage": "Public",
        "employee_count_current": 4200,
        "employee_growth_6mo": 0.02,
        "tech_stack_tags": ["azure", "salesforce", "jira", "splunk", "okta"],
        "icp_fit_score": "Strong",
        "territory": "STRAT",
    },
    # idx 6: ClearPath Health — 201-500, healthcare
    {
        "funding_stage": "Series A",
        "employee_count_current": 280,
        "employee_growth_6mo": 0.15,
        "tech_stack_tags": ["aws", "slack", "hubspot", "jira"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 7: Summit Bank & Trust — 501-1000, financial services
    {
        "funding_stage": "Series C",
        "employee_count_current": 850,
        "employee_growth_6mo": 0.04,
        "tech_stack_tags": ["azure", "salesforce", "crowdstrike", "okta"],
        "icp_fit_score": "Strong",
        "territory": "MM",
    },
    # idx 8: Stackify Inc — 201-500, SaaS
    {
        "funding_stage": "Series B",
        "employee_count_current": 310,
        "employee_growth_6mo": 0.20,
        "tech_stack_tags": ["gcp", "github", "datadog", "slack"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 9: Kestrel Defense Systems — 501-1000, cybersecurity
    {
        "funding_stage": "Series C",
        "employee_count_current": 780,
        "employee_growth_6mo": 0.07,
        "tech_stack_tags": ["aws", "splunk", "crowdstrike", "zscaler", "okta"],
        "icp_fit_score": "Strong",
        "territory": "MM",
    },
    # idx 10: Pinnacle Manufacturing Co — 1001-5000, manufacturing
    {
        "funding_stage": "Series D",
        "employee_count_current": 3200,
        "employee_growth_6mo": 0.03,
        "tech_stack_tags": ["azure", "salesforce", "jira", "pagerduty"],
        "icp_fit_score": "Strong",
        "territory": "STRAT",
    },
    # idx 11: Cloudix Corp — 51-200, SaaS
    {
        "funding_stage": "Seed",
        "employee_count_current": 85,
        "employee_growth_6mo": 0.25,
        "tech_stack_tags": ["gcp", "github", "slack"],
        "icp_fit_score": "Weak",
        "territory": "SMB",
    },
    # idx 12: Vantage Security Group — 201-500, cybersecurity
    {
        "funding_stage": "Series B",
        "employee_count_current": 420,
        "employee_growth_6mo": 0.11,
        "tech_stack_tags": ["aws", "crowdstrike", "zscaler", "datadog"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 13: Redrock Financial — 201-500, financial services
    {
        "funding_stage": "Series A",
        "employee_count_current": 260,
        "employee_growth_6mo": 0.09,
        "tech_stack_tags": ["azure", "salesforce", "okta", "slack"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 14: Medcore Solutions — 501-1000, healthcare
    {
        "funding_stage": "Series B",
        "employee_count_current": 580,
        "employee_growth_6mo": 0.06,
        "tech_stack_tags": ["aws", "okta", "jira", "pagerduty", "slack"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 15: BluePeak Systems — 51-200, SaaS
    {
        "funding_stage": "Seed",
        "employee_count_current": 120,
        "employee_growth_6mo": 0.22,
        "tech_stack_tags": ["gcp", "github", "hubspot"],
        "icp_fit_score": "Weak",
        "territory": "SMB",
    },
    # idx 16: Ironclad Defense — 201-500, cybersecurity
    {
        "funding_stage": "Series A",
        "employee_count_current": 350,
        "employee_growth_6mo": 0.14,
        "tech_stack_tags": ["aws", "splunk", "crowdstrike", "github"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 17: Harbor Capital Group — 1001-5000, financial services
    {
        "funding_stage": "Series D",
        "employee_count_current": 2600,
        "employee_growth_6mo": 0.05,
        "tech_stack_tags": ["azure", "salesforce", "snowflake", "okta", "splunk"],
        "icp_fit_score": "Strong",
        "territory": "ENT",
    },
    # idx 18: NovaMed Health — 201-500, healthcare
    {
        "funding_stage": "Series B",
        "employee_count_current": 290,
        "employee_growth_6mo": 0.13,
        "tech_stack_tags": ["aws", "slack", "jira", "hubspot"],
        "icp_fit_score": "Weak",
        "territory": "MM",
    },
    # idx 19: Quickscale Technologies — 51-200, SaaS
    {
        "funding_stage": "Seed",
        "employee_count_current": 65,
        "employee_growth_6mo": 0.20,
        "tech_stack_tags": ["gcp", "github", "slack"],
        "icp_fit_score": "Weak",
        "territory": "SMB",
    },
    # idx 20: Steelman Industries — 1001-5000, manufacturing
    {
        "funding_stage": "Public",
        "employee_count_current": 4500,
        "employee_growth_6mo": 0.01,
        "tech_stack_tags": ["azure", "salesforce", "jira", "okta", "splunk"],
        "icp_fit_score": "Moderate",
        "territory": "ENT",
    },
    # idx 21: CipherGuard Inc — 51-200, cybersecurity
    {
        "funding_stage": "Series A",
        "employee_count_current": 95,
        "employee_growth_6mo": 0.18,
        "tech_stack_tags": ["aws", "github", "datadog"],
        "icp_fit_score": "Weak",
        "territory": "SMB",
    },
    # idx 22: Atlas Financial Services — 501-1000, financial services
    {
        "funding_stage": "Series C",
        "employee_count_current": 690,
        "employee_growth_6mo": 0.07,
        "tech_stack_tags": ["azure", "salesforce", "snowflake", "okta"],
        "icp_fit_score": "Moderate",
        "territory": "MM",
    },
    # idx 23: WestPoint Healthcare — 501-1000, healthcare
    {
        "funding_stage": "Series B",
        "employee_count_current": 550,
        "employee_growth_6mo": -0.02,
        "tech_stack_tags": ["aws", "okta", "pagerduty", "jira"],
        "icp_fit_score": "Weak",
        "territory": "MM",
    },
    # idx 24: Titan Cloud Corp — 201-500, SaaS
    {
        "funding_stage": "Series C",
        "employee_count_current": 480,
        "employee_growth_6mo": 0.16,
        "tech_stack_tags": ["gcp", "datadog", "github", "slack", "hubspot"],
        "icp_fit_score": "Weak",
        "territory": "MM",
    },
]


# ── Territory derivation ───────────────────────────────────────────────────────

_SIZE_BAND_TO_EMPLOYEE_RANGE = {
    "51-200": (51, 200),
    "201-500": (201, 500),
    "501-1000": (501, 1000),
    "1001-5000": (1001, 5000),
    "5000+": (5001, 15000),
}

def _territory_from_employees(n):
    """Derive territory from employee count."""
    if n < 200:
        return "SMB"
    elif n <= 1000:
        return "MM"
    elif n <= 5000:
        return "ENT"
    return "STRAT"


# ── Funding stage pools by size ──────────────────────────────────────────────

_FUNDING_BY_SIZE = {
    "51-200": ["Seed", "Series A", "Series A", "Series B"],
    "201-500": ["Series A", "Series B", "Series B", "Series C"],
    "501-1000": ["Series B", "Series C", "Series C", "Series D"],
    "1001-5000": ["Series C", "Series D", "Series D", "Public"],
    "5000+": ["Series D", "Public", "Public", "Public"],
}

# ── ICP fit weights by size ──────────────────────────────────────────────────

_ICP_BY_SIZE = {
    "51-200": ["Weak", "Weak", "Moderate"],
    "201-500": ["Weak", "Moderate", "Moderate", "Strong"],
    "501-1000": ["Moderate", "Moderate", "Strong"],
    "1001-5000": ["Moderate", "Strong", "Strong"],
    "5000+": ["Moderate", "Strong"],
}

# ── Tech stack pools by industry ─────────────────────────────────────────────

_STACK_BY_INDUSTRY = {
    "cybersecurity": [
        ["aws", "crowdstrike", "splunk", "okta"],
        ["aws", "zscaler", "datadog", "github"],
        ["azure", "splunk", "crowdstrike", "jira"],
    ],
    "SaaS": [
        ["gcp", "datadog", "github", "slack"],
        ["aws", "github", "hubspot", "jira"],
        ["gcp", "github", "slack", "datadog"],
    ],
    "financial services": [
        ["azure", "salesforce", "snowflake", "okta"],
        ["azure", "salesforce", "splunk", "okta"],
        ["aws", "salesforce", "okta", "jira"],
    ],
    "healthcare": [
        ["aws", "okta", "pagerduty", "jira"],
        ["aws", "slack", "jira", "hubspot"],
        ["azure", "okta", "jira", "pagerduty"],
    ],
    "manufacturing": [
        ["azure", "salesforce", "jira", "okta"],
        ["azure", "jira", "pagerduty", "slack"],
        ["aws", "salesforce", "jira", "splunk"],
    ],
}


def _generate_tier2_enrichment(companies, seed):
    """Generate enrichment data for Tier 2 accounts."""
    rng = random.Random(seed)
    records = []
    for co in companies:
        band = co["company_size_band"]
        industry = co["industry"]
        emp_lo, emp_hi = _SIZE_BAND_TO_EMPLOYEE_RANGE[band]
        emp = rng.randint(emp_lo, emp_hi)
        territory = _territory_from_employees(emp)

        records.append({
            "funding_stage": rng.choice(_FUNDING_BY_SIZE[band]),
            "employee_count_current": emp,
            "employee_growth_6mo": round(rng.uniform(-0.02, 0.25), 2),
            "tech_stack_tags": rng.choice(_STACK_BY_INDUSTRY[industry]),
            "icp_fit_score": rng.choice(_ICP_BY_SIZE[band]),
            "territory": territory,
        })
    return records


def _generate_tier3_enrichment(companies, seed):
    """Generate minimal enrichment data for Tier 3 accounts."""
    rng = random.Random(seed)
    records = []
    for co in companies:
        band = co["company_size_band"]
        industry = co["industry"]
        emp_lo, emp_hi = _SIZE_BAND_TO_EMPLOYEE_RANGE[band]
        emp = rng.randint(emp_lo, emp_hi)
        territory = _territory_from_employees(emp)

        # Tier 3: mostly Weak/Moderate ICP, minimal stack
        icp = rng.choice(["Weak", "Weak", "Moderate"])
        stack = rng.choice(_STACK_BY_INDUSTRY[industry])[:2]  # only 2 tags

        records.append({
            "funding_stage": rng.choice(_FUNDING_BY_SIZE[band]),
            "employee_count_current": emp,
            "employee_growth_6mo": round(rng.uniform(-0.03, 0.15), 2),
            "tech_stack_tags": stack,
            "icp_fit_score": icp,
            "territory": territory,
        })
    return records


# ── Tier 2/3 enrichment data (generated at import time, deterministic) ───────

_TIER2_ENRICHMENT = _generate_tier2_enrichment(get_tier2_accounts(), GLOBAL_SEED + 500)
_TIER3_ENRICHMENT = _generate_tier3_enrichment(get_tier3_accounts(), GLOBAL_SEED + 600)


def _build_enriched_records():
    """Merge company base data with enrichment fields for all 500 accounts."""
    records = []

    # Tier 1: 25 accounts from contact_pool
    for idx, company in enumerate(_COMPANIES):
        enrichment = _ENRICHMENT[idx]
        records.append({
            "company_index": idx,
            "tier": 1,
            "company_name": company["name"],
            "domain": company["domain"],
            "industry": company["industry"],
            "company_size_band": company["company_size_band"],
            "funding_stage": enrichment["funding_stage"],
            "employee_count_current": enrichment["employee_count_current"],
            "employee_growth_6mo": enrichment["employee_growth_6mo"],
            "tech_stack_tags": enrichment["tech_stack_tags"],
            "icp_fit_score": enrichment["icp_fit_score"],
            "territory": enrichment["territory"],
        })

    # Tier 2: 75 accounts
    t2_accounts = get_tier2_accounts()
    for idx, company in enumerate(t2_accounts):
        enrichment = _TIER2_ENRICHMENT[idx]
        records.append({
            "company_index": 25 + idx,
            "tier": 2,
            "company_name": company["name"],
            "domain": company["domain"],
            "industry": company["industry"],
            "company_size_band": company["company_size_band"],
            "funding_stage": enrichment["funding_stage"],
            "employee_count_current": enrichment["employee_count_current"],
            "employee_growth_6mo": enrichment["employee_growth_6mo"],
            "tech_stack_tags": enrichment["tech_stack_tags"],
            "icp_fit_score": enrichment["icp_fit_score"],
            "territory": enrichment["territory"],
        })

    # Tier 3: 400 accounts
    t3_accounts = get_tier3_accounts()
    for idx, company in enumerate(t3_accounts):
        enrichment = _TIER3_ENRICHMENT[idx]
        records.append({
            "company_index": 100 + idx,
            "tier": 3,
            "company_name": company["name"],
            "domain": company["domain"],
            "industry": company["industry"],
            "company_size_band": company["company_size_band"],
            "funding_stage": enrichment["funding_stage"],
            "employee_count_current": enrichment["employee_count_current"],
            "employee_growth_6mo": enrichment["employee_growth_6mo"],
            "tech_stack_tags": enrichment["tech_stack_tags"],
            "icp_fit_score": enrichment["icp_fit_score"],
            "territory": enrichment["territory"],
        })

    return records


def generate_enrichment_files(output_dir):
    """Write account enrichment file to output_dir."""
    os.makedirs(output_dir, exist_ok=True)
    records = _build_enriched_records()

    path = os.path.join(output_dir, "account_enrichment.json")
    with open(path, "w") as f:
        json.dump(records, f, indent=2)

    return {"account_enrichment.json": len(records)}
