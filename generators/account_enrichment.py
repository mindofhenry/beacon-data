"""
generators/account_enrichment.py
Enriches the 25 companies from contact_pool.py with funding, ICP fit,
territory, employee counts, growth rates, and tech stack tags.
"""

import json
import os

from generators.contact_pool import _COMPANIES

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


def _build_enriched_records():
    """Merge company base data with enrichment fields."""
    records = []
    for idx, company in enumerate(_COMPANIES):
        enrichment = _ENRICHMENT[idx]
        records.append({
            "company_index": idx,
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
