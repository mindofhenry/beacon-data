# beacon-data — Claude Code Instructions

## What This Is

beacon-data is a standalone repo that generates all synthetic data for the
Beacon platform. Every module — Loop, Signal, Pair, Views — consumes from
beacon-data's output. No module generates its own synthetic data.

This repo replaces the generators currently in `beacon-loop/pipeline/generators/`.
Once beacon-data is validated end-to-end, those generators are deleted.

**The spec is the source of truth.** It defines the org structure, narrative arcs,
output schemas, data volume targets, and build phases. The spec lives in the
parent Claude project. If something isn't in this file, check the spec before
making assumptions.

## Owner

Henry Marble. Former SDR, targeting GTM Engineering roles at Series B–D B2B SaaS.
This is a portfolio project. Do not reference Pave as a current employer or
accessible resource. Do not assume access to any company's proprietary data or tools.

## Tech Stack

- **Language:** Python 3.12 (pure stdlib — zero paid dependencies)
- **Output format:** JSON and CSV files in `output/`
- **No database writes.** beacon-data generates flat files. Consuming repos
  handle database loading.
- **No external APIs.** Everything is deterministic synthetic data generation.

## Repo Structure

```
beacon-data/
├── generators/
│   ├── __init__.py
│   ├── config.py              # time range, constants, random seeds
│   ├── contact_pool.py        # copied from beacon-loop — source of truth (25 Tier 1 accounts)
│   ├── account_universe.py    # Tier 2 (75) + Tier 3 (400) accounts and Tier 2 contacts
│   ├── account_enrichment.py  # funding, ICP, territory, tech stack (all 500 accounts)
│   ├── org_structure.py       # reps, pairings, managers, rep_events (DOOM Inc, @doom.com)
│   ├── narrative_arcs.py      # arc-specific modifiers (rep curves, etc.)
│   ├── sequencer_outreach.py  # longitudinal Outreach data (Outreach only — no Salesloft)
│   ├── salesforce.py          # expanded SF data with realistic timelines (500 accounts)
│   ├── signals.py             # signal_events, score_history, tribal_patterns
│   ├── alerts.py              # alert_log generation
│   └── run_all.py             # single entry point — generates everything
├── output/                    # all generated files land here (gitignored contents)
├── validation/
│   └── validate.py            # summary stats, arc verification, schema checks
├── CLAUDE.md
├── README.md
├── requirements.txt
└── .gitignore
```

## Reference Repo

**beacon-loop** lives at `C:\Dev\beacon-loop`. It contains the generators being
replaced and the contact pool being copied. Read from it for schema reference.
**Do NOT modify any files in beacon-loop.**

Key reference files:
- `beacon-loop/data/shared/contact_pool.py` — 100 contacts × 25 companies
- `beacon-loop/pipeline/generators/outreach_generator.py` — Outreach schema
- `beacon-loop/pipeline/generators/salesloft_generator.py` — Salesloft schema
- `beacon-loop/pipeline/generators/generate_salesforce.py` — Salesforce schema

## Before You Write Any Code

1. **Read this file first.** Every session starts here.
2. **Read the files you are about to edit** — do not assume you know what is there.
3. **Check `output/` schemas** if you're changing generator output — downstream
   consumers depend on exact field names and types.

## planning-with-files — Required Protocol

Maintain three files in the repo root at all times during multi-step work:

- `findings.md` — what you discovered when reading existing code, files,
  schemas, or data. Write before touching anything.
- `progress.md` — step-by-step status: Not Started / In Progress / Done /
  Blocked. Update after every step.
- `task_plan.md` — full task list with step numbers and current status.
  Update after every step.

### Rules
1. Initialize all three files before writing any code. No exceptions.
2. Update all three after each step — not at the end of the session.
3. If blocked, record the blocker in progress.md immediately and stop.
4. Final update to all three files at session end.

These files are gitignored. They are your working memory. Skipping them is
not allowed.

## Hard Rules — Things CC Gets Wrong

### Data

- **All data is synthetic.** No real company data anywhere. Do not attempt to
  connect to real APIs or orgs.
- **Deterministic with controlled variance.** Global random seed (`GLOBAL_SEED = 42`)
  for reproducibility. Per-rep performance modifiers follow defined curves from
  `narrative_arcs.py`, not random walks. Weekly variance is added on top of
  curves, not instead of them.
- **Weekly cohorts, not one-time batches.** Each sequence/cadence gets new
  prospects entering every week across its active period. This is what produces
  longitudinal depth.
- **Time range is fixed:** April 1, 2025 → March 31, 2026. Hard-coded.
  "Today" in demo mode is March 31, 2026. Do not make this configurable.
- **Email address is the join key** between sequencer activity and Salesforce
  contacts. It must be consistent across all synthetic datasets.
- **Attribution chain is explicit:** Contact pool email → sequencer activity →
  SF contact → OpportunityContactRole → opportunity. The chain must be auditable.

### Output

- **All generated files go to `output/`.** No exceptions.
- **Preserve existing schemas.** Output file names and field structures must
  match what beacon-loop currently consumes. New files can add fields, but
  existing fields cannot be renamed or removed.
- **JSON files use pretty-print** (indent=2) for readability.
- **CSV files use standard headers** matching Salesforce field naming conventions.

### Architecture

- **Arc modifiers are centralized.** `narrative_arcs.py` defines ALL rep
  performance curves, sequence event dates, and account story beats. Individual
  generators import and apply them. Do not scatter arc logic across generators.
- **Signal events are pre-scored.** Each signal_event includes `weight_applied`
  and `reason_text` at generation time. Score history is computed by aggregating
  signal events per account per week.
- **Contact pool is sacred.** `contact_pool.py` was copied from beacon-loop and
  is the source of truth for Tier 1 accounts. Do not modify it. Overflow contacts
  (for high-volume sequences) are generated separately and never appear in Salesforce.
- **Account universe has 3 tiers.** Tier 1 (25 accounts, contact_pool.py) has full
  attribution chains. Tier 2 (75 accounts, account_universe.py) has inbound opps
  and 1-3 contacts each. Tier 3 (400 accounts, account_universe.py) has no contacts
  or opps — they exist for signal data in Phase 4.
- **Company is DOOM Inc.** All reps use `@doom.com` email domain. Company name
  and domain are defined in `org_structure.py` as `COMPANY_NAME` and `COMPANY_DOMAIN`.

## Build Phases

Work phases in order. Each phase must be fully validated before starting the next.

1. **Phase 1** — Repo scaffold + org structure + foundation ✅
2. **Phase 2** — Longitudinal sequencer data (Outreach + Salesloft) ✅
3. **Phase 3** — Expanded Salesforce data ✅
4. **Phase 4** — Signal layer (signal_events, score_history, tribal_patterns) ✅
5. **Phase 5** — Alerts, validation, and wiring to beacon-loop

**Phase-gating rule:** Do not start Phase N+1 until Phase N's output files are
generated and validated. "Validated" means: files exist, schemas match the spec,
summary stats are printed and reviewed, and narrative arc data points are
confirmed present.

## Validation Standards

Every phase must print summary stats when generators run. At minimum:
- Record counts per output file
- Key rate checks (open rate ~27%, reply rate ~2.9% Outreach / ~4% Salesloft)
- Per-rep performance breakdowns (to verify arc curves)
- Per-arc verification (rewrite split visible? PIP inflection present?)

### Phase 3 Salesforce Validation Targets (Expanded)
- **Record counts:** accounts (500), contacts (~247), opportunities (~208),
  OCRs (~491), tasks (~1213)
- **Tier structure:** Tier 1 (25, full attribution), Tier 2 (75, inbound opps),
  Tier 3 (400, bulk imports — no contacts/opps)
- **Stage distribution:** ~21% Closed Won, ~12% Closed Lost, ~67% open
- **Tier 2 opps:** 55-65 filler opps, LeadSource = Inbound/Website/Marketing Event/Referral
- **Arc 8:** 12-24 handoff-attributed opps, LeadSource = sequence name
- **Arc 9:** Nate's $180K deal, 1 OCR, 42+ day task gap, Closed Lost
- **Arc 10:** David has 4-OCR deals, Daniel has 5-OCR $350K deal
- **Arc 11:** Keiko has 2 competitive deals (1W, 1L) with ShieldStack mentions
- **Arc 12:** Elena's forecast accuracy ~62-70%
- **Arc 17:** ENT Negotiation dwell >> SMB Negotiation dwell
- **Rep emails:** All 19 reps use @doom.com domain (DOOM Inc)

### Phase 3 Output Files
- `sf_accounts.csv` — 500 accounts (25 T1 + 75 T2 + 400 T3)
- `sf_contacts.csv` — ~247 contacts (100 T1 + ~147 T2)
- `sf_opportunities.csv` — ~208 opps with OwnerId
- `sf_opportunity_contact_roles.csv` — ~491 OCRs
- `sf_tasks.csv` — ~1213 tasks with Description and OwnerId
- `account_enrichment.json` — 500 records with tier field

### Phase 4 Signal Layer Validation Targets
- **Record counts:** signal_events (~5987), score_history (~3017),
  tribal_patterns (7), account_preferences (25)
- **Signal distribution:** T1 high density (35-65/account), T2 medium (20-45),
  T3 low (~38% get 5-20, rest get intent_surge spray only)
- **Arc 5 (Fortress re-engagement):** Zero signals Jul-Aug, 3 minor Sep signals,
  Oct cluster (3 pricing_page_visit + executive_change + content_download).
  Sept 29 score=22, Oct 20 score=87.
- **Arc 6 (Tribal pattern):** tp_001 = Series B + CISO + pricing_page_visit,
  sample_size=8 (qualifying closed-won opps across 6 accounts)
- **Arc 15 (Signal ROI):** job_change ~2.8x, pricing_page_visit ~2.1x,
  intent_surge ~1.1x. Intent sprayed broadly across ~430 accounts.
- **Arc 16 (Scoring drift):** 6 score_history records in Nov with rep_feedback
  on ENT/STRAT accounts (1000+ emp) with no Closed Won opps
- **Date range:** All signals within April 1, 2025 – March 31, 2026

### Phase 4 Output Files
- `signal_events.json` — ~5987 pre-scored signal events
- `score_history.json` — ~3017 weekly score snapshots
- `tribal_patterns.json` — 7 patterns (tp_001 is Arc 6)
- `account_preferences.json` — 25 snooze/override records

## Org Structure Quick Reference

**16 ICs (6 SDRs + 10 AEs) + 3 managers = 19 people total**

| Team | Manager | Reps |
|------|---------|------|
| SDR (6) | mgr_1 Christine Park | sdr_1–sdr_6 |
| AE SMB/MM (6) | mgr_2 James Holloway | ae_1–ae_6 |
| AE ENT/STRAT (4) | mgr_3 Amara Okafor | ae_7–ae_10 |

**6 SDR-to-AE pairs:** sdr_1→ae_1, sdr_2→ae_5, sdr_3→ae_6, sdr_4→ae_3,
sdr_5→ae_7, sdr_6→ae_9

**4 unpaired AEs:** ae_2, ae_4, ae_8, ae_10 (inbound/self-sourced pipeline)

## Common Commands

```bash
# Generate all synthetic data (from repo root)
python -m generators.run_all

# Run validation checks
python -m validation.validate

# Alternative: run directly
python generators/run_all.py
```

## Consumer Pattern

Each consuming repo copies from `beacon-data/output/` into its own
`data/synthetic/` directory:
```bash
# In beacon-loop:
cp ../beacon-data/output/*.json data/synthetic/
cp ../beacon-data/output/*.csv data/synthetic/
```

## Keeping This File Up to Date

Update CLAUDE.md when:
- The repo structure changes (new generator, new output file)
- A new hard rule is identified — something CC got wrong that wasn't covered
- Build phases complete and new validation standards apply
- Common commands change

The rule: **if a future CC session would get it wrong without knowing what you
just built, update this file now.**
