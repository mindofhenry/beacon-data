"""
generators/run_all.py
Single entry point for all beacon-data generation.

Usage:
    python -m generators.run_all      (from repo root)
    python generators/run_all.py      (direct)
"""

import os
import random
import sys

# Allow direct execution from generators/ directory
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from generators.config import GLOBAL_SEED, OUTPUT_DIR
from generators.org_structure import generate_org_files
from generators.account_enrichment import generate_enrichment_files
from generators.sequencer_outreach import generate_outreach_files
from generators.salesforce import generate_salesforce_files
from generators.signals import generate_signal_files
from generators.alerts import generate_alert_files


def main():
    # Set global random seed for reproducibility
    random.seed(GLOBAL_SEED)

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 60)
    print("beacon-data — Synthetic Data Generation")
    print("=" * 60)

    # Phase 1: Org structure
    print("\n-- Org Structure --")
    org_counts = generate_org_files(OUTPUT_DIR)
    for filename, count in org_counts.items():
        print(f"  {filename}: {count} records")

    # Phase 1: Account enrichment
    print("\n-- Account Enrichment --")
    enrich_counts = generate_enrichment_files(OUTPUT_DIR)
    for filename, count in enrich_counts.items():
        print(f"  {filename}: {count} records")

    # Phase 2B: Outreach sequencer (weekly cohorts)
    print("\n-- Outreach Sequencer --")
    outreach_counts = generate_outreach_files(OUTPUT_DIR)
    for filename, count in outreach_counts.items():
        print(f"  {filename}: {count} records")

    # Phase 3: Salesforce data
    print("\n-- Salesforce Data --")
    sf_counts = generate_salesforce_files(OUTPUT_DIR)
    for filename, count in sf_counts.items():
        print(f"  {filename}: {count} records")

    # Phase 4: Signal layer
    print("\n-- Signal Layer --")
    sig_counts = generate_signal_files(OUTPUT_DIR)
    for filename, count in sig_counts.items():
        print(f"  {filename}: {count} records")

    # Phase 5: Alert log
    print("\n-- Alert Log --")
    alert_counts = generate_alert_files(OUTPUT_DIR)
    for filename, count in alert_counts.items():
        print(f"  {filename}: {count} records")

    # Summary
    print("\n" + "=" * 60)
    all_counts = [org_counts, enrich_counts, outreach_counts, sf_counts, sig_counts, alert_counts]
    total_files = sum(len(c) for c in all_counts)
    total_records = sum(sum(c.values()) for c in all_counts)
    print(f"Total: {total_files} files, {total_records} records")
    print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    print(f"Account universe: {sf_counts.get('sf_accounts.csv', '?')} accounts "
          f"({sf_counts.get('sf_contacts.csv', '?')} contacts, "
          f"{sf_counts.get('sf_opportunities.csv', '?')} opps)")
    print(f"Signal layer: {sig_counts.get('signal_events.json', '?')} events, "
          f"{sig_counts.get('score_history.json', '?')} scores")
    print(f"Alert log: {alert_counts.get('alert_log.json', '?')} alerts")
    print("Phase 1-5 generation complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
