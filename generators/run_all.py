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

    # Summary
    print("\n" + "=" * 60)
    total_files = len(org_counts) + len(enrich_counts) + len(outreach_counts)
    total_records = (
        sum(org_counts.values())
        + sum(enrich_counts.values())
        + sum(outreach_counts.values())
    )
    print(f"Total: {total_files} files, {total_records} records")
    print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    print("Phase 1 + Phase 2B generation complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
