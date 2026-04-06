"""
generators/narrative_arcs.py
Defines rep performance curves, sequence event dates, and account story beats.

Each arc modifies a rep's baseline metrics over time to create realistic
longitudinal patterns (ramp-ups, plateaus, PIP recoveries, etc.).

Arc modifiers are centralized here. Individual generators import and apply them.
"""

# Populated in Phase 2
ARC_DEFINITIONS = {}
