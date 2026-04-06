"""
generators/account_universe.py
Generates Tier 2 (75) and Tier 3 (400) accounts plus Tier 2 contacts (~150).

Tier 1 accounts (25) come from contact_pool.py and are NOT duplicated here.
This module only generates the expansion universe.

Tier 2: Inbound/self-sourced accounts with 1-3 contacts each.
Tier 3: Bulk-imported accounts (Clay/ZoomInfo). No contacts, no opps.
"""

import random

from .config import GLOBAL_SEED
from .contact_pool import _COMPANIES as TIER1_COMPANIES

_rng = random.Random(GLOBAL_SEED + 100)  # offset seed to avoid collisions

# ── Name component pools ─────────────────────────────────────────────────────
# Used to deterministically generate plausible B2B company names.

_PREFIXES = [
    "Apex", "Vanguard", "Stratos", "Caliber", "Vertex", "Sentinel",
    "Evergreen", "Northstar", "Citadel", "Argon", "Paladin", "Trident",
    "Elevate", "Cascade", "Ridgeline", "Clearwater", "Ironbridge", "Optima",
    "Summit", "Quantum", "Aegis", "Nebula", "Polaris", "Keystone", "Helix",
    "Prism", "Cobalt", "Onyx", "Ember", "Crest", "Zenith", "Pinnacle",
    "Equinox", "Radian", "Axiom", "Fulcrum", "Bastion", "Lumen", "Aether",
    "Nimbus", "Trellis", "Sable", "Sterling", "Riviera", "Cordova",
    "Maverick", "Obsidian", "Halcyon", "Ardent", "Vector", "Paragon",
    "Ascent", "Fortis", "Veridian", "Talon", "Radix", "Solace", "Vigil",
    "Cipher", "Atlas", "Beacon", "Nova", "Orion", "Drake", "Flint",
    "Granite", "Haven", "Ionic", "Juno", "Kinetic", "Lyric", "Metric",
    "Neon", "Opal", "Pike", "Quasar", "Relay", "Sigma", "Terra",
    "Unity", "Volta", "Wren", "Xenon", "Yield", "Zephyr",
]

_SUFFIXES = [
    "Systems", "Technologies", "Solutions", "Group", "Corp", "Labs",
    "Networks", "Security", "Analytics", "Dynamics", "Software", "Digital",
    "Platforms", "Services", "Innovations", "Consulting", "Partners",
    "Enterprises", "Industries", "Capital", "Ventures", "Health",
    "Financial", "Logistics", "Engineering", "Data", "Cloud",
    "Intelligence", "Automation", "Works",
]

_FIRST_NAMES = [
    "Aaron", "Abigail", "Adam", "Adrian", "Aisha", "Alan", "Alexandra",
    "Amara", "Andrea", "Angela", "Anthony", "Beatrice", "Benjamin",
    "Bianca", "Blake", "Brenda", "Cameron", "Carmen", "Carolyn",
    "Catherine", "Charles", "Charlotte", "Clara", "Claudia", "Connor",
    "Cynthia", "Damian", "Dana", "Daphne", "Darren", "Deborah", "Dennis",
    "Diana", "Dominic", "Dorothy", "Douglas", "Dylan", "Edward", "Elaine",
    "Elijah", "Emma", "Ethan", "Eva", "Fernando", "Fiona", "Frances",
    "Frank", "Gabriel", "Garrett", "George", "Gloria", "Gregory", "Gwen",
    "Hannah", "Harold", "Helen", "Henry", "Holly", "Ian", "Irene",
    "Isaac", "Isabella", "Ivan", "Jacqueline", "Jamal", "Janet", "Jason",
    "Jean", "Jerome", "Jessica", "Joan", "Joel", "Jonathan", "Jordan",
    "Joseph", "Joyce", "Julia", "Julian", "Karen", "Keith", "Kelly",
    "Kenneth", "Kira", "Kyle", "Laura", "Leonard", "Linda", "Logan",
    "Lois", "Lucas", "Lydia", "Malcolm", "Margaret", "Martin", "Maxine",
    "Maya", "Melissa", "Michelle", "Monica", "Morgan", "Nadine", "Nancy",
    "Nathan", "Neil", "Nicole", "Noah", "Nora", "Oliver", "Olivia",
    "Oscar", "Patricia", "Paul", "Paula", "Peter", "Philip", "Rachel",
    "Ralph", "Rebecca", "Regina", "Renee", "Richard", "Rita", "Robert",
    "Robin", "Rodney", "Rosa", "Russell", "Ruth", "Samantha", "Sandra",
    "Sean", "Sharon", "Sheila", "Simon", "Sonia", "Sophie", "Stanley",
    "Stephanie", "Steven", "Susan", "Sylvia", "Teresa", "Tiffany",
    "Timothy", "Todd", "Tony", "Vanessa", "Veronica", "Victor",
    "Virginia", "Walter", "Warren", "Wayne", "Wendy", "Wesley",
    "Whitney", "William", "Xavier", "Yolanda", "Zachary",
]

_LAST_NAMES = [
    "Adams", "Allen", "Anderson", "Baker", "Barnes", "Bell", "Bennett",
    "Bishop", "Black", "Blake", "Bradley", "Brooks", "Brown", "Bryant",
    "Burke", "Burns", "Butler", "Campbell", "Carter", "Chapman", "Chen",
    "Clark", "Cole", "Collins", "Cook", "Cooper", "Craig", "Crawford",
    "Cruz", "Curtis", "Daniels", "Davis", "Dean", "Diaz", "Dixon",
    "Duncan", "Edwards", "Ellis", "Evans", "Ferguson", "Fisher",
    "Fleming", "Foster", "Fox", "Freeman", "Garcia", "Gibson", "Gordon",
    "Graham", "Grant", "Gray", "Green", "Griffin", "Hall", "Hamilton",
    "Hansen", "Harper", "Harris", "Hart", "Hayes", "Henderson",
    "Hernandez", "Hill", "Holland", "Holmes", "Howard", "Hughes",
    "Hunt", "Hunter", "Jackson", "James", "Jenkins", "Johnson",
    "Jones", "Jordan", "Kelly", "Kennedy", "Kim", "King", "Knight",
    "Lambert", "Lane", "Lawrence", "Lee", "Lewis", "Long", "Lopez",
    "Marshall", "Martin", "Martinez", "Mason", "Matthews", "McCarthy",
    "McDonald", "Meyer", "Miller", "Mitchell", "Moore", "Morgan",
    "Morris", "Murphy", "Murray", "Nelson", "Newman", "Nguyen",
    "Nichols", "O'Brien", "Oliver", "Owens", "Palmer", "Parker",
    "Patel", "Patterson", "Perez", "Perry", "Peterson", "Phillips",
    "Pierce", "Porter", "Powell", "Price", "Quinn", "Ramirez",
    "Reed", "Reynolds", "Richards", "Richardson", "Riley", "Rivera",
    "Roberts", "Robinson", "Rodriguez", "Rogers", "Rose", "Ross",
    "Russell", "Ryan", "Sanders", "Schmidt", "Scott", "Shaw", "Silva",
    "Simmons", "Singh", "Smith", "Spencer", "Stevens", "Stewart",
    "Stone", "Sullivan", "Taylor", "Thomas", "Thompson", "Torres",
    "Turner", "Walker", "Wallace", "Walsh", "Ward", "Warren",
    "Washington", "Watson", "Webb", "Wells", "West", "White",
    "Williams", "Wilson", "Wood", "Wright", "Young",
]

_TITLES = [
    "CISO", "VP of Security", "IT Director", "Head of InfoSec",
    "Director of IT", "VP of IT", "Security Engineer", "CTO",
    "VP of Engineering", "Director of Security Operations",
]

# Title weights for contact generation (similar to contact_pool distribution)
_TITLE_WEIGHTS = [20, 15, 15, 12, 10, 8, 8, 5, 4, 3]

_INDUSTRIES = ["cybersecurity", "SaaS", "financial services", "healthcare", "manufacturing"]

_SIZE_BANDS = ["51-200", "201-500", "501-1000", "1001-5000", "5000+"]

# ── Tier 3 distribution targets ──────────────────────────────────────────────

_T3_INDUSTRY_WEIGHTS = {
    "cybersecurity": 0.25,
    "SaaS": 0.25,
    "financial services": 0.20,
    "healthcare": 0.15,
    "manufacturing": 0.15,
}

_T3_SIZE_WEIGHTS = {
    "51-200": 0.30,
    "201-500": 0.30,
    "501-1000": 0.20,
    "1001-5000": 0.15,
    "5000+": 0.05,
}


# ── Generation helpers ────────────────────────────────────────────────────────

def _make_domain(name):
    """Convert company name to a .mock domain."""
    # Take first two words, lowercase, no spaces/punctuation
    parts = name.lower().replace("&", "").replace("'", "").split()
    slug = "".join(parts[:2])
    return f"{slug}.mock"


def _generate_companies(n, start_seed, existing_domains):
    """Generate n unique company dicts with name, domain, industry, size_band."""
    rng = random.Random(start_seed)
    companies = []
    used_names = set()
    used_domains = set(existing_domains)

    # Pre-shuffle name pools deterministically
    prefixes = _PREFIXES[:]
    suffixes = _SUFFIXES[:]
    rng.shuffle(prefixes)
    rng.shuffle(suffixes)

    idx = 0
    attempts = 0
    while len(companies) < n and attempts < n * 10:
        attempts += 1
        p = prefixes[idx % len(prefixes)]
        s = suffixes[(idx * 7 + idx // len(prefixes)) % len(suffixes)]
        idx += 1

        name = f"{p} {s}"
        domain = _make_domain(name)

        if name in used_names or domain in used_domains:
            continue

        used_names.add(name)
        used_domains.add(domain)
        companies.append({"name": name, "domain": domain})

    return companies


def _assign_industry_size(companies, industry_weights, size_weights, seed):
    """Assign industry and size_band to companies based on target distributions."""
    rng = random.Random(seed)

    # Build industry slots
    industry_slots = []
    for industry, weight in industry_weights.items():
        count = round(weight * len(companies))
        industry_slots.extend([industry] * count)
    # Fill any rounding gap
    while len(industry_slots) < len(companies):
        industry_slots.append(rng.choice(list(industry_weights.keys())))
    industry_slots = industry_slots[:len(companies)]
    rng.shuffle(industry_slots)

    # Build size_band slots
    size_slots = []
    for band, weight in size_weights.items():
        count = round(weight * len(companies))
        size_slots.extend([band] * count)
    while len(size_slots) < len(companies):
        size_slots.append(rng.choice(list(size_weights.keys())))
    size_slots = size_slots[:len(companies)]
    rng.shuffle(size_slots)

    for i, co in enumerate(companies):
        co["industry"] = industry_slots[i]
        co["company_size_band"] = size_slots[i]


def _generate_contacts(companies, seed):
    """Generate 1-3 contacts per company. Returns list of contact dicts."""
    rng = random.Random(seed)
    contacts = []
    used_emails = set()

    # Pre-shuffle name pools
    firsts = _FIRST_NAMES[:]
    lasts = _LAST_NAMES[:]
    rng.shuffle(firsts)
    rng.shuffle(lasts)

    name_idx = 0

    for co in companies:
        n_contacts = rng.randint(1, 3)
        for _ in range(n_contacts):
            # Pick title weighted
            title = rng.choices(_TITLES, weights=_TITLE_WEIGHTS, k=1)[0]

            # Pick name deterministically
            first = firsts[name_idx % len(firsts)]
            last = lasts[(name_idx * 3 + 1) % len(lasts)]
            name_idx += 1

            email = f"{first.lower()}.{last.lower()}@{co['domain']}"
            # Handle duplicates by adding a number suffix
            if email in used_emails:
                email = f"{first.lower()}.{last.lower()}2@{co['domain']}"
            used_emails.add(email)

            contacts.append({
                "email": email,
                "first_name": first,
                "last_name": last,
                "title": title,
                "company": co["name"],
                "industry": co["industry"],
                "company_size_band": co["company_size_band"],
            })

    return contacts


# ── Tier 2/3 industry and size distributions ─────────────────────────────────
# Tier 2 mirrors Tier 1 distribution roughly but with more SMB/MM

_T2_INDUSTRY_WEIGHTS = {
    "cybersecurity": 0.25,
    "SaaS": 0.25,
    "financial services": 0.20,
    "healthcare": 0.15,
    "manufacturing": 0.15,
}

_T2_SIZE_WEIGHTS = {
    "51-200": 0.25,
    "201-500": 0.35,
    "501-1000": 0.25,
    "1001-5000": 0.12,
    "5000+": 0.03,
}


# ── Build the universe ────────────────────────────────────────────────────────

# Collect existing domains to avoid collisions
_existing_domains = {co["domain"] for co in TIER1_COMPANIES}

# Tier 2: 75 companies
_TIER2_COMPANIES = _generate_companies(75, GLOBAL_SEED + 200, _existing_domains)
_assign_industry_size(_TIER2_COMPANIES, _T2_INDUSTRY_WEIGHTS, _T2_SIZE_WEIGHTS, GLOBAL_SEED + 201)

# Tier 2 contacts (~150 total, 1-3 per company)
_TIER2_CONTACTS = _generate_contacts(_TIER2_COMPANIES, GLOBAL_SEED + 300)

# Tier 3: 400 companies
_t3_existing = _existing_domains | {co["domain"] for co in _TIER2_COMPANIES}
_TIER3_COMPANIES = _generate_companies(400, GLOBAL_SEED + 400, _t3_existing)
_assign_industry_size(_TIER3_COMPANIES, _T3_INDUSTRY_WEIGHTS, _T3_SIZE_WEIGHTS, GLOBAL_SEED + 401)


# ── Public API ────────────────────────────────────────────────────────────────

def get_tier2_accounts():
    """Return list of 75 Tier 2 company dicts."""
    return _TIER2_COMPANIES


def get_tier2_contacts():
    """Return list of ~150 Tier 2 contact dicts."""
    return _TIER2_CONTACTS


def get_tier3_accounts():
    """Return list of 400 Tier 3 company dicts."""
    return _TIER3_COMPANIES


def get_all_accounts():
    """Return combined list: Tier 1 (25) + Tier 2 (75) + Tier 3 (400) = 500."""
    tier1 = [
        {
            "name": co["name"],
            "domain": co["domain"],
            "industry": co["industry"],
            "company_size_band": co["company_size_band"],
        }
        for co in TIER1_COMPANIES
    ]
    return tier1 + list(_TIER2_COMPANIES) + list(_TIER3_COMPANIES)
