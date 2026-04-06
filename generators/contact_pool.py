"""
data/shared/contact_pool.py
Shared contact pool for all synthetic data generators.

100 B2B security/IT contacts across 25 fictional companies.
Email format: firstname.lastname@companydomain.mock
Title weighting: CISO (25), VP of Security (20), IT Director (15),
                 Head of InfoSec (12), Director of IT (10), VP of IT (8),
                 Security Engineer (6), CTO (4)
"""

# fmt: off

_COMPANIES = [
    # idx | name | domain | industry | size_band
    {"name": "Fortress Security Corp",   "domain": "fortresssecurity.mock",   "industry": "cybersecurity",       "company_size_band": "1001-5000"},  # 0
    {"name": "IronShield Technologies",  "domain": "ironshieldtech.mock",     "industry": "cybersecurity",       "company_size_band": "501-1000"},   # 1
    {"name": "Meridian Financial Group", "domain": "meridianfinancial.mock",  "industry": "financial services",  "company_size_band": "1001-5000"},  # 2
    {"name": "Apex Health Systems",      "domain": "apexhealth.mock",         "industry": "healthcare",          "company_size_band": "501-1000"},   # 3
    {"name": "Nexus SaaS Labs",          "domain": "nexussaas.mock",          "industry": "SaaS",                "company_size_band": "201-500"},    # 4
    {"name": "Hartwell Industries",      "domain": "hartwellindustries.mock", "industry": "manufacturing",       "company_size_band": "1001-5000"},  # 5
    {"name": "ClearPath Health",         "domain": "clearpathealth.mock",     "industry": "healthcare",          "company_size_band": "201-500"},    # 6
    {"name": "Summit Bank & Trust",      "domain": "summitbank.mock",         "industry": "financial services",  "company_size_band": "501-1000"},   # 7
    {"name": "Stackify Inc",             "domain": "stackify.mock",           "industry": "SaaS",                "company_size_band": "201-500"},    # 8
    {"name": "Kestrel Defense Systems",  "domain": "kestreldefense.mock",     "industry": "cybersecurity",       "company_size_band": "501-1000"},   # 9
    {"name": "Pinnacle Manufacturing Co","domain": "pinnaclemfg.mock",        "industry": "manufacturing",       "company_size_band": "1001-5000"},  # 10
    {"name": "Cloudix Corp",             "domain": "cloudix.mock",            "industry": "SaaS",                "company_size_band": "51-200"},     # 11
    {"name": "Vantage Security Group",   "domain": "vantagesec.mock",         "industry": "cybersecurity",       "company_size_band": "201-500"},    # 12
    {"name": "Redrock Financial",        "domain": "redrockfin.mock",         "industry": "financial services",  "company_size_band": "201-500"},    # 13
    {"name": "Medcore Solutions",        "domain": "medcore.mock",            "industry": "healthcare",          "company_size_band": "501-1000"},   # 14
    {"name": "BluePeak Systems",         "domain": "bluepeak.mock",           "industry": "SaaS",                "company_size_band": "51-200"},     # 15
    {"name": "Ironclad Defense",         "domain": "ironclad.mock",           "industry": "cybersecurity",       "company_size_band": "201-500"},    # 16
    {"name": "Harbor Capital Group",     "domain": "harborcapital.mock",      "industry": "financial services",  "company_size_band": "1001-5000"},  # 17
    {"name": "NovaMed Health",           "domain": "novamed.mock",            "industry": "healthcare",          "company_size_band": "201-500"},    # 18
    {"name": "Quickscale Technologies",  "domain": "quickscale.mock",         "industry": "SaaS",                "company_size_band": "51-200"},     # 19
    {"name": "Steelman Industries",      "domain": "steelmanindustries.mock", "industry": "manufacturing",       "company_size_band": "1001-5000"},  # 20
    {"name": "CipherGuard Inc",          "domain": "cipherguard.mock",        "industry": "cybersecurity",       "company_size_band": "51-200"},     # 21
    {"name": "Atlas Financial Services", "domain": "atlasfinancial.mock",     "industry": "financial services",  "company_size_band": "501-1000"},   # 22
    {"name": "WestPoint Healthcare",     "domain": "westpointhealth.mock",    "industry": "healthcare",          "company_size_band": "501-1000"},   # 23
    {"name": "Titan Cloud Corp",         "domain": "titancloud.mock",         "industry": "SaaS",                "company_size_band": "201-500"},    # 24
]

# Role slot assignments (4 people per company = 100 total):
#   Slot 1 (CISO):         all 25 companies
#   Slot 2 (VP Security):  idx 0-19  | IT Director:   idx 20-24
#   Slot 3 (IT Director):  idx 0-9   | Head of InfoSec: idx 10-21 | Dir of IT: idx 22-24
#   Slot 4 (Dir of IT):    idx 0-6   | VP of IT: idx 7-14 | Sec Eng: idx 15-20 | CTO: idx 21-24
#
# Title totals: CISO=25, VP Security=20, IT Dir=15, Head InfoSec=12,
#               Dir IT=10, VP IT=8, Sec Eng=6, CTO=4  → 100

# (company_idx, first_name, last_name, title)
_PEOPLE = [
    # ── Fortress Security Corp (0) ──────────────────────────────────────
    (0,  "James",     "Okafor",      "CISO"),
    (0,  "Rachel",    "Ng",          "VP of Security"),
    (0,  "Derek",     "Moss",        "IT Director"),
    (0,  "Tamara",    "Wells",       "Director of IT"),
    # ── IronShield Technologies (1) ─────────────────────────────────────
    (1,  "Priya",     "Sharma",      "CISO"),
    (1,  "Michael",   "Donovan",     "VP of Security"),
    (1,  "Keiko",     "Yamamoto",    "IT Director"),
    (1,  "Luis",      "Mendez",      "Director of IT"),
    # ── Meridian Financial Group (2) ────────────────────────────────────
    (2,  "Eric",      "Blackwood",   "CISO"),
    (2,  "Anya",      "Petrov",      "VP of Security"),
    (2,  "Thomas",    "Brewer",      "IT Director"),
    (2,  "Grace",     "Ihejirika",   "Director of IT"),
    # ── Apex Health Systems (3) ─────────────────────────────────────────
    (3,  "Carlos",    "Vega",        "CISO"),
    (3,  "Diane",     "Holloway",    "VP of Security"),
    (3,  "Ahmed",     "Saleh",       "IT Director"),
    (3,  "Wendy",     "Park",        "Director of IT"),
    # ── Nexus SaaS Labs (4) ─────────────────────────────────────────────
    (4,  "Brandon",   "Keel",        "CISO"),
    (4,  "Sonia",     "Johansson",   "VP of Security"),
    (4,  "Jin",       "Hu",          "IT Director"),
    (4,  "Natalie",   "Cross",       "Director of IT"),
    # ── Hartwell Industries (5) ─────────────────────────────────────────
    (5,  "Kevin",     "Steele",      "CISO"),
    (5,  "Maya",      "Abramowitz",  "VP of Security"),
    (5,  "Oliver",    "Knox",        "IT Director"),
    (5,  "Irene",     "Cato",        "Director of IT"),
    # ── ClearPath Health (6) ────────────────────────────────────────────
    (6,  "David",     "Otieno",      "CISO"),
    (6,  "Leila",     "Nazari",      "VP of Security"),
    (6,  "Raymond",   "Frost",       "IT Director"),
    (6,  "Yasmin",    "Ali",         "Director of IT"),
    # ── Summit Bank & Trust (7) ─────────────────────────────────────────
    (7,  "Patrick",   "Holt",        "CISO"),
    (7,  "Ingrid",    "Sorensen",    "VP of Security"),
    (7,  "Kwame",     "Asante",      "IT Director"),
    (7,  "Victor",    "Reyes",       "VP of IT"),
    # ── Stackify Inc (8) ────────────────────────────────────────────────
    (8,  "Sarah",     "Wallace",     "CISO"),
    (8,  "Omar",      "Flynn",       "VP of Security"),
    (8,  "Christina", "Chang",       "IT Director"),
    (8,  "Hiroshi",   "Nakamura",    "VP of IT"),
    # ── Kestrel Defense Systems (9) ─────────────────────────────────────
    (9,  "Andre",     "Osei",        "CISO"),
    (9,  "Jennifer",  "Patel",       "VP of Security"),
    (9,  "Brian",     "Morrison",    "IT Director"),
    (9,  "Lisa",      "Fischer",     "VP of IT"),
    # ── Pinnacle Manufacturing Co (10) ──────────────────────────────────
    (10, "Santiago",  "Cardenas",    "CISO"),
    (10, "Emily",     "Tanaka",      "VP of Security"),
    (10, "Marcus",    "Cohen",       "Head of InfoSec"),
    (10, "Nadia",     "Eze",         "VP of IT"),
    # ── Cloudix Corp (11) ───────────────────────────────────────────────
    (11, "Tyler",     "Gonzalez",    "CISO"),
    (11, "Aysha",     "Yoon",        "VP of Security"),
    (11, "Robert",    "Nguyen",      "Head of InfoSec"),
    (11, "Diana",     "Kumar",       "VP of IT"),
    # ── Vantage Security Group (12) ─────────────────────────────────────
    (12, "Fatima",    "Mbeki",       "CISO"),
    (12, "Tyler",     "Fitzpatrick", "VP of Security"),
    (12, "Maria",     "Larsson",     "Head of InfoSec"),
    (12, "Robert",    "Castillo",    "VP of IT"),
    # ── Redrock Financial (13) ──────────────────────────────────────────
    (13, "Kwame",     "Boateng",     "CISO"),
    (13, "Emily",     "Weiss",       "VP of Security"),
    (13, "Omar",      "Espinoza",    "Head of InfoSec"),
    (13, "Ling",      "Nakagawa",    "VP of IT"),
    # ── Medcore Solutions (14) ──────────────────────────────────────────
    (14, "Andre",     "Adeyemi",     "CISO"),
    (14, "Christina", "Dubois",      "VP of Security"),
    (14, "Santiago",  "Okonkwo",     "Head of InfoSec"),
    (14, "Sonia",     "Krishnan",    "VP of IT"),
    # ── BluePeak Systems (15) ───────────────────────────────────────────
    (15, "Marcus",    "Fitzgerald",  "CISO"),
    (15, "Nadia",     "Adeyemi",     "VP of Security"),
    (15, "James",     "Flynn",       "Head of InfoSec"),
    (15, "Rachel",    "Castillo",    "Security Engineer"),
    # ── Ironclad Defense (16) ───────────────────────────────────────────
    (16, "Diana",     "Kim",         "CISO"),
    (16, "Carlos",    "Weiss",       "VP of Security"),
    (16, "Ingrid",    "Okafor",      "Head of InfoSec"),
    (16, "Kevin",     "Nakamura",    "Security Engineer"),
    # ── Harbor Capital Group (17) ───────────────────────────────────────
    (17, "Thomas",    "Osei",        "CISO"),
    (17, "Priya",     "Yoon",        "VP of Security"),
    (17, "Brandon",   "Larsson",     "Head of InfoSec"),
    (17, "Leila",     "Gonzalez",    "Security Engineer"),
    # ── NovaMed Health (18) ─────────────────────────────────────────────
    (18, "Victor",    "Eze",         "CISO"),
    (18, "Tamara",    "Morrison",    "VP of Security"),
    (18, "Derek",     "Cohen",       "Head of InfoSec"),
    (18, "Jennifer",  "Liu",         "Security Engineer"),
    # ── Quickscale Technologies (19) ────────────────────────────────────
    (19, "Grace",     "Donovan",     "CISO"),
    (19, "Michael",   "Asante",      "VP of Security"),
    (19, "Fatima",    "Reyes",       "Head of InfoSec"),
    (19, "Brian",     "Petrov",      "Security Engineer"),
    # ── Steelman Industries (20) ────────────────────────────────────────
    (20, "Anya",      "Fischer",     "CISO"),
    (20, "Omar",      "Saleh",       "IT Director"),
    (20, "Luis",      "Watkins",     "Head of InfoSec"),
    (20, "Yasmin",    "Brewer",      "Security Engineer"),
    # ── CipherGuard Inc (21) ────────────────────────────────────────────
    (21, "Patrick",   "Nakagawa",    "CISO"),
    (21, "Rachel",    "Okonkwo",     "IT Director"),
    (21, "Santiago",  "Hayashi",     "Head of InfoSec"),
    (21, "Diane",     "Espinoza",    "CTO"),
    # ── Atlas Financial Services (22) ───────────────────────────────────
    (22, "Jin",       "Morrison",    "CISO"),
    (22, "Sarah",     "Vega",        "IT Director"),
    (22, "Hiroshi",   "Boateng",     "Director of IT"),
    (22, "Nadia",     "Fitzgerald",  "CTO"),
    # ── WestPoint Healthcare (23) ───────────────────────────────────────
    (23, "Robert",    "Holt",        "CISO"),
    (23, "Maria",     "Mbeki",       "IT Director"),
    (23, "Tyler",     "Adeyemi",     "Director of IT"),
    (23, "Christina", "Kim",         "CTO"),
    # ── Titan Cloud Corp (24) ───────────────────────────────────────────
    (24, "Kevin",     "Dubois",      "CISO"),
    (24, "Leila",     "Fitzgerald",  "IT Director"),
    (24, "Marcus",    "Yoon",        "Director of IT"),
    (24, "Sonia",     "Okafor",      "CTO"),
]

# fmt: on

CONTACT_POOL = [
    {
        "email": f"{first.lower()}.{last.lower()}@{_COMPANIES[cidx]['domain']}",
        "first_name": first,
        "last_name": last,
        "title": title,
        "company": _COMPANIES[cidx]["name"],
        "industry": _COMPANIES[cidx]["industry"],
        "company_size_band": _COMPANIES[cidx]["company_size_band"],
    }
    for cidx, first, last, title in _PEOPLE
]

assert len(CONTACT_POOL) == 100, f"Expected 100 contacts, got {len(CONTACT_POOL)}"
assert len({c["email"] for c in CONTACT_POOL}) == 100, "Duplicate emails in CONTACT_POOL"
