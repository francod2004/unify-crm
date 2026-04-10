#!/usr/bin/env python3
"""
Caliber Lead Sourcer Agent v2.0
================================
Multi-source lead sourcing across the GTA. Scrapes:
  - YellowPages.ca
  - Google Maps (via Google Places text search)
  - Yelp.ca
  - Bing Places
  - BBB (Better Business Bureau)
  - 411.ca

Enriches leads from business websites, filters out chains/franchises,
deduplicates against Supabase CRM, and texts Franco when a batch is ready.

RULE: Never source a lead without an owner/contact name.

Usage:
    python lead_sourcer.py                     # Run with defaults
    python lead_sourcer.py --vertical Restaurants --area "Brampton, ON" --max 10
    python lead_sourcer.py --dry-run           # Preview without writing to DB

Requires env vars or .env file -- see .env.template
"""

import os, sys, re, json, time, random, argparse, hashlib, uuid
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# -- Configuration ------------------------------------------------------------

def load_env(path=".env"):
    """Load key=value pairs from .env file if it exists."""
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

load_env()

SUPABASE_URL  = os.getenv("SUPABASE_URL", "https://alfzjwzeccqswtytcylo.supabase.co")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY", "")
TWILIO_SID    = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN  = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM   = os.getenv("TWILIO_FROM", "")   # Your Twilio phone number
FRANCO_PHONE  = os.getenv("FRANCO_PHONE", "")   # Franco's cell

# -- Search Parameters --------------------------------------------------------

VERTICALS = {
    "Restaurants": [
        "restaurant", "cafe", "bakery", "pizzeria", "sushi restaurant",
        "bar and grill", "catering", "food truck", "diner", "bistro",
        "brunch spot", "steakhouse", "thai restaurant", "indian restaurant",
        "italian restaurant", "mexican restaurant", "bbq restaurant",
    ],
    "Retail": [
        "boutique", "clothing store", "gift shop", "jewelry store",
        "pet store", "florist", "furniture store", "shoe store",
        "home decor store", "sporting goods store", "vintage shop",
        "bridal shop", "optical store", "luggage store",
    ],
    "Trades": [
        "plumber", "electrician", "HVAC contractor", "roofing contractor",
        "landscaping company", "painting contractor", "general contractor",
        "handyman service", "fence installer", "garage door repair",
        "pest control", "tree service", "pool company", "paving contractor",
        "foundation repair", "waterproofing company", "septic service",
    ],
    "Dental & Medical": [
        "dentist", "dental clinic", "chiropractor", "physiotherapy clinic",
        "optometrist", "veterinary clinic", "walk-in clinic",
        "dermatologist", "orthodontist", "massage therapy clinic",
    ],
    "Salons & Spas": [
        "hair salon", "barbershop", "nail salon", "med spa",
        "beauty salon", "tanning salon", "day spa", "waxing studio",
        "lash studio", "tattoo shop",
    ],
    "Professional Services": [
        "law firm", "accounting firm", "insurance agency",
        "real estate agency", "mortgage broker", "financial advisor",
        "tax preparation", "notary public", "immigration consultant",
    ],
    "Fitness & Wellness": [
        "gym", "fitness studio", "yoga studio", "pilates studio",
        "crossfit gym", "martial arts studio", "personal training",
        "dance studio", "swimming school",
    ],
    "Auto Services": [
        "auto repair shop", "car detailing", "tire shop",
        "auto body shop", "oil change", "car wash",
        "transmission repair", "muffler shop",
    ],
    "Cleaning & Property": [
        "cleaning company", "janitorial service", "carpet cleaning",
        "window cleaning company", "property management company",
        "moving company", "junk removal", "storage facility",
    ],
}

GTA_AREAS = [
    # Core Toronto
    "Toronto, ON", "Scarborough, ON", "Etobicoke, ON", "North York, ON",
    # Peel Region
    "Brampton, ON", "Mississauga, ON", "Caledon, ON", "Bolton, ON",
    # York Region
    "Vaughan, ON", "Markham, ON", "Richmond Hill, ON",
    "Newmarket, ON", "Aurora, ON", "Stouffville, ON", "King City, ON",
    # Halton Region
    "Oakville, ON", "Burlington, ON", "Milton, ON", "Georgetown, ON",
    "Halton Hills, ON", "Acton, ON",
    # Durham Region
    "Ajax, ON", "Pickering, ON", "Oshawa, ON", "Whitby, ON",
    "Clarington, ON", "Bowmanville, ON", "Uxbridge, ON",
    # ~80km radius expansions
    "Hamilton, ON", "Stoney Creek, ON", "Ancaster, ON", "Dundas, ON",
    "Grimsby, ON", "St. Catharines, ON", "Niagara Falls, ON",
    "Welland, ON", "Niagara-on-the-Lake, ON",
    "Guelph, ON", "Kitchener, ON", "Waterloo, ON", "Cambridge, ON",
    "Barrie, ON", "Innisfil, ON", "Orillia, ON", "Alliston, ON",
    "Orangeville, ON", "Shelburne, ON",
    "Cobourg, ON", "Port Hope, ON", "Peterborough, ON",
    "Brantford, ON", "Woodstock, ON", "Simcoe, ON",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# -- Chain / Franchise Blocklist ----------------------------------------------

CHAIN_KEYWORDS = {
    # Fast food / QSR chains
    "mcdonald", "burger king", "wendy", "subway", "tim horton", "tims",
    "starbucks", "dunkin", "popeyes", "chick-fil-a", "kfc", "taco bell",
    "pizza hut", "domino", "little caesars", "papa john", "five guys",
    "chipotle", "panera", "panda express", "arby", "sonic drive",
    "dairy queen", "baskin robbins", "cold stone", "auntie anne",
    "harvey", "mary brown", "swiss chalet", "st-hubert",
    "a&w", "new york fries", "mr. sub", "mr sub",
    # Casual dining chains
    "boston pizza", "east side mario", "montana", "the keg", "milestones",
    "jack astor", "kelsey", "casey", "moxie", "earls", "cactus club",
    "joey restaurant", "red lobster", "olive garden", "applebee",
    "denny", "ihop", "waffle house", "cheesecake factory",
    "the works", "wild wing", "buffalo wild wings", "wingstop",
    "freshii", "qdoba", "nando",
    # Coffee chains
    "second cup", "timothy", "balzac", "mccafe",
    # Grocery / retail chains
    "sobeys", "loblaws", "metro", "food basics", "freshco", "no frills",
    "walmart", "costco", "real canadian superstore", "superstore",
    "shoppers drug mart", "rexall", "dollarama", "dollar tree",
    "canadian tire", "home depot", "lowe", "rona", "home hardware",
    "winners", "marshalls", "homesense", "value village",
    "old navy", "gap", "h&m", "zara", "forever 21", "uniqlo",
    "best buy", "staples", "the source", "bed bath",
    "petsmart", "pet valu", "petcetera",
    "indigo", "chapters",
    "lcbo", "beer store",
    # Trade chains / big contractors
    "mr. rooter", "mr rooter", "roto-rooter", "roto rooter",
    "mr. electric", "mr electric", "molly maid", "merry maids",
    "servpro", "servicemaster", "home instead",
    # Dental / medical chains
    "dentalcorp", "123 dentist", "appletree medical",
    # Salon / spa chains
    "great clips", "supercuts", "first choice haircutters",
    "sport clips", "fantastic sams", "mastercuts",
    # Fitness chains
    "goodlife fitness", "planet fitness", "anytime fitness",
    "la fitness", "fit4less", "orangetheory", "f45 training",
    "curves", "snap fitness",
    # Auto chains
    "mr. lube", "mr lube", "jiffy lube", "midas", "meineke",
    "speedy auto", "canadian tire auto", "kal tire",
    # Cleaning chains
    "servicemaster clean", "jan-pro", "coverall", "openworks",
    # Insurance / finance chains
    "state farm", "desjardins", "intact insurance",
    "allstate", "sun life", "manulife",
    "remax", "re/max", "royal lepage", "century 21", "keller williams",
    "coldwell banker", "sutton group",
    # Banks / insurance / corporate
    "td bank", "rbc", "bmo", "scotiabank", "cibc",
}

def is_chain_or_franchise(name):
    """Check if a business name matches a known chain or franchise."""
    name_lower = name.lower().strip()
    for keyword in CHAIN_KEYWORDS:
        if keyword in name_lower:
            return True
    franchise_patterns = [
        r'#\d+',
        r'store\s*#?\d+',
        r'location\s*#?\d+',
        r'unit\s*#?\d+',
    ]
    for pattern in franchise_patterns:
        if re.search(pattern, name_lower):
            return True
    return False

def clean_business_name(name):
    """Clean up scraped business name."""
    name = re.sub(r'^\d+', '', name).strip()
    name = re.sub(
        r'\s*-\s*(Toronto|Brampton|Mississauga|Vaughan|Markham|Scarborough|Etobicoke|North York'
        r'|Hamilton|Barrie|Guelph|Kitchener|Waterloo|Cambridge|Oshawa|Burlington|Oakville'
        r'|St\.? Catharines|Niagara Falls|Peterborough|Brantford|Whitby|Ajax|Pickering)\s*$',
        '', name, flags=re.I
    )
    return name.strip()

# -- Supabase Helpers ---------------------------------------------------------

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

def sb_get_existing_names():
    """Fetch all existing prospect names for dedup."""
    url = f"{SUPABASE_URL}/rest/v1/prospects?select=name"
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return {row["name"].strip().lower() for row in r.json()}
    print(f"  Warning: Could not fetch existing prospects: {r.status_code}")
    return set()

def sb_insert_prospects(prospects):
    """Insert a batch of prospects into Supabase. Returns count inserted."""
    if not prospects:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/prospects"
    headers = sb_headers()
    headers["Prefer"] = "return=representation"
    r = requests.post(url, headers=headers, json=prospects, timeout=30)
    if r.status_code in (200, 201):
        return len(prospects)
    print(f"  Warning: Supabase insert error {r.status_code}: {r.text[:200]}")
    return 0

# -- Twilio SMS Helper --------------------------------------------------------

def send_sms(body):
    """Send an SMS via Twilio REST API."""
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, FRANCO_PHONE]):
        print("  Warning: Twilio not configured -- skipping SMS")
        print(f"  Message would be:\n     {body}")
        return False
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    r = requests.post(
        url,
        auth=(TWILIO_SID, TWILIO_TOKEN),
        data={"From": TWILIO_FROM, "To": FRANCO_PHONE, "Body": body},
        timeout=15,
    )
    if r.status_code == 201:
        print(f"  SMS sent to {FRANCO_PHONE}")
        return True
    print(f"  Warning: SMS failed ({r.status_code}): {r.text[:200]}")
    return False


# ==============================================================================
# SOURCE 1: YellowPages.ca
# ==============================================================================

YP_SEARCH_TERMS = {
    "Restaurants": ["Restaurants", "Cafes", "Bakeries", "Pizza", "Catering", "Steakhouse", "Brunch"],
    "Retail": ["Boutiques", "Clothing+Stores", "Gift+Shops", "Pet+Stores", "Florists", "Furniture+Store"],
    "Trades": ["Plumbers", "Electricians", "HVAC", "Roofing", "Landscaping", "Painters", "Pest+Control"],
    "Dental & Medical": ["Dentists", "Dental+Clinic", "Chiropractors", "Physiotherapy", "Veterinarians", "Optometrists"],
    "Salons & Spas": ["Hair+Salons", "Barbershops", "Nail+Salons", "Day+Spas", "Beauty+Salons", "Med+Spa"],
    "Professional Services": ["Law+Firms", "Accounting+Firms", "Insurance+Agency", "Real+Estate+Agency", "Mortgage+Broker"],
    "Fitness & Wellness": ["Gyms", "Fitness+Studio", "Yoga+Studio", "Martial+Arts", "Dance+Studio", "Personal+Training"],
    "Auto Services": ["Auto+Repair", "Car+Detailing", "Tire+Shop", "Auto+Body+Shop", "Car+Wash"],
    "Cleaning & Property": ["Cleaning+Company", "Janitorial+Services", "Property+Management", "Moving+Company", "Junk+Removal"],
}

def scrape_yellowpages(search_term, area, max_results=10):
    """Scrape YellowPages.ca for business listings."""
    results = []
    location = area.replace(", ", "+").replace(" ", "+")
    url = f"https://www.yellowpages.ca/search/si/1/{quote_plus(search_term)}/{location}"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text
        print(f"     [YP] response: {r.status_code}, {len(html)} chars")

        soup = BeautifulSoup(html, "lxml")
        listings = soup.select("div.listing, div.listing__content, div[class*='listing']")
        if not listings:
            listings = soup.select("div.resultList div, div.result")

        for listing in listings[:max_results]:
            name_el = listing.select_one(
                "a.listing__name--link, h3.listing__name, "
                "a[class*='listing__name'], span.listing__name, h2 a, h3 a"
            )
            if not name_el:
                continue
            raw_name = name_el.get_text(strip=True)
            if not raw_name or len(raw_name) < 3:
                continue

            name = clean_business_name(raw_name)
            if not name or is_chain_or_franchise(name):
                if name:
                    print(f"   [YP] Filtered chain: {name}")
                continue

            # Address
            addr_el = listing.select_one(
                "span.listing__address--full, span[class*='address'], "
                "div.listing__address, span.adr"
            )
            address = addr_el.get_text(strip=True) if addr_el else ""

            # Phone
            phone = ""
            phone_el = listing.select_one(
                "a[class*='phone'], span[class*='phone'], "
                "a[data-phone], a[href^='tel:'], "
                "span.mlr__sub-text, li.mlr__item--phone, span.listing__phone"
            )
            if phone_el:
                tel_href = phone_el.get("href", "")
                if tel_href.startswith("tel:"):
                    phone = tel_href.replace("tel:", "").strip()
                elif phone_el.get("data-phone"):
                    phone = phone_el.get("data-phone")
                else:
                    phone = phone_el.get_text(strip=True)
            if not phone:
                for a in listing.select("a[href^='tel:']"):
                    tel = a.get("href", "").replace("tel:", "").strip()
                    if len(tel) >= 10:
                        phone = tel
                        break
            if not phone:
                all_text = listing.get_text(" ", strip=True)
                phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', all_text)
                if phone_match:
                    phone = phone_match.group(1)
            phone = re.sub(r'[^\d+()-.\s]', '', phone).strip()

            # Website
            website = ""
            web_el = listing.select_one(
                "a[class*='website'], a[data-analytics='website'], a.listing__link--website"
            )
            if web_el:
                website = web_el.get("href", "")
            if not website:
                for a in listing.select("a[href^='http']"):
                    href = a.get("href", "")
                    if "yellowpages.ca" not in href and "ypcdn" not in href:
                        website = href
                        break

            # Category / snippet
            cat_el = listing.select_one("span[class*='category'], div[class*='category']")
            snippet = cat_el.get_text(strip=True) if cat_el else ""

            results.append({
                "name": name, "address": address, "phone": phone,
                "website": website, "snippet": snippet[:200], "source": "YellowPages"
            })

    except Exception as e:
        print(f"  [YP] scrape error: {e}")

    return results


# ==============================================================================
# SOURCE 2: Google Maps (via maps search results page scraping)
# ==============================================================================

def scrape_google_maps(search_term, area, max_results=10):
    """
    Scrape Google search results for local businesses.
    We search Google with the query "search_term near area" and parse
    the local pack / organic results for business info.
    """
    results = []
    query = f"{search_term} near {area}"
    url = f"https://www.google.com/search?q={quote_plus(query)}&num=20&gl=ca&hl=en"

    try:
        headers = {**HEADERS, "Accept": "text/html,application/xhtml+xml"}
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        html = r.text
        print(f"     [Google] response: {r.status_code}, {len(html)} chars")

        soup = BeautifulSoup(html, "lxml")

        # Parse local pack results (the map section)
        # Google local pack entries are often in divs with data-attrid or specific classes
        local_results = soup.select("div.VkpGBb, div[data-local-attribute], div.rllt__details")

        for item in local_results[:max_results]:
            # Business name
            name_el = item.select_one("div.dbg0pd, span.OSrXXb, div[role='heading']")
            if not name_el:
                name_el = item.select_one("a[data-ved]")
            if not name_el:
                continue

            name = clean_business_name(name_el.get_text(strip=True))
            if not name or len(name) < 3 or is_chain_or_franchise(name):
                continue

            # Address (usually in a secondary span)
            address = ""
            addr_candidates = item.select("span, div.rllt__details div")
            for ac in addr_candidates:
                txt = ac.get_text(strip=True)
                # Look for text that contains area-ish keywords or has a postal code
                if re.search(r'(ON|Ontario|\d{3}\s*\w{3}|Street|Ave|Rd|Dr|Blvd)', txt, re.I):
                    address = txt
                    break

            # Phone
            phone = ""
            all_text = item.get_text(" ", strip=True)
            phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', all_text)
            if phone_match:
                phone = phone_match.group(1)

            # Website (extract from any link that isn't google)
            website = ""
            for a in item.select("a[href^='http']"):
                href = a.get("href", "")
                if "google.com" not in href and "gstatic" not in href:
                    website = href
                    break

            results.append({
                "name": name, "address": address, "phone": phone,
                "website": website, "snippet": "", "source": "Google"
            })

        # Also try parsing organic results if local pack is empty
        if not results:
            for div in soup.select("div.g, div[data-sokoban-container]")[:max_results]:
                title_el = div.select_one("h3")
                if not title_el:
                    continue
                name = clean_business_name(title_el.get_text(strip=True))
                if not name or len(name) < 3 or is_chain_or_franchise(name):
                    continue

                link_el = div.select_one("a[href^='http']")
                website = link_el.get("href", "") if link_el else ""
                if "google.com" in website:
                    website = ""

                snippet_el = div.select_one("div.VwiC3b, span.st")
                snippet = snippet_el.get_text(strip=True)[:200] if snippet_el else ""

                phone = ""
                phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', snippet)
                if phone_match:
                    phone = phone_match.group(1)

                results.append({
                    "name": name, "address": "", "phone": phone,
                    "website": website, "snippet": snippet, "source": "Google"
                })

    except Exception as e:
        print(f"  [Google] scrape error: {e}")

    return results


# ==============================================================================
# SOURCE 3: Yelp.ca
# ==============================================================================

YELP_SEARCH_TERMS = {
    "Restaurants": ["restaurants", "cafes", "bakeries", "pizza", "catering"],
    "Retail": ["boutiques", "clothing", "gift+shops", "pet+stores", "florists"],
    "Trades": ["plumbers", "electricians", "hvac", "roofing", "landscaping"],
}

def scrape_yelp(search_term, area, max_results=10):
    """Scrape Yelp.ca for business listings."""
    results = []
    location = area.replace(", ", "+").replace(" ", "+")
    url = f"https://www.yelp.ca/search?find_desc={quote_plus(search_term)}&find_loc={quote_plus(area)}"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text
        print(f"     [Yelp] response: {r.status_code}, {len(html)} chars")

        soup = BeautifulSoup(html, "lxml")

        # Yelp uses various div containers for search results
        # Try multiple selector strategies
        listings = soup.select(
            "div[data-testid='serp-ia-card'], "
            "li.border-color--default__09f24__BAILS, "
            "div.container__09f24__FeTO6, "
            "div[class*='businessName'], "
            "div.arrange-unit__09f24__rqHTg"
        )

        # If targeted selectors fail, use a broader approach
        if not listings:
            # Look for links to business pages on Yelp
            biz_links = soup.select("a[href*='/biz/']")
            seen_names = set()
            for link in biz_links[:max_results * 2]:
                name = link.get_text(strip=True)
                name = clean_business_name(name)
                if not name or len(name) < 3 or name.lower() in seen_names:
                    continue
                if is_chain_or_franchise(name):
                    continue
                seen_names.add(name.lower())

                # Try to find parent container for more info
                parent = link.find_parent("div")
                address = ""
                phone = ""
                if parent:
                    text = parent.get_text(" ", strip=True)
                    phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', text)
                    if phone_match:
                        phone = phone_match.group(1)
                    addr_match = re.search(r'(\d+\s+[\w\s]+(?:St|Ave|Rd|Dr|Blvd|Cres|Way|Ct)[\w\s,]*ON)', text, re.I)
                    if addr_match:
                        address = addr_match.group(1).strip()

                biz_href = link.get("href", "")
                website = ""
                if biz_href.startswith("/biz/"):
                    # We could follow the Yelp biz page, but that's extra requests
                    pass

                results.append({
                    "name": name, "address": address, "phone": phone,
                    "website": website, "snippet": "", "source": "Yelp"
                })
                if len(results) >= max_results:
                    break
        else:
            for listing in listings[:max_results]:
                # Try to extract name
                name_el = listing.select_one(
                    "a[href*='/biz/'], h3, span[data-testid='serp-ia-card-title']"
                )
                if not name_el:
                    continue
                name = clean_business_name(name_el.get_text(strip=True))
                if not name or len(name) < 3 or is_chain_or_franchise(name):
                    continue

                all_text = listing.get_text(" ", strip=True)

                phone = ""
                phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', all_text)
                if phone_match:
                    phone = phone_match.group(1)

                address = ""
                addr_match = re.search(r'(\d+\s+[\w\s]+(?:St|Ave|Rd|Dr|Blvd|Cres|Way|Ct)[\w\s,]*ON)', all_text, re.I)
                if addr_match:
                    address = addr_match.group(1).strip()

                results.append({
                    "name": name, "address": address, "phone": phone,
                    "website": "", "snippet": "", "source": "Yelp"
                })

    except Exception as e:
        print(f"  [Yelp] scrape error: {e}")

    return results


# ==============================================================================
# SOURCE 4: Bing Places
# ==============================================================================

def scrape_bing_places(search_term, area, max_results=10):
    """Scrape Bing local search for business listings."""
    results = []
    query = f"{search_term} near {area}"
    url = f"https://www.bing.com/search?q={quote_plus(query)}&setmkt=en-CA"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text
        print(f"     [Bing] response: {r.status_code}, {len(html)} chars")

        soup = BeautifulSoup(html, "lxml")

        # Bing local results
        local_items = soup.select(
            "div.b_scard, div.local-card, li.b_algo, "
            "div[data-partnertag*='local'], div.b_locald"
        )

        for item in local_items[:max_results]:
            name_el = item.select_one("h2 a, h3 a, a.tilk, div.lc_content h2")
            if not name_el:
                name_el = item.select_one("a[href]")
            if not name_el:
                continue

            name = clean_business_name(name_el.get_text(strip=True))
            if not name or len(name) < 3 or is_chain_or_franchise(name):
                continue

            all_text = item.get_text(" ", strip=True)

            # Phone
            phone = ""
            phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', all_text)
            if phone_match:
                phone = phone_match.group(1)

            # Address
            address = ""
            addr_match = re.search(
                r'(\d+\s+[\w\s]+(?:St|Ave|Rd|Dr|Blvd|Cres|Way|Ct|Lane|Pkwy|Hwy)[\w\s.,]*(?:ON|Ontario))',
                all_text, re.I
            )
            if addr_match:
                address = addr_match.group(1).strip()

            # Website
            website = ""
            for a in item.select("a[href^='http']"):
                href = a.get("href", "")
                if "bing.com" not in href and "microsoft" not in href:
                    website = href
                    break

            results.append({
                "name": name, "address": address, "phone": phone,
                "website": website, "snippet": "", "source": "Bing"
            })

    except Exception as e:
        print(f"  [Bing] scrape error: {e}")

    return results


# ==============================================================================
# SOURCE 5: BBB (Better Business Bureau)
# ==============================================================================

BBB_CATEGORIES = {
    "Restaurants": "restaurant",
    "Retail": "retail-stores",
    "Trades": "contractors-general",
}

def scrape_bbb(search_term, area, max_results=10):
    """Scrape BBB for accredited business listings."""
    results = []
    # BBB search URL format
    city = area.split(",")[0].strip()
    province = area.split(",")[1].strip() if "," in area else "ON"
    url = (
        f"https://www.bbb.org/search?find_country=CAN&find_entity=0032-000"
        f"&find_text={quote_plus(search_term)}"
        f"&find_loc={quote_plus(city)}&find_state={quote_plus(province)}"
        f"&page=1&sort=Relevance"
    )

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text
        print(f"     [BBB] response: {r.status_code}, {len(html)} chars")

        soup = BeautifulSoup(html, "lxml")

        # BBB result cards
        listings = soup.select(
            "div.search-result, a.text-blue-medium, "
            "div[class*='result-item'], div.bds-body"
        )

        # Broader approach: find all links to BBB profile pages
        biz_links = soup.select("a[href*='/profile/']")
        seen = set()

        for link in biz_links[:max_results * 2]:
            name = link.get_text(strip=True)
            name = clean_business_name(name)
            if not name or len(name) < 3 or name.lower() in seen:
                continue
            if is_chain_or_franchise(name):
                continue
            seen.add(name.lower())

            parent = link.find_parent("div")
            phone = ""
            address = ""
            if parent:
                text = parent.get_text(" ", strip=True)
                phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', text)
                if phone_match:
                    phone = phone_match.group(1)
                addr_match = re.search(
                    r'(\d+\s+[\w\s]+(?:St|Ave|Rd|Dr|Blvd|Cres|Way)[\w\s.,]*)',
                    text, re.I
                )
                if addr_match:
                    address = addr_match.group(1).strip()

            results.append({
                "name": name, "address": address, "phone": phone,
                "website": "", "snippet": "BBB Listed", "source": "BBB"
            })
            if len(results) >= max_results:
                break

    except Exception as e:
        print(f"  [BBB] scrape error: {e}")

    return results


# ==============================================================================
# SOURCE 6: 411.ca
# ==============================================================================

def scrape_411ca(search_term, area, max_results=10):
    """Scrape 411.ca Canadian business directory."""
    results = []
    city = area.split(",")[0].strip()
    url = (
        f"https://411.ca/search/"
        f"?q={quote_plus(search_term)}"
        f"&l={quote_plus(city + ' ON')}"
        f"&t=business"
    )

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text
        print(f"     [411] response: {r.status_code}, {len(html)} chars")

        soup = BeautifulSoup(html, "lxml")

        # 411.ca listing cards
        listings = soup.select(
            "div.listing, div.vcard, div.result-card, "
            "div[class*='listing'], article"
        )

        for listing in listings[:max_results]:
            name_el = listing.select_one(
                "h2 a, h3 a, a.listing-name, span.fn, "
                "a[class*='name'], div.listing-title a"
            )
            if not name_el:
                continue
            name = clean_business_name(name_el.get_text(strip=True))
            if not name or len(name) < 3 or is_chain_or_franchise(name):
                continue

            all_text = listing.get_text(" ", strip=True)

            # Phone
            phone = ""
            phone_el = listing.select_one("a[href^='tel:']")
            if phone_el:
                phone = phone_el.get("href", "").replace("tel:", "").strip()
            if not phone:
                phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', all_text)
                if phone_match:
                    phone = phone_match.group(1)

            # Address
            address = ""
            addr_el = listing.select_one("span.adr, div[class*='address'], span.street-address")
            if addr_el:
                address = addr_el.get_text(strip=True)
            if not address:
                addr_match = re.search(
                    r'(\d+\s+[\w\s]+(?:St|Ave|Rd|Dr|Blvd|Cres|Way)[\w\s.,]*)',
                    all_text, re.I
                )
                if addr_match:
                    address = addr_match.group(1).strip()

            # Website
            website = ""
            for a in listing.select("a[href^='http']"):
                href = a.get("href", "")
                if "411.ca" not in href:
                    website = href
                    break

            results.append({
                "name": name, "address": address, "phone": phone,
                "website": website, "snippet": "", "source": "411.ca"
            })

    except Exception as e:
        print(f"  [411] scrape error: {e}")

    return results


# ==============================================================================
# WEBSITE ENRICHMENT (unchanged from v1.1)
# ==============================================================================

def enrich_from_website(url):
    """
    Visit a business website and try to extract:
    - Email address
    - Phone number
    - Owner/contact name
    """
    info = {"email": "", "phone": "", "owner": ""}
    if not url:
        return info

    try:
        r = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        text = r.text[:80_000]
        soup = BeautifulSoup(text, "lxml")

        # -- Email --
        for a in soup.select("a[href^='mailto:']"):
            email = a.get("href", "").replace("mailto:", "").split("?")[0].strip()
            if "@" in email and "example" not in email:
                info["email"] = email
                break

        if not info["email"]:
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            skip = ["example.com", "sentry.io", "wixpress", "googleapis",
                    "wordpress", "w3.org", "schema.org", "gravatar",
                    "jquery", "cloudflare", "google", "facebook"]
            emails = [e for e in emails if not any(s in e.lower() for s in skip)]
            if emails:
                info["email"] = emails[0]

        # -- Phone --
        for a in soup.select("a[href^='tel:']"):
            tel = a.get("href", "").replace("tel:", "").strip()
            if len(re.sub(r'\D', '', tel)) >= 10:
                info["phone"] = tel
                break
        if not info["phone"]:
            phones = re.findall(r'\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}', text)
            if phones:
                info["phone"] = phones[0]

        # -- Owner Name --
        # Method 1: Meta tags
        for tag in soup.select("meta[name='author'], meta[property='article:author']"):
            content = tag.get("content", "").strip()
            if content and len(content) < 50 and " " in content:
                info["owner"] = content
                break

        # Method 2: Text patterns
        owner_patterns = [
            r'(?:owner|proprietor|founded by|chef|operated by|managed by)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            r'(?:owner|proprietor|founded by|chef)[:\s]+([A-Z][a-z]+)',
            r'(?:Hi,?\s+I\'?m|Meet)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
            r'(?:Hi,?\s+I\'?m|Meet)\s+([A-Z][a-z]+)',
        ]
        if not info["owner"]:
            visible_text = soup.get_text(" ", strip=True)[:10_000]
            for pattern in owner_patterns:
                match = re.search(pattern, visible_text)
                if match:
                    candidate = match.group(1).strip()
                    if 2 < len(candidate) < 40 and not any(c.isdigit() for c in candidate):
                        info["owner"] = candidate
                        break

        # Method 3: Check About page
        if not info["owner"] and url.rstrip("/").count("/") <= 3:
            about_urls = [
                url.rstrip("/") + "/about",
                url.rstrip("/") + "/about-us",
                url.rstrip("/") + "/our-story",
            ]
            for about_url in about_urls:
                try:
                    r2 = requests.get(about_url, headers=HEADERS, timeout=8, allow_redirects=True)
                    if r2.status_code == 200 and len(r2.text) > 500:
                        about_text = BeautifulSoup(r2.text[:30_000], "lxml").get_text(" ", strip=True)[:5000]
                        for pattern in owner_patterns:
                            match = re.search(pattern, about_text)
                            if match:
                                candidate = match.group(1).strip()
                                if 2 < len(candidate) < 40 and not any(c.isdigit() for c in candidate):
                                    info["owner"] = candidate
                                    break
                        if info["owner"]:
                            break
                except Exception:
                    pass

    except Exception:
        pass

    return info


# -- Prospect Builder ---------------------------------------------------------

AI_GAPS_BY_VERTICAL = {
    "Restaurants": "AI booking, review response, menu optimization, inventory forecasting",
    "Retail": "AI inventory mgmt, customer chatbot, personalized marketing, POS analytics",
    "Trades": "AI scheduling & dispatch, automated quoting, review mgmt, lead follow-up",
    "Dental & Medical": "AI appointment booking, patient reminders, intake forms, review mgmt",
    "Salons & Spas": "AI online booking, no-show prediction, client retention, social media",
    "Professional Services": "AI client intake, document automation, scheduling, follow-up emails",
    "Fitness & Wellness": "AI class scheduling, member retention, billing automation, lead nurture",
    "Auto Services": "AI appointment booking, parts inventory, customer follow-up, estimates",
    "Cleaning & Property": "AI scheduling & routing, quoting, customer portal, invoice automation",
}

def _ai_gap_for_vertical(vertical, area):
    """Generate a specific AI gap description based on the vertical."""
    gaps = AI_GAPS_BY_VERTICAL.get(vertical, "AI automation opportunity")
    city = area.split(",")[0]
    return f"AI automation opportunity â {vertical.lower()} in {city}. Gaps: {gaps}"

def build_prospect(raw, vertical, area):
    """Convert raw scraped data into a CRM prospect record."""
    now = datetime.now(timezone.utc).isoformat()
    source = raw.get("source", "Unknown")
    return {
        "id": str(uuid.uuid4()),
        "name": raw["name"][:100],
        "cat": vertical,
        "status": "NOT CONTACTED",
        "address": raw.get("address", "")[:200] or area,
        "phone": raw.get("phone", ""),
        "email": raw.get("email", ""),
        "owner": raw.get("owner", ""),
        "opp": _ai_gap_for_vertical(vertical, area),
        "action": "Research & qualify",
        "notes": f"[Auto-sourced {datetime.now().strftime('%Y-%m-%d')} via {source}] {raw.get('snippet', '')[:120]}",
        "last_contact": None,
        "date": None,
        "activities": json.dumps([]),
        "created_at": now,
        "updated_at": now,
    }


# -- Multi-Source Scraper Dispatcher ------------------------------------------

def scrape_all_sources(search_term, area, max_results=5):
    """
    Run all 6 scrapers for a given search term + area.
    Returns a combined, name-deduplicated list.
    Merges data when the same business appears in multiple sources.
    """
    all_results = []
    seen_names = {}  # norm_name -> index in all_results

    # Define which sources to use and how
    sources = [
        ("YellowPages", lambda: scrape_yellowpages(search_term, area, max_results)),
        ("Google",       lambda: scrape_google_maps(search_term, area, max_results)),
        ("Yelp",         lambda: scrape_yelp(search_term, area, max_results)),
        ("Bing",         lambda: scrape_bing_places(search_term, area, max_results)),
        ("BBB",          lambda: scrape_bbb(search_term, area, max_results)),
        ("411.ca",       lambda: scrape_411ca(search_term, area, max_results)),
    ]

    for source_name, scrape_fn in sources:
        try:
            results = scrape_fn()
            print(f"   [{source_name}] returned {len(results)} results")

            for item in results:
                norm = item["name"].strip().lower()
                if norm in seen_names:
                    # Merge: fill in missing fields from this source
                    idx = seen_names[norm]
                    existing = all_results[idx]
                    if not existing.get("phone") and item.get("phone"):
                        existing["phone"] = item["phone"]
                    if not existing.get("address") and item.get("address"):
                        existing["address"] = item["address"]
                    if not existing.get("website") and item.get("website"):
                        existing["website"] = item["website"]
                    if not existing.get("email") and item.get("email"):
                        existing["email"] = item["email"]
                    # Track that multiple sources found this business
                    existing["source"] = existing.get("source", "") + f"+{source_name}"
                else:
                    seen_names[norm] = len(all_results)
                    all_results.append(item)

        except Exception as e:
            print(f"   [{source_name}] failed: {e}")

        # Small delay between sources to be polite
        time.sleep(random.uniform(1.0, 2.5))

    return all_results


# -- Main Agent Logic ---------------------------------------------------------

def run_agent(verticals=None, areas=None, max_per_search=5, dry_run=False):
    """
    Main entry point. Searches 6 sources for leads, deduplicates,
    enriches from websites, writes to Supabase, and notifies Franco.
    """
    verticals = verticals or list(VERTICALS.keys())
    areas = areas or GTA_AREAS

    print("=" * 60)
    print("  Caliber Lead Sourcer Agent v2.0 (Multi-Source)")
    print("=" * 60)
    print(f"  Verticals : {', '.join(verticals)}")
    print(f"  Areas     : {len(areas)} GTA locations")
    print(f"  Max/search: {max_per_search}")
    print(f"  Sources   : YellowPages, Google, Yelp, Bing, BBB, 411.ca")
    print(f"  Dry run   : {dry_run}")
    print(f"  Filter    : Chains BLOCKED, no-name SKIPPED")
    print(f"  Supabase  : {'Connected' if SUPABASE_KEY else 'No key'}")
    print(f"  Twilio    : {'Configured' if TWILIO_SID else 'Skipped'}")
    print()

    # Step 1: Get existing prospects for deduplication
    existing = set()
    if not dry_run and SUPABASE_KEY:
        print("Fetching existing prospects for dedup...")
        existing = sb_get_existing_names()
        print(f"   Found {len(existing)} existing prospects\n")

    # Step 2: Multi-source scraping
    all_leads = []
    searches_done = 0
    sources_hit = {"YellowPages": 0, "Google": 0, "Yelp": 0, "Bing": 0, "BBB": 0, "411.ca": 0}

    for v_name in verticals:
        # Pick search terms for this vertical
        yp_terms = YP_SEARCH_TERMS.get(v_name, [v_name])
        sample_size = min(3, len(yp_terms))
        chosen_terms = random.sample(yp_terms, sample_size)

        for search_term in chosen_terms:
            # Pick random areas to search
            chosen_areas = random.sample(areas, min(5, len(areas)))

            for area in chosen_areas:
                print(f"\n{'='*50}")
                print(f"SEARCH: {search_term} in {area}")
                print(f"{'='*50}")

                # Run ALL sources for this search term + area
                raw_results = scrape_all_sources(search_term, area, max_results=max_per_search)
                print(f"\n   Combined: {len(raw_results)} unique businesses from all sources")

                for raw in raw_results:
                    # Dedup against existing CRM
                    norm_name = raw["name"].strip().lower()
                    if norm_name in existing:
                        print(f"   SKIP duplicate: {raw['name']}")
                        continue

                    # Enrich from website
                    if raw.get("website"):
                        print(f"   Enriching: {raw['name']}")
                        extra = enrich_from_website(raw["website"])
                        raw["email"] = raw.get("email") or extra["email"]
                        raw["phone"] = raw.get("phone") or extra["phone"]
                        raw["owner"] = raw.get("owner") or extra["owner"]

                    # FILTER: Skip leads with no owner/contact name
                    # Franco's rule: "IF it doesn't have a name its useless to me"
                    if not raw.get("owner", "").strip():
                        print(f"   SKIP (no contact name): {raw['name']}")
                        continue

                    # Track source stats
                    for src in raw.get("source", "").split("+"):
                        src = src.strip()
                        if src in sources_hit:
                            sources_hit[src] += 1

                    prospect = build_prospect(raw, v_name, area)
                    all_leads.append(prospect)
                    existing.add(norm_name)

                searches_done += 1
                # Rate limit between search combos
                delay = random.uniform(2.0, 4.0)
                print(f"   Waiting {delay:.1f}s...\n")
                time.sleep(delay)

    # Step 3: Summary
    print("\n" + "=" * 60)
    print(f"  Sourcing Complete")
    print(f"     Searches run    : {searches_done}")
    print(f"     New leads       : {len(all_leads)}")
    print(f"     Source breakdown : {json.dumps(sources_hit)}")
    print(f"     Chains filtered : (see logs above)")
    print("=" * 60)

    if not all_leads:
        print("\n  No new leads found this run.")
        return

    # Print preview
    print("\n  Preview of new leads:")
    for i, p in enumerate(all_leads[:15], 1):
        email_flag = "E" if p["email"] else " "
        phone_flag = "P" if p["phone"] else " "
        owner_flag = "O" if p["owner"] else " "
        print(f"   {i:>2}. [{email_flag}{phone_flag}{owner_flag}] {p['name'][:35]:<35} | {p['cat']:<22} | {p['owner'][:20]}")
    if len(all_leads) > 15:
        print(f"   ... and {len(all_leads) - 15} more")

    # Step 4: Write to Supabase
    if dry_run:
        print("\n  Dry run -- nothing written to database.")
        with open("leads_preview.json", "w") as f:
            json.dump(all_leads, f, indent=2)
        print("  Preview saved to leads_preview.json")
        return

    if not SUPABASE_KEY:
        print("\n  No SUPABASE_KEY -- saving to leads_export.json instead")
        with open("leads_export.json", "w") as f:
            json.dump(all_leads, f, indent=2)
        return

    print(f"\n  Writing {len(all_leads)} prospects to Supabase...")
    inserted = 0
    for i in range(0, len(all_leads), 25):
        batch = all_leads[i:i+25]
        count = sb_insert_prospects(batch)
        inserted += count
        if count:
            print(f"     Batch {i//25 + 1}: {count} inserted")
        else:
            print(f"     Batch {i//25 + 1}: failed")

    print(f"\n  Total inserted: {inserted}/{len(all_leads)}")

    # Step 5: Notify Franco (always send summary, even if 0 new)
    cats = {}
    for p in all_leads:
        cats[p["cat"]] = cats.get(p["cat"], 0) + 1
    breakdown = ", ".join(f"{v} {k}" for k, v in cats.items())

    if inserted > 0:
        msg = (
            f"Caliber: {inserted} new leads added ({breakdown}). "
            f"Sources: YP/Google/Yelp/Bing/BBB/411. "
            f"Review: synapse-crm-coral.vercel.app"
        )
    else:
        msg = (
            f"Caliber: Lead sourcer ran â {len(all_leads)} leads found but "
            f"all were duplicates (already in CRM). "
            f"No new additions. Next run may use different search terms."
        )
    print(f"\n  Notifying Franco...")
    send_sms(msg)

    print("\n  Agent run complete.")


# -- CLI ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Caliber Lead Sourcer Agent v2.0")
    parser.add_argument("--vertical", "-v", nargs="+",
                        choices=list(VERTICALS.keys()),
                        help="Which verticals to search (default: all)")
    parser.add_argument("--area", "-a", nargs="+",
                        help="Specific areas to search (default: all GTA)")
    parser.add_argument("--max", "-m", type=int, default=5,
                        help="Max results per search query (default: 5)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Preview results without writing to DB or sending SMS")
    args = parser.parse_args()

    run_agent(
        verticals=args.vertical,
        areas=args.area,
        max_per_search=args.max,
        dry_run=args.dry_run,
    )

if __name__ == "__main__":
    main()
