#!/usr/bin/env python3
"""
Caliber Lead Sourcer Agent v1.1
================================
Finds privately-owned SMB prospects across the GTA, enriches them,
writes to Supabase, and texts Franco when a batch is ready for review.

Filters out chains, franchises, and large corporations — targets
independent businesses doing roughly $15M revenue or less.

Usage:
    python lead_sourcer.py                     # Run with defaults
    python lead_sourcer.py --vertical Restaurants --area "Brampton, ON" --max 10
    python lead_sourcer.py --dry-run           # Preview without writing to DB

Requires a .env file (or exported env vars) — see .env.template
"""

import os, sys, re, json, time, random, argparse, hashlib, uuid
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

# ── Configuration ────────────────────────────────────────────────────────────

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

# ── Search Parameters ────────────────────────────────────────────────────────

VERTICALS = {
    "Restaurants": [
        "restaurant", "cafe", "bakery", "pizzeria", "sushi restaurant",
        "bar and grill", "catering", "food truck", "diner", "bistro"
    ],
    "Retail": [
        "boutique", "clothing store", "gift shop", "jewelry store",
        "pet store", "florist", "furniture store", "shoe store"
    ],
    "Trades": [
        "plumber", "electrician", "HVAC contractor", "roofing contractor",
        "landscaping company", "painting contractor", "general contractor",
        "handyman service", "fence installer", "garage door repair"
    ],
}

GTA_AREAS = [
    "Toronto, ON", "Brampton, ON", "Mississauga, ON", "Vaughan, ON",
    "Markham, ON", "Richmond Hill, ON", "Oakville, ON", "Burlington, ON",
    "Caledon, ON", "Bolton, ON", "Ajax, ON", "Pickering, ON",
    "Oshawa, ON", "Newmarket, ON", "Aurora, ON", "Milton, ON",
    "Georgetown, ON", "Scarborough, ON", "Etobicoke, ON", "North York, ON",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Chain / Franchise Blocklist ──────────────────────────────────────────────
# These are chains, franchises, or large corporations — not our target market.
# We want privately-owned independents doing ~$15M revenue or less.

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
    # Banks / insurance / corporate
    "td bank", "rbc", "bmo", "scotiabank", "cibc",
}

def is_chain_or_franchise(name):
    """Check if a business name matches a known chain or franchise."""
    name_lower = name.lower().strip()
    for keyword in CHAIN_KEYWORDS:
        if keyword in name_lower:
            return True
    # Also flag if name contains common franchise indicators
    franchise_patterns = [
        r'#\d+',           # Store numbers like "#1234"
        r'store\s*#?\d+',  # "Store 45" or "Store #45"
        r'location\s*#?\d+',
        r'unit\s*#?\d+',
    ]
    for pattern in franchise_patterns:
        if re.search(pattern, name_lower):
            return True
    return False

def clean_business_name(name):
    """Clean up scraped business name — remove leading numbers, extra whitespace."""
    # Remove leading digits (YellowPages ranking numbers like "1Pizza Gigi")
    name = re.sub(r'^\d+', '', name).strip()
    # Remove trailing location info that sometimes gets appended
    name = re.sub(r'\s*-\s*(Toronto|Brampton|Mississauga|Vaughan|Markham|Scarborough|Etobicoke|North York)\s*$', '', name, flags=re.I)
    return name.strip()

# ── Supabase Helpers ─────────────────────────────────────────────────────────

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
    print(f"  ⚠ Could not fetch existing prospects: {r.status_code}")
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
    print(f"  ⚠ Supabase insert error {r.status_code}: {r.text[:200]}")
    return 0

# ── Twilio SMS Helper ────────────────────────────────────────────────────────

def send_sms(body):
    """Send an SMS via Twilio REST API (no SDK needed)."""
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, FRANCO_PHONE]):
        print("  ⚠ Twilio not configured — skipping SMS")
        print(f"  📋 Message would be:\n     {body}")
        return False
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    r = requests.post(
        url,
        auth=(TWILIO_SID, TWILIO_TOKEN),
        data={"From": TWILIO_FROM, "To": FRANCO_PHONE, "Body": body},
        timeout=15,
    )
    if r.status_code == 201:
        print(f"  ✅ SMS sent to {FRANCO_PHONE}")
        return True
    print(f"  ⚠ SMS failed ({r.status_code}): {r.text[:200]}")
    return False

# ── Web Scraping Engine ──────────────────────────────────────────────────────
# Scrapes YellowPages.ca — a Canadian business directory with structured
# listings (name, address, phone, website).

# Map our vertical names to YellowPages search terms
YP_SEARCH_TERMS = {
    "Restaurants": ["Restaurants", "Cafes", "Bakeries", "Pizza", "Catering"],
    "Retail": ["Boutiques", "Clothing+Stores", "Gift+Shops", "Pet+Stores", "Florists"],
    "Trades": ["Plumbers", "Electricians", "HVAC", "Roofing", "Landscaping", "Painters"],
}

def area_to_yp_location(area):
    """Convert 'Toronto, ON' to 'Toronto+ON' for YellowPages URL."""
    return area.replace(", ", "+").replace(" ", "+")

def scrape_yellowpages(search_term, area, max_results=10):
    """
    Scrape YellowPages.ca for business listings.
    Returns a list of dicts: {name, address, phone, website, snippet}
    """
    results = []
    location = area_to_yp_location(area)
    url = f"https://www.yellowpages.ca/search/si/1/{quote_plus(search_term)}/{location}"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        html = r.text
        print(f"     YP response: {r.status_code}, {len(html)} chars")

        soup = BeautifulSoup(html, "lxml")

        # YellowPages listings are in div.listing or similar containers
        listings = soup.select("div.listing, div.listing__content, div[class*='listing']")

        if not listings:
            listings = soup.select("div.resultList div, div.result")

        for listing in listings[:max_results]:
            # ── Business Name ──
            name_el = listing.select_one(
                "a.listing__name--link, h3.listing__name, "
                "a[class*='listing__name'], span.listing__name, "
                "h2 a, h3 a"
            )
            if not name_el:
                continue
            raw_name = name_el.get_text(strip=True)
            if not raw_name or len(raw_name) < 3:
                continue

            name = clean_business_name(raw_name)
            if not name:
                continue

            # ── Chain Filter ──
            if is_chain_or_franchise(name):
                print(f"   🚫 Filtered chain: {name}")
                continue

            # ── Address ──
            addr_el = listing.select_one(
                "span.listing__address--full, span[class*='address'], "
                "div.listing__address, span.adr"
            )
            address = addr_el.get_text(strip=True) if addr_el else ""

            # ── Phone Number ──
            # Strategy 1: Look for phone-specific elements
            phone = ""
            phone_el = listing.select_one(
                "a[class*='phone'], span[class*='phone'], "
                "a[data-phone], a[href^='tel:'], "
                "span.mlr__sub-text, li.mlr__item--phone, "
                "span.listing__phone"
            )
            if phone_el:
                # Check href="tel:..." first (most reliable)
                tel_href = phone_el.get("href", "")
                if tel_href.startswith("tel:"):
                    phone = tel_href.replace("tel:", "").strip()
                # Check data-phone attribute
                elif phone_el.get("data-phone"):
                    phone = phone_el.get("data-phone")
                else:
                    phone = phone_el.get_text(strip=True)

            # Strategy 2: Search all tel: links in the listing
            if not phone:
                for a in listing.select("a[href^='tel:']"):
                    tel = a.get("href", "").replace("tel:", "").strip()
                    if len(tel) >= 10:
                        phone = tel
                        break

            # Strategy 3: Regex the entire listing text
            if not phone:
                all_text = listing.get_text(" ", strip=True)
                phone_match = re.search(r'(\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4})', all_text)
                if phone_match:
                    phone = phone_match.group(1)

            # Clean phone format
            phone = re.sub(r'[^\d+()-.\s]', '', phone).strip()

            # ── Website ──
            website = ""
            web_el = listing.select_one(
                "a[class*='website'], a[data-analytics='website'], "
                "a.listing__link--website"
            )
            if web_el:
                website = web_el.get("href", "")
            if not website:
                for a in listing.select("a[href^='http']"):
                    href = a.get("href", "")
                    if "yellowpages.ca" not in href and "ypcdn" not in href:
                        website = href
                        break

            # ── Category / snippet ──
            cat_el = listing.select_one("span[class*='category'], div[class*='category']")
            snippet = cat_el.get_text(strip=True) if cat_el else ""

            results.append({
                "name": name,
                "address": address,
                "phone": phone,
                "website": website,
                "snippet": snippet[:200],
            })

    except Exception as e:
        print(f"  ⚠ YellowPages scrape error: {e}")

    return results

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
        text = r.text[:80_000]  # first 80KB
        soup = BeautifulSoup(text, "lxml")

        # ── Email ──
        # Method 1: mailto links (most reliable)
        for a in soup.select("a[href^='mailto:']"):
            email = a.get("href", "").replace("mailto:", "").split("?")[0].strip()
            if "@" in email and "example" not in email:
                info["email"] = email
                break

        # Method 2: Regex scan
        if not info["email"]:
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            skip = ["example.com", "sentry.io", "wixpress", "googleapis",
                    "wordpress", "w3.org", "schema.org", "gravatar",
                    "jquery", "cloudflare", "google", "facebook"]
            emails = [e for e in emails if not any(s in e.lower() for s in skip)]
            if emails:
                info["email"] = emails[0]

        # ── Phone ──
        # Method 1: tel: links
        for a in soup.select("a[href^='tel:']"):
            tel = a.get("href", "").replace("tel:", "").strip()
            if len(re.sub(r'\D', '', tel)) >= 10:
                info["phone"] = tel
                break

        # Method 2: Regex
        if not info["phone"]:
            phones = re.findall(r'\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}', text)
            if phones:
                info["phone"] = phones[0]

        # ── Owner Name ──
        # Method 1: Meta tags
        for tag in soup.select("meta[name='author'], meta[property='article:author']"):
            content = tag.get("content", "").strip()
            if content and len(content) < 50 and " " in content:
                info["owner"] = content
                break

        # Method 2: Look for "owner", "founded by", "about" patterns in text
        if not info["owner"]:
            # Common patterns on small business sites
            owner_patterns = [
                r'(?:owner|proprietor|founded by|chef|operated by|managed by)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'(?:owner|proprietor|founded by|chef)[:\s]+([A-Z][a-z]+)',
                r'(?:Hi,?\s+I\'?m|Meet)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'(?:Hi,?\s+I\'?m|Meet)\s+([A-Z][a-z]+)',
            ]
            # Only search visible text (not scripts/styles)
            visible_text = soup.get_text(" ", strip=True)[:10_000]
            for pattern in owner_patterns:
                match = re.search(pattern, visible_text)
                if match:
                    candidate = match.group(1).strip()
                    # Sanity check: should look like a name
                    if 2 < len(candidate) < 40 and not any(c.isdigit() for c in candidate):
                        info["owner"] = candidate
                        break

        # Method 3: Check "About" page if we're on the homepage
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

# ── Prospect Builder ─────────────────────────────────────────────────────────

def build_prospect(raw, vertical, area):
    """Convert raw scraped data into a CRM prospect record."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": str(uuid.uuid4()),
        "name": raw["name"][:100],
        "cat": vertical,
        "status": "NOT CONTACTED",
        "address": raw.get("address", "")[:200] or area,
        "phone": raw.get("phone", ""),
        "email": raw.get("email", ""),
        "owner": raw.get("owner", ""),
        "opp": f"AI automation opportunity — {vertical.lower()} in {area.split(',')[0]}",
        "action": "Research & qualify",
        "notes": f"[Auto-sourced {datetime.now().strftime('%Y-%m-%d')}] {raw.get('snippet', '')[:150]}",
        "last_contact": None,
        "date": None,
        "activities": json.dumps([]),
        "created_at": now,
        "updated_at": now,
    }

# ── Main Agent Logic ─────────────────────────────────────────────────────────

def run_agent(verticals=None, areas=None, max_per_search=5, dry_run=False):
    """
    Main entry point. Searches for leads, deduplicates, enriches,
    writes to Supabase, and notifies Franco.
    """
    verticals = verticals or list(VERTICALS.keys())
    areas = areas or GTA_AREAS

    print("=" * 60)
    print("  🔍 Caliber Lead Sourcer Agent v1.1")
    print("=" * 60)
    print(f"  Verticals : {', '.join(verticals)}")
    print(f"  Areas     : {len(areas)} GTA locations")
    print(f"  Max/search: {max_per_search}")
    print(f"  Dry run   : {dry_run}")
    print(f"  Filter    : Chains/franchises BLOCKED")
    print(f"  Supabase  : {'✅ Connected' if SUPABASE_KEY else '❌ No key'}")
    print(f"  Twilio    : {'✅ Configured' if TWILIO_SID else '⏭ Skipped'}")
    print()

    # Step 1: Get existing prospects for deduplication
    existing = set()
    if not dry_run and SUPABASE_KEY:
        print("📋 Fetching existing prospects for dedup...")
        existing = sb_get_existing_names()
        print(f"   Found {len(existing)} existing prospects\n")

    # Step 2: Scrape for new leads
    all_leads = []
    chains_blocked = 0
    searches_done = 0

    for v_name in verticals:
        yp_terms = YP_SEARCH_TERMS.get(v_name, [v_name])
        sample_size = min(3, len(yp_terms))
        chosen_terms = random.sample(yp_terms, sample_size)

        for search_term in chosen_terms:
            chosen_areas = random.sample(areas, min(5, len(areas)))

            for area in chosen_areas:
                print(f"🔎 Searching: {search_term} in {area}")

                raw_results = scrape_yellowpages(search_term, area, max_results=max_per_search)
                print(f"   Found {len(raw_results)} raw results")

                for raw in raw_results:
                    # Dedup check
                    norm_name = raw["name"].strip().lower()
                    if norm_name in existing:
                        print(f"   ⏭ Skipping duplicate: {raw['name']}")
                        continue

                    # Enrich from website if we have one
                    if raw.get("website"):
                        print(f"   🔗 Enriching: {raw['name']}")
                        extra = enrich_from_website(raw["website"])
                        raw["email"] = raw.get("email") or extra["email"]
                        raw["phone"] = raw.get("phone") or extra["phone"]
                        raw["owner"] = raw.get("owner") or extra["owner"]

                    prospect = build_prospect(raw, v_name, area)
                    all_leads.append(prospect)
                    existing.add(norm_name)  # prevent dupes within same run

                searches_done += 1
                # Rate limit: be respectful
                delay = random.uniform(2.0, 5.0)
                print(f"   ⏳ Waiting {delay:.1f}s...\n")
                time.sleep(delay)

    # Step 3: Summary
    print("=" * 60)
    print(f"  📊 Sourcing Complete")
    print(f"     Searches run    : {searches_done}")
    print(f"     New leads       : {len(all_leads)}")
    print(f"     Chains filtered : (see 🚫 above)")
    print("=" * 60)

    if not all_leads:
        print("\n  No new leads found this run.")
        return

    # Print preview
    print("\n  Preview of new leads:")
    for i, p in enumerate(all_leads[:10], 1):
        email_flag = "📧" if p["email"] else "  "
        phone_flag = "📞" if p["phone"] else "  "
        owner_flag = "👤" if p["owner"] else "  "
        print(f"   {i:>2}. {email_flag}{phone_flag}{owner_flag} {p['name'][:35]:<35} | {p['cat']:<12} | {p['address'][:30]}")
    if len(all_leads) > 10:
        print(f"   ... and {len(all_leads) - 10} more")

    # Step 4: Write to Supabase
    if dry_run:
        print("\n  🏁 Dry run — nothing written to database.")
        with open("leads_preview.json", "w") as f:
            json.dump(all_leads, f, indent=2)
        print("  💾 Preview saved to leads_preview.json")
        return

    if not SUPABASE_KEY:
        print("\n  ⚠ No SUPABASE_KEY — saving to leads_export.json instead")
        with open("leads_export.json", "w") as f:
            json.dump(all_leads, f, indent=2)
        return

    print(f"\n  📤 Writing {len(all_leads)} prospects to Supabase...")
    inserted = 0
    for i in range(0, len(all_leads), 25):
        batch = all_leads[i:i+25]
        count = sb_insert_prospects(batch)
        inserted += count
        if count:
            print(f"     ✅ Batch {i//25 + 1}: {count} inserted")
        else:
            print(f"     ⚠ Batch {i//25 + 1}: failed")

    print(f"\n  ✅ Total inserted: {inserted}/{len(all_leads)}")

    # Step 5: Notify Franco
    if inserted > 0:
        cats = {}
        for p in all_leads:
            cats[p["cat"]] = cats.get(p["cat"], 0) + 1
        breakdown = ", ".join(f"{v} {k}" for k, v in cats.items())

        msg = (
            f"Caliber: {inserted} new leads added ({breakdown}). "
            f"Review: synapse-crm-coral.vercel.app"
        )
        print(f"\n  📱 Notifying Franco...")
        send_sms(msg)

    print("\n  🏁 Agent run complete.")

# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Caliber Lead Sourcer Agent")
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
