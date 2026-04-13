#!/usr/bin/env python3
"""
Unify Cold Email Agent v4.0
=============================
1. Searches entire CRM for leads missing owner/manager names
2. Cross-references Facebook + LinkedIn via Google to find names & titles
3. Drafts personalized cold emails using industry-specific templates
4. Saves drafts directly into Franco's Gmail for review and sending

Two modes:
  --draft  : Enrich names + create Gmail drafts (runs daily, unattended)
  --send   : Send all approved emails from agent_queue via Resend (manual)

RULE: Never send an email without Franco's explicit approval.
Drafts go to Gmail for Franco to review, personalize, and send manually.

Usage:
    python cold_email_agent.py --draft              # Enrich + draft to Gmail
    python cold_email_agent.py --send               # Send approved queue items
    python cold_email_agent.py --draft --max 20     # Limit drafts per run
    python cold_email_agent.py --dry-run --draft    # Preview without writing

Requires env vars or .env file + gmail_token.json
"""

import os, sys, re, json, time, random, argparse, base64
from datetime import datetime, timezone
from urllib.parse import quote_plus
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
from bs4 import BeautifulSoup

# -- Configuration ------------------------------------------------------------

def load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

load_env()

SUPABASE_URL   = os.getenv("SUPABASE_URL", "https://alfzjwzeccqswtytcylo.supabase.co")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY", "")
TWILIO_SID     = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN   = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM    = os.getenv("TWILIO_FROM", "")
FRANCO_PHONE   = os.getenv("FRANCO_PHONE", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
SENDER_EMAIL   = os.getenv("SENDER_EMAIL", "franco@unifyaipartners.ca")

# Gmail token can be env var (JSON string) or file
GMAIL_TOKEN_JSON = os.getenv("GMAIL_TOKEN_JSON", "")
GMAIL_TOKEN_FILE = os.getenv("GMAIL_TOKEN_FILE", "gmail_token.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

GENERIC_EMAILS = {
    "info@", "admin@", "noreply@", "no-reply@", "contact@",
    "hello@", "support@", "sales@", "office@", "help@",
    "webmaster@", "mail@", "enquiries@", "inquiries@",
}


# -- Gmail Setup --------------------------------------------------------------

def get_gmail_service():
    """Build Gmail API service from saved token."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        token_data = None

        # Try env var first (for GitHub Actions)
        if GMAIL_TOKEN_JSON:
            token_data = json.loads(GMAIL_TOKEN_JSON)
        # Fall back to file (for local runs)
        elif os.path.exists(GMAIL_TOKEN_FILE):
            with open(GMAIL_TOKEN_FILE) as f:
                token_data = json.load(f)

        if not token_data:
            print("  Warning: No Gmail token found -- cannot create drafts")
            return None

        creds = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri=token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=token_data.get("client_id"),
            client_secret=token_data.get("client_secret"),
        )

        return build("gmail", "v1", credentials=creds)

    except Exception as e:
        print(f"  Warning: Gmail setup failed: {e}")
        return None


def create_gmail_draft(service, to_email, to_name, subject, body_html, body_text):
    """Create a draft email in Franco's Gmail inbox."""
    if not service:
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["to"] = f"{to_name} <{to_email}>"
        msg["from"] = f"Franco Di Giovanni <{SENDER_EMAIL}>"
        msg["subject"] = subject

        # Plain text version
        part1 = MIMEText(body_text, "plain")
        # HTML version
        part2 = MIMEText(body_html, "html")

        msg.attach(part1)
        msg.attach(part2)

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        draft = service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw}}
        ).execute()

        print(f"    Gmail draft created: {draft['id']}")
        return True

    except Exception as e:
        print(f"    Gmail draft failed: {e}")
        return False


# -- Supabase Helpers ---------------------------------------------------------

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def get_all_prospects():
    """Fetch ALL prospects from CRM."""
    url = f"{SUPABASE_URL}/rest/v1/prospects?select=*"
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return r.json()
    print(f"  Warning: Could not fetch prospects: {r.status_code}")
    return []


def get_prospects_needing_enrichment():
    """Fetch prospects that have no owner name."""
    all_prospects = get_all_prospects()
    return [p for p in all_prospects if not (p.get("owner") or "").strip()]


def get_prospects_to_email():
    """
    Fetch prospects ready for email drafting:
    - Has owner name
    - Has email (not generic)
    - Status = NOT CONTACTED
    """
    all_prospects = get_all_prospects()
    ready = []
    for p in all_prospects:
        owner = (p.get("owner") or "").strip()
        email = (p.get("email") or "").strip().lower()
        status = (p.get("status") or "").strip().upper()

        # Owner name is the ONLY hard filter — no name = can't personalize
        if not owner:
            continue
        # Must have some email (generic like info@ is fine if we have a name)
        if not email or "@" not in email:
            continue
        if status != "NOT CONTACTED":
            continue

        ready.append(p)
    return ready


def get_existing_queue_ids():
    """Fetch prospect IDs that already have a cold_email in agent_queue."""
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?select=prospect_id"
        f"&action_type=eq.cold_email"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return {row["prospect_id"] for row in r.json() if row.get("prospect_id")}
    return set()


def update_prospect_owner(prospect_id, owner_name):
    """Write enriched owner name back to the prospect record."""
    url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{prospect_id}"
    r = requests.patch(url, headers=sb_headers(), json={"owner": owner_name}, timeout=15)
    return r.status_code in (200, 204)


def insert_draft_to_queue(prospect_id, payload):
    """Insert a draft email into agent_queue for tracking."""
    url = f"{SUPABASE_URL}/rest/v1/agent_queue"
    row = {
        "prospect_id": prospect_id,
        "action_type": "cold_email",
        "payload": payload,
        "status": "pending",
    }
    headers = sb_headers()
    headers["Prefer"] = "return=representation"
    r = requests.post(url, headers=headers, json=[row], timeout=15)
    return r.status_code in (200, 201)


def update_queue_status(queue_id, status):
    url = f"{SUPABASE_URL}/rest/v1/agent_queue?id=eq.{queue_id}"
    r = requests.patch(url, headers=sb_headers(), json={"status": status}, timeout=15)
    return r.status_code in (200, 204)


def get_approved_emails():
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?select=*"
        f"&action_type=eq.cold_email"
        f"&status=eq.approved"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return r.json()
    return []


def update_prospect_after_send(prospect_id):
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{prospect_id}"
    r = requests.patch(
        url, headers=sb_headers(),
        json={"last_contact": today, "action": "Follow up in 5 days"},
        timeout=15,
    )
    return r.status_code in (200, 204)


# -- Twilio SMS ---------------------------------------------------------------

def send_sms(body):
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, FRANCO_PHONE]):
        print("  Warning: Twilio not configured -- skipping SMS")
        print(f"  Message would be:\n     {body}")
        return False
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    r = requests.post(
        url,
        auth=(TWILIO_SID, TWILIO_TOKEN),
        data={"From": TWILIO_FROM, "To": FRANCO_PHONE, "Body": body[:1600]},
        timeout=15,
    )
    if r.status_code == 201:
        print(f"  SMS sent to {FRANCO_PHONE}")
        return True
    print(f"  Warning: SMS failed ({r.status_code}): {r.text[:200]}")
    return False


# -- Resend Email Sending (for --send mode) -----------------------------------

def send_email_via_resend(to_email, to_name, subject, body_html, body_text):
    if not RESEND_API_KEY:
        print("  Warning: RESEND_API_KEY not set")
        return False
    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": f"Franco Di Giovanni <{SENDER_EMAIL}>",
            "to": [to_email],
            "subject": subject,
            "html": body_html,
            "text": body_text,
        },
        timeout=15,
    )
    if r.status_code in (200, 201):
        print(f"  Email sent to {to_email}")
        return True
    print(f"  Warning: Resend failed ({r.status_code}): {r.text[:200]}")
    return False


# ==============================================================================
# ENRICHMENT v4 — Datacenter-safe sources (no Google dependency)
# ==============================================================================

# Flexible name patterns — handles accented, hyphenated, ALL-CAPS names
_NAME_WORD = r"[A-Za-z\u00C0-\u024F][A-Za-z\u00C0-\u024F'\-]*"
_FULL_NAME = _NAME_WORD + r"(?:\s+" + _NAME_WORD + r")+"
_TITLE_KEYWORDS = (
    r"Owner|Founder|Proprietor|President|CEO|Principal|Director|"
    r"Manager|Partner|Operator"
)

# Google cooldown (recoverable, not permanent)
_google_cooldown_until = 0


def _is_google_available():
    return time.time() >= _google_cooldown_until


def _set_google_cooldown(seconds=300):
    global _google_cooldown_until
    _google_cooldown_until = time.time() + seconds
    print(f"     [Google] Cooldown set for {seconds}s")


def enrich_prospect(business_name, city, website_url="", existing_email=""):
    """
    Full enrichment pipeline — datacenter-safe sources FIRST:
      1. YellowPages.ca (always works from datacenter — finds website URL + owner)
      2. Business website About/Team/Contact pages (uses URL from YP or CRM)
      3. Google panel (fallback — reviews + owner, cooldown on 429)
      4. LinkedIn via Google (fallback)
      5. Facebook via Google (fallback)
    Returns dict: {owner, source, stars, review_count, personal_email, found_email}
    """
    result = {
        "owner": "", "source": "", "stars": 0,
        "review_count": 0, "personal_email": "", "found_email": "",
    }

    # =========================================================================
    # STEP 1: YellowPages (ALWAYS runs first — proven on GitHub Actions)
    # This is our workhorse: finds website URLs, emails, sometimes owners.
    # =========================================================================
    print(f"     [YP] Searching for {business_name} in {city}...")
    yp_result = _scrape_yellowpages_owner(business_name, city)

    if yp_result.get("owner"):
        result["owner"] = yp_result["owner"]
        result["source"] = "YellowPages"
        if yp_result.get("email"):
            result["found_email"] = yp_result["email"]
        if yp_result.get("website") and not website_url:
            website_url = yp_result["website"]
        result["personal_email"] = _guess_owner_email(
            yp_result["owner"], existing_email, website_url
        )
        return result

    # Even without owner, grab email + website from YP for later steps
    if yp_result.get("email") and not result["found_email"]:
        result["found_email"] = yp_result["email"]
    if yp_result.get("website") and not website_url:
        website_url = yp_result["website"]
        print(f"     [YP] Got website URL: {website_url}")

    # =========================================================================
    # STEP 2: Website scraping (uses URL from CRM or discovered via YP)
    # =========================================================================
    if website_url:
        print(f"     [Website] Scraping {website_url}...")
        name, emails = _scrape_website_for_owner_and_email(website_url, business_name)
        if emails:
            result["found_email"] = emails[0]
        if name:
            result["owner"] = name
            result["source"] = "Website"
            result["personal_email"] = _guess_owner_email(name, existing_email, website_url)
            return result
    else:
        print(f"     [Website] No URL available — skipping")

    # =========================================================================
    # STEP 3: Google panel (reviews + owner — fallback, may be rate limited)
    # =========================================================================
    if _is_google_available():
        stars, count, panel_owner = _scrape_google_panel(business_name, city)
        result["stars"] = stars
        result["review_count"] = count
        if panel_owner:
            result["owner"] = panel_owner
            result["source"] = "Google"
            result["personal_email"] = _guess_owner_email(panel_owner, existing_email, website_url)
            return result
    else:
        print(f"     [Google] Skipped — cooldown active")

    # =========================================================================
    # STEP 4: LinkedIn + Facebook via Google search (last resort)
    # =========================================================================
    if _is_google_available():
        for source_name, queries in [
            ("LinkedIn", [
                f'site:linkedin.com/in "{business_name}" "{city}" owner OR founder OR manager',
                f'site:linkedin.com/in "{business_name}" "{city}"',
            ]),
            ("Facebook", [
                f'site:facebook.com "{business_name}" "{city}" owner OR founder',
                f'site:facebook.com "{business_name}" "{city}"',
            ]),
        ]:
            if not _is_google_available():
                print(f"     [{source_name}] Skipped — Google cooldown active")
                break
            name = _google_search_for_name(queries, source_name)
            if name:
                result["owner"] = name
                result["source"] = source_name
                result["personal_email"] = _guess_owner_email(name, existing_email, website_url)
                return result

    return result


# -- Source 1: Google Panel (reviews + owner, fallback only) -------------------

def _scrape_google_panel(business_name, city):
    """
    Google search for reviews + knowledge panel owner.
    Returns (stars, count, owner_name_or_empty).
    Gracefully fails — sets cooldown on 429 instead of killing everything.
    """
    try:
        query = f"{business_name} {city}"
        url = f"https://www.google.com/search?q={quote_plus(query)}&gl=ca&hl=en"
        r = requests.get(url, headers=HEADERS, timeout=10)

        if r.status_code == 429:
            _set_google_cooldown(300)
            return 0, 0, ""
        if r.status_code != 200:
            print(f"     [Google] HTTP {r.status_code}")
            return 0, 0, ""

        text = r.text
        soup = BeautifulSoup(r.text, "lxml")

        # --- Reviews ---
        star_match = re.search(r'(\d+\.?\d*)\s*(?:stars?|/\s*5)', text, re.I)
        count_match = re.search(r'(\d[\d,]*)\s*(?:reviews?|ratings?|Google reviews?)', text, re.I)
        stars = float(star_match.group(1)) if star_match else 0
        count = int(count_match.group(1).replace(",", "")) if count_match else 0
        if stars > 5:
            stars = 0
        if stars > 0:
            print(f"     [Reviews] {stars} stars, {count} reviews")

        # --- Owner from panel ---
        owner = ""
        for pattern in [
            rf'(?:{_TITLE_KEYWORDS})\s*[:]\s*({_FULL_NAME})',
            rf'(?:Owned by|Founded by)\s+({_FULL_NAME})',
        ]:
            match = re.search(pattern, text, re.I)
            if match:
                name = _normalize_name(match.group(1).strip())
                if _is_valid_person_name(name):
                    owner = name
                    print(f"     [Google] Found in panel: {owner}")
                    break

        if not owner:
            for el in soup.select("[data-attrid*='owner'], [data-attrid*='founder']"):
                txt = el.get_text(strip=True)
                name_match = re.search(rf'({_FULL_NAME})', txt)
                if name_match:
                    name = _normalize_name(name_match.group(1).strip())
                    if _is_valid_person_name(name):
                        owner = name
                        print(f"     [Google] Found in attrid: {owner}")
                        break

        time.sleep(random.uniform(1.5, 3))
        return stars, count, owner

    except Exception as e:
        print(f"     [Google] Error: {e}")
        return 0, 0, ""


# -- Source 2: Business Website (owner name + email scraping) ------------------

def _scrape_website_for_owner_and_email(website_url, business_name):
    """
    Hit the business website's About, Team, Contact pages.
    Returns (owner_name, [list_of_emails_found]).
    """
    if not website_url:
        return "", []

    base = website_url.rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base

    pages = [
        base + "/about",
        base + "/about-us",
        base + "/team",
        base + "/our-team",
        base + "/contact",
        base + "/contact-us",
        base,  # homepage fallback
    ]

    found_owner = ""
    found_emails = []

    for page_url in pages:
        try:
            r = requests.get(page_url, headers=HEADERS, timeout=8, allow_redirects=True)
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "lxml")
            text = soup.get_text(" ", strip=True)

            # --- Scrape emails from this page ---
            for mailto in soup.select("a[href^='mailto:']"):
                email = mailto.get("href", "").replace("mailto:", "").split("?")[0].strip().lower()
                if email and "@" in email and email not in found_emails:
                    found_emails.append(email)

            # Regex email extraction from page text
            for match in re.finditer(r'[\w.+-]+@[\w-]+\.[\w.-]+', text):
                email = match.group(0).lower().rstrip(".")
                if email not in found_emails and not email.endswith((".png", ".jpg", ".gif")):
                    found_emails.append(email)

            # --- Scrape owner name ---
            if not found_owner:
                for pattern in [
                    rf'(?:{_TITLE_KEYWORDS})[:\s]+({_FULL_NAME})',
                    rf'({_FULL_NAME})\s*[,\-]\s*(?:{_TITLE_KEYWORDS})',
                    rf'(?:owned and operated by|founded by|run by|managed by)\s+({_FULL_NAME})',
                    rf'(?:Meet|About)\s+({_FULL_NAME})\s*[,\-]\s*(?:the\s+)?(?:owner|founder)',
                ]:
                    match = re.search(pattern, text, re.I)
                    if match:
                        name = _normalize_name(match.group(1).strip())
                        if _is_valid_person_name(name):
                            found_owner = name
                            print(f"     [Website] Found on /{page_url.split('/')[-1]}: {name}")
                            break

                # JSON-LD structured data
                if not found_owner:
                    for script in soup.select('script[type="application/ld+json"]'):
                        try:
                            ld = json.loads(script.string or "")
                            items = ld if isinstance(ld, list) else [ld]
                            for item in items:
                                for key in ("founder", "employee", "author", "member"):
                                    person = item.get(key)
                                    if isinstance(person, dict):
                                        name = person.get("name", "")
                                        if _is_valid_person_name(name):
                                            found_owner = _normalize_name(name)
                                            print(f"     [Website] JSON-LD: {found_owner}")
                                    elif isinstance(person, list):
                                        for p in person:
                                            if isinstance(p, dict):
                                                name = p.get("name", "")
                                                if _is_valid_person_name(name):
                                                    found_owner = _normalize_name(name)
                                                    print(f"     [Website] JSON-LD: {found_owner}")
                                                    break
                                    if found_owner:
                                        break
                                if found_owner:
                                    break
                        except (json.JSONDecodeError, TypeError):
                            pass

                # Meta author
                if not found_owner:
                    for meta in soup.select('meta[name="author"], meta[property="article:author"]'):
                        name = (meta.get("content") or "").strip()
                        if _is_valid_person_name(name):
                            found_owner = _normalize_name(name)
                            print(f"     [Website] Meta author: {found_owner}")
                            break

            # If we found both owner and emails, stop early
            if found_owner and found_emails:
                break

        except Exception as e:
            print(f"     [Website] Error on {page_url.split('/')[-1]}: {e}")
            continue

        time.sleep(random.uniform(0.5, 1))

    if found_emails:
        print(f"     [Website] Emails found: {', '.join(found_emails[:3])}")

    return found_owner, found_emails


# -- Source 3: YellowPages.ca Direct Scrape ------------------------------------

def _scrape_yellowpages_owner(business_name, city):
    """
    Search YellowPages.ca directly (no Google needed — proven on GitHub Actions).
    Returns dict: {owner, email, website} or empty values.
    """
    result = {"owner": "", "email": "", "website": ""}
    try:
        location = f"{city}+ON".replace(" ", "+")
        search_url = (
            f"https://www.yellowpages.ca/search/si/1/"
            f"{quote_plus(business_name)}/{location}"
        )
        r = requests.get(search_url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            print(f"     [YP] Search HTTP {r.status_code}")
            return result

        soup = BeautifulSoup(r.text, "lxml")

        # Find the best matching listing
        listings = soup.select("div.listing, div.listing__content, div[class*='listing']")
        if not listings:
            listings = soup.select("div.resultList div, div.result")

        detail_url = ""
        best_listing = None
        for listing in listings[:5]:
            name_el = listing.select_one(
                "a.listing__name--link, h3.listing__name, "
                "a[class*='listing__name'], span.listing__name, h2 a, h3 a"
            )
            if not name_el:
                continue
            listed_name = name_el.get_text(strip=True).lower()
            biz_lower = business_name.lower()
            # Flexible matching: first significant word, first 8 chars, or mutual containment
            first_word = biz_lower.split()[0] if biz_lower.split() else ""
            match = (
                biz_lower[:8] in listed_name
                or listed_name[:8] in biz_lower
                or (first_word and len(first_word) > 3 and first_word in listed_name)
                or listed_name in biz_lower
            )
            if match:
                href = name_el.get("href", "")
                if href and not href.startswith("http"):
                    href = "https://www.yellowpages.ca" + href
                detail_url = href

                # Grab website from listing
                web_el = listing.select_one("a[class*='website'], a[data-analytics='website']")
                if web_el:
                    result["website"] = web_el.get("href", "")

                # Grab email from listing
                email_el = listing.select_one("a[href^='mailto:']")
                if email_el:
                    result["email"] = email_el.get("href", "").replace("mailto:", "").strip()
                break

        if not detail_url:
            print(f"     [YP] No matching listing found")
            return result

        # Visit detail page for owner name + more contact info
        time.sleep(random.uniform(1, 2))
        r2 = requests.get(detail_url, headers=HEADERS, timeout=12)
        if r2.status_code == 200:
            detail_soup = BeautifulSoup(r2.text, "lxml")
            detail_text = detail_soup.get_text(" ", strip=True)

            # Look for owner name in detail page
            for pattern in [
                rf'(?:{_TITLE_KEYWORDS})[:\s]+({_FULL_NAME})',
                rf'({_FULL_NAME})\s*[,\-]\s*(?:{_TITLE_KEYWORDS})',
                rf'(?:owned by|founded by|operated by|managed by)\s+({_FULL_NAME})',
            ]:
                match = re.search(pattern, detail_text, re.I)
                if match:
                    name = _normalize_name(match.group(1).strip())
                    if _is_valid_person_name(name):
                        result["owner"] = name
                        print(f"     [YP] Found owner: {name}")
                        break

            # Grab email from detail page if not found yet
            if not result["email"]:
                for mailto in detail_soup.select("a[href^='mailto:']"):
                    email = mailto.get("href", "").replace("mailto:", "").split("?")[0].strip()
                    if email and "@" in email:
                        result["email"] = email.lower()
                        break
                if not result["email"]:
                    email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', detail_text)
                    if email_match:
                        result["email"] = email_match.group(0).lower().rstrip(".")

            # Grab website if not found yet
            if not result["website"]:
                for a in detail_soup.select("a[href^='http']"):
                    href = a.get("href", "")
                    if "yellowpages.ca" not in href and "ypcdn" not in href:
                        result["website"] = href
                        break

        if result["email"]:
            print(f"     [YP] Email found: {result['email']}")
        if result["website"]:
            print(f"     [YP] Website found: {result['website']}")

        time.sleep(random.uniform(1, 2))

    except Exception as e:
        print(f"     [YP] Error: {e}")

    return result


# -- Source 4: Canada411 Direct Scrape -----------------------------------------

def _scrape_canada411_owner(business_name, city):
    """
    Search 411.ca directly. Returns owner name or "".
    NOTE: 411.ca frequently returns 403 from datacenter IPs (GitHub Actions).
    Kept as a best-effort attempt — not relied upon.
    """
    try:
        url = (
            f"https://411.ca/search/"
            f"?q={quote_plus(business_name)}"
            f"&l={quote_plus(city + ' ON')}"
            f"&t=business"
        )
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            print(f"     [411] HTTP {r.status_code}")
            return ""

        soup = BeautifulSoup(r.text, "lxml")
        listings = soup.select(
            "div.listing, div.vcard, div.result-card, "
            "div[class*='listing'], article"
        )

        for listing in listings[:5]:
            name_el = listing.select_one("h2 a, h3 a, a.listing-name, span.fn")
            if not name_el:
                continue
            listed_name = name_el.get_text(strip=True).lower()
            if business_name.lower()[:10] not in listed_name:
                continue

            # Found matching listing — look for contact person
            all_text = listing.get_text(" ", strip=True)
            for pattern in [
                rf'(?:{_TITLE_KEYWORDS})[:\s]+({_FULL_NAME})',
                rf'({_FULL_NAME})\s*[,\-]\s*(?:{_TITLE_KEYWORDS})',
                rf'(?:Contact|Representative)[:\s]+({_FULL_NAME})',
            ]:
                match = re.search(pattern, all_text, re.I)
                if match:
                    name = _normalize_name(match.group(1).strip())
                    if _is_valid_person_name(name):
                        print(f"     [411] Found: {name}")
                        return name

        time.sleep(random.uniform(1, 2))

    except Exception as e:
        print(f"     [411] Error: {e}")

    return ""


# -- Source 5-6: Google-dependent (LinkedIn, Facebook) -------------------------

def _google_search_for_name(queries, source_name):
    """
    Execute Google searches and extract person names from results.
    Uses cooldown instead of permanent block on 429.
    """
    if not _is_google_available():
        return ""

    for query in queries:
        try:
            url = f"https://www.google.com/search?q={quote_plus(query)}&num=5&gl=ca&hl=en"
            r = requests.get(url, headers=HEADERS, timeout=10)

            if r.status_code == 429:
                _set_google_cooldown(300)
                return ""
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "lxml")

            # Parse search result titles
            for div in soup.select("div.g, div[data-sokoban-container]"):
                title_el = div.select_one("h3")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                name_match = re.match(
                    rf'^({_FULL_NAME})\s*[-|~(]', title
                )
                if name_match:
                    name = _normalize_name(name_match.group(1).strip())
                    if _is_valid_person_name(name):
                        print(f"     [{source_name}] Found in title: {name}")
                        return name

            # Parse snippets
            for div in soup.select("div.g"):
                snippet_el = div.select_one("div.VwiC3b, span.st")
                if not snippet_el:
                    continue
                snippet = snippet_el.get_text(strip=True)

                for pattern in [
                    rf'(?:{_TITLE_KEYWORDS})[:\s]+({_FULL_NAME})',
                    rf'({_FULL_NAME})\s*[-,]\s*(?:{_TITLE_KEYWORDS})',
                    rf'(?:owned by|founded by|managed by|operated by|run by)\s+({_FULL_NAME})',
                ]:
                    match = re.search(pattern, snippet, re.I)
                    if match:
                        name = _normalize_name(match.group(1).strip())
                        if _is_valid_person_name(name):
                            print(f"     [{source_name}] Found in snippet: {name}")
                            return name

            time.sleep(random.uniform(3, 5))

        except Exception as e:
            print(f"     [{source_name}] Error: {e}")

    return ""


# -- Owner Email Construction --------------------------------------------------

def _guess_owner_email(owner_name, existing_email, website_url):
    """
    Given an owner name and business domain, construct likely personal email.
    Returns best guess or "" if no domain.
    """
    domain = ""
    if existing_email and "@" in existing_email:
        domain = existing_email.split("@")[1].strip().lower()
    elif website_url:
        cleaned = website_url.lower().replace("https://", "").replace("http://", "").replace("www.", "")
        domain = cleaned.split("/")[0].strip()

    if not domain:
        return ""

    parts = owner_name.lower().split()
    if len(parts) < 2:
        return ""

    first = re.sub(r"[^a-z]", "", parts[0])
    last = re.sub(r"[^a-z]", "", parts[-1])
    if not first or not last:
        return ""

    # If existing email already looks personal, don't replace it
    if existing_email and "@" in existing_email:
        local = existing_email.split("@")[0].lower()
        generic = {"info", "admin", "contact", "hello", "support",
                    "sales", "office", "help", "mail", "enquiries",
                    "inquiries", "noreply", "no-reply", "webmaster"}
        if local not in generic:
            return ""  # already has a personal email

    best = f"{first}@{domain}"
    print(f"     [Email] Guessed: {best}")
    return best


# -- Name Helpers --------------------------------------------------------------

def _normalize_name(name):
    """Normalize ALL-CAPS or weird casing to Title Case."""
    if not name:
        return name
    if name.isupper() or name.islower():
        # Handle hyphenated names: JEAN-PIERRE → Jean-Pierre
        return "-".join(part.capitalize() for part in name.split("-"))  \
            if "-" in name else name.title()
    return name


def _is_valid_person_name(name):
    """Validate that a string looks like a real person's name."""
    if not name or len(name) < 3 or len(name) > 40:
        return False
    normalized = _normalize_name(name.strip())
    if any(c.isdigit() for c in normalized):
        return False
    if len(normalized.split()) < 2:
        return False
    # False positives (case-insensitive check)
    false_positives_lower = {
        "about us", "contact us", "our team", "home page",
        "read more", "learn more", "sign up", "log in",
        "privacy policy", "terms service", "google maps",
        "google reviews", "view more", "see more", "show more",
        "north york", "east york", "west toronto", "south etobicoke",
        "better business", "yellow pages", "white pages",
        "all rights", "click here", "find out", "get started",
    }
    if normalized.lower() in false_positives_lower:
        return False
    # Business words
    biz_words = ["inc", "ltd", "llc", "corp", "restaurant", "pizza",
                 "salon", "dental", "auto", "clinic", "services",
                 "cleaning", "plumbing", "hvac", "roofing"]
    if any(w in normalized.lower() for w in biz_words):
        return False
    return True


# ==============================================================================
# EMAIL TEMPLATES — Industry-specific
# ==============================================================================

# Industry-specific body paragraphs — same consultative tone, different pain points
# Each has a "pitch" (what we do for their industry) and "consult" (free offer framing)
VERTICAL_COPY = {
    "Restaurants": {
        "pitch": (
            "We work with local restaurants across the GTA. We take the time "
            "to really understand how your operation runs — not just the "
            "surface level stuff, but the day-to-day things that quietly eat "
            "into your time and your margins. Most restaurant owners we sit "
            "down with are surprised by what we find."
        ),
        "consult": (
            "Our consultations are completely free — we just sit down with "
            "you, learn how things work from the inside, and show you where "
            "the opportunities are. From there, if it makes sense for both "
            "of us, we'll put something together for you."
        ),
    },
    "Retail": {
        "pitch": (
            "We work with local retail businesses across the GTA. We spend "
            "time understanding how your store actually operates — not just "
            "what's visible, but the behind-the-scenes work that's costing "
            "you time and money without you even realizing it. Most shop "
            "owners we meet are surprised by how much room there is to "
            "tighten things up."
        ),
        "consult": (
            "Our consultations are completely free — we sit down, learn how "
            "things run, and show you exactly where the gaps are. If it makes "
            "sense for both of us, we'll build something around it."
        ),
    },
    "Trades": {
        "pitch": (
            "We work with local trades businesses across the GTA. We take "
            "the time to understand how your operation actually moves — from "
            "the first call to the finished job — and find where hours and "
            "dollars are quietly getting lost. Most trades owners we sit "
            "down with don't realize how much is slipping through the cracks."
        ),
        "consult": (
            "Our consultations are completely free — we walk through your "
            "process, show you where things can be tightened up, and if it "
            "makes sense, we'll put together a plan."
        ),
    },
    "Dental & Medical": {
        "pitch": (
            "We work with local dental and medical practices across the GTA. "
            "We take the time to understand how your practice actually runs "
            "day to day — the stuff that takes up your team's time and holds "
            "things back from running as smoothly as they could. Most practice "
            "owners we meet are surprised by what comes up."
        ),
        "consult": (
            "Our consultations are completely free — we sit down with you, "
            "look at how things work on the ground, and show you where the "
            "real bottlenecks are. From there, if it makes sense, we'll put "
            "something together."
        ),
    },
    "Salons & Spas": {
        "pitch": (
            "We work with local salons and spas across the GTA. We take the "
            "time to understand how your business actually operates — the "
            "things that eat into your day and cost you clients without you "
            "always seeing it. Most salon owners we meet are surprised by "
            "how much they're leaving on the table."
        ),
        "consult": (
            "Our consultations are completely free — we learn how you "
            "operate, show you where the opportunities are, and if it makes "
            "sense for both of us, we'll build something around it."
        ),
    },
    "Professional Services": {
        "pitch": (
            "We work with local professional service firms across the GTA. "
            "We take the time to understand how your firm actually operates "
            "— the processes that quietly eat into your team's time and keep "
            "things from running as efficiently as they should. Most firms we "
            "sit down with don't realize how much they're losing to it."
        ),
        "consult": (
            "Our consultations are completely free — we walk through how "
            "things work, show you where the inefficiencies are, and if it "
            "makes sense, we'll build a plan around it."
        ),
    },
    "Fitness & Wellness": {
        "pitch": (
            "We work with local fitness and wellness businesses across the "
            "GTA. We take the time to understand how your business actually "
            "runs — the things that take up your time, cost you members, and "
            "keep things from operating as smoothly as they could. Most gym "
            "and studio owners we meet are surprised by what we uncover."
        ),
        "consult": (
            "Our consultations are completely free — we sit down, learn how "
            "things operate, and show you exactly where the gaps are. If it "
            "makes sense, we'll put something together."
        ),
    },
    "Auto Services": {
        "pitch": (
            "We work with local auto service businesses across the GTA. We "
            "take the time to understand how your shop actually runs — the "
            "things that slow jobs down, cost you customers, and eat into "
            "your bottom line without always being obvious. Most shop owners "
            "we meet don't realize how much smoother things can get."
        ),
        "consult": (
            "Our consultations are completely free — we walk through your "
            "operation, show you where time and money are being lost, and if "
            "it makes sense, we'll build a plan."
        ),
    },
    "Cleaning & Property": {
        "pitch": (
            "We work with local cleaning and property service businesses "
            "across the GTA. We take the time to understand how your "
            "operation actually runs — the things that quietly slow you down, "
            "cost you jobs, and eat into your profits. Most owners we sit "
            "down with are surprised by how much tighter things can get."
        ),
        "consult": (
            "Our consultations are completely free — we learn how things "
            "run, show you where the opportunities are, and if it makes "
            "sense for both of us, we'll put something together."
        ),
    },
}

# Fallback for any vertical not in the dict
_DEFAULT_COPY = {
    "pitch": (
        "We work with local businesses across the GTA. We take the time to "
        "understand how you operate, find where time and money are being "
        "lost, and put the right systems in place to help your business run "
        "more efficiently — so you can focus on the work that actually matters."
    ),
    "consult": (
        "Our consultations are completely free — we just sit down with you, "
        "learn how things run, and show you where the opportunities are. "
        "From there, if it makes sense for both of us, we'll put something "
        "together for you."
    ),
}


def _generate_compliment(business_name, city, notes):
    """Generate a personalized compliment based on Google review data."""
    stars = 0
    count = 0

    # Parse review data from notes field
    if notes:
        star_match = re.search(r'(\d+\.?\d*)\s*stars?,\s*(\d+)\s*reviews?', notes)
        if star_match:
            stars = float(star_match.group(1))
            count = int(star_match.group(2))

    if stars >= 4.5 and count >= 50:
        return f"I came across {business_name} in {city} — {stars} stars across {count} reviews is no joke."
    elif stars >= 4.5 and count > 0:
        return f"I came across {business_name} in {city} — {stars} stars says a lot about how you run things."
    elif stars >= 4.0 and count > 0:
        return f"I came across {business_name} in {city} — your reviews speak for themselves."
    elif stars > 0 and count > 0:
        return f"I came across {business_name} in {city} — it's clear you've built something real."
    else:
        return f"I came across {business_name} in {city} — really like what you've built."


def generate_email(prospect):
    """
    Generate a personalized cold email for a prospect.
    Returns dict with subject, body_html, body_text, or None if can't personalize.
    """
    owner = (prospect.get("owner") or "").strip()
    if not owner:
        return None

    first_name = owner.split()[0]
    business_name = prospect.get("name", "your business")
    vertical = prospect.get("cat", "")
    ai_gap = prospect.get("opp", "")
    address = prospect.get("address", "")
    notes = prospect.get("notes", "")

    # Extract city
    city = "the GTA"
    if address:
        parts = address.split(",")
        if len(parts) >= 2:
            city = parts[-2].strip()
        elif parts:
            city = parts[0].strip()

    # Rotating subject lines (randomized per email)
    subject_templates = [
        f"{first_name}",
        f"idea for {business_name}",
        f"{first_name}, quick thought",
    ]
    subject = random.choice(subject_templates)

    # Personalized compliment based on Google reviews
    compliment = _generate_compliment(business_name, city, notes)

    # Industry-specific body copy
    copy = VERTICAL_COPY.get(vertical, _DEFAULT_COPY)
    pitch = copy["pitch"]
    consult = copy["consult"]

    body_text = (
        f"Hi {first_name},\n\n"
        f"{compliment}\n\n"
        f"{pitch}\n\n"
        f"{consult}\n\n"
        f"Open to a quick chat? Reply here or text me at (647) 210-3737.\n\n"
        f"Franco Di Giovanni\n"
        f"Unify AI Partners\n"
        f"franco@unifyaipartners.ca\n\n"
        f"---\n"
        f"If you'd prefer not to hear from us, just reply with 'unsubscribe'."
    )

    body_html = (
        f"<p>Hi {first_name},</p>"
        f"<p>{compliment}</p>"
        f"<p>{pitch}</p>"
        f"<p>{consult}</p>"
        f"<p>Open to a quick chat? Reply here or text me at (647) 210-3737.</p>"
        f"<p>Franco Di Giovanni<br>"
        f"<strong>Unify AI Partners</strong><br>"
        f"<a href='mailto:franco@unifyaipartners.ca'>franco@unifyaipartners.ca</a></p>"
        f"<hr style='border:none;border-top:1px solid #ddd;margin-top:20px'>"
        f"<p style='font-size:11px;color:#999'>If you'd prefer not to hear from us, "
        f"just reply with 'unsubscribe'.</p>"
    )

    return {
        "to_email": prospect.get("email", ""),
        "to_name": first_name,
        "subject": subject,
        "body_html": body_html,
        "body_text": body_text,
        "vertical": vertical,
        "ai_gap": ai_gap,
    }


# ==============================================================================
# DRAFT MODE — Main workflow
# ==============================================================================

def run_draft(max_drafts=20, dry_run=False):
    """
    1. Search entire CRM for leads without owner names
    2. Enrich via LinkedIn + Facebook Google search
    3. Draft personalized emails per industry
    4. Save drafts to Franco's Gmail inbox
    """
    print("=" * 60)
    print("  Unify Cold Email Agent v4.0 — DRAFT MODE")
    print("=" * 60)
    print(f"  Max drafts  : {max_drafts}")
    print(f"  Dry run     : {dry_run}")
    print(f"  Supabase    : {'Connected' if SUPABASE_KEY else 'No key'}")
    print(f"  Gmail       : Checking...")
    print()

    # Set up Gmail
    gmail = None
    if not dry_run:
        gmail = get_gmail_service()
        if gmail:
            print("  Gmail       : Connected")
        else:
            print("  Gmail       : NOT connected -- drafts will only go to agent_queue")

    # =========================================================================
    # PHASE 1: Enrich all leads missing owner names
    # =========================================================================
    print("\n" + "-" * 60)
    print("  PHASE 1: Enriching leads without owner names")
    print("-" * 60)

    needs_enrichment = get_prospects_needing_enrichment()
    print(f"  Found {len(needs_enrichment)} prospects without owner names")

    enriched_count = 0
    source_counts = {}  # Track which sources find names

    # Cap enrichment to max_drafts * 2 to keep runs fast
    enrich_limit = max_drafts * 2
    print(f"  Enriching up to {enrich_limit} prospects this run")

    for prospect in needs_enrichment[:enrich_limit]:
        pid = prospect.get("id", "unknown")
        name = prospect.get("name", "Unknown")
        address = prospect.get("address", "")
        existing_email = prospect.get("email", "")
        website = prospect.get("website", "") or ""  # website URL from lead sourcer

        # Fallback: try to extract URL from notes field
        notes = prospect.get("notes", "") or ""
        if not website:
            url_match = re.search(r'https?://[^\s]+', notes)
            if url_match:
                website = url_match.group(0)

        # Extract city for search
        city = "Toronto"
        if address:
            parts = address.split(",")
            if len(parts) >= 2:
                city = parts[-2].strip()
            elif parts:
                city = parts[0].strip()

        print(f"\n  [{needs_enrichment.index(prospect)+1}/{min(len(needs_enrichment), enrich_limit)}] "
              f"Enriching: {name} ({city})")
        print(f"    Website URL: {website or 'NONE'}")
        print(f"    Existing email: {existing_email or 'NONE'}")
        enrichment = enrich_prospect(name, city, website_url=website, existing_email=existing_email)

        # Store review data regardless of owner name result
        if enrichment["stars"] > 0 and not dry_run:
            review_note = f" | {enrichment['stars']} stars, {enrichment['review_count']} reviews"
            if review_note not in notes:
                patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
                requests.patch(patch_url, headers=sb_headers(),
                    json={"notes": notes + review_note}, timeout=15)

        if enrichment["owner"]:
            enriched_count += 1
            src = enrichment["source"]
            source_counts[src] = source_counts.get(src, 0) + 1

            print(f"    Found: {enrichment['owner']} via {src}")
            if not dry_run:
                update_data = {"owner": enrichment["owner"], "action": "Ready for cold email"}
                # If we found a personal email, upgrade their email field
                if enrichment["personal_email"]:
                    update_data["email"] = enrichment["personal_email"]
                    print(f"    Personal email: {enrichment['personal_email']}")
                # If we found an email on the website but no personal email, still use it
                elif enrichment.get("found_email") and not existing_email:
                    update_data["email"] = enrichment["found_email"]
                    print(f"    Website email: {enrichment['found_email']}")
                patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
                requests.patch(patch_url, headers=sb_headers(),
                    json=update_data, timeout=15)
        else:
            # Even without owner, store any email we found
            if enrichment.get("found_email") and not existing_email and not dry_run:
                patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
                requests.patch(patch_url, headers=sb_headers(),
                    json={"email": enrichment["found_email"]}, timeout=15)
                print(f"    No owner, but found email: {enrichment['found_email']}")
            else:
                print(f"    No owner found")

        # Rate limit between searches
        time.sleep(random.uniform(2, 4))

    source_summary = ", ".join(f"{v} {k}" for k, v in sorted(source_counts.items(), key=lambda x: -x[1]))
    attempted = min(len(needs_enrichment), enrich_limit)
    print(f"\n  {'='*56}")
    print(f"  ENRICHMENT SUMMARY")
    print(f"  {'='*56}")
    print(f"    Total needing enrichment : {len(needs_enrichment)}")
    print(f"    Attempted this run       : {attempted}")
    print(f"    Owner names found        : {enriched_count}")
    print(f"    Success rate             : {enriched_count}/{attempted} ({(enriched_count*100//attempted) if attempted else 0}%)")
    print(f"    Sources breakdown        : {source_summary or 'none'}")
    print(f"    Google available         : {'Yes' if _is_google_available() else 'No (cooldown)'}")

    # =========================================================================
    # PHASE 2: Draft emails for prospects with owner names
    # =========================================================================
    print("\n" + "-" * 60)
    print("  PHASE 2: Drafting personalized cold emails")
    print("-" * 60)

    prospects = get_prospects_to_email()
    existing_ids = get_existing_queue_ids()
    prospects = [p for p in prospects if p.get("id") not in existing_ids]

    print(f"  {len(prospects)} prospects ready for email drafts")

    drafted = 0

    for prospect in prospects:
        if drafted >= max_drafts:
            print(f"\n  DAILY CAP REACHED: {drafted} drafts")
            break

        pid = prospect.get("id", "unknown")
        name = prospect.get("name", "Unknown")
        email = prospect.get("email", "")
        owner = prospect.get("owner", "")

        print(f"\n  Drafting for: {name} ({owner}) -> {email}")

        email_data = generate_email(prospect)
        if not email_data:
            print(f"    Skipped: could not generate email")
            continue

        if dry_run:
            print(f"    [DRY RUN] Subject: {email_data['subject']}")
            print(f"    [DRY RUN] To: {email_data['to_email']}")
            drafted += 1
            continue

        # Save to Gmail drafts
        gmail_ok = False
        if gmail:
            gmail_ok = create_gmail_draft(
                gmail,
                email_data["to_email"],
                email_data["to_name"],
                email_data["subject"],
                email_data["body_html"],
                email_data["body_text"],
            )

        # Also save to agent_queue for tracking
        insert_draft_to_queue(pid, email_data)

        # Move prospect from "Not Contacted" → "Phone Call Ready"
        patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
        requests.patch(patch_url, headers=sb_headers(), json={
            "status": "PHONE CALL READY",
            "action": "Email drafted — review in Gmail, then call",
        }, timeout=15)

        if gmail_ok:
            drafted += 1
            print(f"    Saved to Gmail + queue — status → Phone Call Ready")
        else:
            drafted += 1
            print(f"    Saved to queue — status → Phone Call Ready (Gmail unavailable)")

    # =========================================================================
    # SUMMARY + SMS
    # =========================================================================
    print("\n" + "=" * 60)
    print(f"  Unify Cold Email Agent — Draft Complete")
    print(f"  {'='*56}")
    print(f"     Names enriched     : {enriched_count} ({source_summary or 'none'})")
    print(f"     Email drafts       : {drafted}")
    print(f"     Saved to Gmail     : {'Yes' if gmail else 'No (token missing)'}")
    print("=" * 60)

    if drafted > 0:
        msg = (
            f"Unify: {drafted} new leads ready to be called. "
            f"Emails drafted in your Gmail — review, send, and call. "
            f"Enriched {enriched_count} owner names this run."
        )
    elif enriched_count > 0:
        msg = (
            f"Unify: Enriched {enriched_count} owner names, "
            f"0 new drafts this run (no prospects with email + owner ready). "
            f"Leads are building — more coming soon."
        )
    else:
        msg = (
            f"Unify: No new leads to call right now. "
            f"0 drafts, 0 enriched this run. All caught up."
        )

    print(f"\n  Notifying Franco...")
    if not dry_run:
        send_sms(msg)
    else:
        print(f"  [DRY RUN] SMS: {msg}")

    print("\n  Done.")


# ==============================================================================
# SEND MODE — Send approved emails via Resend
# ==============================================================================

def run_send(dry_run=False):
    """Send approved emails from agent_queue via Resend API."""
    print("=" * 60)
    print("  Unify Cold Email Agent v4.0 — SEND MODE")
    print("=" * 60)
    print(f"  Resend : {'Configured' if RESEND_API_KEY else 'No key'}")
    print(f"  Sender : {SENDER_EMAIL}")
    print()

    if not RESEND_API_KEY and not dry_run:
        print("  ERROR: RESEND_API_KEY not configured.")
        send_sms("Unify Email Agent ERROR: RESEND_API_KEY not set.")
        return

    approved = get_approved_emails()
    print(f"  Found {len(approved)} approved emails to send")

    if not approved:
        msg = "Unify: 0 approved emails to send. Approve drafts first."
        print(f"\n  {msg}")
        if not dry_run:
            send_sms(msg)
        return

    sent = 0
    failed = 0

    for item in approved:
        payload = item.get("payload", {})
        queue_id = item.get("id")
        prospect_id = item.get("prospect_id")

        to_email = payload.get("to_email", "")
        to_name = payload.get("to_name", "")
        subject = payload.get("subject", "")
        body_html = payload.get("body_html", "")
        body_text = payload.get("body_text", "")

        print(f"\n  Sending: {subject} -> {to_email}")

        if dry_run:
            print(f"    [DRY RUN] Would send")
            sent += 1
            continue

        if send_email_via_resend(to_email, to_name, subject, body_html, body_text):
            update_queue_status(queue_id, "sent")
            update_prospect_after_send(prospect_id)
            sent += 1
        else:
            failed += 1

        time.sleep(2)

    if sent > 0:
        msg = f"Unify: {sent} cold emails sent! {failed} failed."
    else:
        msg = f"Unify: 0 emails sent. {failed} failed."

    if not dry_run:
        send_sms(msg)

    print(f"\n  Done. {sent} sent, {failed} failed.")


# -- CLI ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Unify Cold Email Agent v4.0")
    parser.add_argument("--draft", action="store_true",
                        help="Enrich names + generate email drafts in Gmail")
    parser.add_argument("--send", action="store_true",
                        help="Send approved emails via Resend")
    parser.add_argument("--max", "-m", type=int, default=20,
                        help="Max drafts per run (default: 20)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Preview without writing to DB, Gmail, or sending")
    args = parser.parse_args()

    if not args.draft and not args.send:
        print("Error: Must specify --draft or --send")
        parser.print_help()
        sys.exit(1)

    if args.draft:
        run_draft(max_drafts=args.max, dry_run=args.dry_run)
    elif args.send:
        run_send(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
