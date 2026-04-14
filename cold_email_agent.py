#!/usr/bin/env python3
"""
Unify Cold Email Agent v5.1
=============================
1. Enriches CRM leads (owner names, emails, websites, years in business)
2. Constructs info@domain emails when no email found (MX verified)
3. Drafts personalized cold emails using 3-tier hook + 9 vertical templates
4. Flags missing website/email in CRM for Franco's visibility

Three modes:
  --enrich-only : Enrich leads only (self-hosted runner on Franco's PC)
  --draft       : Create Gmail drafts from enriched data (GitHub Actions)
  --send        : Send approved emails via Resend (manual trigger)

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

# Dead-end email prefixes -- never worth emailing
DEAD_END_EMAILS = {"noreply@", "no-reply@", "donotreply@", "do-not-reply@"}

# Aggregator / directory / social domains -- emails on these aren't real
# business inboxes. They're scraped artifacts (e.g. info@canpages.ca is
# the directory itself, not the business), or social platforms that don't
# accept external email.
DEAD_END_DOMAINS = {
    "canpages.ca",
    "foodpages.ca",
    "yellowpages.ca",
    "yp.ca",
    "411.ca",
    "findopen.ca",
    "findopenhours.com",
    "cylex-canada.ca",
    "yelp.com",
    "yelp.ca",
    "bbb.org",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "google.com",
}


def _is_dead_end_email(email: str) -> bool:
    """True if email is a dead-end prefix (noreply@) or an aggregator domain."""
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return False
    if any(e.startswith(p) for p in DEAD_END_EMAILS):
        return True
    domain = e.split("@", 1)[1]
    if any(domain == d or domain.endswith("." + d) for d in DEAD_END_DOMAINS):
        return True
    return False


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
    """Fetch prospects that need enrichment — no owner name OR no email."""
    all_prospects = get_all_prospects()
    needs = []
    for p in all_prospects:
        has_owner = bool((p.get("owner") or "").strip())
        has_email = bool((p.get("email") or "").strip())
        # Need enrichment if missing owner OR missing email
        if not has_owner or not has_email:
            needs.append(p)
    return needs


def get_prospects_to_email(redraft=False):
    """
    Fetch prospects ready for email drafting.
    The ONLY hard filter is: must have an email address.
    Owner name is preferred but NOT required — use "Hi there" fallback.
    If redraft=True, also includes prospects already in PHONE CALL READY stage.
    """
    all_prospects = get_all_prospects()
    print(f"  [DEBUG] Total prospects fetched: {len(all_prospects)}")
    ready = []
    allowed_statuses = {"NOT CONTACTED", "PHONE CALL READY"} if redraft else {"NOT CONTACTED"}
    no_email = 0
    wrong_status = 0
    dead_end = 0
    for p in all_prospects:
        email = (p.get("email") or "").strip().lower()
        status = (p.get("status") or "").strip().upper()

        # Email is the ONLY hard filter
        if not email or "@" not in email:
            no_email += 1
            continue
        # Skip dead-end addresses (noreply prefixes + aggregator domains)
        if _is_dead_end_email(email):
            dead_end += 1
            continue
        if status not in allowed_statuses:
            wrong_status += 1
            continue

        ready.append(p)
    print(f"  [DEBUG] Skipped: {no_email} no email, {dead_end} dead-end, {wrong_status} wrong status")
    print(f"  [DEBUG] Ready for drafting: {len(ready)}")
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

# Domains to skip when extracting business websites from YP listings
_SKIP_WEBSITE_DOMAINS = [
    "yellowpages.ca", "pagesjaunes.ca", "yp.ca", "ypcdn",
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "google.com", "yelp.com", "bbb.org", "411.ca", "canada411",
    "linkedin.com", "tiktok.com", "pinterest.com", "youtube.com",
]

# Google cooldown (recoverable, not permanent)
_google_cooldown_until = 0


def _extract_domain(url):
    """Extract clean domain from a URL (strips protocol, www, path)."""
    if not url:
        return ""
    cleaned = url.lower().replace("https://", "").replace("http://", "").replace("www.", "")
    return cleaned.split("/")[0].strip()


def _domain_has_mx(domain):
    """Verify domain can receive email via MX record lookup."""
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, 'MX')
        return len(answers) > 0
    except Exception:
        # If dnspython not installed or DNS fails, try basic resolution
        try:
            import socket
            socket.getaddrinfo(domain, 25)
            return True
        except Exception:
            return False


def _get_mx_host(domain):
    """Get primary MX host for a domain (lowest preference value)."""
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, 'MX')
        mx_records = sorted([(r.preference, str(r.exchange).rstrip('.')) for r in answers])
        return mx_records[0][1] if mx_records else ""
    except Exception:
        return ""


def _verify_smtp_mailbox(email, timeout=8):
    """
    Verify a mailbox exists via SMTP RCPT TO (without sending any message).
    Returns True if mailbox accepts. Returns False on block/reject/port-blocked.

    NOTE: Many residential ISPs (Rogers, Bell, Comcast) block outbound port 25.
    In that case this silently returns False and the caller should fall back
    to _domain_has_mx() construction.
    """
    try:
        import smtplib
        if not email or "@" not in email:
            return False
        domain = email.split("@")[1].strip().lower()
        mx_host = _get_mx_host(domain)
        if not mx_host:
            return False

        with smtplib.SMTP(timeout=timeout) as server:
            server.connect(mx_host, 25)
            server.helo("unifyaipartners.ca")
            server.mail("franco@unifyaipartners.ca")
            code, _ = server.rcpt(email)
        # 250/251/252 = accepted; 550/551/553 = rejected; 4xx = greylist (treat as unknown -> False)
        return code in (250, 251, 252)
    except Exception:
        return False


# Email patterns to try (ordered by likelihood of success)
_EMAIL_PATTERNS = ["info", "contact", "hello", "office", "admin", "inquiries", "team"]


def _find_verified_email(domain):
    """
    Try multiple common email patterns against a domain using SMTP RCPT TO.
    Returns first verified pattern. Falls back to info@domain if MX exists but
    no pattern verifies (port 25 blocked, catch-all server, etc.) — never returns
    worse than existing info@ construction.
    """
    if not domain or not _domain_has_mx(domain):
        return ""

    for pattern in _EMAIL_PATTERNS:
        candidate = f"{pattern}@{domain}"
        if _verify_smtp_mailbox(candidate, timeout=8):
            print(f"     [SMTP] Verified: {candidate}")
            return candidate
        time.sleep(0.3)  # be polite to mail servers

    # Fallback: info@ with just MX validation (same as old behavior)
    print(f"     [SMTP] No pattern verified -- falling back to info@{domain} (MX only)")
    return f"info@{domain}"


def _scrape_facebook_email(business_name, city):
    """
    Search for business's Facebook page (via mobile FB) and scrape email from
    About/Contact section. Returns email or "".
    Fails silently if FB blocks — no permanent state, no cooldown needed.
    """
    try:
        query = f"{business_name} {city}"
        search_url = f"https://m.facebook.com/public/{quote_plus(query)}"
        r = requests.get(search_url, headers=HEADERS, timeout=10, allow_redirects=True)
        if r.status_code != 200:
            return ""

        soup = BeautifulSoup(r.text, "lxml")

        # Find first FB page link matching business
        page_url = ""
        biz_lower = business_name.lower()
        biz_first = biz_lower.split()[0] if biz_lower.split() else ""
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            # FB page links look like /pages/... or /BusinessName-123/
            if not (href.startswith("/") or "facebook.com/" in href):
                continue
            # Skip search/login/help URLs
            if any(x in href for x in ["/search", "/login", "/help", "/privacy", "/terms"]):
                continue
            if biz_first and biz_first in text and len(text) > 2:
                page_url = href if href.startswith("http") else f"https://m.facebook.com{href}"
                break

        if not page_url:
            return ""

        # Fetch page (About section often on main URL in mobile FB)
        time.sleep(random.uniform(2, 4))
        base_url = page_url.split("?")[0].rstrip("/")
        for suffix in ["/about", ""]:
            try:
                target = base_url + suffix
                r2 = requests.get(target, headers=HEADERS, timeout=10)
                if r2.status_code != 200:
                    continue
                # Extract first non-junk email
                for match in re.finditer(r'[\w.+-]+@[\w-]+\.[\w.-]+', r2.text):
                    email = match.group(0).lower().rstrip(".")
                    if email.endswith((".png", ".jpg", ".gif", ".webp", ".svg")):
                        continue
                    if any(d in email for d in ["fbcdn", "fb.com"]):
                        continue
                    if _is_dead_end_email(email):
                        continue
                    print(f"     [Facebook] Found email: {email}")
                    return email
            except Exception:
                continue

    except Exception as e:
        print(f"     [Facebook] Error: {e}")

    return ""


def _google_search_email(business_name, city):
    """
    Absolute last-resort Google search for business email address.
    STRICTLY respects Google cooldown and adds long random delay (15-30s)
    to avoid triggering spam block. Uses longer cooldown (600s) on 429.
    """
    if not _is_google_available():
        print(f"     [Google Email] Skipped -- cooldown active")
        return ""

    try:
        # Long randomized delay to look human
        delay = random.uniform(15, 30)
        print(f"     [Google Email] Waiting {delay:.0f}s before search...")
        time.sleep(delay)

        query = f'"{business_name}" "{city}" email contact'
        url = f"https://www.google.com/search?q={quote_plus(query)}&num=10&gl=ca&hl=en"
        r = requests.get(url, headers=HEADERS, timeout=12)

        if r.status_code == 429:
            _set_google_cooldown(600)  # longer cooldown since this is last-resort
            print(f"     [Google Email] 429 -- extended cooldown 600s")
            return ""
        if r.status_code != 200:
            return ""

        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text(" ", strip=True)

        # Pull all emails, filter junk
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
        biz_first = business_name.lower().split()[0] if business_name else ""

        candidates = []
        for email in emails:
            email = email.lower().rstrip(".")
            if email.endswith((".png", ".jpg", ".gif", ".webp", ".svg")):
                continue
            if any(d in email for d in ["googlemail", "gstatic", "googleusercontent", "schema.org", "w3.org"]):
                continue
            if _is_dead_end_email(email):
                continue
            candidates.append(email)

        # Prefer emails where domain or local-part matches business name
        for email in candidates:
            local = email.split("@")[0]
            domain = email.split("@")[1] if "@" in email else ""
            if biz_first and len(biz_first) > 3 and (biz_first in domain or biz_first in local):
                print(f"     [Google Email] Found business match: {email}")
                return email

        # Otherwise first reasonable candidate
        if candidates:
            print(f"     [Google Email] Found: {candidates[0]}")
            return candidates[0]

    except Exception as e:
        print(f"     [Google Email] Error: {e}")

    return ""


def _is_google_available():
    return time.time() >= _google_cooldown_until


def _set_google_cooldown(seconds=300):
    global _google_cooldown_until
    _google_cooldown_until = time.time() + seconds
    print(f"     [Google] Cooldown set for {seconds}s")


def enrich_prospect(business_name, city, website_url="", existing_email=""):
    """
    Full enrichment pipeline — datacenter-safe sources FIRST, email boosters LAST:
      1. YellowPages.ca (always works from datacenter — finds website URL + owner)
      2. Business website About/Team/Contact pages (uses URL from YP or CRM)
      3. Google panel (fallback — reviews + owner, cooldown on 429)
      4. LinkedIn + Facebook via Google (owner name fallback)
      5. SMTP multi-pattern email discovery (info/contact/hello/office/admin)
      6. Facebook page email scraping (business FB page About section)
      7. Google "{business} email" search (absolute last resort, respects cooldown)
    Returns dict: {owner, source, stars, review_count, personal_email, found_email,
                   years_in_business, specialization, website}
    """
    result = {
        "owner": "", "source": "", "stars": 0,
        "review_count": 0, "personal_email": "", "found_email": "",
        "years_in_business": 0, "specialization": "", "website": "",
    }

    # =========================================================================
    # STEP 1: YellowPages (ALWAYS runs first — proven on GitHub Actions)
    # This is our workhorse: finds website URLs, emails, sometimes owners.
    # =========================================================================
    print(f"     [YP] Searching for {business_name} in {city}...")
    yp_result = _scrape_yellowpages_owner(business_name, city)

    # Always grab YP enrichment data (years, specialization) regardless of owner
    if yp_result.get("years_in_business"):
        result["years_in_business"] = yp_result["years_in_business"]
    if yp_result.get("specialization"):
        result["specialization"] = yp_result["specialization"]

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
        # Only skip website scraping if we already have an email
        if result["found_email"] or result["personal_email"]:
            result["website"] = website_url
            return result
        # else: fall through to website scraping for email

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
                result["website"] = website_url
                return result

    # =========================================================================
    # STEP 5: SMTP multi-pattern email discovery (when we have a domain)
    # Tries info/contact/hello/office/admin/inquiries/team via SMTP RCPT TO.
    # Falls back to info@domain if port 25 blocked or no pattern verifies —
    # never worse than the old info@-only construction.
    # =========================================================================
    if not result["found_email"] and not existing_email and website_url:
        domain = _extract_domain(website_url)
        if domain:
            verified = _find_verified_email(domain)
            if verified:
                result["found_email"] = verified
            else:
                print(f"     [SMTP] {domain} has no MX records -- skipped")

    # =========================================================================
    # STEP 6: Facebook page email scraping (last-resort before Google)
    # =========================================================================
    if not result["found_email"] and not existing_email:
        print(f"     [Facebook] Searching for page...")
        fb_email = _scrape_facebook_email(business_name, city)
        if fb_email:
            result["found_email"] = fb_email

    # =========================================================================
    # STEP 7: Google search "business email" (absolute last resort)
    # Respects existing Google cooldown + adds 15-30s delay to avoid spam block.
    # Extended cooldown (600s) on 429 since this is the last fallback.
    # =========================================================================
    if not result["found_email"] and not existing_email and _is_google_available():
        g_email = _google_search_email(business_name, city)
        if g_email:
            result["found_email"] = g_email

    result["website"] = website_url
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
        base + "/contact",
        base + "/contact-us",
        base + "/about",
        base + "/about-us",
        base + "/team",
        base + "/our-team",
        base + "/staff",
        base + "/people",
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

            # Footer-specific email extraction (many small biz sites only show email in footer)
            if not found_emails:
                footer = soup.select_one("footer, [class*='footer'], [id*='footer']")
                if footer:
                    for mailto in footer.select("a[href^='mailto:']"):
                        email = mailto.get("href", "").replace("mailto:", "").split("?")[0].strip().lower()
                        if email and "@" in email and email not in found_emails:
                            found_emails.append(email)
                    for match in re.finditer(r'[\w.+-]+@[\w-]+\.[\w.-]+', footer.get_text(" ", strip=True)):
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
    result = {"owner": "", "email": "", "website": "", "years_in_business": 0, "specialization": ""}
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
        for listing in listings[:10]:
            # Get listing name — try text from multiple elements
            name_el = listing.select_one(
                "a.listing__name--link, h3.listing__name, "
                "a[class*='listing__name'], span.listing__name, h2 a, h3 a"
            )
            if not name_el:
                continue
            listed_name = name_el.get_text(strip=True).lower()
            biz_lower = business_name.lower()

            # Strip leading "1" prefix from lead sourcer artifacts
            if biz_lower.startswith("1") and not biz_lower[1:2].isdigit():
                biz_lower = biz_lower[1:]

            # Flexible matching
            first_word = biz_lower.split()[0] if biz_lower.split() else ""
            match = (
                biz_lower[:8] in listed_name
                or listed_name[:8] in biz_lower
                or (first_word and len(first_word) > 3 and first_word in listed_name)
                or listed_name in biz_lower
            )
            if match:
                # Get detail link — may be on a child <a>, not on name_el itself
                link_el = listing.select_one("a.listing__name--link, a[href*='/bus/']")
                if link_el:
                    href = link_el.get("href", "")
                    if href and not href.startswith("http"):
                        href = "https://www.yellowpages.ca" + href
                    detail_url = href

                # Grab website from listing — YP uses /gourl/ redirect links
                for a in listing.select("a[href*='/gourl/']"):
                    href = a.get("href", "")
                    if "redirect=" in href:
                        from urllib.parse import unquote
                        actual = unquote(href.split("redirect=")[1].split("&")[0])
                        if actual.startswith("http") and not any(d in actual.lower() for d in _SKIP_WEBSITE_DOMAINS):
                            result["website"] = actual
                            break
                if not result["website"]:
                    web_el = listing.select_one(
                        "a[class*='website'], a[data-analytics='website']"
                    )
                    if web_el:
                        href = web_el.get("href", "")
                        if not any(d in href.lower() for d in _SKIP_WEBSITE_DOMAINS):
                            result["website"] = href

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

            # Grab website if not found yet — check /gourl/ redirects first
            if not result["website"]:
                for a in detail_soup.select("a[href*='/gourl/']"):
                    href = a.get("href", "")
                    if "redirect=" in href:
                        from urllib.parse import unquote
                        actual = unquote(href.split("redirect=")[1].split("&")[0])
                        if actual.startswith("http") and not any(d in actual.lower() for d in _SKIP_WEBSITE_DOMAINS):
                            result["website"] = actual
                            break
            if not result["website"]:
                for a in detail_soup.select("a[href^='http']"):
                    href = a.get("href", "")
                    if not any(d in href.lower() for d in _SKIP_WEBSITE_DOMAINS):
                        result["website"] = href
                        break

            # Extract years in business from detail page
            years_match = re.search(
                r'(?:in\s*business\s*since|established|since|founded)\s*:?\s*(\d{4})',
                detail_text, re.I
            )
            if years_match:
                founded = int(years_match.group(1))
                result["years_in_business"] = datetime.now().year - founded
                if result["years_in_business"] > 0:
                    print(f"     [YP] Years in business: {result['years_in_business']}")
            if not result["years_in_business"]:
                yrs_match = re.search(r'(\d+)\+?\s*years?\s*(?:in\s*business|of\s*experience|serving)', detail_text, re.I)
                if yrs_match:
                    result["years_in_business"] = int(yrs_match.group(1))
                    print(f"     [YP] Years in business: {result['years_in_business']}")

            # Extract specialization/subcategory from detail page
            cat_el = detail_soup.select_one(
                "span.listing__category, a.listing__category, "
                "span[class*='category'], div.categories a"
            )
            if cat_el:
                result["specialization"] = cat_el.get_text(strip=True)
                print(f"     [YP] Specialization: {result['specialization']}")

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
        # Handle hyphenated names: JEAN-PIERRE -> Jean-Pierre
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
        "better business", "yellow pages", "white pages", "pages jaunes",
        "all rights", "click here", "find out", "get started",
        "of this business", "this business", "claim this", "claim your",
        "write a review", "report this", "is this your",
        "and operator", "and founder", "and manager", "and owner",
        "and principal", "and president", "and director",
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


# 3-tier personalized hooks by vertical — one entry per (vertical, tier).
# All three tiers produce COMPLETE opening paragraphs (not fragments).
#   tier1: reviews data available (stars >= 4.0 AND count >= 10)
#   tier2: years in business available (years >= 10)
#   tier3: no data — warmer, vaguer "research" framing
_VERTICAL_HOOKS = {
    "Restaurants": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — people don't keep coming back like that unless something's being done right, and that's not easy to curate.",
        "tier2": "{years} years serving the same community says something — longevity like that doesn't happen by accident, it means you're doing right by your regulars.",
        "tier3": "I do research on independent restaurants across the GTA, which is how I came across {business_name}. You had that feel of a spot where real people still care about what they're doing, and those are the businesses I like to connect with.",
    },
    "Retail": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — customers don't leave that kind of feedback unless something's being done right, and that's not easy to curate.",
        "tier2": "{years} years serving the same neighbourhood says something — longevity like that doesn't happen by accident in retail, it means you're doing right by the people who walk through your door.",
        "tier3": "I do research on independent retail shops across the GTA, which is how I came across {business_name}. You had that feel of a shop where real people still care about what they're doing, and those are the businesses I like to connect with.",
    },
    "Trades": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — trades reviews like that don't show up by accident, it means you're doing the work right, and that's not easy to curate.",
        "tier2": "{years} years on the tools says something — longevity like that doesn't happen by accident, it means you've built a name on referrals and repeat work.",
        "tier3": "I do research on independent trades across the GTA, which is how I came across {business_name}. You had that feel of a business where real people still care about doing the job right, and those are the ones I like to connect with.",
    },
    "Dental & Medical": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — patients don't leave feedback like that unless they trust the care they're getting, and that's not easy to curate.",
        "tier2": "{years} years serving the same community says something — longevity like that doesn't happen by accident in healthcare, it means you've earned the trust of your patients.",
        "tier3": "I do research on independent practices across the GTA, which is how I came across {business_name}. You had that feel of a practice where real people still care about the patients in front of them, and those are the ones I like to connect with.",
    },
    "Salons & Spas": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — clients don't keep coming back like that unless something's being done right, and that's not easy to curate.",
        "tier2": "{years} years behind the chair says something — longevity like that doesn't happen by accident, it means you're doing right by your regulars.",
        "tier3": "I do research on independent salons and spas across the GTA, which is how I came across {business_name}. You had that feel of a place where real people still care about the clients in their chair, and those are the businesses I like to connect with.",
    },
    "Professional Services": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — clients don't recommend firms like that unless something's being done right, and that's not easy to curate.",
        "tier2": "{years} years of serving clients says something — longevity like that doesn't happen by accident, it means you've built trust with the people you work for.",
        "tier3": "I do research on independent professional service firms across the GTA, which is how I came across {business_name}. You had that feel of a firm where real people still care about their clients, and those are the businesses I like to connect with.",
    },
    "Fitness & Wellness": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — members don't stick around like that unless something's being done right, and that's not easy to curate.",
        "tier2": "{years} years in the community says something — longevity like that doesn't happen by accident, it means you've built something people keep coming back to.",
        "tier3": "I do research on independent fitness and wellness studios across the GTA, which is how I came across {business_name}. You had that feel of a place where real people still care about the members who walk through the door, and those are the businesses I like to connect with.",
    },
    "Auto Services": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — customers don't leave feedback like that for an auto shop unless you're doing right by them, and that's not easy to curate.",
        "tier2": "{years} years in the bay says something — longevity like that doesn't happen by accident, it means you've earned the trust of your customers.",
        "tier3": "I do research on independent auto shops across the GTA, which is how I came across {business_name}. You had that feel of a shop where real people still care about doing the work right, and those are the businesses I like to connect with.",
    },
    "Cleaning & Property": {
        "tier1": "A {rating} rating across {reviews} reviews tells me a lot — clients don't keep renewing like that unless something's being done right, and that's not easy to curate.",
        "tier2": "{years} years of holding onto clients says something — longevity like that doesn't happen by accident, it means you're delivering consistently.",
        "tier3": "I do research on independent cleaning and property businesses across the GTA, which is how I came across {business_name}. You had that feel of a business where real people still care about the work, and those are the ones I like to connect with.",
    },
}

# Fallback tier 3 for unknown verticals — keeps same shape
_DEFAULT_TIER3 = "I do research on independent local businesses across the GTA, which is how I came across {business_name}. You had that feel of a place where real people still care about what they're doing, and those are the businesses I like to connect with."


def _generate_hook(business_name, city, notes, vertical):
    """
    Generate a personalized hook using 3-tier system:
      Tier 1: Reviews (stars >= 4.0 AND count >= 10) — most personalized
      Tier 2: Years in business (years >= 10) — still grounded in data
      Tier 3: No data — warmer, vaguer "research" framing
    Returns a complete opening paragraph (not a fragment).
    """
    stars = 0.0
    count = 0
    years = 0

    if notes:
        star_match = re.search(r'(\d+\.?\d*)\s*stars?,\s*(\d+)\s*reviews?', notes)
        if star_match:
            stars = float(star_match.group(1))
            count = int(star_match.group(2))
        years_match = re.search(r'(\d+)\s*(?:years?\s*in\s*business|yrs?\s*in\s*biz)', notes, re.I)
        if years_match:
            years = int(years_match.group(1))

    templates = _VERTICAL_HOOKS.get(vertical)
    if not templates:
        return _DEFAULT_TIER3.format(business_name=business_name)

    # Tier 1: credible reviews (quality + volume)
    if stars >= 4.0 and count >= 10:
        rating_str = f"{stars:.1f}"
        return templates["tier1"].format(rating=rating_str, reviews=count, business_name=business_name)

    # Tier 2: established business (10+ years)
    if years >= 10:
        return templates["tier2"].format(years=years, business_name=business_name)

    # Tier 3: no hard data — softer, vaguer warmth
    # Strip trailing period (e.g. "Brandvision Inc.") so the template sentence
    # doesn't produce a double period before "You had that feel...".
    return templates["tier3"].format(business_name=business_name.rstrip('.'))


def generate_email(prospect):
    """
    Generate a personalized cold email for a prospect.
    Owner name is preferred but NOT required — uses "Hi there" fallback.
    The ONLY hard filter is: must have an email address.
    """
    email = (prospect.get("email") or "").strip()
    if not email or "@" not in email:
        return None

    owner = (prospect.get("owner") or "").strip()
    business_name = prospect.get("name", "your business")
    vertical = prospect.get("cat", "")
    ai_gap = prospect.get("opp", "")
    address = prospect.get("address", "")
    notes = prospect.get("notes", "")

    # Greeting: use owner first name if available, "Hi {business} team" if not
    if owner:
        first_name = owner.split()[0]
        greeting = f"Hi {first_name}"
        to_name = first_name
    else:
        greeting = f"Hi {business_name} team"
        to_name = business_name

    # Extract city
    city = "the GTA"
    if address:
        parts = address.split(",")
        if len(parts) >= 2:
            city = parts[-2].strip()
        elif parts:
            city = parts[0].strip()

    # Single subject line
    subject = f"Quick question about {business_name}"

    # 3-tier personalized hook (reviews -> years -> specialization)
    hook = _generate_hook(business_name, city, notes, vertical)

    # Industry-specific body copy (9 verticals)
    copy = VERTICAL_COPY.get(vertical, _DEFAULT_COPY)
    pitch = copy["pitch"]
    consult = copy["consult"]

    body_text = (
        f"{greeting},\n\n"
        f"{hook}\n\n"
        f"{pitch}\n\n"
        f"{consult}\n\n"
        f"Open to a quick chat? Reply here or text me at (647) 210-3737.\n\n"
        f"Look forward to hearing from you.\n\n"
        f"--\n"
        f"Franco Di Giovanni\n"
        f"Founder | Unify AI Partners\n"
        f"(647) 210-3737\n"
        f"franco@unifyaipartners.ca\n"
        f"unifyaipartners.ca"
    )

    # Professional HTML signature with Unify logo
    logo_url = "https://raw.githubusercontent.com/francod2004/unify-crm/main/assets/unify_logo.png"
    signature_html = (
        '<table cellpadding="0" cellspacing="0" border="0" style="margin-top:24px;border-top:2px solid #024AA5;padding-top:16px;font-family:Arial,Helvetica,sans-serif;">'
        '<tr>'
        # Logo column
        '<td style="vertical-align:top;padding-right:14px;">'
        f'<a href="https://unifyaipartners.ca" style="text-decoration:none;">'
        f'<img src="{logo_url}" alt="Unify AI Partners" width="60" height="65" style="display:block;border:0;" />'
        f'</a>'
        '</td>'
        # Divider
        '<td style="width:2px;background-color:#024AA5;font-size:0;line-height:0;" width="2">&nbsp;</td>'
        # Info column
        '<td style="vertical-align:top;padding-left:14px;">'
        '<table cellpadding="0" cellspacing="0" border="0">'
        '<tr><td style="font-size:15px;font-weight:700;color:#0A1E3D;padding-bottom:1px;font-family:Arial,Helvetica,sans-serif;">Franco Di Giovanni</td></tr>'
        '<tr><td style="font-size:11px;color:#024AA5;padding-bottom:8px;font-family:Arial,Helvetica,sans-serif;letter-spacing:0.8px;font-weight:600;">FOUNDER</td></tr>'
        '<tr><td style="font-size:12px;color:#555;padding-bottom:3px;font-family:Arial,Helvetica,sans-serif;">'
        '(647) 210-3737'
        '</td></tr>'
        '<tr><td style="font-size:12px;padding-bottom:3px;font-family:Arial,Helvetica,sans-serif;">'
        '<a href="mailto:franco@unifyaipartners.ca" style="color:#024AA5;text-decoration:none;">franco@unifyaipartners.ca</a>'
        '</td></tr>'
        '<tr><td style="font-size:12px;font-family:Arial,Helvetica,sans-serif;">'
        '<a href="https://unifyaipartners.ca" style="color:#024AA5;text-decoration:none;font-weight:600;">unifyaipartners.ca</a>'
        '</td></tr>'
        '</table>'
        '</td>'
        '</tr>'
        '</table>'
    )

    body_html = (
        f"<p style='font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333;'>{greeting},</p>"
        f"<p style='font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333;'>{hook}</p>"
        f"<p style='font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333;'>{pitch}</p>"
        f"<p style='font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333;'>{consult}</p>"
        f"<p style='font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333;'>Open to a quick chat? Reply here or text me at (647) 210-3737.</p>"
        f"<p style='font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333;'>Look forward to hearing from you.</p>"
        f"{signature_html}"
    )

    return {
        "to_email": email,
        "to_name": to_name,
        "subject": subject,
        "body_html": body_html,
        "body_text": body_text,
        "vertical": vertical,
        "ai_gap": ai_gap,
    }


# ==============================================================================
# DRAFT MODE — Main workflow
# ==============================================================================

def run_enrich(max_enrich=50, dry_run=False):
    """
    ENRICHMENT ONLY — for self-hosted runner on Franco's PC.
    Enriches prospects missing owner name, email, or website.
    Does NOT draft emails or touch Gmail.
    """
    print("=" * 60)
    print("  Unify Cold Email Agent v5.1 -- ENRICH MODE")
    print("=" * 60)
    print(f"  Max enrich  : {max_enrich}")
    print(f"  Dry run     : {dry_run}")
    print(f"  Supabase    : {'Connected' if SUPABASE_KEY else 'No key'}")
    print()

    needs_enrichment = get_prospects_needing_enrichment()
    has_owner_no_email = len([p for p in needs_enrichment if (p.get("owner") or "").strip() and not (p.get("email") or "").strip()])
    no_owner = len([p for p in needs_enrichment if not (p.get("owner") or "").strip()])
    print(f"  Found {len(needs_enrichment)} prospects needing enrichment")
    print(f"    - {no_owner} missing owner name")
    print(f"    - {has_owner_no_email} have owner but missing email")
    print(f"  Enriching up to {max_enrich} prospects this run")

    enriched_count = 0
    emails_found = 0
    websites_found = 0
    no_website_flagged = 0
    no_email_flagged = 0
    source_counts = {}

    # Wall-clock cut-off: stop processing new prospects at 35 min so SMS summary
    # still fires before the 45-min GitHub Actions workflow timeout kills us.
    # Anything already persisted (via per-prospect Supabase PATCHes above) is kept.
    ENRICH_TIMEOUT_SECONDS = 35 * 60
    start_time = time.time()
    timed_out = False

    for i, prospect in enumerate(needs_enrichment[:max_enrich]):
        elapsed = time.time() - start_time
        if elapsed > ENRICH_TIMEOUT_SECONDS:
            print(f"\n  [TIMEOUT] Wall-clock cut-off hit at {elapsed/60:.1f} min. "
                  f"Stopping after {i} prospects; proceeding to SMS summary.")
            timed_out = True
            break

        pid = prospect.get("id", "unknown")
        name = prospect.get("name", "Unknown")
        address = prospect.get("address", "")
        existing_email = prospect.get("email", "")
        website = prospect.get("website", "") or ""

        notes = prospect.get("notes", "") or ""
        if not website:
            url_match = re.search(r'https?://[^\s]+', notes)
            if url_match:
                website = url_match.group(0)

        city = "Toronto"
        if address:
            parts = address.split(",")
            if len(parts) >= 2:
                city = parts[-2].strip()
            elif parts:
                city = parts[0].strip()

        print(f"\n  [{i+1}/{min(len(needs_enrichment), max_enrich)}] "
              f"Enriching: {name} ({city})")
        print(f"    Website URL: {website or 'NONE'}")
        print(f"    Existing email: {existing_email or 'NONE'}")
        enrichment = enrich_prospect(name, city, website_url=website, existing_email=existing_email)

        # Store enrichment data (reviews, years, specialization) in notes field
        notes_additions = ""
        if enrichment["stars"] > 0:
            review_note = f" | {enrichment['stars']} stars, {enrichment['review_count']} reviews"
            if review_note not in notes:
                notes_additions += review_note
        if enrichment.get("years_in_business") and enrichment["years_in_business"] > 0:
            years_note = f" | {enrichment['years_in_business']} years in business"
            if "years in business" not in notes:
                notes_additions += years_note
        if enrichment.get("specialization"):
            spec_note = f" | Specialization: {enrichment['specialization']}"
            if "Specialization:" not in notes:
                notes_additions += spec_note

        # Flag no website found -- low digital presence (Change 8)
        if not enrichment.get("website") and not website:
            if "No website found" not in notes:
                notes_additions += " | No website found -- low digital presence"
                no_website_flagged += 1
                print(f"    Flagged: no website found (low digital presence)")

        # Flag no email found (Change 9)
        if not enrichment.get("found_email") and not existing_email:
            if "No email found" not in notes:
                notes_additions += " | No email found"
                no_email_flagged += 1
                print(f"    Flagged: no email found")

        if notes_additions and not dry_run:
            patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
            requests.patch(patch_url, headers=sb_headers(),
                json={"notes": notes + notes_additions}, timeout=15)

        # Store discovered website URL (Change 2)
        if enrichment.get("website") and not website and not dry_run:
            patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
            requests.patch(patch_url, headers=sb_headers(),
                json={"website": enrichment["website"]}, timeout=15)
            websites_found += 1
            print(f"    Website stored: {enrichment['website']}")

        if enrichment["owner"]:
            enriched_count += 1
            src = enrichment["source"]
            source_counts[src] = source_counts.get(src, 0) + 1

            print(f"    Found: {enrichment['owner']} via {src}")
            if not dry_run:
                update_data = {"owner": enrichment["owner"]}
                if enrichment["personal_email"]:
                    update_data["email"] = enrichment["personal_email"]
                    emails_found += 1
                    print(f"    Personal email: {enrichment['personal_email']}")
                elif enrichment.get("found_email") and not existing_email:
                    update_data["email"] = enrichment["found_email"]
                    emails_found += 1
                    print(f"    Email: {enrichment['found_email']}")
                patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
                requests.patch(patch_url, headers=sb_headers(),
                    json=update_data, timeout=15)
        else:
            if enrichment.get("found_email") and not existing_email and not dry_run:
                patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
                requests.patch(patch_url, headers=sb_headers(),
                    json={"email": enrichment["found_email"]}, timeout=15)
                emails_found += 1
                print(f"    No owner, but found email: {enrichment['found_email']}")
            elif not enrichment.get("found_email") and not existing_email:
                print(f"    No owner or email found")
            else:
                print(f"    No owner found")

        time.sleep(random.uniform(2, 4))

    source_summary = ", ".join(f"{v} {k}" for k, v in sorted(source_counts.items(), key=lambda x: -x[1]))
    # `i` is the last prospect index we touched (0-based). If we broke on timeout,
    # we processed `i` prospects (0..i-1). If the loop ran to completion, we
    # processed i+1 prospects (0..i inclusive).
    if timed_out:
        attempted = i
    else:
        attempted = min(len(needs_enrichment), max_enrich)
    elapsed_min = (time.time() - start_time) / 60
    print(f"\n  {'='*56}")
    print(f"  ENRICHMENT SUMMARY")
    print(f"  {'='*56}")
    print(f"    Total needing enrichment : {len(needs_enrichment)}")
    print(f"    Attempted this run       : {attempted}")
    print(f"    Elapsed                  : {elapsed_min:.1f} min")
    print(f"    Timed out                : {timed_out}")
    print(f"    Owner names found        : {enriched_count}")
    print(f"    Emails found/constructed : {emails_found}")
    print(f"    Websites discovered      : {websites_found}")
    print(f"    Flagged no website       : {no_website_flagged}")
    print(f"    Flagged no email         : {no_email_flagged}")
    print(f"    Sources breakdown        : {source_summary or 'none'}")
    print(f"    Google available         : {'Yes' if _is_google_available() else 'No (cooldown)'}")

    prefix = "Unify Enrichment (PARTIAL - 35m cutoff)" if timed_out else "Unify Enrichment"
    msg = (
        f"{prefix}: {attempted} prospects processed in {elapsed_min:.0f}m. "
        f"{enriched_count} owners, {emails_found} emails found, "
        f"{websites_found} websites discovered. "
        f"{no_website_flagged} flagged no website, {no_email_flagged} flagged no email."
    )
    print(f"\n  Notifying Franco...")
    if not dry_run:
        send_sms(msg)
    else:
        print(f"  [DRY RUN] SMS: {msg}")

    print("\n  Done.")


def _clear_cold_email_queue():
    """Delete all cold_email entries from agent_queue so drafts can be regenerated."""
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?action_type=eq.cold_email"
    )
    r = requests.delete(url, headers=sb_headers(), timeout=15)
    if r.status_code in (200, 204):
        print("  Cleared old cold_email entries from agent_queue")
        return True
    else:
        print(f"  WARNING: Could not clear agent_queue ({r.status_code})")
        return False


def run_draft(max_drafts=20, dry_run=False, redraft=False):
    """
    DRAFT MODE — generates email drafts from already-enriched data.
    Does NOT run enrichment. Use --enrich-only for that.
    If redraft=True, clears old queue entries and regenerates all drafts.
    """
    mode_label = "REDRAFT MODE" if redraft else "DRAFT MODE"
    print("=" * 60)
    print(f"  Unify Cold Email Agent v5.1 -- {mode_label}")
    print("=" * 60)
    print(f"  Max drafts  : {max_drafts}")
    print(f"  Dry run     : {dry_run}")
    print(f"  Redraft     : {redraft}")
    print(f"  Supabase    : {'Connected' if SUPABASE_KEY else 'No key'}")
    print(f"  Gmail       : Checking...")
    print()

    # If redraft, reset stages and clear old queue entries
    if redraft and not dry_run:
        # Reset "PHONE CALL READY" prospects back to "NOT CONTACTED"
        reset_url = (
            f"{SUPABASE_URL}/rest/v1/prospects"
            f"?status=eq.PHONE CALL READY"
        )
        r = requests.patch(reset_url, headers=sb_headers(),
                           json={"status": "NOT CONTACTED", "action": None},
                           timeout=15)
        if r.status_code in (200, 204):
            reset_count = len(r.json()) if r.text.strip() else 0
            print(f"  Reset {reset_count} prospects from PHONE CALL READY -> NOT CONTACTED")
        else:
            print(f"  WARNING: Could not reset stages ({r.status_code}: {r.text[:200]})")
        _clear_cold_email_queue()

    # Set up Gmail
    gmail = None
    if not dry_run:
        gmail = get_gmail_service()
        if gmail:
            print("  Gmail       : Connected")
        else:
            print("  Gmail       : NOT connected -- drafts will only go to agent_queue")

    # =========================================================================
    # Draft emails for ALL prospects with email addresses
    # Owner name preferred but not required — "Hi there" fallback
    # =========================================================================
    print("\n" + "-" * 60)
    print("  Drafting cold emails (email = only hard filter)")
    print("-" * 60)

    prospects = get_prospects_to_email(redraft=redraft)
    if not redraft:
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

        # Move prospect from "Not Contacted" -> "Phone Call Ready"
        patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
        requests.patch(patch_url, headers=sb_headers(), json={
            "status": "PHONE CALL READY",
            "action": "Email drafted -- review in Gmail, then call",
        }, timeout=15)

        if gmail_ok:
            drafted += 1
            print(f"    Saved to Gmail + queue — status -> Phone Call Ready")
        else:
            drafted += 1
            print(f"    Saved to queue — status -> Phone Call Ready (Gmail unavailable)")

    # =========================================================================
    # SUMMARY + SMS
    # =========================================================================
    print("\n" + "=" * 60)
    print(f"  Unify Cold Email Agent -- Draft Complete")
    print(f"  {'='*56}")
    print(f"     Email drafts       : {drafted}")
    print(f"     Saved to Gmail     : {'Yes' if gmail else 'No (token missing)'}")
    print("=" * 60)

    if drafted > 0:
        msg = (
            f"Unify: {drafted} new leads ready to be called. "
            f"Emails drafted in your Gmail -- review, send, and call."
        )
    else:
        msg = (
            f"Unify: 0 new drafts this run. "
            f"All prospects either already drafted or no email available."
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
    print("  Unify Cold Email Agent v5.1 -- SEND MODE")
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
    parser = argparse.ArgumentParser(description="Unify Cold Email Agent v5.1")
    parser.add_argument("--draft", action="store_true",
                        help="Generate email drafts in Gmail (no enrichment)")
    parser.add_argument("--redraft", action="store_true",
                        help="Clear old drafts from queue and regenerate all emails")
    parser.add_argument("--send", action="store_true",
                        help="Send approved emails via Resend")
    parser.add_argument("--enrich-only", action="store_true",
                        help="Run enrichment only (for self-hosted runner)")
    parser.add_argument("--max", "-m", type=int, default=20,
                        help="Max items per run (default: 20)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Preview without writing to DB, Gmail, or sending")
    args = parser.parse_args()

    if not args.draft and not args.send and not args.enrich_only and not args.redraft:
        print("Error: Must specify --draft, --redraft, --send, or --enrich-only")
        parser.print_help()
        sys.exit(1)

    if args.enrich_only:
        run_enrich(max_enrich=args.max, dry_run=args.dry_run)
    elif args.redraft:
        run_draft(max_drafts=args.max, dry_run=args.dry_run, redraft=True)
    elif args.draft:
        run_draft(max_drafts=args.max, dry_run=args.dry_run)
    elif args.send:
        run_send(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
