#!/usr/bin/env python3
"""
Unify Cold Email Agent v2.0
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

        if not owner:
            continue
        if not email or "@" not in email:
            continue
        if any(email.startswith(prefix) for prefix in GENERIC_EMAILS):
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
# ENRICHMENT: Find owner names via Google search of LinkedIn + Facebook
# ==============================================================================

def enrich_owner_name(business_name, city):
    """
    Cross-reference LinkedIn and Facebook via Google search to find
    owner/manager names for a business.
    Returns (name, source) tuple or ("", "").
    """
    # Try LinkedIn first, then Facebook
    name = _search_linkedin(business_name, city)
    if name:
        return name, "LinkedIn"

    name = _search_facebook(business_name, city)
    if name:
        return name, "Facebook"

    return "", ""


def _search_linkedin(business_name, city):
    """Search Google for LinkedIn profiles linked to this business."""
    queries = [
        f'site:linkedin.com/in "{business_name}" "{city}" owner OR founder OR manager OR director',
        f'site:linkedin.com/in "{business_name}" "{city}"',
    ]
    return _google_search_for_name(queries, "LinkedIn")


def _search_facebook(business_name, city):
    """Search Google for Facebook business pages to find owner names."""
    queries = [
        f'site:facebook.com "{business_name}" "{city}" owner OR founder OR manager',
        f'site:facebook.com "{business_name}" "{city}"',
    ]
    return _google_search_for_name(queries, "Facebook")


def _google_search_for_name(queries, source_name):
    """
    Execute Google searches and extract person names from results.
    Works for both LinkedIn and Facebook result patterns.
    """
    for query in queries:
        try:
            url = f"https://www.google.com/search?q={quote_plus(query)}&num=5&gl=ca&hl=en"
            r = requests.get(url, headers=HEADERS, timeout=10)

            if r.status_code == 429:
                print(f"     [{source_name}] Google 429 -- rate limited")
                return ""
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "lxml")

            # Method 1: Parse search result titles
            # LinkedIn: "FirstName LastName - Title - Company | LinkedIn"
            # Facebook: "FirstName LastName - City | Facebook"
            for div in soup.select("div.g, div[data-sokoban-container]"):
                title_el = div.select_one("h3")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                # Extract name from beginning of title
                name_match = re.match(
                    r'^([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[-|~]',
                    title
                )
                if name_match:
                    name = name_match.group(1).strip()
                    if _is_valid_person_name(name):
                        print(f"     [{source_name}] Found in title: {name}")
                        return name

            # Method 2: Parse snippets for title patterns
            for div in soup.select("div.g"):
                snippet_el = div.select_one("div.VwiC3b, span.st")
                if not snippet_el:
                    continue
                snippet = snippet_el.get_text(strip=True)

                for pattern in [
                    r'(?:Owner|Founder|Manager|Director|President|CEO|Proprietor)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                    r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[-,]\s*(?:Owner|Founder|Manager|Director|President|CEO|Proprietor)',
                    r'(?:owned by|founded by|managed by|operated by)\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                ]:
                    match = re.search(pattern, snippet)
                    if match:
                        name = match.group(1).strip()
                        if _is_valid_person_name(name):
                            print(f"     [{source_name}] Found in snippet: {name}")
                            return name

            time.sleep(random.uniform(3, 5))

        except Exception as e:
            print(f"     [{source_name}] Error: {e}")

    return ""


def _is_valid_person_name(name):
    """Validate that a string looks like a real person's name."""
    if not name or len(name) < 3 or len(name) > 40:
        return False
    if any(c.isdigit() for c in name):
        return False
    if len(name.split()) < 2:
        return False
    # Filter out common false positives
    false_positives = {
        "About Us", "Contact Us", "Our Team", "Home Page",
        "Read More", "Learn More", "Sign Up", "Log In",
        "Privacy Policy", "Terms Service",
    }
    if name in false_positives:
        return False
    return True


# ==============================================================================
# EMAIL TEMPLATES — Industry-specific
# ==============================================================================

EMAIL_TEMPLATES = {
    "Restaurants": {
        "angle": "AI-powered booking systems, automated review responses, and menu optimization",
        "save": "10-15 hours a week",
        "pain": "managing reservations, responding to reviews, and keeping menus updated",
    },
    "Retail": {
        "angle": "AI inventory management, customer chatbots, and personalized marketing",
        "save": "hours on manual inventory tracking and customer service",
        "pain": "keeping track of stock, responding to customer inquiries, and running promotions",
    },
    "Trades": {
        "angle": "AI scheduling and dispatch, automated quoting, and review management",
        "save": "time on scheduling, follow-ups, and admin work",
        "pain": "coordinating jobs, chasing quotes, and managing your online reputation",
    },
    "Dental & Medical": {
        "angle": "AI appointment booking, patient reminders, and automated intake forms",
        "save": "hours on front-desk admin and patient communication",
        "pain": "no-shows, phone tag for appointments, and manual paperwork",
    },
    "Salons & Spas": {
        "angle": "AI online booking, no-show prediction, and client retention automation",
        "save": "time on booking management and client follow-ups",
        "pain": "last-minute cancellations, manual booking, and keeping clients coming back",
    },
    "Professional Services": {
        "angle": "AI client intake, document automation, and smart scheduling",
        "save": "hours on admin, paperwork, and scheduling",
        "pain": "manual intake processes, document prep, and scheduling back-and-forth",
    },
    "Fitness & Wellness": {
        "angle": "AI class scheduling, member retention, and billing automation",
        "save": "time on class management and member communication",
        "pain": "class scheduling conflicts, member churn, and billing headaches",
    },
    "Auto Services": {
        "angle": "AI appointment booking, parts inventory tracking, and customer follow-up",
        "save": "hours on scheduling and customer reminders",
        "pain": "missed appointments, parts tracking, and keeping customers updated",
    },
    "Cleaning & Property": {
        "angle": "AI scheduling and routing, automated quoting, and customer portals",
        "save": "time on route planning, quoting, and invoicing",
        "pain": "route inefficiencies, manual quoting, and chasing invoices",
    },
}


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

    # Extract city
    city = "the GTA"
    if address:
        parts = address.split(",")
        if len(parts) >= 2:
            city = parts[-2].strip()
        elif parts:
            city = parts[0].strip()

    template = EMAIL_TEMPLATES.get(vertical, {
        "angle": "AI automation tailored to your business",
        "save": "hours on manual work each week",
        "pain": "repetitive tasks that take up your day",
    })

    subject = f"Quick question about {business_name}"

    body_text = (
        f"Hi {first_name},\n\n"
        f"I came across {business_name} in {city} and had a quick question.\n\n"
        f"I know a lot of {vertical.lower()} businesses deal with {template['pain']}. "
        f"We've been helping local businesses in the GTA set up {template['angle']} "
        f"- stuff that saves {template['save']} without changing how you already operate.\n\n"
        f"Would you be open to a quick 15-minute call this week to see if it's a fit?\n\n"
        f"Best,\n"
        f"Franco Di Giovanni\n"
        f"Unify AI Partners - AI Automation for Local Businesses\n"
        f"franco@unifyaipartners.ca\n\n"
        f"---\n"
        f"If you'd prefer not to hear from us, just reply with 'unsubscribe'."
    )

    body_html = (
        f"<p>Hi {first_name},</p>"
        f"<p>I came across <strong>{business_name}</strong> in {city} and had a quick question.</p>"
        f"<p>I know a lot of {vertical.lower()} businesses deal with {template['pain']}. "
        f"We've been helping local businesses in the GTA set up {template['angle']} "
        f"&mdash; stuff that saves {template['save']} without changing how you already operate.</p>"
        f"<p>Would you be open to a quick 15-minute call this week to see if it's a fit?</p>"
        f"<p>Best,<br>"
        f"Franco Di Giovanni<br>"
        f"<strong>Unify AI Partners</strong> &mdash; AI Automation for Local Businesses<br>"
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
    print("  Unify Cold Email Agent v2.0 — DRAFT MODE")
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
    enriched_linkedin = 0
    enriched_facebook = 0

    # Cap enrichment to max_drafts * 2 to keep runs fast
    enrich_limit = max_drafts * 2
    print(f"  Enriching up to {enrich_limit} prospects this run")

    for prospect in needs_enrichment[:enrich_limit]:
        pid = prospect.get("id", "unknown")
        name = prospect.get("name", "Unknown")
        address = prospect.get("address", "")

        # Extract city for search
        city = "Toronto"
        if address:
            parts = address.split(",")
            if len(parts) >= 2:
                city = parts[-2].strip()
            elif parts:
                city = parts[0].strip()

        print(f"\n  Enriching: {name} ({city})")
        owner_name, source = enrich_owner_name(name, city)

        if owner_name:
            enriched_count += 1
            if source == "LinkedIn":
                enriched_linkedin += 1
            else:
                enriched_facebook += 1

            print(f"    Found: {owner_name} via {source}")
            if not dry_run:
                update_prospect_owner(pid, owner_name)
                # Also update the action field
                url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
                requests.patch(url, headers=sb_headers(),
                    json={"action": "Ready for cold email"}, timeout=15)
        else:
            print(f"    No owner found")

        # Rate limit between searches
        time.sleep(random.uniform(2, 4))

    print(f"\n  Enrichment complete: {enriched_count} names found")
    print(f"    LinkedIn: {enriched_linkedin}, Facebook: {enriched_facebook}")

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

        if gmail_ok:
            drafted += 1
            print(f"    Saved to Gmail drafts + agent_queue")
        else:
            drafted += 1
            print(f"    Saved to agent_queue only (Gmail unavailable)")

    # =========================================================================
    # SUMMARY + SMS
    # =========================================================================
    print("\n" + "=" * 60)
    print(f"  Unify Cold Email Agent — Draft Complete")
    print(f"  {'='*56}")
    print(f"     Names enriched     : {enriched_count} ({enriched_linkedin} LinkedIn, {enriched_facebook} Facebook)")
    print(f"     Email drafts       : {drafted}")
    print(f"     Saved to Gmail     : {'Yes' if gmail else 'No (token missing)'}")
    print("=" * 60)

    if drafted > 0:
        msg = (
            f"Unify: {drafted} email drafts ready in your Gmail! "
            f"Enriched {enriched_count} owner names. "
            f"Review drafts and send when ready."
        )
    elif enriched_count > 0:
        msg = (
            f"Unify: Enriched {enriched_count} owner names but 0 drafts "
            f"(no prospects with email + owner ready). "
            f"More leads needed."
        )
    else:
        msg = (
            f"Unify Email Agent: 0 drafts, 0 enriched. "
            f"Need more leads with contact info."
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
    print("  Unify Cold Email Agent v2.0 — SEND MODE")
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
    parser = argparse.ArgumentParser(description="Unify Cold Email Agent v2.0")
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
