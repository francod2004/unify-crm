#!/usr/bin/env python3
"""
Unify Cold Email Agent v1.0
=============================
Reads prospects from Supabase, enriches missing owner names via
Google-LinkedIn search, drafts personalized cold emails per vertical,
stores drafts in agent_queue for Franco's approval, and sends approved ones.

Two-phase design:
  --draft  : Enrich + generate email drafts (runs daily, unattended)
  --send   : Send all approved emails via Resend (manual trigger)

RULE: Never send an email without Franco's explicit approval.

Usage:
    python cold_email_agent.py --draft              # Generate drafts
    python cold_email_agent.py --send               # Send approved emails
    python cold_email_agent.py --draft --max 20     # Limit drafts per run
    python cold_email_agent.py --dry-run --draft    # Preview without writing

Requires env vars or .env file
"""

import os, sys, re, json, time, random, argparse
from datetime import datetime, timezone
from urllib.parse import quote_plus

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
TWILIO_FROM   = os.getenv("TWILIO_FROM", "")
FRANCO_PHONE  = os.getenv("FRANCO_PHONE", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
SENDER_EMAIL   = os.getenv("SENDER_EMAIL", "franco@unifyaipartners.ca")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Generic email prefixes to skip
GENERIC_EMAILS = {
    "info@", "admin@", "noreply@", "no-reply@", "contact@",
    "hello@", "support@", "sales@", "office@", "help@",
    "webmaster@", "mail@", "enquiries@", "inquiries@",
}


# -- Supabase Helpers ---------------------------------------------------------

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

def get_prospects_to_email():
    """
    Fetch prospects that need email drafts:
    - status = NOT CONTACTED
    - has email
    - not a generic email
    """
    url = (
        f"{SUPABASE_URL}/rest/v1/prospects"
        f"?select=*"
        f"&status=eq.NOT CONTACTED"
        f"&email=neq."
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        prospects = r.json()
        # Filter out prospects with no email or generic emails
        filtered = []
        for p in prospects:
            email = (p.get("email") or "").strip().lower()
            if not email or not "@" in email:
                continue
            if any(email.startswith(prefix) for prefix in GENERIC_EMAILS):
                continue
            filtered.append(p)
        return filtered
    print(f"  Warning: Could not fetch prospects: {r.status_code} {r.text[:200]}")
    return []

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
    print(f"  Warning: Could not fetch agent_queue: {r.status_code}")
    return set()

def insert_draft(prospect_id, payload):
    """Insert a draft email into agent_queue."""
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
    if r.status_code in (200, 201):
        return True
    print(f"  Warning: Insert draft failed: {r.status_code} {r.text[:200]}")
    return False

def update_prospect_owner(prospect_id, owner_name):
    """Write enriched owner name back to the prospect record."""
    url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{prospect_id}"
    r = requests.patch(url, headers=sb_headers(), json={"owner": owner_name}, timeout=15)
    if r.status_code in (200, 204):
        return True
    print(f"  Warning: Update owner failed: {r.status_code}")
    return False

def update_prospect_after_send(prospect_id):
    """Update prospect after email is sent."""
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{prospect_id}"
    r = requests.patch(
        url, headers=sb_headers(),
        json={
            "last_contact": today,
            "action": "Follow up in 5 days",
        },
        timeout=15,
    )
    return r.status_code in (200, 204)

def update_queue_status(queue_id, status):
    """Update agent_queue item status (sent, rejected)."""
    url = f"{SUPABASE_URL}/rest/v1/agent_queue?id=eq.{queue_id}"
    r = requests.patch(url, headers=sb_headers(), json={"status": status}, timeout=15)
    return r.status_code in (200, 204)

def get_approved_emails():
    """Fetch approved cold email drafts from agent_queue."""
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?select=*"
        f"&action_type=eq.cold_email"
        f"&status=eq.approved"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return r.json()
    print(f"  Warning: Could not fetch approved emails: {r.status_code}")
    return []


# -- Twilio SMS ---------------------------------------------------------------

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
        data={"From": TWILIO_FROM, "To": FRANCO_PHONE, "Body": body[:1600]},
        timeout=15,
    )
    if r.status_code == 201:
        print(f"  SMS sent to {FRANCO_PHONE}")
        return True
    print(f"  Warning: SMS failed ({r.status_code}): {r.text[:200]}")
    return False


# -- Resend Email Sending -----------------------------------------------------

def send_email_via_resend(to_email, to_name, subject, body_html, body_text):
    """Send an email via Resend API."""
    if not RESEND_API_KEY:
        print("  Warning: RESEND_API_KEY not set -- cannot send")
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


# -- LinkedIn Enrichment via Google Search ------------------------------------

def enrich_owner_from_linkedin(business_name, city):
    """
    Search Google for LinkedIn profiles linked to this business.
    Returns owner name or empty string.
    """
    queries = [
        f'site:linkedin.com/in "{business_name}" "{city}" owner OR founder OR manager OR director',
        f'site:linkedin.com/in "{business_name}" "{city}"',
    ]

    for query in queries:
        try:
            url = f"https://www.google.com/search?q={quote_plus(query)}&num=5&gl=ca&hl=en"
            r = requests.get(url, headers=HEADERS, timeout=10)

            if r.status_code == 429:
                print(f"     [LinkedIn] Google 429 -- rate limited, skipping")
                return ""

            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text, "lxml")

            # Parse search results for LinkedIn profile patterns
            # LinkedIn titles show: "FirstName LastName - Title - Company | LinkedIn"
            for div in soup.select("div.g, div[data-sokoban-container]"):
                title_el = div.select_one("h3")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)

                # Match pattern: "Name - Title - Company | LinkedIn"
                # or "Name | LinkedIn"
                linkedin_match = re.match(
                    r'^([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[-|]',
                    title
                )
                if linkedin_match:
                    name = linkedin_match.group(1).strip()
                    # Validate: must be 2+ words, no digits, reasonable length
                    if (len(name.split()) >= 2 and
                        len(name) < 40 and
                        not any(c.isdigit() for c in name)):
                        print(f"     [LinkedIn] Found: {name}")
                        return name

            # Also try snippet text
            for div in soup.select("div.g"):
                snippet_el = div.select_one("div.VwiC3b, span.st")
                if not snippet_el:
                    continue
                snippet = snippet_el.get_text(strip=True)

                # Look for "Owner", "Founder", etc. followed by a name
                for pattern in [
                    r'(?:Owner|Founder|Manager|Director|President|CEO)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)',
                    r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s*[-,]\s*(?:Owner|Founder|Manager|Director|President|CEO)',
                ]:
                    match = re.search(pattern, snippet)
                    if match:
                        name = match.group(1).strip()
                        if len(name) < 40 and not any(c.isdigit() for c in name):
                            print(f"     [LinkedIn] Found in snippet: {name}")
                            return name

            # Delay between queries
            time.sleep(random.uniform(3, 5))

        except Exception as e:
            print(f"     [LinkedIn] Error: {e}")

    return ""


# -- Email Templates ----------------------------------------------------------

EMAIL_TEMPLATES = {
    "Restaurants": {
        "angle": "AI-powered booking systems, automated review responses, and menu optimization",
        "save": "10-15 hours a week",
    },
    "Retail": {
        "angle": "AI inventory management, customer chatbots, and personalized marketing",
        "save": "hours on manual inventory tracking and customer service",
    },
    "Trades": {
        "angle": "AI scheduling and dispatch, automated quoting, and review management",
        "save": "time on scheduling, follow-ups, and admin work",
    },
    "Dental & Medical": {
        "angle": "AI appointment booking, patient reminders, and automated intake forms",
        "save": "hours on front-desk admin and patient communication",
    },
    "Salons & Spas": {
        "angle": "AI online booking, no-show prediction, and client retention automation",
        "save": "time on booking management and client follow-ups",
    },
    "Professional Services": {
        "angle": "AI client intake, document automation, and smart scheduling",
        "save": "hours on admin, paperwork, and scheduling",
    },
    "Fitness & Wellness": {
        "angle": "AI class scheduling, member retention, and billing automation",
        "save": "time on class management and member communication",
    },
    "Auto Services": {
        "angle": "AI appointment booking, parts inventory tracking, and customer follow-up",
        "save": "hours on scheduling and customer reminders",
    },
    "Cleaning & Property": {
        "angle": "AI scheduling and routing, automated quoting, and customer portals",
        "save": "time on route planning, quoting, and invoicing",
    },
}

def generate_email(prospect):
    """
    Generate a personalized cold email for a prospect.
    Returns dict with subject, body_html, body_text, or None if can't generate.
    """
    owner = (prospect.get("owner") or "").strip()
    if not owner:
        return None

    first_name = owner.split()[0] if owner else "there"
    business_name = prospect.get("name", "your business")
    vertical = prospect.get("cat", "")
    ai_gap = prospect.get("opp", "")
    address = prospect.get("address", "")

    # Extract city from address
    city = "the GTA"
    if address:
        parts = address.split(",")
        if len(parts) >= 2:
            city = parts[-2].strip()  # Usually "City" before "ON"
        elif parts:
            city = parts[0].strip()

    # Get vertical-specific template
    template = EMAIL_TEMPLATES.get(vertical, {
        "angle": "AI automation tailored to your business",
        "save": "hours on manual work each week",
    })

    subject = f"Quick question about {business_name}"

    body_text = (
        f"Hi {first_name},\n\n"
        f"I came across {business_name} in {city} and noticed you might benefit from "
        f"some automation around {template['angle'].split(',')[0].lower()}.\n\n"
        f"We work with local {vertical.lower()} businesses in the GTA to set up things like "
        f"{template['angle']} - stuff that saves {template['save']} without changing "
        f"how you already operate.\n\n"
        f"Would you be open to a quick 15-minute call this week to see if it's a fit?\n\n"
        f"Best,\n"
        f"Franco Di Giovanni\n"
        f"Unify AI Partners - AI Automation for Local Businesses\n"
        f"franco@unifyaipartners.ca\n\n"
        f"---\n"
        f"If you'd prefer not to receive emails from us, just reply with 'unsubscribe'."
    )

    body_html = (
        f"<p>Hi {first_name},</p>"
        f"<p>I came across <strong>{business_name}</strong> in {city} and noticed you might benefit from "
        f"some automation around {template['angle'].split(',')[0].lower()}.</p>"
        f"<p>We work with local {vertical.lower()} businesses in the GTA to set up things like "
        f"{template['angle']} &mdash; stuff that saves {template['save']} without changing "
        f"how you already operate.</p>"
        f"<p>Would you be open to a quick 15-minute call this week to see if it's a fit?</p>"
        f"<p>Best,<br>"
        f"Franco Di Giovanni<br>"
        f"<strong>Unify AI Partners</strong> &mdash; AI Automation for Local Businesses<br>"
        f"<a href='mailto:franco@unifyaipartners.ca'>franco@unifyaipartners.ca</a></p>"
        f"<hr style='border:none;border-top:1px solid #ddd;margin-top:20px'>"
        f"<p style='font-size:11px;color:#999'>If you'd prefer not to receive emails from us, "
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


# -- Draft Mode ---------------------------------------------------------------

def run_draft(max_drafts=20, dry_run=False):
    """
    Main draft mode:
    1. Fetch prospects needing emails
    2. Enrich missing owner names via LinkedIn
    3. Generate personalized email drafts
    4. Store in agent_queue
    5. SMS Franco
    """
    print("=" * 60)
    print("  Unify Cold Email Agent v1.0 — DRAFT MODE")
    print("=" * 60)
    print(f"  Max drafts  : {max_drafts}")
    print(f"  Dry run     : {dry_run}")
    print(f"  Supabase    : {'Connected' if SUPABASE_KEY else 'No key'}")
    print(f"  Twilio      : {'Configured' if TWILIO_SID else 'Skipped'}")
    print()

    # Step 1: Fetch prospects
    print("Fetching prospects to email...")
    prospects = get_prospects_to_email()
    print(f"  Found {len(prospects)} prospects with email + NOT CONTACTED")

    if not prospects:
        msg = "Unify Email Agent: 0 prospects need emails. All caught up."
        print(f"\n  {msg}")
        if not dry_run:
            send_sms(msg)
        return

    # Step 2: Filter out already-drafted prospects
    print("Checking agent_queue for existing drafts...")
    existing_ids = get_existing_queue_ids()
    prospects = [p for p in prospects if p.get("id") not in existing_ids]
    print(f"  {len(prospects)} prospects need new drafts (filtered {len(existing_ids)} existing)")

    if not prospects:
        msg = "Unify Email Agent: All prospects already have drafts. No new drafts needed."
        print(f"\n  {msg}")
        if not dry_run:
            send_sms(msg)
        return

    # Step 3: Enrich + Draft
    drafted = 0
    enriched = 0
    skipped_no_name = 0
    skipped_generic = 0

    for prospect in prospects[:max_drafts * 2]:  # Process extra to fill max_drafts
        if drafted >= max_drafts:
            break

        pid = prospect.get("id", "unknown")
        name = prospect.get("name", "Unknown")
        owner = (prospect.get("owner") or "").strip()
        email = (prospect.get("email") or "").strip()

        print(f"\n  Processing: {name} ({pid})")

        # Enrich owner if missing
        if not owner:
            city = ""
            address = prospect.get("address", "")
            if address:
                parts = address.split(",")
                city = parts[-2].strip() if len(parts) >= 2 else parts[0].strip()
            city = city or "Toronto"

            print(f"    No owner — searching LinkedIn for {name} in {city}...")
            owner = enrich_owner_from_linkedin(name, city)

            if owner:
                enriched += 1
                prospect["owner"] = owner
                if not dry_run:
                    update_prospect_owner(pid, owner)
                print(f"    Enriched: {owner}")
            else:
                print(f"    No owner found — skipping")
                skipped_no_name += 1
                continue

            # Rate limit between LinkedIn searches
            time.sleep(random.uniform(2, 4))

        # Generate email
        email_data = generate_email(prospect)
        if not email_data:
            skipped_no_name += 1
            continue

        # Store draft
        if dry_run:
            print(f"    [DRY RUN] Would draft: {email_data['subject']}")
            print(f"    To: {email_data['to_email']} ({email_data['to_name']})")
            drafted += 1
        else:
            if insert_draft(pid, email_data):
                print(f"    Drafted: {email_data['subject']}")
                drafted += 1
            else:
                print(f"    Failed to insert draft")

    # Step 4: Summary
    print("\n" + "=" * 60)
    print(f"  Unify Cold Email Agent — Draft Complete")
    print(f"  {'='*56}")
    print(f"     Drafts created   : {drafted}")
    print(f"     Owners enriched  : {enriched}")
    print(f"     Skipped (no name): {skipped_no_name}")
    print("=" * 60)

    # Step 5: SMS
    if drafted > 0:
        msg = (
            f"Unify: {drafted} cold email drafts ready for review! "
            f"{enriched} owners found via LinkedIn. "
            f"Skipped {skipped_no_name} (no name found). "
            f"Review and approve in CRM."
        )
    else:
        msg = (
            f"Unify Email Agent: 0 drafts created. "
            f"Enriched {enriched} owners, skipped {skipped_no_name} (no name). "
            f"Need more leads with owner names."
        )

    print(f"\n  Notifying Franco...")
    if not dry_run:
        send_sms(msg)
    else:
        print(f"  [DRY RUN] SMS would be: {msg}")

    print("\n  Draft run complete.")


# -- Send Mode ----------------------------------------------------------------

def run_send(dry_run=False):
    """
    Send mode:
    1. Fetch approved emails from agent_queue
    2. Send via Resend
    3. Update queue status to 'sent'
    4. Update prospect last_contact and action
    5. SMS Franco with results
    """
    print("=" * 60)
    print("  Unify Cold Email Agent v1.0 — SEND MODE")
    print("=" * 60)
    print(f"  Dry run     : {dry_run}")
    print(f"  Resend      : {'Configured' if RESEND_API_KEY else 'No key'}")
    print(f"  Sender      : {SENDER_EMAIL}")
    print()

    if not RESEND_API_KEY and not dry_run:
        print("  ERROR: RESEND_API_KEY not configured. Cannot send emails.")
        send_sms("Unify Email Agent ERROR: RESEND_API_KEY not set. Cannot send.")
        return

    # Fetch approved emails
    print("Fetching approved emails...")
    approved = get_approved_emails()
    print(f"  Found {len(approved)} approved emails to send")

    if not approved:
        msg = "Unify Email Agent: 0 approved emails to send. Approve drafts first."
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

        print(f"\n  Sending to {to_email} ({to_name}): {subject}")

        if dry_run:
            print(f"    [DRY RUN] Would send email")
            sent += 1
            continue

        if send_email_via_resend(to_email, to_name, subject, body_html, body_text):
            update_queue_status(queue_id, "sent")
            update_prospect_after_send(prospect_id)
            sent += 1
        else:
            failed += 1

        # Rate limit: 1 email per 2 seconds
        time.sleep(2)

    # Summary
    print("\n" + "=" * 60)
    print(f"  Send Complete: {sent} sent, {failed} failed")
    print("=" * 60)

    if sent > 0:
        msg = f"Unify: {sent} cold emails sent successfully! {failed} failed."
    else:
        msg = f"Unify Email Agent: 0 emails sent. {failed} failed."

    print(f"\n  Notifying Franco...")
    if not dry_run:
        send_sms(msg)
    else:
        print(f"  [DRY RUN] SMS would be: {msg}")

    print("\n  Send run complete.")


# -- CLI ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Unify Cold Email Agent v1.0")
    parser.add_argument("--draft", action="store_true",
                        help="Generate email drafts for prospects")
    parser.add_argument("--send", action="store_true",
                        help="Send all approved email drafts")
    parser.add_argument("--max", "-m", type=int, default=20,
                        help="Max drafts per run (default: 20)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Preview without writing to DB or sending")
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
