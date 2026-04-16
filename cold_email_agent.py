#!/usr/bin/env python3
"""
Unify Cold Email Agent v5.2
=============================
Reads enriched prospects from Supabase and drafts personalized cold emails
using the 3-tier hook system and 9 vertical-specific templates. Enrichment
has been extracted into enrichment_agent.py; this agent no longer fetches
websites or scrapes directories.

Modes:
  --draft    : Create Gmail drafts from enriched data
  --redraft  : Clear old queue entries and regenerate all drafts
  --send     : Send approved emails via Resend (manual trigger)

RULE: Never send an email without Franco's explicit approval.
Drafts go to Gmail for Franco to review, personalize, and send manually.

Usage:
    python cold_email_agent.py --draft              # Draft to Gmail
    python cold_email_agent.py --send               # Send approved queue items
    python cold_email_agent.py --draft --max 20     # Limit drafts per run
    python cold_email_agent.py --dry-run --draft    # Preview without writing

Requires env vars or .env file + gmail_token.json
"""

import os, sys, re, json, time, argparse, base64
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests

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
    Enrichment is handled separately by enrichment_agent.py.
    If redraft=True, clears old queue entries and regenerates all drafts.
    """
    mode_label = "REDRAFT MODE" if redraft else "DRAFT MODE"
    print("=" * 60)
    print(f"  Unify Cold Email Agent v5.2 -- {mode_label}")
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
    print("  Unify Cold Email Agent v5.2 -- SEND MODE")
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
    parser = argparse.ArgumentParser(description="Unify Cold Email Agent v5.2")
    parser.add_argument("--draft", action="store_true",
                        help="Generate email drafts in Gmail (no enrichment)")
    parser.add_argument("--redraft", action="store_true",
                        help="Clear old drafts from queue and regenerate all emails")
    parser.add_argument("--send", action="store_true",
                        help="Send approved emails via Resend")
    parser.add_argument("--max", "-m", type=int, default=20,
                        help="Max items per run (default: 20)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Preview without writing to DB, Gmail, or sending")
    args = parser.parse_args()

    if not args.draft and not args.send and not args.redraft:
        print("Error: Must specify --draft, --redraft, or --send")
        parser.print_help()
        sys.exit(1)

    if args.redraft:
        run_draft(max_drafts=args.max, dry_run=args.dry_run, redraft=True)
    elif args.draft:
        run_draft(max_drafts=args.max, dry_run=args.dry_run)
    elif args.send:
        run_send(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
