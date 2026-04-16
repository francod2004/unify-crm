#!/usr/bin/env python3
"""
Unify Cold Email Agent v6 (Loom Pivot)
=======================================
Four-sentence cold emails that promise a 3-minute Loom. Reads enriched
prospects from Supabase and drafts personalized touches routed through
the agent_queue for Franco's approval.

Key v6 changes vs v5.2:
  - Email body collapsed to 4 sentences: greeting / hook / observation / Loom offer
  - Vertical body templates removed -- entire "sell the consultation" section cut
  - Subject kept: "Quick question about {business_name}"
  - Hooks read rating / review_count / years_in_business directly from Supabase
    columns (closes v6 wiring gap -- v5.2 parsed them out of the free-text notes)
  - Priority-ordered processing: high drafts first, medium second, low skipped
  - manual_work_signal drives sentence 3 (fallback per vertical if missing)
  - agent_queue status enum extended with loom_requested + loom_recorded
  - New follow-up sequence: Day 0 / Day 4 / Day 11 / Day 14 -- all via approval
  - Seven paused verticals raise PausedVerticalError if ever queried (sourcer
    filters to dental + trades, so reaching a paused vertical is a real bug)
  - pending -> sent transition is handled by a DB trigger that bumps
    prospects.touch_count + stamps last_touch_at atomically (see migrations)

Modes:
  --draft        : Draft Day-0 cold emails, priority-ordered
  --follow-ups   : Draft Day 4 / Day 11 / Day 14 touches for sent prospects
  --loom-script  : Poll loom_requested entries, draft 3-bullet Loom scripts
  --loom-recorded: Draft follow-up emails with Loom link for loom_recorded entries
  --send         : Send approved emails via Resend (unchanged from v5.2)
  --mark-sent    : Flip a queue entry to 'sent' manually (triggers touch update)
  --redraft      : Clear cold_email queue + reset PHONE CALL READY -> NOT CONTACTED

RULE: Never send an email without Franco's explicit approval.
"""

import os, sys, re, json, time, argparse, base64
from datetime import datetime, timezone, timedelta
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
CRM_URL        = os.getenv("CRM_URL", "https://unify-crm-coral.vercel.app")

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

# Dead-end email prefixes -- never worth emailing
DEAD_END_EMAILS = {"noreply@", "no-reply@", "donotreply@", "do-not-reply@"}

# Aggregator / directory / social domains -- emails on these aren't real
# business inboxes (scraped artifacts pointing to the directory itself).
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


# -- v6 Hook Dict (27 hooks, 9 verticals x 3 tiers) ---------------------------
#
# Tier 1: reviews (rating >= 4.0 AND review_count >= 10)
# Tier 2: years_in_business (>= 10)
# Tier 3: research framing fallback
#
# Only Dental & Medical + Trades are filled. The other seven are sentinels --
# the sourcer filters to dental + trades so a paused vertical should never
# reach this agent. If it does, raise loudly rather than papering over with
# placeholder copy.

_PAUSED_VERTICAL = object()  # sentinel


class PausedVerticalError(ValueError):
    """Raised when a hook lookup hits a paused vertical -- indicates the
    sourcer let through a prospect that shouldn't have reached the email
    agent. Re-enable the vertical's hooks before running again."""


_VERTICAL_HOOKS = {
    "Dental & Medical": {
        "tier1": "Saw the {rating} stars and {reviews}+ reviews on {business_name} -- the patient experience is clearly dialed in.",
        "tier2": "{years}+ years serving the community is rare in independent dental -- {business_name} has clearly built something patients stick with.",
        "tier3": "Been looking at independent dental practices across the GTA and {business_name} stood out as owner-operated rather than part of a corporate group.",
    },
    "Trades": {
        "tier1": "The {rating} stars and {reviews}+ reviews on {business_name} tell me the actual work is dialed -- most trades I look at don't come close to that.",
        "tier2": "{years}+ years in the trade is the kind of track record that usually means the work speaks for itself and referrals carry the business.",
        "tier3": "Been looking at owner-operated trades across the GTA and {business_name} came up as independent rather than a franchise -- that's specifically who I wanted to reach.",
    },
    # Seven paused verticals -- hooks to be written together when sourcer
    # re-expands beyond dental + trades (targeted v6.1, ~60 days out).
    "Restaurants":           _PAUSED_VERTICAL,
    "Retail":                _PAUSED_VERTICAL,
    "Salons & Spas":         _PAUSED_VERTICAL,
    "Professional Services": _PAUSED_VERTICAL,
    "Fitness & Wellness":    _PAUSED_VERTICAL,
    "Auto Services":         _PAUSED_VERTICAL,
    "Cleaning & Property":   _PAUSED_VERTICAL,
}


# Fallback observation (sentence 3) when manual_work_signal is missing.
# Phrased as an assertion, not a hedge. Any prospect that scored 3+ on the
# sourcer's manual-work checklist has the pattern -- we're just stating it
# without naming the specific tripwire that flagged the score. "I poked
# around" / "looks like it still" reads too casual and hedgy for cold
# professional outreach; "Took a look at your site --" is diagnostic.
_OBSERVATION_FALLBACK = {
    "Dental & Medical": (
        "Took a look at your site -- your intake still runs through the "
        "front desk for booking and confirmations."
    ),
    "Trades": (
        "Took a look at your site -- your quote process still runs through "
        "phone and email rather than a structured intake form."
    ),
}


def _get_vertical_hook_set(vertical: str):
    """Resolve a vertical name to its hook dict. Raises PausedVerticalError
    if the vertical is paused. Raises KeyError for unknown verticals."""
    if vertical not in _VERTICAL_HOOKS:
        raise KeyError(
            f"Unknown vertical '{vertical}' -- not in _VERTICAL_HOOKS. "
            f"Active verticals: Dental & Medical, Trades."
        )
    hooks = _VERTICAL_HOOKS[vertical]
    if hooks is _PAUSED_VERTICAL:
        raise PausedVerticalError(
            f"Hook for {vertical} is paused -- re-enable in v6.1 when sourcer "
            f"expands beyond dental/trades. A prospect in this vertical reached "
            f"the email agent, which means the sourcer's filter is broken or "
            f"legacy data is leaking through."
        )
    return hooks


# -- Gmail Setup --------------------------------------------------------------

def get_gmail_service():
    """Build Gmail API service from saved token."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        token_data = None
        if GMAIL_TOKEN_JSON:
            token_data = json.loads(GMAIL_TOKEN_JSON)
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
        msg["to"] = f"{to_name} <{to_email}>" if to_name else to_email
        msg["from"] = f"Franco Di Giovanni <{SENDER_EMAIL}>"
        msg["subject"] = subject

        msg.attach(MIMEText(body_text, "plain"))
        msg.attach(MIMEText(body_html, "html"))

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

def sb_headers(return_representation=False):
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation" if return_representation else "return=minimal",
    }


def _fetch_prospects(params: str):
    """Generic prospect fetcher. `params` is a raw PostgREST query string
    (without leading '?')."""
    url = f"{SUPABASE_URL}/rest/v1/prospects?{params}"
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return r.json()
    print(f"  Warning: prospect fetch failed ({r.status_code}): {r.text[:200]}")
    return []


def get_prospects_by_priority(priority: str, redraft: bool = False):
    """Fetch NOT CONTACTED prospects with email, filtered by priority.
    priority: 'high' | 'medium' | 'low'
    """
    statuses = "(NOT%20CONTACTED,PHONE%20CALL%20READY)" if redraft else "(NOT%20CONTACTED)"
    params = (
        f"select=*"
        f"&status=in.{statuses}"
        f"&email=not.is.null"
        f"&priority=eq.{priority}"
    )
    rows = _fetch_prospects(params)
    # Apply dead-end filter client-side (domain matching is easier than encoding in URL)
    return [p for p in rows if not _is_dead_end_email(p.get("email") or "")]


def get_prospects_for_loom_recorded():
    """Fetch prospects whose most recent agent_queue entry is loom_recorded
    AND whose loom_link is set (Franco has pasted it)."""
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?select=*,prospects(*)"
        f"&action_type=eq.cold_email"
        f"&status=eq.loom_recorded"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code != 200:
        print(f"  Warning: loom_recorded fetch failed ({r.status_code})")
        return []
    rows = r.json()
    return [row for row in rows if (row.get("prospects") or {}).get("loom_link")]


def get_loom_requested_entries():
    """Fetch agent_queue entries with status=loom_requested that don't yet
    have a loom_script in their payload."""
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?select=*,prospects(*)"
        f"&action_type=eq.cold_email"
        f"&status=eq.loom_requested"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code != 200:
        print(f"  Warning: loom_requested fetch failed ({r.status_code})")
        return []
    rows = r.json()
    return [row for row in rows if not (row.get("payload") or {}).get("loom_script")]


def get_existing_queue_ids(action_types=("cold_email",)):
    """Fetch prospect IDs that already have an entry in agent_queue."""
    types_filter = ",".join(action_types)
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?select=prospect_id"
        f"&action_type=in.({types_filter})"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code == 200:
        return {row["prospect_id"] for row in r.json() if row.get("prospect_id")}
    return set()


def get_sent_prospects_awaiting_followup():
    """Fetch prospects where the last cold_email entry is 'sent' and touch_count
    in (1, 2, 3) -- these need Day 4 / Day 11 / Day 14 follow-up evaluation."""
    url = (
        f"{SUPABASE_URL}/rest/v1/prospects"
        f"?select=*"
        f"&status=eq.PHONE%20CALL%20READY"
        f"&touch_count=gte.1"
        f"&touch_count=lte.3"
        f"&last_touch_at=not.is.null"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    if r.status_code != 200:
        print(f"  Warning: follow-up fetch failed ({r.status_code})")
        return []
    return r.json()


def insert_draft_to_queue(prospect_id, payload, action_type="cold_email"):
    """Insert a draft (email or loom script or follow-up) into agent_queue."""
    url = f"{SUPABASE_URL}/rest/v1/agent_queue"
    row = {
        "prospect_id": prospect_id,
        "action_type": action_type,
        "payload": payload,
        "status": "pending",
    }
    r = requests.post(url, headers=sb_headers(return_representation=True),
                      json=[row], timeout=15)
    if r.status_code in (200, 201):
        result = r.json()
        return result[0]["id"] if result else True
    print(f"    Queue insert failed ({r.status_code}): {r.text[:200]}")
    return False


def update_queue_payload(queue_id, payload):
    """Merge new payload data into an existing agent_queue entry."""
    url = f"{SUPABASE_URL}/rest/v1/agent_queue?id=eq.{queue_id}"
    r = requests.patch(url, headers=sb_headers(),
                       json={"payload": payload}, timeout=15)
    return r.status_code in (200, 204)


def update_queue_status(queue_id, status):
    url = f"{SUPABASE_URL}/rest/v1/agent_queue?id=eq.{queue_id}"
    r = requests.patch(url, headers=sb_headers(),
                       json={"status": status}, timeout=15)
    return r.status_code in (200, 204)


def mark_sent(queue_id, prospect_id=None):
    """Flip agent_queue.status to 'sent'. A DB trigger on agent_queue
    (bump_touch_on_send) updates prospects.last_touch_at and increments
    touch_count in the same transaction. If the trigger isn't installed yet,
    fall back to a two-step update (with explicit warning)."""
    ok = update_queue_status(queue_id, "sent")
    if not ok:
        return False
    # Best-effort verification that the trigger fired. If the trigger isn't
    # installed, emit a warning and do the fallback write.
    if prospect_id:
        # Read the prospect back to see if last_touch_at is recent
        url = (f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{prospect_id}"
               f"&select=last_touch_at,touch_count")
        r = requests.get(url, headers=sb_headers(), timeout=15)
        if r.status_code == 200 and r.json():
            row = r.json()[0]
            last = row.get("last_touch_at")
            if not last:
                # Trigger didn't fire -- do the writes manually
                print(f"    Warning: trigger not installed? Doing manual touch update.")
                patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{prospect_id}"
                new_count = (row.get("touch_count") or 0) + 1
                requests.patch(patch_url, headers=sb_headers(), json={
                    "last_touch_at": datetime.now(timezone.utc).isoformat(),
                    "touch_count": new_count,
                }, timeout=15)
    return True


def get_approved_emails():
    url = (
        f"{SUPABASE_URL}/rest/v1/agent_queue"
        f"?select=*"
        f"&action_type=eq.cold_email"
        f"&status=eq.approved"
    )
    r = requests.get(url, headers=sb_headers(), timeout=15)
    return r.json() if r.status_code == 200 else []


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


# -- Resend (for --send mode) -------------------------------------------------

def send_email_via_resend(to_email, to_name, subject, body_html, body_text):
    if not RESEND_API_KEY:
        print("  Warning: RESEND_API_KEY not set")
        return False
    r = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                 "Content-Type": "application/json"},
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


# =============================================================================
# v6 Email Generation
# =============================================================================

def _clean_business_name(name: str) -> str:
    """Strip the trailing period only on the three abbreviations where a
    template-closing '.' would produce a double period ("Inc..", "Corp..",
    "LLC.."). 'Co.', 'Ltd.', 'Co-op', etc. are preserved as integral to the
    name itself -- stripping them reads as if the name is incomplete.

    Recurring theme (see memory.md Lessons 2026-04-14): abbreviation
    handling and encoding edge cases bite this codebase repeatedly.
    Default-test any templating change against the fixtures below before
    shipping."""
    n = (name or "").rstrip()
    for suffix in (" Inc.", " Corp.", " LLC."):
        if n.endswith(suffix):
            return n[:-1]
    return n


def _to_em_dash(s: str) -> str:
    """Convert double-hyphen to Unicode em-dash in rendered email bodies.

    Source templates and print statements keep '--' for Windows cp1252
    safety (see memory.md: Unicode arrows and em-dashes in PowerShell /
    stdout trigger encoding errors on Windows). Email bodies ride Gmail
    and Resend as UTF-8 so real em-dash renders clean and reads more
    polished than '--' which looks like draft notation.

    Handles two cases: mid-sentence ' -- ' (spaces both sides) and the
    line-starting sign-off '-- Franco'. Any '--' inside a signal or
    other user-supplied text with spaces also gets converted, which is
    the desired behaviour."""
    return s.replace(" -- ", " \u2014 ").replace("-- Franco", "\u2014 Franco")


# -- v6.2 Subject Line Generation ---------------------------------------------
#
# Cold-outreach subjects. The body cannot perform if the email never opens,
# so the subject is the highest-leverage decision here.
#
# Day 0 (touch=1): bare lowercase cleaned business name ("peel plumbing inc").
#   Pattern-interrupts the SMB owner's inbox by reading as internal
#   correspondence rather than templated marketing. Owner-operators have
#   been trained to ignore "Quick question about X" / "Following up" over
#   years of spam.
# Day 4 (touch=2): "did this get buried?" -- no business name, no caps.
#   Business name absent by design: repeating it makes the follow-up feel
#   mechanical to the recipient. Lowercase by design: Title Case reads
#   corporate.
# Day 11 (touch=3, LinkedIn): no subject -- body-only. Helper raises.
# Day 14 (touch=4, SMS): no subject -- internal. Helper raises.
# Loom delivery (separate from the linear touch sequence): "here's that
#   loom" via dedicated _build_loom_delivery_subject(). See that helper.


class BannedSubjectError(ValueError):
    """Raised when the subject contains an anti-pattern (banned token,
    all-caps word, emoji, exclamation mark, or thread-faking prefix).

    For Day 0 subjects this can fire if the business name itself contains
    a banned word (e.g. a business literally called "AI Plumbing"). Such
    prospects should be surfaced in the run's SMS summary so Franco can
    hand-craft those one-offs rather than silently skipping them."""


# Banned tokens. Word-boundary matched case-insensitively via \b...\b.
# Never substring-match -- that would incorrectly ban names containing
# harmless letter sequences like "aid", "said", "daily", or "aire".
_BANNED_TOKENS = (
    # AI / automation family
    "AI", "automation", "automate", "automated",
    # Generic-tech family
    "tool", "tools", "software", "system", "solution", "platform",
    # Growth-hype family
    "transform", "transformation", "scale", "scaling",
    "growth", "grow", "10x", "explode",
    # Templated-opener family
    "quick question", "following up", "circling back", "touching base",
    # Urgency manipulation
    "urgent", "important",
)


def _validate_subject(subject: str) -> None:
    """Raise BannedSubjectError if the subject hits any anti-pattern.

    Runs as a safety net after every subject build. The current builders
    (Day 0 bare name, Day 4 fixed string, Loom delivery fixed string)
    can't produce most of these tokens, but the Day 0 bare-name builder
    CAN if the business is literally named e.g. "AI Plumbing". Raising
    loudly there matches the paused-vertical sentinel philosophy from v6
    -- surface the case to a human rather than ship compromised copy."""
    if not subject:
        raise BannedSubjectError("Empty subject")

    # Exclamation points -- no exceptions
    if "!" in subject:
        raise BannedSubjectError(f"Subject contains '!': {subject!r}")

    # ALL CAPS words longer than 3 characters. Catches HVAC, HURRY, etc.
    # Day 0 subjects are lowercased so this is purely defensive.
    all_caps = re.findall(r"\b[A-Z]{4,}\b", subject)
    if all_caps:
        raise BannedSubjectError(
            f"Subject contains ALL CAPS word(s) {all_caps}: {subject!r}")

    # Emoji / non-text glyph. Em-dash (U+2014) is BELOW 0x2600 so it
    # passes; curly quotes and ellipsis also pass. 0x2600+ covers the
    # Miscellaneous Symbols block onward (where hearts, stars, check
    # marks, and all emoji proper live).
    for ch in subject:
        if ord(ch) >= 0x2600:
            raise BannedSubjectError(
                f"Subject contains emoji / non-text glyph {ch!r} "
                f"(U+{ord(ch):04X}): {subject!r}")

    # Banned word-boundary tokens
    for token in _BANNED_TOKENS:
        pattern = rf"\b{re.escape(token)}\b"
        if re.search(pattern, subject, re.IGNORECASE):
            raise BannedSubjectError(
                f"Subject contains banned token {token!r}: {subject!r}")

    # Thread-faking prefixes. ':' is a non-word char so \b after it
    # won't match reliably -- handle these with explicit literals. The
    # leading \b ensures we don't flag words that happen to contain
    # "re:" as a substring (none exist in practice, but defensive).
    if re.search(r"\bRe:", subject, re.IGNORECASE):
        raise BannedSubjectError(
            f"Subject uses thread-faking prefix 'Re:': {subject!r}")
    if re.search(r"\bFwd:", subject, re.IGNORECASE):
        raise BannedSubjectError(
            f"Subject uses thread-faking prefix 'Fwd:': {subject!r}")


def _build_subject(prospect: dict, touch_number: int) -> str:
    """Build the subject line for the given touch in the linear follow-up
    sequence (1=Day 0, 2=Day 4). Touches 3+ have no subject -- LinkedIn
    is body-only, Day 14 is an internal SMS. Raises for those to force
    callers to reach for a different helper rather than silently
    generating a default.

    For the Loom delivery email (which fires on a status trigger, not a
    time trigger), use _build_loom_delivery_subject() instead. Wedging
    that into this helper via a synthetic touch_number=4 would confuse
    touch_count semantics."""
    if touch_number == 1:
        subject = _clean_business_name(prospect.get("name") or "").lower()
    elif touch_number == 2:
        subject = "did this get buried?"
    elif touch_number == 3:
        raise ValueError(
            "Touch 3 is LinkedIn -- body only, no subject. "
            "LinkedIn connection requests do not carry a subject field.")
    elif touch_number == 4:
        raise ValueError(
            "Touch 4 is an internal SMS to Franco, not an outbound email. "
            "No subject applies.")
    else:
        raise ValueError(f"No subject defined for touch {touch_number}")
    _validate_subject(subject)
    return subject


def _build_loom_delivery_subject(prospect: dict) -> str:
    """Subject for the email delivering the Loom link after a 'yes' reply.

    Kept separate from _build_subject() because this fires on a status
    trigger (loom_recorded), not on a linear touch-count position. The
    prospect asked for the Loom -- the subject's job is to confirm "the
    thing you asked for is here", matching the tone of how a friend would
    text you a video, not to pattern-interrupt or nudge."""
    subject = "here's that loom"
    _validate_subject(subject)
    return subject


def _extract_first_name(owner: str) -> str:
    """Return first token of owner name, stripping titles like 'Dr.' etc."""
    if not owner:
        return ""
    parts = owner.strip().split()
    # Drop titles
    while parts and parts[0].rstrip(".").lower() in {"dr", "mr", "ms", "mrs", "miss"}:
        parts = parts[1:]
    return parts[0] if parts else ""


def _generate_hook(prospect: dict):
    """Pick a hook tier and return (tier_number, formatted_hook_sentence).

    Reads review and longevity data directly from Supabase columns populated
    by enrichment_agent.py:
      - rating (numeric)
      - review_count (integer)
      - years_in_business (integer)

    Tier 1: rating >= 4.0 AND review_count >= 10
    Tier 2: years_in_business >= 10
    Tier 3: research framing fallback
    """
    vertical = prospect.get("cat", "")
    hooks = _get_vertical_hook_set(vertical)  # raises if paused/unknown

    business_name = _clean_business_name(prospect.get("name", "your business"))

    # Pull review + years data from Supabase columns (v5.2 parsed them out of
    # the notes string; v6 reads them directly).
    rating = prospect.get("rating")
    review_count = prospect.get("review_count")
    years = prospect.get("years_in_business")

    # Tier 1: credible reviews (quality + volume)
    try:
        if rating is not None and review_count is not None:
            if float(rating) >= 4.0 and int(review_count) >= 10:
                return 1, hooks["tier1"].format(
                    rating=f"{float(rating):.1f}",
                    reviews=int(review_count),
                    business_name=business_name,
                )
    except (TypeError, ValueError):
        pass

    # Tier 2: established business (10+ years)
    try:
        if years is not None and int(years) >= 10:
            return 2, hooks["tier2"].format(
                years=int(years),
                business_name=business_name,
            )
    except (TypeError, ValueError):
        pass

    # Tier 3: research framing
    return 3, hooks["tier3"].format(business_name=business_name)


def _build_observation(prospect: dict) -> str:
    """Sentence 3 -- names a concrete manual-work friction.

    Uses manual_work_signal verbatim when present. When empty, falls back
    to a vertical-specific assertion pulled from _OBSERVATION_FALLBACK.
    The fallback is phrased as diagnostic, not hedging -- the prospect
    already scored 3+ on the sourcer's manual-work checklist, so the
    pattern is there; we just don't know which specific tripwire fired.
    """
    signal = (prospect.get("manual_work_signal") or "").strip().rstrip(".")
    if signal:
        return f"One thing I noticed: {signal}."

    vertical = prospect.get("cat", "")
    fallback = _OBSERVATION_FALLBACK.get(vertical)
    if fallback:
        return fallback
    # Defensive default -- paused verticals should have raised upstream.
    return ("Took a look at your site and noticed a few spots where a "
            "manual step adds time across the week.")


def _build_email_body(prospect: dict, tier: int, hook: str):
    """Build the 4-sentence email body. Returns (subject, text, html).

    Structure (4 sentences + greeting + sign-off):
      1. Greeting
      2. Hook
      3. Observation (manual_work_signal)
      4. Loom offer
      sign-off: -- Franco
    """
    business_name = _clean_business_name(prospect.get("name", "your business"))
    owner = (prospect.get("owner_name") or prospect.get("owner") or "").strip()

    first_name = _extract_first_name(owner)

    # Last name -- drop any leading honorific, then take the final token.
    last_name = ""
    if owner:
        parts = owner.split()
        while parts and parts[0].rstrip(".").lower() in {"dr", "mr", "ms", "mrs", "miss"}:
            parts = parts[1:]
        if len(parts) >= 2:
            last_name = parts[-1].rstrip(",.")

    # Credentialed professional rule: if credentials field contains DDS,
    # DMD, MD, DO, or DC (word-boundary match), use "Hi Dr. {last_name},".
    # Fall back to "Hi there," rather than hang a title on a first-name-only
    # record -- "Hi Dr.," with no last name reads broken.
    creds = (prospect.get("credentials") or "").upper()
    is_credentialed = bool(re.search(r"\b(DDS|DMD|MD|DO|DC)\b", creds))

    if is_credentialed and last_name:
        greeting = f"Hi Dr. {last_name},"
        to_name = f"Dr. {last_name}"
    elif first_name and not is_credentialed:
        greeting = f"Hi {first_name},"
        to_name = first_name
    else:
        greeting = "Hi there,"
        to_name = prospect.get("name", "") or ""

    observation = _build_observation(prospect)

    loom_offer = (
        f"I put together a 3-minute Loom showing three specific automations "
        f"I'd build for {business_name} -- want me to send it?"
    )

    signoff = "-- Franco"

    # Plain text version -- em-dash conversion applied at the end so the
    # source lines stay ASCII-safe for Windows cp1252 stdout.
    text = _to_em_dash(
        f"{greeting}\n\n"
        f"{hook}\n\n"
        f"{observation}\n\n"
        f"{loom_offer}\n\n"
        f"{signoff}\n"
    )

    # HTML version (plain paragraphs, no signature block)
    html = _to_em_dash(
        f"<p>{greeting}</p>\n"
        f"<p>{hook}</p>\n"
        f"<p>{observation}</p>\n"
        f"<p>{loom_offer}</p>\n"
        f"<p>{signoff}</p>\n"
    )

    # v6.2: bare lowercase cleaned business name. _build_subject calls
    # _validate_subject internally and raises BannedSubjectError if the
    # business is named something like "AI Plumbing". Caller decides
    # whether to skip + log or surface to Franco.
    subject = _build_subject(prospect, touch_number=1)
    return subject, text, html, to_name


def generate_email(prospect: dict):
    """Build the v6 4-sentence cold email for a prospect. Returns a dict
    or None if email is missing / dead-end / vertical is paused."""
    email = (prospect.get("email") or "").strip()
    if not email or "@" not in email:
        return None
    if _is_dead_end_email(email):
        return None

    try:
        tier, hook = _generate_hook(prospect)
    except PausedVerticalError as e:
        print(f"    SKIP (paused vertical): {e}")
        return None
    except KeyError as e:
        print(f"    SKIP (unknown vertical): {e}")
        return None

    subject, text, html, to_name = _build_email_body(prospect, tier, hook)

    return {
        "to_email": email,
        "to_name": to_name,
        "subject": subject,
        "body_text": text,
        "body_html": html,
        "tier": tier,
        "hook": hook,
        "sentence_count": 4,
        "touch": 1,  # Day 0
        "drafted_at": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# Follow-up Sequence (Day 4 / Day 11 / Day 14)
# =============================================================================

def _days_since(ts_str):
    """Days elapsed since an ISO timestamp. Returns None if unparseable."""
    if not ts_str:
        return None
    try:
        # Supabase returns 'YYYY-MM-DDTHH:MM:SS.ffffff+00:00' or similar
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() / 86400.0
    except (ValueError, AttributeError):
        return None


def _build_day4_email(prospect: dict):
    """Day 4 follow-up: one-liner nudge."""
    business_name = _clean_business_name(prospect.get("name", "your business"))
    owner = (prospect.get("owner_name") or prospect.get("owner") or "").strip()
    first_name = _extract_first_name(owner)
    greeting = f"Hi {first_name}," if first_name else "Hi there,"

    body = "Did my last note get buried? Happy to send that Loom over whenever works."
    signoff = "-- Franco"

    text = _to_em_dash(f"{greeting}\n\n{body}\n\n{signoff}\n")
    html = _to_em_dash(f"<p>{greeting}</p>\n<p>{body}</p>\n<p>{signoff}</p>\n")

    return {
        "to_email": prospect.get("email"),
        "to_name": first_name or prospect.get("name", ""),
        "subject": _build_subject(prospect, touch_number=2),
        "body_text": text,
        "body_html": html,
        "touch": 2,
        "drafted_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_day11_linkedin(prospect: dict):
    """Day 11: short LinkedIn connection request for Franco to send manually."""
    business_name = _clean_business_name(prospect.get("name", "your business"))
    owner = (prospect.get("owner_name") or prospect.get("owner") or "").strip()
    first_name = _extract_first_name(owner)

    note = (
        f"Hi{' ' + first_name if first_name else ''}, "
        f"reaching out because I've been researching independent businesses "
        f"in the GTA and {business_name} caught my eye. "
        f"Would be great to connect."
    )[:300]  # LinkedIn note limit is 300 chars

    return {
        "channel": "linkedin",
        "to_name": first_name or prospect.get("name", ""),
        "note": note,
        "touch": 3,
        "drafted_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_day14_call_sms(prospect: dict):
    """Day 14: SMS Franco directly prompting a phone call."""
    name = prospect.get("name", "unknown")
    phone = prospect.get("phone", "(no phone on file)")
    return f"Unify: Time to call {name}, {phone}"


def run_follow_ups(max_per_day=20, dry_run=False):
    """Sweep sent prospects for Day 4 / 11 / 14 touches."""
    print("=" * 60)
    print("  Unify Cold Email Agent v6 -- FOLLOW-UPS MODE")
    print("=" * 60)

    candidates = get_sent_prospects_awaiting_followup()
    print(f"  {len(candidates)} prospects with touch_count in [1,2,3]")

    gmail = None if dry_run else get_gmail_service()
    drafted_day4 = 0
    drafted_day11 = 0
    sent_day14 = 0

    for p in candidates:
        pid = p.get("id")
        touch = p.get("touch_count") or 0
        days = _days_since(p.get("last_touch_at"))
        if days is None:
            continue

        # Decide which follow-up (if any)
        if touch == 1 and days >= 4.0:
            # Day 4 email
            if drafted_day4 >= max_per_day:
                continue
            email = (p.get("email") or "").strip()
            if not email or _is_dead_end_email(email):
                continue
            payload = _build_day4_email(p)
            print(f"\n  Day 4 follow-up: {p.get('name')} -> {email}")
            if not dry_run:
                if gmail:
                    create_gmail_draft(gmail, payload["to_email"], payload["to_name"],
                                       payload["subject"], payload["body_html"],
                                       payload["body_text"])
                insert_draft_to_queue(pid, payload, action_type="cold_email")
            drafted_day4 += 1
        elif touch == 2 and days >= 11.0:
            # Day 11 LinkedIn
            if drafted_day11 >= max_per_day:
                continue
            payload = _build_day11_linkedin(p)
            print(f"\n  Day 11 LinkedIn note: {p.get('name')}")
            if not dry_run:
                insert_draft_to_queue(pid, payload, action_type="linkedin_note")
            drafted_day11 += 1
        elif touch == 3 and days >= 14.0:
            # Day 14 SMS to Franco
            msg = _build_day14_call_sms(p)
            print(f"\n  Day 14 SMS: {msg}")
            if not dry_run:
                send_sms(msg)
                # Bump touch_count so we don't re-notify every day
                url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
                requests.patch(url, headers=sb_headers(), json={
                    "touch_count": 4,
                    "last_touch_at": datetime.now(timezone.utc).isoformat(),
                }, timeout=15)
            sent_day14 += 1

    print("\n" + "=" * 60)
    print(f"  Day 4 drafts  : {drafted_day4}")
    print(f"  Day 11 notes  : {drafted_day11}")
    print(f"  Day 14 SMS    : {sent_day14}")
    print("=" * 60)

    if drafted_day4 + drafted_day11 + sent_day14 > 0 and not dry_run:
        send_sms(
            f"Unify follow-ups: {drafted_day4} Day-4 drafts, "
            f"{drafted_day11} Day-11 LI notes, {sent_day14} Day-14 call prompts."
        )


# =============================================================================
# Loom Workflow (human-in-the-loop)
# =============================================================================

def _build_loom_script(prospect: dict) -> str:
    """Three bullet points Franco can use as a recording guide. References
    specific enrichment data (rating, review_count, manual_work_signal,
    years_in_business) when available."""
    business_name = _clean_business_name(prospect.get("name", "your business"))
    vertical = prospect.get("cat", "")
    signal = (prospect.get("manual_work_signal") or "").strip()
    rating = prospect.get("rating")
    review_count = prospect.get("review_count")
    years = prospect.get("years_in_business")

    # Bullet 1: point at the specific friction from manual_work_signal
    if signal:
        bullet1 = (
            f'- Open {business_name} website, point at the friction: "Right now '
            f'{signal} -- that\'s typically 5-10 minutes of staff time per request '
            f'that an automation can absorb."'
        )
    else:
        fallback = _OBSERVATION_FALLBACK.get(vertical, "a manual step in your intake")
        bullet1 = (
            f'- Open {business_name} website, point at {fallback}, say: '
            f'"This is the kind of step that adds up across a week -- and it\'s '
            f'exactly the kind of thing I\'d build an automation for."'
        )

    # Bullet 2: reference reviews or years in business for credibility
    if rating is not None and review_count is not None and review_count >= 5:
        bullet2 = (
            f'- Point at Google reviews, say: "I see you have {int(review_count)} '
            f'reviews at {float(rating):.1f} stars -- automating a post-visit '
            f'follow-up that invites 5-star reviews would compound that."'
        )
    elif years is not None and int(years) >= 5:
        bullet2 = (
            f'- Reference longevity: "{int(years)}+ years in the business means '
            f'you already have patterns worth preserving -- the automation '
            f'should work around them, not force new ones."'
        )
    else:
        bullet2 = (
            f'- Reference their independent/owner-operated status: "What I build '
            f'is designed to slot into how you already work -- no new software '
            f'to learn, no hire-to-operate."'
        )

    # Bullet 3: pitch the three automations (vertical-specific)
    if vertical == "Dental & Medical":
        three = ("appointment confirmation and reminder sequence, "
                 "no-show recovery, review request flow")
    elif vertical == "Trades":
        three = ("quote-request auto-response, "
                 "job scheduling SMS sequence, "
                 "review request after job completion")
    else:
        three = ("intake automation, follow-up sequence, review capture")

    bullet3 = (
        f'- Close on: "Three automations I\'d build for you: {three}. '
        f'Happy to walk through any of them on a quick call."'
    )

    return "\n".join([bullet1, bullet2, bullet3])


def run_loom_scripts(dry_run=False):
    """Poll for loom_requested entries, draft 3-bullet Loom scripts,
    SMS Franco when one is ready to record."""
    print("=" * 60)
    print("  Unify Cold Email Agent v6 -- LOOM SCRIPT MODE")
    print("=" * 60)

    entries = get_loom_requested_entries()
    print(f"  {len(entries)} loom_requested entries without scripts")

    drafted = 0
    for entry in entries:
        prospect = entry.get("prospects") or {}
        if not prospect:
            continue
        queue_id = entry.get("id")
        name = prospect.get("name", "unknown")
        print(f"\n  Drafting Loom script for: {name}")

        script = _build_loom_script(prospect)
        print(f"  --- Script preview ---\n{script}\n  ---")

        if dry_run:
            drafted += 1
            continue

        # Merge script into existing payload
        existing = entry.get("payload") or {}
        existing["loom_script"] = script
        existing["loom_script_drafted_at"] = datetime.now(timezone.utc).isoformat()

        if update_queue_payload(queue_id, existing):
            send_sms(f"Unify: Record Loom for {name}. Script in CRM.")
            drafted += 1
        else:
            print(f"    WARNING: could not write script back to queue")

    if drafted == 0:
        print("  Nothing to draft.")
    print("\n" + "=" * 60)
    print(f"  Loom scripts drafted: {drafted}")
    print("=" * 60)


def _build_loom_recorded_followup(prospect: dict, loom_link: str):
    """Follow-up email once Franco records the Loom and pastes the link."""
    business_name = _clean_business_name(prospect.get("name", "your business"))
    owner = (prospect.get("owner_name") or prospect.get("owner") or "").strip()
    first_name = _extract_first_name(owner)
    greeting = f"Hi {first_name}," if first_name else "Hi there,"

    body = f"Here's that Loom -- 3 minutes: {loom_link}"
    callout = ("If anything in it feels worth a deeper conversation, "
               "happy to jump on a quick call.")
    signoff = "-- Franco"

    text = _to_em_dash(f"{greeting}\n\n{body}\n\n{callout}\n\n{signoff}\n")
    html = _to_em_dash(
        f"<p>{greeting}</p>\n"
        f"<p>{body}</p>\n"
        f"<p>{callout}</p>\n"
        f"<p>{signoff}</p>\n"
    )

    return {
        "to_email": prospect.get("email"),
        "to_name": first_name or prospect.get("name", ""),
        "subject": _build_loom_delivery_subject(prospect),
        "body_text": text,
        "body_html": html,
        "loom_link": loom_link,
        "touch": 2,  # This is their second warm touch
        "drafted_at": datetime.now(timezone.utc).isoformat(),
    }


def run_loom_recorded_followups(dry_run=False):
    """For each loom_recorded entry with a loom_link, draft a follow-up
    email that delivers the Loom."""
    print("=" * 60)
    print("  Unify Cold Email Agent v6 -- LOOM RECORDED FOLLOW-UP MODE")
    print("=" * 60)

    entries = get_prospects_for_loom_recorded()
    print(f"  {len(entries)} loom_recorded entries with loom_link set")

    gmail = None if dry_run else get_gmail_service()
    drafted = 0
    for entry in entries:
        prospect = entry.get("prospects") or {}
        loom_link = prospect.get("loom_link")
        pid = prospect.get("id")
        if not loom_link or not pid:
            continue

        payload = _build_loom_recorded_followup(prospect, loom_link)
        print(f"\n  Drafting Loom delivery: {prospect.get('name')} -> {payload['to_email']}")
        if dry_run:
            drafted += 1
            continue

        if gmail:
            create_gmail_draft(gmail, payload["to_email"], payload["to_name"],
                               payload["subject"], payload["body_html"],
                               payload["body_text"])
        # Insert as new queue entry so the approval flow is identical to Day-0
        insert_draft_to_queue(pid, payload, action_type="loom_delivery")
        # Flip the original loom_recorded entry so we don't re-draft
        update_queue_status(entry.get("id"), "loom_delivered")
        drafted += 1

    print("\n" + "=" * 60)
    print(f"  Loom delivery drafts: {drafted}")
    print("=" * 60)


# =============================================================================
# Draft Mode (Day 0)
# =============================================================================

def _clear_cold_email_queue():
    """Delete all cold_email entries from agent_queue (for --redraft)."""
    url = f"{SUPABASE_URL}/rest/v1/agent_queue?action_type=eq.cold_email"
    r = requests.delete(url, headers=sb_headers(), timeout=15)
    if r.status_code in (200, 204):
        print("  Cleared old cold_email entries from agent_queue")
        return True
    print(f"  WARNING: Could not clear agent_queue ({r.status_code})")
    return False


def _draft_one(prospect, gmail, dry_run):
    """Draft a single cold email for a prospect.

    Returns (outcome, tier). Outcome is one of:
      'ok'              -- draft created
      'skip'            -- generate_email returned None (missing email,
                           dead-end domain, paused vertical, etc.)
      'banned_subject'  -- business name contains a v6.2 banned token;
                           surfaced in the SMS summary so Franco can
                           hand-craft the subject line manually.
    """
    pid = prospect.get("id", "unknown")
    name = prospect.get("name", "Unknown")
    email = prospect.get("email", "")
    owner = (prospect.get("owner_name") or prospect.get("owner") or "").strip() or "(no name)"

    print(f"\n  Drafting for: {name} ({owner}) -> {email}")

    try:
        email_data = generate_email(prospect)
    except BannedSubjectError as e:
        print(f"    SKIP (banned subject): {name} ({pid}) -- {e}")
        return "banned_subject", None

    if not email_data:
        return "skip", None

    # Acceptance sanity check: 4 sentences in body
    # (greeting + hook + observation + loom_offer, sign-off is sentence-level
    # too but conventionally excluded -- count the hook/observation/loom body)
    body_sentences = [ln for ln in email_data["body_text"].split("\n") if ln.strip()]
    # greeting, hook, observation, loom_offer, signoff = 5 non-empty lines
    if len(body_sentences) != 5:
        print(f"    Warning: email has {len(body_sentences)} lines, expected 5")

    if dry_run:
        print(f"    [DRY RUN] Subject: {email_data['subject']}")
        print(f"    [DRY RUN] Tier: {email_data['tier']}")
        print(f"    [DRY RUN] Body:\n{'-'*60}\n{email_data['body_text']}{'-'*60}")
        return "ok", email_data["tier"]

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

    insert_draft_to_queue(pid, email_data, action_type="cold_email")

    # Move status NOT CONTACTED -> PHONE CALL READY (same as v5.2)
    patch_url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{pid}"
    requests.patch(patch_url, headers=sb_headers(), json={
        "status": "PHONE CALL READY",
        "action": "Email drafted -- review in Gmail, then send or call",
    }, timeout=15)

    print(f"    Tier {email_data['tier']} -- saved to "
          f"{'Gmail+queue' if gmail_ok else 'queue only'}")
    return "ok", email_data["tier"]


def run_draft(max_drafts=50, dry_run=False, redraft=False):
    """Day-0 draft mode, priority-ordered: high first, medium second, low skipped."""
    mode_label = "REDRAFT MODE" if redraft else "DRAFT MODE"
    print("=" * 60)
    print(f"  Unify Cold Email Agent v6 -- {mode_label}")
    print("=" * 60)
    print(f"  Max drafts  : {max_drafts}")
    print(f"  Dry run     : {dry_run}")
    print(f"  Supabase    : {'Connected' if SUPABASE_KEY else 'No key'}")
    print()

    if redraft and not dry_run:
        reset_url = f"{SUPABASE_URL}/rest/v1/prospects?status=eq.PHONE CALL READY"
        r = requests.patch(reset_url, headers=sb_headers(return_representation=True),
                           json={"status": "NOT CONTACTED", "action": None},
                           timeout=15)
        if r.status_code in (200, 204):
            reset_count = len(r.json()) if r.text.strip() else 0
            print(f"  Reset {reset_count} prospects PHONE CALL READY -> NOT CONTACTED")
        _clear_cold_email_queue()

    gmail = None
    if not dry_run:
        gmail = get_gmail_service()
        print(f"  Gmail       : {'Connected' if gmail else 'NOT connected'}")

    existing_ids = set() if redraft else get_existing_queue_ids(("cold_email",))

    counts = {"high": 0, "medium": 0, "tier1": 0, "tier2": 0, "tier3": 0}
    drafted = 0
    banned_subject_skips = 0  # v6.2: businesses whose name tripped the validator

    # Priority-ordered: high first, medium second, low skipped
    for priority in ("high", "medium"):
        if drafted >= max_drafts:
            break
        prospects = get_prospects_by_priority(priority, redraft=redraft)
        prospects = [p for p in prospects if p.get("id") not in existing_ids]
        print(f"\n  --- {priority.upper()} priority: {len(prospects)} prospects "
              f"ready ---")

        for prospect in prospects:
            if drafted >= max_drafts:
                print(f"\n  DAILY CAP REACHED: {drafted} drafts")
                break
            outcome, tier = _draft_one(prospect, gmail, dry_run)
            if outcome == "banned_subject":
                banned_subject_skips += 1
                continue
            if outcome != "ok":
                continue
            drafted += 1
            counts[priority] += 1
            if tier in (1, 2, 3):
                counts[f"tier{tier}"] += 1

    # Low priority is skipped entirely per spec
    low = get_prospects_by_priority("low", redraft=redraft)
    if low:
        print(f"\n  --- LOW priority: {len(low)} prospects SKIPPED (per v6 spec) ---")

    print("\n" + "=" * 60)
    print(f"  Drafts total         : {drafted}")
    print(f"  High-priority drafts : {counts['high']}")
    print(f"  Medium drafts        : {counts['medium']}")
    print(f"  Tier distribution    : T1={counts['tier1']} "
          f"T2={counts['tier2']} T3={counts['tier3']}")
    if banned_subject_skips:
        print(f"  Banned-subject skips : {banned_subject_skips} "
              f"(see logs; subject needs manual handling)")
    print("=" * 60)

    if drafted > 0:
        msg = (
            f"Unify: {drafted} cold email drafts ready "
            f"({counts['high']} high, {counts['medium']} medium). "
            f"Review at {CRM_URL}"
        )
    else:
        msg = f"Unify: 0 new drafts this run. All priority prospects already drafted."

    if banned_subject_skips:
        msg += (f" {banned_subject_skips} prospect(s) skipped due to "
                f"banned-token in business name (see logs).")

    if not dry_run:
        send_sms(msg)
    else:
        print(f"  [DRY RUN] SMS: {msg}")
    print("\n  Done.")


# =============================================================================
# Send Mode
# =============================================================================

def run_send(dry_run=False):
    """Send approved emails via Resend. Uses mark_sent() so the DB trigger
    bumps touch_count + last_touch_at atomically."""
    print("=" * 60)
    print("  Unify Cold Email Agent v6 -- SEND MODE")
    print("=" * 60)

    if not RESEND_API_KEY and not dry_run:
        print("  ERROR: RESEND_API_KEY not configured.")
        send_sms("Unify Email Agent ERROR: RESEND_API_KEY not set.")
        return

    approved = get_approved_emails()
    print(f"  {len(approved)} approved emails to send")
    if not approved:
        send_sms("Unify: 0 approved emails to send. Approve drafts first.")
        return

    sent = 0
    failed = 0
    for item in approved:
        payload = item.get("payload", {})
        queue_id = item.get("id")
        prospect_id = item.get("prospect_id")
        to_email = payload.get("to_email", "")

        print(f"\n  Sending -> {to_email}")
        if dry_run:
            sent += 1
            continue
        ok = send_email_via_resend(
            to_email, payload.get("to_name", ""),
            payload.get("subject", ""),
            payload.get("body_html", ""),
            payload.get("body_text", ""),
        )
        if ok:
            mark_sent(queue_id, prospect_id)
            sent += 1
        else:
            failed += 1
        time.sleep(2)

    if not dry_run:
        send_sms(f"Unify: {sent} cold emails sent, {failed} failed.")
    print(f"\n  Done. {sent} sent, {failed} failed.")


# =============================================================================
# Self-Test
# =============================================================================
# Fast, dependency-free assertions on the tricky template edge cases that
# have bitten this codebase (abbreviation handling, cp1252 encoding, greeting
# rules). Run with: python cold_email_agent.py --self-test
# The suite MUST pass before any PR that touches templating is merged --
# see memory.md Lessons 2026-04-14 (v5.1 "Inc." double-period incident) and
# 2026-04-16 (v6 conditional-rstrip).

def _self_test():
    print("  [self-test] business name cleanup")
    assert _clean_business_name("Joe's Pizza Inc.") == "Joe's Pizza Inc"
    assert _clean_business_name("Acme Corp.") == "Acme Corp"
    assert _clean_business_name("Smith LLC.") == "Smith LLC"
    # Co. / Ltd. / Co-op / trailing periods that ARE part of the name stay
    assert _clean_business_name("Oakridge Smile Co.") == "Oakridge Smile Co."
    assert _clean_business_name("Hartman Dental Ltd.") == "Hartman Dental Ltd."
    assert _clean_business_name("Ottawa Dental Co-op") == "Ottawa Dental Co-op"
    assert _clean_business_name("Plain Name") == "Plain Name"
    assert _clean_business_name("") == ""

    print("  [self-test] em-dash renderer")
    assert "\u2014" in _to_em_dash("Foo -- Bar")
    assert "\u2014 Franco" in _to_em_dash("-- Franco")
    # Leave source '--' inside words or without surrounding spaces alone
    assert _to_em_dash("call-for-quote") == "call-for-quote"
    # Idempotent -- running twice shouldn't double-convert
    assert _to_em_dash(_to_em_dash("Foo -- Bar")) == _to_em_dash("Foo -- Bar")

    print("  [self-test] greeting rules (credentialed professionals)")
    # Credentialed with last name -> Dr. Lastname
    p = {"name":"Test","cat":"Dental & Medical","email":"x@y.com",
         "owner_name":"Dr. Priya Patel","credentials":"DDS MSc"}
    e = generate_email(p)
    assert e["body_text"].startswith("Hi Dr. Patel,"), \
        f"Expected Dr. Patel greeting, got: {e['body_text'][:40]!r}"
    # Credentialed with DMD
    p["credentials"] = "DMD"; p["owner_name"] = "Dr. Alan Chen"
    e = generate_email(p)
    assert e["body_text"].startswith("Hi Dr. Chen,")
    # Credentialed with only first name -> Hi there (never hang the title)
    p["owner_name"] = "Dr. Mike"; p["credentials"] = "DDS"
    e = generate_email(p)
    assert e["body_text"].startswith("Hi there,"), \
        f"Hanging-title fallback expected, got: {e['body_text'][:40]!r}"
    # Non-credentialed with first + last -> first name only
    p = {"name":"Test","cat":"Trades","email":"x@y.com",
         "owner_name":"Matt Rossi"}
    e = generate_email(p)
    assert e["body_text"].startswith("Hi Matt,")
    # Non-credentialed, first name only
    p["owner_name"] = "Sam"
    e = generate_email(p)
    assert e["body_text"].startswith("Hi Sam,")
    # No owner -> Hi there
    p["owner_name"] = None
    e = generate_email(p)
    assert e["body_text"].startswith("Hi there,")

    print("  [self-test] paused vertical raises via generate_email None")
    assert generate_email({"name":"Pizza","cat":"Restaurants",
                           "email":"x@y.com"}) is None

    print("  [self-test] em-dash appears in rendered body")
    p = {"name":"Peel Plumbing Inc.","cat":"Trades","email":"x@y.com",
         "owner_name":"Matt Rossi","rating":4.9,"review_count":126}
    e = generate_email(p)
    assert "\u2014" in e["body_text"], "rendered body should use em-dash"
    assert "\u2014" in e["body_html"]
    # And sign-off should be em-dash too
    assert e["body_text"].rstrip().endswith("\u2014 Franco")

    print("  [self-test] fallback observation is diagnostic, not hedging")
    p = {"name":"Test","cat":"Dental & Medical","email":"x@y.com",
         "manual_work_signal":""}
    e = generate_email(p)
    assert "Took a look at your site" in e["body_text"]
    assert "I poked around" not in e["body_text"]
    assert "looks like it still" not in e["body_text"]

    # v6.2: subject line generation ------------------------------------------
    print("  [self-test] v6.2 Day-0 subject is bare lowercase business name")
    # Inc. -- period stripped
    s = _build_subject({"name": "Peel Plumbing Inc."}, 1)
    assert s == "peel plumbing inc", f"Expected 'peel plumbing inc', got {s!r}"
    # Co. -- period preserved
    s = _build_subject({"name": "Oakridge Smile Co."}, 1)
    assert s == "oakridge smile co.", f"Expected 'oakridge smile co.', got {s!r}"
    # Ltd. -- period preserved
    s = _build_subject({"name": "Hartman Dental Ltd."}, 1)
    assert s == "hartman dental ltd.", f"Expected 'hartman dental ltd.', got {s!r}"
    # LLC. -- period stripped
    s = _build_subject({"name": "Smith LLC."}, 1)
    assert s == "smith llc", f"Expected 'smith llc', got {s!r}"
    # Plain name
    s = _build_subject({"name": "Aire One Heating & Cooling"}, 1)
    assert s == "aire one heating & cooling"

    print("  [self-test] v6.2 Day-4 subject is the fixed nudge")
    s = _build_subject({"name": "Anything"}, 2)
    assert s == "did this get buried?"

    print("  [self-test] v6.2 touch>=3 raises (LinkedIn and SMS have no subject)")
    for t in (3, 4, 5):
        try:
            _build_subject({"name": "X"}, t)
            raise AssertionError(f"Expected ValueError for touch {t}")
        except ValueError:
            pass  # expected

    print("  [self-test] v6.2 Loom delivery subject is dedicated path")
    assert _build_loom_delivery_subject({"name": "X"}) == "here's that loom"

    print("  [self-test] v6.2 validator uses word boundaries, not substring")
    # 'aid', 'said', 'daily', 'aire', 'main' must all pass -- they contain
    # the letter pair 'ai' but are not the banned word 'AI'.
    _validate_subject("aid station")
    _validate_subject("said and done")
    _validate_subject("daily grind")
    _validate_subject("aire one heating")   # 'aire' contains 'ai' as prefix
    _validate_subject("main street dental")  # 'main' contains 'ai'
    # But standalone AI (case-insensitive) must be caught
    for banned in ("AI plumbing", "ai plumbing", "the AI shop", "automation station",
                   "grow your business", "quick question about X",
                   "following up tomorrow", "touching base", "re: something",
                   "Re: something", "Fwd: heads up", "urgent update",
                   "GREAT deal", "hello!"):
        try:
            _validate_subject(banned)
            raise AssertionError(f"Expected banned: {banned!r}")
        except BannedSubjectError:
            pass  # expected

    print("  [self-test] v6.2 validator catches emoji / non-text glyphs")
    # Em-dash (U+2014) and curly quotes must PASS -- they're valid typography
    _validate_subject("something \u2014 else")  # em-dash
    _validate_subject("it\u2019s fine")         # right single quote
    # But emoji must be caught
    for emoji in ("hi \U0001F389", "heart \u2764", "check \u2705", "star \u2605"):
        try:
            _validate_subject(emoji)
            raise AssertionError(f"Expected emoji rejection: {emoji!r}")
        except BannedSubjectError:
            pass

    print("  [self-test] v6.2 end-to-end: generate_email Day-0 subject is bare")
    p = {"name":"Peel Plumbing Inc.","cat":"Trades","email":"x@y.com",
         "owner_name":"Matt Rossi","rating":4.9,"review_count":126}
    e = generate_email(p)
    assert e["subject"] == "peel plumbing inc", \
        f"Day-0 subject from generate_email should be bare lowercase, got {e['subject']!r}"

    print("  [self-test] v6.2 end-to-end: Day-4 email carries 'did this get buried?'")
    d4 = _build_day4_email(p)
    assert d4["subject"] == "did this get buried?"

    print("  [self-test] v6.2 end-to-end: Loom delivery carries 'here's that loom'")
    lrf = _build_loom_recorded_followup(p, "https://loom.example/abc")
    assert lrf["subject"] == "here's that loom"

    print("  [self-test] v6.2 business names that ARE banned words raise")
    # Real-world edge case: business literally called "AI Plumbing"
    try:
        _build_subject({"name": "AI Plumbing"}, 1)
        raise AssertionError("Expected BannedSubjectError for 'AI Plumbing'")
    except BannedSubjectError:
        pass  # expected

    print("  [self-test] ALL TESTS PASSED")


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Unify Cold Email Agent v6 (Loom Pivot)")
    parser.add_argument("--draft", action="store_true",
                        help="Draft Day-0 emails, priority-ordered")
    parser.add_argument("--redraft", action="store_true",
                        help="Clear queue + redraft everything")
    parser.add_argument("--follow-ups", action="store_true", dest="follow_ups",
                        help="Day 4/11/14 follow-up sweep")
    parser.add_argument("--loom-script", action="store_true", dest="loom_script",
                        help="Draft 3-bullet Loom scripts for loom_requested entries")
    parser.add_argument("--loom-recorded", action="store_true", dest="loom_recorded",
                        help="Draft follow-up emails for loom_recorded entries")
    parser.add_argument("--send", action="store_true",
                        help="Send approved emails via Resend")
    parser.add_argument("--mark-sent", type=str, dest="mark_sent",
                        help="Manually flip a queue entry to 'sent' by id")
    parser.add_argument("--self-test", action="store_true", dest="self_test",
                        help="Run tone/template assertions and exit")
    parser.add_argument("--max", "-m", type=int, default=50,
                        help="Max items per run (default: 50)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Preview without writing")
    args = parser.parse_args()

    if args.self_test:
        _self_test()
        return

    if args.mark_sent:
        ok = mark_sent(args.mark_sent)
        print(f"  mark_sent({args.mark_sent}): {'OK' if ok else 'FAILED'}")
        return

    modes = [args.draft, args.redraft, args.follow_ups, args.loom_script,
             args.loom_recorded, args.send]
    if not any(modes):
        print("Error: must specify --draft / --redraft / --follow-ups / "
              "--loom-script / --loom-recorded / --send / --mark-sent / "
              "--self-test")
        parser.print_help()
        sys.exit(1)

    if args.redraft:
        run_draft(max_drafts=args.max, dry_run=args.dry_run, redraft=True)
    elif args.draft:
        run_draft(max_drafts=args.max, dry_run=args.dry_run)
    elif args.follow_ups:
        run_follow_ups(max_per_day=args.max, dry_run=args.dry_run)
    elif args.loom_script:
        run_loom_scripts(dry_run=args.dry_run)
    elif args.loom_recorded:
        run_loom_recorded_followups(dry_run=args.dry_run)
    elif args.send:
        run_send(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
