#!/usr/bin/env python3
"""
Unify Enrichment Agent v2

Three passes per prospect:
  Pass 1: homepage -> email, phone, owner hint, internal links
  Pass 2: About Us / Our Team / Meet-the-Doctor page -> owner name
          (regex + Haiku fallback), years in business, 500-char snippet,
          dental/medical credentials
  Pass 3: Google Places API (New) -> rating, review count, hours,
          operational status, phone

Parallelism: ThreadPoolExecutor(max_workers=4). 5-10 s randomized sleep
between prospect completions so ubuntu-latest runners don't look like bots.

Circuit breaker: 3 consecutive Pass 2 failures of the same kind
(timeout / 403 / 429) pauses Pass 2 for the rest of the run. Passes 1 and 3
keep going.

Required env vars:
  SUPABASE_URL, SUPABASE_KEY
  TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, FRANCO_PHONE
  GOOGLE_PLACES_API_KEY, ANTHROPIC_API_KEY
"""

import argparse
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup


# =============================================================================
# ENV / CONSTANTS
# =============================================================================

def load_env(path=".env"):
    """Local dev helper: load .env if present. Non-empty env vars take priority,
    but empty-string env vars don't shadow the .env value."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if not os.environ.get(k):
                os.environ[k] = v


load_env()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
TWILIO_SID = os.getenv("TWILIO_SID", "")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN", "")
TWILIO_FROM = os.getenv("TWILIO_FROM", "")
FRANCO_PHONE = os.getenv("FRANCO_PHONE", "")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
REQ_HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en-CA,en;q=0.9"}
REQ_TIMEOUT = 10

ABOUT_URL_PATTERNS = [
    "/about", "/about-us", "/about_us", "/our-team", "/our_team", "/team",
    "/meet-the-doctor", "/meet-the-team", "/meet-the-owner", "/staff",
    "/who-we-are", "/our-story", "/the-team", "/providers", "/doctors",
]
ABOUT_NAV_KEYWORDS = ["about", "team", "doctor", "owner", "staff", "story"]

HAIKU_MODEL = "claude-haiku-4-5-20251001"

DENTAL_MEDICAL_VERTICALS = {"Dental & Medical", "dental-medical", "dental"}

DATA_FIELDS = {
    "email", "phone", "owner", "owner_name",
    "about_us_content", "years_in_business", "credentials",
    "rating", "review_count", "hours", "is_operational",
    "manual_work_score", "manual_work_signal", "priority",
}


# =============================================================================
# DEAD-END EMAIL FILTER (duplicated from cold_email_agent.py by design --
# keeps both agents independent; 30 lines is cheaper than a shared module)
# =============================================================================

DEAD_END_EMAILS = ["noreply@", "no-reply@", "donotreply@", "do-not-reply@"]

DEAD_END_DOMAINS = {
    "canpages.ca", "foodpages.ca", "yellowpages.ca", "yp.ca", "411.ca",
    "findopen.ca", "findopenhours.com", "cylex-canada.ca",
    "yelp.com", "yelp.ca", "bbb.org",
    "facebook.com", "instagram.com", "twitter.com", "x.com", "linkedin.com",
    "google.com",
}


def _is_dead_end_email(email):
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return False
    if any(e.startswith(p) for p in DEAD_END_EMAILS):
        return True
    domain = e.split("@", 1)[1]
    if any(domain == d or domain.endswith("." + d) for d in DEAD_END_DOMAINS):
        return True
    return False


# =============================================================================
# SUPABASE
# =============================================================================

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def get_prospects_to_enrich(max_prospects=100, prospect_id=None):
    """
    Fetch prospects where enriched_at is NULL or older than 30 days.
    If prospect_id is provided, fetch only that row.
    """
    if prospect_id:
        url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{quote(prospect_id)}"
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        url = (
            f"{SUPABASE_URL}/rest/v1/prospects"
            f"?or=(enriched_at.is.null,enriched_at.lt.{quote(cutoff)})"
            f"&order=created_at.asc"
            f"&limit={max_prospects}"
        )
    resp = requests.get(url, headers=sb_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def update_prospect_enrichment(prospect_id, fields):
    url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{quote(prospect_id)}"
    resp = requests.patch(url, headers=sb_headers(), json=fields, timeout=30)
    resp.raise_for_status()


# =============================================================================
# TWILIO SMS
# =============================================================================

def send_sms(body):
    if not (TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM and FRANCO_PHONE):
        print("SMS skipped: missing Twilio env vars")
        return
    try:
        requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json",
            auth=(TWILIO_SID, TWILIO_TOKEN),
            data={"From": TWILIO_FROM, "To": FRANCO_PHONE, "Body": body[:1600]},
            timeout=15,
        )
    except Exception as e:
        print(f"SMS send failed: {e}")


# =============================================================================
# PASS 1 -- HOMEPAGE FETCH
# =============================================================================

_HOMEPAGE_OWNER_RE = re.compile(
    r"(?:Owned by|Founded by|Owner[:\s]+|Founder[:\s]+|Dr\.\s+)"
    r"([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)"
)


def _classify_http_error(status):
    if status == 403:
        return "403"
    if status == 429:
        return "429"
    return "other"


def fetch_homepage(url):
    """
    Returns dict with: email, phone, owner_hint, internal_links, html, error_kind.
    error_kind is None on success, else 'timeout'/'403'/'429'/'other'.
    """
    result = {
        "email": None,
        "phone": None,
        "owner_hint": None,
        "internal_links": [],
        "html": None,
        "error_kind": None,
    }
    if not url:
        result["error_kind"] = "other"
        return result
    try:
        resp = requests.get(url, headers=REQ_HEADERS, timeout=REQ_TIMEOUT, allow_redirects=True)
        if resp.status_code >= 400:
            result["error_kind"] = _classify_http_error(resp.status_code)
            return result
    except requests.Timeout:
        result["error_kind"] = "timeout"
        return result
    except Exception:
        result["error_kind"] = "other"
        return result

    html = resp.text or ""
    result["html"] = html
    soup = BeautifulSoup(html, "lxml")

    # Email: mailto first, then regex scan
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().startswith("mailto:"):
            candidate = href[7:].split("?")[0].strip()
            if candidate and not _is_dead_end_email(candidate):
                result["email"] = candidate
                break
    if not result["email"]:
        text = soup.get_text(" ", strip=True)
        for m in re.finditer(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text):
            cand = m.group(0)
            if not _is_dead_end_email(cand):
                result["email"] = cand
                break

    # Phone: tel: links
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().startswith("tel:"):
            phone = re.sub(r"[^\d+]", "", href[4:])
            if len(phone) >= 10:
                result["phone"] = phone
                break

    # Owner hint from header/footer only
    for tag in soup.find_all(["header", "footer"]):
        zone = tag.get_text(" ", strip=True)
        m = _HOMEPAGE_OWNER_RE.search(zone)
        if m:
            result["owner_hint"] = m.group(1).strip()
            break

    # Internal links for Pass 2
    base_netloc = urlparse(url).netloc.lower()
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.lower().startswith(("mailto:", "tel:")):
            continue
        full = urljoin(url, href)
        parsed = urlparse(full)
        if not parsed.scheme.startswith("http"):
            continue
        if parsed.netloc and parsed.netloc.lower() != base_netloc:
            continue
        if full in seen:
            continue
        seen.add(full)
        result["internal_links"].append(full)

    return result


# =============================================================================
# PASS 2 -- ABOUT US CRAWL + EXTRACTORS
# =============================================================================

def find_about_url(homepage_url, internal_links, homepage_html):
    """First URL-pattern match wins; otherwise scan nav text."""
    for link in internal_links:
        path = urlparse(link).path.rstrip("/").lower() or "/"
        for pattern in ABOUT_URL_PATTERNS:
            if path == pattern or path.endswith(pattern):
                return link
    if homepage_html:
        soup = BeautifulSoup(homepage_html, "lxml")
        nav = soup.find("nav") or soup
        for a in nav.find_all("a", href=True):
            text = a.get_text(" ", strip=True).lower()
            if not text:
                continue
            if any(kw in text for kw in ABOUT_NAV_KEYWORDS):
                full = urljoin(homepage_url, a["href"].strip())
                if urlparse(full).scheme.startswith("http"):
                    return full
    return None


def fetch_about_page(url):
    """Returns dict: text, error_kind."""
    result = {"text": None, "error_kind": None}
    try:
        resp = requests.get(url, headers=REQ_HEADERS, timeout=REQ_TIMEOUT, allow_redirects=True)
        if resp.status_code >= 400:
            result["error_kind"] = _classify_http_error(resp.status_code)
            return result
    except requests.Timeout:
        result["error_kind"] = "timeout"
        return result
    except Exception:
        result["error_kind"] = "other"
        return result

    soup = BeautifulSoup(resp.text or "", "lxml")
    for tag in soup(["nav", "footer", "script", "style", "header", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    result["text"] = re.sub(r"\s+", " ", text).strip()
    return result


_OWNER_REGEXES = [
    re.compile(r"Dr\.\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)"),
    re.compile(r"(?:Owner|Founder|Founded by|Owned by)[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)"),
    re.compile(r"(?:I'm|I am|My name is)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"),
    re.compile(r"([A-Z][a-z]+\s+[A-Z][a-z]+),?\s+(?:DDS|DMD|MD|owner|founder|principal)"),
]

_YEARS_SINCE_RE = re.compile(
    r"(?:since|founded in|established in|serving [A-Za-z ]{1,40} since)\s+(\d{4})",
    re.I,
)
_YEARS_OVER_RE = re.compile(r"(?:over|more than)\s+(\d+)\s+years", re.I)

_CREDENTIAL_RES = [
    re.compile(r"\bDDS\b"),
    re.compile(r"\bDMD\b"),
    re.compile(r"Specialist in [A-Z][A-Za-z ]{2,40}", re.I),
    re.compile(r"accredited by [A-Z][A-Za-z &]{2,60}", re.I),
]


def extract_owner_name(text):
    """Try 4 regexes in order; on miss, one Claude Haiku call."""
    if not text:
        return None
    for rx in _OWNER_REGEXES:
        m = rx.search(text)
        if m:
            return m.group(1).strip()
    return _haiku_extract_owner(text)


def _haiku_extract_owner(text):
    if not ANTHROPIC_API_KEY:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        msg = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=40,
            messages=[{
                "role": "user",
                "content": (
                    "Extract the owner or principal's full name from this text. "
                    "Return only the name, or 'UNKNOWN' if not clearly identifiable.\n\n"
                    + text[:4000]
                ),
            }],
        )
        answer = "".join(
            getattr(block, "text", "") for block in msg.content
            if getattr(block, "type", None) == "text"
        ).strip()
        if not answer or answer.upper() == "UNKNOWN":
            return None
        if len(answer) > 80 or "\n" in answer:
            return None
        return answer
    except Exception as e:
        print(f"Haiku owner-extract failed: {e}")
        return None


def extract_years_in_business(text):
    if not text:
        return None
    current_year = datetime.now().year
    best = None
    m1 = _YEARS_SINCE_RE.search(text)
    if m1:
        try:
            yr = int(m1.group(1))
            if 1800 < yr <= current_year:
                best = current_year - yr
        except ValueError:
            pass
    m2 = _YEARS_OVER_RE.search(text)
    if m2:
        try:
            n = int(m2.group(1))
            if 0 < n < 200 and (best is None or n > best):
                best = n
        except ValueError:
            pass
    return best


def extract_about_snippet(text):
    return text[:500] if text else None


def extract_credentials(text, vertical):
    if not text or vertical not in DENTAL_MEDICAL_VERTICALS:
        return None
    hits = []
    for rx in _CREDENTIAL_RES:
        for m in rx.finditer(text):
            val = m.group(0).strip()
            if val and val not in hits:
                hits.append(val)
    return "; ".join(hits[:5]) if hits else None


# =============================================================================
# PASS 3 -- GOOGLE PLACES API (NEW)
# =============================================================================

def places_lookup(business_name):
    """
    Returns dict: rating, review_count, hours, is_operational, phone, review_texts.
    All None if Places has no match. Raises RuntimeError only if API key missing.
    review_texts is a list of review text strings (up to 5).
    """
    result = {
        "rating": None, "review_count": None, "hours": None,
        "is_operational": None, "phone": None, "review_texts": [],
    }
    if not GOOGLE_PLACES_API_KEY:
        raise RuntimeError("GOOGLE_PLACES_API_KEY is required")
    if not business_name:
        return result

    # Text Search -> place_id
    try:
        resp = requests.post(
            "https://places.googleapis.com/v1/places:searchText",
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": "places.id",
            },
            json={"textQuery": f"{business_name} Ontario Canada"},
            timeout=15,
        )
        if resp.status_code >= 400:
            return result
        data = resp.json() or {}
    except Exception as e:
        print(f"Places text search failed for '{business_name}': {e}")
        return result

    places = data.get("places") or []
    if not places:
        return result
    place_id = places[0].get("id")
    if not place_id:
        return result

    # Place Details
    try:
        resp = requests.get(
            f"https://places.googleapis.com/v1/places/{place_id}",
            headers={
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": (
                    "rating,userRatingCount,regularOpeningHours,"
                    "businessStatus,nationalPhoneNumber,reviews.text"
                ),
            },
            timeout=15,
        )
        if resp.status_code >= 400:
            return result
        details = resp.json() or {}
    except Exception as e:
        print(f"Places details failed for '{business_name}': {e}")
        return result

    if "rating" in details:
        try:
            result["rating"] = round(float(details["rating"]), 2)
        except (TypeError, ValueError):
            pass
    if "userRatingCount" in details:
        try:
            result["review_count"] = int(details["userRatingCount"])
        except (TypeError, ValueError):
            pass
    if details.get("regularOpeningHours"):
        result["hours"] = details["regularOpeningHours"]
    if "businessStatus" in details:
        result["is_operational"] = (details["businessStatus"] == "OPERATIONAL")
    raw_phone = details.get("nationalPhoneNumber")
    if raw_phone:
        phone = re.sub(r"[^\d+]", "", raw_phone)
        if len(phone) >= 10:
            result["phone"] = phone

    # Extract review texts (up to 5)
    for rev in details.get("reviews", []):
        text_obj = rev.get("text", {})
        text = text_obj.get("text", "") if isinstance(text_obj, dict) else str(text_obj)
        if text:
            result["review_texts"].append(text)

    return result


# =============================================================================
# MANUAL-WORK SCORING (duplicated from lead_sourcer.py by design --
# keeps agents independent per the "one clean script" rule)
# =============================================================================

_DENTAL_BOOKING_KEYWORDS = [
    "book online", "schedule online", "book appointment", "online booking",
    "book now", "schedule now", "request appointment", "book your appointment",
    "schedule your", "online scheduling",
]

_LIVE_CHAT_SCRIPTS = [
    "intercom", "drift", "tidio", "livechat", "hubspot",
    "zendesk", "tawk", "crisp", "olark", "freshchat",
]

_OWNER_DENTIST_PATTERNS = [
    r"dr\.\s+\w+",
    r"owner[\s-]operator",
    r"locally\s+owned",
    r"family\s+practice",
    r"family[\s-]owned",
    r"our\s+dentist",
    r"your\s+dentist",
    r"meet\s+the\s+doctor",
    r"meet\s+dr\.",
]

_CALL_FOR_QUOTE_KEYWORDS = [
    "call for quote", "call for estimate", "call for a free",
    "call today for", "call us for", "call now for",
    "phone for a quote", "give us a call",
]

_OWNER_OPERATOR_KEYWORDS = [
    "family-owned", "family owned", "owner-operated", "owner operated",
    "locally owned", "locally-owned", "i've been serving",
    "my team and i", "our family business", "family run",
    "family-run", "we are a family",
]

_SLOW_RESPONSE_KEYWORDS = [
    "hard to reach", "couldn't get through", "could not get through",
    "no answer", "called several times", "never answered",
    "slow to respond", "didn't return my call", "didn't call back",
    "hard to get a hold", "hard to contact", "unreachable",
    "left multiple messages", "never got back",
]


def compute_manual_work_score(vertical, html_text, homepage_fetched, review_texts):
    """
    Compute a 0-10 manual work score based on homepage signals + review texts.
    Returns (score, priority, signal_string).
    """
    score = 0
    signals = []  # (points, description)

    html_lower = html_text.lower() if html_text else ""

    if vertical in DENTAL_MEDICAL_VERTICALS or vertical == "Dental & Medical":
        # No online booking link (+3)
        has_booking = any(kw in html_lower for kw in _DENTAL_BOOKING_KEYWORDS)
        if not has_booking and homepage_fetched:
            score += 3
            signals.append((3, "no online booking system found"))

        # Contact form to generic mailer (+2)
        if homepage_fetched and html_text:
            soup = BeautifulSoup(html_text, "lxml")
            for form in soup.select("form"):
                action = (form.get("action") or "").lower()
                form_text = form.get_text(" ", strip=True).lower()
                if ("contact" in form_text or "message" in form_text or
                    "mailto:" in action or "formspree" in action or
                    "getform" in action or "netlify" in action):
                    if not any(bk in form_text for bk in ["book", "schedule", "appointment"]):
                        score += 2
                        signals.append((2, "contact form without booking integration"))
                        break

        # No live chat widget (+1)
        has_chat = any(kw in html_lower for kw in _LIVE_CHAT_SCRIPTS)
        if not has_chat and homepage_fetched:
            score += 1
            signals.append((1, "no live chat widget"))

        # Outdated site design (+1)
        if homepage_fetched and html_text:
            has_viewport = 'name="viewport"' in html_lower or "name='viewport'" in html_lower
            has_doctype = html_lower.strip().startswith("<!doctype html")
            inline_count = html_lower.count('style="')
            if (not has_viewport) or (not has_doctype) or (inline_count > 20):
                score += 1
                signals.append((1, "outdated site design"))

        # Single location (+1)
        if homepage_fetched and html_text:
            addr_matches = re.findall(
                r'\d+\s+[\w\s]+(?:st|ave|rd|dr|blvd|cres|way|ct|lane|pkwy|hwy)',
                html_lower
            )
            unique_addrs = {re.sub(r'\s+', ' ', a.strip()) for a in addr_matches}
            if len(unique_addrs) <= 1:
                score += 1
                signals.append((1, "single location"))

        # Owner-dentist language (+2)
        for pat in _OWNER_DENTIST_PATTERNS:
            if re.search(pat, html_lower, re.I):
                score += 2
                signals.append((2, "owner-dentist language on site"))
                break

    elif vertical == "Trades":
        # "Call for quote" language (+3)
        has_call_cta = any(kw in html_lower for kw in _CALL_FOR_QUOTE_KEYWORDS)
        if not has_call_cta and homepage_fetched and html_text:
            soup = BeautifulSoup(html_text, "lxml")
            tel_links = soup.select("a[href^='tel:']")
            quote_forms = soup.select("form")
            has_quote_form = False
            for f in quote_forms:
                ft = f.get_text(" ", strip=True).lower()
                if any(w in ft for w in ["quote", "estimate", "book", "schedule"]):
                    has_quote_form = True
                    break
            if tel_links and not has_quote_form:
                has_call_cta = True
        if has_call_cta:
            score += 3
            signals.append((3, "call for quote with no online form"))

        # No online quote/booking form (+2)
        if homepage_fetched and html_text:
            soup = BeautifulSoup(html_text, "lxml")
            has_quote_form = False
            for f in soup.select("form"):
                ft = f.get_text(" ", strip=True).lower()
                if any(w in ft for w in ["quote", "estimate", "book", "schedule", "request"]):
                    has_quote_form = True
                    break
            if not has_quote_form:
                score += 2
                signals.append((2, "no online quote or booking form"))

        # No SMS/auto-response mention (+1)
        has_sms = any(kw in html_lower for kw in [
            "text us", "we'll text back", "auto-reply", "auto reply", "sms", "text message",
        ])
        if not has_sms and homepage_fetched:
            score += 1
            signals.append((1, "no SMS or auto-response"))

        # Owner-operator language (+2)
        for kw in _OWNER_OPERATOR_KEYWORDS:
            if kw in html_lower:
                score += 2
                signals.append((2, "owner-operator language on site"))
                break

        # Single location (+1)
        if homepage_fetched and html_text:
            addr_matches = re.findall(
                r'\d+\s+[\w\s]+(?:st|ave|rd|dr|blvd|cres|way|ct|lane|pkwy|hwy)',
                html_lower
            )
            unique_addrs = {re.sub(r'\s+', ' ', a.strip()) for a in addr_matches}
            if len(unique_addrs) <= 1:
                score += 1
                signals.append((1, "single location"))

    # Reviews mention slow response (+1) -- applies to both verticals
    if review_texts:
        all_review_text = " ".join(review_texts).lower()
        if any(kw in all_review_text for kw in _SLOW_RESPONSE_KEYWORDS):
            score += 1
            signals.append((1, "reviews mention slow response"))

    score = min(score, 10)

    if score >= 6:
        priority = "high"
    elif score >= 3:
        priority = "medium"
    else:
        priority = "low"

    # Build signal string from top signals
    signals.sort(key=lambda x: x[0], reverse=True)
    if not signals:
        signal_str = "no signals detected"
    else:
        parts = []
        total_len = 0
        for _, desc in signals:
            if total_len + len(desc) + 2 > 100:
                break
            parts.append(desc)
            total_len += len(desc) + 2
        signal_str = ", ".join(parts) if parts else signals[0][1][:100]

    return score, priority, signal_str


# =============================================================================
# ORCHESTRATION
# =============================================================================

class CircuitBreaker:
    """Trip after N consecutive Pass 2 failures of the same kind."""
    def __init__(self, threshold=3):
        self.threshold = threshold
        self.last_kind = None
        self.streak = 0
        self.tripped_reason = None
        self._lock = threading.Lock()

    def record(self, error_kind):
        with self._lock:
            if self.tripped_reason:
                return
            if error_kind is None:
                self.last_kind = None
                self.streak = 0
                return
            if error_kind == self.last_kind:
                self.streak += 1
            else:
                self.last_kind = error_kind
                self.streak = 1
            if self.streak >= self.threshold:
                self.tripped_reason = error_kind

    def is_tripped(self):
        with self._lock:
            return self.tripped_reason is not None


def enrich_one(prospect, circuit_breaker):
    name = prospect.get("name") or ""
    website = prospect.get("website") or ""
    vertical = prospect.get("cat") or ""
    existing_owner = prospect.get("owner")
    existing_owner_name = prospect.get("owner_name")
    existing_email = prospect.get("email")
    existing_phone = prospect.get("phone")

    patch = {"enriched_at": datetime.now(timezone.utc).isoformat()}

    # Pass 1
    if website:
        p1 = fetch_homepage(website)
    else:
        p1 = {
            "email": None, "phone": None, "owner_hint": None,
            "internal_links": [], "html": None, "error_kind": "other",
        }
    pass1_ok = p1["error_kind"] is None
    if pass1_ok:
        if p1["email"] and not existing_email:
            patch["email"] = p1["email"]
        if p1["phone"] and not existing_phone:
            patch["phone"] = p1["phone"]

    # Pass 2
    pass2_ok = False
    pass2_owner = None
    if pass1_ok and not circuit_breaker.is_tripped():
        about_url = find_about_url(website, p1["internal_links"], p1["html"])
        if about_url:
            p2 = fetch_about_page(about_url)
            circuit_breaker.record(p2["error_kind"])
            if p2["error_kind"] is None and p2["text"]:
                pass2_ok = True
                text = p2["text"]
                pass2_owner = extract_owner_name(text)
                yrs = extract_years_in_business(text)
                if yrs is not None:
                    patch["years_in_business"] = yrs
                snippet = extract_about_snippet(text)
                if snippet:
                    patch["about_us_content"] = snippet
                creds = extract_credentials(text, vertical)
                if creds:
                    patch["credentials"] = creds

    # Owner dual-write (Pass 2 first, then Pass 1 hint). Only fill null fields.
    owner_candidate = pass2_owner or p1["owner_hint"]
    if owner_candidate:
        if not existing_owner:
            patch["owner"] = owner_candidate
        if not existing_owner_name:
            patch["owner_name"] = owner_candidate

    # Pass 3
    p3 = places_lookup(name)
    pass3_ok = any(v is not None for k, v in p3.items() if k != "review_texts")
    if p3["rating"] is not None:
        patch["rating"] = p3["rating"]
    if p3["review_count"] is not None:
        patch["review_count"] = p3["review_count"]
    if p3["hours"] is not None:
        patch["hours"] = p3["hours"]
    if p3["is_operational"] is not None:
        patch["is_operational"] = p3["is_operational"]
    if p3["phone"] and "phone" not in patch and not existing_phone:
        patch["phone"] = p3["phone"]

    # Manual-work scoring (uses Pass 1 HTML + Pass 3 review texts)
    if pass1_ok or p3["review_texts"]:
        mw_score, mw_priority, mw_signal = compute_manual_work_score(
            vertical, p1["html"], pass1_ok, p3["review_texts"]
        )
        patch["manual_work_score"] = mw_score
        patch["manual_work_signal"] = mw_signal
        patch["priority"] = mw_priority

    # Status
    has_data = any(k in patch for k in DATA_FIELDS)
    if pass1_ok and pass2_ok and pass3_ok:
        patch["enrichment_status"] = "enriched"
    elif has_data:
        patch["enrichment_status"] = "partial"
    else:
        patch["enrichment_status"] = "failed"

    return {
        "prospect_id": prospect["id"],
        "name": name,
        "patch": patch,
        "pass1_ok": pass1_ok,
        "pass2_ok": pass2_ok,
        "pass3_ok": pass3_ok,
        "found_owner": bool(owner_candidate),
        "found_reviews": patch.get("rating") is not None or patch.get("review_count") is not None,
        "found_about": "about_us_content" in patch,
    }


def run(max_prospects=100, dry_run=False, prospect_id=None):
    if not GOOGLE_PLACES_API_KEY:
        raise RuntimeError("GOOGLE_PLACES_API_KEY is required -- aborting")
    if not ANTHROPIC_API_KEY:
        print("WARNING: ANTHROPIC_API_KEY is not set; LLM owner fallback disabled")

    print(f"[{datetime.now().isoformat()}] Unify Enrichment v2 starting "
          f"(max={max_prospects}, dry_run={dry_run})")
    prospects = get_prospects_to_enrich(max_prospects=max_prospects, prospect_id=prospect_id)
    print(f"Fetched {len(prospects)} prospect(s) to enrich")
    if not prospects:
        send_sms("Unify Enrichment: 0 processed, 0 owners, 0 reviews, 0 failed.")
        return

    cb = CircuitBreaker(threshold=3)
    results = []
    errors = 0

    def worker(p):
        try:
            out = enrich_one(p, cb)
            time.sleep(random.uniform(5, 10))
            return out
        except Exception as e:
            return {"prospect_id": p.get("id"), "error": str(e)}

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker, p) for p in prospects]
        for fut in as_completed(futures):
            r = fut.result()
            if "error" in r:
                errors += 1
                print(f"Prospect {r.get('prospect_id')} errored: {r['error']}")
                continue
            results.append(r)
            pid = str(r["prospect_id"])
            print(
                f"  {pid[:40]:40}  p1={int(r['pass1_ok'])} p2={int(r['pass2_ok'])} "
                f"p3={int(r['pass3_ok'])} owner={int(r['found_owner'])} "
                f"rev={int(r['found_reviews'])} about={int(r['found_about'])}"
            )
            if not dry_run:
                try:
                    update_prospect_enrichment(pid, r["patch"])
                except Exception as e:
                    errors += 1
                    print(f"  PATCH failed for {pid}: {e}")

    n = len(results)
    n_owner = sum(1 for r in results if r["found_owner"])
    n_reviews = sum(1 for r in results if r["found_reviews"])
    n_failed = errors + sum(1 for r in results if r["patch"].get("enrichment_status") == "failed")
    body = (
        f"Unify Enrichment: {n} processed, {n_owner} owners, "
        f"{n_reviews} reviews, {n_failed} failed."
    )
    if cb.tripped_reason:
        body += f" Pass 2 circuit-broken: {cb.tripped_reason}."
    print(body)
    send_sms(body)


def main():
    parser = argparse.ArgumentParser(description="Unify Enrichment Agent v2")
    parser.add_argument("--max", "-m", type=int, default=100,
                        help="Max prospects to process (default 100)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Skip writing to Supabase; print results only")
    parser.add_argument("--prospect-id", help="Enrich only this specific prospect id")
    args = parser.parse_args()
    try:
        run(max_prospects=args.max, dry_run=args.dry_run, prospect_id=args.prospect_id)
    except RuntimeError as e:
        print(f"FATAL: {e}")
        send_sms(f"Unify Enrichment FATAL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
