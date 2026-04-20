#!/usr/bin/env python3
"""
Unify Enrichment Agent v3

Three passes per prospect:
  Pass 1: homepage + contact/booking subpages -> email, phone, owner hint,
          internal links (multi-strategy email extraction: mailto / jsonld /
          text regex / html regex / obfuscated)
  Pass 2: About Us / Our Team / Meet-the-Doctor page -> owner name
          (regex + Haiku fallback), years in business, 500-char snippet,
          dental/medical credentials
  Pass 3: Google Places API (New) -> rating, review count, hours,
          operational status, phone, formatted address, website URI
          (used as fallback when prospect has no website)

Parallelism: ThreadPoolExecutor(max_workers=4). 5-10 s randomized sleep
between prospect completions so ubuntu-latest runners don't look like bots.

Circuit breaker: 3 consecutive Pass 2 failures of the same kind
(timeout / 403 / 429) pauses Pass 2 for the rest of the run. Passes 1 and 3
keep going.

Canaries: 5 hardcoded live-verified businesses run before every batch. If
2+ fail, abort. If 1 fails, warn via SMS but continue.

Required env vars:
  SUPABASE_URL, SUPABASE_KEY
  TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM, FRANCO_PHONE
  GOOGLE_PLACES_API_KEY, ANTHROPIC_API_KEY
"""

import argparse
import json
import os
import random
import re
import sys
import threading
import time
from collections import defaultdict
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
REQ_TIMEOUT = 20

UAS = [
    USER_AGENT,  # existing Chrome/124
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

CONTACT_URL_PATTERNS = [
    "/contact", "/contact-us", "/contact_us", "/contacts",
    "/get-in-touch", "/reach-us",
    "/book", "/booking", "/bookings", "/book-online", "/book-now",
    "/appointment", "/appointments", "/request-appointment",
]

# Tracking/analytics email fragments to filter out (noise in html regex)
TRACKING_EMAIL_NEEDLES = [
    "sentry", "wixpress", "wix.com", "intercom",
    "googletagmanager", "doubleclick",
]

# Canary tests -- must be hardcoded, live-verified as of 2026-04-20
# Each tuple: (business_name, expected_website_domain_fragment, expected_email)
# websiteUri match is substring; email match is EXACT (Franco's explicit rule).
CANARIES = [
    ("Drain King Plumbers",              "drainkingplumbers.ca",         "info@drainkingplumbers.ca"),
    ("Fairview Mall Dental Centre",      "fairviewmalldentalcentre.com", "info@fairviewmalldentalcentre.com"),
    ("Cynthia's Chinese Restaurant",     "cynthiaschinese.com",          "info@cynthiaschinese.com"),
    ("Work Of Art Barber Shop",          "workofartbarber.ca",           "workofartbarbershop@gmail.com"),
    ("Durham Autocare",                  "durhamautocare.ca",            "durhamautocare@gmail.com"),
]

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
# PLACEHOLDER / TEMPLATE EMAIL BLOCKLIST (added 2026-04-20)
# -----------------------------------------------------------------------------
# Catches template values that slip through html_regex / text_regex strategies.
# Real-world examples seen in the 2026-04-20 partial backfill:
#   - example@mysite.com  (Wix placeholder, appeared on 2 prospects' sites)
#   - your@email.com / user@domain.com  (template comments)
# RFC 2606 reserves example.{com,org,net} as non-routable placeholder domains.
# =============================================================================

PLACEHOLDER_EMAILS = {
    # Wix / Squarespace / template defaults seen in the wild
    "example@mysite.com", "example@example.com",
    "email@email.com", "email@website.com",
    "test@test.com", "test@example.com",
    "admin@admin.com",
    # "Your..." placeholder fields in contact-form templates
    "your@email.com", "your.email@email.com",
    "contact@yoursite.com", "info@yoursite.com",
    "contact@yourdomain.com", "hello@yourdomain.com",
    # Generic form placeholders
    "user@domain.com", "name@domain.com", "name@example.com",
    "firstname@lastname.com", "first.last@company.com",
}

PLACEHOLDER_DOMAINS = {
    # RFC 2606 reserved test domains
    "example.com", "example.org", "example.net",
    # Common placeholder domains on Wix / Squarespace / template sites
    "yourdomain.com", "yoursite.com", "yourcompany.com", "yourbusiness.com",
    "mysite.com",
}

# Local-part EXACT-match prefixes. Any email where `local == prefix` is
# rejected regardless of domain. Keep this list short -- each entry can
# false-positive on real addresses (e.g. "test@realbusiness.com" if we
# scrape a site that publishes a testing inbox). The current set is
# chosen because false-positive cost is low and template-catch value
# is high.
PLACEHOLDER_LOCAL_PREFIXES = {
    "example", "your.email", "firstname.lastname", "test",
}


def _is_placeholder_email(email):
    """Return True if this looks like a template/placeholder email that
    should never be written to a prospect record."""
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return False
    if e in PLACEHOLDER_EMAILS:
        return True
    local, _, domain = e.partition("@")
    if domain in PLACEHOLDER_DOMAINS:
        return True
    if any(domain.endswith("." + d) for d in PLACEHOLDER_DOMAINS):
        return True
    if local in PLACEHOLDER_LOCAL_PREFIXES:
        return True
    return False


# =============================================================================
# TRUSTED FREE-EMAIL PROVIDERS (added 2026-04-20)
# -----------------------------------------------------------------------------
# html_regex hits on unknown domains are too often false positives (font CDNs,
# third-party scripts, CSS comments). When the extracted email's domain does
# NOT match the business website, only accept it if it's a common free-email
# provider -- a real business email pattern.
# =============================================================================

TRUSTED_FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com",
    "yahoo.com", "yahoo.ca", "yahoo.co.uk",
    "outlook.com", "hotmail.com", "live.com", "live.ca", "msn.com",
    "icloud.com", "me.com", "mac.com",
}


def _is_trusted_free_email(email):
    domain = (email or "").partition("@")[2].lower()
    return domain in TRUSTED_FREE_EMAIL_DOMAINS


# =============================================================================
# MULTI-STRATEGY EMAIL EXTRACTION
# =============================================================================

_EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")


def _email_passes_noise_filter(email):
    """Reject emails whose local-part is too long or contains tracking fragments."""
    e = (email or "").strip().lower()
    if not e or "@" not in e:
        return False
    local, _, domain = e.partition("@")
    if len(local) > 30:
        return False
    if any(needle in e for needle in TRACKING_EMAIL_NEEDLES):
        return False
    return True


def _walk_jsonld_for_emails(node, out):
    """Recursively walk a parsed JSON-LD structure collecting values under any
    key named 'email' (case-insensitive)."""
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(k, str) and k.lower() == "email":
                if isinstance(v, str):
                    out.append(v.strip())
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, str):
                            out.append(item.strip())
                        else:
                            _walk_jsonld_for_emails(item, out)
            else:
                _walk_jsonld_for_emails(v, out)
    elif isinstance(node, list):
        for item in node:
            _walk_jsonld_for_emails(item, out)


def _extract_emails_from_html(html, business_domain=None):
    """
    Returns list[tuple[str, str]] of (strategy_name, email_lowercased).
    Strategies run in order: mailto, jsonld, text_regex, html_regex, obfuscated.
    DEAD_END emails are skipped. Noise-filter (length + tracking needles) is
    applied to regex-based strategies. Caller dedupes; we preserve order and
    do NOT dedupe across strategies (so domain-preference logic can see all).
    """
    results = []
    if not html:
        return results

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = None

    # Strategy 1: mailto
    if soup is not None:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().startswith("mailto:"):
                candidate = href[7:].split("?")[0].strip().lower()
                if "@" not in candidate:
                    continue
                if _is_dead_end_email(candidate):
                    continue
                if _is_placeholder_email(candidate):
                    continue
                results.append(("mailto", candidate))

    # Strategy 2: jsonld
    if soup is not None:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string or script.get_text() or ""
            raw = raw.strip()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except Exception:
                continue
            collected = []
            _walk_jsonld_for_emails(parsed, collected)
            for cand in collected:
                cand = (cand or "").strip().lower()
                if "@" not in cand:
                    continue
                if _is_dead_end_email(cand):
                    continue
                if _is_placeholder_email(cand):
                    continue
                results.append(("jsonld", cand))

    # Strategy 3: text_regex
    if soup is not None:
        text = soup.get_text(" ", strip=True)
        for m in _EMAIL_REGEX.finditer(text):
            cand = m.group(0).strip().lower()
            if _is_dead_end_email(cand):
                continue
            if _is_placeholder_email(cand):
                continue
            if not _email_passes_noise_filter(cand):
                continue
            results.append(("text_regex", cand))

    # Strategy 4: html_regex (raw HTML -- catches attributes, inline scripts)
    seen_so_far = {e for _, e in results}
    for m in _EMAIL_REGEX.finditer(html):
        cand = m.group(0).strip().lower()
        if cand in seen_so_far:
            continue
        if _is_dead_end_email(cand):
            continue
        if _is_placeholder_email(cand):
            continue
        if not _email_passes_noise_filter(cand):
            continue
        results.append(("html_regex", cand))

    # Strategy 5: obfuscated
    decoded = html
    decoded = decoded.replace("&#64;", "@").replace("&commat;", "@").replace("&#x40;", "@")
    # Patterns like "name [at] domain [dot] com" or "name(at)domain(dot)com"
    patterns = [
        re.compile(
            r"([A-Za-z0-9._%+\-]+)\s*(?:\[at\]|\(at\))\s*([A-Za-z0-9.\-]+)\s*"
            r"(?:\[dot\]|\(dot\))\s*([A-Za-z]{2,})",
            re.I,
        ),
        re.compile(
            r"([A-Za-z0-9._%+\-]+)\s*(?:\s+at\s+)\s*([A-Za-z0-9.\-]+)"
            r"\s+(?:dot)\s+([A-Za-z]{2,})",
            re.I,
        ),
    ]
    seen_so_far = {e for _, e in results}
    for pat in patterns:
        for m in pat.finditer(html):
            try:
                local = m.group(1).strip()
                domain = m.group(2).strip()
                tld = m.group(3).strip()
            except (IndexError, AttributeError):
                continue
            if not local or not domain or not tld:
                continue
            cand = f"{local}@{domain}.{tld}".lower()
            if not _EMAIL_REGEX.fullmatch(cand):
                continue
            if cand in seen_so_far:
                continue
            if _is_dead_end_email(cand):
                continue
            if _is_placeholder_email(cand):
                continue
            if not _email_passes_noise_filter(cand):
                continue
            results.append(("obfuscated", cand))
            seen_so_far.add(cand)
    # Also scan the entity-decoded HTML for normal-looking emails we might have missed
    for m in _EMAIL_REGEX.finditer(decoded):
        cand = m.group(0).strip().lower()
        if cand in seen_so_far:
            continue
        if _is_dead_end_email(cand):
            continue
        if _is_placeholder_email(cand):
            continue
        if not _email_passes_noise_filter(cand):
            continue
        # Only count as obfuscated if the original HTML didn't already surface it
        if cand not in html.lower():
            results.append(("obfuscated", cand))
            seen_so_far.add(cand)

    return results


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


def get_prospects_to_enrich(max_prospects=100, prospect_id=None, backfill=False):
    """
    Fetch prospects where enriched_at is NULL or older than 30 days.
    If prospect_id is provided, fetch only that row.
    If backfill is True, pick prospects where email IS NULL or email='',
    regardless of enriched_at.
    """
    if prospect_id:
        url = f"{SUPABASE_URL}/rest/v1/prospects?id=eq.{quote(prospect_id)}"
    elif backfill:
        # Match both empty string and true NULL in case any slip through.
        # PostgREST OR-combinator: "email IS NULL OR email = ''"
        url = (
            f"{SUPABASE_URL}/rest/v1/prospects"
            f"?or=(email.is.null,email.eq.)"
            f"&order=created_at.asc"
            f"&limit={max_prospects}"
        )
    else:
        # Cron selector (widened 2026-04-20): include prospects that still have
        # no email regardless of enriched_at. Without this clause, prospects
        # whose enrichment ran recently but didn't recover an email would be
        # locked out of re-processing for 30 days, which means fixes to the
        # extraction pipeline don't reach them until a month later.
        # Same OR-predicate as backfill mode: one source of truth for
        # "empty email."
        # Tradeoff: permanently-hopeless prospects (dead DNS, no Places match)
        # will be retried on every cron. Budget-safe at current volume; add a
        # cooldown clause if the tail grows.
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        url = (
            f"{SUPABASE_URL}/rest/v1/prospects"
            f"?or=(enriched_at.is.null,enriched_at.lt.{quote(cutoff)},"
            f"email.is.null,email.eq.)"
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


def _insert_enrichment_run(row):
    """Best-effort insert into enrichment_runs telemetry table.
    Returns the inserted row's id (UUID string) or None on failure.
    """
    try:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/enrichment_runs",
            headers=sb_headers(),
            json=row,
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        if isinstance(body, list) and body:
            return body[0].get("id")
        if isinstance(body, dict):
            return body.get("id")
    except Exception as e:
        print(f"WARN: could not insert enrichment_runs row: {e}")
    return None


def _update_enrichment_run(row_id, fields):
    """Best-effort PATCH of an existing enrichment_runs row.
    Used to flip a 'starting' row to final metrics at end of run. Survives
    workflow cancellation: if the agent is killed before this call, the
    starting row remains with duration_seconds=NULL and canary_pass=FALSE,
    making the kill visible in telemetry."""
    if not row_id:
        return
    try:
        resp = requests.patch(
            f"{SUPABASE_URL}/rest/v1/enrichment_runs?id=eq.{quote(row_id)}",
            headers=sb_headers(),
            json=fields,
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"WARN: could not update enrichment_runs row {row_id}: {e}")


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
# FETCH HELPERS
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


def _build_req_headers():
    """Rotate User-Agent per request, add Accept header."""
    return {
        "User-Agent": random.choice(UAS),
        "Accept-Language": "en-CA,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _fetch_with_retry(url, timeout=REQ_TIMEOUT):
    """
    One attempt. On timeout or status >= 500: sleep 30 s, retry once.
    Returns dict: {status, html, error_kind}. error_kind is None on success.
    """
    def _try_once():
        try:
            resp = requests.get(
                url,
                headers=_build_req_headers(),
                timeout=timeout,
                allow_redirects=True,
            )
            return {"status": resp.status_code, "html": resp.text or "", "error_kind": None, "exception": None}
        except requests.Timeout:
            return {"status": None, "html": None, "error_kind": "timeout", "exception": "timeout"}
        except Exception as e:
            return {"status": None, "html": None, "error_kind": "other", "exception": str(e)[:60]}

    out = _try_once()
    # Success
    if out["error_kind"] is None and out["status"] is not None and out["status"] < 500:
        if out["status"] >= 400:
            out["error_kind"] = _classify_http_error(out["status"])
        return out
    # Retry path
    if out["error_kind"] == "timeout" or (out["status"] is not None and out["status"] >= 500):
        time.sleep(30)
        out2 = _try_once()
        if out2["error_kind"] is None and out2["status"] is not None:
            if out2["status"] >= 400:
                out2["error_kind"] = _classify_http_error(out2["status"])
        return out2
    # Other error (e.g. DNS) -- one shot only
    return out


def _fetch_site_pages(homepage_url):
    """
    Fetch homepage + up to 4 contact/booking subpages on the same netloc.

    Returns:
      {
        "pages": [{"url", "suffix", "status", "html"}, ...],
        "error_kind": None | "timeout" | "403" | "429" | "other",
      }
    error_kind is set from the HOMEPAGE fetch only. Subpage errors are silent.
    """
    result = {"pages": [], "error_kind": None}
    if not homepage_url:
        result["error_kind"] = "other"
        return result

    # Homepage first
    hp = _fetch_with_retry(homepage_url)
    if hp["error_kind"] is not None:
        result["error_kind"] = hp["error_kind"]
        return result

    homepage_html = hp["html"] or ""
    result["pages"].append({
        "url": homepage_url,
        "suffix": "/",
        "status": hp["status"],
        "html": homepage_html,
    })

    base_netloc = urlparse(homepage_url).netloc.lower()

    # 1) Discover subpage URLs from homepage links matching CONTACT_URL_PATTERNS
    discovered = []
    seen_urls = {homepage_url.rstrip("/").lower()}
    try:
        soup = BeautifulSoup(homepage_html, "lxml")
    except Exception:
        soup = None

    if soup is not None:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#"):
                continue
            if href.lower().startswith(("mailto:", "tel:")):
                continue
            full = urljoin(homepage_url, href)
            parsed = urlparse(full)
            if not parsed.scheme.startswith("http"):
                continue
            if parsed.netloc and parsed.netloc.lower() != base_netloc:
                continue
            full_norm = full.rstrip("/").lower()
            if full_norm in seen_urls:
                continue
            path_lower = (parsed.path or "").rstrip("/").lower() or "/"
            anchor_text = a.get_text(" ", strip=True).lower()
            # Match if path hits a contact pattern, or href/anchor text mentions a keyword
            keyword_hit = False
            for pat in CONTACT_URL_PATTERNS:
                if path_lower == pat or path_lower.endswith(pat):
                    keyword_hit = True
                    break
                tail = pat.lstrip("/")
                if tail and (tail in href.lower() or tail in anchor_text):
                    keyword_hit = True
                    break
            if not keyword_hit:
                continue
            discovered.append(full)
            seen_urls.add(full_norm)

    # 2) Also try hardcoded paths even if not linked
    parsed_home = urlparse(homepage_url)
    scheme = parsed_home.scheme or "https"
    for pat in CONTACT_URL_PATTERNS:
        guess = f"{scheme}://{base_netloc}{pat}"
        guess_norm = guess.rstrip("/").lower()
        if guess_norm in seen_urls:
            continue
        discovered.append(guess)
        seen_urls.add(guess_norm)

    # 3) Cap at 4 subpages (5 total including homepage)
    homepage_len = len(homepage_html)
    for sub_url in discovered:
        if len(result["pages"]) >= 5:
            break
        time.sleep(2)
        sub = _fetch_with_retry(sub_url)
        if sub["error_kind"] is not None:
            continue
        if sub["status"] == 404:
            continue
        sub_html = sub["html"] or ""
        # Skip if looks like a duplicate of homepage
        if homepage_len > 0:
            ratio = abs(len(sub_html) - homepage_len) / max(homepage_len, 1)
            if ratio < 0.05:
                continue
        parsed_sub = urlparse(sub_url)
        sub_path = parsed_sub.path.rstrip("/").lower() or "/"
        result["pages"].append({
            "url": sub_url,
            "suffix": sub_path,
            "status": sub["status"],
            "html": sub_html,
        })

    return result


# =============================================================================
# PASS 1 -- HOMEPAGE FETCH (+ CONTACT / BOOKING SUBPAGES)
# =============================================================================

def _select_best_email(candidates, business_domain):
    """
    candidates: list of (strategy, email, source_page) tuples.
    Returns (email, strategy, source_page) or (None, None, None).
    Preference:
      (a) mailto AND email-domain matches business-domain
      (b) any strategy AND email-domain matches business-domain
      (c) mailto on any domain
      (d) any non-html_regex strategy on any non-DEAD-END domain
      (e) html_regex ONLY if email-domain is a trusted free provider
          (gmail/yahoo/outlook/hotmail/icloud)

    Rationale for (d) vs (e): html_regex scans raw HTML including CSS
    comments, font-license blobs, and inline-script bodies. Seen in the
    2026-04-20 partial backfill: info@indiantypefoundry.com (font CDN)
    extracted from a legitimate business site. Limiting unmatched-domain
    html_regex hits to known free-email providers avoids that class of
    false positive.
    """
    bd = (business_domain or "").lower().lstrip(".")

    def domain_matches(email):
        if not bd:
            return False
        _, _, edom = email.partition("@")
        edom = edom.lower()
        if not edom:
            return False
        return edom == bd or edom.endswith("." + bd)

    # Bucket the candidates
    a_bucket = []
    b_bucket = []
    c_bucket = []
    d_bucket = []
    e_bucket = []
    for strategy, email, src in candidates:
        dm = domain_matches(email)
        if strategy == "mailto" and dm:
            a_bucket.append((strategy, email, src))
        elif dm:
            b_bucket.append((strategy, email, src))
        elif strategy == "mailto":
            c_bucket.append((strategy, email, src))
        elif strategy == "html_regex":
            # Gated: only accept if domain is a trusted free provider
            if _is_trusted_free_email(email):
                e_bucket.append((strategy, email, src))
            # else: silently dropped as likely false positive
        else:
            d_bucket.append((strategy, email, src))

    for bucket in (a_bucket, b_bucket, c_bucket, d_bucket, e_bucket):
        if bucket:
            s, e, src = bucket[0]
            return e, s, src
    return None, None, None


def fetch_homepage(url):
    """
    Fetches homepage + up to 4 contact/booking subpages. Runs 5-strategy email
    extraction across all pages. Returns richer dict (v3 wire format):

      {
        "email": str | None,           # best email found across all pages
        "email_source_page": str,      # which suffix won
        "email_source_strategy": str,  # which strategy won
        "email_candidates": list,      # all (strategy, email, source_page) tuples found
        "phone": str | None,
        "owner_hint": str | None,      # header/footer only, homepage
        "internal_links": list,        # homepage-only, for Pass 2 about-page lookup
        "html": str | None,            # HOMEPAGE html (backward compat)
        "error_kind": str | None,
        "pages_fetched": list,         # [{"suffix": "/", "status": 200}, ...]
      }
    """
    result = {
        "email": None,
        "email_source_page": None,
        "email_source_strategy": None,
        "email_candidates": [],
        "phone": None,
        "owner_hint": None,
        "internal_links": [],
        "html": None,
        "error_kind": None,
        "pages_fetched": [],
    }
    if not url:
        result["error_kind"] = "other"
        return result

    site = _fetch_site_pages(url)
    if site["error_kind"] is not None:
        result["error_kind"] = site["error_kind"]
        # pages may still be empty
        result["pages_fetched"] = [
            {"suffix": p["suffix"], "status": p["status"]} for p in site["pages"]
        ]
        return result

    pages = site["pages"]
    if not pages:
        result["error_kind"] = "other"
        return result

    # Homepage HTML for backward compat
    homepage = pages[0]
    result["html"] = homepage["html"]
    result["pages_fetched"] = [
        {"suffix": p["suffix"], "status": p["status"]} for p in pages
    ]

    # Business domain for email preference
    business_domain = urlparse(url).netloc.lower()
    if business_domain.startswith("www."):
        business_domain = business_domain[4:]

    # Collect candidates across all pages
    all_candidates = []
    for page in pages:
        for strategy, email in _extract_emails_from_html(page["html"], business_domain):
            all_candidates.append((strategy, email, page["suffix"]))

    result["email_candidates"] = all_candidates

    best_email, best_strategy, best_src = _select_best_email(all_candidates, business_domain)
    result["email"] = best_email
    result["email_source_strategy"] = best_strategy
    result["email_source_page"] = best_src

    # Homepage-only: phone, owner hint, internal links
    try:
        soup = BeautifulSoup(homepage["html"], "lxml")
    except Exception:
        soup = None

    if soup is not None:
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
        resp = requests.get(url, headers=_build_req_headers(), timeout=REQ_TIMEOUT, allow_redirects=True)
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
    Returns dict: rating, review_count, hours, is_operational, phone,
    review_texts, website_uri, formatted_address.
    All None (or empty list) if Places has no match. Raises RuntimeError only if
    API key missing. review_texts is a list of review text strings (up to 5).
    """
    result = {
        "rating": None, "review_count": None, "hours": None,
        "is_operational": None, "phone": None, "review_texts": [],
        "website_uri": None, "formatted_address": None,
    }
    if not GOOGLE_PLACES_API_KEY:
        raise RuntimeError("GOOGLE_PLACES_API_KEY is required")
    if not business_name:
        return result

    # Text Search -> place_id (+ formattedAddress for sanity check / website hint)
    try:
        resp = requests.post(
            "https://places.googleapis.com/v1/places:searchText",
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": "places.id,places.formattedAddress,places.websiteUri",
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

    # Capture early: formattedAddress + websiteUri from text search
    ts_formatted_address = places[0].get("formattedAddress")
    ts_website_uri = places[0].get("websiteUri")
    if ts_formatted_address:
        result["formatted_address"] = ts_formatted_address
    if ts_website_uri:
        result["website_uri"] = ts_website_uri

    # Place Details
    try:
        resp = requests.get(
            f"https://places.googleapis.com/v1/places/{place_id}",
            headers={
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask": (
                    "rating,userRatingCount,regularOpeningHours,"
                    "businessStatus,nationalPhoneNumber,reviews.text,"
                    "websiteUri,formattedAddress"
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

    # Prefer details-level websiteUri / formattedAddress when present
    if details.get("websiteUri"):
        result["website_uri"] = details["websiteUri"]
    if details.get("formattedAddress"):
        result["formatted_address"] = details["formattedAddress"]

    # Extract review texts (up to 5)
    for rev in details.get("reviews", []):
        text_obj = rev.get("text", {})
        text = text_obj.get("text", "") if isinstance(text_obj, dict) else str(text_obj)
        if text:
            result["review_texts"].append(text)

    return result


def _sanity_check_places_result(prospect, formatted_address, places_phone):
    """
    Return (passed: bool, reason: str). Checks whether Places result matches
    prospect's city (from prospect.address) OR area code (from prospect.phone).

    City extraction has to survive the wild CRM address format produced by
    the YP sourcer, which looks like:
        "100 Saint Regis Crescent,North York,ONM3J 1Y8Get directions"
    Note:
      - No space between comma and next token (already handled by split+strip)
      - "ON" glued to postal code (ONM3J) -- defeats plain postal regex
      - "Get directions" / "View all add" / similar YP trailing junk
    This v8 parser strips those before tokenizing.
    """
    addr = prospect.get("address") or ""

    # Pre-clean: strip known YP / directory trailing junk
    addr = re.sub(
        r"\b(Get\s+directions|View\s+all[\w\s]*|View\s+on\s+map|Directions)\b.*$",
        "",
        addr,
        flags=re.IGNORECASE,
    ).strip()
    # Split "ON" glued to a postal prefix: "ONM3J" -> "ON M3J"
    addr = re.sub(r"\b([Oo][Nn])([A-Za-z]\d[A-Za-z])", r"\1 \2", addr)

    tokens = [t.strip() for t in addr.split(",") if t.strip()]

    def _looks_like_postal(tok):
        # Canadian postal pattern, embedded anywhere in the token.
        # "M3J 1Y8" / "M3J1Y8" / "ON M3J 1Y8" all match.
        return bool(re.search(r"[A-Za-z]\d[A-Za-z]\s*\d[A-Za-z]\d", tok))

    def _is_province(tok):
        return tok.strip().lower() in ("on", "ontario")

    def _has_too_little_signal(tok):
        # Filter tokens that are empty or lack at least 3 alphanumeric chars
        cleaned = re.sub(r"[^\w]", "", tok).strip()
        return len(cleaned) < 3

    prospect_city = None
    filtered = [
        t for t in tokens
        if not _looks_like_postal(t)
        and not _is_province(t)
        and not _has_too_little_signal(t)
    ]
    if filtered:
        # Pick the LAST remaining token -- Canadian address order is
        # "street, city, province postal", so after stripping province + postal
        # the last survivor is the city (or, if no city given, the street).
        prospect_city = filtered[-1].lower()

    fa_lower = (formatted_address or "").lower()
    city_match = bool(prospect_city and fa_lower and prospect_city in fa_lower)

    def _area_code(phone):
        digits = re.sub(r"\D", "", phone or "")
        if len(digits) < 10:
            return None
        tail10 = digits[-10:]
        return tail10[:3]

    prospect_ac = _area_code(prospect.get("phone") or "")
    places_ac = _area_code(places_phone or "")
    ac_match = bool(prospect_ac and places_ac and prospect_ac == places_ac)

    if city_match or ac_match:
        return True, "ok"
    reason = (
        f"city '{prospect_city}' not in '{formatted_address}'; "
        f"area code {prospect_ac} != {places_ac}"
    )
    return False, reason


# =============================================================================
# CANARIES -- pre-flight sanity checks for Places + fetch pipeline
# =============================================================================

def run_canaries():
    """
    Returns list of (name, reason) for failed canaries. Empty = all pass.
    """
    failures = []
    for name, expected_domain, expected_email in CANARIES:
        try:
            p3 = places_lookup(name)
            uri = (p3.get("website_uri") or "").lower()
            if expected_domain.lower() not in uri:
                failures.append((name, f"websiteUri mismatch: {p3.get('website_uri')}"))
                continue
            p1 = fetch_homepage(p3["website_uri"])
            cands = {e.lower() for _, e, _ in p1.get("email_candidates", [])}
            if expected_email.lower() not in cands:
                near = [e for e in cands if expected_email.split("@")[1].lower() in e]
                failures.append((name, f"exact email not found. got: {sorted(near)[:3] or sorted(cands)[:3]}"))
        except Exception as e:
            failures.append((name, f"exception: {type(e).__name__}: {str(e)[:60]}"))
    return failures


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
    website = (prospect.get("website") or "").strip()
    vertical = prospect.get("cat") or ""
    existing_owner = prospect.get("owner")
    existing_owner_name = prospect.get("owner_name")
    existing_email = prospect.get("email")
    existing_phone = prospect.get("phone")

    already_had_email = bool((existing_email or "").strip())

    patch = {"enriched_at": datetime.now(timezone.utc).isoformat()}
    log = {
        "prospect_id": prospect["id"],
        "name": name[:40],
        "pages_fetched": [],
        "strategies_tried": [],
        "strategies_hit": [],
        "final_email": None,
        "email_source_page": None,
        "email_source_strategy": None,
        "website_was_null": not bool(website),
        "website_from_places": None,
        "sanity_check": "n/a",
        "reason_if_skipped": None,
        "had_email_before": already_had_email,
    }

    # Pass 3 (Places) -- run FIRST so we can use websiteUri as fallback
    p3 = places_lookup(name)
    pass3_ok = any(
        v is not None and v != []
        for k, v in p3.items()
        if k not in ("review_texts", "formatted_address")
    )
    if p3["rating"] is not None:
        patch["rating"] = p3["rating"]
    if p3["review_count"] is not None:
        patch["review_count"] = p3["review_count"]
    if p3["hours"] is not None:
        patch["hours"] = p3["hours"]
    if p3["is_operational"] is not None:
        patch["is_operational"] = p3["is_operational"]
    if p3["phone"] and not existing_phone:
        patch["phone"] = p3["phone"]

    # Website fallback from Places (sanity-checked)
    if not website and p3.get("website_uri"):
        passed, reason = _sanity_check_places_result(
            prospect, p3.get("formatted_address"), p3.get("phone")
        )
        log["sanity_check"] = "passed" if passed else f"failed ({reason})"
        if passed:
            website = p3["website_uri"].strip()
            patch["website"] = website
            log["website_from_places"] = website
        else:
            log["reason_if_skipped"] = f"places website rejected: {reason}"

    # Pass 1 + subpages (multi-page, multi-strategy email extraction)
    pass1_ok = False
    p1 = {
        "email": None, "phone": None, "owner_hint": None,
        "internal_links": [], "html": None, "error_kind": "other",
        "pages_fetched": [], "email_candidates": [],
        "email_source_page": None, "email_source_strategy": None,
    }
    if website:
        p1 = fetch_homepage(website)
        log["pages_fetched"] = p1.get("pages_fetched", [])
        log["strategies_tried"] = sorted({s for s, _, _ in p1.get("email_candidates", [])})
        pass1_ok = p1["error_kind"] is None

        if pass1_ok:
            if p1.get("email") and not existing_email:
                patch["email"] = p1["email"]
                log["final_email"] = p1["email"]
                log["email_source_page"] = p1.get("email_source_page")
                log["email_source_strategy"] = p1.get("email_source_strategy")
                if p1.get("email_source_strategy"):
                    log["strategies_hit"] = [p1["email_source_strategy"]]
            if p1.get("phone") and not existing_phone and "phone" not in patch:
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
    owner_candidate = pass2_owner or p1.get("owner_hint")
    if owner_candidate:
        if not existing_owner:
            patch["owner"] = owner_candidate
        if not existing_owner_name:
            patch["owner_name"] = owner_candidate

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
        "log": log,
    }


def run(max_prospects=100, dry_run=False, prospect_id=None, backfill=False):
    if not GOOGLE_PLACES_API_KEY:
        raise RuntimeError("GOOGLE_PLACES_API_KEY is required -- aborting")
    if not ANTHROPIC_API_KEY:
        print("WARNING: ANTHROPIC_API_KEY is not set; LLM owner fallback disabled")

    run_start = time.time()
    # Determine trigger
    trigger = "backfill" if backfill else ("manual" if prospect_id is None and max_prospects and max_prospects != 100 else "cron")
    if prospect_id:
        trigger = "manual"

    print(f"[{datetime.now().isoformat()}] Unify Enrichment v3 starting "
          f"(max={max_prospects}, dry_run={dry_run}, backfill={backfill}, trigger={trigger})")

    # Canaries (before any prospect work)
    print(f"[{datetime.now().isoformat()}] Running canaries...")
    canary_failures = run_canaries()
    canary_pass = True
    if len(canary_failures) == 0:
        print("Canaries: all 5 passed")
    elif len(canary_failures) == 1:
        n, r = canary_failures[0]
        print(f"Canary WARNING (1 of 5 failed): {n} -- {r}")
        send_sms(f"\u26a0\ufe0f CANARY FLAKY: {n} -- {r[:200]}")
        canary_pass = True  # still continue
    else:
        msg = (
            f"\u26a0\ufe0f CANARY ABORT: {len(canary_failures)}/5 failed. "
            "Run aborted, 0 prospects touched. "
            + "; ".join(f"{n}: {r[:80]}" for n, r in canary_failures[:3])
        )
        print(msg)
        send_sms(msg[:1500])
        _insert_enrichment_run({
            "trigger": trigger,
            "duration_seconds": int(time.time() - run_start),
            "total_scanned": 0,
            "emails_found": 0,
            "emails_already_present": 0,
            "websites_discovered": 0,
            "websites_rejected_sanity": 0,
            "per_strategy": {},
            "per_vertical": {},
            "canary_pass": False,
            "canary_failures": [{"name": n, "reason": r} for n, r in canary_failures],
        })
        return

    prospects = get_prospects_to_enrich(
        max_prospects=max_prospects, prospect_id=prospect_id, backfill=backfill,
    )
    print(f"Fetched {len(prospects)} prospect(s) to enrich")

    # Insert a STARTING row early so telemetry survives workflow cancellation.
    # If the agent is killed before the final PATCH, this row remains with
    # duration_seconds=NULL, making the kill visible in telemetry.
    # We PATCH this row at the end with real metrics.
    run_row_id = _insert_enrichment_run({
        "trigger": trigger,
        "duration_seconds": None,
        "total_scanned": len(prospects),
        "emails_found": 0,
        "emails_already_present": 0,
        "websites_discovered": 0,
        "websites_rejected_sanity": 0,
        "per_strategy": {},
        "per_vertical": {},
        "canary_pass": canary_pass,
        "canary_failures": [{"name": n, "reason": r} for n, r in canary_failures] if canary_failures else None,
    })
    if run_row_id:
        print(f"enrichment_runs starting row id: {run_row_id}")

    if not prospects:
        send_sms("Unify enrichment: 0 processed, 0 owners, 0 reviews, 0 failed.")
        _update_enrichment_run(run_row_id, {
            "duration_seconds": int(time.time() - run_start),
        })
        return

    cb = CircuitBreaker(threshold=3)
    results = []
    errors = 0

    metrics = {
        "total_scanned": len(prospects),
        "emails_found": 0,
        "emails_already_present": 0,
        "websites_discovered": 0,
        "websites_rejected_sanity": 0,
        "per_strategy": defaultdict(int),
        "per_vertical": defaultdict(lambda: {"scanned": 0, "email_found": 0}),
    }

    def worker(p):
        try:
            out = enrich_one(p, cb)
            time.sleep(random.uniform(5, 10))
            return out
        except Exception as e:
            return {"prospect_id": p.get("id"), "error": str(e)}

    # Build lookup for per-prospect vertical
    prospect_by_id = {p["id"]: p for p in prospects}

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

            # Structured log line (JSON)
            log = r.get("log", {})
            try:
                print(json.dumps(log))
            except Exception:
                pass

            # Metrics
            p_src = prospect_by_id.get(r["prospect_id"]) or {}
            vertical = p_src.get("cat") or "UNKNOWN"
            metrics["per_vertical"][vertical]["scanned"] += 1
            if r["patch"].get("email") and not log.get("had_email_before"):
                metrics["emails_found"] += 1
                metrics["per_vertical"][vertical]["email_found"] += 1
                if log.get("email_source_strategy"):
                    metrics["per_strategy"][log["email_source_strategy"]] += 1
            if log.get("had_email_before"):
                metrics["emails_already_present"] += 1
            if log.get("website_from_places"):
                metrics["websites_discovered"] += 1
            if isinstance(log.get("sanity_check"), str) and log["sanity_check"].startswith("failed"):
                metrics["websites_rejected_sanity"] += 1

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

    duration = int(time.time() - run_start)
    hit_rate = 100.0 * metrics["emails_found"] / metrics["total_scanned"] if metrics["total_scanned"] else 0.0

    # Flip the starting row to final metrics (PATCH, not INSERT). If the
    # agent was killed between the starting INSERT and this PATCH, the row
    # remains with duration_seconds=NULL -- that's the cancellation signal.
    _update_enrichment_run(run_row_id, {
        "duration_seconds": duration,
        "total_scanned": metrics["total_scanned"],
        "emails_found": metrics["emails_found"],
        "emails_already_present": metrics["emails_already_present"],
        "websites_discovered": metrics["websites_discovered"],
        "websites_rejected_sanity": metrics["websites_rejected_sanity"],
        "per_strategy": dict(metrics["per_strategy"]),
        "per_vertical": {k: dict(v) for k, v in metrics["per_vertical"].items()},
    })

    # SMS summary
    prefix = (
        "\u26a0\ufe0f ENRICHMENT HEALTH: "
        if hit_rate < 25.0 and metrics["total_scanned"] >= 20
        else "Unify enrichment: "
    )
    body = (
        f"{prefix}{metrics['emails_found']}/{metrics['total_scanned']} new emails "
        f"({hit_rate:.1f}% hit). {metrics['websites_discovered']} sites via Places. "
        f"Canaries: {5 - len(canary_failures)}/5 passed. {duration}s."
    )
    if cb.tripped_reason:
        body += f" Pass 2 circuit-broken: {cb.tripped_reason}."
    print(body)
    print(
        f"Breakdown: {n} processed, {n_owner} owners, "
        f"{n_reviews} reviews, {n_failed} failed."
    )
    send_sms(body)


def main():
    parser = argparse.ArgumentParser(description="Unify Enrichment Agent v3")
    parser.add_argument("--max", "-m", type=int, default=100,
                        help="Max prospects to process (default 100)")
    parser.add_argument("--dry-run", "-d", action="store_true",
                        help="Skip writing to Supabase; print results only")
    parser.add_argument("--prospect-id", help="Enrich only this specific prospect id")
    parser.add_argument("--backfill", action="store_true",
                        help="Backfill mode: pick prospects where email='' regardless of enriched_at")
    args = parser.parse_args()
    try:
        run(
            max_prospects=args.max,
            dry_run=args.dry_run,
            prospect_id=args.prospect_id,
            backfill=args.backfill,
        )
    except RuntimeError as e:
        print(f"FATAL: {e}")
        send_sms(f"Unify Enrichment FATAL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
