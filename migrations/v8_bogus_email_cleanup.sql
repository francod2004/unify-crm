-- v8 cleanup: three bogus emails extracted in the 2026-04-20 partial backfill
-- before the placeholder filter (PLACEHOLDER_EMAILS/PLACEHOLDER_DOMAINS) and
-- the html_regex domain-match gate were added in enrichment_agent.py v3.1.
--
-- After these UPDATEs run, tomorrow's scheduled cron (using the widened
-- selector `email IS NULL OR email = ''`) will naturally re-enrich these
-- three rows with the fixed extraction logic.

-- Placeholder template email: appeared on 2 prospect sites (Wix template default).
-- Safe to null globally -- "example@mysite.com" is never a legitimate business.
UPDATE prospects
   SET email = ''
 WHERE email = 'example@mysite.com';

-- Font foundry email pulled from a CSS font-license comment on the Sams
-- Handyman site (samsrenoconst.ca). Guard with the specific prospect id
-- in case a future prospect legitimately has this address -- we only want
-- to null the one row that got it via domain-mismatched html_regex.
UPDATE prospects
   SET email = ''
 WHERE email = 'info@indiantypefoundry.com'
   AND id = 'h6';
