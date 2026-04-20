-- v7 Enrichment observability: per-run metrics table + one stale-email fix
--
-- Runs in production by merging the enrichment-v3 PR. Safe to re-run (IF NOT
-- EXISTS + UPDATE with idempotent WHERE).

CREATE TABLE IF NOT EXISTS enrichment_runs (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ran_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trigger                   TEXT NOT NULL CHECK (trigger IN ('cron', 'manual', 'backfill')),
    duration_seconds          INTEGER,
    total_scanned             INTEGER NOT NULL DEFAULT 0,
    emails_found              INTEGER NOT NULL DEFAULT 0,
    emails_already_present    INTEGER NOT NULL DEFAULT 0,
    websites_discovered       INTEGER NOT NULL DEFAULT 0,
    websites_rejected_sanity  INTEGER NOT NULL DEFAULT 0,
    per_strategy              JSONB NOT NULL DEFAULT '{}'::jsonb,
    per_vertical              JSONB NOT NULL DEFAULT '{}'::jsonb,
    canary_pass               BOOLEAN NOT NULL DEFAULT FALSE,
    canary_failures           JSONB
);

CREATE INDEX IF NOT EXISTS enrichment_runs_ran_at_idx
    ON enrichment_runs (ran_at DESC);

-- Work Of Art CRM cleanup: the live site no longer shows
-- art@workofartbarber.ca; the only findable email today is the gmail.
-- This row is excluded from the backfill sweep (email != '') so it would
-- otherwise sit stale forever and the cold email agent would draft to a
-- dead address.
UPDATE prospects
   SET email = 'workofartbarbershop@gmail.com'
 WHERE id = 'work-of-art-mens-barber-shop-oakville'
   AND email = 'art@workofartbarber.ca';
