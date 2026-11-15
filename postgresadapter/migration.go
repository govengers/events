package postgresadapter

// MigrationV1 creates the events tables: subscriptions, the delivery ledger and the inbox. Run it
// from one of the application's goose migrations, after jobs' postgresadapter.MigrationV1, in the
// schema the store's pool uses:
//
//	-- +goose Up
//	-- the content of postgresadapter.MigrationV1
//
// or from a goose Go migration with tx.ExecContext(ctx, postgresadapter.MigrationV1). Later
// versions of the module add MigrationV2 and so on; never edit an applied migration. It runs
// inside the migration's transaction (SET LOCAL); nothing in the library runs DDL at startup.
const MigrationV1 = `
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE govengers_event_subscriptions (
    subscriber        text NOT NULL CHECK (subscriber <> ''),
    name              text NOT NULL CHECK (name <> ''),
    topic             text NOT NULL CHECK (topic <> ''),
    endpoint          text,
    sealed_secret     bytea,
    queue             text,
    -- attempt and failure counters stay far below the int range
    -- squawk-ignore prefer-bigint-over-int
    maximum_attempts  integer,
    -- squawk-ignore prefer-bigint-over-int
    failed_deliveries integer NOT NULL DEFAULT 0 CHECK (failed_deliveries >= 0),
    disabled_at       timestamptz,
    disabled_by       text CHECK (disabled_by IN ('failures', 'admin')),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber, name),
    CHECK ((endpoint IS NULL) = (sealed_secret IS NULL)),
    CHECK ((endpoint IS NULL) = (queue IS NOT NULL AND maximum_attempts > 0)),
    CHECK ((disabled_at IS NULL) = (disabled_by IS NULL))
);

CREATE INDEX govengers_event_subscriptions_topic ON govengers_event_subscriptions (topic)
    WHERE disabled_at IS NULL;

CREATE TABLE govengers_event_deliveries (
    subscriber       text NOT NULL,
    subscription     text NOT NULL,
    event_id         text NOT NULL,
    topic            text NOT NULL,
    event            jsonb NOT NULL,
    status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'failed')),
    -- squawk-ignore prefer-bigint-over-int
    attempts         integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    -- squawk-ignore prefer-bigint-over-int
    last_status_code integer,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    PRIMARY KEY (subscriber, subscription, event_id),
    CHECK ((status = 'pending') = (finished_at IS NULL))
);

CREATE INDEX govengers_event_deliveries_finished ON govengers_event_deliveries (finished_at)
    WHERE status IN ('delivered', 'failed');

CREATE TABLE govengers_event_inbox (
    subscriber   text NOT NULL,
    subscription text NOT NULL,
    event_id     text NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber, subscription, event_id)
);

CREATE INDEX govengers_event_inbox_received ON govengers_event_inbox (received_at);
`
