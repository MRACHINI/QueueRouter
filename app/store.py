"""SQLite persistence: configuration edited in the admin UI, users, audit log.

Small synchronous operations behind a lock; every call is well under a millisecond, so they
run directly on the event loop. Secrets are encrypted with Fernet; client keys are stored only
as SHA-256 hashes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import shutil
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, timedelta

from cryptography.fernet import Fernet, InvalidToken

from .answer_rules import (
    ANSWER_KINDS,
    CHECK_WHEN,
    DEFAULT_ANSWER_KINDS,
    LEARN_MODES,
    REJECTED_KEEP,
    RULES_PER_ALIAS,
    SPOT_DEFAULT,
    Rule,
    kinds_of,
    legacy_kinds,
    next_state,
    rule_key,
)
from .config import PRIORITIES, SECRET_TUNABLES, TUNABLES, ClientPolicy, Settings, parse_access_keys
from .edits import Edit
from .names import EVERY
from .taggers import PICKS, Line, Tag, Tagger, chain_json, chain_of, without
from .taggers import Step as TagStep

log = logging.getLogger("queuerouter")

MIGRATIONS: list[str] = [
    # 1: initial schema
    """
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE limit_pools (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        plan TEXT,
        max_concurrency INTEGER NOT NULL CHECK (max_concurrency >= 1),
        context_cap INTEGER,
        min_spacing_ms INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL
    );
    CREATE TABLE upstreams (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        preset TEXT,
        base_url TEXT NOT NULL,
        api_key_enc TEXT NOT NULL,
        pool_id INTEGER NOT NULL REFERENCES limit_pools(id),
        weight INTEGER NOT NULL DEFAULT 1,
        enabled INTEGER NOT NULL DEFAULT 1,
        paused INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL
    );
    CREATE TABLE clients (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        key_hash TEXT NOT NULL UNIQUE,
        key_prefix TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'normal',
        enabled INTEGER NOT NULL DEFAULT 1,
        expires_at REAL,
        notes TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL
    );
    CREATE TABLE admin_users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        can_view_content INTEGER NOT NULL DEFAULT 0,
        totp_secret_enc TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at REAL NOT NULL
    );
    CREATE TABLE admin_sessions (
        token_hash TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
        csrf TEXT NOT NULL,
        ip TEXT,
        created_at REAL NOT NULL,
        last_seen REAL NOT NULL,
        expires_at REAL NOT NULL
    );
    CREATE TABLE audit_log (
        id INTEGER PRIMARY KEY,
        ts REAL NOT NULL,
        actor TEXT NOT NULL,
        action TEXT NOT NULL,
        target TEXT NOT NULL DEFAULT '',
        detail TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX audit_log_ts ON audit_log(ts);
    """,
    # 2: per-client policies (JSON, see config.ClientPolicy)
    """
    ALTER TABLE clients ADD COLUMN policy TEXT NOT NULL DEFAULT '{}';
    """,
    # 3: model aliases
    """
    CREATE TABLE aliases (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        target_model TEXT NOT NULL,
        upstream_ids TEXT NOT NULL DEFAULT '[]',
        enabled INTEGER NOT NULL DEFAULT 1,
        notes TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL
    );
    """,
    # 4: request log (metadata only, never content) and daily usage rollup
    """
    CREATE TABLE requests (
        id TEXT PRIMARY KEY,
        ts REAL NOT NULL,
        client TEXT,
        priority TEXT,
        session TEXT,
        path TEXT,
        model TEXT,
        target_model TEXT,
        upstream TEXT,
        stream INTEGER NOT NULL DEFAULT 0,
        status INTEGER,
        outcome TEXT,
        position INTEGER,
        queue_ms INTEGER,
        upstream_ms INTEGER,
        total_ms INTEGER,
        retries INTEGER NOT NULL DEFAULT 0,
        prompt_tokens INTEGER,
        completion_tokens INTEGER,
        finish_reason TEXT,
        ip TEXT
    );
    CREATE INDEX requests_ts ON requests(ts);
    CREATE INDEX requests_client_ts ON requests(client, ts);
    CREATE TABLE usage_daily (
        day TEXT NOT NULL,
        client TEXT NOT NULL,
        model TEXT NOT NULL,
        upstream TEXT NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        prompt_tokens INTEGER NOT NULL DEFAULT 0,
        completion_tokens INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (day, client, model, upstream)
    );
    """,
    # 5: cost tracking - cached prompt tokens, plan prices, per-model price overrides
    """
    ALTER TABLE requests ADD COLUMN cached_tokens INTEGER;
    ALTER TABLE usage_daily ADD COLUMN cached_tokens INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE limit_pools ADD COLUMN monthly_price REAL;
    CREATE TABLE model_prices (
        model TEXT PRIMARY KEY,
        reference TEXT,
        input REAL,
        output REAL,
        cached_input REAL,
        updated_at REAL NOT NULL
    );
    """,
    # 6: per-alias parameter rules (JSON, see app/params.py)
    """
    ALTER TABLE aliases ADD COLUMN params TEXT NOT NULL DEFAULT '';
    """,
    # 7: fallback chains on aliases
    """
    ALTER TABLE aliases ADD COLUMN fallbacks TEXT NOT NULL DEFAULT '[]';
    ALTER TABLE aliases ADD COLUMN fallback_on TEXT NOT NULL DEFAULT '["unavailable", "errors", "context"]';
    """,
    # 8: request tags
    """
    ALTER TABLE requests ADD COLUMN tags TEXT;
    """,
    # 9: alerts: rule overrides, webhooks (URL and secret encrypted), in-app notifications
    """
    CREATE TABLE alert_rules (
        kind TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL,
        threshold REAL,
        window_s INTEGER,
        cooldown_s INTEGER
    );
    CREATE TABLE webhooks (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        kind TEXT NOT NULL,
        url TEXT NOT NULL,
        secret TEXT,
        min_level TEXT NOT NULL DEFAULT 'warning',
        enabled INTEGER NOT NULL DEFAULT 1,
        last_at REAL,
        last_ok INTEGER,
        last_error TEXT,
        created_at REAL NOT NULL
    );
    CREATE TABLE notifications (
        id INTEGER PRIMARY KEY,
        ts REAL NOT NULL,
        level TEXT NOT NULL,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL DEFAULT '',
        read INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX notifications_ts ON notifications (ts);
    """,
    # 10: two-factor login (TOTP secret encrypted, recovery codes hashed)
    """
    ALTER TABLE admin_users ADD COLUMN totp_secret TEXT;
    ALTER TABLE admin_users ADD COLUMN totp_last INTEGER NOT NULL DEFAULT -1;
    ALTER TABLE admin_users ADD COLUMN recovery_codes TEXT NOT NULL DEFAULT '[]';
    """,
    # 11: Responses API conversations for previous_response_id (encrypted, kept a few days)
    """
    CREATE TABLE stored_responses (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        created REAL NOT NULL,
        data TEXT NOT NULL
    );
    CREATE INDEX stored_responses_created ON stored_responses (created);
    """,
    # 12: async jobs, and OpenAI-style files + batches (request bodies and results encrypted)
    """
    CREATE TABLE jobs (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        batch_id TEXT,
        custom_id TEXT,
        status TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'low',
        endpoint TEXT NOT NULL,
        request TEXT NOT NULL,
        run_at REAL,
        deadline REAL,
        idle_only INTEGER NOT NULL DEFAULT 0,
        callback_url TEXT,
        callback_secret TEXT,
        callback_status TEXT,
        tags TEXT,
        created REAL NOT NULL,
        started REAL,
        finished REAL,
        attempts INTEGER NOT NULL DEFAULT 0,
        status_code INTEGER,
        result TEXT,
        error TEXT,
        model TEXT,
        prompt_tokens INTEGER,
        completion_tokens INTEGER
    );
    CREATE INDEX jobs_due ON jobs (status, run_at);
    CREATE INDEX jobs_batch ON jobs (batch_id);
    CREATE INDEX jobs_client ON jobs (client, created);
    CREATE TABLE files (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        purpose TEXT NOT NULL,
        filename TEXT NOT NULL,
        bytes INTEGER NOT NULL,
        created REAL NOT NULL,
        data TEXT NOT NULL
    );
    CREATE TABLE batches (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        input_file_id TEXT NOT NULL,
        completion_window TEXT NOT NULL,
        status TEXT NOT NULL,
        created REAL NOT NULL,
        expires_at REAL,
        finished_at REAL,
        cancelled_at REAL,
        output_file_id TEXT,
        error_file_id TEXT,
        total INTEGER NOT NULL,
        metadata TEXT
    );
    """,
    # 13: opt-in content logging (prompts and replies, encrypted)
    """
    CREATE TABLE request_content (
        id TEXT PRIMARY KEY,
        ts REAL NOT NULL,
        client TEXT NOT NULL,
        request TEXT NOT NULL,
        response TEXT NOT NULL,
        truncated INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX request_content_ts ON request_content (ts);
    """,
    # 14: recurring jobs (cron schedules that create jobs)
    """
    CREATE TABLE schedules (
        id TEXT PRIMARY KEY,
        client TEXT NOT NULL,
        name TEXT NOT NULL,
        cron TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        request TEXT NOT NULL,
        priority TEXT NOT NULL DEFAULT 'low',
        tags TEXT,
        idle_only INTEGER NOT NULL DEFAULT 0,
        deadline_s REAL,
        callback_url TEXT,
        callback_secret TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created REAL NOT NULL,
        next_run REAL,
        last_run REAL,
        last_job_id TEXT
    );
    CREATE INDEX schedules_due ON schedules (enabled, next_run);
    """,
    # 15: per-upstream parameter rules (e.g. strip fields a provider rejects) and retry limit
    """
    ALTER TABLE upstreams ADD COLUMN params TEXT NOT NULL DEFAULT '';
    ALTER TABLE upstreams ADD COLUMN max_retries INTEGER;
    ALTER TABLE requests ADD COLUMN error TEXT;
    """,
    # 16: time to first byte per request
    """
    ALTER TABLE requests ADD COLUMN ttft_ms INTEGER;
    """,
    # 17: the calling app's User-Agent per request
    """
    ALTER TABLE requests ADD COLUMN user_agent TEXT;
    """,
    # 18: outgoing User-Agent default per upstream (clients can override it)
    """
    ALTER TABLE upstreams ADD COLUMN ua_mode TEXT NOT NULL DEFAULT 'keep';
    ALTER TABLE upstreams ADD COLUMN ua_template TEXT NOT NULL DEFAULT '';
    """,
    # 19: the User-Agent QueueRouter sent upstream, when it differs from the app's own
    """
    ALTER TABLE requests ADD COLUMN sent_user_agent TEXT;
    """,
    # 20: model context sizes beyond what providers report: manual (upstream_id 0 = every upstream) and
    # learned from a provider's own "too long" error (per upstream)
    """
    CREATE TABLE model_limits (
        upstream_id INTEGER NOT NULL DEFAULT 0,
        model       TEXT NOT NULL,
        source      TEXT NOT NULL,
        context     INTEGER,
        output      INTEGER,
        note        TEXT NOT NULL DEFAULT '',
        updated_at  REAL NOT NULL,
        PRIMARY KEY (upstream_id, model, source)
    );
    """,
    # 21: compaction per alias (off / trim / summarise, the model that writes summaries, the context size
    # told to apps) and what was done to each request
    """
    ALTER TABLE aliases ADD COLUMN compact TEXT NOT NULL DEFAULT 'off';
    ALTER TABLE aliases ADD COLUMN compact_model TEXT NOT NULL DEFAULT '';
    ALTER TABLE aliases ADD COLUMN virtual_context INTEGER;
    ALTER TABLE requests ADD COLUMN compaction TEXT;
    """,
    # 22: conversation summaries (compaction, summarise mode), when "Save summaries" is on; encrypted
    """
    CREATE TABLE compaction_summaries (
        key        TEXT PRIMARY KEY,
        client     TEXT NOT NULL,
        data       TEXT NOT NULL,
        updated_at REAL NOT NULL
    );
    CREATE INDEX compaction_summaries_client ON compaction_summaries (client);
    """,
    # 23: a separate parallel limit for /v1/embeddings per pool (blank = shared with chat)
    """
    ALTER TABLE limit_pools ADD COLUMN embedding_concurrency INTEGER;
    """,
    # 24: model council per alias (off / pick / combine, members, judge, when) and what it did per request
    """
    ALTER TABLE aliases ADD COLUMN council TEXT NOT NULL DEFAULT 'off';
    ALTER TABLE aliases ADD COLUMN council_members TEXT NOT NULL DEFAULT '[]';
    ALTER TABLE aliases ADD COLUMN council_judge TEXT NOT NULL DEFAULT '';
    ALTER TABLE aliases ADD COLUMN council_when TEXT NOT NULL DEFAULT 'planning';
    ALTER TABLE requests ADD COLUMN council TEXT;
    """,
    # 25: what each client sent about itself (identifying headers and body fields, as JSON)
    """
    ALTER TABLE requests ADD COLUMN client_headers TEXT;
    """,
    # 26: which models an upstream offers: '' = all, else JSON {"offer_new": bool, "models": [...]} (hidden
    # ones when offer_new, else the only ones offered)
    """
    ALTER TABLE upstreams ADD COLUMN model_filter TEXT NOT NULL DEFAULT '';
    """,
    # 27: requests QueueRouter makes for another one (council members and judge, summaries): their role and
    # the request they served
    """
    ALTER TABLE requests ADD COLUMN role TEXT;
    ALTER TABLE requests ADD COLUMN parent TEXT;
    """,
    # 28: the administrator's own instructions for a council's judge (added to the built-in ones)
    """
    ALTER TABLE aliases ADD COLUMN council_instructions TEXT NOT NULL DEFAULT '';
    """,
    # 29: council members may search the web (QueueRouter runs the searches): on/off, searches per member and
    # step, results per search
    """
    ALTER TABLE aliases ADD COLUMN council_search INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE aliases ADD COLUMN council_search_max INTEGER NOT NULL DEFAULT 3;
    ALTER TABLE aliases ADD COLUMN council_search_results INTEGER NOT NULL DEFAULT 5;
    """,
    # 30: orchestrator budgets per alias (rounds, member tasks, seconds) and web page reading for its tools
    """
    ALTER TABLE aliases ADD COLUMN orch_rounds INTEGER NOT NULL DEFAULT 6;
    ALTER TABLE aliases ADD COLUMN orch_tasks INTEGER NOT NULL DEFAULT 8;
    ALTER TABLE aliases ADD COLUMN orch_seconds INTEGER NOT NULL DEFAULT 300;
    ALTER TABLE aliases ADD COLUMN council_fetch INTEGER NOT NULL DEFAULT 0;
    """,
    # 31: member roles per council / orchestrator alias ("member = role: instructions" lines), and the timeline
    # of a council's or orchestrator's work per request (Requests > trace page)
    """
    ALTER TABLE aliases ADD COLUMN council_roles TEXT NOT NULL DEFAULT '';
    ALTER TABLE requests ADD COLUMN timeline TEXT;
    """,
    # 32: the administrator's own strength score per model (orders the opencode model list; app/ranking.py)
    """
    CREATE TABLE model_scores (
        model TEXT PRIMARY KEY,
        score REAL NOT NULL,
        updated_at REAL NOT NULL
    );
    """,
    # 33: the administrator's own thinking levels per model ("none,low,high", or "-" for no selector; app/thinking.py)
    """
    CREATE TABLE model_thinking (
        model TEXT PRIMARY KEY,
        levels TEXT NOT NULL,
        updated_at REAL NOT NULL
    );
    """,
    # 34: the provider's own timings per request (Response times page; app/main.py RequestLog)
    """
    ALTER TABLE requests ADD COLUMN headers_ms INTEGER;
    ALTER TABLE requests ADD COLUMN first_token_ms INTEGER;
    ALTER TABLE requests ADD COLUMN answer_ms INTEGER;
    ALTER TABLE requests ADD COLUMN tokens_per_s REAL;
    ALTER TABLE requests ADD COLUMN stall_ms INTEGER;
    ALTER TABLE requests ADD COLUMN backoff_ms INTEGER;
    """,
    # 35: client keys kept encrypted too, so an admin can show one again (NULL: made before, only the hash known)
    """
    ALTER TABLE clients ADD COLUMN key_secret TEXT;
    """,
    # 36: a model on a provider is one entry: its custom name, and prices, scores and thinking levels per upstream
    # (upstream_id 0: every provider, as they were set before); how far a cancelled request had got
    """
    CREATE TABLE model_names (
        upstream_id INTEGER NOT NULL,
        model TEXT NOT NULL,
        name TEXT NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (upstream_id, model)
    );
    CREATE TABLE model_prices_new (
        upstream_id INTEGER NOT NULL DEFAULT 0,
        model TEXT NOT NULL,
        reference TEXT,
        input REAL,
        output REAL,
        cached_input REAL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (upstream_id, model)
    );
    INSERT INTO model_prices_new (upstream_id, model, reference, input, output, cached_input, updated_at)
        SELECT 0, model, reference, input, output, cached_input, updated_at FROM model_prices;
    DROP TABLE model_prices;
    ALTER TABLE model_prices_new RENAME TO model_prices;
    CREATE TABLE model_scores_new (
        upstream_id INTEGER NOT NULL DEFAULT 0,
        model TEXT NOT NULL,
        score REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (upstream_id, model)
    );
    INSERT INTO model_scores_new (upstream_id, model, score, updated_at)
        SELECT 0, model, score, updated_at FROM model_scores;
    DROP TABLE model_scores;
    ALTER TABLE model_scores_new RENAME TO model_scores;
    CREATE TABLE model_thinking_new (
        upstream_id INTEGER NOT NULL DEFAULT 0,
        model TEXT NOT NULL,
        levels TEXT NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (upstream_id, model)
    );
    INSERT INTO model_thinking_new (upstream_id, model, levels, updated_at)
        SELECT 0, model, levels, updated_at FROM model_thinking;
    DROP TABLE model_thinking;
    ALTER TABLE model_thinking_new RENAME TO model_thinking;
    ALTER TABLE requests ADD COLUMN stopped TEXT;
    """,
    # 37: prompt tokens written to the provider's cache (Claude bills them above the input price)
    """
    ALTER TABLE requests ADD COLUMN cache_write_tokens INTEGER;
    ALTER TABLE usage_daily ADD COLUMN cache_write_tokens INTEGER NOT NULL DEFAULT 0;
    """,
    # 38: client groups, with the models and limits of their clients (config.with_group). A client's own model list
    # becomes a group (clients with the same list share one, named after the first), the others go in "All models",
    # and a client's "delay" and "warn" (the defaults) go, so the group's apply: nobody's access or limits change
    """
    CREATE TABLE client_groups (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        policy TEXT NOT NULL DEFAULT '{}',
        notes TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL
    );
    ALTER TABLE clients ADD COLUMN group_id INTEGER REFERENCES client_groups(id);
    INSERT INTO client_groups (name, created_at) VALUES ('All models', CAST(strftime('%s', 'now') AS REAL));
    INSERT INTO client_groups (name, policy, created_at)
        SELECT MIN(name), json_object('allowed_models', json(json_extract(policy, '$.allowed_models'))),
            CAST(strftime('%s', 'now') AS REAL)
        FROM clients
        WHERE json_valid(policy) AND json_array_length(policy, '$.allowed_models') > 0
        GROUP BY json_extract(policy, '$.allowed_models');
    UPDATE clients SET group_id = (
        SELECT g.id FROM client_groups g
        WHERE g.policy = json_object('allowed_models', json(json_extract(clients.policy, '$.allowed_models')))
    ) WHERE json_valid(policy) AND json_array_length(policy, '$.allowed_models') > 0;
    UPDATE clients SET group_id = (SELECT id FROM client_groups WHERE name = 'All models') WHERE group_id IS NULL;
    UPDATE clients SET policy = json_remove(policy, '$.allowed_models') WHERE json_valid(policy);
    UPDATE clients SET policy = json_remove(policy, '$.on_limit')
        WHERE json_valid(policy) AND json_extract(policy, '$.on_limit') = 'delay';
    UPDATE clients SET policy = json_remove(policy, '$.loop_action')
        WHERE json_valid(policy) AND json_extract(policy, '$.loop_action') = 'warn';
    """,
    # 39: answer checks per alias (a checker model and its instructions; app/answer_check.py), and per request
    # why it moved to a fallback model and what the checks said
    """
    ALTER TABLE aliases ADD COLUMN check_answers INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE aliases ADD COLUMN check_model TEXT NOT NULL DEFAULT '';
    ALTER TABLE aliases ADD COLUMN check_instructions TEXT NOT NULL DEFAULT '';
    ALTER TABLE requests ADD COLUMN fallback TEXT;
    ALTER TABLE requests ADD COLUMN answer_check TEXT;
    """,
    # 40: usage per hour too (the Usage page's last 24 hours), filled from the request log for as far back as it
    # goes, the way record_requests adds to the daily rollup; Store.hourly_usage_from says from which day it's whole
    """
    CREATE TABLE usage_hourly (
        hour TEXT NOT NULL,
        client TEXT NOT NULL,
        model TEXT NOT NULL,
        upstream TEXT NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        errors INTEGER NOT NULL DEFAULT 0,
        prompt_tokens INTEGER NOT NULL DEFAULT 0,
        completion_tokens INTEGER NOT NULL DEFAULT 0,
        cached_tokens INTEGER NOT NULL DEFAULT 0,
        cache_write_tokens INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (hour, client, model, upstream)
    );
    INSERT INTO usage_hourly
        SELECT strftime('%Y-%m-%dT%H', CAST(ts AS INTEGER), 'unixepoch'), COALESCE(NULLIF(client, ''), '-'),
            COALESCE(NULLIF(target_model, ''), NULLIF(model, ''), '-'), COALESCE(NULLIF(upstream, ''), '-'),
            COUNT(*), SUM(outcome IS NOT 'done'), SUM(COALESCE(prompt_tokens, 0)),
            SUM(COALESCE(completion_tokens, 0)), SUM(COALESCE(cached_tokens, 0)),
            SUM(COALESCE(cache_write_tokens, 0))
        FROM requests WHERE outcome IS NOT 'refused' AND COALESCE(council, '') = ''
        GROUP BY 1, 2, 3, 4;
    """,
    # 41: requests forwarded from alias to alias: the way they came ("a → b"; the app asked for a)
    """
    ALTER TABLE requests ADD COLUMN via TEXT;
    """,
    # 42: prompt edits (app/edits.py): the edits, those a "learn" edit's model made (parent_id), the paragraphs it
    # has read, kept rewrites (encrypted), which edits an alias uses; per request what they did, and with content
    # logging on the texts before and after (encrypted)
    """
    CREATE TABLE prompt_edits (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER REFERENCES prompt_edits(id) ON DELETE CASCADE,
        name TEXT NOT NULL DEFAULT '',
        kind TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'on',
        settings TEXT NOT NULL DEFAULT '{}',
        notes TEXT NOT NULL DEFAULT '',
        hits INTEGER NOT NULL DEFAULT 0,
        last_hit REAL,
        created_at REAL NOT NULL
    );
    CREATE UNIQUE INDEX prompt_edits_name ON prompt_edits (name) WHERE parent_id IS NULL;
    CREATE INDEX prompt_edits_parent ON prompt_edits (parent_id);
    CREATE TABLE prompt_edit_seen (
        edit_id INTEGER NOT NULL REFERENCES prompt_edits(id) ON DELETE CASCADE,
        hash TEXT NOT NULL,
        PRIMARY KEY (edit_id, hash)
    ) WITHOUT ROWID;
    CREATE TABLE prompt_edit_results (
        edit_id INTEGER NOT NULL REFERENCES prompt_edits(id) ON DELETE CASCADE,
        hash TEXT NOT NULL,
        result TEXT NOT NULL,
        used_at REAL NOT NULL,
        PRIMARY KEY (edit_id, hash)
    );
    ALTER TABLE aliases ADD COLUMN edits TEXT NOT NULL DEFAULT '[]';
    ALTER TABLE requests ADD COLUMN edits TEXT;
    CREATE INDEX requests_edited ON requests (ts) WHERE edits IS NOT NULL;
    ALTER TABLE request_content ADD COLUMN edits TEXT;
    """,
    # 43: an alias's display name (opencode's model list, the Models page); its name stays the ID apps send
    """
    ALTER TABLE aliases ADD COLUMN label TEXT NOT NULL DEFAULT '';
    """,
    # 44: answer rules (app/answer_rules.py): an alias's own checks of its answers by text or pattern, attached in
    # order; and what an alias's answer check reads (text answers, ones that call tools, empty ones)
    """
    CREATE TABLE answer_rules (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        kind TEXT NOT NULL,
        find TEXT NOT NULL,
        match_case INTEGER NOT NULL DEFAULT 0,
        scope TEXT NOT NULL DEFAULT 'start',
        enabled INTEGER NOT NULL DEFAULT 1,
        notes TEXT NOT NULL DEFAULT '',
        hits INTEGER NOT NULL DEFAULT 0,
        last_hit REAL,
        created_at REAL NOT NULL
    );
    CREATE UNIQUE INDEX answer_rules_name ON answer_rules (name);
    ALTER TABLE aliases ADD COLUMN answer_rules TEXT NOT NULL DEFAULT '[]';
    ALTER TABLE aliases ADD COLUMN check_kinds TEXT NOT NULL DEFAULT '["text", "empty"]';
    """,
    # 45: what a rule does (fail, pass, pass keeping the free checks, ask the checker), "doesn't", watching; rules an
    # alias's rule maker made (app/rule_maker.py) and how often they agreed with the checker; when an alias asks its
    # checker, its spot checks and rule maker; the checked answers kept for the rule maker (encrypted)
    """
    ALTER TABLE answer_rules ADD COLUMN action TEXT NOT NULL DEFAULT 'fail';
    ALTER TABLE answer_rules ADD COLUMN negate INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE answer_rules ADD COLUMN state TEXT NOT NULL DEFAULT 'on';
    UPDATE answer_rules SET state = 'off' WHERE enabled = 0;
    ALTER TABLE answer_rules ADD COLUMN made_by INTEGER;
    ALTER TABLE answer_rules ADD COLUMN why TEXT NOT NULL DEFAULT '';
    ALTER TABLE answer_rules ADD COLUMN sample_fails INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE answer_rules ADD COLUMN sample_passes INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE answer_rules ADD COLUMN agreed INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE answer_rules ADD COLUMN disagreed INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE answer_rules ADD COLUMN recent TEXT NOT NULL DEFAULT '';
    ALTER TABLE answer_rules ADD COLUMN awaiting INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE answer_rules ADD COLUMN note TEXT NOT NULL DEFAULT '';
    ALTER TABLE aliases ADD COLUMN check_when TEXT NOT NULL DEFAULT 'undecided';
    ALTER TABLE aliases ADD COLUMN spot_checks INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE aliases ADD COLUMN learn_rules TEXT NOT NULL DEFAULT 'off';
    ALTER TABLE aliases ADD COLUMN learn_model TEXT NOT NULL DEFAULT '';
    ALTER TABLE aliases ADD COLUMN learn_pass INTEGER NOT NULL DEFAULT 0;
    CREATE TABLE answer_samples (
        id INTEGER PRIMARY KEY,
        alias_id INTEGER NOT NULL,
        verdict TEXT NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        answer TEXT NOT NULL,
        created_at REAL NOT NULL
    );
    CREATE INDEX answer_samples_alias ON answer_samples (alias_id, verdict, created_at);
    CREATE TABLE rule_learning (
        alias_id INTEGER PRIMARY KEY,
        last_run REAL,
        new_fails INTEGER NOT NULL DEFAULT 0,
        new_passes INTEGER NOT NULL DEFAULT 0,
        note TEXT NOT NULL DEFAULT '',
        rejected TEXT NOT NULL DEFAULT '[]'
    );
    """,
    # 46: an alias can turn its checker model's verdict around (its PASS fails the answer, its FAIL passes it)
    """
    ALTER TABLE aliases ADD COLUMN check_flip INTEGER NOT NULL DEFAULT 0;
    """,
    # 47: tags and taggers (app/taggers.py): an alias's chain of taggers replaces its checker model (each alias's
    # checker becomes a tagger of its own: _convert_checkers), a rule can give a tag, and a request keeps the tags its
    # answers got
    """
    CREATE TABLE answer_tags (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        meaning TEXT NOT NULL DEFAULT '',
        given INTEGER NOT NULL DEFAULT 0,
        last_given REAL,
        created_at REAL NOT NULL
    );
    CREATE UNIQUE INDEX answer_tags_name ON answer_tags (name);
    CREATE TABLE taggers (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        model TEXT NOT NULL DEFAULT '',
        instructions TEXT NOT NULL DEFAULT '',
        prompt TEXT NOT NULL DEFAULT '',
        tags TEXT NOT NULL DEFAULT '[]',
        pick TEXT NOT NULL DEFAULT 'any',
        reads TEXT NOT NULL DEFAULT '[]',
        sees_message INTEGER NOT NULL DEFAULT 1,
        sees_turns INTEGER NOT NULL DEFAULT 0,
        sees_system INTEGER NOT NULL DEFAULT 0,
        asks_reason INTEGER NOT NULL DEFAULT 1,
        notes TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL
    );
    CREATE UNIQUE INDEX taggers_name ON taggers (name);
    ALTER TABLE aliases ADD COLUMN tag_chain TEXT NOT NULL DEFAULT '[]';
    ALTER TABLE answer_rules ADD COLUMN tag_id INTEGER;
    ALTER TABLE requests ADD COLUMN answer_tags TEXT;
    """,
    # 48: a thinking level lowered to one the model has (app/thinking.py nearest), per request
    """
    ALTER TABLE requests ADD COLUMN thinking TEXT;
    """,
]
CHECKERS_CONVERTED = "checkers_converted"  # meta: each alias's checker model became a tagger (_convert_checkers)
CHECKER_TAGS = (  # what a converted checker gives: exactly one of these
    ("ok", "the answer is acceptable under the instructions"),
    ("bad", "the answer isn't acceptable under the instructions"),
)
HOURLY_USAGE = 40  # the migration that added usage_hourly

JOB_DONE = ("succeeded", "failed", "cancelled", "expired")
# Tables whose rows belong to a client (by name): deleted with it, renamed with it.
CLIENT_DATA_TABLES = (
    "jobs",
    "files",
    "batches",
    "schedules",
    "stored_responses",
    "request_content",
    "compaction_summaries",
)

CALLS_WITHIN_S = 6 * 3600  # a call for a request ends this close to it, at most (longest a request is tracked)
REQUEST_COLUMNS = (
    "id",
    "ts",
    "client",
    "priority",
    "session",
    "path",
    "model",
    "target_model",
    "upstream",
    "stream",
    "status",
    "outcome",
    "position",
    "queue_ms",
    "upstream_ms",
    "total_ms",
    "retries",
    "prompt_tokens",
    "completion_tokens",
    "cached_tokens",
    "cache_write_tokens",
    "stopped",
    "tags",
    "finish_reason",
    "ip",
    "error",
    "ttft_ms",
    "user_agent",
    "sent_user_agent",
    "compaction",
    "council",
    "client_headers",
    "role",
    "parent",
    "timeline",
    "headers_ms",
    "first_token_ms",
    "answer_ms",
    "tokens_per_s",
    "stall_ms",
    "backoff_ms",
    "fallback",
    "answer_check",
    "via",
    "edits",
    "answer_tags",
    "thinking",
)


def storable(value: object) -> object:
    """``value``, with any lone surrogate in a string (JSON allows "\\ud800") replaced: SQLite takes UTF-8 only."""
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            return value.encode("utf-8", "replace").decode("utf-8")
    return value


_UPSERT = "INSERT INTO {table} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value"
# a finished request added to the daily or hourly usage totals
_ADD_USAGE = (
    "INSERT INTO {table} ({slot}, client, model, upstream, requests, errors, prompt_tokens, completion_tokens, "
    "cached_tokens, cache_write_tokens) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?) "
    "ON CONFLICT({slot}, client, model, upstream) DO UPDATE SET requests = requests + 1, "
    "errors = errors + excluded.errors, prompt_tokens = prompt_tokens + excluded.prompt_tokens, "
    "completion_tokens = completion_tokens + excluded.completion_tokens, "
    "cached_tokens = cached_tokens + excluded.cached_tokens, "
    "cache_write_tokens = cache_write_tokens + excluded.cache_write_tokens"
)
HOUR_FORMAT = "%Y-%m-%dT%H"  # usage_hourly's hours (UTC)


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def new_client_key() -> str:
    return "qr-" + secrets.token_urlsafe(32)


@dataclass(frozen=True)
class Pool:
    id: int
    name: str
    plan: str | None
    max_concurrency: int
    context_cap: int | None
    min_spacing_ms: int
    monthly_price: float | None = None  # plan fee, for "savings vs official prices"
    embedding_concurrency: int | None = None  # /v1/embeddings get their own slots (None = shared with chat)


@dataclass(frozen=True)
class Upstream:
    id: int
    name: str
    preset: str | None
    base_url: str
    pool_id: int
    weight: int
    enabled: bool
    paused: bool
    params: str = ""  # parameter rules applied to every request sent here (app/params.py)
    max_retries: int | None = None  # retries here after 429 / 5xx / errors (None = the client's / global budget)
    ua_mode: str = "keep"  # outgoing User-Agent default for requests here: keep | remove | template
    ua_template: str = ""
    model_filter: str = ""  # which models it offers (see Catalogs.filters); "" = all


@dataclass(frozen=True)
class Client:
    id: int
    name: str
    key_hash: str
    key_prefix: str
    priority: str
    enabled: bool
    expires_at: float | None
    notes: str
    created_at: float
    policy: ClientPolicy = ClientPolicy()  # its own: the group's apply where it has none (config.with_group)
    key_kept: bool = False  # its key can be shown again (made or renewed since 0.42; older ones: only the hash)
    group_id: int | None = None

    @property
    def expired(self) -> bool:
        return self.expires_at is not None and self.expires_at <= time.time()


@dataclass(frozen=True)
class Group:
    """Clients with the same models and limits (a plan). Every client is in one."""

    id: int
    name: str
    policy: ClientPolicy  # allowed_models (empty = every model, new ones too) and the limits its clients get
    notes: str
    created_at: float


@dataclass(frozen=True)
class Webhook:
    id: int
    name: str
    kind: str  # discord | slack | ntfy | json
    url: str
    secret: str | None  # HMAC key for "json" webhooks
    min_level: str
    enabled: bool
    last_at: float | None = None
    last_ok: bool | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class Alias:
    id: int
    name: str
    target_model: str
    upstream_ids: tuple[int, ...]  # empty = any upstream
    enabled: bool
    notes: str
    params: str = ""  # parameter rules JSON (app/params.py)
    fallbacks: tuple[str, ...] = ()  # backup models, tried in order
    fallback_on: tuple[str, ...] = ("unavailable", "errors", "context")
    compact: str = "off"  # off / trim / summarise: shrink conversations too big for the model (app/compaction.py)
    compact_model: str = ""  # writes the summaries ("summarise"); blank = the target model
    virtual_context: int | None = None  # context size told to apps (opencode); None = the real one
    council: str = "off"  # off / pick / combine: several models answer, a judge decides (app/council.py)
    council_members: tuple[str, ...] = ()
    council_judge: str = ""  # blank = the target model
    council_when: str = "planning"  # planning / always
    council_instructions: str = ""  # added to the judge's built-in instruction
    council_search: bool = False  # members may search the web (app/websearch.py)
    council_search_max: int = 3  # searches per member per step
    council_search_results: int = 5  # results per search
    council_fetch: bool = False  # the orchestrator (and its members) may read web pages
    orch_rounds: int = 6  # orchestrator budget per step: rounds of delegation...
    orch_tasks: int = 8  # ...member tasks...
    orch_seconds: int = 300  # ...and time
    council_roles: str = ""  # "member = role: instructions" per line (app/council.py parse_roles)
    edits: tuple[int, ...] = ()  # prompt edits, in order (app/edits.py)
    label: str = ""  # display name (opencode, the Models page); blank = the name. Apps always send the name
    answer_rules: tuple[int, ...] = ()  # its answer rules, in order (app/answer_rules.py)
    check_kinds: tuple[str, ...] = DEFAULT_ANSWER_KINDS  # the answer types its answer check reads
    check_when: str = "undecided"  # its taggers: for every answer no rule decided, or only those "asked"
    spot_checks: int = SPOT_DEFAULT  # % of answers decided without the taggers that they read too, in the background
    learn_rules: str = "off"  # its rule maker (app/rule_maker.py): off / propose / auto
    learn_model: str = ""  # the rule maker's model; blank = its first tagger's
    learn_pass: bool = False  # rules that pass answers may turn on by themselves too
    tag_chain: tuple[TagStep, ...] = ()  # its taggers, in order, and what it does with their tags (app/taggers.py)

    @property
    def checks(self) -> bool:
        """It has taggers."""
        return bool(self.tag_chain)

    @property
    def shown(self) -> str:
        return self.label or self.name


ROLES = ("viewer", "operator", "admin", "owner")  # ascending privilege


@dataclass(frozen=True)
class AdminUser:
    id: int
    username: str
    role: str
    can_view_content: bool
    enabled: bool
    created_at: float
    has_2fa: bool = False

    def can(self, role: str) -> bool:
        """Whether this user has at least ``role``."""
        return ROLES.index(self.role) >= ROLES.index(role)


def _admin(row: sqlite3.Row) -> AdminUser:
    return AdminUser(
        row["id"],
        row["username"],
        row["role"],
        bool(row["can_view_content"]),
        bool(row["enabled"]),
        row["created_at"],
        row["totp_secret"] is not None,
    )


class DataDirError(RuntimeError):
    """The data directory can't be used; the message says how to fix it."""


def check_data_dir(data_dir: str) -> None:
    """Fail early, with a fix, when the data directory (or a file in it) isn't writable.

    Typical cause: a bind mount or pre-existing volume owned by root, while the container
    runs as an unprivileged user.
    """
    try:
        os.makedirs(data_dir, exist_ok=True)
    except PermissionError:
        pass  # reported below with the ownership details
    uid, gid = os.getuid(), os.getgid()
    problems = []
    if not os.access(data_dir, os.W_OK | os.X_OK):
        problems.append(data_dir)
    elif os.path.isdir(data_dir):
        for name in os.listdir(data_dir):
            path = os.path.join(data_dir, name)
            if os.path.isfile(path) and not os.access(path, os.R_OK | os.W_OK):
                problems.append(path)
    if not problems:
        return
    try:
        st = os.stat(data_dir)
        owner_uid, owner = st.st_uid, f"{st.st_uid}:{st.st_gid}"
    except OSError:
        owner_uid, owner = None, "unknown"
    run_as_owner = (
        f'  - or run the container as the owner: add  user: "{owner}"  to the service\n'
        if owner_uid not in (None, 0)  # never suggest running as root
        else ""
    )
    raise DataDirError(
        f"{', '.join(problems)} is not writable by this container (running as {uid}:{gid}; "
        f"{data_dir} is owned by {owner}). Fix it with one of:\n"
        "  - let the container fix it: remove `user:` from the service and add\n"
        "    cap_add: [CHOWN, SETUID, SETGID]  (set PUID/PGID to choose the owner)\n"
        f"  - named volume:  docker run --rm -v <volume>:{data_dir} alpine chown -R {uid}:{gid} {data_dir}\n"
        f"  - bind mount:    sudo chown -R {uid}:{gid} /path/on/host\n"
        f"{run_as_owner}"
        "See README > Data volume permissions."
    )


def load_fernet(data_dir: str, secret_key: str | None) -> Fernet:
    """Fernet from SECRET_KEY, or from <data_dir>/secret.key (created on first start)."""
    if secret_key:
        return Fernet(secret_key.encode())
    path = os.path.join(data_dir, "secret.key")
    if not os.path.exists(path):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(Fernet.generate_key())
        log.info(json.dumps({"event": "secret_key_created", "path": path}))
    with open(path, "rb") as f:
        return Fernet(f.read().strip())


class Store:
    def __init__(self, path: str, fernet: Fernet) -> None:
        self.path = path
        self.fernet = fernet
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA busy_timeout=5000")
        # The admin pages' long reads get their own connection: WAL lets them run beside writes, and they don't
        # hold the lock requests and the background loop wait for.
        self._read_lock = threading.Lock()
        self._reader = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._reader.row_factory = sqlite3.Row
        self._reader.execute("PRAGMA query_only=ON")
        self._reader.execute("PRAGMA busy_timeout=5000")

    @classmethod
    def open(cls, data_dir: str, secret_key: str | None) -> Store:
        check_data_dir(data_dir)
        from .backup import apply_staged_restore  # imported here: backup imports this module

        apply_staged_restore(data_dir)
        store = cls(os.path.join(data_dir, "queuerouter.db"), load_fernet(data_dir, secret_key))
        store.migrate()
        return store

    def close(self) -> None:
        with self._read_lock:
            self._reader.close()
        with self._lock:
            self._db.close()

    def backup_to(self, dest: str) -> None:
        """A consistent copy of the live database (SQLite online backup)."""
        target = sqlite3.connect(dest)
        try:
            with self._lock:
                self._db.backup(target)
        finally:
            target.close()

    # -- plumbing ------------------------------------------------------------------------

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._db.execute("COMMIT")

    def query(self, sql: str, params: tuple | dict = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def read(self, sql: str, params: tuple | dict = ()) -> list[sqlite3.Row]:
        """Like query, on the admin pages' own connection (see __init__)."""
        with self._read_lock:
            return self._reader.execute(sql, params).fetchall()

    def encrypt(self, value: str) -> str:
        return self.fernet.encrypt(value.encode("utf-8", "replace")).decode()  # a lone surrogate becomes "?"

    def decrypt(self, token: str) -> str:
        try:
            return self.fernet.decrypt(token.encode()).decode()
        except InvalidToken as exc:
            raise RuntimeError("cannot decrypt a stored secret: SECRET_KEY / secret.key changed?") from exc

    def reveal(self, value: str | None) -> str | None:
        """A value stored encrypted, or as plain text by an older version (returned as is)."""
        if not value:
            return value
        try:
            return self.fernet.decrypt(value.encode()).decode()
        except InvalidToken:
            return value

    def schema_version(self) -> int:
        with self._lock:
            return self._db.execute("PRAGMA user_version").fetchone()[0]

    def migrate(self) -> None:
        current = self.schema_version()
        if current > len(MIGRATIONS):
            raise RuntimeError(f"database schema v{current} is newer than this QueueRouter (v{len(MIGRATIONS)})")
        if current == len(MIGRATIONS):
            self._convert_checkers()  # a start after a conversion that failed half-way tries again
            return
        if current > 0 and os.path.exists(self.path):
            backup_dir = os.path.join(os.path.dirname(self.path), "backups")
            os.makedirs(backup_dir, exist_ok=True)
            backup = os.path.join(backup_dir, f"queuerouter-v{current}-{int(time.time())}.db")
            with self._lock:
                self._db.execute("PRAGMA wal_checkpoint(FULL)")
            shutil.copy2(self.path, backup)
            log.info(json.dumps({"event": "db_backup", "path": backup}))
        for version in range(current + 1, len(MIGRATIONS) + 1):
            with self._lock:
                self._db.executescript(f"BEGIN;\n{MIGRATIONS[version - 1]}\nPRAGMA user_version={version};\nCOMMIT;")
            log.info(json.dumps({"event": "db_migrated", "version": version}))
        if current < HOURLY_USAGE:
            self._start_hourly_usage()
        self._convert_checkers()

    def _convert_checkers(self) -> None:
        """Once (meta CHECKERS_CONVERTED): each alias's checker model (0.39-0.76) becomes a tagger of its own, with its
        model and instructions, giving exactly one of the tags "ok" and "bad" (made if there are none), and the alias's
        chain fails an answer tagged "bad" ("ok", when the alias flipped its checker's verdict); what each alias's
        check read becomes the answer types those kinds held. One transaction."""
        if self.get_meta(CHECKERS_CONVERTED):
            return
        now = time.time()
        with self.tx() as db:
            aliases = db.execute("SELECT * FROM aliases").fetchall()
            checkers = [a for a in aliases if a["check_answers"] and (a["check_instructions"] or "").strip()]
            checking = {a["id"] for a in checkers}
            tags: dict[str, int] = {}
            for name, meaning in CHECKER_TAGS if checkers else ():
                row = db.execute("SELECT id FROM answer_tags WHERE name = ?", (name,)).fetchone()
                if row is None:
                    cur = db.execute(
                        "INSERT INTO answer_tags (name, meaning, created_at) VALUES (?, ?, ?)", (name, meaning, now)
                    )
                    tags[name] = cur.lastrowid
                else:
                    tags[name] = row["id"]
            taken = {r["name"] for r in db.execute("SELECT name FROM taggers").fetchall()}
            for alias in aliases:
                raw = json.loads(alias["check_kinds"] or "[]")
                newer = any(k in ANSWER_KINDS and k != "text" for k in raw)  # written by 0.77 already
                kinds = kinds_of(raw) if newer else legacy_kinds(raw)
                chain = alias["tag_chain"] or "[]"
                if alias["id"] in checking:
                    name, n = f"{alias['name']} checker"[:60], 2
                    while name in taken:
                        name, n = f"{alias['name'][:52]} checker {n}", n + 1
                    taken.add(name)
                    cur = db.execute(
                        "INSERT INTO taggers (name, model, instructions, tags, pick, reads, created_at) "
                        "VALUES (?, ?, ?, ?, 'one', ?, ?)",
                        (
                            name,
                            alias["check_model"] or "",
                            alias["check_instructions"].strip(),
                            json.dumps([tags["ok"], tags["bad"]]),
                            json.dumps(list(ANSWER_KINDS)),
                            now,
                        ),
                    )
                    failing = tags["ok"] if alias["check_flip"] else tags["bad"]
                    chain = chain_json([TagStep(cur.lastrowid, (Line("any", (failing,), "fail"),))])
                db.execute(
                    "UPDATE aliases SET check_kinds = ?, tag_chain = ? WHERE id = ?",
                    (json.dumps(list(kinds)), chain, alias["id"]),
                )
            db.execute(_UPSERT.format(table="meta"), (CHECKERS_CONVERTED, str(now)))
        if checkers:
            log.info(json.dumps({"event": "checkers_converted", "aliases": [a["name"] for a in checkers]}))

    def _start_hourly_usage(self) -> None:
        """Hourly totals filled from the request log hold all usage only for the days whose totals match the daily
        rollup's (the log keeps 30 days by default, can be off, and its oldest day is cut part-way). Going back from
        today, the first day that doesn't match ends them: from the next day on, hours are used; before it, whole
        days. Hourly rows before that are dropped."""
        sums = "SUM(requests), SUM(errors), SUM(prompt_tokens), SUM(completion_tokens), SUM(cached_tokens)"

        def per_day(day: str, table: str) -> dict[str, tuple]:
            rows = self.query(f"SELECT {day}, {sums} FROM {table} GROUP BY 1")  # noqa: S608
            return {r[0]: tuple(r)[1:] for r in rows}

        daily, hourly = per_day("day", "usage_daily"), per_day("substr(hour, 1, 10)", "usage_hourly")
        today = time.strftime("%Y-%m-%d", time.gmtime())
        start = ""  # all of it (a new database, or a log that holds all the usage)
        for day in sorted({d for d in daily.keys() | hourly.keys() if d <= today}, reverse=True):
            if daily.get(day) != hourly.get(day):
                start = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
                break
        with self.tx() as db:
            db.execute("DELETE FROM usage_hourly WHERE hour < ?", (start,))
            db.execute(_UPSERT.format(table="meta"), ("usage_hourly_from", start))
        log.info(json.dumps({"event": "hourly_usage", "from": start}))

    def hourly_usage_from(self) -> str:
        """The first day (YYYY-MM-DD) whose usage is kept per hour; earlier days only have daily totals ("": all
        days are). Read on the pages' own connection, like the usage it goes with."""
        rows = self.read("SELECT value FROM meta WHERE key = 'usage_hourly_from'")
        return rows[0]["value"] if rows else ""

    def audit(self, actor: str, action: str, target: str = "", detail: object = "") -> None:
        text = detail if isinstance(detail, str) else json.dumps(detail, default=str)
        with self.tx() as db:
            db.execute(
                "INSERT INTO audit_log (ts, actor, action, target, detail) VALUES (?, ?, ?, ?, ?)",
                (time.time(), actor, action, target, text),
            )

    # -- meta / first start --------------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        rows = self.query("SELECT value FROM meta WHERE key = ?", (key,))
        return rows[0]["value"] if rows else None

    def set_meta(self, key: str, value: str) -> None:
        with self.tx() as db:
            db.execute(_UPSERT.format(table="meta"), (key, value))

    @property
    def initialized(self) -> bool:
        return self.get_meta("initialized") is not None

    def import_settings(self, seed: Settings, source: str = "environment") -> dict:
        """One-time import of env configuration on first start. Returns a summary."""
        summary = {"settings": 0, "upstream": None, "clients": []}
        parsed = parse_access_keys(seed.access_keys)
        with self.tx() as db:
            for name, value in seed.tunables().items():
                if name in SECRET_TUNABLES and value:
                    value = self.encrypt(value)
                db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (name, json.dumps(value)))
                summary["settings"] += 1
            if seed.upstream_api_key:
                now = time.time()
                is_arliai = "arliai.com" in seed.upstream_url
                cur = db.execute(
                    "INSERT INTO limit_pools (name, plan, max_concurrency, context_cap, min_spacing_ms, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        "arliai-main" if is_arliai else "main",
                        None,
                        seed.max_concurrency,
                        None,
                        seed.min_spacing_ms,
                        now,
                    ),
                )
                db.execute(
                    "INSERT INTO upstreams (name, preset, base_url, api_key_enc, pool_id, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        "arliai" if is_arliai else "upstream",
                        "arliai" if is_arliai else "custom",
                        seed.upstream_url,
                        self.encrypt(seed.upstream_api_key),
                        cur.lastrowid,
                        now,
                    ),
                )
                summary["upstream"] = seed.upstream_url
            for p in parsed:
                db.execute(
                    "INSERT INTO clients (name, key_hash, key_prefix, key_secret, priority, created_at, group_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, (SELECT MIN(id) FROM client_groups))",
                    (p.name, hash_key(p.key), p.key[:6], self.encrypt(p.key), p.priority, time.time()),
                )
                summary["clients"].append(p.name)
            db.execute("INSERT INTO meta (key, value) VALUES ('initialized', ?)", (str(time.time()),))
            db.execute("INSERT INTO meta (key, value) VALUES ('imported_from', ?)", (source,))
        self.audit("system", "import", source, summary)
        return summary

    # -- settings ------------------------------------------------------------------------

    def get_tunables(self) -> dict[str, object]:
        values = {}
        for row in self.query("SELECT key, value FROM settings"):
            if row["key"] in TUNABLES:
                value = json.loads(row["value"])
                if row["key"] in SECRET_TUNABLES and value:
                    value = self.decrypt(value)
                values[row["key"]] = TUNABLES[row["key"]](value)
        return values

    def set_tunables(self, values: dict[str, object], actor: str) -> None:
        unknown = set(values) - set(TUNABLES)
        if unknown:
            raise ValueError(f"unknown settings: {', '.join(sorted(unknown))}")
        with self.tx() as db:
            for name, value in values.items():
                value = TUNABLES[name](value)
                if name in SECRET_TUNABLES and value:
                    value = self.encrypt(value)  # stored encrypted like upstream keys
                db.execute(_UPSERT.format(table="settings"), (name, json.dumps(value)))
        self.audit(
            actor,
            "settings.update",
            "",
            {k: ("(secret changed)" if k in SECRET_TUNABLES else v) for k, v in values.items()},
        )

    # -- pools and upstreams -------------------------------------------------------------

    def list_pools(self) -> list[Pool]:
        return [
            Pool(
                r["id"],
                r["name"],
                r["plan"],
                r["max_concurrency"],
                r["context_cap"],
                r["min_spacing_ms"],
                r["monthly_price"],
                r["embedding_concurrency"],
            )
            for r in self.query("SELECT * FROM limit_pools ORDER BY id")
        ]

    def list_upstreams(self) -> list[Upstream]:
        return [
            Upstream(
                r["id"],
                r["name"],
                r["preset"],
                r["base_url"],
                r["pool_id"],
                r["weight"],
                bool(r["enabled"]),
                bool(r["paused"]),
                r["params"] or "",
                r["max_retries"],
                r["ua_mode"] or "keep",
                r["ua_template"] or "",
                r["model_filter"] or "",
            )
            for r in self.query("SELECT * FROM upstreams ORDER BY id")
        ]

    def upstream_key(self, upstream_id: int) -> str:
        rows = self.query("SELECT api_key_enc FROM upstreams WHERE id = ?", (upstream_id,))
        if not rows:
            raise KeyError(upstream_id)
        return self.decrypt(rows[0]["api_key_enc"])

    def create_pool(
        self,
        name: str,
        max_concurrency: int,
        actor: str,
        plan: str | None = None,
        context_cap: int | None = None,
        min_spacing_ms: int = 0,
        monthly_price: float | None = None,
        embedding_concurrency: int | None = None,
    ) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO limit_pools (name, plan, max_concurrency, context_cap, min_spacing_ms, monthly_price, "
                "embedding_concurrency, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name,
                    plan,
                    max_concurrency,
                    context_cap,
                    min_spacing_ms,
                    monthly_price,
                    embedding_concurrency,
                    time.time(),
                ),
            )
        self.audit(actor, "pool.create", name, {"max_concurrency": max_concurrency, "plan": plan})
        return cur.lastrowid

    def update_pool(self, pool_id: int, actor: str, **fields: object) -> None:
        allowed = {
            "name",
            "plan",
            "max_concurrency",
            "context_cap",
            "min_spacing_ms",
            "monthly_price",
            "embedding_concurrency",
        }
        self._update("limit_pools", pool_id, allowed, fields)
        self.audit(actor, "pool.update", str(pool_id), fields)

    def create_upstream(
        self,
        name: str,
        base_url: str,
        api_key: str,
        pool_id: int,
        actor: str,
        preset: str | None = None,
        weight: int = 1,
        params: str = "",
        max_retries: int | None = None,
        ua_mode: str = "keep",
        ua_template: str = "",
    ) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO upstreams (name, preset, base_url, api_key_enc, pool_id, weight, params, max_retries, "
                "ua_mode, ua_template, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name,
                    preset,
                    base_url.rstrip("/"),
                    self.encrypt(api_key),
                    pool_id,
                    weight,
                    params,
                    max_retries,
                    ua_mode,
                    ua_template,
                    time.time(),
                ),
            )
        self.audit(actor, "upstream.create", name, {"base_url": base_url, "pool_id": pool_id})
        return cur.lastrowid

    def update_upstream(self, upstream_id: int, actor: str, **fields: object) -> None:
        if "api_key" in fields:
            fields["api_key_enc"] = self.encrypt(str(fields.pop("api_key")))
        allowed = {
            "name",
            "model_filter",
            "preset",
            "base_url",
            "api_key_enc",
            "pool_id",
            "weight",
            "enabled",
            "paused",
            "params",
            "max_retries",
            "ua_mode",
            "ua_template",
        }
        self._update("upstreams", upstream_id, allowed, fields)
        self.audit(actor, "upstream.update", str(upstream_id), {k: v for k, v in fields.items() if k != "api_key_enc"})

    def delete_upstream(self, upstream_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM upstreams WHERE id = ?", (upstream_id,))
            for table in ("model_limits", "model_names", "model_prices", "model_scores", "model_thinking"):
                db.execute(f"DELETE FROM {table} WHERE upstream_id = ?", (upstream_id,))  # noqa: S608
        self.audit(actor, "upstream.delete", str(upstream_id))

    def delete_pool(self, pool_id: int, actor: str) -> None:
        """Delete a pool; refuses while upstreams still use it."""
        with self.tx() as db:
            used = db.execute("SELECT COUNT(*) FROM upstreams WHERE pool_id = ?", (pool_id,)).fetchone()[0]
            if used:
                raise ValueError("pool is still used by upstreams")
            db.execute("DELETE FROM limit_pools WHERE id = ?", (pool_id,))
        self.audit(actor, "pool.delete", str(pool_id))

    # -- clients -------------------------------------------------------------------------

    def list_clients(self) -> list[Client]:
        return [
            Client(
                r["id"],
                r["name"],
                r["key_hash"],
                r["key_prefix"],
                r["priority"],
                bool(r["enabled"]),
                r["expires_at"],
                r["notes"],
                r["created_at"],
                ClientPolicy.from_json(r["policy"]),
                r["key_secret"] is not None,
                r["group_id"],
            )
            for r in self.query("SELECT * FROM clients ORDER BY name")
        ]

    def create_client(
        self,
        name: str,
        actor: str,
        priority: str = "normal",
        notes: str = "",
        expires_at: float | None = None,
        group_id: int | None = None,
    ) -> str:
        """Create a client and return its new key (hashed to check requests, encrypted to show it again). It goes in
        ``group_id``, else the first group."""
        if priority not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        key = new_client_key()
        with self.tx() as db:
            group = db.execute(
                "SELECT id, name FROM client_groups WHERE id = COALESCE(?, (SELECT MIN(id) FROM client_groups))",
                (group_id,),
            ).fetchone()
            if group is None:
                raise ValueError("no such group")
            db.execute(
                "INSERT INTO clients (name, key_hash, key_prefix, key_secret, priority, notes, expires_at, created_at, "
                "group_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (name, hash_key(key), key[:6], self.encrypt(key), priority, notes, expires_at, time.time(), group[0]),
            )
        self.audit(actor, "client.create", name, {"priority": priority, "group": group[1]})
        return key

    def update_client(self, client_id: int, actor: str, **fields: object) -> None:
        if "priority" in fields and fields["priority"] not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        old = None
        if "name" in fields:
            rows = self.query("SELECT name FROM clients WHERE id = ?", (client_id,))
            old = rows[0]["name"] if rows and rows[0]["name"] != fields["name"] else None
        if isinstance(fields.get("policy"), ClientPolicy):
            fields["policy"] = fields["policy"].to_json()
        # renamed first (fails if the name is taken), then its jobs, files, schedules... move with it, so a failed
        # rename never hands its data to the client that already has that name
        allowed = {"name", "priority", "enabled", "expires_at", "notes", "policy", "group_id"}
        self._update("clients", client_id, allowed, fields)  # a group that doesn't exist: IntegrityError
        if old is not None:
            new = fields["name"]
            with self.tx() as db:
                for table in CLIENT_DATA_TABLES:
                    db.execute(f"UPDATE {table} SET client = ? WHERE client = ?", (new, old))  # noqa: S608
                # its history too (Requests, Usage, monthly budgets); usage rows merge with any the name already has
                db.execute("UPDATE requests SET client = ? WHERE client = ?", (new, old))
                for table, slot in (("usage_daily", "day"), ("usage_hourly", "hour")):
                    keys = (slot, "client", "model", "upstream")
                    counts = [r["name"] for r in db.execute(f"PRAGMA table_info({table})") if r["name"] not in keys]
                    db.execute(
                        f"INSERT INTO {table} ({', '.join(keys + tuple(counts))}) "  # noqa: S608
                        f"SELECT {slot}, ?, model, upstream, {', '.join(counts)} FROM {table} WHERE client = ? "
                        f"ON CONFLICT({slot}, client, model, upstream) DO UPDATE SET "
                        + ", ".join(f"{c} = {c} + excluded.{c}" for c in counts),
                        (new, old),
                    )
                    db.execute(f"DELETE FROM {table} WHERE client = ?", (old,))  # noqa: S608
        self.audit(actor, "client.update", str(client_id), fields)

    def regenerate_client_key(self, client_id: int, actor: str) -> str:
        key = new_client_key()
        fields = {"key_hash": hash_key(key), "key_prefix": key[:6], "key_secret": self.encrypt(key)}
        self._update("clients", client_id, set(fields), fields)
        self.audit(actor, "client.regenerate_key", str(client_id))
        return key

    def client_key(self, client_id: int, actor: str) -> str | None:
        """A client's key, to show it again (audited); None when only its hash is kept (made before 0.42) or
        it can't be decrypted (another SECRET_KEY)."""
        rows = self.query("SELECT name, key_secret FROM clients WHERE id = ?", (client_id,))
        if not rows or not rows[0]["key_secret"]:
            return None
        try:
            key = self.decrypt(rows[0]["key_secret"])
        except RuntimeError:
            return None
        self.audit(actor, "client.show_key", rows[0]["name"])
        return key

    def delete_client(self, client_id: int, actor: str) -> None:
        """Delete a client and everything it owns, so a later client with the same name (or
        key) starts empty instead of inheriting its jobs, files and saved conversations."""
        with self.tx() as db:
            row = db.execute("SELECT name FROM clients WHERE id = ?", (client_id,)).fetchone()
            removed = {}
            if row is not None:
                for table in CLIENT_DATA_TABLES:
                    cur = db.execute(f"DELETE FROM {table} WHERE client = ?", (row["name"],))  # noqa: S608
                    if cur.rowcount:
                        removed[table] = cur.rowcount
                db.execute("UPDATE requests SET timeline = NULL WHERE client = ?", (row["name"],))  # its excerpts
            db.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        self.audit(actor, "client.delete", str(client_id), {"name": row["name"] if row else None, "removed": removed})

    # -- client groups -------------------------------------------------------------------

    def list_groups(self) -> list[Group]:
        return [
            Group(r["id"], r["name"], ClientPolicy.from_json(r["policy"]), r["notes"], r["created_at"])
            for r in self.query("SELECT * FROM client_groups ORDER BY name COLLATE NOCASE")
        ]

    def create_group(self, name: str, actor: str, policy: ClientPolicy, notes: str = "") -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO client_groups (name, policy, notes, created_at) VALUES (?, ?, ?, ?)",
                (name, policy.to_json(), notes, time.time()),
            )
        self.audit(actor, "group.create", name, {"policy": policy.to_json()})
        return cur.lastrowid

    def update_group(self, group_id: int, actor: str, **fields: object) -> None:
        if isinstance(fields.get("policy"), ClientPolicy):
            fields["policy"] = fields["policy"].to_json()
        self._update("client_groups", group_id, {"name", "policy", "notes"}, fields)
        self.audit(actor, "group.update", str(group_id), fields)

    def delete_group(self, group_id: int, actor: str) -> None:
        """Only a group no client is in, and never the last one: every client is in a group."""
        with self.tx() as db:
            row = db.execute("SELECT name FROM client_groups WHERE id = ?", (group_id,)).fetchone()
            if row is None:
                raise KeyError(group_id)
            if db.execute("SELECT 1 FROM clients WHERE group_id = ? LIMIT 1", (group_id,)).fetchone():
                raise ValueError("group_in_use")
            if db.execute("SELECT COUNT(*) FROM client_groups").fetchone()[0] <= 1:
                raise ValueError("group_last")
            db.execute("DELETE FROM client_groups WHERE id = ?", (group_id,))
        self.audit(actor, "group.delete", row["name"])

    # -- request log and usage -----------------------------------------------------------

    def record_requests(self, rows: list[dict], keep_rows: bool = True) -> None:
        """Store finished requests (metadata) and add them to the daily usage rollup."""
        if not rows:
            return
        placeholders = ", ".join("?" for _ in REQUEST_COLUMNS)
        rows = [{k: storable(v) for k, v in row.items()} for row in rows]  # one odd string mustn't lose them all
        with self.tx() as db:
            for row in rows:
                if keep_rows:
                    db.execute(
                        f"INSERT OR IGNORE INTO requests ({', '.join(REQUEST_COLUMNS)}) VALUES ({placeholders})",
                        tuple(int(row[c]) if c == "stream" else row.get(c) for c in REQUEST_COLUMNS),
                    )
                if row.get("outcome") in ("refused", "forwarded") or row.get("council"):
                    continue  # turned away before any work, a council (its members and judge are usage), or forwarded
                    # to another alias (the request it was forwarded as is)
                counts = (
                    row.get("client") or "-",
                    row.get("target_model") or row.get("model") or "-",
                    row.get("upstream") or "-",
                    0 if row.get("outcome") == "done" else 1,
                    row.get("prompt_tokens") or 0,
                    row.get("completion_tokens") or 0,
                    row.get("cached_tokens") or 0,
                    row.get("cache_write_tokens") or 0,
                )
                when = time.gmtime(row["ts"])
                for table, slot, fmt in (("usage_daily", "day", "%Y-%m-%d"), ("usage_hourly", "hour", HOUR_FORMAT)):
                    db.execute(_ADD_USAGE.format(table=table, slot=slot), (time.strftime(fmt, when), *counts))

    def request_family(self, request_id: str) -> list[sqlite3.Row]:
        """A request and the ones QueueRouter made for it (council members and judge, orchestrator calls),
        oldest first."""
        return self.query("SELECT * FROM requests WHERE id = ? OR parent = ? ORDER BY ts", (request_id, request_id))

    def list_requests(
        self,
        limit: int = 200,
        after: float | None = None,
        before: tuple[float, str] | None = None,
        columns: str = "*",
        **filters: str,
    ) -> list[sqlite3.Row]:
        """Newest first; ``after`` keeps rows written at or after that time (the page's live refresh),
        ``before`` the ones older than that row, given as (ts, id) (its next page)."""
        clauses, params = [], []
        if after is not None:
            clauses.append("ts >= ?")
            params.append(after)
        if before is not None:
            clauses.append("ts <= ? AND (ts < ? OR id < ?)")
            params.extend((before[0], before[0], before[1]))
        for column in ("client", "outcome", "model", "upstream"):
            if value := filters.get(column):
                clauses.append(f"{column} = ?")
                params.append(value)
        for column, key in (("tags", "tag"), ("answer_tags", "answer_tag")):
            if tag := filters.get(key):
                clauses.append(f"(',' || {column} || ',') LIKE ?")
                params.append(f"%,{tag},%")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT {columns} FROM requests {where} ORDER BY ts DESC, id DESC LIMIT ?"  # noqa: S608
        return self.read(sql, (*params, limit))

    def request_row(self, request_id: str) -> sqlite3.Row | None:
        rows = self.read("SELECT * FROM requests WHERE id = ?", (request_id,))
        return rows[0] if rows else None

    def calls_of(self, parents: list[sqlite3.Row]) -> list[sqlite3.Row]:
        """The calls QueueRouter made for these requests (council members and judge, orchestrator calls). They
        finish around their parent, and the time range keeps this on the ts index."""
        if not parents:
            return []
        marks = ",".join("?" for _ in parents)
        times = [r["ts"] for r in parents]
        return self.read(
            "SELECT parent, model, target_model, upstream, prompt_tokens, completion_tokens, cached_tokens, "
            "cache_write_tokens FROM requests "
            f"WHERE ts BETWEEN ? AND ? AND parent IN ({marks})",  # noqa: S608
            (min(times) - CALLS_WITHIN_S, max(times) + CALLS_WITHIN_S, *(r["id"] for r in parents)),
        )

    def response_rows(self, since: float) -> list[sqlite3.Row]:
        """Timings of the requests finished since ``since``, by the model actually served (app/latency.py).
        Councils are left out: their members' and judge's calls are rows of their own."""
        return self.read(
            "SELECT ts, COALESCE(target_model, model) AS model, upstream, outcome, headers_ms, first_token_ms, "
            "answer_ms, tokens_per_s, stall_ms, backoff_ms FROM requests "
            "WHERE ts >= ? AND upstream IS NOT NULL AND council IS NULL",
            (since,),
        )

    def request_filter_values(self) -> dict[str, list[str]]:
        values = {
            column: [
                r[0]
                for r in self.read(
                    f"SELECT DISTINCT {column} FROM requests WHERE {column} IS NOT NULL ORDER BY {column} LIMIT 200"
                )
            ]  # noqa: S608
            for column in ("client", "outcome", "model", "upstream")
        }
        for column, key in (("tags", "tag"), ("answer_tags", "answer_tag")):
            tags: set[str] = set()
            sql = f"SELECT DISTINCT {column} FROM requests WHERE {column} IS NOT NULL AND {column} != '' LIMIT 500"  # noqa: S608
            for (text,) in self.read(sql):
                tags.update(t for t in text.split(",") if t)
            values[key] = sorted(tags)
        return values

    def usage_month(self, month: str) -> list[sqlite3.Row]:
        """This month's usage per client, model and upstream (``month`` = YYYY-MM)."""
        return self.query(
            "SELECT client, model, upstream, SUM(requests) AS requests, SUM(prompt_tokens) AS prompt_tokens, "
            "SUM(completion_tokens) AS completion_tokens, SUM(cached_tokens) AS cached_tokens, "
            "SUM(cache_write_tokens) AS cache_write_tokens "
            "FROM usage_daily WHERE day >= ? AND day < ? GROUP BY client, model, upstream",
            (f"{month}-01", f"{month}-32"),
        )

    def usage_between(self, start: float, end: float) -> list[sqlite3.Row]:
        """Usage per hour, client, model and upstream from ``start`` to ``end`` (UTC seconds, ``end`` not included;
        cost is computed per model and upstream, then grouped). Days before hourly totals began
        (hourly_usage_from) have only daily ones: their rows have no ``hour``, and count the whole day."""
        first = self.hourly_usage_from()
        hours = [time.strftime(HOUR_FORMAT, time.gmtime(t)) for t in (start, end)]
        days = [time.strftime("%Y-%m-%d", time.gmtime(t)) for t in (start, end - 1)]
        counts = "requests, errors, prompt_tokens, completion_tokens, cached_tokens, cache_write_tokens"
        return self.read(
            f"SELECT hour, substr(hour, 1, 10) AS day, client, model, upstream, {counts} FROM usage_hourly "  # noqa: S608
            "WHERE hour >= ? AND hour < ? AND hour >= ? UNION ALL "
            f"SELECT NULL, day, client, model, upstream, {counts} FROM usage_daily "
            "WHERE day >= ? AND day <= ? AND day < ? ORDER BY day DESC, client, model, upstream",
            (*hours, first, *days, first),
        )

    def token_totals(self, days: int, since: str = "") -> tuple[int, int, int]:
        """(prompt, cached, completion) tokens over the last ``days`` days, from the day ``since`` (YYYY-MM-DD) on
        when that is later."""
        since = max(time.strftime("%Y-%m-%d", time.gmtime(time.time() - (days - 1) * 86_400)), since)
        row = self.read(
            "SELECT COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(cached_tokens), 0), "
            "COALESCE(SUM(completion_tokens), 0) FROM usage_daily WHERE day >= ?",
            (since,),
        )[0]
        return row[0], row[1], row[2]

    def used_models(self, days: int | None = None) -> list[tuple[str, str]]:
        """(upstream, model) pairs in the usage history (only the last ``days`` days when given)."""
        since = time.strftime("%Y-%m-%d", time.gmtime(time.time() - days * 86_400)) if days else ""
        rows = self.query(
            "SELECT DISTINCT upstream, model FROM usage_daily WHERE model != '-' AND day >= ? ORDER BY model, upstream",
            (since,),
        )
        return [(r[0], r[1]) for r in rows]

    # -- model prices (manual overrides) ----------------------------------------------------

    def list_model_limits(self) -> list[dict]:
        return [dict(r) for r in self.query("SELECT * FROM model_limits ORDER BY model, upstream_id")]

    def set_model_limit(
        self, model: str, actor: str, context: int | None, output: int | None, upstream_id: int = EVERY
    ) -> None:
        """Manual context / output size for ``model`` on that upstream (or every one); both None removes it."""
        with self.tx() as db:
            if context is None and output is None:
                db.execute(
                    "DELETE FROM model_limits WHERE upstream_id = ? AND model = ? AND source = 'manual'",
                    (upstream_id, model),
                )
            else:
                db.execute(
                    "INSERT INTO model_limits (upstream_id, model, source, context, output, updated_at) "
                    "VALUES (?, ?, 'manual', ?, ?, ?) ON CONFLICT(upstream_id, model, source) DO UPDATE SET "
                    "context = excluded.context, output = excluded.output, updated_at = excluded.updated_at",
                    (upstream_id, model, context, output, time.time()),
                )
        self.audit(actor, "model_limit.update", model, {"context": context, "output": output, "upstream": upstream_id})

    def learn_model_limit(self, upstream_id: int, model: str, context: int, note: str) -> None:
        """A context size read from the provider's own overflow error."""
        with self.tx() as db:
            db.execute(
                "INSERT INTO model_limits (upstream_id, model, source, context, note, updated_at) "
                "VALUES (?, ?, 'learned', ?, ?, ?) ON CONFLICT(upstream_id, model, source) DO UPDATE SET "
                "context = excluded.context, note = excluded.note, updated_at = excluded.updated_at",
                (upstream_id, model, context, note[:300], time.time()),
            )

    def learn_model_output(self, upstream_id: int, model: str, output: int, note: str) -> None:
        """An output size read from the provider refusing a bigger answer size (beside a learned context)."""
        with self.tx() as db:
            db.execute(
                "INSERT INTO model_limits (upstream_id, model, source, output, note, updated_at) "
                "VALUES (?, ?, 'learned', ?, ?, ?) ON CONFLICT(upstream_id, model, source) DO UPDATE SET "
                "output = excluded.output, note = excluded.note, updated_at = excluded.updated_at",
                (upstream_id, model, output, note[:300], time.time()),
            )

    def forget_learned_limit(self, upstream_id: int, model: str, actor: str) -> None:
        with self.tx() as db:
            db.execute(
                "DELETE FROM model_limits WHERE upstream_id = ? AND model = ? AND source = 'learned'",
                (upstream_id, model),
            )
        self.audit(actor, "model_limit.forget", model, {"upstream_id": upstream_id})

    def list_model_names(self) -> dict[tuple[int, str], str]:
        return {(r["upstream_id"], r["model"]): r["name"] for r in self.query("SELECT * FROM model_names")}

    def set_model_name(self, upstream_id: int, model: str, actor: str, name: str | None) -> None:
        """A model's custom name on an upstream (None removes it: its ID shows again)."""
        with self.tx() as db:
            if name is None:
                db.execute("DELETE FROM model_names WHERE upstream_id = ? AND model = ?", (upstream_id, model))
            else:
                db.execute(
                    "INSERT INTO model_names (upstream_id, model, name, updated_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(upstream_id, model) DO UPDATE SET name = excluded.name, "
                    "updated_at = excluded.updated_at",
                    (upstream_id, model, name, time.time()),
                )
        self.audit(actor, "model.name", model, {"upstream": upstream_id, "name": name})

    def list_model_thinking(self) -> dict[tuple[int, str], str]:
        rows = self.query("SELECT upstream_id, model, levels FROM model_thinking")
        return {(r["upstream_id"], r["model"]): r["levels"] for r in rows}

    def set_model_thinking(self, model: str, actor: str, levels: str | None, upstream_id: int = EVERY) -> None:
        """The administrator's thinking levels for a model (None removes them: detection decides again)."""
        with self.tx() as db:
            if levels is None:
                db.execute("DELETE FROM model_thinking WHERE upstream_id = ? AND model = ?", (upstream_id, model))
            else:
                db.execute(
                    "INSERT INTO model_thinking (upstream_id, model, levels, updated_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(upstream_id, model) DO UPDATE SET levels = excluded.levels, "
                    "updated_at = excluded.updated_at",
                    (upstream_id, model, levels, time.time()),
                )
        self.audit(actor, "model.thinking", model, {"levels": levels, "upstream": upstream_id})

    def list_model_scores(self) -> dict[tuple[int, str], float]:
        rows = self.query("SELECT upstream_id, model, score FROM model_scores")
        return {(r["upstream_id"], r["model"]): r["score"] for r in rows}

    def set_model_score(self, model: str, actor: str, score: float | None, upstream_id: int = EVERY) -> None:
        """The administrator's strength score for a model (None removes it: the index decides again)."""
        with self.tx() as db:
            if score is None:
                db.execute("DELETE FROM model_scores WHERE upstream_id = ? AND model = ?", (upstream_id, model))
            else:
                db.execute(
                    "INSERT INTO model_scores (upstream_id, model, score, updated_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(upstream_id, model) DO UPDATE SET score = excluded.score, "
                    "updated_at = excluded.updated_at",
                    (upstream_id, model, score, time.time()),
                )
        self.audit(actor, "model.score", model, {"score": score, "upstream": upstream_id})

    def list_model_prices(self) -> dict[tuple[int, str], sqlite3.Row]:
        return {(r["upstream_id"], r["model"]): r for r in self.query("SELECT * FROM model_prices")}

    def set_model_price(
        self,
        model: str,
        actor: str,
        reference: str | None,
        input_price: float | None,
        output_price: float | None,
        cached_price: float | None,
        upstream_id: int = EVERY,
    ) -> None:
        with self.tx() as db:
            if reference is None and input_price is None and output_price is None and cached_price is None:
                db.execute("DELETE FROM model_prices WHERE upstream_id = ? AND model = ?", (upstream_id, model))
            else:
                db.execute(
                    "INSERT INTO model_prices (upstream_id, model, reference, input, output, cached_input, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(upstream_id, model) DO UPDATE SET "
                    "reference = excluded.reference, input = excluded.input, output = excluded.output, "
                    "cached_input = excluded.cached_input, updated_at = excluded.updated_at",
                    (upstream_id, model, reference, input_price, output_price, cached_price, time.time()),
                )
        self.audit(
            actor,
            "price.update",
            model,
            {
                "reference": reference,
                "input": input_price,
                "output": output_price,
                "cached": cached_price,
                "upstream": upstream_id,
            },
        )

    def recent_activity(self, since: float) -> list[sqlite3.Row]:
        """Dispatched requests since ``since``: to restore rate-limit windows after a restart."""
        return self.query(
            "SELECT client, ts, queue_ms, prompt_tokens, completion_tokens FROM requests "
            "WHERE ts >= ? AND queue_ms IS NOT NULL",
            (since,),
        )

    def prune_requests(self, keep_days: int) -> int:
        with self.tx() as db:
            cur = db.execute("DELETE FROM requests WHERE ts < ?", (time.time() - keep_days * 86_400,))
        return cur.rowcount

    # -- aliases -------------------------------------------------------------------------

    def list_aliases(self) -> list[Alias]:
        return [
            Alias(
                r["id"],
                r["name"],
                r["target_model"],
                tuple(json.loads(r["upstream_ids"])),
                bool(r["enabled"]),
                r["notes"],
                r["params"],
                tuple(json.loads(r["fallbacks"] or "[]")),
                tuple(json.loads(r["fallback_on"] or "[]")),
                r["compact"] or "off",
                r["compact_model"] or "",
                r["virtual_context"],
                r["council"] or "off",
                tuple(json.loads(r["council_members"] or "[]")),
                r["council_judge"] or "",
                r["council_when"] or "planning",
                r["council_instructions"] or "",
                bool(r["council_search"]),
                r["council_search_max"] or 3,
                r["council_search_results"] or 5,
                bool(r["council_fetch"]),
                r["orch_rounds"] or 6,
                r["orch_tasks"] or 8,
                r["orch_seconds"] or 300,
                r["council_roles"] or "",
                edits=tuple(i for i in json.loads(r["edits"] or "[]") if isinstance(i, int)),
                label=r["label"] or "",
                answer_rules=tuple(i for i in json.loads(r["answer_rules"] or "[]") if isinstance(i, int)),
                check_kinds=kinds_of(json.loads(r["check_kinds"] or "[]")),
                check_when=r["check_when"] if r["check_when"] in CHECK_WHEN else "undecided",
                spot_checks=r["spot_checks"] if r["spot_checks"] is not None else SPOT_DEFAULT,
                learn_rules=r["learn_rules"] if r["learn_rules"] in LEARN_MODES else "off",
                learn_model=r["learn_model"] or "",
                learn_pass=bool(r["learn_pass"]),
                tag_chain=chain_of(r["tag_chain"]),
            )
            for r in self.query("SELECT * FROM aliases ORDER BY name")
        ]

    def create_alias(
        self,
        name: str,
        target_model: str,
        upstream_ids: tuple[int, ...],
        actor: str,
        notes: str = "",
        params: str = "",
        fallbacks: tuple[str, ...] = (),
        fallback_on: tuple[str, ...] = ("unavailable", "errors", "context"),
        compact: str = "off",
        compact_model: str = "",
        virtual_context: int | None = None,
        council: str = "off",
        council_members: tuple[str, ...] = (),
        council_judge: str = "",
        council_when: str = "planning",
        council_instructions: str = "",
        council_search: bool = False,
        council_search_max: int = 3,
        council_search_results: int = 5,
        council_fetch: bool = False,
        orch_rounds: int = 6,
        orch_tasks: int = 8,
        orch_seconds: int = 300,
        council_roles: str = "",
        edits: tuple[int, ...] = (),
        label: str = "",
        answer_rules: tuple[int, ...] = (),
        check_kinds: tuple[str, ...] = DEFAULT_ANSWER_KINDS,
        check_when: str = "undecided",
        spot_checks: int = SPOT_DEFAULT,
        learn_rules: str = "off",
        learn_model: str = "",
        learn_pass: bool = False,
        tag_chain: tuple[TagStep, ...] = (),
    ) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO aliases (name, target_model, upstream_ids, notes, params, fallbacks, fallback_on, "
                "compact, compact_model, virtual_context, council, council_members, council_judge, council_when, "
                "council_instructions, council_search, council_search_max, council_search_results, council_fetch, "
                "orch_rounds, orch_tasks, orch_seconds, council_roles, edits, label, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name,
                    target_model,
                    json.dumps(list(upstream_ids)),
                    notes,
                    params,
                    json.dumps(list(fallbacks)),
                    json.dumps(list(fallback_on)),
                    compact,
                    compact_model,
                    virtual_context,
                    council,
                    json.dumps(list(council_members)),
                    council_judge,
                    council_when,
                    council_instructions,
                    int(council_search),
                    council_search_max,
                    council_search_results,
                    int(council_fetch),
                    orch_rounds,
                    orch_tasks,
                    orch_seconds,
                    council_roles,
                    json.dumps(list(edits)),
                    label,
                    time.time(),
                ),
            )
            db.execute(
                "UPDATE aliases SET answer_rules = ?, check_kinds = ?, check_when = ?, spot_checks = ?, "
                "learn_rules = ?, learn_model = ?, learn_pass = ?, tag_chain = ? WHERE id = ?",
                (json.dumps(list(answer_rules)), json.dumps(list(check_kinds)), check_when, spot_checks,
                 learn_rules, learn_model, int(learn_pass), chain_json(tag_chain), cur.lastrowid),
            )  # fmt: skip
        self.audit(
            actor,
            "alias.create",
            name,
            {"target_model": target_model, "upstreams": list(upstream_ids), "fallbacks": list(fallbacks)},
        )
        return cur.lastrowid

    def update_alias(self, alias_id: int, actor: str, **fields: object) -> None:
        lists = ("upstream_ids", "fallbacks", "fallback_on", "council_members", "edits", "answer_rules", "check_kinds")
        for name in lists:
            if name in fields:
                fields[name] = json.dumps(list(fields[name]))
        if "tag_chain" in fields:
            fields["tag_chain"] = chain_json(fields["tag_chain"])
        allowed = {"target_model", "upstream_ids", "enabled", "notes", "params", "fallbacks", "fallback_on"}
        allowed |= {
            "compact",
            "compact_model",
            "virtual_context",
            "council",
            "council_members",
            "council_judge",
            "council_when",
            "council_instructions",
            "council_search",
            "council_search_max",
            "council_search_results",
            "council_fetch",
            "orch_rounds",
            "orch_tasks",
            "orch_seconds",
            "council_roles",
            "edits",
            "label",
            "answer_rules",
            "check_kinds",
            "check_when",
            "spot_checks",
            "learn_rules",
            "learn_model",
            "learn_pass",
            "tag_chain",
        }
        self._update("aliases", alias_id, allowed, fields)
        if fields.get("learn_rules") == "off":  # its rule maker is off: the answers kept for it go
            with self.tx() as db:
                db.execute("DELETE FROM answer_samples WHERE alias_id = ?", (alias_id,))
                db.execute("UPDATE rule_learning SET new_fails = 0, new_passes = 0 WHERE alias_id = ?", (alias_id,))
        self.audit(actor, "alias.update", str(alias_id), fields)

    def rename_alias(self, alias_id: int, name: str, actor: str, own_settings: bool = True) -> None:
        """The alias's name (the ID apps send) becomes ``name``, and what QueueRouter keeps that names it follows:
        other aliases (model, fallbacks, council members and judge, roles, summary and rule maker models), taggers'
        models, the model lists of clients and groups, prompt edits' models, schedules and queued jobs, and
        (``own_settings``: the old name isn't also a provider's model, whose settings these would be) its score,
        thinking levels, price and limits set on the Models page. One transaction; IntegrityError when another alias
        has the name. The request log keeps the name each request asked for; usage counts the real models, so it has
        nothing."""
        with self.tx() as db:
            row = db.execute("SELECT name FROM aliases WHERE id = ?", (alias_id,)).fetchone()
            if row is None:
                raise KeyError(alias_id)
            old = row["name"]
            if old == name:
                return
            db.execute("UPDATE aliases SET name = ? WHERE id = ?", (name, alias_id))

            def one(value: str) -> str:
                return name if value == old else value

            def many(text: str | None) -> str:
                return json.dumps([one(v) for v in json.loads(text or "[]")])

            def role(line: str) -> str:  # "member = role: instructions"
                member, sep, rest = line.partition("=")
                return f"{name} ={rest}" if sep and member.strip() == old else line

            for a in db.execute("SELECT * FROM aliases").fetchall():
                roles = "\n".join(role(line) for line in (a["council_roles"] or "").splitlines())
                db.execute(
                    "UPDATE aliases SET target_model = ?, fallbacks = ?, council_members = ?, council_judge = ?, "
                    "compact_model = ?, learn_model = ?, council_roles = ? WHERE id = ?",
                    (
                        one(a["target_model"]),
                        many(a["fallbacks"]),
                        many(a["council_members"]),
                        one(a["council_judge"] or ""),
                        one(a["compact_model"] or ""),
                        one(a["learn_model"] or ""),
                        roles,
                        a["id"],
                    ),
                )
            db.execute("UPDATE taggers SET model = ? WHERE model = ?", (name, old))
            for table in ("clients", "client_groups"):
                for owner in db.execute(f"SELECT id, policy FROM {table}").fetchall():  # noqa: S608
                    policy = ClientPolicy.from_json(owner["policy"])
                    if old in policy.allowed_models:
                        moved = replace(policy, allowed_models=tuple(one(m) for m in policy.allowed_models))
                        db.execute(f"UPDATE {table} SET policy = ? WHERE id = ?", (moved.to_json(), owner["id"]))  # noqa: S608
            for edit in db.execute("SELECT id, settings FROM prompt_edits").fetchall():
                settings = json.loads(edit["settings"] or "{}")
                if settings.get("model") == old:
                    settings["model"] = name
                    db.execute(
                        "UPDATE prompt_edits SET settings = ? WHERE id = ?",
                        (json.dumps(settings, ensure_ascii=False), edit["id"]),
                    )
            for table, where in (("schedules", ""), ("jobs", " WHERE status = 'queued'")):
                for job in db.execute(f"SELECT id, request FROM {table}{where}").fetchall():  # noqa: S608
                    try:
                        body = json.loads(self.reveal(job["request"]) or "")
                    except ValueError:
                        continue
                    if isinstance(body, dict) and body.get("model") == old:
                        body["model"] = name
                        sealed = self.encrypt(json.dumps(body))
                        db.execute(f"UPDATE {table} SET request = ? WHERE id = ?", (sealed, job["id"]))  # noqa: S608
            if own_settings:
                for table in ("model_scores", "model_thinking", "model_prices", "model_limits"):
                    db.execute(
                        f"UPDATE OR REPLACE {table} SET model = ? WHERE model = ? AND upstream_id = ?",  # noqa: S608
                        (name, old, EVERY),
                    )
        self.audit(actor, "alias.rename", old, {"to": name})

    def delete_alias(self, alias_id: int, actor: str) -> None:
        """The alias, the answers kept for its rule maker, and its rule maker's state; the rules it made stay, as
        made by hand (a new alias may get its id)."""
        with self.tx() as db:
            db.execute("DELETE FROM aliases WHERE id = ?", (alias_id,))
            db.execute("DELETE FROM answer_samples WHERE alias_id = ?", (alias_id,))
            db.execute("DELETE FROM rule_learning WHERE alias_id = ?", (alias_id,))
            db.execute("UPDATE answer_rules SET made_by = NULL, awaiting = 0 WHERE made_by = ?", (alias_id,))
        self.audit(actor, "alias.delete", str(alias_id))

    # -- answer rules (app/answer_rules.py, app/rule_maker.py) ---------------------------------

    def list_rules(self) -> list[Rule]:
        return [
            Rule(
                r["id"],
                r["name"],
                r["kind"],
                r["find"],
                bool(r["match_case"]),
                r["scope"],
                r["state"],
                r["notes"],
                r["hits"],
                r["last_hit"],
                r["created_at"],
                r["action"],
                bool(r["negate"]),
                r["made_by"],
                r["why"],
                r["sample_fails"],
                r["sample_passes"],
                r["agreed"],
                r["disagreed"],
                r["recent"],
                bool(r["awaiting"]),
                r["note"],
                r["tag_id"] if r["action"] == "tag" else None,
            )
            for r in self.query("SELECT * FROM answer_rules ORDER BY name COLLATE NOCASE")
        ]

    _RULE_FIELDS = ("name", "kind", "find", "match_case", "scope", "action", "negate", "state", "notes", "tag_id")

    @staticmethod
    def _rule_row(rule: dict) -> tuple:
        """``rule`` (answer_rules.clean_rule) as the columns in ``_RULE_FIELDS``."""
        return (
            rule["name"],
            rule["kind"],
            rule["find"],
            int(bool(rule["case"])),
            rule["scope"],
            rule.get("action", "fail"),
            int(bool(rule.get("negate"))),
            rule.get("state", "on"),
            rule.get("notes", ""),
            rule.get("tag") if rule.get("action") == "tag" else None,
        )

    def create_rule(self, actor: str, **rule: object) -> int:
        """``rule``: answer_rules.clean_rule's fields. IntegrityError: the name is taken."""
        with self.tx() as db:
            cur = db.execute(
                f"INSERT INTO answer_rules ({', '.join(self._RULE_FIELDS)}, created_at) "  # noqa: S608
                f"VALUES ({'?, ' * len(self._RULE_FIELDS)}?)",
                (*self._rule_row(rule), time.time()),
            )
        self.audit(actor, "rule.create", str(rule["name"]))
        return cur.lastrowid

    # what a rule does: its counts go with it
    _MATCHING = ("kind", "find", "match_case", "scope", "action", "negate", "tag_id")

    def update_rule(self, rule_id: int, actor: str, **rule: object) -> None:
        """A change by hand: a rule the model made that you turn on is no longer waiting for you. A change to what
        it looks for or does starts its counts with the taggers afresh (they were the old rule's); a change of
        state starts its recent outcomes afresh (or old disagreements would turn it back at once)."""
        sets = ", ".join(f"{name} = ?" for name in self._RULE_FIELDS)
        row = self._rule_row(rule)
        new = dict(zip(self._RULE_FIELDS, row, strict=True))
        with self.tx() as db:
            old = db.execute("SELECT * FROM answer_rules WHERE id = ?", (rule_id,)).fetchone()
            if old is None:
                raise KeyError(rule_id)
            db.execute(
                f"UPDATE answer_rules SET {sets}, awaiting = awaiting AND ? != 'on' WHERE id = ?",  # noqa: S608
                (*row, new["state"], rule_id),
            )
            if any(old[f] != new[f] for f in self._MATCHING):
                db.execute(
                    "UPDATE answer_rules SET agreed = 0, disagreed = 0, recent = '', note = '', sample_fails = 0, "
                    "sample_passes = 0 WHERE id = ?",
                    (rule_id,),
                )
            elif old["state"] != new["state"]:
                db.execute("UPDATE answer_rules SET recent = '', note = '' WHERE id = ?", (rule_id,))
        self.audit(actor, "rule.update", str(rule_id), {"name": rule["name"], "state": rule.get("state")})

    def set_rule_state(self, rule_id: int, actor: str, state: str) -> None:
        """On, watching or off, by hand (turning a rule the model made on is approving it); its recent outcomes
        start afresh (or old disagreements would turn it back to watching at once)."""
        with self.tx() as db:
            cur = db.execute(
                "UPDATE answer_rules SET state = ?, awaiting = awaiting AND ? != 'on', note = '', recent = '' "
                "WHERE id = ?",
                (state, state, rule_id),
            )
            if cur.rowcount == 0:
                raise KeyError(rule_id)
        self.audit(actor, "rule.state", str(rule_id), {"state": state})

    def delete_rule(self, rule_id: int, actor: str) -> None:
        """The rule, and its place in aliases. A rule a rule maker made is remembered for it, so it isn't proposed
        again."""
        with self.tx() as db:
            row = db.execute("SELECT * FROM answer_rules WHERE id = ?", (rule_id,)).fetchone()
            if row is None:
                raise KeyError(rule_id)
            db.execute("DELETE FROM answer_rules WHERE id = ?", (rule_id,))
            if row["made_by"] is not None:
                learning = db.execute(
                    "SELECT rejected FROM rule_learning WHERE alias_id = ?", (row["made_by"],)
                ).fetchone()
                rejected = json.loads(learning["rejected"] or "[]") if learning else []
                rejected = [*rejected, rule_key(row["kind"], row["find"], bool(row["negate"]))][-REJECTED_KEEP:]
                db.execute(
                    "INSERT INTO rule_learning (alias_id, rejected) VALUES (?, ?) "
                    "ON CONFLICT (alias_id) DO UPDATE SET rejected = excluded.rejected",
                    (row["made_by"], json.dumps(rejected, ensure_ascii=False)),
                )
            for alias in db.execute("SELECT id, answer_rules FROM aliases").fetchall():
                ids = json.loads(alias["answer_rules"] or "[]")
                if rule_id in ids:
                    kept = json.dumps([i for i in ids if i != rule_id])
                    db.execute("UPDATE aliases SET answer_rules = ? WHERE id = ?", (kept, alias["id"]))
        self.audit(actor, "rule.delete", row["name"])

    def rule_users(self) -> dict[int, list[str]]:
        """Rule id -> the aliases that use it."""
        used: dict[int, list[str]] = {}
        for alias in self.list_aliases():
            for i in alias.answer_rules:
                used.setdefault(i, []).append(alias.name)
        return used

    def add_rule_hits(self, hits: dict[int, tuple[int, float]]) -> None:
        if not hits:
            return
        with self.tx() as db:
            db.executemany(
                "UPDATE answer_rules SET hits = hits + ?, last_hit = MAX(COALESCE(last_hit, 0), ?) WHERE id = ?",
                [(n, at, i) for i, (n, at) in hits.items()],
            )

    def apply_rule_outcomes(self, outcomes: list[tuple[int, bool]]) -> list[str]:
        """Whether the checker agreed with what each rule did or would have done, in order; a rule the model made
        may turn on or go back to watching by itself (answer_rules.next_state, by its alias's settings). What
        changed by itself, in words."""
        if not outcomes:
            return []
        changed: list[str] = []
        updated: dict[int, Rule] = {}
        with self.tx() as db:  # read and written in one go: a change by hand meanwhile isn't undone
            aliases = {a.id: a for a in self.list_aliases()}
            rules = {r.id: r for r in self.list_rules()}
            for rule_id, agreed in outcomes:
                rule = updated.get(rule_id) or rules.get(rule_id)
                if rule is None:
                    continue
                owner = aliases.get(rule.made_by) if rule.made_by is not None else None
                learn, learn_pass = (owner.learn_rules, owner.learn_pass) if owner else ("off", False)
                fields = next_state(rule, agreed, learn, learn_pass)
                if fields.get("state", rule.state) != rule.state:
                    changed.append(f"rule '{rule.name}': {fields['note']}")
                updated[rule_id] = replace(rule, **fields)
            db.executemany(
                "UPDATE answer_rules SET agreed = ?, disagreed = ?, recent = ?, state = ?, awaiting = ?, note = ? "
                "WHERE id = ?",
                [(r.agreed, r.disagreed, r.recent, r.state, int(r.awaiting), r.note, i) for i, r in updated.items()],
            )
        return changed

    def add_made_rules(self, alias_id: int, made: list[dict], actor: str = "rule maker") -> list[str]:
        """Rules an alias's rule maker made (answer_rules.clean_rule's fields, plus why, sample_fails,
        sample_passes, awaiting), attached at the end of its rules, up to RULES_PER_ALIAS, except that one that
        fails or passes answers goes ahead of the rules it made that ask the checker (one of those matching first
        would leave it nothing to decide); a name already taken gets a number. Their names."""
        names: list[str] = []
        with self.tx() as db:
            row = db.execute("SELECT answer_rules FROM aliases WHERE id = ?", (alias_id,)).fetchone()
            if row is None:
                return names
            ids = json.loads(row["answer_rules"] or "[]")
            asking = {
                r["id"]
                for r in db.execute(
                    "SELECT id FROM answer_rules WHERE made_by = ? AND action = 'ask'", (alias_id,)
                ).fetchall()
            }
            taken = {r["name"] for r in db.execute("SELECT name FROM answer_rules").fetchall()}
            for rule in made:
                if len(ids) >= RULES_PER_ALIAS:
                    break
                name, n = rule["name"], 2
                while name in taken:
                    name, n = f"{rule['name'][:56]} ({n})", n + 1
                taken.add(name)
                cur = db.execute(
                    f"INSERT INTO answer_rules ({', '.join(self._RULE_FIELDS)}, made_by, why, sample_fails, "  # noqa: S608
                    f"sample_passes, awaiting, created_at) VALUES ({'?, ' * (len(self._RULE_FIELDS) + 5)}?)",
                    (*self._rule_row({**rule, "name": name}), alias_id, rule.get("why", "")[:300],
                     rule.get("sample_fails", 0), rule.get("sample_passes", 0), int(bool(rule.get("awaiting"))),
                     time.time()),
                )  # fmt: skip
                first_ask = next((n for n, i in enumerate(ids) if i in asking), len(ids))
                if rule.get("action") == "ask":
                    asking.add(cur.lastrowid)
                    ids.append(cur.lastrowid)
                else:
                    ids.insert(first_ask, cur.lastrowid)
                names.append(name)
            db.execute("UPDATE aliases SET answer_rules = ? WHERE id = ?", (json.dumps(ids), alias_id))
        if names:
            self.audit(actor, "rule.made", str(alias_id), {"rules": names})
        return names

    # -- the answers kept for an alias's rule maker ---------------------------------------------

    def add_samples(self, samples: list[tuple[int, str, str, str]], keep: int, days: float) -> None:
        """Checked answers (alias id, "pass" or "fail", the checker's reason, the answer), encrypted; each alias
        keeps its newest ``keep`` of each verdict, none older than ``days``. Its rule maker's counts of new ones go
        up."""
        if not samples:
            return
        now = time.time()
        with self.tx() as db:
            for alias_id, verdict, reason, answer in samples:
                db.execute(
                    "INSERT INTO answer_samples (alias_id, verdict, reason, answer, created_at) VALUES (?, ?, ?, ?, ?)",
                    (alias_id, verdict, reason[:300], self.encrypt(answer), now),
                )
                column = "new_fails" if verdict == "fail" else "new_passes"
                db.execute(
                    f"INSERT INTO rule_learning (alias_id, {column}) VALUES (?, 1) "  # noqa: S608
                    f"ON CONFLICT (alias_id) DO UPDATE SET {column} = {column} + 1",
                    (alias_id,),
                )
            db.execute("DELETE FROM answer_samples WHERE created_at < ?", (now - days * 86_400,))
            for alias_id, verdict in {(s[0], s[1]) for s in samples}:
                db.execute(
                    "DELETE FROM answer_samples WHERE alias_id = ? AND verdict = ? AND id NOT IN "
                    "(SELECT id FROM answer_samples WHERE alias_id = ? AND verdict = ? ORDER BY id DESC LIMIT ?)",
                    (alias_id, verdict, alias_id, verdict, keep),
                )

    def samples_for(self, alias_id: int) -> list[dict]:
        """An alias's kept answers, newest first: verdict, reason, answer."""
        rows = self.query(
            "SELECT verdict, reason, answer FROM answer_samples WHERE alias_id = ? ORDER BY id DESC", (alias_id,)
        )
        return [
            {"verdict": r["verdict"], "reason": r["reason"], "answer": self.reveal(r["answer"]) or ""} for r in rows
        ]

    def sample_counts(self) -> dict[int, dict[str, int]]:
        counts: dict[int, dict[str, int]] = {}
        for r in self.query("SELECT alias_id, verdict, COUNT(*) AS n FROM answer_samples GROUP BY alias_id, verdict"):
            counts.setdefault(r["alias_id"], {})[r["verdict"]] = r["n"]
        return counts

    def forget_samples(self, alias_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM answer_samples WHERE alias_id = ?", (alias_id,))
            db.execute("UPDATE rule_learning SET new_fails = 0, new_passes = 0 WHERE alias_id = ?", (alias_id,))
        self.audit(actor, "samples.forget", str(alias_id))

    def rule_learning(self) -> dict[int, dict]:
        """Alias id -> its rule maker's last run, new answers since, what it last did, and the rules it made that
        you deleted (answer_rules.rule_key)."""
        learning = {}
        for r in self.query("SELECT * FROM rule_learning"):
            learning[r["alias_id"]] = {**dict(r), "rejected": json.loads(r["rejected"] or "[]")}
        return learning

    def made_rule_keys(self, alias_id: int) -> list[list]:
        """What a rule maker shouldn't propose again: the rules it made, attached or not, and those you deleted."""
        made = self.query("SELECT kind, find, negate FROM answer_rules WHERE made_by = ?", (alias_id,))
        keys = [rule_key(r["kind"], r["find"], bool(r["negate"])) for r in made]
        return keys + (self.rule_learning().get(alias_id) or {}).get("rejected", [])

    def prune_samples(self, keep_days: float) -> None:
        """Kept answers older than ``keep_days`` go (also for aliases that keep no new ones)."""
        with self.tx() as db:
            db.execute("DELETE FROM answer_samples WHERE created_at < ?", (time.time() - keep_days * 86_400,))

    def note_rule_maker_run(self, alias_id: int, note: str) -> None:
        """Its rule maker ran (or tried to): when, and what it did; its counts of new answers start again."""
        with self.tx() as db:
            db.execute(
                "INSERT INTO rule_learning (alias_id, last_run, note) VALUES (?, ?, ?) ON CONFLICT (alias_id) "
                "DO UPDATE SET last_run = excluded.last_run, note = excluded.note, new_fails = 0, new_passes = 0",
                (alias_id, time.time(), note[:500]),
            )

    # -- tags and taggers (app/taggers.py) ----------------------------------------------------------

    def list_tags(self) -> list[Tag]:
        return [
            Tag(r["id"], r["name"], r["meaning"], r["given"], r["last_given"], r["created_at"])
            for r in self.query("SELECT * FROM answer_tags ORDER BY name")
        ]

    def create_tag(self, actor: str, name: str, meaning: str = "") -> int:
        """IntegrityError: the name is taken."""
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO answer_tags (name, meaning, created_at) VALUES (?, ?, ?)", (name, meaning, time.time())
            )
        self.audit(actor, "tag.create", name)
        return cur.lastrowid

    def update_tag(self, tag_id: int, actor: str, name: str, meaning: str = "") -> None:
        """IntegrityError: the name is taken; KeyError: no such tag. Requests already logged keep the old name."""
        with self.tx() as db:
            cur = db.execute("UPDATE answer_tags SET name = ?, meaning = ? WHERE id = ?", (name, meaning, tag_id))
            if cur.rowcount == 0:
                raise KeyError(tag_id)
        self.audit(actor, "tag.update", str(tag_id), {"name": name})

    def delete_tag(self, tag_id: int, actor: str) -> None:
        """The tag, from the taggers that give it and from every alias's decisions (one left with no tags goes).
        ValueError("tag_in_use"): a rule gives it (change the rule first); KeyError: no such tag."""
        with self.tx() as db:
            row = db.execute("SELECT name FROM answer_tags WHERE id = ?", (tag_id,)).fetchone()
            if row is None:
                raise KeyError(tag_id)
            if db.execute("SELECT 1 FROM answer_rules WHERE action = 'tag' AND tag_id = ?", (tag_id,)).fetchone():
                raise ValueError("tag_in_use")
            db.execute("DELETE FROM answer_tags WHERE id = ?", (tag_id,))
            for t in db.execute("SELECT id, tags FROM taggers").fetchall():
                ids = json.loads(t["tags"] or "[]")
                if tag_id in ids:
                    kept = json.dumps([i for i in ids if i != tag_id])
                    db.execute("UPDATE taggers SET tags = ? WHERE id = ?", (kept, t["id"]))
            for a in db.execute("SELECT id, tag_chain FROM aliases").fetchall():
                chain = chain_of(a["tag_chain"])
                if (kept := without(chain, tag=tag_id)) != chain:
                    db.execute("UPDATE aliases SET tag_chain = ? WHERE id = ?", (chain_json(kept), a["id"]))
        self.audit(actor, "tag.delete", row["name"])

    def add_tag_hits(self, hits: dict[int, tuple[int, float]]) -> None:
        """Tag id -> (answers it was given to, when last)."""
        if not hits:
            return
        with self.tx() as db:
            db.executemany(
                "UPDATE answer_tags SET given = given + ?, last_given = MAX(COALESCE(last_given, 0), ?) WHERE id = ?",
                [(n, at, i) for i, (n, at) in hits.items()],
            )

    def list_taggers(self) -> list[Tagger]:
        return [
            Tagger(
                r["id"],
                r["name"],
                r["model"] or "",
                r["instructions"] or "",
                r["prompt"] or "",
                tuple(i for i in json.loads(r["tags"] or "[]") if isinstance(i, int)),
                r["pick"] if r["pick"] in PICKS else "any",
                kinds_of(json.loads(r["reads"] or "[]"), ANSWER_KINDS),
                bool(r["sees_message"]),
                r["sees_turns"] or 0,
                bool(r["sees_system"]),
                bool(r["asks_reason"]),
                r["notes"] or "",
                r["created_at"],
            )
            for r in self.query("SELECT * FROM taggers ORDER BY name COLLATE NOCASE")
        ]

    _TAGGER_FIELDS = (
        "name",
        "model",
        "instructions",
        "prompt",
        "tags",
        "pick",
        "reads",
        "sees_message",
        "sees_turns",
        "sees_system",
        "asks_reason",
        "notes",
    )

    @staticmethod
    def _tagger_row(t: dict) -> tuple:
        """``t`` (taggers.clean_tagger) as the columns in ``_TAGGER_FIELDS``."""
        return (
            t["name"],
            t["model"],
            t["instructions"],
            t["prompt"],
            json.dumps(list(t["tags"])),
            t["pick"],
            json.dumps(list(t["reads"])),
            int(t["message"]),
            t["turns"],
            int(t["system"]),
            int(t["reason"]),
            t.get("notes", ""),
        )

    def create_tagger(self, actor: str, **tagger: object) -> int:
        """``tagger``: taggers.clean_tagger's fields. IntegrityError: the name is taken."""
        with self.tx() as db:
            cur = db.execute(
                f"INSERT INTO taggers ({', '.join(self._TAGGER_FIELDS)}, created_at) "  # noqa: S608
                f"VALUES ({'?, ' * len(self._TAGGER_FIELDS)}?)",
                (*self._tagger_row(tagger), time.time()),
            )
        self.audit(actor, "tagger.create", str(tagger["name"]))
        return cur.lastrowid

    def update_tagger(self, tagger_id: int, actor: str, **tagger: object) -> None:
        """IntegrityError: the name is taken; KeyError: no such tagger."""
        sets = ", ".join(f"{name} = ?" for name in self._TAGGER_FIELDS)
        with self.tx() as db:
            cur = db.execute(f"UPDATE taggers SET {sets} WHERE id = ?", (*self._tagger_row(tagger), tagger_id))  # noqa: S608
            if cur.rowcount == 0:
                raise KeyError(tagger_id)
        self.audit(actor, "tagger.update", str(tagger_id), {"name": tagger["name"]})

    def delete_tagger(self, tagger_id: int, actor: str) -> None:
        """The tagger, and its step in every alias's chain (a jump to it goes on to the next tagger instead)."""
        with self.tx() as db:
            row = db.execute("SELECT name FROM taggers WHERE id = ?", (tagger_id,)).fetchone()
            if row is None:
                raise KeyError(tagger_id)
            db.execute("DELETE FROM taggers WHERE id = ?", (tagger_id,))
            for a in db.execute("SELECT id, tag_chain FROM aliases").fetchall():
                chain = chain_of(a["tag_chain"])
                if (kept := without(chain, tagger=tagger_id)) != chain:
                    db.execute("UPDATE aliases SET tag_chain = ? WHERE id = ?", (chain_json(kept), a["id"]))
        self.audit(actor, "tagger.delete", row["name"])

    def tagger_users(self) -> dict[int, list[str]]:
        """Tagger id -> the aliases that run it."""
        used: dict[int, list[str]] = {}
        for alias in self.list_aliases():
            for step in alias.tag_chain:
                used.setdefault(step.tagger, []).append(alias.name)
        return used

    # -- prompt edits (app/edits.py) -----------------------------------------------------------

    @staticmethod
    def _edit(row: sqlite3.Row, children: tuple[Edit, ...] = ()) -> Edit:
        try:
            settings = json.loads(row["settings"] or "{}")
        except ValueError:
            settings = {}
        return Edit(
            row["id"],
            row["name"],
            row["kind"],
            row["state"],
            settings if isinstance(settings, dict) else {},
            row["parent_id"],
            row["notes"],
            row["hits"],
            row["last_hit"],
            row["created_at"],
            children,
        )

    def list_edits(self) -> list[Edit]:
        """The edits, by name, each with those its model made (oldest first)."""
        rows = self.query("SELECT * FROM prompt_edits ORDER BY parent_id IS NOT NULL, name COLLATE NOCASE, id")
        made: dict[int, list[Edit]] = {}
        for row in rows:
            if row["parent_id"] is not None:
                made.setdefault(row["parent_id"], []).append(self._edit(row))
        return [self._edit(r, tuple(made.get(r["id"], ()))) for r in rows if r["parent_id"] is None]

    def create_edit(self, name: str, kind: str, settings: dict, actor: str, notes: str = "", state: str = "on") -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO prompt_edits (name, kind, state, settings, notes, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (name, kind, state, json.dumps(settings, ensure_ascii=False), notes, time.time()),
            )
        self.audit(actor, "edit.create", name, {"kind": kind})
        return cur.lastrowid

    def update_edit(self, edit_id: int, actor: str, **fields: object) -> None:
        """A change to what shapes a rewrite (kind, model, instructions) forgets the kept rewrites: they were made
        by the old ones. Other settings keep them (the time limit, what happens on failure, the shortest share,
        which is checked when a kept rewrite is used, and "only this part": rewrites are kept per text).
        Forgetting and saving are one transaction: a rewrite being kept meanwhile (keep_edit_result) sees either
        the old settings, and is then forgotten, or the new ones, and isn't kept; a save that fails (a name
        already taken) forgets nothing."""
        bad = set(fields) - {"name", "kind", "state", "settings", "notes"}
        if bad:
            raise ValueError(f"cannot update {', '.join(sorted(bad))}")
        settings = fields.get("settings")
        if "settings" in fields:
            fields["settings"] = json.dumps(settings, ensure_ascii=False)
        with self.tx() as db:
            row = db.execute("SELECT settings, kind FROM prompt_edits WHERE id = ?", (edit_id,)).fetchone()
            if row is None:
                raise KeyError(edit_id)
            old = json.loads(row["settings"] or "{}")
            new = settings if isinstance(settings, dict) else old
            shaped = fields.get("kind", row["kind"]) != row["kind"]
            if shaped or any(new.get(k) != old.get(k) for k in ("model", "instructions")):
                db.execute("DELETE FROM prompt_edit_results WHERE edit_id = ?", (edit_id,))
            if fields:
                columns = ", ".join(f"{name} = ?" for name in fields)
                values = [int(v) if isinstance(v, bool) else v for v in fields.values()]
                db.execute(f"UPDATE prompt_edits SET {columns} WHERE id = ?", (*values, edit_id))  # noqa: S608
        self.audit(actor, "edit.update", str(edit_id), {k: v for k, v in fields.items() if k != "settings"})

    def delete_edit(self, edit_id: int, actor: str) -> None:
        """The edit (and what its model made), and its place in aliases, groups and clients."""
        with self.tx() as db:
            row = db.execute("SELECT name FROM prompt_edits WHERE id = ?", (edit_id,)).fetchone()
            if row is None:
                raise KeyError(edit_id)
            db.execute("DELETE FROM prompt_edits WHERE id = ?", (edit_id,))
            for alias in db.execute("SELECT id, edits FROM aliases").fetchall():
                ids = json.loads(alias["edits"] or "[]")
                if edit_id in ids:
                    kept = json.dumps([i for i in ids if i != edit_id])
                    db.execute("UPDATE aliases SET edits = ? WHERE id = ?", (kept, alias["id"]))
            for table in ("clients", "client_groups"):
                for owner in db.execute(f"SELECT id, policy FROM {table}").fetchall():  # noqa: S608
                    policy = ClientPolicy.from_json(owner["policy"])
                    if edit_id in policy.edits:
                        changed = replace(policy, edits=tuple(i for i in policy.edits if i != edit_id))
                        db.execute(f"UPDATE {table} SET policy = ? WHERE id = ?", (changed.to_json(), owner["id"]))  # noqa: S608
        self.audit(actor, "edit.delete", row["name"])

    def edit_users(self) -> dict[int, list[str]]:
        """Edit id -> where it's used ("alias x", "group y", "client z")."""
        used: dict[int, list[str]] = {}
        for alias in self.list_aliases():
            for i in alias.edits:
                used.setdefault(i, []).append(f"alias {alias.name}")
        for group in self.list_groups():
            for i in group.policy.edits:
                used.setdefault(i, []).append(f"group {group.name}")
        for client in self.list_clients():
            for i in client.policy.edits:
                used.setdefault(i, []).append(f"client {client.name}")
        return used

    def add_made_edits(self, parent_id: int, made: list[tuple[str, dict, str]], read: list[str]) -> list[Edit]:
        """Edits a "learn" edit's model made, (kind, settings, state), and the paragraphs it has now read."""
        now = time.time()
        stored = []
        with self.tx() as db:
            if db.execute("SELECT 1 FROM prompt_edits WHERE id = ?", (parent_id,)).fetchone() is None:
                return []  # deleted meanwhile
            for kind, settings, state in made:
                cur = db.execute(
                    "INSERT INTO prompt_edits (parent_id, kind, state, settings, created_at) VALUES (?, ?, ?, ?, ?)",
                    (parent_id, kind, state, json.dumps(settings, ensure_ascii=False), now),
                )
                stored.append(Edit(cur.lastrowid, "", kind, state, settings, parent_id, created_at=now))
            db.executemany(
                "INSERT OR IGNORE INTO prompt_edit_seen (edit_id, hash) VALUES (?, ?)", [(parent_id, h) for h in read]
            )
        if made:
            self.audit("model", "edit.made", str(parent_id), {"made": len(made)})
        return stored

    def update_made_edit(self, parent_id: int, edit_id: int, actor: str, **fields: object) -> None:
        if "settings" in fields:
            fields["settings"] = json.dumps(fields["settings"], ensure_ascii=False)
        rows = self.query("SELECT 1 FROM prompt_edits WHERE id = ? AND parent_id = ?", (edit_id, parent_id))
        if not rows:
            raise KeyError(edit_id)
        self._update("prompt_edits", edit_id, {"state", "settings"}, fields)
        self.audit(actor, "edit.made.update", f"{parent_id}/{edit_id}", {k: v for k, v in fields.items()})

    def set_made_seen(self, fills: list[tuple[int, list[str]]]) -> None:
        """Where made edits were found (the text before and after), for those made before it was kept."""
        with self.tx() as db:
            db.executemany(
                "UPDATE prompt_edits SET settings = json_set(settings, '$.seen', json(?)) "
                "WHERE id = ? AND parent_id IS NOT NULL AND json_valid(settings)",
                [(json.dumps(seen, ensure_ascii=False), edit_id) for edit_id, seen in fills],
            )

    def delete_made_edit(self, parent_id: int, edit_id: int | None, actor: str) -> None:
        """One made edit, or (``edit_id`` None) all of them and what was read: the model reads it all again."""
        with self.tx() as db:
            if edit_id is None:
                db.execute("DELETE FROM prompt_edits WHERE parent_id = ?", (parent_id,))
                db.execute("DELETE FROM prompt_edit_seen WHERE edit_id = ?", (parent_id,))
            else:
                db.execute("DELETE FROM prompt_edits WHERE id = ? AND parent_id = ?", (edit_id, parent_id))
        self.audit(actor, "edit.made.delete", f"{parent_id}/{edit_id or '*'}")

    def approve_made_edits(self, parent_id: int, actor: str) -> int:
        with self.tx() as db:
            cur = db.execute(
                "UPDATE prompt_edits SET state = 'on' WHERE parent_id = ? AND state = 'pending'", (parent_id,)
            )
        self.audit(actor, "edit.made.approve", str(parent_id), {"edits": cur.rowcount})
        return cur.rowcount

    def forget_read(self, edit_id: int, actor: str) -> None:
        """A learn edit reads every paragraph again (its made edits stay)."""
        with self.tx() as db:
            db.execute("DELETE FROM prompt_edit_seen WHERE edit_id = ?", (edit_id,))
        self.audit(actor, "edit.reread", str(edit_id))

    def edit_seen(self, edit_id: int) -> set[str]:
        return {r["hash"] for r in self.query("SELECT hash FROM prompt_edit_seen WHERE edit_id = ?", (edit_id,))}

    def edit_seen_count(self) -> dict[int, int]:
        rows = self.query("SELECT edit_id, COUNT(*) AS n FROM prompt_edit_seen GROUP BY edit_id")
        return {r["edit_id"]: r["n"] for r in rows}

    def edit_result(self, edit_id: int, digest: str) -> str | None:
        rows = self.query("SELECT result FROM prompt_edit_results WHERE edit_id = ? AND hash = ?", (edit_id, digest))
        if not rows:
            return None
        with self.tx() as db:
            db.execute(
                "UPDATE prompt_edit_results SET used_at = ? WHERE edit_id = ? AND hash = ?",
                (time.time(), edit_id, digest),
            )
        return self.reveal(rows[0]["result"])

    def keep_edit_result(
        self, edit_id: int, digest: str, text: str, model: str | None = None, instructions: str | None = None
    ) -> bool:
        """Kept, unless the edit is gone or (given what the rewrite was asked with) its model or instructions
        changed while it was being made: then it's the old instructions' rewrite (False)."""
        with self.tx() as db:
            row = db.execute("SELECT settings FROM prompt_edits WHERE id = ?", (edit_id,)).fetchone()
            if row is None:
                return False
            if model is not None or instructions is not None:
                now = json.loads(row["settings"] or "{}")
                if (now.get("model"), now.get("instructions")) != (model, instructions):
                    return False
            db.execute(
                "INSERT OR REPLACE INTO prompt_edit_results (edit_id, hash, result, used_at) VALUES (?, ?, ?, ?)",
                (edit_id, digest, self.encrypt(text), time.time()),
            )
        return True

    def edit_result_count(self) -> dict[int, int]:
        rows = self.query("SELECT edit_id, COUNT(*) AS n FROM prompt_edit_results GROUP BY edit_id")
        return {r["edit_id"]: r["n"] for r in rows}

    def forget_edit_results(self, edit_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM prompt_edit_results WHERE edit_id = ?", (edit_id,))
        self.audit(actor, "edit.results.delete", str(edit_id))

    def prune_edit_results(self, keep_days: float) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM prompt_edit_results WHERE used_at < ?", (time.time() - keep_days * 86_400,))

    def add_edit_hits(self, hits: dict[int, tuple[int, float]]) -> None:
        if not hits:
            return
        with self.tx() as db:
            db.executemany(
                "UPDATE prompt_edits SET hits = hits + ?, last_hit = MAX(COALESCE(last_hit, 0), ?) WHERE id = ?",
                [(n, at, i) for i, (n, at) in hits.items()],
            )

    def edited_requests(
        self,
        limit: int,
        after: float | None = None,
        before: tuple[float, str] | None = None,
        client: str = "",
        edit: str = "",
        missed: bool = False,
    ) -> list[sqlite3.Row]:
        """Requests whose prompt edits ran (the Live tab), newest first; ``edit`` one that changed (or, with
        ``missed``, found nothing in) them."""
        clauses, params = ["edits IS NOT NULL"], []
        if after is not None:
            clauses.append("ts >= ?")
            params.append(after)
        if before is not None:
            clauses.append("ts <= ? AND (ts < ? OR id < ?)")
            params.extend((before[0], before[0], before[1]))
        if client:
            clauses.append("client = ?")
            params.append(client)
        if edit:
            clauses.append("EXISTS (SELECT 1 FROM json_each(edits, ?) WHERE value = ?)")
            params.extend(("$.missed" if missed else "$.changed", edit))
        elif missed:
            clauses.append("json_array_length(edits, '$.missed') > 0")
        sql = (
            "SELECT id, ts, client, model, target_model, via, outcome, role, edits FROM requests "
            f"WHERE {' AND '.join(clauses)} ORDER BY ts DESC, id DESC LIMIT ?"  # noqa: S608
        )
        return self.read(sql, (*params, limit))

    # -- alerts, webhooks, notifications -------------------------------------------------

    def alert_rule_overrides(self) -> dict[str, sqlite3.Row]:
        return {r["kind"]: r for r in self.query("SELECT * FROM alert_rules")}

    def save_alert_rule(
        self,
        kind: str,
        enabled: bool,
        threshold: float | None,
        window_s: int | None,
        cooldown_s: int | None,
        actor: str,
    ) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT INTO alert_rules (kind, enabled, threshold, window_s, cooldown_s) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(kind) DO UPDATE SET enabled = excluded.enabled, threshold = excluded.threshold, "
                "window_s = excluded.window_s, cooldown_s = excluded.cooldown_s",
                (kind, int(enabled), threshold, window_s, cooldown_s),
            )
        self.audit(
            actor,
            "alert.rule",
            kind,
            {"enabled": enabled, "threshold": threshold, "window_s": window_s, "cooldown_s": cooldown_s},
        )

    def list_webhooks(self) -> list[Webhook]:
        return [
            Webhook(
                r["id"],
                r["name"],
                r["kind"],
                self.decrypt(r["url"]),
                self.decrypt(r["secret"]) if r["secret"] else None,
                r["min_level"],
                bool(r["enabled"]),
                r["last_at"],
                None if r["last_ok"] is None else bool(r["last_ok"]),
                r["last_error"],
            )
            for r in self.query("SELECT * FROM webhooks ORDER BY name")
        ]

    def create_webhook(self, name: str, kind: str, url: str, secret: str | None, min_level: str, actor: str) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO webhooks (name, kind, url, secret, min_level, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (name, kind, self.encrypt(url), self.encrypt(secret) if secret else None, min_level, time.time()),
            )
        self.audit(actor, "webhook.create", name, {"kind": kind, "min_level": min_level})
        return cur.lastrowid

    def update_webhook(self, webhook_id: int, actor: str, **fields: object) -> None:
        if "url" in fields:
            fields["url"] = self.encrypt(str(fields["url"]))
        if "secret" in fields:
            fields["secret"] = self.encrypt(str(fields["secret"])) if fields["secret"] else None
        self._update("webhooks", webhook_id, {"url", "secret", "min_level", "enabled", "kind"}, fields)
        logged = {k: v for k, v in fields.items() if k not in ("url", "secret")}
        logged.update({k: "changed" for k in ("url", "secret") if k in fields})
        self.audit(actor, "webhook.update", str(webhook_id), logged)

    def webhook_result(self, webhook_id: int, ok: bool, error: str | None) -> None:
        with self.tx() as db:
            db.execute(
                "UPDATE webhooks SET last_at = ?, last_ok = ?, last_error = ? WHERE id = ?",
                (time.time(), int(ok), error, webhook_id),
            )

    def delete_webhook(self, webhook_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM webhooks WHERE id = ?", (webhook_id,))
        self.audit(actor, "webhook.delete", str(webhook_id))

    def add_notification(self, level: str, kind: str, title: str, body: str = "") -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO notifications (ts, level, kind, title, body) VALUES (?, ?, ?, ?, ?)",
                (time.time(), level, kind, title, body),
            )
        return cur.lastrowid

    def list_notifications(self, limit: int = 100) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM notifications ORDER BY id DESC LIMIT ?", (limit,))

    def unread_notifications(self) -> int:
        return self.query("SELECT COUNT(*) AS n FROM notifications WHERE read = 0")[0]["n"]

    def mark_notifications_read(self) -> None:
        with self.tx() as db:
            db.execute("UPDATE notifications SET read = 1 WHERE read = 0")

    def prune_notifications(self, keep_days: int = 30, keep_rows: int = 1000) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM notifications WHERE ts < ?", (time.time() - keep_days * 86_400,))
            db.execute(
                "DELETE FROM notifications WHERE id NOT IN (SELECT id FROM notifications ORDER BY id DESC LIMIT ?)",
                (keep_rows,),
            )

    def tokens_on(self, day: str) -> dict[str, int]:
        """Tokens (prompt + completion) per client on a UTC day (YYYY-MM-DD), from the rollup."""
        rows = self.query(
            "SELECT client, SUM(prompt_tokens + completion_tokens) AS n FROM usage_daily WHERE day = ? GROUP BY client",
            (day,),
        )
        return {r["client"]: int(r["n"] or 0) for r in rows}

    # -- async jobs, files, batches ------------------------------------------------------

    def create_jobs(self, jobs: list[dict]) -> None:
        """Insert jobs (``request`` is a dict, stored encrypted)."""
        with self.tx() as db:
            for job in jobs:
                db.execute(
                    "INSERT INTO jobs (id, client, batch_id, custom_id, status, priority, endpoint, request, run_at, "
                    "deadline, idle_only, callback_url, callback_secret, tags, created) "
                    "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        job["id"],
                        job["client"],
                        job.get("batch_id"),
                        job.get("custom_id"),
                        job.get("priority", "low"),
                        job["endpoint"],
                        self.encrypt(json.dumps(job["request"])),
                        job.get("run_at"),
                        job.get("deadline"),
                        int(bool(job.get("idle_only"))),
                        self.encrypt(job["callback_url"]) if job.get("callback_url") else None,
                        self.encrypt(job["callback_secret"]) if job.get("callback_secret") else None,
                        ",".join(job.get("tags") or ()) or None,
                        job.get("created", time.time()),
                    ),
                )

    def get_job(self, job_id: str, client: str | None = None) -> sqlite3.Row | None:
        sql, params = "SELECT * FROM jobs WHERE id = ?", [job_id]
        if client is not None:
            sql += " AND client = ?"
            params.append(client)
        rows = self.query(sql, tuple(params))
        return rows[0] if rows else None

    def job_request(self, row: sqlite3.Row) -> dict:
        return json.loads(self.decrypt(row["request"]))

    def job_result(self, row: sqlite3.Row) -> object:
        return json.loads(self.decrypt(row["result"])) if row["result"] else None

    def list_jobs(
        self,
        client: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        batch_id: str | None = None,
        limit: int = 100,
    ) -> list[sqlite3.Row]:
        sql, params = "SELECT * FROM jobs WHERE 1=1", []
        for column, value in (("client", client), ("status", status), ("batch_id", batch_id)):
            if value:
                sql += f" AND {column} = ?"
                params.append(value)
        if tag:
            sql += " AND (',' || tags || ',') LIKE ?"
            params.append(f"%,{tag},%")
        sql += " ORDER BY created DESC LIMIT ?"
        params.append(limit)
        return self.query(sql, tuple(params))

    def due_jobs(self, now: float, limit: int, idle_too: bool = True) -> list[sqlite3.Row]:
        """``idle_too`` False: leave out "spare capacity only" jobs (live requests are waiting), so they can't fill
        the ``limit`` ahead of jobs that may run now. A job past its deadline is never due, even if it was added
        just after the tick expired the others: the next tick expires it."""
        return self.query(
            "SELECT * FROM jobs WHERE status = 'queued' AND (run_at IS NULL OR run_at <= ?) "
            "AND (deadline IS NULL OR deadline > ?) AND (? OR idle_only = 0) "
            "ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END, created LIMIT ?",
            (now, now, int(idle_too), limit),
        )

    def claim_job(self, job_id: str) -> bool:
        """queued -> running; False if someone else (or a cancel) got there first."""
        with self.tx() as db:
            cur = db.execute(
                "UPDATE jobs SET status = 'running', started = ?, attempts = attempts + 1 "
                "WHERE id = ? AND status = 'queued'",
                (time.time(), job_id),
            )
        return cur.rowcount == 1

    def finish_job(self, job_id: str, status: str, **fields: object) -> None:
        if "result" in fields and fields["result"] is not None:
            fields["result"] = self.encrypt(json.dumps(fields["result"]))
        fields = {"status": status, "finished": time.time(), **fields}
        columns = ", ".join(f"{name} = ?" for name in fields)
        with self.tx() as db:
            db.execute(f"UPDATE jobs SET {columns} WHERE id = ?", (*fields.values(), job_id))  # noqa: S608

    def set_job_fields(self, job_id: str, **fields: object) -> None:
        columns = ", ".join(f"{name} = ?" for name in fields)
        with self.tx() as db:
            db.execute(f"UPDATE jobs SET {columns} WHERE id = ?", (*fields.values(), job_id))  # noqa: S608

    def cancel_jobs(self, job_ids: list[str] | None = None, batch_id: str | None = None) -> int:
        """Cancel jobs that haven't started (running ones finish)."""
        with self.tx() as db:
            if batch_id is not None:
                cur = db.execute(
                    "UPDATE jobs SET status = 'cancelled', finished = ? WHERE batch_id = ? AND status = 'queued'",
                    (time.time(), batch_id),
                )
            else:
                marks = ",".join("?" for _ in job_ids or [])
                cur = db.execute(
                    f"UPDATE jobs SET status = 'cancelled', finished = ? WHERE status = 'queued' AND id IN ({marks})",  # noqa: S608
                    (time.time(), *(job_ids or [])),
                )
        return cur.rowcount

    def retry_job(self, job_id: str) -> bool:
        """Run it again from scratch: its attempts start over (else a 429 wouldn't be retried), and a deadline
        already past is lifted (else it would expire again before it could start)."""
        with self.tx() as db:
            cur = db.execute(
                "UPDATE jobs SET status = 'queued', finished = NULL, started = NULL, error = NULL, result = NULL, "
                "status_code = NULL, attempts = 0, deadline = CASE WHEN deadline <= ? THEN NULL ELSE deadline END "
                "WHERE id = ? AND status IN ('failed', 'expired', 'cancelled')",
                (time.time(), job_id),
            )
        return cur.rowcount == 1

    def open_batch_ids(self) -> list[str]:
        return [r["id"] for r in self.query("SELECT id FROM batches WHERE status IN ('in_progress', 'cancelling')")]

    def pending_callbacks(self, within_s: float = 86_400) -> list[str]:
        """Finished jobs (last day) whose callback never completed: a shutdown interrupted it."""
        done = ",".join(f"'{s}'" for s in JOB_DONE)
        rows = self.query(
            f"SELECT id FROM jobs WHERE status IN ({done}) AND callback_url IS NOT NULL "  # noqa: S608
            "AND callback_status IS NULL AND finished > ?",
            (time.time() - within_s,),
        )
        return [r["id"] for r in rows]

    def requeue_running_jobs(self, max_attempts: int) -> int:
        """After a restart: jobs that were running start again (or fail after too many tries)."""
        with self.tx() as db:
            db.execute(
                "UPDATE jobs SET status = 'failed', finished = ?, error = 'interrupted by restarts too often' "
                "WHERE status = 'running' AND attempts >= ?",
                (time.time(), max_attempts),
            )
            cur = db.execute("UPDATE jobs SET status = 'queued', started = NULL WHERE status = 'running'")
        return cur.rowcount

    def expire_jobs(self, now: float) -> list[str]:
        """Queued jobs past their deadline -> expired; returns their batch ids."""
        rows = self.query(
            "SELECT id, batch_id FROM jobs WHERE status = 'queued' AND deadline IS NOT NULL AND deadline < ?", (now,)
        )
        if rows:
            with self.tx() as db:
                db.executemany(
                    "UPDATE jobs SET status = 'expired', finished = ?, error = 'deadline passed before it could run' "
                    "WHERE id = ? AND status = 'queued'",
                    [(now, r["id"]) for r in rows],
                )
        return sorted({r["batch_id"] for r in rows if r["batch_id"]})

    def jobs_by_status(self) -> dict[str, int]:
        return {r["status"]: r["n"] for r in self.query("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status")}

    def job_counts(self, batch_id: str) -> dict[str, int]:
        rows = self.query("SELECT status, COUNT(*) AS n FROM jobs WHERE batch_id = ? GROUP BY status", (batch_id,))
        return {r["status"]: r["n"] for r in rows}

    def prune_jobs(self, keep_days: float) -> None:
        cutoff = time.time() - keep_days * 86_400
        with self.tx() as db:
            db.execute(f"DELETE FROM jobs WHERE status IN {JOB_DONE!r} AND finished < ?", (cutoff,))  # noqa: S608
            db.execute("DELETE FROM batches WHERE finished_at IS NOT NULL AND finished_at < ?", (cutoff,))
            db.execute(
                "DELETE FROM files WHERE created < ? AND id NOT IN (SELECT input_file_id FROM batches) "
                "AND id NOT IN (SELECT output_file_id FROM batches WHERE output_file_id IS NOT NULL) "
                "AND id NOT IN (SELECT error_file_id FROM batches WHERE error_file_id IS NOT NULL)",
                (cutoff,),
            )

    def create_schedule(self, s: dict) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT INTO schedules (id, client, name, cron, endpoint, request, priority, tags, idle_only, "
                "deadline_s, callback_url, callback_secret, enabled, created, next_run) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    s["id"],
                    s["client"],
                    s["name"],
                    s["cron"],
                    s["endpoint"],
                    self.encrypt(json.dumps(s["request"])),
                    s.get("priority", "low"),
                    ",".join(s.get("tags") or ()) or None,
                    int(bool(s.get("idle_only"))),
                    s.get("deadline_s"),
                    self.encrypt(s["callback_url"]) if s.get("callback_url") else None,
                    self.encrypt(s["callback_secret"]) if s.get("callback_secret") else None,
                    int(s.get("enabled", True)),
                    time.time(),
                    s["next_run"],
                ),
            )

    def list_schedules(self, client: str | None = None) -> list[sqlite3.Row]:
        if client is None:
            return self.query("SELECT * FROM schedules ORDER BY client, name")
        return self.query("SELECT * FROM schedules WHERE client = ? ORDER BY name", (client,))

    def get_schedule(self, schedule_id: str, client: str | None = None) -> sqlite3.Row | None:
        sql, params = "SELECT * FROM schedules WHERE id = ?", [schedule_id]
        if client is not None:
            sql += " AND client = ?"
            params.append(client)
        rows = self.query(sql, tuple(params))
        return rows[0] if rows else None

    def due_schedules(self, now: float) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM schedules WHERE enabled = 1 AND next_run <= ? ORDER BY next_run", (now,))

    def set_schedule(self, schedule_id: str, **fields: object) -> None:
        columns = ", ".join(f"{name} = ?" for name in fields)
        values = [int(v) if isinstance(v, bool) else v for v in fields.values()]
        with self.tx() as db:
            db.execute(f"UPDATE schedules SET {columns} WHERE id = ?", (*values, schedule_id))  # noqa: S608

    def delete_schedule(self, schedule_id: str, client: str | None = None) -> bool:
        with self.tx() as db:
            if client is None:
                cur = db.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
            else:
                cur = db.execute("DELETE FROM schedules WHERE id = ? AND client = ?", (schedule_id, client))
        return cur.rowcount == 1

    def create_file(self, file_id: str, client: str, purpose: str, filename: str, data: bytes) -> sqlite3.Row:
        with self.tx() as db:
            db.execute(
                "INSERT INTO files (id, client, purpose, filename, bytes, created, data) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (file_id, client, purpose, filename, len(data), time.time(), self.encrypt(data.decode("utf-8"))),
            )
        return self.get_file(file_id, client)

    def get_file(self, file_id: str, client: str) -> sqlite3.Row | None:
        rows = self.query("SELECT * FROM files WHERE id = ? AND client = ?", (file_id, client))
        return rows[0] if rows else None

    def file_content(self, row: sqlite3.Row) -> str:
        return self.decrypt(row["data"])

    def delete_file(self, file_id: str, client: str) -> bool:
        with self.tx() as db:
            cur = db.execute("DELETE FROM files WHERE id = ? AND client = ?", (file_id, client))
        return cur.rowcount == 1

    def create_batch(self, batch: dict) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT INTO batches (id, client, endpoint, input_file_id, completion_window, status, created, "
                "expires_at, total, metadata) VALUES (?, ?, ?, ?, ?, 'in_progress', ?, ?, ?, ?)",
                (
                    batch["id"],
                    batch["client"],
                    batch["endpoint"],
                    batch["input_file_id"],
                    batch["completion_window"],
                    batch["created"],
                    batch["expires_at"],
                    batch["total"],
                    json.dumps(batch.get("metadata")) if batch.get("metadata") else None,
                ),
            )

    def get_batch(self, batch_id: str, client: str | None = None) -> sqlite3.Row | None:
        sql, params = "SELECT * FROM batches WHERE id = ?", [batch_id]
        if client is not None:
            sql += " AND client = ?"
            params.append(client)
        rows = self.query(sql, tuple(params))
        return rows[0] if rows else None

    def list_batches(self, client: str, limit: int = 20, after: str | None = None) -> list[sqlite3.Row]:
        if after and (anchor := self.get_batch(after, client)):
            return self.query(
                "SELECT * FROM batches WHERE client = ? AND created < ? ORDER BY created DESC LIMIT ?",
                (client, anchor["created"], limit),
            )
        return self.query("SELECT * FROM batches WHERE client = ? ORDER BY created DESC LIMIT ?", (client, limit))

    def update_batch(self, batch_id: str, **fields: object) -> None:
        columns = ", ".join(f"{name} = ?" for name in fields)
        with self.tx() as db:
            db.execute(f"UPDATE batches SET {columns} WHERE id = ?", (*fields.values(), batch_id))  # noqa: S608

    # -- opt-in content logging ----------------------------------------------------------

    def save_contents(self, rows: list[tuple]) -> None:
        """(request id, ts, client, request text, reply text, truncated[, what prompt edits changed as JSON])."""
        with self.tx() as db:
            db.executemany(
                "INSERT OR REPLACE INTO request_content (id, ts, client, request, response, truncated, edits) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        i,
                        ts,
                        storable(c),
                        self.encrypt(req),
                        self.encrypt(resp),
                        int(cut),
                        self.encrypt(rest[0]) if rest and rest[0] else None,
                    )
                    for i, ts, c, req, resp, cut, *rest in rows
                ],
            )

    def get_content(self, request_id: str) -> dict | None:
        rows = self.query("SELECT * FROM request_content WHERE id = ?", (request_id,))
        if not rows:
            return None
        row = rows[0]
        try:
            edits = json.loads(self.decrypt(row["edits"])) if row["edits"] else None
        except ValueError:
            edits = None
        return {
            "id": row["id"],
            "ts": row["ts"],
            "client": row["client"],
            "request": self.decrypt(row["request"]),
            "response": self.decrypt(row["response"]),
            "truncated": bool(row["truncated"]),
            "edits": edits if isinstance(edits, dict) else None,
        }

    def logged_requests(self, limit: int = 30) -> list[sqlite3.Row]:
        """The newest requests with logged content (to try prompt edits on)."""
        return self.read("SELECT id, ts, client FROM request_content ORDER BY ts DESC LIMIT ?", (limit,))

    def content_ids(self, request_ids: list[str]) -> set[str]:
        if not request_ids:
            return set()
        marks = ",".join("?" for _ in request_ids)
        rows = self.query(f"SELECT id FROM request_content WHERE id IN ({marks})", tuple(request_ids))  # noqa: S608
        return {r["id"] for r in rows}

    def purge_content(self, client: str | None, actor: str) -> int:
        with self.tx() as db:
            if client is None:
                cur = db.execute("DELETE FROM request_content")
                db.execute("UPDATE requests SET timeline = NULL WHERE timeline IS NOT NULL")
            else:
                cur = db.execute("DELETE FROM request_content WHERE client = ?", (client,))
                db.execute("UPDATE requests SET timeline = NULL WHERE client = ? AND timeline IS NOT NULL", (client,))
        self.audit(actor, "content.purge", client or "*", {"rows": cur.rowcount})
        return cur.rowcount

    def prune_content(self, keep_days: float) -> None:
        with self.tx() as db:
            cutoff = time.time() - keep_days * 86_400
            db.execute("DELETE FROM request_content WHERE ts < ?", (cutoff,))
            # council / orchestrator timelines may hold (encrypted) excerpts: kept as long as logged content
            db.execute("UPDATE requests SET timeline = NULL WHERE ts < ? AND timeline IS NOT NULL", (cutoff,))

    # -- compaction summaries (saved only when "Save summaries" is on) --------------------

    def save_summary(self, key: str, client: str, data: dict) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO compaction_summaries (key, client, data, updated_at) VALUES (?, ?, ?, ?)",
                (key, client, self.encrypt(json.dumps(data)), time.time()),
            )

    def load_summary(self, key: str) -> dict | None:
        rows = self.query("SELECT data FROM compaction_summaries WHERE key = ?", (key,))
        if not rows:
            return None
        try:
            return json.loads(self.decrypt(rows[0]["data"]))
        except (ValueError, TypeError):
            return None

    def prune_summaries(self, keep_days: float) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM compaction_summaries WHERE updated_at < ?", (time.time() - keep_days * 86_400,))

    # -- Responses API conversations ----------------------------------------------------

    def save_response(self, response_id: str, client: str, messages: list[dict]) -> None:
        with self.tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO stored_responses (id, client, created, data) VALUES (?, ?, ?, ?)",
                (response_id, client, time.time(), self.encrypt(json.dumps(messages))),
            )

    def load_response(self, response_id: str, client: str) -> list[dict] | None:
        """The whole conversation up to that response, or None (unknown, expired or another client's)."""
        rows = self.query("SELECT data FROM stored_responses WHERE id = ? AND client = ?", (response_id, client))
        return json.loads(self.decrypt(rows[0]["data"])) if rows else None

    def prune_responses(self, keep_days: float) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM stored_responses WHERE created < ?", (time.time() - keep_days * 86_400,))

    # -- admin users and sessions --------------------------------------------------------

    def count_admins(self, role: str | None = None, enabled_only: bool = False) -> int:
        sql, params = "SELECT COUNT(*) AS n FROM admin_users WHERE 1=1", []
        if role:
            sql += " AND role = ?"
            params.append(role)
        if enabled_only:
            sql += " AND enabled = 1"
        return self.query(sql, tuple(params))[0]["n"]

    def list_admins(self) -> list[AdminUser]:
        return [_admin(r) for r in self.query("SELECT * FROM admin_users ORDER BY username")]

    def get_admin(self, user_id: int | None = None, username: str | None = None) -> AdminUser | None:
        if user_id is not None:
            rows = self.query("SELECT * FROM admin_users WHERE id = ?", (user_id,))
        else:
            rows = self.query("SELECT * FROM admin_users WHERE username = ?", (username,))
        return _admin(rows[0]) if rows else None

    def password_hash(self, user_id: int) -> str:
        return self.query("SELECT password_hash FROM admin_users WHERE id = ?", (user_id,))[0]["password_hash"]

    def create_admin(self, username: str, password_hash: str, role: str, actor: str) -> int:
        with self.tx() as db:
            cur = db.execute(
                "INSERT INTO admin_users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
                (username, password_hash, role, time.time()),
            )
        self.audit(actor, "user.create", username, {"role": role})
        return cur.lastrowid

    def update_admin(self, user_id: int, actor: str, **fields: object) -> None:
        allowed = {"role", "enabled", "password_hash", "can_view_content"}
        self._update("admin_users", user_id, allowed, fields)
        if "password_hash" in fields or fields.get("enabled") is False:
            self.delete_sessions(user_id)
        logged = {k: v for k, v in fields.items() if k != "password_hash"}
        if "password_hash" in fields:
            logged["password"] = "changed"
        self.audit(actor, "user.update", str(user_id), logged)

    def totp_state(self, user_id: int) -> tuple[str | None, int, list[str]]:
        """(secret or None, last used time step, recovery code hashes)."""
        row = self.query("SELECT totp_secret, totp_last, recovery_codes FROM admin_users WHERE id = ?", (user_id,))[0]
        secret = self.decrypt(row["totp_secret"]) if row["totp_secret"] else None
        return secret, row["totp_last"], json.loads(row["recovery_codes"] or "[]")

    def set_totp(self, user_id: int, secret: str | None, recovery_hashes: list[str], actor: str) -> None:
        """Turn two-factor login on (secret) or off (None)."""
        with self.tx() as db:
            db.execute(
                "UPDATE admin_users SET totp_secret = ?, totp_last = -1, recovery_codes = ? WHERE id = ?",
                (self.encrypt(secret) if secret else None, json.dumps(recovery_hashes), user_id),
            )
        self.audit(actor, "user.2fa", str(user_id), {"enabled": secret is not None})

    def totp_used(self, user_id: int, counter: int) -> None:
        with self.tx() as db:
            db.execute("UPDATE admin_users SET totp_last = MAX(totp_last, ?) WHERE id = ?", (counter, user_id))

    def use_recovery_code(self, user_id: int, code_hash: str) -> bool:
        """Consume a recovery code; False if it isn't (or no longer) valid."""
        with self.tx() as db:
            row = db.execute("SELECT recovery_codes FROM admin_users WHERE id = ?", (user_id,)).fetchone()
            codes = json.loads(row["recovery_codes"] or "[]") if row else []
            if code_hash not in codes:
                return False
            codes.remove(code_hash)
            db.execute("UPDATE admin_users SET recovery_codes = ? WHERE id = ?", (json.dumps(codes), user_id))
        return True

    def delete_admin(self, user_id: int, actor: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM admin_users WHERE id = ?", (user_id,))
        self.audit(actor, "user.delete", str(user_id))

    def create_session(self, user_id: int, token_hash: str, csrf: str, ip: str, ttl_s: float) -> None:
        now = time.time()
        with self.tx() as db:
            db.execute("DELETE FROM admin_sessions WHERE expires_at < ?", (now,))
            db.execute(
                "INSERT INTO admin_sessions (token_hash, user_id, csrf, ip, created_at, last_seen, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (token_hash, user_id, csrf, ip, now, now, now + ttl_s),
            )

    def get_session(self, token_hash: str) -> sqlite3.Row | None:
        rows = self.query("SELECT * FROM admin_sessions WHERE token_hash = ?", (token_hash,))
        return rows[0] if rows else None

    def touch_session(self, token_hash: str) -> None:
        with self.tx() as db:
            db.execute("UPDATE admin_sessions SET last_seen = ? WHERE token_hash = ?", (time.time(), token_hash))

    def delete_session(self, token_hash: str) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM admin_sessions WHERE token_hash = ?", (token_hash,))

    def delete_sessions(self, user_id: int) -> None:
        with self.tx() as db:
            db.execute("DELETE FROM admin_sessions WHERE user_id = ?", (user_id,))

    def list_audit(self, limit: int = 200) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,))

    # -- helpers -------------------------------------------------------------------------

    def _update(self, table: str, row_id: int, allowed: set[str], fields: dict[str, object]) -> None:
        bad = set(fields) - allowed
        if bad:
            raise ValueError(f"cannot update {', '.join(sorted(bad))}")
        if not fields:
            return
        columns = ", ".join(f"{name} = ?" for name in fields)
        values = [int(v) if isinstance(v, bool) else v for v in fields.values()]
        with self.tx() as db:
            cur = db.execute(f"UPDATE {table} SET {columns} WHERE id = ?", (*values, row_id))  # noqa: S608
            if cur.rowcount == 0:
                raise KeyError(row_id)
