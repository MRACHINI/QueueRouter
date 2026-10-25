"""Async jobs and the OpenAI-compatible Batch API.

A job is a normal API request that QueueRouter runs later, without the caller waiting: stored in
SQLite (request and result encrypted), so it survives restarts. The worker feeds due jobs into the
same pipeline as live requests (policies, aliases, fallbacks, usage), at the job's priority
(``low`` by default) and never more than ``max_jobs_in_flight`` at once, so interactive users keep
their slots. Batches are groups of jobs made from an uploaded JSONL file.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import socket
import sqlite3
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx

from . import cron
from .store import JOB_DONE, Store

log = logging.getLogger("queuerouter")

MAX_ATTEMPTS = 3  # restarts while running
RETRY_STATUSES = (429, 502, 503, 504)  # passing refusals: the job waits and runs again
RETRY_ATTEMPTS = 8


def retry_after(status: int, attempts: int) -> float | None:
    """Seconds before a job refused with a passing status runs again (backing off), or None."""
    if status not in RETRY_STATUSES or attempts >= RETRY_ATTEMPTS:
        return None
    return float(min(600, 15 * 2 ** (attempts - 1)))


BATCH_ENDPOINTS = ("/v1/chat/completions", "/v1/completions", "/v1/embeddings")
WINDOWS = {"24h": 86_400}
MAX_BATCH_LINES = 50_000
CALLBACK_RETRY_S = (5, 30, 120)


class JobError(ValueError):
    """Bad job / batch input (answered with a 400)."""


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


def parse_time(value) -> float | None:
    """Epoch seconds or ISO 8601 (``Z`` allowed) -> epoch seconds."""
    if value is None or value == "":
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if moment.tzinfo is None:  # no offset given: UTC, like cron and quotas (not the server's local zone)
            moment = moment.replace(tzinfo=UTC)
        return moment.timestamp()
    except ValueError as exc:
        raise JobError(f"invalid time {value!r} (use ISO 8601 or epoch seconds)") from exc


# QueueRouter's own endpoints and the API adapters (/v1/messages, /v1/responses) can't run as jobs: a job is
# sent to the provider as it is.
NOT_JOBS = ("jobs", "batches", "files", "schedules", "messages", "responses", "queue", "models", "opencode-config")


def check_endpoint(endpoint: str) -> str:
    endpoint = "/" + endpoint.strip().lstrip("/")
    if not endpoint.startswith("/v1/") or endpoint.split("/")[2] in NOT_JOBS or ".." in endpoint:
        raise JobError(f"endpoint {endpoint!r} can't run as a job")
    return endpoint


def check_callback(url: str | None) -> str | None:
    """Callbacks must be HTTPS to a public host (never into the server's own network)."""
    if not url:
        return None
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        raise JobError("callback_url must be an https:// URL")
    try:
        port = parts.port
    except ValueError as exc:
        raise JobError("callback_url has an invalid port") from exc
    if port not in (None, 443):
        raise JobError("callback_url must use the standard HTTPS port (443)")
    if parts.hostname == "localhost" or parts.hostname.endswith((".localhost", ".internal", ".local")):
        raise JobError("callback_url must be a public host")
    try:
        literal = ipaddress.ip_address(parts.hostname)
    except ValueError:
        literal = None  # a name: checked again when sending (after DNS)
    if literal is not None and not literal.is_global:
        raise JobError("callback_url must be a public host")
    return url


async def public_address(host: str) -> str | None:
    """One address of ``host`` if every address it resolves to is public, else None."""
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError:
        return None
    addresses = [ipaddress.ip_address(info[4][0].split("%")[0]) for info in infos]
    if not addresses or not all(a.is_global for a in addresses):
        return None
    return str(addresses[0])


def pinned_url(url: str, address: str) -> str:
    """``url`` with its host replaced by ``address`` (the TLS name and Host header stay the host's)."""
    parts = urlsplit(url)
    host = f"[{address}]" if ":" in address else address
    return parts._replace(netloc=host).geturl()


def new_job(client: str, body: dict, priority: str = "low") -> dict:
    """A job from a ``POST /v1/jobs`` body."""
    if not isinstance(body, dict) or not isinstance(body.get("request"), dict):
        raise JobError("'request' (the API request body) is required")
    request = dict(body["request"])
    request.pop("stream", None)
    request.pop("stream_options", None)
    tags = body.get("tags") or []
    if isinstance(tags, str):
        tags = [t for t in tags.split(",") if t]
    if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
        raise JobError("'tags' must be a list of strings")
    run_at, deadline = parse_time(body.get("run_at")), parse_time(body.get("deadline"))
    if run_at and deadline and deadline <= run_at:
        raise JobError("'deadline' must be after 'run_at'")
    secret = body.get("callback_secret")
    if secret is not None and (not isinstance(secret, str) or len(secret) > 500):
        raise JobError("'callback_secret' must be a string (up to 500 characters)")
    wanted = body.get("priority", priority)
    if wanted not in ("low", "normal", "high"):
        raise JobError("'priority' must be low, normal or high")
    return {
        "id": new_id("job"),
        "client": client,
        "priority": wanted,
        "endpoint": check_endpoint(str(body.get("endpoint") or "/v1/chat/completions")),
        "request": request,
        "run_at": run_at,
        "deadline": deadline,
        "idle_only": bool(body.get("idle_only")),
        "callback_url": check_callback(body.get("callback_url")),
        "callback_secret": body.get("callback_secret") or None,
        "tags": [t.strip().lower()[:64] for t in tags][:10],
    }


def job_view(store: Store, row: sqlite3.Row, with_result: bool = True) -> dict:
    status = row["status"]
    if status == "queued" and row["run_at"] and row["run_at"] > time.time():
        status = "scheduled"
    view = {
        "id": row["id"],
        "object": "job",
        "status": status,
        "endpoint": row["endpoint"],
        "priority": row["priority"],
        "model": row["model"],
        "batch_id": row["batch_id"],
        "custom_id": row["custom_id"],
        "tags": [t for t in (row["tags"] or "").split(",") if t],
        "created_at": row["created"],
        "run_at": row["run_at"],
        "deadline": row["deadline"],
        "idle_only": bool(row["idle_only"]),
        "started_at": row["started"],
        "finished_at": row["finished"],
        "attempts": row["attempts"],
        "status_code": row["status_code"],
        "usage": {"prompt_tokens": row["prompt_tokens"], "completion_tokens": row["completion_tokens"]}
        if row["prompt_tokens"] is not None
        else None,
        "error": row["error"],
        "callback_status": row["callback_status"],
    }
    if with_result and row["status"] in JOB_DONE:
        view["result"] = store.job_result(row)
    return view


# -- files and batches -----------------------------------------------------------------------


def file_view(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "object": "file",
        "bytes": row["bytes"],
        "created_at": int(row["created"]),
        "filename": row["filename"],
        "purpose": row["purpose"],
        "status": "processed",
    }


def batch_jobs(client: str, batch_id: str, endpoint: str, content: str, deadline: float) -> list[dict]:
    """Jobs from the input JSONL (one request per line, as OpenAI batch files)."""
    jobs, seen = [], set()
    lines = [line for line in content.splitlines() if line.strip()]
    if not lines:
        raise JobError("the input file is empty")
    if len(lines) > MAX_BATCH_LINES:
        raise JobError(f"at most {MAX_BATCH_LINES} requests per batch")
    for number, line in enumerate(lines, 1):
        try:
            item = json.loads(line)
        except ValueError as exc:
            raise JobError(f"line {number} isn't valid JSON") from exc
        custom_id = item.get("custom_id") if isinstance(item, dict) else None
        if not custom_id or custom_id in seen:
            raise JobError(f"line {number}: 'custom_id' must be present and unique")
        seen.add(custom_id)
        if item.get("url") != endpoint or str(item.get("method", "POST")).upper() != "POST":
            raise JobError(f"line {number}: every request must be POST {endpoint}")
        if not isinstance(item.get("body"), dict):
            raise JobError(f"line {number}: 'body' must be an object")
        body = dict(item["body"])
        body.pop("stream", None)
        jobs.append(
            {
                "id": new_id("job"),
                "client": client,
                "batch_id": batch_id,
                "custom_id": str(custom_id),
                "priority": "low",
                "endpoint": endpoint,
                "request": body,
                "deadline": deadline,
                "tags": ["batch"],
            }
        )
    return jobs


def batch_view(store: Store, row: sqlite3.Row) -> dict:
    counts = store.job_counts(row["id"])
    done = counts.get("succeeded", 0)
    failed = counts.get("failed", 0) + counts.get("expired", 0)
    return {
        "id": row["id"],
        "object": "batch",
        "endpoint": row["endpoint"],
        "errors": None,
        "input_file_id": row["input_file_id"],
        "completion_window": row["completion_window"],
        "status": row["status"],
        "output_file_id": row["output_file_id"],
        "error_file_id": row["error_file_id"],
        "created_at": int(row["created"]),
        "in_progress_at": int(row["created"]),
        "expires_at": int(row["expires_at"]) if row["expires_at"] else None,
        "finalizing_at": None,
        "completed_at": int(row["finished_at"]) if row["finished_at"] and row["status"] == "completed" else None,
        "failed_at": None,
        "expired_at": None,
        "cancelling_at": int(row["cancelled_at"]) if row["cancelled_at"] else None,
        "cancelled_at": int(row["finished_at"]) if row["finished_at"] and row["status"] == "cancelled" else None,
        "request_counts": {"total": row["total"], "completed": done, "failed": failed},
        "metadata": json.loads(row["metadata"]) if row["metadata"] else None,
    }


_BATCH_LOCK = threading.Lock()  # two jobs of a batch finishing at once must not both write its files


def finish_batch_if_done(store: Store, batch_id: str) -> bool:
    """Once every job of a batch is done, write the output / error files (OpenAI's format)."""
    with _BATCH_LOCK:
        return _finish_batch(store, batch_id)


def _finish_batch(store: Store, batch_id: str) -> bool:
    batch = store.get_batch(batch_id)
    if batch is None or batch["status"] not in ("in_progress", "cancelling"):
        return False
    counts = store.job_counts(batch_id)
    if any(status not in JOB_DONE for status in counts):
        return False
    outputs, errors = [], []
    for row in reversed(store.list_jobs(batch_id=batch_id, limit=MAX_BATCH_LINES)):
        line_id = f"batch_req_{row['id'][4:]}"
        if row["status"] == "succeeded":
            outputs.append(
                {
                    "id": line_id,
                    "custom_id": row["custom_id"],
                    "response": {
                        "status_code": row["status_code"],
                        "request_id": row["id"],
                        "body": store.job_result(row),
                    },
                    "error": None,
                }
            )
        else:
            code = {"expired": "batch_expired", "cancelled": "batch_cancelled"}.get(row["status"], "request_failed")
            errors.append(
                {
                    "id": line_id,
                    "custom_id": row["custom_id"],
                    "response": {
                        "status_code": row["status_code"],
                        "request_id": row["id"],
                        "body": store.job_result(row),
                    }
                    if row["status_code"]
                    else None,
                    "error": {"code": code, "message": row["error"] or row["status"]},
                }
            )
    fields: dict = {
        "status": "cancelled" if batch["status"] == "cancelling" else "completed",
        "finished_at": time.time(),
    }
    for name, lines in (("output_file_id", outputs), ("error_file_id", errors)):
        if lines:
            file_id = new_id("file")
            content = "".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines)
            store.create_file(
                file_id, batch["client"], "batch_output", f"{batch_id}_{name[:-8]}.jsonl", content.encode()
            )
            fields[name] = file_id
    store.update_batch(batch_id, **fields)
    return True


# -- the worker ------------------------------------------------------------------------------


class JobRunner:
    """Moves due jobs into the pipeline. ``execute(job_row, request) -> (status_code, body bytes)``."""

    def __init__(
        self,
        store: Store,
        execute: Callable[[sqlite3.Row, dict], Awaitable[tuple[int, bytes]]],
        http: Callable[[], httpx.AsyncClient],
        usage_of: Callable[[bytes], tuple],
    ) -> None:
        self.store = store
        self.execute = execute
        self._http = http
        self._usage_of = usage_of
        self.running: dict[str, asyncio.Task] = {}
        self._callbacks: set[asyncio.Task] = set()

    def start(self) -> int:
        """At startup: jobs a previous run was in the middle of go back in the queue, and callbacks a shutdown
        interrupted are sent again."""
        for job_id in self.store.pending_callbacks():
            task = asyncio.get_running_loop().create_task(self._callback(job_id))
            self._callbacks.add(task)
            task.add_done_callback(self._callbacks.discard)
        return self.store.requeue_running_jobs(MAX_ATTEMPTS + RETRY_ATTEMPTS)  # retries count as attempts too

    async def tick(self, max_in_flight: int, live_waiting: int) -> None:
        now = time.time()
        for schedule in await asyncio.to_thread(self.store.due_schedules, now):
            await asyncio.to_thread(fire_schedule, self.store, schedule, now)
        for batch_id in await asyncio.to_thread(self.store.expire_jobs, now):
            await asyncio.to_thread(finish_batch_if_done, self.store, batch_id)
        free = max_in_flight - len(self.running)
        if free <= 0:
            return
        for row in await asyncio.to_thread(self.store.due_jobs, now, free * 4):
            if free <= 0:
                break
            if row["idle_only"] and live_waiting > 0:
                continue  # spare capacity only
            if row["id"] in self.running or not await asyncio.to_thread(self.store.claim_job, row["id"]):
                continue
            free -= 1
            task = asyncio.get_running_loop().create_task(self._run(row))
            self.running[row["id"]] = task
            task.add_done_callback(lambda _, job_id=row["id"]: self.running.pop(job_id, None))

    async def _run(self, row: sqlite3.Row) -> None:
        job_id = row["id"]
        finishing = False  # once its result is being written, a shutdown must not queue it again
        try:
            request = self.store.job_request(row)
            status, body = await self.execute(row, request)
            try:
                result = json.loads(body) if body else None
            except ValueError:
                result = {"raw": body.decode("utf-8", "replace")[:100_000]}
            fields: dict = {"status_code": status, "result": result, "model": request.get("model")}
            message = (result or {}).get("error", {}).get("message") if isinstance(result, dict) else None
            delay = retry_after(status, row["attempts"] or 1)
            if delay is not None and (not row["deadline"] or time.time() + delay < row["deadline"]):
                # a passing refusal (queue full, rate or quota limit, no upstream right now): try again later
                note = f"HTTP {status}: {message or 'temporarily refused'}; retrying in {delay:.0f} s"[:1000]
                await asyncio.to_thread(
                    self.store.set_job_fields,
                    job_id,
                    status="queued",
                    started=None,
                    run_at=time.time() + delay,
                    error=note,
                )
                return
            finishing = True
            if status < 400:
                prompt, completion, _ = self._usage_of(body)
                fields.update(prompt_tokens=prompt, completion_tokens=completion)
                await asyncio.to_thread(self.store.finish_job, job_id, "succeeded", **fields)
            else:
                fields["error"] = str(message or f"HTTP {status}")[:1000]
                await asyncio.to_thread(self.store.finish_job, job_id, "failed", **fields)
        except asyncio.CancelledError:  # shutdown: it runs again after the restart (unless already finished)
            if not finishing:
                await asyncio.to_thread(self.store.set_job_fields, job_id, status="queued", started=None)
            raise
        except Exception as exc:
            log.error(json.dumps({"event": "job_failed", "job": job_id, "error": repr(exc)[:300]}))
            await asyncio.to_thread(self.store.finish_job, job_id, "failed", error=f"internal error: {exc!r}"[:1000])
        if row["batch_id"]:
            await asyncio.to_thread(finish_batch_if_done, self.store, row["batch_id"])
        if row["callback_url"]:  # in its own task: retries must not hold a job slot
            task = asyncio.get_running_loop().create_task(self._callback(job_id))
            self._callbacks.add(task)
            task.add_done_callback(self._callbacks.discard)

    async def _callback(self, job_id: str) -> None:
        row = await asyncio.to_thread(self.store.get_job, job_id)
        if row is None:
            return
        body = json.dumps(job_view(self.store, row)).encode()
        headers = {"content-type": "application/json", "user-agent": "QueueRouter"}
        if row["callback_secret"]:
            secret = self.store.decrypt(row["callback_secret"])
            headers["x-queuerouter-signature"] = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        url = self.store.reveal(row["callback_url"])
        host = urlsplit(url).hostname or ""
        status = "failed"
        for attempt, delay in enumerate((0, *CALLBACK_RETRY_S)):
            await asyncio.sleep(delay)
            # Resolve once, check, and connect to exactly that address: a DNS answer can't
            # change between the check and the request. TLS still verifies the host's name.
            address = await public_address(host)
            if address is None:
                status = "refused: not a public address"
                break
            try:
                resp = await self._http().post(
                    pinned_url(url, address),
                    content=body,
                    headers={**headers, "host": host},
                    timeout=15,
                    extensions={"sni_hostname": host},
                )
                if resp.status_code < 300:
                    status = "delivered"
                    break
                status = f"HTTP {resp.status_code}"
            except (httpx.HTTPError, OSError):
                status = "failed"  # no details: they could tell open from closed ports
            if attempt == len(CALLBACK_RETRY_S):
                break
        await asyncio.to_thread(self.store.set_job_fields, job_id, callback_status=status)

    async def stop(self) -> None:
        tasks = [*self.running.values(), *self._callbacks]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


# -- recurring jobs (cron schedules) ---------------------------------------------------------


def new_schedule(client: str, body: dict, now: float | None = None) -> dict:
    """A schedule from a ``POST /v1/schedules`` body (same fields as a job, plus ``name`` and ``cron``)."""
    name = str(body.get("name") or "").strip()
    if not name or len(name) > 100:
        raise JobError("'name' is required (up to 100 characters)")
    try:
        when = cron.parse(str(body.get("cron") or ""))
    except cron.CronError as exc:
        raise JobError(f"cron: {exc}") from exc
    deadline_s = body.get("deadline_s")
    if deadline_s is not None and (not isinstance(deadline_s, int | float) or deadline_s <= 0):
        raise JobError("'deadline_s' must be a positive number of seconds")
    job = new_job(client, {k: v for k, v in body.items() if k not in ("run_at", "deadline")})
    return {
        **{k: job[k] for k in ("client", "endpoint", "request", "priority", "tags", "idle_only", "callback_url")},
        "callback_secret": job["callback_secret"],
        "id": new_id("sched"),
        "name": name,
        "cron": str(body["cron"]).strip(),
        "deadline_s": deadline_s,
        "enabled": body.get("enabled", True) is not False,
        "next_run": when.next_after(now or time.time()),
    }


def schedule_view(store: Store, row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "object": "schedule",
        "name": row["name"],
        "cron": row["cron"],
        "endpoint": row["endpoint"],
        "priority": row["priority"],
        "tags": [t for t in (row["tags"] or "").split(",") if t],
        "idle_only": bool(row["idle_only"]),
        "deadline_s": row["deadline_s"],
        "callback_url": store.reveal(row["callback_url"]),
        "enabled": bool(row["enabled"]),
        "created_at": row["created"],
        "next_run": row["next_run"] if row["enabled"] else None,
        "last_run": row["last_run"],
        "last_job_id": row["last_job_id"],
    }


def fire_schedule(store: Store, row: sqlite3.Row, now: float, manual: bool = False) -> str:
    """Create this run's job; move the schedule to its next time (unless run by hand). Returns the job id."""
    job = {
        "id": new_id("job"),
        "client": row["client"],
        "priority": row["priority"],
        "endpoint": row["endpoint"],
        "request": json.loads(store.decrypt(row["request"])),
        "deadline": now + row["deadline_s"] if row["deadline_s"] else None,
        "idle_only": bool(row["idle_only"]),
        "callback_url": store.reveal(row["callback_url"]),
        "callback_secret": store.decrypt(row["callback_secret"]) if row["callback_secret"] else None,
        "tags": ["schedule", *[t for t in (row["tags"] or "").split(",") if t]][:10],
        "created": now,
    }
    store.create_jobs([job])
    fields: dict = {"last_run": now, "last_job_id": job["id"]}
    if not manual:
        try:  # a missed run (e.g. while down) runs once; then the normal rhythm resumes
            fields["next_run"] = cron.parse(row["cron"]).next_after(now)
        except cron.CronError:
            fields["enabled"] = False
    store.set_schedule(row["id"], **fields)
    return job["id"]
