"""Async jobs and the OpenAI-compatible Batch API."""

import asyncio
import json
import time

import httpx
import openai
import pytest

from app.config import Bootstrap
from app.jobs import JobError, check_callback, new_job
from app.state import open_store
from tests.conftest import ACCESS_KEY
from tests.test_admin import OWNER, csrf_of, login

CHAT = {"model": "mock-model", "messages": [{"role": "user", "content": "hi"}]}


async def wait_for(c: httpx.AsyncClient, headers: dict, job_id: str, statuses=("succeeded", "failed"), timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = (await c.get(f"/v1/jobs/{job_id}", headers=headers)).json()
        if job["status"] in statuses:
            return job
        await asyncio.sleep(0.2)
    raise AssertionError(f"job stayed {job['status']}")


def test_job_validation():
    with pytest.raises(JobError):
        new_job("c", {"request": CHAT, "endpoint": "/v1/jobs"})
    with pytest.raises(JobError):
        new_job("c", {"request": CHAT, "run_at": "2030-01-02T00:00:00Z", "deadline": "2030-01-01T00:00:00Z"})
    job = new_job("c", {"request": {**CHAT, "stream": True}, "run_at": "2030-01-01T00:00:00Z", "tags": "a,b"})
    assert "stream" not in job["request"] and job["run_at"] == 1893456000.0 and job["tags"] == ["a", "b"]
    for bad in ("http://example.com/hook", "https://localhost/x", "https://10.0.0.5/x", "https://127.0.0.1/x"):
        with pytest.raises(JobError):
            check_callback(bad)
    assert check_callback("https://hooks.example.com/x") == "https://hooks.example.com/x"


def test_restart_requeues_running_jobs(tmp_path):
    store = open_store(Bootstrap(data_dir=str(tmp_path)), None)
    store.create_jobs(
        [new_job("c", {"request": CHAT}) | {"id": "job_a"}, new_job("c", {"request": CHAT}) | {"id": "job_b"}]
    )
    assert store.claim_job("job_a") and not store.claim_job("job_a")
    store.claim_job("job_b")
    store.set_job_fields("job_b", attempts=3)
    assert store.requeue_running_jobs(3) == 1
    assert store.get_job("job_a")["status"] == "queued" and store.get_job("job_b")["status"] == "failed"
    store.close()


async def test_jobs_api(make_stack):
    stack = make_stack()
    h = stack.headers
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/jobs", headers=h, json={"request": {**CHAT, "mock_tag": "j1"}, "tags": ["nightly"]})
        assert r.status_code == 202 and r.json()["status"] == "queued"
        job = await wait_for(c, h, r.json()["id"])
        assert job["status"] == "succeeded" and job["result"]["choices"][0]["message"]["content"] == "hi j1"
        assert job["usage"] == {"prompt_tokens": 10, "completion_tokens": 5} and job["tags"] == ["nightly"]
        assert "job" in stack.mock["last_headers"].get("x-queuerouter-tags", "job")  # never forwarded upstream
        assert "x-queuerouter-tags" not in stack.mock["last_headers"]

        # one header turns any request into a job
        r = await c.post("/v1/chat/completions", headers={**h, "X-QueueRouter-Async": "true"}, json=CHAT)
        assert r.status_code == 202
        assert (await wait_for(c, h, r.json()["id"]))["status"] == "succeeded"

        # scheduled for later: doesn't run; can be cancelled
        later = await c.post("/v1/jobs", headers=h, json={"request": CHAT, "run_at": time.time() + 3600})
        assert later.json()["status"] == "scheduled"
        await asyncio.sleep(1.5)
        assert (await c.get(f"/v1/jobs/{later.json()['id']}", headers=h)).json()["status"] == "scheduled"
        assert (await c.delete(f"/v1/jobs/{later.json()['id']}", headers=h)).json()["status"] == "cancelled"

        # past its deadline before it could start
        expired = await c.post("/v1/jobs", headers=h, json={"request": CHAT, "deadline": time.time() - 1})
        assert (await wait_for(c, h, expired.json()["id"], ("expired",)))["error"]

        # upstream errors fail the job with the error kept
        failing = await c.post("/v1/jobs", headers=h, json={"request": {**CHAT, "mock_status": 400}})
        job = await wait_for(c, h, failing.json()["id"])
        assert job["status"] == "failed" and job["status_code"] == 400

        listed = (await c.get("/v1/jobs?tag=nightly", headers=h)).json()["data"]
        assert len(listed) == 1 and "result" not in listed[0]
        assert (
            await c.post("/v1/jobs", headers=h, json={"request": CHAT, "callback_url": "http://x"})
        ).status_code == 400


async def test_jobs_run_one_at_a_time(make_stack):
    stack = make_stack()
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        ids = []
        for _ in range(3):
            r = await c.post("/v1/jobs", headers=stack.headers, json={"request": {**CHAT, "mock_delay": 0.1}})
            ids.append(r.json()["id"])
        for job_id in ids:
            await wait_for(c, stack.headers, job_id)
    assert stack.mock["peak"] == 1  # max_jobs_in_flight = 1 by default


async def test_batch_api_with_the_openai_sdk(make_stack):
    stack = make_stack()
    client = openai.AsyncOpenAI(base_url=f"{stack.url}/v1", api_key=ACCESS_KEY, max_retries=0)
    lines = [
        {"custom_id": "a", "method": "POST", "url": "/v1/chat/completions", "body": {**CHAT, "mock_tag": "A"}},
        {"custom_id": "b", "method": "POST", "url": "/v1/chat/completions", "body": {**CHAT, "mock_status": 400}},
        {"custom_id": "c", "method": "POST", "url": "/v1/chat/completions", "body": {**CHAT, "mock_tag": "C"}},
    ]
    jsonl = "".join(json.dumps(line) + "\n" for line in lines).encode()
    uploaded = await client.files.create(file=("in.jsonl", jsonl), purpose="batch")
    assert uploaded.bytes == len(jsonl)
    batch = await client.batches.create(
        input_file_id=uploaded.id, endpoint="/v1/chat/completions", completion_window="24h", metadata={"run": "1"}
    )
    assert batch.status == "in_progress" and batch.request_counts.total == 3
    for _ in range(75):
        batch = await client.batches.retrieve(batch.id)
        if batch.status == "completed":
            break
        await asyncio.sleep(0.2)
    assert batch.status == "completed" and batch.request_counts.completed == 2 and batch.request_counts.failed == 1
    output = [json.loads(line) for line in (await client.files.content(batch.output_file_id)).text.splitlines()]
    assert {o["custom_id"]: o["response"]["body"]["choices"][0]["message"]["content"] for o in output} == {
        "a": "hi A",
        "c": "hi C",
    }
    errors = [json.loads(line) for line in (await client.files.content(batch.error_file_id)).text.splitlines()]
    assert errors[0]["custom_id"] == "b" and errors[0]["response"]["status_code"] == 400
    assert [b.id async for b in client.batches.list()] == [batch.id]

    with pytest.raises(openai.BadRequestError):  # every line must target the batch's endpoint
        bad = await client.files.create(
            file=("bad.jsonl", b'{"custom_id": "x", "url": "/v1/embeddings", "body": {}}\n'), purpose="batch"
        )
        await client.batches.create(input_file_id=bad.id, endpoint="/v1/chat/completions", completion_window="24h")


async def test_jobs_are_private_and_admin_page(make_stack):
    stack = make_stack(bootstrap=OWNER)
    async with httpx.AsyncClient(base_url=stack.url, timeout=30) as c:
        r = await c.post("/v1/jobs", headers=stack.headers, json={"request": CHAT, "run_at": time.time() + 3600})
        job_id = r.json()["id"]
        await login(c, stack.url)
        page = await c.get("/admin/jobs")
        assert page.status_code == 200 and job_id in page.text
        csrf = csrf_of(page.text)
        other = await c.post("/admin/clients", data={"csrf": csrf, "name": "other", "priority": "normal"})
        key = __import__("re").search(r'id="new-key">([^<]+)<', other.text).group(1)
        assert (await c.get(f"/v1/jobs/{job_id}", headers={"Authorization": f"Bearer {key}"})).status_code == 404
        await c.post(f"/admin/jobs/{job_id}/cancel", data={"csrf": csrf})
        assert stack.app.state.store.get_job(job_id)["status"] == "cancelled"
