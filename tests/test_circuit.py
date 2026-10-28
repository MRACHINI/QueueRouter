import time

from app.circuit import Circuits


def test_opens_after_threshold_and_recovers_via_single_trial(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    changes = []
    c = Circuits(threshold=3, cooldown_s=10)
    c.on_change = lambda uid, state, reason: changes.append(state)

    for _ in range(2):
        c.failure(1, "HTTP 503")
    assert c.state(1) == "closed" and c.allows(1)
    c.failure(1, "HTTP 503")
    assert c.state(1) == "open" and not c.allows(1)

    now[0] += 10
    assert c.state(1) == "half-open" and c.allows(1)
    c.begin(1)
    assert not c.allows(1)  # only one trial at a time
    c.failure(1, "still down")
    assert c.state(1) == "open"

    now[0] += 10
    c.begin(1)
    c.success(1)
    assert c.state(1) == "closed" and c.allows(1)
    assert changes == ["open", "open", "closed"]


def test_threshold_zero_never_takes_an_upstream_out(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    changes = []
    c = Circuits(threshold=1, cooldown_s=60)
    c.on_change = lambda uid, state, reason: changes.append(state)
    c.failure(1, "HTTP 502")
    assert c.state(1) == "open"
    c.threshold = 0  # set to 0 in Settings while it's out: back at once
    assert c.state(1) == "closed" and c.allows(1)
    for _ in range(20):
        c.failure(1, "HTTP 502")
    assert c.state(1) == "closed" and c.allows(1) and c.describe(1)["last_error"] == "HTTP 502"
    c.success(1)
    assert changes == ["open", "closed"]  # the "recovered" notice still clears the alert


def test_success_resets_count_and_abandon_frees_trial(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    c = Circuits(threshold=2, cooldown_s=5)
    c.failure(1, "x")
    c.success(1)
    c.failure(1, "x")
    assert c.state(1) == "closed"
    c.failure(1, "x")
    now[0] += 5
    c.begin(1)
    c.abandon(1)
    assert c.allows(1)
    assert c.describe(1)["last_error"] == "x"
