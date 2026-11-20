"""End-to-end behavior tests for the streaming failover gateway.

Run from the repository root:  python -m pytest -v

Tests are written as async coroutines and driven with asyncio.run (no extra
pytest plugin required).
"""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from common.deterministic import expected_token_ids, token_text
from gateway.app import MarkerGate


PROMPT = "请写一段关于秋天的短文"
MAX_TOKENS = 12
STALL_BEFORE = 4


async def collect_sse(
    client: httpx.AsyncClient,
    scenario: str,
    max_tokens: int = MAX_TOKENS,
    stop_marker: str = "",
) -> list[tuple[str, dict]]:
    events: list[tuple[str, dict]] = []
    async with client.stream(
        "POST", "/api/chat",
        json={"prompt": PROMPT, "max_tokens": max_tokens, "scenario": scenario,
              "stop_marker": stop_marker},
    ) as resp:
        assert resp.status_code == 200
        event = None
        data_lines: list[str] = []
        async for raw in resp.aiter_lines():
            line = raw.rstrip("\r")
            if line == "":
                if event is not None or data_lines:
                    events.append((event or "message", json.loads("\n".join(data_lines))))
                event, data_lines = None, []
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
        if event is not None or data_lines:
            events.append((event or "message", json.loads("\n".join(data_lines))))
    return events


def tokens_of(events):
    return [(e["index"], e["id"], e["text"], e["node"]) for name, e in events if name == "token"]


def names_of(events):
    return [name for name, _ in events]


# --- async scenarios ---------------------------------------------------------

async def _healthy(gateway_client):
    events = await collect_sse(gateway_client, "healthy")
    tokens = tokens_of(events)
    assert len(tokens) == MAX_TOKENS
    assert [t[0] for t in tokens] == list(range(MAX_TOKENS))           # strict order
    assert [t[1] for t in tokens] == expected_token_ids(PROMPT, 0, MAX_TOKENS)
    assert [t[3] for t in tokens] == ["node-a"] * MAX_TOKENS          # only primary
    assert names_of(events)[-1] == "done"


async def _failover_success(gateway_client):
    healthy = tokens_of(await collect_sse(gateway_client, "healthy"))
    events = await collect_sse(gateway_client, "failover_success")
    names = names_of(events)

    assert "fallback" in names
    fallback = next(e for n, e in events if n == "fallback")
    assert fallback["reason"] == "read_timeout"
    assert fallback["accepted_count"] == STALL_BEFORE

    tokens = tokens_of(events)
    assert len(tokens) == MAX_TOKENS
    assert [t[0] for t in tokens] == list(range(MAX_TOKENS))          # no gaps/dup
    # Append-only: first STALL_BEFORE tokens from primary, rest from backup.
    assert [t[3] for t in tokens] == ["node-a"] * STALL_BEFORE + ["node-b"] * (MAX_TOKENS - STALL_BEFORE)
    # The decisive property: continuation reproduces the healthy stream.
    assert [(i, tid, text) for i, tid, text, _ in tokens] == \
           [(i, tid, text) for i, tid, text, _ in healthy]
    done = next(e for n, e in events if n == "done")
    assert done["identical_to_healthy"] is True
    assert done["nodes"] == {"node-a": STALL_BEFORE, "node-b": MAX_TOKENS - STALL_BEFORE}


async def _shared_budget(gateway_client):
    events = await collect_sse(gateway_client, "failover_success")
    done = next(e for n, e in events if n == "done")
    assert done["token_count"] == MAX_TOKENS
    assert len(tokens_of(events)) == MAX_TOKENS


async def _backup_failure(gateway_client):
    events = await collect_sse(gateway_client, "failover_backup_error")
    tokens = tokens_of(events)
    # Primary produced 4, backup fails at absolute index 7 -> 3 more shown.
    assert len(tokens) == 7
    assert [t[3] for t in tokens] == ["node-a"] * 4 + ["node-b"] * 3
    incomplete = [e for n, e in events if n == "incomplete"]
    assert len(incomplete) == 1
    assert incomplete[0]["reason"] == "backup_failed"
    # Displayed text is the untouched deterministic prefix.
    assert [t[1] for t in tokens] == expected_token_ids(PROMPT, 0, 7)
    assert incomplete[0]["prefix_text"] == " ".join(token_text(PROMPT, i) for i in range(7))
    assert "done" not in names_of(events)


async def _version_mismatch(gateway_client):
    events = await collect_sse(gateway_client, "version_mismatch")
    tokens = tokens_of(events)
    assert len(tokens) == STALL_BEFORE
    assert all(t[3] == "node-a" for t in tokens)
    incomplete = next(e for n, e in events if n == "incomplete")
    assert incomplete["reason"] == "version_mismatch"
    assert incomplete["backup_identity"]["tokenizer_version"] == "tok-v2"
    assert incomplete["primary_identity"]["tokenizer_version"] == "tok-v1"
    # Prefix preserved verbatim.
    assert [t[1] for t in tokens] == expected_token_ids(PROMPT, 0, STALL_BEFORE)
    assert "done" not in names_of(events)


async def _late_packets_ignored(registry, gateway_client):
    events = await collect_sse(gateway_client, "failover_success")
    tokens = tokens_of(events)

    # The stalled node's detached late-generation task recorded late packets.
    client = registry.client(registry.node_a_url)
    async with client:
        late = (await client.get("/debug/late")).json()
    late_entries = [v for values in late.values() for v in values]
    assert late_entries, "primary should have emitted late packets after timeout"
    late_indices = {p["index"] for p in late_entries}
    # Every late index was accepted exactly once (from the backup), never via
    # the late primary delivery.
    accepted = {t[0]: t[3] for t in tokens}
    for idx in late_indices:
        assert accepted[idx] == "node-b"
    # No duplicate indices in the user-visible stream.
    assert len({t[0] for t in tokens}) == len(tokens)


async def _backup_rejects_wrong_prefix(registry):
    client = registry.client(registry.node_b_url)
    async with client:
        r = await client.post("/generate", json={
            "request_id": "bad", "prompt": PROMPT, "start": 2,
            "max_tokens": 3, "accepted_ids": [1, 1],
        })
        assert r.status_code == 409


async def _determinism(registry):
    async def fetch(url):
        client = registry.client(url)
        async with client:
            r = await client.post("/generate", json={
                "request_id": "x", "prompt": PROMPT, "start": 0, "max_tokens": 5,
            })
            ids = []
            async for line in r.aiter_lines():
                if line.startswith("data:") and '"id"' in line:
                    ids.append(json.loads(line[5:].lstrip())["id"])
            return ids
    a = await fetch(registry.node_a_url)
    b1 = await fetch(registry.node_b_url)
    b2 = await fetch(registry.node_b_url)
    assert a == b1 == b2 == expected_token_ids(PROMPT, 0, 5)


# --- stop marker -------------------------------------------------------------

async def _marker_hit_on_primary(gateway_client):
    # Stream: garden dragon forest bridge jasmine …
    events = await collect_sse(gateway_client, "healthy", stop_marker="forest bridge")
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == [0, 1]                    # untouched prefix only
    assert [t[2] for t in tokens] == ["garden", "dragon"]
    done = next(e for n, e in events if n == "done")
    assert done["reason"] == "stop_marker"
    assert done["marker_index"] == 2 and done["marker_end_index"] == 4
    assert done["token_count"] == 4                            # matched span counted
    assert done["emitted_count"] == 2
    assert "done" == names_of(events)[-1]


async def _marker_counts_toward_budget_at_the_limit(gateway_client):
    # The last two tokens (indices 10, 11) are "over over"; matching them
    # consumes the whole budget and downstream nothing is emitted.
    events = await collect_sse(gateway_client, "healthy", stop_marker="over over")
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == list(range(10))
    done = next(e for n, e in events if n == "done")
    assert done["reason"] == "stop_marker"
    assert done["marker_index"] == 10
    assert done["token_count"] == 12 and done["emitted_count"] == 10


async def _marker_crosses_failover(gateway_client):
    # node-a stalls before index 4. "bridge" (index 3, from node-a) is held
    # back; the backup must therefore be asked to continue from index 4 --
    # i.e. from every token the gateway accepted, including the unsent one.
    events = await collect_sse(gateway_client, "failover_success",
                               stop_marker="bridge jasmine")
    fallback = next(e for n, e in events if n == "fallback")
    assert fallback["accepted_count"] == 4
    assert fallback["marker_pending_count"] == 1
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == [0, 1, 2]
    assert [t[3] for t in tokens] == ["node-a"] * 3
    done = next(e for n, e in events if n == "done")
    assert done["reason"] == "stop_marker"
    assert done["marker_index"] == 3 and done["marker_end_index"] == 5
    # Accepted stream: 4 from node-a (index 3 held back) + index 4 from node-b.
    assert done["nodes"] == {"node-a": 4, "node-b": 1}
    assert done["token_count"] == 5 and done["emitted_count"] == 3
    assert done["identical_to_healthy"] is True


async def _marker_completes_entirely_on_backup(gateway_client):
    # Marker starts after the switch: both of its tokens come from node-b.
    events = await collect_sse(gateway_client, "failover_success",
                               stop_marker="jasmine nebula")
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == [0, 1, 2, 3]
    done = next(e for n, e in events if n == "done")
    assert done["reason"] == "stop_marker"
    assert done["marker_index"] == 4 and done["marker_end_index"] == 6
    assert done["nodes"] == {"node-a": 4, "node-b": 2}
    assert done["token_count"] == 6 and done["emitted_count"] == 4


async def _partial_marker_flushed_at_budget(gateway_client):
    # Budget 11 cuts the stream right after the first "over" of "over over":
    # only a partial marker is at the tail and must be emitted normally.
    events = await collect_sse(gateway_client, "healthy", max_tokens=11,
                               stop_marker="over over")
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == list(range(11))
    assert tokens[-1][2] == "over"
    done = next(e for n, e in events if n == "done")
    assert done["reason"] == "max_tokens"
    assert done["partial_marker"] == ["over"]
    assert done["emitted_count"] == 11


async def _partial_marker_flushed_on_fault(gateway_client):
    # Stall before 4, backup fails before absolute index 7. Have the marker
    # begin at index 6 ("anchor jasmine"): the held "anchor" must be flushed
    # when the reply is marked incomplete.
    events = await collect_sse(gateway_client, "failover_backup_error",
                               stop_marker="anchor jasmine")
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == list(range(7))
    assert tokens[-1][2] == "anchor"
    incomplete = next(e for n, e in events if n == "incomplete")
    assert incomplete["reason"] == "backup_failed"
    assert incomplete["partial_marker"] == ["anchor"]
    assert "done" not in names_of(events)


async def _partial_marker_flushed_when_backup_refused(gateway_client):
    # Version mismatch: no continuation at all, and the held "bridge"
    # (index 3) is still emitted as plain text with the incomplete marker.
    events = await collect_sse(gateway_client, "version_mismatch",
                               stop_marker="bridge jasmine")
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == [0, 1, 2, 3]
    assert tokens[-1][2] == "bridge"
    incomplete = next(e for n, e in events if n == "incomplete")
    assert incomplete["reason"] == "version_mismatch"
    assert incomplete["partial_marker"] == ["bridge"]


async def _candidate_that_cannot_complete_is_released(gateway_client):
    # "jasmine" (index 4) matches the marker's first token and is held, but
    # index 5 is "nebula" != "zzz", so it is released and later tokens flow.
    events = await collect_sse(gateway_client, "healthy", stop_marker="jasmine zzz")
    tokens = tokens_of(events)
    assert [t[0] for t in tokens] == list(range(MAX_TOKENS))
    done = next(e for n, e in events if n == "done")
    assert done["reason"] == "max_tokens"
    assert done["partial_marker"] is None


async def _empty_marker_keeps_original_behavior(gateway_client):
    with_empty = await collect_sse(gateway_client, "healthy", stop_marker="")
    without_param = await collect_sse(gateway_client, "healthy")
    assert tokens_of(with_empty) == tokens_of(without_param)
    assert len(tokens_of(with_empty)) == MAX_TOKENS


def test_marker_gate_handles_overlapping_candidates():
    # Marker "a b" in the stream "a a b": the first "a" is released because
    # the run "a a" cannot complete, while the second "a" is held as the new
    # candidate start.  The match therefore starts at index 1, not 0.
    gate = MarkerGate(["a", "b"])
    release = []
    for i, text in enumerate(["a", "a", "b"]):
        gate.push({"index": i, "text": text})
        release += gate.release()
    assert gate.hit == {"start": 1, "end": 3}
    assert release == [{"index": 0, "text": "a"}]


def test_marker_gate_holds_only_possible_prefix():
    gate = MarkerGate(["alpha", "bravo"])
    gate.push({"index": 0, "text": "x"})
    assert [t["index"] for t in gate.release()] == [0]        # sent immediately
    gate.push({"index": 1, "text": "alpha"})
    assert gate.release() == []                               # held: could start marker
    gate.push({"index": 2, "text": "charlie"})
    # Candidate broke; both are safe now.
    assert [t["index"] for t in gate.release()] == [1, 2]


def test_marker_gate_swallows_first_complete_match():
    gate = MarkerGate(["a", "b"])
    for i, text in enumerate(["a", "a", "b", "a", "b"]):
        gate.push({"index": i, "text": text})
        gate.release()
    # First complete match starts at index 1 and clears everything after it.
    assert gate.hit == {"start": 1, "end": 3}
    assert gate.release() == []


def test_marker_gate_flush_keeps_partial_tail():
    gate = MarkerGate(["alpha", "bravo"])
    gate.push({"index": 0, "text": "alpha"})
    assert gate.release() == []
    assert [t["index"] for t in gate.release(force=True)] == [0]


# --- sync wrappers -----------------------------------------------------------

def test_healthy_baseline_matches_deterministic_model(gateway_client):
    asyncio.run(_healthy(gateway_client))


def test_failover_success_is_identical_to_healthy(gateway_client):
    asyncio.run(_failover_success(gateway_client))


def test_shared_token_budget_never_exceeded(gateway_client):
    asyncio.run(_shared_budget(gateway_client))


def test_backup_failure_keeps_prefix_and_marks_incomplete(gateway_client):
    asyncio.run(_backup_failure(gateway_client))


def test_version_mismatch_refuses_continuation(gateway_client):
    asyncio.run(_version_mismatch(gateway_client))


def test_late_packets_from_primary_are_ignored(registry, gateway_client):
    asyncio.run(_late_packets_ignored(registry, gateway_client))


def test_backup_rejects_wrong_prefix(registry):
    asyncio.run(_backup_rejects_wrong_prefix(registry))




def test_determinism_across_nodes_and_runs(registry):
    asyncio.run(_determinism(registry))


def test_marker_hit_on_primary(gateway_client):
    asyncio.run(_marker_hit_on_primary(gateway_client))


def test_marker_counts_toward_token_budget(gateway_client):
    asyncio.run(_marker_counts_toward_budget_at_the_limit(gateway_client))


def test_marker_crosses_failover(gateway_client):
    asyncio.run(_marker_crosses_failover(gateway_client))


def test_marker_completes_entirely_on_backup(gateway_client):
    asyncio.run(_marker_completes_entirely_on_backup(gateway_client))


def test_partial_marker_flushed_at_token_budget(gateway_client):
    asyncio.run(_partial_marker_flushed_at_budget(gateway_client))


def test_partial_marker_flushed_on_fault_termination(gateway_client):
    asyncio.run(_partial_marker_flushed_on_fault(gateway_client))


def test_partial_marker_flushed_when_backup_refused(gateway_client):
    asyncio.run(_partial_marker_flushed_when_backup_refused(gateway_client))


def test_marker_candidate_that_cannot_complete_is_released(gateway_client):
    asyncio.run(_candidate_that_cannot_complete_is_released(gateway_client))


def test_empty_marker_keeps_original_behavior(gateway_client):
    asyncio.run(_empty_marker_keeps_original_behavior(gateway_client))
