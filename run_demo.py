"""Real-HTTP trial run of every scenario (no Docker required).

Spins up the actual uvicorn servers for node-a, node-b and the gateway on
localhost ports, then drives the SSE API exactly like the browser does and
prints a full report: accepted tokens per node, failover events, the late
packets the abandoned primary emitted afterwards, and the equivalence check
against the healthy baseline.

Usage:  python run_demo.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
import time

import httpx
import uvicorn

from common.deterministic import expected_token_ids, token_text
from gateway.app import GatewayConfig, create_app as create_gateway
from node.app import NodeConfig, create_app as create_node

PROMPT = "请写一段关于秋天的短文"
MAX_TOKENS = 12
PORT_A, PORT_B, PORT_G = 8901, 8902, 8908
READ_TIMEOUT = 0.5
EMIT_INTERVAL = 0.05


def serve(app, port):
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    return server


async def consume(client: httpx.AsyncClient, scenario: str, stop_marker: str = ""):
    events: list[tuple[str, dict]] = []
    async with client.stream(
        "POST", "http://127.0.0.1:8908/api/chat",
        json={"prompt": PROMPT, "max_tokens": MAX_TOKENS, "scenario": scenario,
              "stop_marker": stop_marker},
    ) as resp:
        event = None
        data: list[str] = []
        async for raw in resp.aiter_lines():
            line = raw.rstrip("\r")
            if line == "":
                if event is not None or data:
                    events.append((event or "message", json.loads("\n".join(data))))
                event, data = None, []
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
    return events


def report(scenario: str, events, healthy_ids, stop_marker: str = ""):
    print(f"\n{'=' * 78}\n场景: {scenario}"
          f"{f'（结束标记 = {stop_marker!r}）' if stop_marker else ''}\n{'=' * 78}")
    tokens = [e for n, e in events if n == "token"]
    for name, e in events:
        if name == "meta":
            print(f"[meta] request={e['request_id'][:8]}… 上限={e['max_tokens']}")
        elif name == "fallback":
            print(f"[fallback] {e['reason']}：{e['from_node']} 已采纳 {e['accepted_count']} "
                  f"个词元后切换到 {e['to_node']}"
                  + (f"（其中 {e.get('marker_pending_count')} 个因可能是标记而暂存）"
                     if e.get("marker_pending_count") else ""))
        elif name == "incomplete":
            print(f"[incomplete] reason={e['reason']} 保留 {e['accepted_count']} 个词元")
            if "primary_identity" in e:
                print(f"             primary={e['primary_identity']}")
                print(f"             backup ={e['backup_identity']}")
    end = next((e for n, e in events if n in ("done", "incomplete")), {})
    by_node: dict[str, list[str]] = {}
    for t in tokens:
        by_node.setdefault(t["node"], []).append(t["text"])
    for node, texts in by_node.items():
        print(f"[tokens:{node}] ({len(texts)}) {' '.join(texts)}")
    ids = [t["id"] for t in tokens]
    n = len(ids)
    prefix_ok = ids == healthy_ids[:n]
    if end.get("reason") == "stop_marker":
        print(f"[marker] 命中结束标记 {end['stop_marker']}，下标 {end['marker_index']}–"
              f"{end['marker_end_index'] - 1} 计入上限但未下发；"
              f"共接受 {end['token_count']} 个，下发 {end['emitted_count']} 个")
        ok = prefix_ok and n == end["marker_index"] and end["emitted_count"] == n
        print(f"[核对] 下发内容是否为基准在标记之前的前缀: {ok}")
        assert ok
    elif "done" not in {nm for nm, _ in events}:
        print(f"[incomplete] 保留 {n}/{MAX_TOKENS} 个已下发词元，与 healthy 同位置前缀一致: {prefix_ok}")
        assert prefix_ok
    else:
        ok = prefix_ok and n == MAX_TOKENS
        print(f"[核对] 完整回复词元数={n}/{MAX_TOKENS}，与 healthy 一致: {ok}")
        assert ok, "failover result diverged from healthy baseline"
    if end.get("partial_marker"):
        print(f"[marker] 末尾只有半截标记 {end['partial_marker']}，已照常作为正文发出")
    return tokens


async def main():
    node_a = create_node(NodeConfig("node-a", emit_interval=EMIT_INTERVAL))
    node_b = create_node(NodeConfig("node-b", emit_interval=EMIT_INTERVAL))
    gateway = create_gateway(GatewayConfig(
        node_a_url=f"http://127.0.0.1:{PORT_A}",
        node_b_url=f"http://127.0.0.1:{PORT_B}",
        read_timeout=READ_TIMEOUT,
    ))
    servers = [serve(node_a, PORT_A), serve(node_b, PORT_B), serve(gateway, PORT_G)]
    # Wait for readiness.
    async with httpx.AsyncClient() as client:
        for port in (PORT_A, PORT_B, PORT_G):
            for _ in range(50):
                try:
                    r = await client.get(f"http://127.0.0.1:{port}/info" if port != PORT_G
                                         else f"http://127.0.0.1:{port}/api/scenarios")
                    if r.status_code == 200:
                        break
                except httpx.HTTPError:
                    await asyncio.sleep(0.1)

    healthy_ids = expected_token_ids(PROMPT, 0, MAX_TOKENS)
    print(f"确定性基准词元 id 序列: {healthy_ids}")
    started = time.time()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            healthy = report("healthy", await consume(client, "healthy"), healthy_ids)
            report("failover_success", await consume(client, "failover_success"), healthy_ids)
            report("failover_backup_error", await consume(client, "failover_backup_error"), healthy_ids)
            report("version_mismatch", await consume(client, "version_mismatch"), healthy_ids)

            # --- stop marker demos ------------------------------------------
            texts = [token_text(PROMPT, i) for i in range(MAX_TOKENS)]
            print(f"\n确定性词元文本: {texts}")

            # Marker on the primary node ("forest bridge" at 2–3).
            report("healthy", await consume(client, "healthy", "forest bridge"),
                   healthy_ids, "forest bridge")
            # Marker spanning the failover boundary ("bridge jasmine": index 3
            # from node-a, index 4 from node-b).
            report("failover_success", await consume(client, "failover_success", "bridge jasmine"),
                   healthy_ids, "bridge jasmine")
            # Budget runs out on a partial marker: index 10 ("over") is the
            # only token of "over zzz" that arrives, so it is emitted.
            report("healthy", await consume(client, "healthy", f"{texts[10]} zzz"),
                   healthy_ids, f"{texts[10]} zzz")
            # Fault termination right after a partial marker ("anchor" at 6).
            report("failover_backup_error",
                   await consume(client, "failover_backup_error", f"{texts[6]} zzz"),
                   healthy_ids, f"{texts[6]} zzz")

            # Prove the abandoned primary really sent late packets afterwards.
            await asyncio.sleep(2.2)
            late = (await client.get(f"http://127.0.0.1:{PORT_A}/debug/late")).json()
            flat = [p for vals in late.values() for p in vals]
            print(f"\n[迟到回包] node-a 在被放弃后仍产生 {len(flat)} 个迟到词元 "
                  f"(indices={sorted({p['index'] for p in flat})})，网关均未采纳。")
            assert flat
    finally:
        for s in servers:
            s.should_exit = True
    print(f"\n全部场景试跑通过，耗时 {time.time() - started:.1f}s。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except AssertionError as exc:
        print(f"\n试跑失败: {exc}", file=sys.stderr)
        sys.exit(1)
