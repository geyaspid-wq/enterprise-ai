#!/usr/bin/env python3
"""
Fleet Orchestrator v2 - Redis-backed middleware for 50,000+ concurrent agents
=============================================================================

Requirements : Python 3.10+, `pip install "redis>=5.0.1"`, Redis 6.2+ (7.x recommended)

Quick start
    python fleet_orchestrator.py                          # self-contained demo: 50k simulated agents
    python fleet_orchestrator.py --agents 5000 --duration 10

Multi-host deployment (shared --namespace is mandatory outside the demo)
    host A..B:  python fleet_orchestrator.py --role agents  --namespace fleet:prod --redis-url redis://...
    host C..Z:  python fleet_orchestrator.py --role workers --namespace fleet:prod --redis-url redis://...

ARCHITECTURE - why the middleware is decoupled from compute and storage
-----------------------------------------------------------------------

  simulated / real agents                (compute layer: add hosts to add agents)
        | submit(Message)
        v
  +----------------------------+
  | IngestGateway (per proc.)  |        (middleware layer: STATELESS)
  |  bounded staging buffer    |          * holds no per-agent / per-route state
  |  -> micro-batches of N     |          * every decision is a Redis round trip
  +----------------------------+          * any instance can die or be added at any time
        | (1) ONE pipeline: N x Lua circuit-breaker checks   (O(1) per route)
        | (2) ONE pipeline: N x XADD of the allowed messages
        v
  +--------------------------------------------------------------+
  | Redis (storage + broker layer)                               |
  |   loop:{a|b}   TTL counters   -> interaction counts          |
  |   trip:{a|b}   TTL flags      -> open circuits               |
  |   events:{0..S-1}  Streams    -> durable, sharded event bus  |
  |   one consumer group          -> work distribution           |
  +--------------------------------------------------------------+
        | XREADGROUP (blocking, batched)
        v
  WorkerPool on N hosts                   (compute layer: add hosts to add throughput)
        | handler(msg) -> XACK (+XDEL)      crashed worker? -> XAUTOCLAIM re-delivers
        '-> replies re-enter through the local IngestGateway

Memory model: the middleware's own RAM is O(buffer size), independent of the
number of agents or routes. The 50,000 agent coroutines below are the *load
simulator*, not part of the middleware. All route state lives in Redis and
expires by TTL, so nothing accumulates forever.

Scaling limits (honest version): "infinite" is really "linear until Redis
saturates". One Redis primary handles on the order of 10^5 simple ops/s; the key
design (hash tags, sharded streams) lets you move to Redis Cluster to scale
storage horizontally as well.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import signal
import socket
import sys
import time
import uuid
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoScriptError, RedisError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

log = logging.getLogger("fleet")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class Config:
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    namespace: str = "fleet:demo"

    # Event broker (Redis Streams)
    num_shards: int = 16          # streams are sharded by destination agent
    group: str = "fleet-workers"  # one consumer group = each event goes to ONE worker
    stream_maxlen: int = 100_000  # per shard; approximate trim = hard memory ceiling
    delete_on_ack: bool = True    # XDEL after XACK so XLEN == real backlog
    read_count: int = 100         # entries per stream per XREADGROUP call
    block_ms: int = 1_000
    claim_idle_ms: int = 15_000   # re-deliver messages a dead worker left unacked

    # Circuit breaker
    max_allowed_loops: int = 25   # interactions per route per window before tripping
    loop_window_ms: int = 5_000   # TTL of the interaction counter
    trip_cooldown_ms: int = 8_000  # how long a tripped route stays dropped
    max_hops: int = 64            # cheap local second line of defence (no Redis needed)

    # Ingest micro-batching
    batch_size: int = 500
    batch_window_ms: int = 5
    ingest_buffer: int = 20_000   # bounded => backpressure instead of unbounded RAM
    flushers: int = 4

    # Redis client
    pool_size: int = 64
    pool_wait_s: float = 30.0
    redis_retries: int = 4
    stats_ttl_s: int = 3_600


# --------------------------------------------------------------------------- #
# Redis plumbing
# --------------------------------------------------------------------------- #
def make_client(cfg: Config) -> aioredis.Redis:
    """One shared client per process. BlockingConnectionPool makes coroutines
    queue for a connection instead of erroring when the pool is exhausted."""
    pool = aioredis.BlockingConnectionPool.from_url(
        cfg.redis_url,
        max_connections=cfg.pool_size,
        timeout=cfg.pool_wait_s,
        decode_responses=True,
        health_check_interval=30,
        socket_keepalive=True,
        # Must exceed the XREADGROUP block time or healthy idle reads time out.
        socket_timeout=cfg.block_ms / 1000 + 10,
    )
    return aioredis.Redis(connection_pool=pool)


async def close_client(r: aioredis.Redis) -> None:
    closer = getattr(r, "aclose", None) or r.close
    await closer()
    await r.connection_pool.disconnect()


class Keys:
    """All key naming in one place.

    Hash tags ({...}) pin the two keys of a route to the SAME cluster slot so the
    Lua script can touch both atomically (no CROSSSLOT errors), while different
    routes spread evenly over all slots/nodes. Agent ids must not contain braces.
    """

    def __init__(self, namespace: str, num_shards: int) -> None:
        self.ns = namespace
        self.num_shards = num_shards
        self.stats = f"{namespace}:stats"
        self.dlq = f"{namespace}:dlq"

    @staticmethod
    def _tag(a: str, b: str) -> str:
        lo, hi = (a, b) if a <= b else (b, a)  # undirected: A->B and B->A share a counter
        return "{" + lo + "|" + hi + "}"

    def loop(self, a: str, b: str) -> str:
        return f"{self.ns}:loop:{self._tag(a, b)}"

    def trip(self, a: str, b: str) -> str:
        return f"{self.ns}:trip:{self._tag(a, b)}"

    def stream(self, shard: int) -> str:
        return f"{self.ns}:events:" + "{" + str(shard) + "}"

    def shard_of(self, agent_id: str) -> int:
        # Sharding by destination keeps one agent's inbox on one stream.
        return zlib.crc32(agent_id.encode()) % self.num_shards


# --------------------------------------------------------------------------- #
# Messages
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class Message:
    src: str
    dst: str
    payload: str = ""
    hop: int = 0
    ts_ms: int = field(default_factory=lambda: time.time_ns() // 1_000_000)

    def to_fields(self) -> dict:
        return {"src": self.src, "dst": self.dst, "hop": self.hop, "ts": self.ts_ms, "p": self.payload}

    @classmethod
    def from_fields(cls, f: dict) -> "Message":
        return cls(src=f["src"], dst=f["dst"], payload=f.get("p", ""),
                   hop=int(f.get("hop", 0)), ts_ms=int(f.get("ts", 0)))


Handler = Callable[[Message], Awaitable[None]]


# --------------------------------------------------------------------------- #
# Fleet-wide counters (HINCRBY), flushed in bulk
# --------------------------------------------------------------------------- #
class Stats:
    """Local deltas (a handful of ints, never per-agent) are folded into ONE
    pipelined HINCRBY burst per second, so metrics cost ~0 Redis ops per message.
    The hash carries a TTL that is refreshed on each flush: if the whole fleet
    stops, the counters evaporate on their own."""

    def __init__(self, r: aioredis.Redis, key: str, ttl_s: int) -> None:
        self._r, self._key, self._ttl = r, key, ttl_s
        self._pending: dict[str, int] = defaultdict(int)

    def incr(self, name: str, n: int = 1) -> None:
        if n:
            self._pending[name] += n

    async def flush(self) -> None:
        if not self._pending:
            return
        pending, self._pending = self._pending, defaultdict(int)
        try:
            async with self._r.pipeline(transaction=False) as pipe:
                for name, n in pending.items():
                    pipe.hincrby(self._key, name, n)
                pipe.expire(self._key, self._ttl)
                await pipe.execute()
        except RedisError as exc:  # keep the deltas; try again next tick
            log.warning("stats flush failed: %s", exc)
            for name, n in pending.items():
                self._pending[name] += n

    async def run(self, interval: float = 1.0) -> None:
        while True:
            await asyncio.sleep(interval)
            await self.flush()


# --------------------------------------------------------------------------- #
# Circuit breaker: atomic, O(1), one Lua call per route
# --------------------------------------------------------------------------- #
# KEYS[1] interaction counter   KEYS[2] "circuit open" flag
# ARGV[1] window ms             ARGV[2] max loops           ARGV[3] cooldown ms
# Returns {count, blocked}: blocked=1 -> drop this message.
#   count == -1 : circuit was already open (cheapest path: a single EXISTS)
#   count  >  0 : the counter value (== max+1 on the call that trips the circuit)
#
# The counter is a plain string key: INCR + PEXPIRE give us a per-route TTL window.
# (HINCRBY fields cannot carry their own TTL before Redis 7.4, so a hash-per-fleet
# would never forget a route. One tiny key per active route forgets by itself.)
# The whole script is a handful of O(1) commands and runs atomically, so two
# workers can never both "miss" the trip.
_BREAKER_LUA = """
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {-1, 1}
end
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
if n > tonumber(ARGV[2]) then
  redis.call('SET', KEYS[2], '1', 'PX', ARGV[3])
  redis.call('DEL', KEYS[1])
  return {n, 1}
end
return {n, 0}
"""


@dataclass(slots=True)
class Verdict:
    allowed: bool
    count: int
    newly_tripped: bool


class RouteBreaker:
    def __init__(self, r: aioredis.Redis, keys: Keys, cfg: Config) -> None:
        self._r, self._keys, self._cfg = r, keys, cfg
        self._sha: Optional[str] = None

    async def _ensure_script(self) -> str:
        # Loaded once and then invoked by SHA (a 40-byte call instead of the whole script).
        # We use raw EVALSHA rather than register_script() inside pipelines because
        # redis-py's script-in-pipeline helper adds a SCRIPT EXISTS round trip to
        # every execute(), which would double the cost of each micro-batch.
        if self._sha is None:
            self._sha = await self._r.script_load(_BREAKER_LUA)
        return self._sha

    async def check_batch(self, routes: list[tuple[str, str]]) -> list[Verdict]:
        """Micro-batch: N route checks cost ONE network round trip.

        Each check is O(1) inside Redis, and a tripped route only affects its own
        two keys - the other 49,000+ routes keep flowing, because "dropping" a
        route is just a verdict in the result list, never a lock or a wait.
        Fixed-window counting is deliberate (cheapest possible); swap in a
        sliding-window or token-bucket script if you need smoother limits."""
        if not routes:
            return []
        c = self._cfg
        args = [c.loop_window_ms, c.max_allowed_loops, c.trip_cooldown_ms]
        for attempt in (1, 2):
            sha = await self._ensure_script()
            try:
                async with self._r.pipeline(transaction=False) as pipe:
                    for src, dst in routes:
                        pipe.evalsha(sha, 2, self._keys.loop(src, dst), self._keys.trip(src, dst), *args)
                    raw = await pipe.execute()
                break
            except NoScriptError:  # Redis restarted / SCRIPT FLUSH / failed over to a fresh node
                self._sha = None
                if attempt == 2:
                    raise
        return [Verdict(allowed=not blocked, count=count, newly_tripped=bool(blocked) and count > 0)
                for count, blocked in raw]


# --------------------------------------------------------------------------- #
# Event broker: sharded Redis Streams + consumer group
# --------------------------------------------------------------------------- #
class StreamBroker:
    """Replaces asyncio.Queue. Streams are durable, shared across hosts, and a
    consumer group hands each entry to exactly one worker. Producers and consumers
    never talk to each other - they only know Redis - so either side scales (or
    crashes) independently."""

    def __init__(self, r: aioredis.Redis, keys: Keys, cfg: Config, stats: Stats) -> None:
        self._r, self._keys, self._cfg, self._stats = r, keys, cfg, stats

    async def ensure_groups(self) -> None:
        for shard in range(self._cfg.num_shards):
            try:
                await self._r.xgroup_create(self._keys.stream(shard), self._cfg.group, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):  # another host created it first: fine
                    raise

    async def publish_many(self, msgs: list[Message]) -> None:
        c = self._cfg
        async with self._r.pipeline(transaction=False) as pipe:
            for m in msgs:
                pipe.xadd(self._keys.stream(self._keys.shard_of(m.dst)), m.to_fields(),
                          maxlen=c.stream_maxlen, approximate=True)
            await pipe.execute()
        # NOTE: MAXLEN ~ is the memory safety valve. If consumers fall hopelessly
        # behind, the OLDEST entries are trimmed instead of Redis running out of RAM.
        # For lossless semantics, have the gateway check XLEN and shed load instead.

    # -- consuming ---------------------------------------------------------- #
    async def consume(self, consumer: str, handler: Handler, stop: asyncio.Event) -> None:
        c = self._cfg
        streams = {self._keys.stream(i): ">" for i in range(c.num_shards)}
        backoff = 0.1
        while not stop.is_set():
            try:
                resp = await self._r.xreadgroup(c.group, consumer, streams, count=c.read_count, block=c.block_ms)
                backoff = 0.1
            except (RedisConnectionError, RedisTimeoutError) as exc:
                log.warning("[%s] redis unavailable (%s); retry in %.1fs", consumer, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue
            except ResponseError as exc:
                if "NOGROUP" in str(exc):  # Redis was flushed/failed over: recreate and carry on
                    await self.ensure_groups()
                    continue
                raise
            if not resp:
                continue
            entries = [(stream, mid, fields) for stream, items in resp for mid, fields in items]
            try:
                await self._handle_entries(entries, handler)
            except RedisError as exc:
                # Unacked entries stay in the group's pending list and are re-delivered
                # by reclaim() after claim_idle_ms. At-least-once delivery.
                log.warning("[%s] ack failed (%s); entries will be reclaimed", consumer, exc)

    async def reclaim(self, consumer: str, handler: Handler) -> None:
        """Crash recovery: adopt entries another worker read but never acked."""
        c = self._cfg
        interval = max(1.0, c.claim_idle_ms / 3000)
        while True:
            await asyncio.sleep(interval)
            for shard in range(c.num_shards):
                stream, cursor = self._keys.stream(shard), "0-0"
                while True:
                    try:
                        res = await self._r.xautoclaim(stream, c.group, consumer,
                                                       min_idle_time=c.claim_idle_ms,
                                                       start_id=cursor, count=c.read_count)
                    except RedisError as exc:
                        log.warning("xautoclaim failed: %s", exc)
                        break
                    cursor, claimed = res[0], res[1]
                    claimed = [(stream, mid, f) for mid, f in claimed if mid]
                    if claimed:
                        self._stats.incr("reclaimed", len(claimed))
                        try:
                            await self._handle_entries(claimed, handler)
                        except RedisError as exc:
                            log.warning("reclaim ack failed: %s", exc)
                            break
                    if cursor in ("0-0", b"0-0"):
                        break

    async def _handle_entries(self, entries: list[tuple[str, str, Optional[dict]]], handler: Handler) -> None:
        c = self._cfg
        live = [e for e in entries if e[2]]
        # Handlers run concurrently: a slow LLM/tool call never blocks its neighbours.
        results = await asyncio.gather(*(self._run_handler(handler, f) for _, _, f in live))
        acks: dict[str, list[str]] = defaultdict(list)
        poison = 0
        async with self._r.pipeline(transaction=False) as pipe:
            for (stream, mid, fields), err in zip(live, results):
                acks[stream].append(mid)
                if err is not None:  # poison message: park it in a DLQ so it can't loop forever
                    poison += 1
                    pipe.xadd(self._keys.dlq, {**fields, "err": repr(err)[:200]}, maxlen=10_000, approximate=True)
            for stream, mid, fields in entries:
                if not fields:  # entry was trimmed away while pending: just clear it
                    acks[stream].append(mid)
            for stream, ids in acks.items():
                pipe.xack(stream, c.group, *ids)
                if c.delete_on_ack:
                    pipe.xdel(stream, *ids)
            await pipe.execute()
        self._stats.incr("consumed", len(entries))
        self._stats.incr("dead_lettered", poison)

    @staticmethod
    async def _run_handler(handler: Handler, fields: dict) -> Optional[Exception]:
        try:
            await handler(Message.from_fields(fields))
            return None
        except Exception as exc:  # noqa: BLE001 - isolate per-message failures
            return exc


# --------------------------------------------------------------------------- #
# Ingest gateway: staging buffer -> micro-batches -> breaker -> broker
# --------------------------------------------------------------------------- #
class IngestGateway:
    """The only path onto the bus. The bounded buffer is NOT the broker; it is a
    few-millisecond staging area that lets us turn 50k tiny writes into a few
    hundred pipelined round trips. When it fills, submit() awaits - natural
    backpressure on the producers instead of unbounded RAM growth."""

    def __init__(self, breaker: RouteBreaker, broker: StreamBroker, stats: Stats, cfg: Config) -> None:
        self._breaker, self._broker, self._stats, self._cfg = breaker, broker, stats, cfg
        self._buf: asyncio.Queue[Message] = asyncio.Queue(maxsize=cfg.ingest_buffer)
        self._inflight = 0

    async def submit(self, msg: Message) -> None:
        await self._buf.put(msg)

    def idle(self) -> bool:
        return self._buf.empty() and self._inflight == 0

    def buffered(self) -> int:
        return self._buf.qsize()

    async def run_flusher(self) -> None:
        c = self._cfg
        while True:
            batch = [await self._buf.get()]
            self._inflight += 1
            await asyncio.sleep(c.batch_window_ms / 1000)  # let the batch fill; also yields to producers
            while len(batch) < c.batch_size:
                try:
                    batch.append(self._buf.get_nowait())
                    self._inflight += 1
                except asyncio.QueueEmpty:
                    break
            try:
                await self._flush(batch)
            except Exception:  # noqa: BLE001 - a bad batch must never kill the flusher
                log.exception("flush failed; dropping %d messages", len(batch))
                self._stats.incr("dropped_errors", len(batch))
            finally:
                self._inflight -= len(batch)

    async def _flush(self, batch: list[Message]) -> None:
        c, st = self._cfg, self._stats
        st.incr("ingested", len(batch))
        live = [m for m in batch if m.hop <= c.max_hops]
        st.incr("dropped_hops", len(batch) - len(live))
        if not live:
            return

        # (1) Breaker verdicts for the whole batch in ONE round trip.
        try:
            verdicts = await self._retry(lambda: self._breaker.check_batch([(m.src, m.dst) for m in live]))
        except RedisError:  # Redis down: fail CLOSED (a loop storm is worse than a short pause)
            st.incr("dropped_errors", len(live))
            return
        allowed = [m for m, v in zip(live, verdicts) if v.allowed]
        st.incr("dropped_open", len(live) - len(allowed))
        st.incr("tripped", sum(v.newly_tripped for v in verdicts))

        # (2) Publish survivors in ONE round trip. Not fused into the Lua script on
        # purpose: stream shard and route key hash to different cluster slots.
        if allowed:
            try:
                await self._retry(lambda: self._broker.publish_many(allowed))
            except RedisError:
                st.incr("dropped_errors", len(allowed))
                return
            st.incr("published", len(allowed))

    async def _retry(self, make_coro):
        """Retry transient connection errors with exponential backoff; re-raise when exhausted."""
        delay = 0.05
        for attempt in range(1, self._cfg.redis_retries + 1):
            try:
                return await make_coro()
            except (RedisConnectionError, RedisTimeoutError) as exc:
                if attempt == self._cfg.redis_retries:
                    raise
                log.warning("redis error (attempt %d/%d): %s", attempt, self._cfg.redis_retries, exc)
                await asyncio.sleep(delay)
                delay *= 2


# --------------------------------------------------------------------------- #
# Simulation: thousands of agents, all non-blocking
# --------------------------------------------------------------------------- #
class AgentSimulator:
    """Each simulated agent is one coroutine that sleeps (exponential inter-arrival
    times, i.e. a Poisson process) and submits messages. 50,000 sleeping
    coroutines cost a little RAM and ~zero CPU, which is exactly why the event
    loop can model a fleet this size. A few 'loopy-N' agent pairs always answer
    each other to create real feedback loops for the circuit breaker to cut."""

    def __init__(self, gateway: IngestGateway, num_agents: int, rate: float, loop_pairs: int = 5,
                 kick_interval: float = 2.0) -> None:
        self._gw, self._n, self._rate = gateway, num_agents, rate
        self._pairs, self._kick = loop_pairs, kick_interval

    def start(self) -> list[asyncio.Task]:
        tasks = [asyncio.create_task(self._agent(i)) for i in range(self._n)]
        tasks.append(asyncio.create_task(self._loop_kicker()))
        return tasks

    async def _agent(self, idx: int) -> None:
        me, n, rate, gw = f"a{idx}", self._n, self._rate, self._gw
        await asyncio.sleep(random.random() / rate)  # stagger so 50k agents don't fire in lockstep
        while True:
            await asyncio.sleep(random.expovariate(rate))
            payload = "poison" if random.random() < 1e-4 else "task"
            await gw.submit(Message(me, f"a{random.randrange(n)}", payload))

    async def _loop_kicker(self) -> None:
        while True:
            for k in range(self._pairs):
                await self._gw.submit(Message(f"loopy-{2 * k}", f"loopy-{2 * k + 1}", "ping"))
            await asyncio.sleep(self._kick)


def make_demo_handler(gateway: IngestGateway, num_agents: int) -> Handler:
    """Stand-in for real agent logic (LLM call, tool use...). Non-blocking."""
    async def handler(msg: Message) -> None:
        await asyncio.sleep(random.uniform(0.0005, 0.003))  # simulated I/O-bound work
        if msg.payload == "poison":
            raise ValueError("simulated unprocessable message")
        if msg.dst.startswith("loopy-"):  # these agents always answer: an endless ping-pong
            await gateway.submit(Message(msg.dst, msg.src, "pong", hop=msg.hop + 1))
        elif random.random() < 0.02:  # ordinary agents occasionally trigger follow-ups
            await gateway.submit(Message(msg.dst, f"a{random.randrange(num_agents)}", "followup", hop=msg.hop + 1))
    return handler


# --------------------------------------------------------------------------- #
# Observability
# --------------------------------------------------------------------------- #
async def read_snapshot(r: aioredis.Redis, keys: Keys, cfg: Config) -> tuple[dict[str, int], int]:
    async with r.pipeline(transaction=False) as pipe:
        pipe.hgetall(keys.stats)
        for i in range(cfg.num_shards):
            pipe.xlen(keys.stream(i))
        res = await pipe.execute()
    return {k: int(v) for k, v in res[0].items()}, sum(res[1:])


async def reporter(r: aioredis.Redis, keys: Keys, cfg: Config, gw: IngestGateway, interval: float = 2.0) -> None:
    prev: dict[str, int] = {}
    prev_t = time.monotonic()
    while True:
        await asyncio.sleep(interval)
        try:
            s, backlog = await read_snapshot(r, keys, cfg)
        except RedisError as exc:
            log.warning("reporter: %s", exc)
            continue
        now = time.monotonic()
        dt = max(now - prev_t, 1e-9)
        rate = lambda k: (s.get(k, 0) - prev.get(k, 0)) / dt  # noqa: E731
        log.info("published %7d (%6.0f/s) | consumed %7d (%6.0f/s) | dropped_open %6d | tripped %3d | "
                 "backlog %6d | gateway_buf %5d",
                 s.get("published", 0), rate("published"), s.get("consumed", 0), rate("consumed"),
                 s.get("dropped_open", 0), s.get("tripped", 0), backlog, gw.buffered())
        prev, prev_t = s, now


async def wait_drained(r, keys, cfg, gw, stats, timeout: float, expect_workers: bool) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await stats.flush()
        s, _ = await read_snapshot(r, keys, cfg)
        in_flight = s.get("published", 0) - s.get("consumed", 0)
        if gw.idle() and (in_flight <= 0 or not expect_workers):
            return True
        await asyncio.sleep(0.25)
    return False


def print_summary(s: dict[str, int], backlog: int, elapsed: float) -> None:
    pub, con = s.get("published", 0), s.get("consumed", 0)
    print("\n" + "=" * 62)
    print(f" run summary ({elapsed:.1f}s)")
    print("=" * 62)
    rows = [
        ("ingested into gateway", s.get("ingested", 0)),
        ("published to streams", pub),
        ("consumed + acked", con),
        ("dropped: circuit open", s.get("dropped_open", 0)),
        ("circuits tripped", s.get("tripped", 0)),
        ("dropped: hop limit", s.get("dropped_hops", 0)),
        ("dropped: redis errors", s.get("dropped_errors", 0)),
        ("dead-lettered (poison)", s.get("dead_lettered", 0)),
        ("reclaimed from dead workers", s.get("reclaimed", 0)),
        ("left in streams at exit", backlog),
    ]
    for label, value in rows:
        print(f" {label:<30}{value:>10,}")
    if elapsed > 0:
        print(f" {'avg publish throughput':<30}{pub / elapsed:>9,.0f}/s")
    print("=" * 62)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
async def cancel_all(tasks: list[asyncio.Task]) -> None:
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass


async def cleanup_namespace(r: aioredis.Redis, namespace: str) -> None:
    batch: list[str] = []
    async for key in r.scan_iter(match=f"{namespace}:*", count=1000):
        batch.append(key)
        if len(batch) >= 500:
            await r.unlink(*batch)
            batch = []
    if batch:
        await r.unlink(*batch)


async def run(args: argparse.Namespace) -> int:
    role = args.role
    namespace = args.namespace or f"fleet:{uuid.uuid4().hex[:6]}"
    n_workers = args.workers if role in ("all", "workers") else 0
    cfg = Config(
        redis_url=args.redis_url, namespace=namespace, num_shards=args.shards,
        max_allowed_loops=args.max_loops, loop_window_ms=args.loop_window_ms,
        trip_cooldown_ms=args.trip_cooldown_ms, batch_size=args.batch_size,
        batch_window_ms=args.batch_window_ms, flushers=args.flushers,
        pool_size=n_workers + args.flushers + 16,  # each blocked XREADGROUP pins a connection
    )
    r = make_client(cfg)
    try:
        await r.ping()
    except RedisError as exc:
        log.error("cannot reach Redis at %s: %s", cfg.redis_url, exc)
        await close_client(r)
        return 2

    keys = Keys(cfg.namespace, cfg.num_shards)
    stats = Stats(r, keys.stats, cfg.stats_ttl_s)
    breaker = RouteBreaker(r, keys, cfg)
    broker = StreamBroker(r, keys, cfg, stats)
    gateway = IngestGateway(breaker, broker, stats, cfg)
    await broker.ensure_groups()

    stop = asyncio.Event()
    install_signal_handlers(stop)
    t0 = time.monotonic()
    log.info("namespace=%s role=%s agents=%d workers=%d shards=%d max_loops=%d/%dms",
             namespace, role, args.agents if role != "workers" else 0, n_workers,
             cfg.num_shards, cfg.max_allowed_loops, cfg.loop_window_ms)

    background = [asyncio.create_task(stats.run()), asyncio.create_task(reporter(r, keys, cfg, gateway))]
    flushers = [asyncio.create_task(gateway.run_flusher()) for _ in range(cfg.flushers)]

    worker_stop = asyncio.Event()
    workers: list[asyncio.Task] = []
    reclaimer: list[asyncio.Task] = []
    if n_workers:
        handler = make_demo_handler(gateway, args.agents)
        me = f"{socket.gethostname()}-{os.getpid()}"
        workers = [asyncio.create_task(broker.consume(f"{me}-w{i}", handler, worker_stop)) for i in range(n_workers)]
        reclaimer = [asyncio.create_task(broker.reclaim(f"{me}-reclaim", handler))]

    sim_tasks: list[asyncio.Task] = []
    if role in ("all", "agents"):
        sim_tasks = AgentSimulator(gateway, args.agents, args.rate).start()

    try:
        await asyncio.wait_for(stop.wait(), timeout=args.duration or None)
    except asyncio.TimeoutError:
        pass

    # ---- graceful shutdown: stop the load, drain the pipeline, then stop workers ----
    log.info("stopping: halting agents and draining in-flight work")
    await cancel_all(sim_tasks)
    drained = await wait_drained(r, keys, cfg, gateway, stats, timeout=15.0, expect_workers=n_workers > 0)
    if not drained:
        log.warning("drain timed out; some messages remain in the streams")
    worker_stop.set()
    await cancel_all(reclaimer)
    await asyncio.gather(*workers, return_exceptions=True)
    for _ in range(100):  # replies produced during shutdown
        if gateway.idle():
            break
        await asyncio.sleep(0.05)
    await cancel_all(flushers + background)
    await stats.flush()

    snapshot, backlog = await read_snapshot(r, keys, cfg)
    print_summary(snapshot, backlog, time.monotonic() - t0)

    if role == "all" and not args.keep:
        await cleanup_namespace(r, cfg.namespace)
    await close_client(r)
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1], formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--role", choices=["all", "agents", "workers"], default="all")
    p.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    p.add_argument("--namespace", default=None, help="key prefix; REQUIRED when role != all (all hosts must share it)")
    p.add_argument("--agents", type=int, default=50_000, help="simulated agents")
    p.add_argument("--rate", type=float, default=0.2, help="messages per agent per second")
    p.add_argument("--duration", type=float, default=25.0, help="seconds to run; 0 = until SIGINT/SIGTERM")
    p.add_argument("--workers", type=int, default=16, help="consumer coroutines in this process")
    p.add_argument("--flushers", type=int, default=4, help="parallel micro-batch flushers")
    p.add_argument("--shards", type=int, default=16, help="stream shards")
    p.add_argument("--max-loops", type=int, default=25, help="max_allowed_loops per route per window")
    p.add_argument("--loop-window-ms", type=int, default=5_000)
    p.add_argument("--trip-cooldown-ms", type=int, default=8_000)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--batch-window-ms", type=int, default=5)
    p.add_argument("--keep", action="store_true", help="do not delete the run's keys on exit")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    if args.role != "all" and not args.namespace:
        p.error("--namespace is required when --role is agents or workers")
    return args


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())