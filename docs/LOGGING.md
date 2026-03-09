# eth_getLogs Scaling

## Problem

The JSON-RPC specification was designed under the assumption that the consumer was also a node operator. Computationally expensive calls like `eth_getLogs` were acceptable because the cost was yours to bear.

In the RPC-provider-as-a-service world, that assumption breaks down. Providers restrict `eth_getLogs` by block range to protect shared infrastructure. A call spanning too many blocks is rejected outright (`-32005`, `max_block_range`, etc.). The restriction varies by provider and tier:

| Provider class | Typical max block range |
|----------------|------------------------|
| Free tier      | 2,000 – 10,000 blocks  |
| Paid tier      | 10,000 – 100,000 blocks|
| Archive nodes  | Unlimited (but expensive) |

This forces clients to chunk requests manually, serialize them, and retry — logic that belongs in the infrastructure layer, not the application layer.

---

## Opportunity

Lasso already manages a pool of heterogeneous providers with known capabilities (see `capabilities.max_block_range` in profile YAML). Rather than serializing chunks through a single provider, we can distribute chunks across providers in parallel — the same primitive idea as BitTorrent, but applied to log ranges instead of file chunks.

Key insight: a block range `[fromBlock, toBlock]` can be split into N sub-ranges. Those sub-ranges are independent and can be fetched in parallel from N providers simultaneously. The results are deterministic and mergeable by block number.

---

## Design Space

### 1. Parallel Range Splitting

**Idea:** When `eth_getLogs` arrives with a range exceeding a provider's `max_block_range`, split it into chunks sized to each candidate provider's limit and dispatch all chunks simultaneously.

**Properties:**
- Latency bounded by the slowest chunk (not sum of all chunks)
- Chunk count limited by available providers
- Each chunk goes through the normal selection/circuit-breaker pipeline
- Results merged and sorted by block number before returning

**Open questions:**
- How to size chunks: equal division vs. per-provider capacity weighting?
- What happens when a provider fails mid-batch (partial results)?
- How to handle overlapping chunks if providers have different limits?
- Should the client-visible response wait for all chunks or stream?

---

### 2. Provider Capability Discovery

**Idea:** Proactively know each provider's `max_block_range` rather than discovering it reactively from error responses.

Currently `max_block_range` is declared statically in YAML. In practice this can vary by method, filter complexity, and tier.

**Options:**
- Probe each provider with known-large ranges at startup and measure where it fails
- Parse error responses (`-32005`, message content) to update the known limit at runtime
- Maintain a per-provider-per-chain block range capability in ETS, updated by the probe cycle

**Open questions:**
- Is a static YAML declaration good enough for now?
- Should capability discovery be part of `ProbeCoordinator`?
- How to express "unknown" vs. "unlimited" vs. a measured limit?

---

### 3. Result Caching

**Idea:** Logs for a finalized block range are immutable. Once fetched, they can be cached and reused for identical `eth_getLogs` queries.

**Cache key candidates:**
- `{chain, address, topics, fromBlock, toBlock}` — exact match
- `{chain, address, topics, block}` — per-block granularity enabling range assembly from cache

**Storage options:**
- ETS — fast, in-memory, single-node lifetime
- DETS/SQLite — persistent, survives restart
- External (Postgres, Redis) — shared across nodes

**Properties:**
- Historical (finalized) blocks can be cached indefinitely
- Pending/unfinalized blocks must not be cached or must have short TTL
- Cache hit on a sub-range reduces the set of chunks that need fetching

**Open questions:**
- What granularity makes cache keys reusable across different queries?
- How to handle `"latest"` / relative block references?
- Memory bounds: what eviction policy applies?

---

### 4. Seeding / Peer Distribution (BitTorrent-style)

**Idea:** Once a node has fetched and cached a log range, it can serve that range to other Lasso nodes (or even clients), reducing upstream provider load for popular ranges.

**Primitives available:**
- BEAM clustering is already in place; nodes can RPC-call each other
- `Lasso.PubSub` can broadcast availability of cached ranges
- A node that has `[fromBlock, toBlock]` cached for a given filter can advertise it

**Rough flow:**
1. Node A needs `[0, 1000000]` for filter F.
2. Node A checks if any peer has sub-ranges of F cached (via PubSub or registry lookup).
3. Node A fetches uncached sub-ranges from providers, cached ones from peers.
4. Node A caches its newly fetched sub-ranges and broadcasts availability.

**Open questions:**
- Is peer-to-peer log distribution worth the complexity for our use case?
- What trust model applies — do we verify log data from peers?
- How does this interact with re-org risk on near-head blocks?

---

### 5. Streaming / Incremental Response

**Idea:** Rather than waiting for all chunks to complete before responding, stream results to the client as chunks arrive.

This requires a non-standard response format (JSON-RPC doesn't define streaming for `eth_getLogs`). Options:

- WebSocket subscription: emit a `eth_subscription`-style event per completed chunk
- HTTP chunked transfer: stream the JSON array as chunks arrive
- Lasso-specific extension: new method `lasso_getLogsStream` that wraps `eth_getLogs`

**Open questions:**
- How much client-side complexity does this push downstream?
- Is a standard-compatible response more important than lower perceived latency?

---

## Sequencing

Rough ordering by complexity and dependency:

1. **Parallel range splitting** — highest impact, self-contained, no new storage
2. **Per-provider capability in ETS** — needed for optimal chunk sizing
3. **Result caching (ETS, per-block)** — next leverage point, no new infrastructure
4. **Persistent cache** — extends ETS cache across restarts
5. **Peer seeding** — only valuable once per-node caching is proven

---

## Related Files

| File | Relevance |
|------|-----------|
| `lib/lasso_web/sockets/rpc_socket.ex` | WebSocket entry point for `eth_getLogs` |
| `lib/lasso_web/controllers/rpc_controller.ex` | HTTP entry point |
| `lib/lasso/core/request/request_pipeline.ex` | Pipeline where parallel dispatch would live |
| `lib/lasso/core/providers/capabilities.ex` | Provider capability declarations |
| `lib/lasso/config/chain_config.ex` | `max_block_range` config field |
| `lib/lasso/providers/candidate_listing.ex` | Provider selection for chunk dispatch |
