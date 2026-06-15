# 📖 Concepts — Real-Time Streaming Dashboard

Deep notes on the services and patterns used in this project.

---

## Table of Contents

1. [Amazon Kinesis Data Streams](#amazon-kinesis-data-streams)
2. [WebSocket API Gateway](#websocket-api-gateway)
3. [WebSocket vs REST vs Polling](#websocket-vs-rest-vs-polling)
4. [The Streaming Pipeline Pattern](#the-streaming-pipeline-pattern)
5. [Kinesis vs SQS](#kinesis-vs-sqs)
6. [How It All Works Together](#how-it-all-works-together)
7. [Production Scale — How Hotstar Does It](#production-scale--how-hotstar-does-it-and-where-our-architecture-fits)

---

## Amazon Kinesis Data Streams

### What It Is

Kinesis Data Streams is a managed real-time data streaming service. It accepts continuous high-volume data from any number of producers and makes it available to consumers in order, durably, and at scale.

Think of it as a highway with multiple lanes — each lane is a **shard**. Events flow in one end and consumers read from the other end. The highway doesn't care how fast cars (events) arrive — it just keeps them moving in order.

---

### Shards

A shard is the base unit of capacity in Kinesis:

- Each shard handles **1 MB/s write** and **2 MB/s read**
- Events within a shard are **strictly ordered**
- You choose how many shards your stream has — more shards = more throughput

When a producer writes to Kinesis, it specifies a **partition key**. Kinesis hashes the key to determine which shard the record goes to. Records with the same partition key always go to the same shard — this is how ordering is guaranteed per entity (e.g., all events for match `IPL-42` go to the same shard, in order).

---

### Event Retention and Replay

Kinesis retains records for **24 hours by default** (extendable to 7 days). This means:

- If Lambda is temporarily down or behind, events don't disappear — they wait in the stream
- You can replay the stream from any point — useful for reprocessing after a bug fix
- Multiple consumers can read the same stream independently (unlike SQS where a message is deleted after consumption)

---

### Lambda as a Kinesis Consumer

When Lambda is configured as a Kinesis consumer, it polls the stream automatically and invokes your function with a **batch** of records:

```python
def lambda_handler(event, context):
    for record in event['Records']:
        # Kinesis data is base64-encoded
        payload = json.loads(
            base64.b64decode(record['kinesis']['data']).decode('utf-8')
        )
        # process payload
```

Key behaviors:
- Lambda reads records **in order** per shard
- If your function fails, Lambda retries the **entire batch** — design for idempotency
- Lambda scales by adding one concurrent execution **per shard** — 3 shards = up to 3 concurrent Lambda invocations

---

### Iterator Age — The Key Metric

**Iterator age** is how far behind the consumer is from the tip of the stream. If events are arriving faster than Lambda can process them, iterator age grows.

- Iterator age = 0 → Lambda is keeping up, processing in real time
- Iterator age growing → Lambda is falling behind, add shards or optimize the function

This is the primary CloudWatch metric to monitor for a Kinesis pipeline.

---

## WebSocket API Gateway

### What It Is

WebSocket API Gateway manages persistent, bidirectional connections between clients (browsers) and your backend. Unlike REST where the client always initiates, WebSocket allows **the server to push data to the client at any time**.

A connection is established once and stays open. The server can send messages whenever it wants — no request needed from the client.

---

### How It Works

WebSocket API Gateway has three built-in routes:

| Route | When It Fires |
|-------|--------------|
| `$connect` | Client opens a WebSocket connection |
| `$disconnect` | Client closes the connection or times out |
| `$default` | Client sends a message (any message not matching a custom route) |

Each route is backed by a Lambda function. When a client connects, API Gateway assigns a unique **connection ID** and calls your `$connect` Lambda. You store that connection ID in DynamoDB. When you want to push data to that client, you use the connection ID to post a message back through the API Gateway Management API.

---

### Pushing Data to Clients

The Lambda that processes Kinesis records pushes updates to connected clients like this:

```python
import boto3

apigw = boto3.client(
    'apigatewaymanagementapi',
    endpoint_url=f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage}"
)

# Push to one connected client
apigw.post_to_connection(
    ConnectionId=connection_id,
    Data=json.dumps(update_payload).encode('utf-8')
)
```

The flow:
1. Kinesis triggers Lambda with new data
2. Lambda writes updated state to DynamoDB
3. Lambda reads all active connection IDs from DynamoDB
4. Lambda calls `post_to_connection` for each connection ID
5. API Gateway delivers the message to each connected browser instantly

---

### Stale Connection Handling

Connections can go stale — the client disconnected but the `$disconnect` route didn't fire (network drop, browser crash). When you try to post to a stale connection, API Gateway returns a `GoneException` (410). Your Lambda should catch this and delete the stale connection ID from DynamoDB:

```python
try:
    apigw.post_to_connection(ConnectionId=conn_id, Data=data)
except apigw.exceptions.GoneException:
    # Client disconnected — clean up
    connections_table.delete_item(Key={'connection_id': conn_id})
```

---

### IAM Permission for Posting to Connections

```json
{
  "Effect": "Allow",
  "Action": "execute-api:ManageConnections",
  "Resource": "arn:aws:execute-api:<region>:<account-id>:<api-id>/<stage>/POST/@connections/*"
}
```

---

## WebSocket vs REST vs Polling

| Approach | How It Works | Latency | Server Load | Use Case |
|----------|-------------|---------|-------------|----------|
| **Polling** | Client asks "anything new?" every N seconds | Up to N seconds | High — most requests return nothing | Simple, low-frequency updates |
| **REST (request-response)** | Client requests, server responds | Immediate on request | Normal | Fetching data on demand |
| **WebSocket** | Persistent connection, server pushes when data exists | Sub-second | Low — one connection per client, no empty requests | Real-time: live scores, dashboards, chat |

**Why WebSocket for this project:**
- Score updates happen continuously — polling every second would work but wastes resources
- Updates need to appear instantly — a 5-second poll interval means 5-second-old scores
- Many clients watching simultaneously — WebSocket scales better than thousands of clients polling

**Why REST is still used for initial load:**
WebSocket is for live updates, not for fetching the current state when the page first loads. A REST `GET /state` call gives the client the current snapshot, then WebSocket takes over for all subsequent updates.

---

## The Streaming Pipeline Pattern

This is the standard architecture for any real-time data system:

```
Producers (many, high volume)
        │
        ▼
Stream Buffer (Kinesis)
   - Absorbs any write rate
   - Preserves ordering per partition key
   - Retains data for replay
        │
        ▼
Stream Processor (Lambda)
   - Reads in ordered batches
   - Processes and stores state
   - Notifies connected clients
        │
        ├──────────────────┐
        ▼                  ▼
  State Store         Push Layer
  (DynamoDB)      (WebSocket API GW)
```

**Real-world uses of this exact pattern:**

| Use Case | Producer | Stream | Consumer | Output |
|----------|----------|--------|----------|--------|
| Live cricket scores | Ball-by-ball data feed | Kinesis | Lambda | WebSocket → browser |
| IoT sensor dashboard | Sensors publishing readings | Kinesis | Lambda | WebSocket → monitoring UI |
| Fraud detection | Payment events | Kinesis | Lambda | Alert + block transaction |
| Live order tracking | Delivery GPS updates | Kinesis | Lambda | WebSocket → customer app |
| Stock price ticker | Market data feed | Kinesis | Lambda | WebSocket → trading dashboard |

The infrastructure is identical across all of these — only the event schema and processing logic changes.

---

## Kinesis vs SQS

Both are AWS messaging services but serve different purposes:

| Feature | Kinesis Data Streams | SQS |
|---------|---------------------|-----|
| **Ordering** | Guaranteed per shard (partition key) | No ordering (standard queue) |
| **Message retention** | 24 hours – 7 days, replayable | Deleted after consumption |
| **Multiple consumers** | Yes — multiple consumers read the same stream independently | No — message consumed by one consumer then deleted |
| **Throughput** | 1 MB/s per shard, scale by adding shards | Nearly unlimited, managed automatically |
| **Use case** | Ordered event streams, real-time processing, replay | Task queues, decoupled microservices, DLQ patterns |

**Use Kinesis when:** you need ordering, replay, or multiple independent consumers reading the same events.

**Use SQS when:** you need a task queue where each message is processed once and ordering doesn't matter.

For this project, Kinesis is correct — we need ordering (events for the same match must be processed in sequence) and we want the ability to replay the stream if something goes wrong.

---

## How It All Works Together

### The Four Lambdas and Their Roles

This project has four Lambda functions, each with a single responsibility:

| Lambda | Trigger | What It Does |
|--------|---------|-------------|
| `ws-connect` | WebSocket `$connect` route | Saves the new connection ID to DynamoDB `connections` table |
| `ws-disconnect` | WebSocket `$disconnect` route | Deletes the connection ID from DynamoDB when client leaves |
| `stream-processor` | Kinesis stream | Processes event batch, updates state in DynamoDB, pushes to all connected clients |
| `get-state` | REST `GET /state` | Reads current state from DynamoDB, returns snapshot for initial page load |

Each Lambda is stateless — they share no memory. DynamoDB is the shared state that connects them all.

---

### Why DynamoDB — What Actually Gets Stored

DynamoDB is not storing every event. It stores two things:

**1. Current state (one row per entity, always overwritten)**

Events arrive continuously — ball by ball, sensor reading by sensor reading. Lambda processes them but only writes the **latest** to DynamoDB. Every write overwrites the previous:

```
Event arrives: { match_id: "IPL-42", score: 145, wickets: 2 }  → DynamoDB row updated
Event arrives: { match_id: "IPL-42", score: 147, wickets: 2 }  → same row overwritten
Event arrives: { match_id: "IPL-42", score: 147, wickets: 3 }  → same row overwritten
```

DynamoDB always has exactly one row for `IPL-42` — the current score. Not a history, just the snapshot.

**2. Active WebSocket connection IDs**

Lambda is stateless — it has no memory between invocations. When `stream-processor` runs, it needs to know which browsers are currently connected to push updates to. That list lives in the `connections` DynamoDB table, maintained by `ws-connect` and `ws-disconnect`.

---

### Why Not Push Directly from Kinesis to Browsers?

Kinesis doesn't know about WebSocket connections — it's a data transport layer, not a delivery mechanism. Lambda is the bridge:

```
Kinesis → Lambda → reads connection IDs from DynamoDB → pushes to each browser via API Gateway
```

Lambda is the only component that can talk to both Kinesis (as a consumer) and API Gateway (as a management client). It's the orchestrator.

---

### Batch Processing — Why Lambda Doesn't Write on Every Event

Lambda doesn't process one Kinesis record at a time. It receives a **batch** — configurable up to 10,000 records per invocation. If 50 events arrived since the last invocation, Lambda gets all 50 in one call:

```python
def lambda_handler(event, context):
    for record in event['Records']:  # could be 1 or 100 records
        payload = decode(record)
        # process each one
    # write final state once after processing all records
    table.put_item(Item=latest_state)
    # push to all connections once
    push_to_all_clients(latest_state)
```

This is more efficient than 50 separate DynamoDB writes and 50 separate push rounds. One batch invocation → one DynamoDB write → one push round to all clients.

---

### The Full End-to-End Flow

**New viewer opens the dashboard:**
```
1. Browser calls GET /state
   → get-state Lambda reads DynamoDB stream-state table
   → returns current snapshot (e.g., score: 153, wickets: 3)
   → browser renders current state

2. Browser opens WebSocket connection
   → ws-connect Lambda saves connection ID to DynamoDB connections table
   → browser is now registered to receive live pushes
```

**Live event arrives:**
```
3. Producer sends event to Kinesis
   → Kinesis buffers it in the appropriate shard

4. stream-processor Lambda triggered with batch of records
   → decodes each record
   → writes latest state to DynamoDB (overwrites previous)
   → reads all connection IDs from DynamoDB connections table
   → calls post_to_connection for each ID
   → API Gateway delivers update to every connected browser instantly
   → score updates on screen — no refresh, no polling
```

**Viewer closes the tab:**
```
5. WebSocket $disconnect fires
   → ws-disconnect Lambda deletes connection ID from DynamoDB
   → that browser no longer receives pushes
```

**Stale connection (browser crashed, network dropped):**
```
6. stream-processor tries to push to a dead connection
   → API Gateway returns GoneException (410)
   → Lambda catches it, deletes the stale connection ID from DynamoDB
   → self-healing — no manual cleanup needed
```

---

### Shards vs Viewers — They Are Independent

A common misconception: shards control how many viewers can watch. They don't.

- **Shards** control how fast you can **ingest events** from producers (1 MB/s write per shard)
- **Viewers** are WebSocket connections managed entirely by API Gateway — scales to tens of thousands independently

3 shards means Lambda processes the stream with up to 3 concurrent executions. Each execution pushes to **all** connected viewers regardless of how many there are. 1 shard can serve 10,000 viewers — it just means events are processed one batch at a time on that shard.

What actually limits viewer scale is API Gateway's WebSocket connection limit (default 500, raisable via AWS Support) — not the shard count.

---

## Production Scale — How Hotstar Does It (and Where Our Architecture Fits)

### Where Our Architecture Works in Production

Our stack (Kinesis + Lambda + WebSocket API Gateway + DynamoDB) is genuinely production-grade for:

- **Internal ops dashboards** — live order counts, error rates, deployment status. 50–500 internal users.
- **Restaurant kitchen displays** — exactly this project. Each restaurant has 2–5 screens. Thousands of restaurants = thousands of small isolated WebSocket audiences.
- **IoT monitoring dashboards** — factory floor sensors, fleet tracking for a few hundred vehicles.
- **SaaS product analytics** — live signups, active users, feature usage during a product launch.

For all of these, Lambda + WebSocket API Gateway handles the load with room to spare. This is not a simplified version of something bigger — it's the right tool for the job.

---

### Where It Breaks — The Hotstar Problem

Hotstar streams IPL to **50 million concurrent viewers**. Our architecture hits its limits well before that:

| Limit | Our Stack | Hotstar Scale |
|-------|-----------|---------------|
| WebSocket connections | ~500 default (raisable to ~100K) | 50 million |
| Lambda `post_to_connection` at scale | Looping 100K calls per invocation → timeout | Not viable |
| Event rate | Hundreds/sec per shard | Millions/sec |
| Latency | 100–500ms (Lambda cold start) | <50ms required |

---

### What Hotstar Actually Uses

**Event ingestion: Apache Kafka (not Kinesis)**

Kafka is self-managed (or via Confluent) and handles millions of events per second across hundreds of partitions. Kinesis is capped at 1 MB/s per shard — at Hotstar's scale you'd need thousands of shards, which becomes expensive and complex. Kafka is more cost-effective at extreme volume.

**Live state: Redis (not DynamoDB)**

The current score/state is stored in Redis (ElastiCache). Redis is in-memory — a single node handles 100,000+ writes/second at microsecond latency. DynamoDB On-Demand handles thousands of writes/second fine, but at Hotstar's event rate the cost and latency add up. Redis is the right snapshot store for high-frequency state.

**WebSocket delivery: ECS/Kubernetes WebSocket servers (not Lambda)**

Lambda has a maximum execution time and connection limits. At 50 million concurrent viewers, you need persistent WebSocket server processes — long-running Go or Node.js servers on ECS or Kubernetes that maintain millions of open connections each. Lambda is stateless and short-lived — it can't hold 50 million open connections.

**Push delivery: CDN + pub/sub (not direct post_to_connection)**

Hotstar uses a pub/sub layer (internal or via a service like Pusher) where WebSocket servers subscribe to score update topics. When a score update arrives, it's published to the topic and all subscribed servers push to their connected clients simultaneously — rather than one Lambda looping through 50 million connection IDs.

---

### The Full Comparison

| Component | Our Stack | Hotstar Scale |
|-----------|-----------|---------------|
| Event ingestion | Kinesis Data Streams | Apache Kafka |
| Stream processor | AWS Lambda | Kafka Consumers on ECS/K8s |
| Live state cache | DynamoDB | Redis (ElastiCache) |
| WebSocket management | API Gateway WebSocket | Custom WebSocket servers (Go/Node on ECS) |
| Push delivery | `post_to_connection` loop | Pub/sub → WebSocket server fan-out |
| Persistent storage | DynamoDB | Cassandra / DynamoDB for history |

---

### The Key Insight

The **pattern is identical** — stream buffer → processor → push layer. What changes at Hotstar's scale is replacing managed AWS services with self-managed infrastructure that handles 100x the throughput.

Kinesis → Kafka (same concept, higher throughput)
Lambda → persistent consumers (same concept, no cold start, no timeout)
DynamoDB → Redis (same concept, in-memory speed)
API Gateway WebSocket → custom WebSocket servers (same concept, millions of connections)

Understanding our architecture means you understand Hotstar's architecture. The mental model is the same — only the implementation changes at extreme scale.
