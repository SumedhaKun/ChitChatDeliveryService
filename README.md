# ChitChat Delivery Service

Consumes `messageCreated` Kafka events and delivers them to online recipients
over WebSocket. Presence is tracked in process memory so this service is
intentionally a single instance.

Horizontal scaling requires a shared presence store (Redis) or sticky routing
so a user's sockets can be found from any replica. Do not run more than one
instance until that exists.

## Requirements

- Node.js 22
- npm
- PostgreSQL 16 or a Supabase Postgres database with `conversation_members`
- A Kafka broker: local Docker from Message Service, or Confluent Cloud cluster
  `chitchat_cluster` for production

## Local setup

```sh
nvm use
npm install
cp .env.example .env
npm run dev
```

The HTTP and WebSocket servers share port `8082` by default. `DATABASE_URL`,
`SUPABASE_URL`, `SUPABASE_ANON_KEY`, `PORT`, and `KAFKA_BROKERS` are loaded from
`.env`.

Useful commands:

```sh
npm run typecheck
npm test
npm run lint
npm run format:check
npm run build
npm start
```

## WebSocket protocol

Clients connect to `ws://localhost:8082` and must send this first text frame:

```json
{ "type": "auth", "accessToken": "<supabase-access-token>" }
```

Successful authentication returns `{ "type": "auth_ack" }`. After that the
connection is receive-only. Delivered messages look like:

```json
{
  "type": "message_created",
  "message": {
    "messageId": "…",
    "senderId": "…",
    "conversationId": "…",
    "content": "Hello",
    "createdAt": "2026-09-13T20:00:00.000Z"
  }
}
```

The sender is never included in WebSocket fan-out or push notification. Offline
recipients currently hit a no-op push notifier.

`GET /health` returns `{ "status": "ok" }`.

## Presence and cleanup

Each authenticated socket is stored as `userId -> socket -> lastSeen`. Multiple
devices for the same user are supported. The service pings open sockets every
`HEARTBEAT_INTERVAL_MS` and terminates sockets whose `lastSeen` is older than
`CONNECTION_STALE_MS`. `lastSeen` updates on pong.

## Delivery semantics

- Topic: `messageCreated`
- Consumer group: `KAFKA_GROUP_ID` (default `chitchat-delivery-service`)
- Members are loaded from `conversation_members` by `conversation_id`
- `messageId` is treated as idempotent inside a bounded in-memory TTL window
- Failed sockets are removed and remaining recipients still receive the event
- Real-time delivery is best effort; persisted message history is the recovery path

## Render

`render.yaml` defines a free Render web service. Create a Blueprint from this
repository and provide `DATABASE_URL`, `SUPABASE_URL`, `SUPABASE_ANON_KEY`,
`KAFKA_BROKERS`, `KAFKA_API_KEY`, and `KAFKA_API_SECRET` when prompted. Point
Kafka at Confluent Cloud cluster `chitchat_cluster`: copy the bootstrap server
into `KAFKA_BROKERS`, keep `KAFKA_SSL=true`, and use a cluster API key. Create
topic `messageCreated` on that cluster with multiple partitions before
deploying; Confluent Cloud does not auto-create topics. Render supplies `PORT`;
do not set it manually. For runtime traffic, use the same Supabase
transaction-pooler connection string as Message Service, with SSL enabled.

Keep this service at one instance. Presence lives in process memory, so a
second replica cannot see the first replica's sockets.

After the service is deployed, use its WebSocket origin in the client:

```sh
NEXT_PUBLIC_DELIVERY_SERVICE_URL=wss://your-delivery-service.onrender.com
```

Render free services spin down after 15 minutes without inbound HTTP requests
and stop consuming Kafka while asleep. Upgrade to an always-on plan before
depending on real-time delivery.
