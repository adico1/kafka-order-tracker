# Kafka Order Tracker

A fullstack Kafka training app: place a coffee-shop order in React and watch it move from **PLACED** to **PREPARING** to **READY**.

```text
React → Express → orders.created → Kitchen worker
React ← Server-Sent Events ← Express ← orders.status ← Kitchen worker
```

## Prerequisites

- A current Node.js LTS release and npm.
- A running Kafka broker reachable and advertised at `localhost:9092`.

## Setup

From the repository root:

```bash
cd backend
npm ci
npm run setup:kafka
cd ../frontend
npm ci
```

The setup command creates `orders.created` and `orders.status`, each with one partition and replication factor one for a local single-broker environment.

## Run

Keep Kafka running and use three terminals, starting from the repository root.

API:

```bash
cd backend
npm run dev
```

Kitchen worker:

```bash
cd backend
npm run kitchen
```

Frontend:

```bash
cd frontend
npm run dev
```

Open http://localhost:5173 and place an order. The kitchen simulates two seconds of preparation before publishing READY.

## How it works

- `backend/server.js` accepts `POST /api/orders`, publishes the order keyed by its ID, and returns HTTP 202 after Kafka acknowledges publication.
- `backend/kitchen.js` consumes orders in the `kitchen-team` group and publishes status events.
- The API consumes status events and serves an SSE stream at `/api/events`. New connections receive a snapshot of known orders.
- React merges HTTP responses and stream events without duplicating orders or moving their status backward.
- Vite proxies `/api` requests to the API on port 3001. The health endpoint is `/api/health`.

## Learning experiments

1. Stop the kitchen, place orders, then restart it to see queued orders processed.
2. Refresh the browser to see the current order snapshot.
3. Start two kitchen workers. With one partition, only one worker in the group receives that partition.

## Checks

```bash
cd frontend
npm run lint
npm run build
```

## Training limitations

This is a local, single-API-instance demo without authentication or a database. The API holds its order view in memory and replays retained status events on restart without committing status-consumer offsets. Orders that have not produced a status event are not reconstructed by that replay. Kafka retention limits the available history.

Consumer delivery can repeat after failures; preventing duplicate kitchen side effects, durable persistence, retries, and dead-letter handling are future training exercises.
