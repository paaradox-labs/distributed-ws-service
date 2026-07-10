# WS Service

WebSocket service that consumes Kafka messages and broadcasts them to connected Socket.IO clients.

## Stack

- **Runtime:** Node.js + TypeScript
- **WebSocket:** Socket.IO (with `node:http`)
- **Message Broker:** Kafka (via KafkaJS)
- **Config:** node-config (YAML)
- **Logging:** Winston

## Getting Started

### Prerequisites

- Node.js >= 20
- pnpm
- Kafka broker

### Setup

```bash
pnpm install
```

### Configuration

Config files live in `config/` (gitignored). Create `config/development.yaml`:

```yaml
server:
  port: 5504
kafka:
  broker: "localhost:9092"
```

### Run

```bash
pnpm dev        # dev mode with nodemon + ts-node
pnpm build      # lint + typecheck
pnpm lint       # ESLint
pnpm format     # Prettier
```

## Architecture

- `server.ts` — entry point; connects Kafka consumer, starts Socket.IO server
- `src/config/kafka.ts` — `KafkaBroker` implements `MessageBroker`
- `src/factories/broker-factory.ts` — singleton broker factory
- `src/socket.ts` — Socket.IO server with tenant-based room joining
- `src/types/broker.ts` — `MessageBroker` interface

Clients join a room via `socket.emit("join", { tenantId })` to receive messages scoped to their tenant.
