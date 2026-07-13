# LTAP-RP

A fully-containerised demo of the [LTAP](../LTAP-SDK) (LLM Shared-Bus Turn Allocation Protocol) using [Redpanda](https://redpanda.com/) as the Kafka-compatible message bus.

Multiple LLM-backed agents participate in a structured multi-agent discussion. The LTAP arbiter manages turn allocation — agents bid for the floor, one wins per tick, and its transmission is broadcast to all participants. A live web UI shows the conversation as it unfolds.

## Architecture

```
┌─────────────┐     LTAP protocol over Kafka topics      ┌──────────────┐
│   Arbiter   │◄──────────────────────────────────────►  │  Agent Alice │ (llama2)
│             │                                           │  Agent Bob   │ (llama3.1)
│  tick loop  │◄──────────────────────────────────────►  │  Agent Carol │ (qwen3.6:27b)
│  bid select │                                           │  Agent Dave  │ (llama3:8b)
└─────────────┘                                           └──────────────┘
       │                                                         │
       └──────────────── Redpanda (Kafka) ──────────────────────┘
                                │
                          ┌─────▼─────┐
                          │  Chat UI  │  http://localhost:5000
                          └───────────┘
```

**Kafka topics used:**
- `ltap.control` — registration acks
- `ltap.bid-requests` — arbiter → agents
- `ltap.bids` — agents → arbiter
- `ltap.win-signals` — arbiter → winner
- `ltap.transmissions` — winner → arbiter (and chat UI)
- `ltap.events` — arbiter broadcasts each transmission back to all agents

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Compose v2
- [Ollama](https://ollama.com/) running locally with the desired models pulled

Pull the default models:
```bash
ollama pull llama3:8b-instruct-q8_0
ollama pull llama2:latest
ollama pull llama3.1:latest
ollama pull qwen3.6:27b
```

## Quick start

```bash
git clone https://github.com/minimumbuilds/LTAP-RP
cd LTAP-RP
docker compose up --build -d
```

Then open http://localhost:5000 to watch the conversation live.

## Services

| Service | Description | Port |
|---|---|---|
| `redpanda` | Kafka-compatible broker | 19092 (external) |
| `console` | Redpanda web console | 8080 |
| `arbiter` | LTAP turn allocator | — |
| `chat` | Live web UI | 5000 |
| `agent-alice` | LLM agent (performance focus) | — |
| `agent-bob` | LLM agent (clean code focus) | — |
| `agent-carol` | LLM agent (correctness focus) | — |
| `agent-dave` | LLM agent (distributed systems focus) | — |

## Configuration

All tunables are in `config.yaml`. It is volume-mounted — **restart** containers after changes (no rebuild required).

```yaml
arbiter:
  num_agents: 4          # how many agents participate (2–4)
  tick_interval: 0.5     # seconds between ticks
  cooldown_ticks: 1      # ticks a winner's bids are dampened ×0.3 after a win

llm:                     # global defaults for all agents
  base_url: "http://host.docker.internal:11434/v1"
  model: "llama3:8b-instruct-q8_0"
  max_tokens: 480
  temperature: 0.85

discussion:
  topic: >
    Your discussion topic here...

agents:
  - name: alice
    persona: >
      You are Alice...
    llm:                 # optional per-agent override
      model: "llama2:latest"
```

### Per-agent LLM overrides

Any field under an agent's `llm:` block overrides the global default. Omitted fields inherit from global. Useful for mixing models or giving thinking models more token budget:

```yaml
  - name: carol
    persona: >
      ...
    llm:
      model: "qwen3.6:27b"
      max_tokens: 1024   # thinking models need extra room
```

## Updating after changes

| What changed | Command |
|---|---|
| `config.yaml` | `docker compose restart <service>` |
| Any `.py` source file | `docker compose up --build -d` |

All app containers share the same image — one build updates everything.

## Consuming the Kafka stream directly

Redpanda is exposed on `localhost:19092`. Use any Kafka CLI tool:

```bash
# rpk (Redpanda's CLI)
rpk topic consume ltap.transmissions --brokers localhost:19092

# kcat
kcat -b localhost:19092 -t ltap.transmissions -C
```

## Dependencies

- [LTAP-SDK](../LTAP-SDK) — the turn allocation protocol library (installed from the parent directory at build time)
- [aiokafka](https://github.com/aio-libs/aiokafka) — async Kafka client
- [aiohttp](https://docs.aiohttp.org/) — HTTP server for the chat UI
- [openai](https://github.com/openai/openai-python) — OpenAI-compatible client (used against Ollama's API)
