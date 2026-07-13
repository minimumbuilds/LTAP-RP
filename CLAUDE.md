# LTAP-RP — Claude Code Notes

## What this is
Kafka/Redpanda transport demo for the LTAP SDK. The LTAP-SDK lives one directory up (`../LTAP-SDK`) and is installed into the Docker image at build time. Changes to the SDK require a full rebuild.

## Running the stack
```bash
docker compose up --build -d   # first run or after any Python/Dockerfile change
docker compose down            # stop everything
docker compose logs -f chat    # watch the conversation
```

## Rebuild rules

| Changed file | Action |
|---|---|
| Any `.py` file in `ltap_rp/` | `docker compose up --build -d` |
| `../LTAP-SDK` (the SDK) | `docker compose up --build -d` |
| `Dockerfile` | `docker compose up --build -d` |
| `config.yaml` | `docker compose restart <service(s)>` — it's volume-mounted |

All six app services (arbiter, chat, agent-alice/bob/carol/dave) share the same Dockerfile. One build rebuilds them all. You do not need to rebuild per-service.

## Key services and ports
- **Chat UI**: http://localhost:5000
- **Redpanda Console**: http://localhost:8080
- **Kafka (external)**: localhost:19092
- **Ollama** (host): http://localhost:11434 — accessed from containers via `host.docker.internal:11434`

## Config file (`config.yaml`)
Volume-mounted into every service at `/config/config.yaml`. Restart (not rebuild) is sufficient for any change here.

- `arbiter.num_agents` — how many agents are active (2–4, uses first N from the agents list)
- `arbiter.tick_interval` — seconds between ticks; set to `0.0` for maximum speed
- `arbiter.cooldown_ticks` — ticks a winner's bids are dampened (×0.3) after a win; the winner stays eligible
- `llm.*` — global LLM defaults (base_url, model, max_tokens, temperature)
- `agents[].llm.*` — per-agent overrides; any field omitted inherits from global

## Per-agent LLM override
Add an `llm:` block under any agent in `config.yaml`. Only the fields you specify override the global default:
```yaml
agents:
  - name: carol
    persona: >
      ...
    llm:
      model: "qwen3.6:27b"
      max_tokens: 1024
```
Restart the agent container after editing — no rebuild needed.

## Thinking models (e.g. qwen3)
Thinking model output (`<think>...</think>`) is stripped automatically. If the model outputs *only* a think block with no text after it, the code falls back to extracting the last paragraph from inside the think block. Give thinking models enough `max_tokens` (1024+) so they have budget for both thinking and a response.

## Agent context history
Each agent maintains a rolling in-memory history of the last `_MAX_HISTORY` messages (defined in `ltap_rp/agents/llm_agent.py`). History is reset on container restart. Other agents' messages are stored as `role: user`, own messages as `role: assistant`.

## Adding a new agent
1. Add an entry to `agents:` in `config.yaml`
2. Add a corresponding service in `docker-compose.yml` (copy an existing agent block, change `AGENT_NAME`)
3. Increment `arbiter.num_agents` if you want it active
4. `docker compose up -d <new-service-name>` — no rebuild needed unless you changed Python
