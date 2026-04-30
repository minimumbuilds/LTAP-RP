"""HTTP chat interface: consumes ltap.transmissions and serves a live IM-style UI."""
import asyncio
import json
import logging
import os
from collections import deque
from datetime import datetime, timezone

from aiohttp import web
from aiokafka import AIOKafkaConsumer

from .config import DemoConfig, load as load_config
from .protocol import TOPIC_TRANSMISSIONS, decode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)

MAX_HISTORY = 500
PORT = int(os.environ.get("CHAT_PORT", "5000"))

# ---------------------------------------------------------------------------
# HTML (inlined so the service is a single deployable with no static assets)
# ---------------------------------------------------------------------------

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LTAP Chat</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:       #0f172a;
    --surface:  #1e293b;
    --border:   #334155;
    --muted:    #64748b;
    --text:     #e2e8f0;
    --subtext:  #94a3b8;
    /* agent palette — indexed by order received from /meta */
    --c0: #60a5fa; --c0bg: #1e3a5f;
    --c1: #4ade80; --c1bg: #14532d;
    --c2: #fb923c; --c2bg: #7c2d12;
    --c3: #c084fc; --c3bg: #4a1d96;
    --c4: #f472b6; --c4bg: #831843;
  }

  html, body { height: 100%; background: var(--bg); color: var(--text);
               font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }

  #app { display: flex; flex-direction: column; height: 100vh; }

  /* ---- header ---- */
  #header { background: var(--surface); border-bottom: 1px solid var(--border);
            padding: 14px 20px; flex-shrink: 0; }
  #header h1 { font-size: 18px; font-weight: 700; letter-spacing: .02em; }
  #header h1 span { color: var(--muted); font-weight: 400; font-size: 14px; margin-left: 8px; }
  #topic-line { margin-top: 6px; font-size: 12px; color: var(--subtext);
                max-width: 900px; line-height: 1.5; }

  /* ---- agents bar ---- */
  #agents-bar { display: flex; gap: 10px; padding: 10px 20px;
                background: var(--bg); border-bottom: 1px solid var(--border);
                flex-shrink: 0; flex-wrap: wrap; }
  .agent-chip { display: flex; align-items: center; gap: 6px;
                padding: 4px 10px; border-radius: 99px; font-size: 12px;
                font-weight: 600; border: 1px solid; }

  /* ---- messages ---- */
  #messages { flex: 1; overflow-y: auto; padding: 20px;
              display: flex; flex-direction: column; gap: 16px; }
  #messages:empty::after {
    content: "Waiting for the first message…";
    color: var(--muted); font-size: 14px; text-align: center; margin-top: 40px;
  }

  .msg { display: flex; gap: 12px; max-width: 820px; }
  .avatar { width: 38px; height: 38px; border-radius: 50%; display: flex;
            align-items: center; justify-content: center; font-size: 15px;
            font-weight: 700; flex-shrink: 0; margin-top: 2px; }
  .body  { flex: 1; }
  .meta-line { display: flex; align-items: baseline; gap: 8px; margin-bottom: 5px; }
  .sender { font-size: 13px; font-weight: 700; }
  .tick   { font-size: 11px; color: var(--muted); }
  .bubble { padding: 10px 14px; border-radius: 4px 14px 14px 14px;
            font-size: 14px; line-height: 1.65; white-space: pre-wrap;
            word-break: break-word; }
  .addressed-to { margin-top: 5px; font-size: 11px; color: var(--subtext);
                  font-style: italic; }

  /* ---- status bar ---- */
  #status-bar { display: flex; align-items: center; gap: 8px;
                padding: 8px 20px; background: var(--surface);
                border-top: 1px solid var(--border);
                font-size: 12px; color: var(--subtext); flex-shrink: 0; }
  .dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .dot.ok   { background: #4ade80; }
  .dot.warn { background: #fbbf24; }
  .dot.err  { background: #f87171; }

  /* scrollbar */
  #messages::-webkit-scrollbar { width: 6px; }
  #messages::-webkit-scrollbar-track { background: transparent; }
  #messages::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
</head>
<body>
<div id="app">
  <div id="header">
    <h1>LTAP Chat <span id="channel-label"></span></h1>
    <div id="topic-line"></div>
  </div>
  <div id="agents-bar"></div>
  <div id="messages"></div>
  <div id="status-bar">
    <div class="dot warn" id="dot"></div>
    <span id="status-text">Connecting…</span>
    <span style="margin-left:auto" id="msg-count"></span>
  </div>
</div>

<script>
// ---- palette (mirrors CSS vars) -------------------------------------------
const PALETTE = [
  { fg: "#60a5fa", bg: "#1e3a5f" },
  { fg: "#4ade80", bg: "#14532d" },
  { fg: "#fb923c", bg: "#7c2d12" },
  { fg: "#c084fc", bg: "#4a1d96" },
  { fg: "#f472b6", bg: "#831843" },
];
const agentColor = {};   // name → palette entry

function colorFor(name) {
  if (!agentColor[name]) {
    const idx = Object.keys(agentColor).length % PALETTE.length;
    agentColor[name] = PALETTE[idx];
  }
  return agentColor[name];
}

function esc(s) {
  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

// ---- DOM refs ---------------------------------------------------------------
const msgList   = document.getElementById("messages");
const statusDot = document.getElementById("dot");
const statusTxt = document.getElementById("status-text");
const msgCount  = document.getElementById("msg-count");
let total = 0;

function setStatus(state, text) {
  statusDot.className = "dot " + state;
  statusTxt.textContent = text;
}

// ---- render a single message ------------------------------------------------
function renderMsg(m) {
  const c = colorFor(m.sender);
  const initial = m.sender.charAt(0).toUpperCase();
  const addrLine = m.addressed_to
    ? `<div class="addressed-to">→ ${esc(m.addressed_to)}</div>` : "";

  const div = document.createElement("div");
  div.className = "msg";
  div.innerHTML = `
    <div class="avatar" style="background:${c.bg};color:${c.fg}">${initial}</div>
    <div class="body">
      <div class="meta-line">
        <span class="sender" style="color:${c.fg}">${esc(m.sender)}</span>
        <span class="tick">tick ${m.tick}</span>
      </div>
      <div class="bubble" style="background:${c.bg};border-left:3px solid ${c.fg}">
        ${esc(m.content)}
      </div>
      ${addrLine}
    </div>`;
  return div;
}

let autoScroll = true;
msgList.addEventListener("scroll", () => {
  autoScroll = msgList.scrollTop + msgList.clientHeight >= msgList.scrollHeight - 60;
});

function appendMsg(m) {
  msgList.appendChild(renderMsg(m));
  total++;
  msgCount.textContent = total + " messages";
  if (autoScroll) msgList.scrollTop = msgList.scrollHeight;
}

// ---- agents bar -------------------------------------------------------------
function buildAgentsBar(agents) {
  const bar = document.getElementById("agents-bar");
  bar.innerHTML = "";
  agents.forEach(name => {
    const c = colorFor(name);
    const chip = document.createElement("div");
    chip.className = "agent-chip";
    chip.style.cssText = `color:${c.fg};background:${c.bg};border-color:${c.fg}33`;
    chip.textContent = name;
    bar.appendChild(chip);
  });
}

// ---- boot -------------------------------------------------------------------
async function boot() {
  // 1. meta
  try {
    const meta = await fetch("/meta").then(r => r.json());
    document.getElementById("channel-label").textContent = "#" + meta.channel;
    document.getElementById("topic-line").textContent = meta.topic;
    buildAgentsBar(meta.agents);
  } catch(e) { /* non-fatal */ }

  // 2. history
  try {
    const history = await fetch("/history").then(r => r.json());
    history.forEach(appendMsg);
    msgList.scrollTop = msgList.scrollHeight;
  } catch(e) { /* non-fatal */ }

  // 3. SSE stream
  const es = new EventSource("/stream");
  es.onopen  = () => setStatus("ok",  "Live");
  es.onerror = () => setStatus("err", "Disconnected — reconnecting…");
  es.onmessage = e => appendMsg(JSON.parse(e.data));
}

boot();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Chat service — Kafka consumer + subscriber fan-out
# ---------------------------------------------------------------------------

class ChatService:
    def __init__(self, cfg: DemoConfig) -> None:
        self._cfg = cfg
        self._messages: deque[dict] = deque(maxlen=MAX_HISTORY)
        self._subscribers: set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=128)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def _broadcast(self, msg: dict) -> None:
        dead = set()
        for q in self._subscribers:
            try:
                q.put_nowait(json.dumps(msg))
            except asyncio.QueueFull:
                dead.add(q)
        for q in dead:
            self._subscribers.discard(q)

    def _handle(self, msg) -> None:
        key = msg.key.decode() if msg.key else ""
        # key format: tx:{channel}:{pid}:{tick}
        parts = key.split(":", 3)
        if len(parts) != 4 or parts[0] != "tx":
            return
        _, channel_id, sender, tick_str = parts
        if channel_id != self._cfg.kafka.channel:
            return

        data = decode(msg.value)
        content = (data.get("content") or "").strip()
        if not content:
            return

        record = {
            "tick": int(tick_str),
            "sender": sender,
            "content": content,
            "addressed_to": data.get("addressed_to"),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        self._messages.append(record)
        self._broadcast(record)
        log.info("chat  tick=%-4s %-8s %s", tick_str, sender, content[:80])

    async def consume(self) -> None:
        consumer = AIOKafkaConsumer(
            TOPIC_TRANSMISSIONS,
            bootstrap_servers=self._cfg.kafka.bootstrap,
            group_id="chat-ui",
            auto_offset_reset="earliest",
            fetch_max_wait_ms=self._cfg.kafka.fetch_max_wait_ms,
        )
        await consumer.start()
        log.info("Chat consumer started (bootstrap=%s)", self._cfg.kafka.bootstrap)
        try:
            async for msg in consumer:
                self._handle(msg)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("Chat consumer error")
        finally:
            await consumer.stop()


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------

async def handle_index(request: web.Request) -> web.Response:
    return web.Response(text=_HTML, content_type="text/html")


async def handle_history(request: web.Request) -> web.Response:
    svc: ChatService = request.app["svc"]
    return web.json_response(list(svc._messages))


async def handle_meta(request: web.Request) -> web.Response:
    cfg: DemoConfig = request.app["cfg"]
    return web.json_response({
        "channel": cfg.kafka.channel,
        "topic": cfg.discussion_topic,
        "agents": [a.name for a in cfg.active_agents()],
    })


async def handle_stream(request: web.Request) -> web.StreamResponse:
    svc: ChatService = request.app["svc"]
    resp = web.StreamResponse()
    resp.headers["Content-Type"] = "text/event-stream"
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["X-Accel-Buffering"] = "no"
    await resp.prepare(request)

    q = svc.subscribe()
    try:
        while True:
            try:
                payload = await asyncio.wait_for(q.get(), timeout=25)
                await resp.write(f"data: {payload}\n\n".encode())
            except asyncio.TimeoutError:
                await resp.write(b": keepalive\n\n")
    except (ConnectionResetError, ConnectionAbortedError, asyncio.CancelledError):
        pass
    finally:
        svc.unsubscribe(q)
    return resp


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    cfg = load_config(os.environ.get("CONFIG_PATH"))
    svc = ChatService(cfg)

    app = web.Application()
    app["svc"] = svc
    app["cfg"] = cfg
    app.router.add_get("/", handle_index)
    app.router.add_get("/history", handle_history)
    app.router.add_get("/meta", handle_meta)
    app.router.add_get("/stream", handle_stream)

    consume_task = asyncio.create_task(svc.consume())

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Chat UI → http://0.0.0.0:%d", PORT)

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        consume_task.cancel()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
