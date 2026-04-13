"""
Telegram Slave Userbot
========================
Un solo processo, più account slave in parallelo.
Ogni account è identificato da un numero nel nome delle variabili.

Riceve la configurazione dal master via HTTP.
Non risponde a nessun comando — tutto è controllato dal master.

Auto-risposta PM: solo per utenti che sono in almeno uno dei gruppi destinazione.

── VARIABILI D'AMBIENTE ──────────────────────────────────────────
  MASTER_URL        — URL base del master (es. https://master.railway.app)

  Per ogni slave N (1, 2, 3 …):
    API_ID_N          — API ID dell'account slave N
    API_HASH_N        — API Hash dell'account slave N
    SESSION_STRING_N  — Session string dell'account slave N

  Esempio per 3 slave:
    API_ID_1, API_HASH_1, SESSION_STRING_1
    API_ID_2, API_HASH_2, SESSION_STRING_2
    API_ID_3, API_HASH_3, SESSION_STRING_3
"""

import asyncio
import json
import logging
import os
import time

import aiohttp
from aiohttp import web
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s | slave=%(slave_n)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

MASTER_CONFIG: dict = {}
MASTER_CONFIG_LOCK = asyncio.Lock()

MEMBERSHIP_CACHE: dict[int, tuple[bool, float]] = {}
MEMBERSHIP_CACHE_TTL = 600  # 10 minuti


# ── Logging helper ────────────────────────────────────────────────────────────

def get_log(slave_n: str):
    """Logger con il numero slave già nel nome — visibile nei log di Railway."""
    logger = logging.getLogger(f"slave.{slave_n}")
    logger.addFilter(lambda r: r.__setattr__("slave_n", slave_n) or True)
    return logging.LoggerAdapter(logging.getLogger(f"slave.{slave_n}"), {"slave_n": slave_n})


# ── Fetch config dal master ────────────────────────────────────────────────────

def _empty_config() -> dict:
    return {
        "sources": [],
        "targets": [],
        "interval": 10,
        "running": True,
        "auto_reply_text": "",
        "slave_intervals": {},
        "slave_sources": {},
        "rotation_indices": {},
    }

async def fetch_master_config(log) -> dict:
    """
    Scarica slave_config.json dal master via HTTP.
    Chiamata HTTP pura — zero API Telegram, zero flood wait.
    """
    global MASTER_CONFIG

    master_url = os.environ.get("MASTER_URL", "").rstrip("/")
    if not master_url:
        log.warning("MASTER_URL non impostato — uso config precedente")
        return MASTER_CONFIG or _empty_config()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{master_url}/api/slave-config",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    log.error(f"Master ha risposto {resp.status} — uso config precedente")
                    return MASTER_CONFIG or _empty_config()
                data = await resp.json(content_type=None)

        async with MASTER_CONFIG_LOCK:
            old_rotation = MASTER_CONFIG.get("rotation_indices", {})
            MASTER_CONFIG = data
            MASTER_CONFIG.setdefault("rotation_indices", old_rotation)

        return MASTER_CONFIG
    except Exception as e:
        log.warning(f"Impossibile contattare il master: {e} — uso config precedente")
        return MASTER_CONFIG or _empty_config()


def _slave_config(raw: dict, slave_n: str) -> dict:
    """
    Estrae dalla config master i valori specifici per questo slave:
    - slave_sources[n]  se esiste, altrimenti le sorgenti master
    - slave_intervals[n] se esiste, altrimenti l'intervallo master
    """
    cfg = dict(raw)

    slave_sources = raw.get("slave_sources", {}).get(slave_n)
    cfg["sources"] = slave_sources if slave_sources else raw.get("sources", [])

    slave_interval = raw.get("slave_intervals", {}).get(slave_n)
    cfg["interval"] = int(slave_interval) if slave_interval else raw.get("interval", 10)

    return cfg


# ── Invio messaggi ────────────────────────────────────────────────────────────

async def copy_to_target(client, msg, target, _retries: int = 0, log=None) -> None:
    log = log or logging.getLogger("slave")
    try:
        text     = msg.message or getattr(msg, "caption", "") or ""
        entities = msg.entities or []

        if msg.media:
            try:
                await client.send_file(
                    target, file=msg.media, caption=text,
                    formatting_entities=entities, silent=False,
                )
            except Exception as media_err:
                log.warning(f"Media fallito su {target} ({media_err}) — invio solo testo")
                if text:
                    await client.send_message(target, text, formatting_entities=entities)
        else:
            await client.send_message(target, text, formatting_entities=entities)

        log.info(f"msg {msg.id} → {target}")

    except FloodWaitError as e:
        if _retries >= 3:
            log.error(f"FloodWait ripetuto ({_retries}x) su {target} — saltato")
            return
        log.warning(f"FloodWait {e.seconds}s (tentativo {_retries + 1}/3)")
        await asyncio.sleep(e.seconds + 1)
        await copy_to_target(client, msg, target, _retries + 1, log)
    except Exception as e:
        log.error(f"Errore → {target}: {e}")


# ── Spam loop per singolo slave ───────────────────────────────────────────────

async def spam_loop(client: TelegramClient, slave_n: str) -> None:
    """
    Loop indipendente per ogni account slave.
    Aggiorna la config dal master ad ogni ciclo (solo HTTP).
    Usa la config specifica per questo slave (sorgenti e intervallo dedicati).
    """
    log = get_log(slave_n)
    rotation: dict = {}

    while True:
        raw = await fetch_master_config(log)
        cfg = _slave_config(raw, slave_n)

        if not cfg.get("running", True):
            log.info("Slave in pausa (running=False)")
            await asyncio.sleep(30)
            continue

        sources = cfg.get("sources", [])
        targets = cfg.get("targets", [])

        if not sources:
            log.info("Nessuna sorgente — aspetto")
        elif not targets:
            log.info("Nessuna destinazione — aspetto")
        else:
            for source in sources[:]:
                try:
                    all_msgs = await client.get_messages(source, limit=200)
                    valid = sorted(
                        [m for m in all_msgs if m.message or m.media],
                        key=lambda m: m.id,
                    )
                    if not valid:
                        log.info(f"Nessun post valido in {source}")
                        continue

                    key = str(source)
                    idx = rotation.get(key, 0) % len(valid)
                    msg = valid[idx]
                    log.info(f"Post {idx + 1}/{len(valid)} (id={msg.id}) da {source}")

                    for t in targets:
                        await copy_to_target(client, msg, t, log=log)
                        await asyncio.sleep(1.5)

                    rotation[key] = (idx + 1) % len(valid)

                except Exception as e:
                    log.error(f"Errore sorgente {source}: {e}")

        interval = max(1, cfg.get("interval", 10))
        log.info(f"Prossimo ciclo tra {interval} min")
        await asyncio.sleep(interval * 60)


# ── Membership check ──────────────────────────────────────────────────────────

def _bare_id(peer_id) -> int:
    pid = int(peer_id)
    if pid < 0:
        s = str(pid)
        if s.startswith("-100"):
            return int(s[4:])
        return abs(pid)
    return pid


async def is_member_of_any_target(client: TelegramClient, user_id: int) -> bool:
    targets = MASTER_CONFIG.get("targets", [])
    if not targets:
        return False

    now = time.monotonic()
    cached = MEMBERSHIP_CACHE.get(user_id)
    if cached is not None:
        result, ts = cached
        if now - ts < MEMBERSHIP_CACHE_TTL:
            return result
        del MEMBERSHIP_CACHE[user_id]

    try:
        common_chats = await client.get_common_chats(user_id)
    except Exception:
        MEMBERSHIP_CACHE[user_id] = (False, now)
        return False

    common_ids = {_bare_id(c.id) for c in common_chats}
    target_ids = {_bare_id(t)   for t in targets}
    is_member  = bool(common_ids & target_ids)

    MEMBERSHIP_CACHE[user_id] = (is_member, now)
    return is_member


# ── Auto-risposta PM ──────────────────────────────────────────────────────────

async def handle_auto_reply(client: TelegramClient, event, log) -> None:
    reply_text = MASTER_CONFIG.get("auto_reply_text", "")
    if not reply_text:
        return

    sender = await event.get_sender()
    if not await is_member_of_any_target(client, sender.id):
        return

    try:
        text = reply_text.format(
            first_name=getattr(sender, "first_name", "") or "",
            last_name=getattr(sender,  "last_name",  "") or "",
            full_name=(
                ((getattr(sender, "first_name", "") or "") + " " +
                 (getattr(sender, "last_name",  "") or "")).strip()
            ),
            username=getattr(sender, "username", "") or "",
        )
        await event.reply(text)
        log.info(f"Auto-risposta inviata a {sender.id}")
    except Exception as e:
        log.error(f"Errore auto-risposta: {e}")


# ── Avvio singolo slave ───────────────────────────────────────────────────────

async def start_slave(slave_n: str) -> asyncio.Task | None:
    """
    Avvia un account slave.
    Legge le credenziali da API_ID_N, API_HASH_N, SESSION_STRING_N.
    Ritorna il task dello spam loop, o None se le credenziali mancano.
    """
    log = get_log(slave_n)

    api_id_str     = os.environ.get(f"API_ID_{slave_n}")
    api_hash       = os.environ.get(f"API_HASH_{slave_n}")
    session_string = os.environ.get(f"SESSION_STRING_{slave_n}", "")

    if not api_id_str or not api_hash:
        log.info(f"API_ID_{slave_n} non trovato — slave non avviato")
        return None

    log.info(f"Slave {slave_n} in avvio...")

    client = TelegramClient(StringSession(session_string), int(api_id_str), api_hash)
    await client.start()

    if not session_string:
        print(f"\n{'='*60}")
        print(f"[Slave {slave_n}] Salva questa SESSION_STRING_{slave_n}:")
        print(client.session.save())
        print(f"{'='*60}\n")

    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def auto_reply_handler(event):
        if MASTER_CONFIG.get("auto_reply_text"):
            await handle_auto_reply(client, event, log)

    log.info(f"Slave {slave_n} pronto")
    return asyncio.create_task(spam_loop(client, slave_n))


# ── HTTP healthcheck ──────────────────────────────────────────────────────────

async def start_http_server(slave_count: int) -> None:
    async def handle_health(request):
        return web.Response(
            text=json.dumps({
                "status":      "ok",
                "slave_count": slave_count,
                "running":     MASTER_CONFIG.get("running", True),
            }),
            content_type="application/json",
        )

    port = int(os.environ.get("PORT", 8080))
    app  = web.Application()
    app.router.add_get("/healthz", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logging.info(f"HTTP healthcheck sulla porta {port} — {slave_count} slave attivi")
    while True:
        await asyncio.sleep(3600)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("main")

    # Fetch iniziale config dal master
    master_log = get_log("?")
    await fetch_master_config(master_log)

    # Avvia tutti gli slave trovati (cerca API_ID_1, API_ID_2, API_ID_3 …)
    tasks = []
    slave_n = 1
    while True:
        task = await start_slave(str(slave_n))
        if task is None and slave_n > 1:
            break  # nessuna credenziale trovata per questo numero → stop
        if task:
            tasks.append(task)
        slave_n += 1

    if not tasks:
        log.error("Nessun slave avviato. Imposta almeno API_ID_1, API_HASH_1, SESSION_STRING_1")
        return

    log.info(f"Totale slave avviati: {len(tasks)}")

    http_task = asyncio.create_task(start_http_server(len(tasks)))

    try:
        await asyncio.gather(*tasks, http_task)
    except Exception as e:
        log.error(f"Errore: {e}")


if __name__ == "__main__":
    asyncio.run(main())
