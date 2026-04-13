"""
Telegram Slave Userbot
========================
Riceve la configurazione dal master via HTTP e fa spam autonomamente.
Non risponde a nessun comando — tutto è controllato dal master.

Auto-risposta PM: risponde solo a chi è membro di almeno uno
dei gruppi di destinazione configurati dal master.

── VARIABILI D'AMBIENTE ──────────────────────────────────────────
  API_ID          — API ID dell'account slave  (fallback: API_ID_1)
  API_HASH        — API Hash dell'account slave (fallback: API_HASH_1)
  SESSION_STRING  — Session string              (fallback: SESSION_STRING_1)
  MASTER_URL      — URL base del master (es. https://master.railway.app)
  SLAVE_NUMBER    — Numero dello slave (es. 1, 2, 3 …)
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
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

trigger_now = asyncio.Event()
config: dict = {}

# Cache membership: {user_id: (is_member: bool, timestamp: float)}
# Evita chiamate Telegram ripetute per lo stesso utente
MEMBERSHIP_CACHE: dict[int, tuple[bool, float]] = {}
MEMBERSHIP_CACHE_TTL = 600  # 10 minuti


# ── Fetch config dal master ────────────────────────────────────────────────────

def _empty_config() -> dict:
    return {
        "sources": [],
        "targets": [],
        "interval": 10,
        "running": True,
        "auto_reply_text": "",
        "rotation_indices": {},
    }

async def fetch_config_from_master() -> dict:
    """
    Scarica slave_config.json dal master via HTTP.
    Solo HTTP — zero chiamate Telegram, zero flood wait.
    """
    global config

    master_url = os.environ.get("MASTER_URL", "").rstrip("/")
    if not master_url:
        log.warning("⚠️  MASTER_URL non impostato — uso config precedente")
        return config or _empty_config()

    slave_n = str(os.environ.get("SLAVE_NUMBER", "1"))

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{master_url}/api/slave-config",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    log.error(f"❌ Master ha risposto {resp.status} — uso config precedente")
                    return config or _empty_config()
                raw = await resp.json(content_type=None)
    except Exception as e:
        log.warning(f"⚠️ Impossibile contattare il master: {e} — uso config precedente")
        return config or _empty_config()

    cfg = _empty_config()
    cfg["running"]         = raw.get("running", True)
    cfg["interval"]        = raw.get("interval", 10)
    cfg["auto_reply_text"] = raw.get("auto_reply_text", "")
    cfg["targets"]         = raw.get("targets", [])

    # Sorgenti dedicate a questo slave, altrimenti quelle del master
    slave_sources = raw.get("slave_sources", {}).get(slave_n)
    if slave_sources:
        cfg["sources"] = slave_sources
        log.info(f"📥 Sorgenti slave {slave_n}: {slave_sources}")
    else:
        cfg["sources"] = raw.get("sources", [])

    # Intervallo dedicato a questo slave, altrimenti quello del master
    slave_interval = raw.get("slave_intervals", {}).get(slave_n)
    if slave_interval:
        cfg["interval"] = int(slave_interval)
        log.info(f"⏰ Intervallo slave {slave_n}: {cfg['interval']} min")

    # Preserva gli indici di rotazione già in memoria
    cfg["rotation_indices"] = (config or {}).get("rotation_indices", {})

    log.info(
        f"🔄 Config aggiornata | "
        f"{len(cfg['sources'])} sorgenti | "
        f"{len(cfg['targets'])} destinazioni | "
        f"intervallo {cfg['interval']} min | "
        f"running={cfg['running']}"
    )
    return cfg


# ── Invio messaggi ────────────────────────────────────────────────────────────

async def copy_to_target(
    client: TelegramClient, msg, target, cfg: dict, _retries: int = 0
) -> None:
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
                log.warning(f"⚠️ Media fallito su {target} ({media_err}) — invio solo testo")
                if text:
                    await client.send_message(target, text, formatting_entities=entities)
        else:
            await client.send_message(target, text, formatting_entities=entities)

        log.info(f"✅ msg {msg.id} → {target}")

    except FloodWaitError as e:
        if _retries >= 3:
            log.error(f"❌ FloodWait ripetuto ({_retries}x) su {target}, messaggio saltato.")
            return
        wait = e.seconds + 1
        log.warning(f"⏳ FloodWait {e.seconds}s (tentativo {_retries + 1}/3)")
        await asyncio.sleep(wait)
        await copy_to_target(client, msg, target, cfg, _retries + 1)
    except Exception as e:
        log.error(f"❌ Errore → {target}: {e}")


# ── Spam loop ─────────────────────────────────────────────────────────────────

async def spam_loop(client: TelegramClient) -> None:
    """
    Ad ogni ciclo:
      1. Aggiorna config dal master via HTTP (nessuna chiamata Telegram)
      2. Se running=False, aspetta 30s e riprova
      3. Invia il prossimo messaggio ruotato a tutte le destinazioni
    """
    global config

    while True:
        config = await fetch_config_from_master()

        if not config.get("running", True):
            log.info("⏸ Slave in pausa (running=False dal master)")
            await asyncio.sleep(30)
            continue

        sources = config.get("sources", [])
        targets = config.get("targets", [])

        if not sources:
            log.info("📭 Nessuna sorgente — aspetto prossimo ciclo")
        elif not targets:
            log.info("📭 Nessuna destinazione — aspetto prossimo ciclo")
        else:
            for source in sources[:]:
                try:
                    all_msgs = await client.get_messages(source, limit=200)
                    valid = sorted(
                        [m for m in all_msgs if m.message or m.media],
                        key=lambda m: m.id,
                    )
                    if not valid:
                        log.info(f"📭 Nessun post valido in {source}")
                        continue

                    key = str(source)
                    idx = config["rotation_indices"].get(key, 0) % len(valid)
                    msg = valid[idx]
                    log.info(f"📤 Post {idx + 1}/{len(valid)} (id={msg.id}) da {source}")

                    for t in targets:
                        await copy_to_target(client, msg, t, config)
                        await asyncio.sleep(1.5)
                    config["rotation_indices"][key] = (idx + 1) % len(valid)

                except Exception as e:
                    log.error(f"❌ Errore sorgente {source}: {e}")

        interval = max(1, config.get("interval", 10))
        log.info(f"⏰ Prossimo ciclo tra {interval} min")
        try:
            await asyncio.wait_for(trigger_now.wait(), timeout=interval * 60)
            trigger_now.clear()
        except asyncio.TimeoutError:
            pass


# ── Membership check ──────────────────────────────────────────────────────────

def _bare_id(peer_id) -> int:
    """
    Normalizza qualsiasi formato di peer_id al bare ID numerico.
    Telethon salva i canali/supergruppi come -100XXXXXXXXXX,
    ma get_common_chats() restituisce il bare ID positivo.
    """
    pid = int(peer_id)
    if pid < 0:
        s = str(pid)
        if s.startswith("-100"):
            return int(s[4:])
        return abs(pid)
    return pid


async def is_member_of_any_target(client: TelegramClient, user_id: int) -> bool:
    """
    Controlla se l'utente è membro di almeno uno dei gruppi di destinazione
    usando get_common_chats(): una singola chiamata API che restituisce
    tutti i gruppi in comune tra lo slave e l'utente.

    Nessun bisogno di permessi admin. Nessun loop per gruppo.
    Cache 10 minuti per utente per non ripetere la chiamata.
    """
    targets = config.get("targets", [])
    if not targets:
        return False

    # Controlla cache
    now = time.monotonic()
    cached = MEMBERSHIP_CACHE.get(user_id)
    if cached is not None:
        result, ts = cached
        if now - ts < MEMBERSHIP_CACHE_TTL:
            log.debug(f"👤 {user_id} — risultato da cache: {result}")
            return result
        del MEMBERSHIP_CACHE[user_id]

    # Singola chiamata API: gruppi in comune tra questo account e l'utente
    try:
        common_chats = await client.get_common_chats(user_id)
    except Exception as e:
        log.warning(f"⚠️ get_common_chats fallito per {user_id}: {e}")
        MEMBERSHIP_CACHE[user_id] = (False, now)
        return False

    # Confronta gli ID normalizzati
    common_ids  = {_bare_id(c.id) for c in common_chats}
    target_ids  = {_bare_id(t)   for t in targets}
    is_member   = bool(common_ids & target_ids)

    MEMBERSHIP_CACHE[user_id] = (is_member, now)
    log.debug(
        f"👤 {user_id} — gruppi comuni: {len(common_ids)}, "
        f"match destinazioni: {is_member} (salvato in cache)"
    )
    return is_member


# ── Auto-risposta PM ──────────────────────────────────────────────────────────

async def handle_auto_reply(client: TelegramClient, event) -> None:
    """
    Risponde solo se il mittente è membro di almeno uno dei gruppi destinazione.
    """
    reply_text = config.get("auto_reply_text", "")
    if not reply_text:
        return

    sender = await event.get_sender()
    user_id = sender.id

    # Controlla membership prima di rispondere
    if not await is_member_of_any_target(client, user_id):
        log.debug(f"👤 {user_id} non è in nessun gruppo destinazione — auto-risposta saltata")
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
        log.info(f"💬 Auto-risposta inviata a {user_id} (membro confermato)")
    except Exception as e:
        log.error(f"❌ Errore auto-risposta: {e}")


# ── HTTP healthcheck ──────────────────────────────────────────────────────────

async def start_http_server() -> None:
    async def handle_health(request):
        return web.Response(
            text=json.dumps({
                "status":  "ok",
                "slave":   os.environ.get("SLAVE_NUMBER", "1"),
                "running": config.get("running", True),
            }),
            content_type="application/json",
        )

    port = int(os.environ.get("PORT", 8080))
    app  = web.Application()
    app.router.add_get("/healthz", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info(f"🌐 HTTP healthcheck sulla porta {port}")
    while True:
        await asyncio.sleep(3600)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    global config

    api_id_str     = os.environ.get("API_ID") or os.environ.get("API_ID_1")
    api_hash       = os.environ.get("API_HASH") or os.environ.get("API_HASH_1")
    session_string = os.environ.get("SESSION_STRING") or os.environ.get("SESSION_STRING_1", "")

    if not api_id_str or not api_hash:
        log.error("❌ Imposta API_ID e API_HASH come variabili d'ambiente!")
        return

    slave_n = os.environ.get("SLAVE_NUMBER", "1")
    log.info(f"🤖 Slave {slave_n} in avvio...")

    client = TelegramClient(StringSession(session_string), int(api_id_str), api_hash)
    await client.start()

    if not session_string:
        print("\n" + "=" * 60)
        print("✅ Salva questa SESSION_STRING nelle variabili d'ambiente:")
        print(client.session.save())
        print("=" * 60 + "\n")

    config = await fetch_config_from_master()

    log.info(
        f"🚀 Slave {slave_n} avviato | "
        f"{len(config['sources'])} sorgenti | "
        f"{len(config['targets'])} destinazioni | "
        f"intervallo {config['interval']} min"
    )

    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def auto_reply_handler(event):
        if config.get("auto_reply_text"):
            await handle_auto_reply(client, event)

    spam_task = asyncio.create_task(spam_loop(client))
    http_task = asyncio.create_task(start_http_server())

    log.info(f"🎉 Slave {slave_n} pronto — nessun comando, tutto dal master")
    await client.run_until_disconnected()

    spam_task.cancel()
    http_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
