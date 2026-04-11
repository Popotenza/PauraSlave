"""
Telegram Master Userbot
========================
Controlla tutti gli slave via HTTP.
Invia comandi dai tuoi Messaggi Salvati.

── VARIABILI D'AMBIENTE ──────────────────────────────────────────
  API_ID          — API ID dell'account master
  API_HASH        — API Hash dell'account master
  SESSION_STRING  — Session string (vuota al primo avvio)
"""

import asyncio
import json
import logging
import os
import urllib.parse
from aiohttp import web
from telethon import TelegramClient, events, Button
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.utils import get_peer_id
from telethon.tl.functions.messages import GetDialogFiltersRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CONFIG_FILE       = os.path.join(os.path.dirname(__file__), "config.json")
SLAVE_CONFIG_FILE = os.path.join(os.path.dirname(__file__), "slave_config.json")

trigger_now    = asyncio.Event()
config: dict   = {}
_folder_tasks: dict[str, asyncio.Task] = {}   # task per ogni regola cartella


# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        cfg.setdefault("buttons_rows", [])
        cfg.setdefault("interval", 10)
        cfg.setdefault("slave_intervals", {})
        cfg.setdefault("slave_sources", {})
        cfg.setdefault("last_ids", {})
        cfg.setdefault("running", True)
        cfg.setdefault("rotation_indices", {})
        cfg.setdefault("auto_reply_text", "")
        cfg.setdefault("folder_rules", {})
        return cfg
    return {
        "sources": [],
        "targets": [],
        "interval": 10,
        "slave_intervals": {},
        "slave_sources": {},
        "last_ids": {},
        "running": True,
        "buttons_rows": [],
        "rotation_indices": {},
        "auto_reply_text": "",
        "folder_rules": {},
    }

def save_config(cfg: dict) -> None:
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=4)

async def update_slave_config(client: TelegramClient, cfg: dict) -> None:
    async def resolve(peer_id):
        try:
            ent = await client.get_entity(peer_id)
            username = getattr(ent, "username", None)
            if username:
                return f"@{username}"
        except Exception:
            pass
        return peer_id

    sources = [await resolve(s) for s in cfg.get("sources", [])]
    targets = [await resolve(t) for t in cfg.get("targets", [])]

    resolved_slave_sources = {}
    for k, peers in cfg.get("slave_sources", {}).items():
        resolved_slave_sources[k] = [await resolve(p) for p in peers]

    slave_cfg = {
        "sources":         sources,
        "targets":         targets,
        "buttons_rows":    cfg.get("buttons_rows", []),
        "interval":        cfg.get("interval", 10),
        "slave_intervals": cfg.get("slave_intervals", {}),
        "slave_sources":   resolved_slave_sources,
        "running":         cfg.get("running", True),
        "auto_reply_text": cfg.get("auto_reply_text", ""),
    }
    with open(SLAVE_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(slave_cfg, f, ensure_ascii=False, indent=4)
    log.info("🔄 slave_config.json aggiornato")


# ── Bottoni ───────────────────────────────────────────────────────────────────

COLOR_MAP = {"#g": "🟢", "#r": "🔴", "#p": "🔵"}

def _apply_color(text: str) -> str:
    for prefix, emoji in COLOR_MAP.items():
        if text.lower().startswith(prefix + " "):
            return emoji + " " + text[len(prefix):].strip()
        if text.lower().startswith(prefix):
            return emoji + text[len(prefix):]
    return text

def _make_url(raw_url: str) -> str:
    raw_url = raw_url.strip()
    if raw_url.lower().startswith("share:"):
        encoded = urllib.parse.quote(raw_url[6:].strip(), safe="")
        return f"https://t.me/share/url?text={encoded}"
    return raw_url

def parse_buttons(definition: str) -> list[list[dict]]:
    rows = []
    for line in definition.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        row = []
        for btn_def in line.split("&&"):
            btn_def = btn_def.strip()
            if " - " not in btn_def:
                log.warning(f"Bottone ignorato: {btn_def!r}")
                continue
            raw_text, raw_url = btn_def.split(" - ", 1)
            btn_text = _apply_color(raw_text.strip())
            btn_url  = _make_url(raw_url.strip())
            if btn_text and btn_url:
                row.append({"text": btn_text, "url": btn_url})
        if row:
            rows.append(row)
    return rows

def build_buttons(cfg: dict) -> list | None:
    rows = cfg.get("buttons_rows", [])
    if not rows:
        return None
    return [[Button.url(b["text"], b["url"]) for b in row] for row in rows]

def format_buttons_preview(rows: list[list[dict]]) -> str:
    if not rows:
        return "Nessun bottone configurato."
    return "\n".join(
        "  " + " | ".join(f"[{b['text']}]" for b in row)
        for row in rows
    )


# ── Cartelle ──────────────────────────────────────────────────────────────────

def _folder_title(f) -> str:
    title = f.title
    if isinstance(title, str):
        return title
    if hasattr(title, "text"):
        return title.text
    return str(title)

async def get_folders(client: TelegramClient):
    result = await client(GetDialogFiltersRequest())
    return [f for f in result.filters if hasattr(f, "include_peers")]

async def resolve_folder_peers(client: TelegramClient, folder) -> list[tuple]:
    peers = []
    for peer in folder.include_peers:
        try:
            entity  = await client.get_entity(peer)
            peer_id = get_peer_id(entity)
            name    = getattr(entity, "title", None) or getattr(entity, "username", None) or str(peer_id)
            peers.append((peer_id, name))
        except Exception as e:
            log.warning(f"Impossibile risolvere peer {peer}: {e}")
    return peers

async def add_folder_to_list(client: TelegramClient, event, folder_name: str, is_source: bool) -> None:
    global config
    folders = await get_folders(client)
    matched = next((f for f in folders if _folder_title(f).lower() == folder_name.lower()), None)
    if not matched:
        available = "\n".join(f"  • {_folder_title(f)}" for f in folders) or "Nessuna cartella trovata"
        await event.reply(
            f"❌ **Cartella non trovata:** `{folder_name}`\n\n"
            f"📁 **Disponibili:**\n{available}"
        )
        return

    peers = await resolve_folder_peers(client, matched)
    if not peers:
        await event.reply("⚠️ Cartella vuota o non risolvibile.")
        return

    key  = "sources" if is_source else "targets"
    tipo = "sorgenti" if is_source else "destinazioni"
    added, skipped = [], []
    for peer_id, name in peers:
        if peer_id not in config[key]:
            config[key].append(peer_id)
            added.append(name)
        else:
            skipped.append(name)

    save_config(config)
    await update_slave_config(client, config)

    msg = f"📁 **{_folder_title(matched)}**\n\n"
    if added:
        msg += f"✅ Aggiunti come {tipo} ({len(added)}):\n" + "\n".join(f"  • {n}" for n in added) + "\n"
    if skipped:
        msg += f"\n⚠️ Già presenti ({len(skipped)}):\n" + "\n".join(f"  • {n}" for n in skipped)
    await event.reply(msg)


# ── Invio messaggi ────────────────────────────────────────────────────────────

async def copy_to_target(
    client: TelegramClient, msg, target, cfg: dict, _retries: int = 0,
) -> None:
    try:
        text     = msg.message or getattr(msg, "caption", "") or ""
        entities = msg.entities or []
        buttons  = build_buttons(cfg)

        if msg.media:
            try:
                await client.send_file(
                    target, file=msg.media, caption=text,
                    formatting_entities=entities, buttons=buttons, silent=False
                )
            except Exception as media_err:
                log.warning(f"⚠️ Media fallito su {target} ({media_err}) — invio solo testo")
                if text:
                    await client.send_message(target, text, formatting_entities=entities, buttons=buttons)
        else:
            await client.send_message(target, text, formatting_entities=entities, buttons=buttons)

        log.info(f"✅ msg {msg.id} → {target}")

    except FloodWaitError as e:
        if _retries >= 3:
            log.error(f"❌ FloodWait ripetuto ({_retries}x) su {target}, messaggio saltato.")
            return
        log.warning(f"⏳ FloodWait {e.seconds}s (tentativo {_retries + 1}/3)")
        await asyncio.sleep(e.seconds + 1)
        await copy_to_target(client, msg, target, cfg, _retries + 1)
    except Exception as e:
        log.error(f"❌ Errore → {target}: {e}")

def _folder_rule_peer_ids(cfg: dict) -> set:
    """Ritorna tutti i peer_id gestiti da una regola cartella (esclusi dal loop principale)."""
    ids = set()
    for rule in cfg.get("folder_rules", {}).values():
        ids.update(str(p) for p in rule.get("peers", []))
    return ids

async def send_to_all(client: TelegramClient, msg, cfg: dict) -> None:
    if not cfg["targets"]:
        return
    # Esclude i target gestiti da regole cartella
    folder_peers = _folder_rule_peer_ids(cfg)
    targets = [t for t in cfg["targets"] if str(t) not in folder_peers]
    if not targets:
        return
    await asyncio.gather(*[copy_to_target(client, msg, t, cfg) for t in targets])


# ── Spam loop principale ──────────────────────────────────────────────────────

async def spam_loop(client: TelegramClient, cfg: dict) -> None:
    while True:
        if not cfg["running"]:
            await asyncio.sleep(5)
            continue

        try:
            await asyncio.wait_for(trigger_now.wait(), timeout=cfg["interval"] * 60)
            trigger_now.clear()
        except asyncio.TimeoutError:
            pass

        if not cfg["running"]:
            continue

        updated = False
        for source in cfg["sources"][:]:
            try:
                all_msgs = await client.get_messages(source, limit=200)
                valid = sorted([m for m in all_msgs if m.message or m.media], key=lambda m: m.id)
                if not valid:
                    log.info(f"📭 Nessun post valido in {source}")
                    continue

                key = str(source)
                idx = cfg.setdefault("rotation_indices", {}).get(key, 0) % len(valid)
                msg = valid[idx]
                log.info(f"📤 Post {idx + 1}/{len(valid)} (id={msg.id}) da {source}")
                await send_to_all(client, msg, cfg)
                cfg["rotation_indices"][key] = (idx + 1) % len(valid)
                updated = True
            except Exception as e:
                log.error(f"❌ Errore sorgente {source}: {e}")

        if updated:
            save_config(cfg)


# ── Loop regola cartella ──────────────────────────────────────────────────────

async def folder_rule_loop(client: TelegramClient, folder_name: str) -> None:
    """Loop indipendente per una cartella con sorgente e intervallo dedicati."""
    log.info(f"📁 Loop cartella '{folder_name}' avviato")

    while True:
        rule = config.get("folder_rules", {}).get(folder_name)
        if not rule:
            log.info(f"📁 Regola '{folder_name}' rimossa — loop terminato")
            return

        interval = max(1, rule.get("interval", 10))
        await asyncio.sleep(interval * 60)

        # Rilegge la regola dopo il sleep (potrebbe essere cambiata)
        rule = config.get("folder_rules", {}).get(folder_name)
        if not rule:
            return

        if not config.get("running", True):
            continue

        source = rule.get("source")
        peers  = rule.get("peers", [])

        if not source or not peers:
            log.info(f"📁 Cartella '{folder_name}' senza sorgente o peer — salto")
            continue

        try:
            all_msgs = await client.get_messages(source, limit=200)
            valid = sorted([m for m in all_msgs if m.message or m.media], key=lambda m: m.id)
            if not valid:
                log.info(f"📭 Nessun post in {source} per la cartella '{folder_name}'")
                continue

            rot_key = f"folder_{folder_name}"
            idx = config.setdefault("rotation_indices", {}).get(rot_key, 0) % len(valid)
            msg = valid[idx]
            log.info(
                f"📤 [Cartella '{folder_name}'] Post {idx + 1}/{len(valid)} "
                f"(id={msg.id}) da {source} → {len(peers)} gruppi"
            )

            await asyncio.gather(*[copy_to_target(client, msg, t, config) for t in peers])
            config["rotation_indices"][rot_key] = (idx + 1) % len(valid)
            save_config(config)

        except Exception as e:
            log.error(f"❌ Errore cartella '{folder_name}': {e}")


def _start_folder_task(client: TelegramClient, folder_name: str) -> None:
    """Avvia (o riavvia) il task per una regola cartella."""
    old = _folder_tasks.get(folder_name)
    if old and not old.done():
        old.cancel()
    _folder_tasks[folder_name] = asyncio.create_task(folder_rule_loop(client, folder_name))

def _stop_folder_task(folder_name: str) -> None:
    """Ferma il task di una regola cartella."""
    task = _folder_tasks.pop(folder_name, None)
    if task and not task.done():
        task.cancel()


# ── Aggiungi entità ───────────────────────────────────────────────────────────

async def add_entity(client: TelegramClient, event, link: str, is_source: bool) -> None:
    global config
    try:
        target  = "me" if link.lower() in ["me", "saved"] else link.strip()
        entity  = await client.get_entity(target)
        peer_id = get_peer_id(entity)
        key     = "sources" if is_source else "targets"
        tipo    = "sorgente" if is_source else "destinazione"

        if peer_id not in config[key]:
            config[key].append(peer_id)
            save_config(config)
            await update_slave_config(client, config)
            name = getattr(entity, "title", None) or getattr(entity, "username", None) or str(peer_id)
            await event.reply(f"✅ Aggiunto come **{tipo}**: `{name}`")
        else:
            await event.reply(f"⚠️ Già presente nelle {tipo}i.")
    except Exception as e:
        await event.reply(f"❌ Impossibile aggiungere:\n`{e}`")


# ── HTTP server ───────────────────────────────────────────────────────────────

async def start_http_server() -> None:
    async def handle_slave_config(request):
        if not os.path.exists(SLAVE_CONFIG_FILE):
            return web.Response(
                status=503,
                text=json.dumps({"error": "Config non ancora generata"}),
                content_type="application/json",
            )
        with open(SLAVE_CONFIG_FILE, "r", encoding="utf-8") as f:
            content = f.read()
        return web.Response(text=content, content_type="application/json")

    async def handle_health(request):
        return web.Response(text=json.dumps({"status": "ok"}), content_type="application/json")

    port = int(os.environ.get("PORT", 8080))
    app  = web.Application()
    app.router.add_get("/api/slave-config", handle_slave_config)
    app.router.add_get("/healthz", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info(f"🌐 HTTP server avviato sulla porta {port}")
    while True:
        await asyncio.sleep(3600)


# ── Testi Telegram ────────────────────────────────────────────────────────────

HELP_TEXT = """📋 **COMANDI MASTER**

▶️ **Controllo**
`/on` — avvia l'invio (parte subito)
`/off` — ferma l'invio
`/s` — mostra stato attuale
`/reset` — azzera sorgenti e destinazioni

📥 **Sorgenti e destinazioni**
`/a https://t.me/...` — aggiungi sorgente
`/d https://t.me/...` — aggiungi destinazione
`/lf` — lista cartelle Telegram
`/sf NomeCartella` — aggiungi cartella come sorgenti
`/tf NomeCartella` — aggiungi cartella come destinazioni

👥 **Sorgenti per slave**
`/sa 1 https://t.me/canale` — sorgente dedicata allo slave 1
`/ssl 1` — mostra sorgenti dello slave 1
`/sra 1` — resetta sorgenti slave 1 (torna al master)

⏰ **Intervalli**
`/i 10` — intervallo master (minuti)
`/si 1 5` — intervallo slave 1 a 5 minuti
`/sil` — lista intervalli slave
`/sir` — resetta intervalli slave al default

📁 **Regole per cartella** _(sorgente e intervallo dedicati)_
`/fr NomeCartella @fonte 5` — imposta regola
`/frl` — lista regole cartelle
`/frd NomeCartella` — elimina regola cartella

🔘 **Bottoni inline**
`/b` — mostra bottoni + istruzioni
`/bclear` — rimuovi tutti i bottoni

💬 **Auto-risposta PM (slave)**
`/replytext Ciao {first_name}!` — imposta testo
`/replytext` — mostra testo attuale
`/replyclear` — cancella il testo

🔍 **Diagnostica**
`/debug` — mostra slave\\_config.json
`/refresh` — rigenera slave\\_config.json
`/h` — mostra questo menu"""


def _stato_text(cfg: dict) -> str:
    stato    = "🟢 Attivo" if cfg["running"] else "🔴 Fermo"
    n_src    = len(cfg.get("sources", []))
    n_tgt    = len(cfg.get("targets", []))
    n_btn    = sum(len(r) for r in cfg.get("buttons_rows", []))
    reply_on = "✅ attiva" if cfg.get("auto_reply_text") else "❌ non impostata"
    interval = cfg.get("interval", 10)
    n_rules  = len(cfg.get("folder_rules", {}))

    si = cfg.get("slave_intervals", {})
    si_lines = ("\n" + "\n".join(
        f"  • Slave {k}: {v} min"
        for k, v in sorted(si.items(), key=lambda x: int(x[0]))
    )) if si else " default"

    ss = cfg.get("slave_sources", {})
    ss_lines = ("\n" + "\n".join(
        f"  • Slave {k}: {len(v)} sorgenti proprie"
        for k, v in sorted(ss.items(), key=lambda x: int(x[0]))
    )) if ss else " usano master"

    return (
        f"📊 **STATO** — {stato}\n\n"
        f"⏰ Intervallo master: **{interval} min**\n"
        f"🕐 Intervalli slave:{si_lines}\n\n"
        f"📥 Sorgenti master: **{n_src}**\n"
        f"📥 Sorgenti slave:{ss_lines}\n"
        f"📤 Destinazioni: **{n_tgt}**\n\n"
        f"📁 Regole cartella: **{n_rules}**\n"
        f"🔘 Bottoni: **{n_btn}**\n"
        f"💬 Auto-risposta: {reply_on}"
    )


# ── Handler comandi ───────────────────────────────────────────────────────────

async def handle_command(client: TelegramClient, event, text: str) -> None:
    global config

    # ── Controllo ──────────────────────────────────────────────────────────────

    if text in ["/on", "/start"]:
        config["running"] = True
        save_config(config)
        await update_slave_config(client, config)
        trigger_now.set()
        await event.reply("🚀 **Avviato!**\nControllo immediato in corso...")

    elif text in ["/off", "/stop"]:
        config["running"] = False
        save_config(config)
        await update_slave_config(client, config)
        await event.reply("⛔ **Fermato.**")

    elif text == "/s":
        await event.reply(_stato_text(config))

    elif text == "/reset":
        config["sources"]          = []
        config["targets"]          = []
        config["last_ids"]         = {}
        config["rotation_indices"] = {}
        save_config(config)
        await update_slave_config(client, config)
        await event.reply("🔄 **Reset completato.**\nSorgenti, destinazioni e cronologia azzerati.")

    # ── Regole cartella ────────────────────────────────────────────────────────

    elif text.startswith("/fr "):
        # /fr NomeCartella @fonte intervallo
        try:
            parts = text.split(maxsplit=3)
            if len(parts) < 4:
                raise ValueError("Argomenti insufficienti")

            folder_name = parts[1]
            source_link = parts[2]
            interval    = max(1, int(parts[3]))

            # Risolve la sorgente
            source_entity  = await client.get_entity(source_link)
            source_peer_id = get_peer_id(source_entity)
            source_name    = (
                getattr(source_entity, "title", None)
                or getattr(source_entity, "username", None)
                or str(source_peer_id)
            )

            # Risolve i peer della cartella
            folders = await get_folders(client)
            matched = next(
                (f for f in folders if _folder_title(f).lower() == folder_name.lower()), None
            )
            if not matched:
                available = "\n".join(f"  • {_folder_title(f)}" for f in folders)
                await event.reply(
                    f"❌ Cartella **{folder_name}** non trovata.\n\n"
                    f"📁 **Disponibili:**\n{available}"
                )
                return

            peers_data = await resolve_folder_peers(client, matched)
            peer_ids   = [p[0] for p in peers_data]
            peer_names = [p[1] for p in peers_data]

            if not peer_ids:
                await event.reply("⚠️ Cartella vuota o non risolvibile.")
                return

            config.setdefault("folder_rules", {})[folder_name] = {
                "source":   source_peer_id,
                "interval": interval,
                "peers":    peer_ids,
            }
            save_config(config)
            _start_folder_task(client, folder_name)

            preview = "\n".join(f"  • {n}" for n in peer_names[:10])
            extra   = f"\n  _(e altri {len(peer_names) - 10})_" if len(peer_names) > 10 else ""
            await event.reply(
                f"✅ **Regola cartella impostata!**\n\n"
                f"📁 Cartella: **{_folder_title(matched)}**\n"
                f"📥 Sorgente: `{source_name}`\n"
                f"⏱ Intervallo: **{interval} min**\n"
                f"📤 Gruppi: **{len(peer_ids)}**\n{preview}{extra}\n\n"
                f"ℹ️ Questi gruppi ricevono solo messaggi da questa regola,\n"
                f"non dal loop principale."
            )

        except (IndexError, ValueError):
            await event.reply(
                "ℹ️ Uso: `/fr NomeCartella @fonte 5`\n\n"
                "Esempio: `/fr Hot @miospam 3`\n"
                "→ invia da @miospam ogni 3 min a tutti i gruppi della cartella Hot"
            )
        except Exception as e:
            await event.reply(f"❌ Errore: `{e}`")

    elif text == "/frl":
        rules = config.get("folder_rules", {})
        if not rules:
            await event.reply(
                "📁 Nessuna regola cartella configurata.\n\n"
                "Usa `/fr NomeCartella @fonte 5` per crearne una."
            )
        else:
            lines = []
            for name, rule in rules.items():
                src = rule.get("source", "?")
                ivl = rule.get("interval", "?")
                n   = len(rule.get("peers", []))
                lines.append(f"  • **{name}** — `{src}` ogni **{ivl} min** ({n} gruppi)")
            await event.reply(
                f"📁 **Regole cartella ({len(rules)}):**\n\n"
                + "\n".join(lines)
                + "\n\n`/frd NomeCartella` per eliminare"
            )

    elif text.startswith("/frd "):
        folder_name = text.split(maxsplit=1)[1].strip()
        rules = config.get("folder_rules", {})
        if folder_name in rules:
            del rules[folder_name]
            config["folder_rules"] = rules
            save_config(config)
            _stop_folder_task(folder_name)
            await event.reply(
                f"🗑 **Regola eliminata** per `{folder_name}`.\n"
                "I suoi gruppi tornano al loop principale."
            )
        else:
            await event.reply(
                f"⚠️ Nessuna regola trovata per `{folder_name}`.\n"
                "Usa `/frl` per vedere le regole attive."
            )

    # ── Bottoni ────────────────────────────────────────────────────────────────

    elif text == "/b":
        rows    = config.get("buttons_rows", [])
        preview = format_buttons_preview(rows)
        await event.reply(
            f"🔘 **Bottoni attuali:**\n{preview}\n\n"
            "**Come impostare i bottoni:**\n"
            "```\n/b\n"
            "🔥 Canale - https://t.me/tuocanale\n"
            "#g Contatto - https://t.me/user && #r Limitati - https://t.me/gruppo\n"
            "Condividi - share:Dai un'occhiata!\n```\n\n"
            "• `&&` → stessa riga\n"
            "• `#g` 🟢  `#r` 🔴  `#p` 🔵\n"
            "• `/bclear` → rimuovi tutti"
        )

    elif text.startswith("/b\n") or (text.startswith("/b ") and len(text) > 3):
        definition = text[2:].strip()
        if not definition:
            await event.reply("⚠️ Definizione vuota. Scrivi `/b` per le istruzioni.")
            return
        try:
            rows = parse_buttons(definition)
            if not rows:
                await event.reply("❌ Nessun bottone valido trovato.\n\nFormato: `testo - https://url`")
                return
            config["buttons_rows"] = rows
            save_config(config)
            await update_slave_config(client, config)
            total = sum(len(r) for r in rows)
            await event.reply(
                f"✅ **{total} bottoni impostati** su {len(rows)} righe:\n\n"
                + format_buttons_preview(rows)
            )
        except Exception as e:
            await event.reply(f"❌ Errore nel parsing:\n`{e}`")

    elif text == "/bclear":
        config["buttons_rows"] = []
        save_config(config)
        await update_slave_config(client, config)
        await event.reply("🗑 **Tutti i bottoni rimossi.**")

    # ── Auto-risposta PM ───────────────────────────────────────────────────────

    elif text.startswith("/replytext\n") or (text.startswith("/replytext ") and len(text) > 11):
        reply_text = text[len("/replytext"):].strip()
        config["auto_reply_text"] = reply_text
        save_config(config)
        await update_slave_config(client, config)
        await event.reply(
            f"✅ **Testo auto-risposta impostato:**\n\n{reply_text}\n\n"
            "Segnaposto: `{first_name}` `{last_name}` `{full_name}` `{username}`"
        )

    elif text == "/replytext":
        current = config.get("auto_reply_text", "")
        if current:
            await event.reply(f"📝 **Testo auto-risposta attuale:**\n\n{current}")
        else:
            await event.reply(
                "❌ Nessun testo impostato.\n\n"
                "Usa: `/replytext Ciao {first_name}!`\n"
                "Segnaposto: `{first_name}` `{last_name}` `{full_name}` `{username}`"
            )

    elif text == "/replyshow":
        reply_text = config.get("auto_reply_text", "")
        await event.reply(
            "💬 **Auto-risposta PM slave**\n\n"
            f"📝 Testo: {reply_text or '_(non impostato)_'}\n\n"
            "Per modificare:\n"
            "• `/replytext <testo>` — imposta\n"
            "• `/replyclear` — cancella"
        )

    elif text == "/replyclear":
        config["auto_reply_text"] = ""
        save_config(config)
        await update_slave_config(client, config)
        await event.reply("🗑 **Testo auto-risposta rimosso.**")

    # ── Cartelle ───────────────────────────────────────────────────────────────

    elif text == "/lf":
        try:
            folders = await get_folders(client)
            if not folders:
                await event.reply("📁 Nessuna cartella trovata.")
                return
            lines = "\n".join(f"  • **{_folder_title(f)}** ({len(f.include_peers)} chat)" for f in folders)
            await event.reply(
                f"📁 **Cartelle Telegram:**\n\n{lines}\n\n"
                "`/sf NomeCartella` → aggiungi come sorgenti\n"
                "`/tf NomeCartella` → aggiungi come destinazioni\n"
                "`/fr NomeCartella @fonte 5` → regola dedicata"
            )
        except Exception as e:
            await event.reply(f"❌ Errore: `{e}`")

    elif text.startswith("/sf"):
        name = text.split(maxsplit=1)[1].strip() if len(text.split()) > 1 else ""
        if name:
            await add_folder_to_list(client, event, name, True)
        else:
            await event.reply("ℹ️ Uso: `/sf NomeCartella`")

    elif text.startswith("/tf"):
        name = text.split(maxsplit=1)[1].strip() if len(text.split()) > 1 else ""
        if name:
            await add_folder_to_list(client, event, name, False)
        else:
            await event.reply("ℹ️ Uso: `/tf NomeCartella`")

    # ── Sorgenti per slave ─────────────────────────────────────────────────────

    elif text.startswith("/sa "):
        try:
            parts   = text.split(maxsplit=2)
            slave_n = str(int(parts[1]))
            link    = parts[2].strip()
            entity  = await client.get_entity(link)
            peer_id = get_peer_id(entity)
            name    = getattr(entity, "title", None) or getattr(entity, "username", None) or str(peer_id)
            config.setdefault("slave_sources", {}).setdefault(slave_n, [])
            if peer_id not in config["slave_sources"][slave_n]:
                config["slave_sources"][slave_n].append(peer_id)
                save_config(config)
                await update_slave_config(client, config)
                await event.reply(f"✅ Sorgente slave **{slave_n}** aggiunta: `{name}`")
            else:
                await event.reply("⚠️ Già presente!")
        except Exception as e:
            await event.reply(f"❌ Errore: `{e}`\n\nUso: `/sa 1 https://t.me/canale`")

    elif text.startswith("/ssl "):
        try:
            slave_n = str(int(text.split()[1]))
            ids     = config.get("slave_sources", {}).get(slave_n, [])
            if not ids:
                await event.reply(
                    f"📭 Slave **{slave_n}** non ha sorgenti proprie.\n"
                    f"Usa le sorgenti del master ({len(config['sources'])})."
                )
            else:
                names = []
                for pid in ids:
                    try:
                        ent  = await client.get_entity(pid)
                        name = (f"@{ent.username}" if getattr(ent, "username", None)
                                else getattr(ent, "title", str(pid)))
                        names.append(name)
                    except Exception:
                        names.append(str(pid))
                lines = "\n".join(f"  • {n}" for n in names)
                await event.reply(
                    f"📥 **Sorgenti slave {slave_n}:**\n{lines}\n\n"
                    f"`/sra {slave_n}` per resettare"
                )
        except Exception:
            await event.reply("ℹ️ Uso: `/ssl 1`")

    elif text.startswith("/sra "):
        try:
            slave_n = str(int(text.split()[1]))
            config.setdefault("slave_sources", {}).pop(slave_n, None)
            save_config(config)
            await update_slave_config(client, config)
            await event.reply(f"🔄 Slave **{slave_n}** ora usa le sorgenti del master.")
        except Exception:
            await event.reply("ℹ️ Uso: `/sra 1`")

    # ── Entità singola ─────────────────────────────────────────────────────────

    elif text.startswith("/a "):
        await add_entity(client, event, text.split(maxsplit=1)[1].strip(), True)

    elif text.startswith("/d "):
        await add_entity(client, event, text.split(maxsplit=1)[1].strip(), False)

    elif any(x in text.lower() for x in ["t.me/", "telegram.me"]):
        globals()["pending_link"] = text
        await event.reply("🔗 **Link rilevato!**\nRispondi: `sorgente` o `destinazione`")

    elif text.lower() in ["sorgente", "destinazione"]:
        if globals().get("pending_link"):
            await add_entity(client, event, globals()["pending_link"], text.lower() == "sorgente")
            globals()["pending_link"] = None
        else:
            await event.reply("⚠️ Nessun link in attesa.")

    # ── Intervalli ─────────────────────────────────────────────────────────────

    elif text.startswith("/i "):
        try:
            mins = max(1, int(text.split()[1]))
            config["interval"] = mins
            save_config(config)
            await update_slave_config(client, config)
            await event.reply(f"⏰ Intervallo master impostato a **{mins} minuti**.")
        except Exception:
            await event.reply("ℹ️ Uso: `/i 10`")

    elif text.startswith("/si "):
        try:
            parts   = text.split()
            slave_n = str(int(parts[1]))
            mins    = max(1, int(parts[2]))
            config.setdefault("slave_intervals", {})[slave_n] = mins
            save_config(config)
            await update_slave_config(client, config)
            await event.reply(f"🕐 Slave **{slave_n}** → intervallo impostato a **{mins} minuti**.")
        except Exception:
            await event.reply("ℹ️ Uso: `/si 1 5`  (numero slave — minuti)")

    elif text == "/sil":
        si = config.get("slave_intervals", {})
        if not si:
            await event.reply(
                f"🕐 Nessun intervallo slave personalizzato.\n"
                f"Tutti usano il default master: **{config['interval']} min**"
            )
        else:
            lines = "\n".join(
                f"  • Slave {k}: {v} min"
                for k, v in sorted(si.items(), key=lambda x: int(x[0]))
            )
            await event.reply(
                f"🕐 **Intervalli slave:**\n{lines}\n\n"
                f"_(default master: {config['interval']} min)_"
            )

    elif text == "/sir":
        config["slave_intervals"] = {}
        save_config(config)
        await update_slave_config(client, config)
        await event.reply(f"🔄 Intervalli resettati — tutti usano il default: **{config['interval']} min**")

    # ── Diagnostica ────────────────────────────────────────────────────────────

    elif text == "/refresh":
        await update_slave_config(client, config)
        if os.path.exists(SLAVE_CONFIG_FILE):
            with open(SLAVE_CONFIG_FILE, "r", encoding="utf-8") as f:
                sc = json.load(f)
            await event.reply(
                "🔄 **slave\\_config.json rigenerato**\n\n"
                f"📥 Sorgenti master: `{sc.get('sources', [])}`\n"
                f"📤 Destinazioni: **{len(sc.get('targets', []))}** gruppi\n"
                f"🔀 Sorgenti slave: `{list(sc.get('slave_sources', {}).keys())}`\n"
                f"⏱ Intervallo: **{sc.get('interval')} min**\n"
                f"▶️ Running: **{sc.get('running')}**"
            )
        else:
            await event.reply("⚠️ slave\\_config.json non trovato dopo il refresh.")

    elif text == "/debug":
        if not os.path.exists(SLAVE_CONFIG_FILE):
            await update_slave_config(client, config)
        if os.path.exists(SLAVE_CONFIG_FILE):
            with open(SLAVE_CONFIG_FILE, "r", encoding="utf-8") as f:
                content = f.read()
            await event.reply(f"🔍 **slave\\_config.json:**\n\n```\n{content[:3000]}\n```")
        else:
            await event.reply("❌ slave\\_config.json non trovato.\nManda `/on` o `/refresh` per generarlo.")

    elif text in ["/h", "/help"]:
        await event.reply(HELP_TEXT)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    global config

    api_id_str     = os.environ.get("API_ID")
    api_hash       = os.environ.get("API_HASH")
    session_string = os.environ.get("SESSION_STRING", "")

    if not api_id_str or not api_hash:
        log.error("❌ Imposta API_ID e API_HASH come variabili d'ambiente!")
        return

    client = TelegramClient(StringSession(session_string), int(api_id_str), api_hash)
    await client.start()

    if not session_string:
        print("\n" + "=" * 60)
        print("✅ Salva questa SESSION_STRING nelle variabili d'ambiente:")
        print(client.session.save())
        print("=" * 60 + "\n")

    config = load_config()
    log.info(
        f"🚀 Master avviato | "
        f"{len(config['sources'])} sorgenti | "
        f"{len(config['targets'])} destinazioni | "
        f"{len(config.get('folder_rules', {}))} regole cartella"
    )

    if config["running"]:
        trigger_now.set()

    # Avvia i task per le regole cartella già salvate
    for folder_name in config.get("folder_rules", {}):
        _start_folder_task(client, folder_name)
        log.info(f"📁 Task cartella '{folder_name}' avviato da config salvata")

    globals()["pending_link"] = None

    @client.on(events.NewMessage(chats="me", outgoing=True))
    async def command_handler(event):
        text = (event.message.text or "").strip()
        if text:
            await handle_command(client, event, text)

    spam_task = asyncio.create_task(spam_loop(client, config))
    http_task = asyncio.create_task(start_http_server())

    log.info("🎉 Master pronto! Invia comandi in 'Messaggi Salvati'")
    await client.run_until_disconnected()

    spam_task.cancel()
    http_task.cancel()
    for task in _folder_tasks.values():
        task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
