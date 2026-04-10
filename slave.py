"""
Telegram Slave Userbot — Multi-Account
=======================================
Gestisce N account Telegram in parallelo, controllati dal master.

── VARIABILI D'AMBIENTE ──────────────────────────────────────────
  MASTER_URL       — URL del master (con o senza https://)
                     es: https://tuomaster.up.railway.app

  Per ogni account (N = 1, 2, 3, ...):
    API_ID_N         — API ID dell'account N  (da my.telegram.org)
    API_HASH_N       — API Hash dell'account N
    SESSION_STRING_N — Session string dell'account N

── PRIMO AVVIO ───────────────────────────────────────────────────
  1. Imposta API_ID_1 e API_HASH_1 (SESSION_STRING_1 vuota)
  2. Avvia — nei log apparirà la SESSION_STRING da copiare
  3. Aggiungi SESSION_STRING_1 e procedi con l'account successivo
"""

import asyncio
import json
import logging
import os
import random
import urllib.request
import urllib.error

from telethon import TelegramClient, Button, events
from telethon.errors import FloodWaitError, UserNotParticipantError, ChatAdminRequiredError
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.types import Channel, Chat
from telethon.sessions import StringSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

REPLIED_USERS_MAX = 500


# ── Utilità ───────────────────────────────────────────────────────────────────

def normalize_master_url(url: str) -> str:
    """Aggiunge automaticamente https:// se mancante."""
    url = url.strip().rstrip("/")
    if url and not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
        logging.warning(f"⚠️ MASTER_URL senza schema — aggiunto https:// automaticamente: {url}")
    return url


# ── Stato per account ─────────────────────────────────────────────────────────

class AccountState:
    def __init__(self):
        self.current_targets: list = []
        self.replied_users: set    = set()

    def maybe_clear_replied_users(self) -> None:
        if len(self.replied_users) >= REPLIED_USERS_MAX:
            self.replied_users.clear()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _fetch_config_sync(master_url: str) -> dict | None:
    url = master_url + "/api/slave-config"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        logging.error(f"❌ Errore HTTP dal master: {e.code} {e.reason}")
    except Exception as e:
        logging.error(f"❌ Impossibile contattare il master ({url}): {e}")
    return None

async def fetch_master_config(master_url: str) -> dict | None:
    return await asyncio.to_thread(_fetch_config_sync, master_url)


# ── Telegram helpers ──────────────────────────────────────────────────────────

def build_buttons(buttons_rows: list) -> list | None:
    if not buttons_rows:
        return None
    return [
        [Button.url(b["text"], b["url"]) for b in row]
        for row in buttons_rows
    ]

def format_reply_text(template: str, user) -> str:
    first = user.first_name or ""
    last  = user.last_name  or ""
    full  = f"{first} {last}".strip()
    uname = f"@{user.username}" if user.username else full
    return (
        template
        .replace("{first_name}", first)
        .replace("{last_name}",  last)
        .replace("{full_name}",  full)
        .replace("{username}",   uname)
    )

async def user_is_in_target(client: TelegramClient, user_id: int, target) -> bool:
    try:
        entity = await client.get_entity(target)
        if not isinstance(entity, (Channel, Chat)):
            return False
        await client(GetParticipantRequest(channel=entity, participant=user_id))
        return True
    except (UserNotParticipantError, ChatAdminRequiredError):
        return False
    except Exception:
        return False


# ── Invio messaggi ────────────────────────────────────────────────────────────

async def copy_to_target(
    client: TelegramClient,
    log: logging.Logger,
    msg,
    target,
    buttons_rows: list,
    _retries: int = 0,
) -> None:
    try:
        text     = msg.message or getattr(msg, "caption", "") or ""
        entities = msg.entities or []
        buttons  = build_buttons(buttons_rows)

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
            log.error(f"❌ FloodWait ripetuto ({_retries}x) su {target} — messaggio saltato.")
            return
        log.warning(f"⏳ FloodWait {e.seconds}s (tentativo {_retries + 1}/3)")
        await asyncio.sleep(e.seconds + 1)
        await copy_to_target(client, log, msg, target, buttons_rows, _retries + 1)
    except Exception as e:
        log.error(f"❌ Errore → {target}: {e}")


# ── Auto-risposta PM ──────────────────────────────────────────────────────────

async def handle_private_message(
    event,
    client: TelegramClient,
    log: logging.Logger,
    master_url: str,
    state: AccountState,
) -> None:
    sender = await event.get_sender()
    if sender is None or getattr(sender, "bot", False) or getattr(sender, "is_self", False):
        return

    user_id = sender.id
    if user_id in state.replied_users:
        return
    if not state.current_targets:
        return

    cfg = await fetch_master_config(master_url)
    if not cfg:
        return

    reply_text = cfg.get("auto_reply_text", "")
    if not reply_text:
        return

    in_target = any(
        await user_is_in_target(client, user_id, t)
        for t in state.current_targets
    )
    if not in_target:
        return

    state.replied_users.add(user_id)
    log.info(f"💬 Auto-risposta a {sender.first_name} (id={user_id})")

    try:
        await client.send_message(sender, format_reply_text(reply_text, sender))
    except FloodWaitError as e:
        log.warning(f"⏳ FloodWait auto-risposta {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        log.error(f"❌ Errore auto-risposta a {user_id}: {e}")


# ── Spam loop ─────────────────────────────────────────────────────────────────

async def spam_loop(
    client: TelegramClient,
    log: logging.Logger,
    master_url: str,
    account_index: int,
    state: AccountState,
) -> None:
    while True:
        cfg = await fetch_master_config(master_url)

        if not cfg:
            log.warning("⚠️ Config master non disponibile — riprovo tra 60s")
            await asyncio.sleep(60)
            continue

        if not cfg.get("running", True):
            log.info("⏸ Master ha fermato l'invio — in pausa")
            await asyncio.sleep(cfg.get("interval", 10) * 60)
            continue

        slave_sources_map = cfg.get("slave_sources", {})
        my_sources        = slave_sources_map.get(str(account_index)) or cfg.get("sources", [])
        targets           = cfg.get("targets", [])
        buttons_rows      = cfg.get("buttons_rows", [])
        default_interval  = max(1, cfg.get("interval", 10))
        interval          = max(1, cfg.get("slave_intervals", {}).get(str(account_index), default_interval))

        src_label = (
            f"proprie ({len(my_sources)})"
            if str(account_index) in slave_sources_map
            else f"master ({len(my_sources)})"
        )
        log.info(f"⏱ Intervallo: {interval} min | Sorgenti: {src_label} | Destinazioni: {len(targets)}")

        state.current_targets = targets
        state.maybe_clear_replied_users()

        if not my_sources or not targets:
            log.info("📭 Nessuna sorgente o destinazione configurata — attendo...")
            await asyncio.sleep(interval * 60)
            continue

        for source in my_sources:
            try:
                all_msgs = await client.get_messages(source, limit=200)
                valid    = [m for m in all_msgs if m.message or m.media]

                if not valid:
                    log.info(f"📭 Nessun messaggio valido in {source}")
                    continue

                msg = random.choice(valid)
                log.info(f"📤 Post random (id={msg.id}) da {source} → {len(targets)} destinazioni")

                await asyncio.gather(*[
                    copy_to_target(client, log, msg, t, buttons_rows)
                    for t in targets
                ])

            except Exception as e:
                log.error(f"❌ Errore sorgente {source}: {e}")

        log.info(f"✅ Ciclo completato — prossimo invio tra {interval} min")
        await asyncio.sleep(interval * 60)


# ── Avvio account ─────────────────────────────────────────────────────────────

async def run_account(
    account_index: int,
    api_id: int,
    api_hash: str,
    session_string: str,
    master_url: str,
) -> None:
    log   = logging.getLogger(f"account-{account_index}")
    state = AccountState()

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.start()

    if not session_string:
        saved = client.session.save()
        print("\n" + "=" * 60)
        print(f"✅ Account {account_index} — salva questa variabile su Railway:")
        print(f"SESSION_STRING_{account_index}={saved}")
        print("=" * 60 + "\n")

    me = await client.get_me()
    log.info(f"🚀 Connesso come @{me.username or me.first_name} | master: {master_url}")

    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def pm_handler(event):
        await handle_private_message(event, client, log, master_url, state)

    spam_task = asyncio.create_task(spam_loop(client, log, master_url, account_index, state))
    log.info(f"🎉 Account {account_index} pronto — spam loop avviato!")

    try:
        await client.run_until_disconnected()
    finally:
        spam_task.cancel()
        try:
            await spam_task
        except asyncio.CancelledError:
            pass


# ── Caricamento account ───────────────────────────────────────────────────────

def load_accounts(master_url: str) -> list[dict]:
    accounts = []
    i = 1
    while True:
        api_id_str     = os.environ.get(f"API_ID_{i}")
        api_hash       = os.environ.get(f"API_HASH_{i}")
        session_string = os.environ.get(f"SESSION_STRING_{i}", "")

        if not api_id_str or not api_hash:
            break

        accounts.append({
            "index":          i,
            "api_id":         int(api_id_str),
            "api_hash":       api_hash,
            "session_string": session_string,
            "master_url":     master_url,
        })
        i += 1
    return accounts


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    raw_url    = os.environ.get("MASTER_URL", "")
    master_url = normalize_master_url(raw_url)

    if not master_url:
        logging.error("❌ Imposta MASTER_URL come variabile d'ambiente!")
        return

    logging.info(f"🌐 Master URL: {master_url}")

    accounts = load_accounts(master_url)
    if not accounts:
        logging.error(
            "❌ Nessun account trovato! Imposta almeno:\n"
            "  API_ID_1, API_HASH_1, SESSION_STRING_1"
        )
        return

    logging.info(f"📋 {len(accounts)} account caricati — avvio in parallelo...")

    await asyncio.gather(*[
        run_account(
            acc["index"],
            acc["api_id"],
            acc["api_hash"],
            acc["session_string"],
            acc["master_url"],
        )
        for acc in accounts
    ])


if __name__ == "__main__":
    asyncio.run(main())
