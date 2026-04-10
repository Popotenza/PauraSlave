"""
Telegram Slave Userbot — Multi-Account
=======================================
Un unico script che gestisce N account Telegram in parallelo,
tutti controllati dal master.

── CONFIGURAZIONE VARIABILI D'AMBIENTE ────────────────────────────────────────

  MASTER_URL       — URL del master, es: https://tuomaster.up.railway.app

  Per ogni account (sostituisci N con 1, 2, 3, ...):
    API_ID_N         — API ID dell'account N  (da my.telegram.org)
    API_HASH_N       — API Hash dell'account N
    SESSION_STRING_N — Session string dell'account N (lascia vuota al primo avvio)

  Esempio per 3 account:
    API_ID_1, API_HASH_1, SESSION_STRING_1
    API_ID_2, API_HASH_2, SESSION_STRING_2
    API_ID_3, API_HASH_3, SESSION_STRING_3

── PRIMO AVVIO (generazione SESSION_STRING) ───────────────────────────────────
  1. Imposta API_ID_1 e API_HASH_1 (SESSION_STRING_1 lasciala vuota)
  2. Esegui il bot — nei log apparirà la SESSION_STRING da copiare
  3. Aggiungi SESSION_STRING_1 nelle variabili, poi aggiungi il secondo account, ecc.

NOTA: Lo slave parte automaticamente e posta dal suo account.
      Nessun comando locale — tutto è controllato dal master.
      Il testo auto-risposta PM si imposta dal master con /replytext.
      Le addlist (link t.me/addlist/...) si impostano dal master con /ala e /alg.
      All'avvio lo slave entra in tutti i gruppi delle addlist e li silenzia per sempre.
      Sorgenti e destinazioni vengono silenziate automaticamente.
"""

import asyncio
import json
import logging
import os
import random
import urllib.request
import urllib.error

from telethon import TelegramClient, Button, events
from telethon.errors import (
    FloodWaitError,
    UserNotParticipantError,
    ChatAdminRequiredError,
)
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.chatlists import (
    CheckChatlistInviteRequest,
    JoinChatlistInviteRequest,
)
from telethon.tl.types import (
    Channel, Chat,
    InputPeerNotifySettings,
    InputNotifyPeer,
    ChatsChats,
    ChatsChatsSlice,
)
from telethon.sessions import StringSession
from telethon.utils import get_peer_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


# ── Stato per account ─────────────────────────────────────────────────────────

REPLIED_USERS_MAX = 500

class AccountState:
    def __init__(self):
        self.current_targets: list = []
        self.replied_users: set = set()
        self.silenced_peers: set = set()    # peer già silenziati (peer_id come str)
        self.joined_addlists: set = set()   # slug addlist già processati

    def maybe_clear_replied_users(self):
        if len(self.replied_users) >= REPLIED_USERS_MAX:
            self.replied_users.clear()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _fetch_master_config_sync(master_url: str) -> dict | None:
    url = master_url.rstrip("/") + "/api/slave-config"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        logging.error(f"Errore HTTP master: {e.code} {e.reason}")
    except Exception as e:
        logging.error(f"Impossibile contattare il master: {e}")
    return None

async def fetch_master_config(master_url: str) -> dict | None:
    return await asyncio.to_thread(_fetch_master_config_sync, master_url)


# ── Telegram helpers ──────────────────────────────────────────────────────────

def build_buttons(buttons_rows: list) -> list | None:
    if not buttons_rows:
        return None
    return [
        [Button.url(btn["text"], btn["url"]) for btn in row]
        for row in buttons_rows
    ]


def format_reply_text(template: str, user) -> str:
    first = user.first_name or ""
    last  = user.last_name or ""
    full  = f"{first} {last}".strip()
    uname = f"@{user.username}" if user.username else full
    return (
        template
        .replace("{first_name}", first)
        .replace("{last_name}", last)
        .replace("{full_name}", full)
        .replace("{username}", uname)
    )


async def user_is_in_target(client, user_id: int, target) -> bool:
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


# ── Silenziamento chat ────────────────────────────────────────────────────────

async def silence_entity(client, log, entity, label: str = ""):
    """Silenzia una singola entità Telegram per sempre."""
    try:
        await client(UpdateNotifySettingsRequest(
            peer=InputNotifyPeer(entity),
            settings=InputPeerNotifySettings(
                show_previews=False,
                silent=True,
                mute_until=2147483647,  # unix timestamp massimo = per sempre
            )
        ))
        log.info(f"🔕 Silenziato: {label}")
    except Exception as e:
        log.warning(f"Impossibile silenziare {label}: {e}")


async def silence_peer_by_id(client, log, peer, state: AccountState):
    """Silenzia un peer (id numerico, @username o link) se non è già silenziato."""
    key = str(peer)
    if key in state.silenced_peers:
        return
    try:
        entity = await client.get_entity(peer)
        name = getattr(entity, "title", None) or getattr(entity, "username", None) or str(peer)
        await silence_entity(client, log, entity, name)
        state.silenced_peers.add(key)
    except Exception as e:
        log.warning(f"Errore silenziamento {peer}: {e}")


async def silence_all_peers(client, log, sources: list, targets: list, state: AccountState):
    """Silenzia tutte le sorgenti e destinazioni non ancora silenziate."""
    all_peers = list({str(p): p for p in sources + targets}.values())
    for peer in all_peers:
        await silence_peer_by_id(client, log, peer, state)


# ── Addlist (cartelle condivise t.me/addlist/...) ────────────────────────────

def _extract_addlist_slug(link: str) -> str | None:
    """Estrae lo slug dal link t.me/addlist/SLUG."""
    link = link.strip().rstrip("/")
    for prefix in (
        "https://t.me/addlist/",
        "http://t.me/addlist/",
        "t.me/addlist/",
    ):
        if link.lower().startswith(prefix):
            return link[len(prefix):]
    return None


async def process_addlist_link(client, log, link: str, state: AccountState):
    """
    Elabora un link t.me/addlist/SLUG:
    1. Controlla i peer nella cartella condivisa
    2. Entra in tutti i gruppi/canali della cartella
    3. Silenzia per sempre ogni peer aggiunto
    """
    slug = _extract_addlist_slug(link)
    if not slug:
        log.warning(f"🔗 Addlist: link non riconosciuto come t.me/addlist/...: {link}")
        return

    if slug in state.joined_addlists:
        return  # già processato in questa sessione

    state.joined_addlists.add(slug)
    log.info(f"🔗 Addlist: elaborazione slug={slug}")

    try:
        # Recupera le info della chatlist (lista dei peer nella cartella)
        invite_info = await client(CheckChatlistInviteRequest(slug=slug))
    except Exception as e:
        log.error(f"❌ Addlist: impossibile verificare {link}: {e}")
        return

    # I peer disponibili per l'accesso sono in already_peers + missing_peers
    already_peers = getattr(invite_info, "already_peers", []) or []
    missing_peers = getattr(invite_info, "missing_peers", []) or []
    all_peers = already_peers + missing_peers

    if not all_peers:
        log.info(f"ℹ️ Addlist: cartella vuota o nessun peer accessibile — {link}")
        return

    log.info(f"🔗 Addlist: trovati {len(all_peers)} peer ({len(already_peers)} già membro, {len(missing_peers)} da unire) — {link}")

    # Entra in tutti i peer della cartella
    if missing_peers:
        try:
            await client(JoinChatlistInviteRequest(slug=slug, peers=missing_peers))
            log.info(f"✅ Addlist: entrato in {len(missing_peers)} gruppi/canali — {link}")
        except FloodWaitError as e:
            log.warning(f"FloodWait addlist join {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
            try:
                await client(JoinChatlistInviteRequest(slug=slug, peers=missing_peers))
                log.info(f"✅ Addlist (retry): entrato in {len(missing_peers)} gruppi/canali — {link}")
            except Exception as e2:
                log.error(f"❌ Addlist retry join fallito {link}: {e2}")
        except Exception as e:
            log.error(f"❌ Addlist join fallito {link}: {e}")

    # Silenzia tutti i peer della cartella (sia quelli già presenti che quelli appena aggiunti)
    log.info(f"🔕 Addlist: silenziamento di {len(all_peers)} peer...")
    for peer in all_peers:
        key = str(get_peer_id(peer)) if hasattr(peer, "SUBCLASS_OF_ID") else str(peer)
        if key in state.silenced_peers:
            continue
        try:
            entity = await client.get_entity(peer)
            name = getattr(entity, "title", None) or getattr(entity, "username", None) or str(peer)
            await silence_entity(client, log, entity, name)
            state.silenced_peers.add(key)
            await asyncio.sleep(0.3)  # piccola pausa per non sovraccaricare
        except Exception as e:
            log.warning(f"Impossibile silenziare peer della addlist: {e}")


async def process_all_addlists(client, log, cfg: dict, account_index: int, state: AccountState):
    """Elabora tutte le addlist assegnate a questo slave (globali + specifiche)."""
    global_addlists = cfg.get("global_addlists", [])
    slave_addlists  = cfg.get("slave_addlists", {}).get(str(account_index), [])

    # Deduplicazione mantenendo l'ordine
    seen = set()
    all_links = []
    for link in global_addlists + slave_addlists:
        if link not in seen:
            seen.add(link)
            all_links.append(link)

    if not all_links:
        return

    for link in all_links:
        await process_addlist_link(client, log, link, state)
        await asyncio.sleep(2)  # pausa tra una addlist e l'altra


# ── Invio messaggi ────────────────────────────────────────────────────────────

async def copy_to_target(client, log, msg, target, buttons_rows, _retries=0):
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
                log.warning(f"Media fallito su {target} ({media_err}) — invio solo testo")
                if text:
                    await client.send_message(
                        target, text,
                        formatting_entities=entities, buttons=buttons
                    )
        else:
            await client.send_message(
                target, text,
                formatting_entities=entities, buttons=buttons
            )
        log.info(f"✅ msg {msg.id} → {target}")

    except FloodWaitError as e:
        if _retries >= 3:
            log.error(f"FloodWait ripetuto ({_retries}x) su {target}, messaggio saltato.")
            return
        log.warning(f"FloodWait {e.seconds}s (tentativo {_retries + 1}/3)")
        await asyncio.sleep(e.seconds + 1)
        await copy_to_target(client, log, msg, target, buttons_rows, _retries + 1)
    except Exception as e:
        log.error(f"Errore → {target}: {e}")


# ── Auto-risposta PM ──────────────────────────────────────────────────────────

async def handle_private_message(event, client, log, master_url: str, state: AccountState):
    sender = await event.get_sender()
    if sender is None or sender.bot or sender.is_self:
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

    in_target = False
    for target in state.current_targets:
        if await user_is_in_target(client, user_id, target):
            in_target = True
            break

    if not in_target:
        return

    state.replied_users.add(user_id)
    log.info(f"💬 Auto-risposta PM a {sender.first_name} (id={user_id})")

    try:
        text = format_reply_text(reply_text, sender)
        await client.send_message(sender, text)
    except FloodWaitError as e:
        log.warning(f"FloodWait auto-risposta {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        log.error(f"Errore auto-risposta a {user_id}: {e}")


# ── Spam loop ─────────────────────────────────────────────────────────────────

async def spam_loop(client, log, master_url: str, account_index: int, state: AccountState):
    while True:
        cfg = await fetch_master_config(master_url)
        if not cfg:
            log.warning("Config master non disponibile, riprovo tra 60s")
            await asyncio.sleep(60)
            continue

        if not cfg.get("running", True):
            log.info("Master ha fermato l'invio — in pausa")
            await asyncio.sleep(cfg.get("interval", 10) * 60)
            continue

        slave_sources_map = cfg.get("slave_sources", {})
        slave_key = str(account_index)
        slave_specific = slave_sources_map.get(slave_key)

        if slave_specific:
            my_sources = slave_specific
            src_tipo = f"PROPRIE slave {slave_key}"
        else:
            my_sources = cfg.get("sources", [])
            src_tipo = "MASTER (nessuna sorgente propria impostata)"

        targets      = cfg.get("targets", [])
        buttons_rows = cfg.get("buttons_rows", [])
        default_interval = max(1, cfg.get("interval", 10))
        slave_intervals  = cfg.get("slave_intervals", {})
        interval = max(1, slave_intervals.get(slave_key, default_interval))

        log.info(f"⏱ Intervallo: {interval} min | Sorgenti {src_tipo}: {my_sources}")

        state.current_targets = targets
        state.maybe_clear_replied_users()

        # Elabora le addlist (entra nei gruppi e silenzia tutto)
        await process_all_addlists(client, log, cfg, account_index, state)

        # Silenzia sorgenti e destinazioni
        await silence_all_peers(client, log, my_sources, targets, state)

        if not my_sources or not targets:
            log.info("Nessuna sorgente o destinazione configurata — attendo...")
            await asyncio.sleep(interval * 60)
            continue

        for source in my_sources:
            try:
                all_msgs = await client.get_messages(source, limit=200)
                valid = [m for m in all_msgs if m.message or m.media]
                if not valid:
                    log.warning(f"Nessun messaggio valido in {source}")
                    continue

                msg = random.choice(valid)
                log.info(f"📤 Post random (id={msg.id}) da {source}")

                await asyncio.gather(*[
                    copy_to_target(client, log, msg, t, buttons_rows)
                    for t in targets
                ])

            except Exception as e:
                log.error(f"❌ Errore sorgente {source}: {e} — verifica che l'account sia membro del canale")

        await asyncio.sleep(interval * 60)


# ── Avvio account ─────────────────────────────────────────────────────────────

async def run_account(account_index: int, api_id: int, api_hash: str,
                      session_string: str, master_url: str):
    log   = logging.getLogger(f"account-{account_index}")
    state = AccountState()

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.start()

    if not session_string:
        print("\n" + "=" * 60)
        print(f"✅ Account {account_index} — salva questa SESSION_STRING_{account_index}:")
        print(client.session.save())
        print("=" * 60 + "\n")

    log.info(f"🚀 Connesso | master: {master_url} | avvio automatico")

    # All'avvio: elabora subito addlist e silenzia sorgenti/destinazioni
    initial_cfg = await fetch_master_config(master_url)
    if initial_cfg:
        slave_key = str(account_index)
        slave_specific = initial_cfg.get("slave_sources", {}).get(slave_key)
        init_sources = slave_specific if slave_specific else initial_cfg.get("sources", [])
        init_targets = initial_cfg.get("targets", [])

        log.info("🔗 Elaborazione addlist iniziali...")
        await process_all_addlists(client, log, initial_cfg, account_index, state)

        log.info("🔕 Silenziamento iniziale sorgenti e destinazioni...")
        await silence_all_peers(client, log, init_sources, init_targets, state)

    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def pm_handler(event):
        await handle_private_message(event, client, log, master_url, state)

    spam_task = asyncio.create_task(spam_loop(client, log, master_url, account_index, state))
    log.info(f"🎉 Account {account_index} pronto — in esecuzione!")

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

async def main():
    master_url = os.environ.get("MASTER_URL", "").rstrip("/")

    if not master_url:
        logging.error("❌ Imposta MASTER_URL come variabile d'ambiente!")
        return

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
