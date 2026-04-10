"""
╔══════════════════════════════════════════════════════════════════╗
║          TELEGRAM SLAVE USERBOT — slave.py                      ║
║  Gestisce N account in parallelo, controllati dal master.       ║
╚══════════════════════════════════════════════════════════════════╝

VARIABILI D'AMBIENTE RICHIESTE
───────────────────────────────
  MASTER_URL       → URL del master  (es: https://tuomaster.up.railway.app)

  Per ogni account (ripeti con N = 1, 2, 3, ...):
    API_ID_N         → API ID account N  (da my.telegram.org)
    API_HASH_N       → API Hash account N
    SESSION_STRING_N → Session string N  (lascia vuota al primo avvio)

PRIMO AVVIO
───────────
  1. Imposta API_ID_1 e API_HASH_1 (SESSION_STRING_1 lasciala vuota)
  2. Avvia — nei log apparirà la SESSION_STRING da copiare
  3. Incollala nelle variabili d'ambiente, poi aggiungi gli account successivi

COMPORTAMENTO AUTOMATICO
─────────────────────────
  • Legge la configurazione dal master via HTTP ad ogni ciclo
  • All'avvio entra subito in tutti i gruppi delle addlist assegnate
  • Silenzia per sempre ogni gruppo/canale delle addlist e ogni sorgente/destinazione
  • Invia post casuali dalle sorgenti verso le destinazioni
  • Risponde automaticamente ai PM (se configurato dal master con /replytext)
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
)
from telethon.sessions import StringSession
from telethon.utils import get_peer_id


# ═══════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ═══════════════════════════════════════════════════════════════════
# STATO PER ACCOUNT
# ═══════════════════════════════════════════════════════════════════

REPLIED_USERS_MAX = 500  # dopo questa soglia, la lista degli utenti già risposti viene svuotata

class AccountState:
    """Tiene traccia dello stato runtime di ogni account slave."""

    def __init__(self):
        self.current_targets: list = []       # destinazioni attive (aggiornate ad ogni ciclo)
        self.replied_users:   set  = set()    # utenti a cui è già stata inviata l'auto-risposta
        self.silenced_peers:  set  = set()    # peer già silenziati (stringhe, per evitare duplicati)
        self.joined_addlists: set  = set()    # slug delle addlist già elaborate

    def maybe_clear_replied_users(self):
        """Svuota la lista degli utenti risposti se supera la soglia."""
        if len(self.replied_users) >= REPLIED_USERS_MAX:
            self.replied_users.clear()


# ═══════════════════════════════════════════════════════════════════
# COMUNICAZIONE CON IL MASTER
# ═══════════════════════════════════════════════════════════════════

def _fetch_config_sync(master_url: str) -> dict | None:
    """Scarica la configurazione dal master (chiamata sincrona, eseguita in thread)."""
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
    """Versione async di _fetch_config_sync — non blocca il loop asyncio."""
    return await asyncio.to_thread(_fetch_config_sync, master_url)


# ═══════════════════════════════════════════════════════════════════
# BOTTONI E TESTO
# ═══════════════════════════════════════════════════════════════════

def build_buttons(buttons_rows: list) -> list | None:
    """Converte la lista di bottoni della config in oggetti Telethon Button."""
    if not buttons_rows:
        return None
    return [
        [Button.url(btn["text"], btn["url"]) for btn in row]
        for row in buttons_rows
    ]


def format_reply_text(template: str, user) -> str:
    """Sostituisce i segnaposto nel testo di auto-risposta con i dati reali dell'utente."""
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


# ═══════════════════════════════════════════════════════════════════
# SILENZIAMENTO CHAT
# ═══════════════════════════════════════════════════════════════════

async def silence_entity(client, log, entity, name: str = "") -> None:
    """
    Silenzia una chat/canale in modo permanente.
    mute_until = 2147483647 corrisponde all'unix timestamp massimo (anno 2038+).
    """
    try:
        await client(UpdateNotifySettingsRequest(
            peer=InputNotifyPeer(entity),
            settings=InputPeerNotifySettings(
                show_previews=False,
                silent=True,
                mute_until=2147483647,
            ),
        ))
        log.info(f"🔕 Silenziato: {name or entity}")
    except Exception as e:
        log.warning(f"Impossibile silenziare {name or entity}: {e}")


async def silence_peer(client, log, peer, state: AccountState) -> None:
    """Silenzia un peer (link, username o ID numerico) se non è già in stato.silenced_peers."""
    key = str(peer)
    if key in state.silenced_peers:
        return
    try:
        entity = await client.get_entity(peer)
        name   = getattr(entity, "title", None) or getattr(entity, "username", None) or key
        await silence_entity(client, log, entity, name)
        state.silenced_peers.add(key)
    except Exception as e:
        log.warning(f"Errore silenziamento {peer}: {e}")


async def silence_peers_list(client, log, sources: list, targets: list, state: AccountState) -> None:
    """Silenzia sorgenti e destinazioni, saltando quelli già elaborati."""
    all_peers = list({str(p): p for p in sources + targets}.values())
    for peer in all_peers:
        await silence_peer(client, log, peer, state)


# ═══════════════════════════════════════════════════════════════════
# ADDLIST — CARTELLE CONDIVISE (t.me/addlist/...)
# ═══════════════════════════════════════════════════════════════════

def _extract_slug(link: str) -> str | None:
    """
    Estrae lo slug dal link t.me/addlist/SLUG.
    Restituisce None se il link non ha il formato atteso.
    """
    link = link.strip().rstrip("/")
    for prefix in ("https://t.me/addlist/", "http://t.me/addlist/", "t.me/addlist/"):
        if link.lower().startswith(prefix):
            return link[len(prefix):]
    return None


async def process_addlist(client, log, link: str, state: AccountState) -> None:
    """
    Elabora un singolo link t.me/addlist/SLUG:
      1. Verifica i peer nella cartella condivisa via API Telegram
      2. Entra in tutti i gruppi/canali della cartella
      3. Silenzia permanentemente ogni peer della cartella
    """
    slug = _extract_slug(link)
    if not slug:
        log.warning(f"🔗 Addlist ignorata — non è un link t.me/addlist/...: {link}")
        return

    if slug in state.joined_addlists:
        return  # già elaborata in questa sessione, niente da fare

    state.joined_addlists.add(slug)
    log.info(f"🔗 Addlist: elaborazione slug={slug}")

    # ── STEP 1: recupera la lista dei peer nella cartella ─────────
    try:
        info = await client(CheckChatlistInviteRequest(slug=slug))
    except Exception as e:
        log.error(f"❌ Addlist: impossibile verificare {link}: {e}")
        return

    already_peers = getattr(info, "already_peers", []) or []   # peer in cui sei già membro
    missing_peers = getattr(info, "missing_peers", []) or []   # peer da cui devi ancora entrare
    all_peers     = already_peers + missing_peers

    if not all_peers:
        log.info(f"ℹ️ Addlist: cartella vuota o inaccessibile — {link}")
        return

    log.info(
        f"🔗 Addlist: {len(all_peers)} peer trovati "
        f"({len(already_peers)} già membro, {len(missing_peers)} da unire)"
    )

    # ── STEP 2: entra in tutti i peer mancanti ───────────────────
    if missing_peers:
        try:
            await client(JoinChatlistInviteRequest(slug=slug, peers=missing_peers))
            log.info(f"✅ Addlist: entrato in {len(missing_peers)} gruppi/canali")
        except FloodWaitError as e:
            log.warning(f"FloodWait join addlist {e.seconds}s — riprovo...")
            await asyncio.sleep(e.seconds + 1)
            try:
                await client(JoinChatlistInviteRequest(slug=slug, peers=missing_peers))
                log.info(f"✅ Addlist (retry): entrato in {len(missing_peers)} gruppi/canali")
            except Exception as e2:
                log.error(f"❌ Addlist join fallito anche al retry: {e2}")
        except Exception as e:
            log.error(f"❌ Addlist join fallito: {e}")

    # ── STEP 3: silenzia ogni peer della cartella ────────────────
    log.info(f"🔕 Addlist: silenziamento di {len(all_peers)} peer...")
    for peer in all_peers:
        key = str(get_peer_id(peer)) if hasattr(peer, "SUBCLASS_OF_ID") else str(peer)
        if key in state.silenced_peers:
            continue
        try:
            entity = await client.get_entity(peer)
            name   = getattr(entity, "title", None) or getattr(entity, "username", None) or key
            await silence_entity(client, log, entity, name)
            state.silenced_peers.add(key)
            await asyncio.sleep(0.3)  # piccola pausa per evitare rate-limit
        except Exception as e:
            log.warning(f"Impossibile silenziare peer della addlist: {e}")


async def process_all_addlists(client, log, cfg: dict, account_index: int, state: AccountState) -> None:
    """
    Processa tutte le addlist assegnate a questo slave:
      - addlist globali (valide per tutti gli slave)
      - addlist specifiche per questo slave (identificato da account_index)
    """
    global_links = cfg.get("global_addlists", [])
    slave_links  = cfg.get("slave_addlists", {}).get(str(account_index), [])

    # Unifica e deduplica mantenendo l'ordine
    seen, all_links = set(), []
    for link in global_links + slave_links:
        if link not in seen:
            seen.add(link)
            all_links.append(link)

    for link in all_links:
        await process_addlist(client, log, link, state)
        await asyncio.sleep(2)  # pausa tra un'addlist e l'altra


# ═══════════════════════════════════════════════════════════════════
# INVIO MESSAGGI
# ═══════════════════════════════════════════════════════════════════

async def copy_to_target(client, log, msg, target, buttons_rows: list, _retries: int = 0) -> None:
    """
    Invia un messaggio (testo o media) su una destinazione.
    Gestisce automaticamente FloodWaitError fino a 3 tentativi.
    """
    try:
        text     = msg.message or getattr(msg, "caption", "") or ""
        entities = msg.entities or []
        buttons  = build_buttons(buttons_rows)

        if msg.media:
            try:
                await client.send_file(
                    target, file=msg.media, caption=text,
                    formatting_entities=entities, buttons=buttons, silent=False,
                )
            except Exception as media_err:
                log.warning(f"Media fallito su {target} ({media_err}) — invio solo testo")
                if text:
                    await client.send_message(target, text, formatting_entities=entities, buttons=buttons)
        else:
            await client.send_message(target, text, formatting_entities=entities, buttons=buttons)

        log.info(f"✅ msg {msg.id} → {target}")

    except FloodWaitError as e:
        if _retries >= 3:
            log.error(f"FloodWait ripetuto {_retries}x su {target} — messaggio saltato.")
            return
        log.warning(f"FloodWait {e.seconds}s (tentativo {_retries + 1}/3)")
        await asyncio.sleep(e.seconds + 1)
        await copy_to_target(client, log, msg, target, buttons_rows, _retries + 1)

    except Exception as e:
        log.error(f"Errore invio → {target}: {e}")


# ═══════════════════════════════════════════════════════════════════
# AUTO-RISPOSTA PM
# ═══════════════════════════════════════════════════════════════════

async def handle_private_message(event, client, log, master_url: str, state: AccountState) -> None:
    """
    Risponde automaticamente ai PM degli utenti che sono membri di almeno una destinazione.
    Ogni utente riceve la risposta una sola volta (tracciato in state.replied_users).
    """
    sender = await event.get_sender()
    if sender is None or getattr(sender, "bot", False) or getattr(sender, "is_self", False):
        return

    user_id = sender.id
    if user_id in state.replied_users or not state.current_targets:
        return

    cfg = await fetch_master_config(master_url)
    if not cfg:
        return

    reply_text = cfg.get("auto_reply_text", "")
    if not reply_text:
        return

    # Verifica che l'utente sia membro di almeno una destinazione
    is_member = any([
        await user_is_in_target(client, user_id, t)
        for t in state.current_targets
    ])
    if not is_member:
        return

    state.replied_users.add(user_id)
    log.info(f"💬 Auto-risposta PM a {sender.first_name} (id={user_id})")

    try:
        await client.send_message(sender, format_reply_text(reply_text, sender))
    except FloodWaitError as e:
        log.warning(f"FloodWait auto-risposta {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        log.error(f"Errore auto-risposta a {user_id}: {e}")


async def user_is_in_target(client, user_id: int, target) -> bool:
    """Verifica se un utente è membro di un canale/gruppo destinazione."""
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


# ═══════════════════════════════════════════════════════════════════
# SPAM LOOP
# ═══════════════════════════════════════════════════════════════════

async def spam_loop(client, log, master_url: str, account_index: int, state: AccountState) -> None:
    """
    Loop principale dello slave:
      1. Scarica la config dal master
      2. Elabora addlist (join + silenziamento cartelle)
      3. Silenzia sorgenti e destinazioni
      4. Invia un post casuale da ogni sorgente verso tutte le destinazioni
      5. Attende l'intervallo configurato
    """
    while True:
        # ── Scarica config ────────────────────────────────────────
        cfg = await fetch_master_config(master_url)
        if not cfg:
            log.warning("Config master non disponibile — riprovo tra 60s")
            await asyncio.sleep(60)
            continue

        if not cfg.get("running", True):
            log.info("Master ha fermato l'invio — in pausa")
            await asyncio.sleep(cfg.get("interval", 10) * 60)
            continue

        # ── Determina sorgenti e intervallo ───────────────────────
        slave_key      = str(account_index)
        slave_specific = cfg.get("slave_sources", {}).get(slave_key)

        if slave_specific:
            my_sources = slave_specific
            log.info(f"📥 Sorgenti proprie slave {slave_key}: {my_sources}")
        else:
            my_sources = cfg.get("sources", [])
            log.info(f"📥 Sorgenti dal master (nessuna propria): {my_sources}")

        targets      = cfg.get("targets", [])
        buttons_rows = cfg.get("buttons_rows", [])
        interval     = max(1, cfg.get("slave_intervals", {}).get(slave_key, cfg.get("interval", 10)))

        log.info(f"⏱ Intervallo: {interval} min")
        state.current_targets = targets
        state.maybe_clear_replied_users()

        # ── Addlist: entra nei gruppi e silenzia ──────────────────
        await process_all_addlists(client, log, cfg, account_index, state)

        # ── Silenzia sorgenti e destinazioni ─────────────────────
        await silence_peers_list(client, log, my_sources, targets, state)

        # ── Invio messaggi ────────────────────────────────────────
        if not my_sources or not targets:
            log.info("Nessuna sorgente o destinazione — attendo...")
            await asyncio.sleep(interval * 60)
            continue

        for source in my_sources:
            try:
                all_msgs = await client.get_messages(source, limit=200)
                valid    = [m for m in all_msgs if m.message or m.media]

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
                log.error(f"❌ Errore sorgente {source}: {e}")

        await asyncio.sleep(interval * 60)


# ═══════════════════════════════════════════════════════════════════
# AVVIO SINGOLO ACCOUNT
# ═══════════════════════════════════════════════════════════════════

async def run_account(account_index: int, api_id: int, api_hash: str,
                      session_string: str, master_url: str) -> None:
    """Connette un account Telegram e avvia il loop spam + il gestore PM."""
    log   = logging.getLogger(f"account-{account_index}")
    state = AccountState()

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.start()

    if not session_string:
        print("\n" + "=" * 60)
        print(f"✅ Account {account_index} — salva questa SESSION_STRING_{account_index}:")
        print(client.session.save())
        print("=" * 60 + "\n")

    log.info(f"🚀 Connesso | master: {master_url}")

    # ── Inizializzazione: addlist e silenziamento all'avvio ───────
    initial_cfg = await fetch_master_config(master_url)
    if initial_cfg:
        slave_key    = str(account_index)
        init_sources = initial_cfg.get("slave_sources", {}).get(slave_key) or initial_cfg.get("sources", [])
        init_targets = initial_cfg.get("targets", [])

        log.info("🔗 Elaborazione addlist iniziali...")
        await process_all_addlists(client, log, initial_cfg, account_index, state)

        log.info("🔕 Silenziamento iniziale sorgenti e destinazioni...")
        await silence_peers_list(client, log, init_sources, init_targets, state)

    # ── Gestore messaggi privati ──────────────────────────────────
    @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def pm_handler(event):
        await handle_private_message(event, client, log, master_url, state)

    # ── Avvio spam loop ───────────────────────────────────────────
    spam_task = asyncio.create_task(spam_loop(client, log, master_url, account_index, state))
    log.info(f"🎉 Account {account_index} operativo!")

    try:
        await client.run_until_disconnected()
    finally:
        spam_task.cancel()
        try:
            await spam_task
        except asyncio.CancelledError:
            pass


# ═══════════════════════════════════════════════════════════════════
# CARICAMENTO ACCOUNT DA VARIABILI D'AMBIENTE
# ═══════════════════════════════════════════════════════════════════

def load_accounts(master_url: str) -> list[dict]:
    """
    Legge gli account dalle variabili d'ambiente.
    Si aspetta le variabili API_ID_1, API_HASH_1, SESSION_STRING_1, ecc.
    Si ferma al primo N senza API_ID_N o API_HASH_N.
    """
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


# ═══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════

async def main() -> None:
    master_url = os.environ.get("MASTER_URL", "").rstrip("/")

    if not master_url:
        logging.error("❌ Variabile MASTER_URL non impostata!")
        return

    accounts = load_accounts(master_url)

    if not accounts:
        logging.error(
            "❌ Nessun account trovato!\n"
            "   Imposta almeno: API_ID_1, API_HASH_1, SESSION_STRING_1"
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
