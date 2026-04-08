"""
Telegram Slave Userbot — Multi-Account
=======================================
Un unico script che gestisce N account Telegram in parallelo,
tutti controllati dal master.

── CONFIGURAZIONE VARIABILI D'AMBIENTE ────────────────────────────────────────

  MASTER_URL       — URL del master, es: https://tuomaster.railway.app

  Per ogni account (sostituisci N con 1, 2, 3, ...):
    API_ID_N         — API ID dell'account N  (da my.telegram.org)
    API_HASH_N       — API Hash dell'account N
    SESSION_STRING_N — Session string dell'account N (lascia vuota al primo avvio)

  Esempio per 3 account:
    API_ID_1, API_HASH_1, SESSION_STRING_1
    API_ID_2, API_HASH_2, SESSION_STRING_2
    API_ID_3, API_HASH_3, SESSION_STRING_3

── COMANDI (in "Messaggi Salvati" di qualsiasi account) ───────────────────────
  /start — avvia tutti gli account
  /off   — ferma tutti gli account
  /s     — mostra stato
  /h     — aiuto

── PRIMO AVVIO (generazione SESSION_STRING) ───────────────────────────────────
  1. Imposta API_ID_1 e API_HASH_1 (SESSION_STRING_1 lasciala vuota)
  2. Esegui il bot — nei log apparirà la SESSION_STRING da copiare
  3. Aggiungi SESSION_STRING_1 nelle variabili, poi aggiungi il secondo account, ecc.
"""

import asyncio
import json
import logging
import os
import urllib.request
import urllib.error

from telethon import TelegramClient, events, Button
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ── STATO CONDIVISO TRA TUTTI GLI ACCOUNT ────────────────────────────────────
trigger_now  = asyncio.Event()   # scatta l'invio immediato per tutti
slave_running = True             # se False, tutti gli account si fermano


def fetch_master_config(master_url: str) -> dict | None:
    """Scarica la configurazione dal master via HTTP."""
    url = master_url.rstrip("/") + "/api/slave-config"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        logging.error(f"Errore HTTP master: {e.code} {e.reason}")
    except Exception as e:
        logging.error(f"Impossibile contattare il master: {e}")
    return None


def build_buttons(buttons_rows: list) -> list | None:
    if not buttons_rows:
        return None
    return [
        [Button.url(btn["text"], btn["url"]) for btn in row]
        for row in buttons_rows
    ]


async def copy_to_target(client, log, msg, target, buttons_rows):
    try:
        text     = msg.message or getattr(msg, "caption", "") or ""
        entities = msg.entities or []
        buttons  = build_buttons(buttons_rows)

        if msg.media:
            await client.send_file(
                target, file=msg.media, caption=text,
                formatting_entities=entities, buttons=buttons, silent=False
            )
        else:
            await client.send_message(
                target, text,
                formatting_entities=entities, buttons=buttons
            )
        log.info(f"✅ msg {msg.id} → {target}")

    except FloodWaitError as e:
        log.warning(f"FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        await copy_to_target(client, log, msg, target, buttons_rows)
    except Exception as e:
        log.error(f"Errore → {target}: {e}")


async def spam_loop(client, log, master_url: str, account_index: int):
    """Loop per un singolo account: rotazione post dal master."""
    global slave_running
    rotation_indices: dict[str, int] = {}

    while True:
        if not slave_running:
            await asyncio.sleep(5)
            continue

        cfg = fetch_master_config(master_url)
        if not cfg:
            log.warning("Config master non disponibile, riprovo tra 60s")
            await asyncio.sleep(60)
            continue

        if not cfg.get("running", True):
            log.info("Master ha fermato l'invio — in pausa")
            await asyncio.sleep(cfg.get("interval", 10) * 60)
            continue

        sources      = cfg.get("sources", [])
        targets      = cfg.get("targets", [])
        buttons_rows = cfg.get("buttons_rows", [])
        interval     = max(1, cfg.get("interval", 10))

        if not sources or not targets:
            log.info("Nessuna sorgente o destinazione configurata — attendo...")
            await asyncio.sleep(interval * 60)
            continue

        for source in sources:
            try:
                all_msgs = await client.get_messages(source, limit=200)
                valid = sorted(
                    [m for m in all_msgs if m.message or m.media],
                    key=lambda m: m.id
                )
                if not valid:
                    continue

                # Ogni account ha il proprio indice di rotazione, ma staggered:
                # account 1 parte dal post 0, account 2 dal post 1, ecc.
                key = source
                if key not in rotation_indices:
                    rotation_indices[key] = account_index % len(valid)

                idx = rotation_indices[key] % len(valid)
                msg = valid[idx]
                log.info(f"📤 Post {idx+1}/{len(valid)} (id={msg.id}) da {source}")

                await asyncio.gather(*[
                    copy_to_target(client, log, msg, t, buttons_rows)
                    for t in targets
                ])

                rotation_indices[key] = (idx + 1) % len(valid)

            except Exception as e:
                log.error(f"Errore sorgente {source}: {e}")

        try:
            await asyncio.wait_for(trigger_now.wait(), timeout=interval * 60)
            trigger_now.clear()
        except asyncio.TimeoutError:
            pass


async def run_account(account_index: int, api_id: int, api_hash: str,
                      session_string: str, master_url: str):
    """Avvia un singolo account Telegram."""
    global slave_running
    log = logging.getLogger(f"account-{account_index}")

    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.start()

    if not session_string:
        print("\n" + "=" * 60)
        print(f"✅ Account {account_index} — salva questa SESSION_STRING_{account_index}:")
        print(client.session.save())
        print("=" * 60 + "\n")

    log.info(f"🚀 Connesso | master: {master_url}")

    @client.on(events.NewMessage(chats="me"))
    async def command_handler(event):
        global slave_running
        text = (event.message.text or "").strip()
        if not text:
            return

        if text.startswith("/start"):
            slave_running = True
            trigger_now.set()
            await event.reply(f"✅ Tutti gli slave avviati (da account {account_index})")

        elif text.startswith("/off") or text.startswith("/stop"):
            slave_running = False
            await event.reply(f"⛔ Tutti gli slave fermati (da account {account_index})")

        elif text == "/s":
            cfg = fetch_master_config(master_url)
            if cfg:
                stato_master = "🟢 Attivo" if cfg.get("running") else "🔴 Fermo (dal master)"
                stato_slave  = "🟢 Attivo" if slave_running else "🔴 Fermo (locale)"
                n_src = len(cfg.get("sources", []))
                n_tgt = len(cfg.get("targets", []))
                n_btn = sum(len(r) for r in cfg.get("buttons_rows", []))
                await event.reply(
                    f"📊 **STATO** — Account {account_index}\n\n"
                    f"Slave: {stato_slave}\n"
                    f"Master: {stato_master}\n"
                    f"Intervallo: {cfg.get('interval', '?')} min\n"
                    f"Sorgenti: {n_src} | Destinazioni: {n_tgt} | Bottoni: {n_btn}"
                )
            else:
                await event.reply("❌ Impossibile contattare il master.")

        elif text.startswith("/h"):
            await event.reply(
                "📋 **COMANDI SLAVE**\n\n"
                "`/start` — avvia tutti gli account\n"
                "`/off` — ferma tutti gli account\n"
                "`/s` — mostra stato\n\n"
                "Canali, bottoni e intervallo sono controllati dal master."
            )

    spam_task = asyncio.create_task(spam_loop(client, log, master_url, account_index))
    log.info(f"🎉 Account {account_index} pronto!")
    await client.run_until_disconnected()
    spam_task.cancel()


def load_accounts(master_url: str) -> list[dict]:
    """
    Legge gli account dalle variabili d'ambiente.
    Cerca API_ID_1, API_HASH_1, SESSION_STRING_1, poi _2, _3, ecc.
    Si ferma al primo numero mancante.
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

    # Esegui tutti gli account contemporaneamente
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
