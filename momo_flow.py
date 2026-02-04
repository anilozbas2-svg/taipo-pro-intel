import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, ContextTypes, Application

logger = logging.getLogger("MOMO_FLOW")

# ==========================
# FLOW CONFIG (env)
# ==========================
MOMO_FLOW_ENABLED = os.getenv("MOMO_FLOW_ENABLED", "1").strip() == "1"
MOMO_FLOW_CHAT_ID = os.getenv("MOMO_FLOW_CHAT_ID", "").strip()

MOMO_FLOW_INTERVAL_MIN = int(os.getenv("MOMO_FLOW_INTERVAL_MIN", "2"))

FLOW_PCT_MIN = float(os.getenv("MOMO_FLOW_PCT_MIN", "2.50"))
FLOW_PCT_ROCKET_MIN = float(os.getenv("MOMO_FLOW_PCT_ROCKET_MIN", "4.00"))

FLOW_COOLDOWN_SEC = int(os.getenv("MOMO_FLOW_COOLDOWN_SEC", "1800"))  # 30 min
FLOW_TOP_N = int(os.getenv("MOMO_FLOW_TOP_N", "200"))
FLOW_MAX_ALERTS_PER_SCAN = int(os.getenv("MOMO_FLOW_MAX_ALERTS_PER_SCAN", "5"))

TV_SCAN_URL = os.getenv("MOMO_FLOW_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = int(os.getenv("MOMO_FLOW_TV_TIMEOUT", "12"))

DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
FLOW_STATE_FILE = os.path.join(DATA_DIR, "momo_flow_state.json")
FLOW_LAST_ALERT_FILE = os.path.join(DATA_DIR, "momo_flow_last_alert.json")

# ======================
# SESSION HELPERS
# ======================
from datetime import datetime

def _bist_session_open() -> bool:
    now = datetime.now()  # Render TR saatinde
    wd = now.weekday()    # 0=Pzt ... 6=Paz
    if wd >= 5:
        return False

    hm = now.hour * 60 + now.minute
    return (10 * 60) <= hm <= (18 * 60 + 10)

# ==========================
# JSON helpers
# ==========================
def _load_json(path: str, default: dict) -> dict:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.exception("FLOW load_json error: %s", e)
        return default


def _save_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.exception("FLOW save_json error: %s", e)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_iso(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        return None


def _hash_message(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


def _cooldown_ok(last_alert_ts: Optional[float], now_ts: float) -> bool:
    if last_alert_ts is None:
        return True
    return (now_ts - last_alert_ts) >= FLOW_COOLDOWN_SEC


# ==========================
# Defaults (isolated state)
# ==========================
def _default_flow_state() -> dict:
    return {
        "schema_version": "1.0",
        "system": "momo_flow_rocket",
        "telegram": {
            "momo_flow_chat_id": int(MOMO_FLOW_CHAT_ID) if MOMO_FLOW_CHAT_ID else None
        },
        "scan": {
            "interval_seconds": MOMO_FLOW_INTERVAL_MIN * 60,
            "last_scan_utc": None
        },
        "rules": {
            "pct_min": FLOW_PCT_MIN,
            "pct_rocket_min": FLOW_PCT_ROCKET_MIN,
            "cooldown_seconds": FLOW_COOLDOWN_SEC,
            "top_n": FLOW_TOP_N,
            "max_alerts_per_scan": FLOW_MAX_ALERTS_PER_SCAN
        }
    }


def _default_last_alert() -> dict:
    return {
        "schema_version": "1.0",
        "system": "momo_flow_rocket",
        "cooldown_seconds": FLOW_COOLDOWN_SEC,
        "last_alert_by_symbol": {}
    }


# ==========================
# TradingView scan
# ==========================
def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
    # TradingView sometimes returns "BIST:ALFAS"
    if ":" in s:
        s = s.split(":")[-1].strip()
    return s


def _tv_scan_rows() -> List[dict]:
    payload = {
        "filter": [
            {"left": "market_cap_basic", "operation": "nempty"},
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"}
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "change", "volume", "close"],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, max(0, FLOW_TOP_N - 1)]
    }
    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        out = []
        for row in data.get("data", []) or []:
            d = row.get("d") or []
            if len(d) < 4:
                continue
            sym = _normalize_symbol(str(d[0]))
            try:
                out.append({
                    "symbol": sym,
                    "change_pct": float(d[1]),
                    "volume": float(d[2]),
                    "close": float(d[3])
                })
            except Exception:
                continue
        return out
    except Exception as e:
        logger.error("FLOW TV scan error: %s", e)
        return []


# ==========================
# Message formatting
# ==========================
def _flow_tag(pct: float) -> str:
    if pct >= FLOW_PCT_ROCKET_MIN:
        return "🚀 ROCKET"
    return "⚡️ FLOW"


def _format_flow_message(ticker: str, pct: float, volume: float, close: float) -> str:
    tag = _flow_tag(pct)
    msg = (
        "⚡️ <b>MOMO FLOW ROCKET</b> 🚀\n\n"
        f"<b>HİSSE:</b> {ticker}\n"
        f"<b>AKIŞ:</b> {pct:+.2f}%  <b>({tag})</b>\n"
        f"<b>TV HACİM:</b> {volume:,.0f}\n"
        f"<b>FİYAT:</b> {close:.2f}\n\n"
        "🧠 <i>Mentor notu:</i> Bu sinyal PRIME değil; momentum/akış takibidir.\n"
        f"⏱ {datetime.now().strftime('%H:%M')}"
    )
    return msg


# ==========================
# Decision
# ==========================
def _should_alert(last_alert_map: dict, ticker: str, message_hash: str, now_ts: float) -> bool:
    entry = (last_alert_map.get(ticker) or {})
    last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
    if not _cooldown_ok(last_ts, now_ts):
        return False
    if entry.get("last_message_hash") == message_hash:
        return False
    return True


# ==========================
# /flow command
# ==========================
async def cmd_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub = ""
    if context.args:
        sub = (context.args[0] or "").strip().lower()

    if sub in ("help", ""):
        txt = (
            "⚡️ MOMO FLOW ROCKET 🚀\n\n"
            "Komutlar:\n"
            "• /flow status  → FLOW durum\n"
            "• /flow test    → test mesajı\n\n"
            f"Not: Tarama {MOMO_FLOW_INTERVAL_MIN} dk; koşullar: +%{FLOW_PCT_MIN:.2f} ve üstü. "
            f"ROCKET: +%{FLOW_PCT_ROCKET_MIN:.2f} ve üstü. "
            f"Cooldown: {int(FLOW_COOLDOWN_SEC / 60)} dk. Max/scan: {FLOW_MAX_ALERTS_PER_SCAN}."
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "status":
        st = _load_json(FLOW_STATE_FILE, _default_flow_state())
        la = _load_json(FLOW_LAST_ALERT_FILE, _default_last_alert())
        last_scan = ((st.get("scan") or {}).get("last_scan_utc")) or "n/a"
        n_alerts = len((la.get("last_alert_by_symbol") or {}))
        txt = (
            "⚡️ FLOW STATUS 🚀\n\n"
            f"enabled: {int(MOMO_FLOW_ENABLED)}\n"
            f"chat_id: {MOMO_FLOW_CHAT_ID or 'n/a'}\n"
            f"last_scan_utc: {last_scan}\n"
            f"tracked_alerts: {n_alerts}\n"
            f"cooldown(min): {int(FLOW_COOLDOWN_SEC / 60)}\n"
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "test":
        if not MOMO_FLOW_CHAT_ID:
            await update.effective_message.reply_text("MOMO_FLOW_CHAT_ID yok. Env ayarla.")
            return
        try:
            await context.bot.send_message(
                chat_id=MOMO_FLOW_CHAT_ID,
                text="⚡️ <b>MOMO FLOW ROCKET</b> 🚀\n\nTest mesajı ✅",
                parse_mode=ParseMode.HTML
            )
            await update.effective_message.reply_text("Test gönderildi ✅")
        except Exception as e:
            await update.effective_message.reply_text(f"Test hata: {e}")
        return

    await update.effective_message.reply_text("Bilinmeyen alt komut. /flow help")


def register_momo_flow(app: Application) -> None:
    app.add_handler(CommandHandler("flow", cmd_flow))


# ==========================
# Scheduled job
# ==========================
async def job_momo_flow_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    # 🔒 BIST kapalıysa FLOW tamamen durur
    if not _bist_session_open():
        return

    if not MOMO_FLOW_ENABLED:
        return

    if not MOMO_FLOW_CHAT_ID:
        return

    now_ts = time.time()
    if not MOMO_FLOW_ENABLED:
        return
    if not MOMO_FLOW_CHAT_ID:
        return

    now_ts = time.time()

    st = _load_json(FLOW_STATE_FILE, _default_flow_state())
    la = _load_json(FLOW_LAST_ALERT_FILE, _default_last_alert())
    last_alert_by_symbol = la.get("last_alert_by_symbol") or {}

    rows = _tv_scan_rows()
    st["scan"]["last_scan_utc"] = _utc_now_iso()
    _save_json(FLOW_STATE_FILE, st)

    if not rows:
        logger.info("FLOW: no rows")
        return

    candidates = []
    for r in rows:
        ticker = (r.get("symbol") or "").strip().upper()
        pct = float(r.get("change_pct") or 0.0)
        vol = float(r.get("volume") or 0.0)
        close = float(r.get("close") or 0.0)

        if not ticker:
            continue
        if pct < FLOW_PCT_MIN:
            continue
        if vol <= 0:
            continue

        candidates.append((ticker, pct, vol, close))

    # Higher pct first (true "uçan" öncelik)
    candidates.sort(key=lambda x: x[1], reverse=True)

    sent = 0
    for (ticker, pct, vol, close) in candidates:
        if sent >= FLOW_MAX_ALERTS_PER_SCAN:
            break

        msg = _format_flow_message(ticker, pct, vol, close)
        mh = _hash_message(msg)

        if not _should_alert(last_alert_by_symbol, ticker, mh, now_ts):
            continue

        try:
            await context.bot.send_message(
                chat_id=MOMO_FLOW_CHAT_ID,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            sent += 1
        except Exception as e:
            logger.error("FLOW send error: %s", e)
            continue

        last_alert_by_symbol[ticker] = {
            "last_alert_utc": _utc_now_iso(),
            "last_alert_pct": pct,
            "last_volume": vol,
            "last_close": close,
            "last_message_hash": mh
        }

    la["last_alert_by_symbol"] = last_alert_by_symbol
    _save_json(FLOW_LAST_ALERT_FILE, la)

    if sent > 0:
        logger.info("FLOW: sent=%d", sent)
    else:
        logger.info("FLOW: no alerts")
