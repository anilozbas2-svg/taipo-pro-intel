import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

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

MOMO_FLOW_INTERVAL_MIN = int(os.getenv("MOMO_FLOW_INTERVAL_MIN", "1"))

# EARLY RADAR + ROCKET thresholds
FLOW_PCT_MIN = float(os.getenv("MOMO_FLOW_PCT_MIN", "1.20"))  # ERKEN RADAR
FLOW_PCT_ROCKET_MIN = float(os.getenv("MOMO_FLOW_PCT_ROCKET_MIN", "2.50"))  # ROCKET

FLOW_RADAR_DELTA_MIN = float(os.getenv("MOMO_FLOW_RADAR_DELTA_MIN", "0.35"))
FLOW_ROCKET_DELTA_MIN = float(os.getenv("MOMO_FLOW_ROCKET_DELTA_MIN", "0.60"))

FLOW_VOL_SPIKE_MIN = float(os.getenv("MOMO_FLOW_VOL_SPIKE_MIN", "1.20"))
FLOW_VOL_SPIKE_ROCKET_MIN = float(os.getenv("MOMO_FLOW_VOL_SPIKE_ROCKET_MIN", "1.60"))

# Hard cap (anti-spam): pct > cap ise mesaj atma
FLOW_PCT_CAP = float(os.getenv("MOMO_FLOW_PCT_CAP", "3.00"))

# Anti-spam: cooldown + step-up (same symbol can alert again if it stepped up enough)
FLOW_COOLDOWN_SEC = int(os.getenv("MOMO_FLOW_COOLDOWN_SEC", "900"))  # 15 min
FLOW_STEPUP_PCT = float(os.getenv("MOMO_FLOW_STEPUP_PCT", "0.70"))  # within cooldown, allow if pct increased by this

FLOW_TOP_N = int(os.getenv("MOMO_FLOW_TOP_N", "200"))
FLOW_MAX_ALERTS_PER_SCAN = int(os.getenv("MOMO_FLOW_MAX_ALERTS_PER_SCAN", "6"))

TV_SCAN_URL = os.getenv("MOMO_FLOW_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = int(os.getenv("MOMO_FLOW_TV_TIMEOUT", "12"))

DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
FLOW_STATE_FILE = os.path.join(DATA_DIR, "momo_flow_state.json")
FLOW_LAST_ALERT_FILE = os.path.join(DATA_DIR, "momo_flow_last_alert.json")

# Rolling volume memory (per symbol)
FLOW_VOL_ROLL_N = int(os.getenv("MOMO_FLOW_VOL_ROLL_N", "10"))  # last N scans

# ======================
# SESSION HELPERS
# ======================
def _istanbul_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Istanbul"))
    except Exception:
        # Fallback: if server is already TR time, this is fine; otherwise user should add TZ
        return datetime.now()


def _bist_session_open() -> bool:
    now = _istanbul_now()
    wd = now.weekday()  # 0=Pzt ... 6=Paz
    if wd >= 5:
        return False

    hm = now.hour * 60 + now.minute
    # 10:00 - 18:10 (TR)
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


def _cooldown_ok(
    last_alert_ts: Optional[float],
    now_ts: float,
    cooldown_sec: Optional[int] = None
) -> bool:
    if last_alert_ts is None:
        return True
    cd = int(cooldown_sec) if cooldown_sec is not None else int(FLOW_COOLDOWN_SEC)
    return (now_ts - last_alert_ts) >= cd


# ==========================
# Defaults (isolated state)
# ==========================
def _default_flow_state() -> dict:
    return {
        "schema_version": "2.0",
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
            "radar_delta_min": FLOW_RADAR_DELTA_MIN,
            "rocket_delta_min": FLOW_ROCKET_DELTA_MIN,
            "vol_spike_min": FLOW_VOL_SPIKE_MIN,
            "vol_spike_rocket_min": FLOW_VOL_SPIKE_ROCKET_MIN,
            "cooldown_seconds": FLOW_COOLDOWN_SEC,
            "stepup_pct": FLOW_STEPUP_PCT,
            "top_n": FLOW_TOP_N,
            "max_alerts_per_scan": FLOW_MAX_ALERTS_PER_SCAN,
            "vol_roll_n": FLOW_VOL_ROLL_N
        },
        "recent": {
            "by_symbol": {}
        }
    }


def _default_last_alert() -> dict:
    return {
        "schema_version": "2.0",
        "system": "momo_flow_rocket",
        "cooldown_seconds": FLOW_COOLDOWN_SEC,
        "last_alert_by_symbol": {}
    }


# ==========================
# TradingView scan
# ==========================
def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
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
# Rolling volume + delta helpers
# ==========================
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _roll_append(vals: List[float], v: float, n: int) -> List[float]:
    vals = list(vals or [])
    vals.append(float(v))
    if len(vals) > n:
        vals = vals[-n:]
    return vals


def _avg(vals: List[float]) -> Optional[float]:
    v = [x for x in (vals or []) if isinstance(x, (int, float))]
    if len(v) < 3:
        return None
    return sum(v) / float(len(v))


def _compute_vol_spike(recent_vols: List[float], current_vol: float) -> Optional[float]:
    av = _avg(recent_vols)
    if not av or av <= 0:
        return None
    return current_vol / av


def _level_from_metrics(pct: float, pct_delta: float, vol_spike: Optional[float]) -> Optional[str]:
    # 🔒 Hard cap: % cap üstündeyse hiç sinyal üretme (anti-spam)
    if pct > FLOW_PCT_CAP:
        return None

    vs = vol_spike if vol_spike is not None else 0.0

    # ROCKET first
    if pct >= FLOW_PCT_ROCKET_MIN and pct_delta >= FLOW_ROCKET_DELTA_MIN and vs >= FLOW_VOL_SPIKE_ROCKET_MIN:
        return "ROCKET"

    # EARLY RADAR
    if pct >= FLOW_PCT_MIN and pct_delta >= FLOW_RADAR_DELTA_MIN and vs >= FLOW_VOL_SPIKE_MIN:
        return "RADAR"

    return None


# ==========================
# Message formatting
# ==========================
def _format_flow_message(
    ticker: str,
    pct: float,
    pct_delta: float,
    volume: float,
    close: float,
    level: str,
    vol_spike: Optional[float]
) -> str:
    if level == "ROCKET":
        head = "🚀 <b>MOMO FLOW – ROCKET</b>"
        note = "🧠 <i>Mentor notu:</i> Hızlanma teyitli. Takip + disiplin."
    else:
        head = "🟡 <b>MOMO FLOW – ERKEN RADAR</b>"
        note = "🧠 <i>Mentor notu:</i> Kıvılcım yakalandı. İzleme listesi aç."

    vs_txt = "n/a" if vol_spike is None else f"{vol_spike:.2f}x"
    msg = (
        f"{head}\n\n"
        f"<b>HİSSE:</b> {ticker}\n"
        f"<b>AKIŞ:</b> {pct:+.2f}%  <b>({level})</b>\n"
        f"<b>İVME:</b> {pct_delta:+.2f}% (son taramaya göre)\n"
        f"<b>VOL SPIKE:</b> {vs_txt} (son {FLOW_VOL_ROLL_N} tarama ort)\n"
        f"<b>TV HACİM:</b> {volume:,.0f}\n"
        f"<b>FİYAT:</b> {close:.2f}\n\n"
        f"{note}\n"
        f"⏱ {_istanbul_now().strftime('%H:%M')}"
    )
    return msg


# ==========================
# Decision (cooldown + step-up)
# ==========================
def _should_alert(last_alert_map: dict, ticker: str, pct: float, message_hash: str, now_ts: float) -> bool:
    entry = (last_alert_map.get(ticker) or {})
    last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
    last_pct = _safe_float(entry.get("last_alert_pct"))

    if entry.get("last_message_hash") == message_hash:
        return False

    # Cooldown ok -> allow
    cooldown_sec = FLOW_PRIME_COOLDOWN_SEC if level == "PRIME" else FLOW_COOLDOWN_SEC
    if _cooldown_ok(last_ts, now_ts, cooldown_sec=cooldown_sec):
        return True

    # Still in cooldown -> allow ONLY if stepped up enough
    if last_pct is not None and (pct - last_pct) >= FLOW_STEPUP_PCT:
        return True

    return False


# ==========================
# /flow command
# ==========================
async def cmd_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub = ""
    if context.args:
        sub = (context.args[0] or "").strip().lower()

    # HELP
    if sub in ("help", ""):
        txt = (
            "⚡️ MOMO FLOW – ERKEN YAKALAMA\n\n"
            "Komutlar:\n"
            "• /flow status        → FLOW durum\n"
            "• /flow test          → test mesajı\n"
            "• /flow check X       → tek hisse sağlık kontrolü\n"
            "• /flow watch         → son taramalardan izleme listesi\n\n"
            f"Tarama: {MOMO_FLOW_INTERVAL_MIN} dk\n"
            f"ERKEN RADAR: pct≥{FLOW_PCT_MIN:.2f} | delta≥{FLOW_RADAR_DELTA_MIN:.2f} | vol_spike≥{FLOW_VOL_SPIKE_MIN:.2f}\n"
            f"ROCKET: pct≥{FLOW_PCT_ROCKET_MIN:.2f} | delta≥{FLOW_ROCKET_DELTA_MIN:.2f} | vol_spike≥{FLOW_VOL_SPIKE_ROCKET_MIN:.2f}\n"
            f"Cooldown: {int(FLOW_COOLDOWN_SEC / 60)} dk | Max/scan: {FLOW_MAX_ALERTS_PER_SCAN}\n"
            "BIST kapalıysa FLOW susar."
        )
        await update.effective_message.reply_text(txt)
        return

    # STATUS
    if sub == "status":
        st = _load_json(FLOW_STATE_FILE, _default_flow_state())
        la = _load_json(FLOW_LAST_ALERT_FILE, _default_last_alert())
        last_scan = ((st.get("scan") or {}).get("last_scan_utc")) or "n/a"
        n_alerts = len((la.get("last_alert_by_symbol") or {}))
        txt = (
            "⚡️ FLOW STATUS\n\n"
            f"enabled: {int(MOMO_FLOW_ENABLED)}\n"
            f"chat_id: {MOMO_FLOW_CHAT_ID or 'n/a'}\n"
            f"session_open: {int(_bist_session_open())}\n"
            f"last_scan_utc: {last_scan}\n"
            f"tracked_alerts: {n_alerts}\n"
            f"cooldown(min): {int(FLOW_COOLDOWN_SEC / 60)}\n"
            f"interval(min): {MOMO_FLOW_INTERVAL_MIN}\n"
        )
        await update.effective_message.reply_text(txt)
        return

    # TEST
    if sub == "test":
        if not MOMO_FLOW_CHAT_ID:
            await update.effective_message.reply_text("MOMO_FLOW_CHAT_ID yok. Env ayarla.")
            return
        try:
            await context.bot.send_message(
                chat_id=MOMO_FLOW_CHAT_ID,
                text="⚡️ <b>MOMO FLOW</b>\n\nTest mesajı ✅",
                parse_mode=ParseMode.HTML
            )
            await update.effective_message.reply_text("Test gönderildi ✅")
        except Exception as e:
            await update.effective_message.reply_text(f"Test hata: {e}")
        return

    # CHECK
    if sub == "check":
        if len(context.args) < 2:
            await update.effective_message.reply_text("Kullanım: /flow check VAKFN")
            return

        ticker = (context.args[1] or "").strip().upper()

        st = _load_json(FLOW_STATE_FILE, _default_flow_state())
        recent = (st.get("recent") or {}).get("by_symbol") or {}
        r = recent.get(ticker)

        if not r:
            await update.effective_message.reply_text(f"{ticker}: son taramalarda bulunamadı.")
            return

        pct = _safe_float(r.get("last_pct")) or 0.0
        delta = _safe_float(r.get("last_delta")) or 0.0
        vsp = _safe_float(r.get("last_vol_spike"))
        vol = _safe_float(r.get("last_volume")) or 0.0
        close = _safe_float(r.get("last_close")) or 0.0
        lvl = r.get("last_level") or "n/a"
        seen = r.get("last_seen_utc") or "n/a"

        pct_ok = pct >= FLOW_PCT_MIN
        delta_ok = delta >= FLOW_RADAR_DELTA_MIN
        vsp_ok = (vsp is not None and vsp >= FLOW_VOL_SPIKE_MIN)
        cap_ok = pct <= FLOW_PCT_CAP

        vsp_txt = "n/a" if vsp is None else f"{vsp:.2f}x"

        badge = "⚡"
        title = "MOMO FLOW"
        if lvl == "ROCKET":
            badge = "🚀"
        elif lvl == "RADAR":
            badge = "📡"
        elif lvl == "PRIME":
            badge = "🧠🐳"
            title = "MOMO PRIME (EARLY)"

        extra = ""
        if lvl == "PRIME":
            extra = (
                f"Δsum({FLOW_PRIME_ROLL_N}): {delta_sum:+.2f} | "
                f"VSP: {vsp_txt}\n"
            )

        txt = (
            f"{badge} {title}\n"
            f"{ticker} {pct:+.2f}%\n\n"
            f"AKIŞ: {pct:+.2f}%\n"
            f"İVME: {delta:+.2f}%\n"
            f"{extra}"
            f"VOL SPIKE: {vsp_txt}\n"
            f"HACİM: {vol:,.0f}\n"
            f"FİYAT: {close:.2f}\n"
            f"SEVİYE: {lvl}\n"
            f"last_seen_utc: {seen}\n\n"
            f"pct_ok: {int(pct_ok)}\n"
            f"delta_ok: {int(delta_ok)}\n"
            f"vol_ok: {int(vsp_ok)}\n"
            f"cap_ok: {int(cap_ok)}"
        )

        await update.effective_message.reply_text(txt)
        return
    
    # WATCH
    if sub == "watch":
        st = _load_json(FLOW_STATE_FILE, _default_flow_state())
        recent = (st.get("recent") or {}).get("by_symbol") or {}

        if not recent:
            await update.effective_message.reply_text(
            "📌 FLOW WATCH\n\nListe boş (henüz tarama yok)."
            )
            return

        is_open = _bist_session_open()
        if is_open:
            session_txt = "🟢 Canlı seans – anlık izleme"
        else:
            session_txt = "⏸️ Borsa kapalı (son snapshot)"

        items = []
        for sym, v in recent.items():
            pct = _safe_float(v.get("last_pct")) or 0.0
            lvl = (v.get("last_level") or "IZLE").upper()
            items.append((sym, pct, lvl))

        items = sorted(items, key=lambda x: x[1], reverse=True)[:5]

        lines = []
        for sym, pct, lvl in items:
            if lvl == "RADAR":
                tag = "RADAR"
            elif lvl == "PRIME":
                tag = "PRIME"
            elif lvl == "ROCKET":
                tag = "ROCKET"
            else:
                tag = "IZLE"

            lines.append(f"• {sym:<6} {pct:+.2f}%  [{tag}]")

        txt = (
            "📌 FLOW WATCH – RADAR HAVUZU\n"
            f"{session_txt}\n\n"
            + "\n".join(lines)
            + "\n\nℹ️ Not:\n"
            "Bu liste AL sinyali değildir.\n"
            "PRIME / ROCKET için seans içi teyit gerekir."
        )

        await update.effective_message.reply_text(txt)
        return

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

    st = _load_json(FLOW_STATE_FILE, _default_flow_state())
    la = _load_json(FLOW_LAST_ALERT_FILE, _default_last_alert())

    last_alert_by_symbol = la.get("last_alert_by_symbol") or {}
    recent = (st.get("recent") or {}).get("by_symbol") or {}

    rows = _tv_scan_rows()
    st["scan"]["last_scan_utc"] = _utc_now_iso()
    _save_json(FLOW_STATE_FILE, st)

    if not rows:
        logger.info("FLOW: no rows")
        return

    candidates: List[Tuple[str, float, float, float, float, Optional[float], str]] = []

    # Build candidates with pct_delta + vol_spike
    for r in rows:
        ticker = (r.get("symbol") or "").strip().upper()
        pct = float(r.get("change_pct") or 0.0)
        vol = float(r.get("volume") or 0.0)
        close = float(r.get("close") or 0.0)

        if not ticker or vol <= 0:
            continue
        
        # HARD CAP: %3 üzeri FLOW mesajı kes (ama hafızayı güncelle)
        if pct > FLOW_PCT_CAP:
            prev = recent.get(ticker) or {}
            prev_vols = prev.get("vols") or []
            prev_deltas = prev_r.get("delta_hist") or []
            recent[ticker] = {
                "last_pct": pct,
                "vols": _roll_append(prev_vols, vol, FLOW_VOL_ROLL_N),
                "last_seen_utc": _utc_now_iso()
            }
            continue
        
        prev = recent.get(ticker) or {}
        prev_pct = _safe_float(prev.get("last_pct"))
        prev_vols = prev.get("vols") or []

        pct_delta = pct - (prev_pct if prev_pct is not None else pct)
        vol_spike = _compute_vol_spike(prev_vols, vol)

        level = _level_from_metrics(pct, pct_delta, vol_spike)
        if not level:
            # Still update memory, but no alert candidate
            recent[ticker] = {
                "last_pct": pct,
                "last_delta": pct_delta,
                "last_vol_spike": vol_spike,
                "last_volume": vol,
                "last_close": close,
                "last_level": None,
                "vols": _roll_append(prev_vols, vol, FLOW_VOL_ROLL_N),
                "delta_hist": _roll_append(prev_deltas, pct_delta, FLOW_PRIME_ROLL_N),
                "last_seen_utc": _utc_now_iso()
            }
            continue

        # Candidate -> keep; update memory after
        candidates.append((ticker, pct, pct_delta, vol, close, vol_spike, level))

        recent[ticker] = {
            "last_pct": pct,
            "last_delta": pct_delta,
            "last_vol_spike": vol_spike,
            "last_volume": vol,
            "last_close": close,
            "last_level": level,
            "vols": _roll_append(prev_vols, vol, FLOW_VOL_ROLL_N),
            "last_seen_utc": _utc_now_iso()
        }

    # Persist recent memory
    st.setdefault("recent", {})
    st["recent"]["by_symbol"] = recent
    _save_json(FLOW_STATE_FILE, st)

    if not candidates:
        logger.info("FLOW: no candidates")
        return

    # Priority:
    # 1) ROCKET first
    # 2) Higher pct_delta
    # 3) Higher vol_spike
    # 4) Higher pct
    def _prio(x: Tuple[str, float, float, float, float, Optional[float], str]) -> Tuple[int, float, float, float]:
        _ticker, _pct, _delta, _vol, _close, _vsp, _lvl = x
        lvl_rank = 2 if _lvl == "ROCKET" else 1
        vsp = _vsp if _vsp is not None else 0.0
        return (lvl_rank, _delta, vsp, _pct)

    candidates.sort(key=_prio, reverse=True)

    sent = 0
    for (ticker, pct, pct_delta, vol, close, vol_spike, level) in candidates:
        if sent >= FLOW_MAX_ALERTS_PER_SCAN:
            break

        msg = _format_flow_message(ticker, pct, pct_delta, vol, close, level, vol_spike)
        mh = _hash_message(msg)

        if not _should_alert(last_alert_by_symbol, ticker, pct, mh, now_ts):
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
            "last_level": level,
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
