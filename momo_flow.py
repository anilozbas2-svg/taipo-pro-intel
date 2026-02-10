import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Optional, List, Tuple, Dict

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

FLOW_TOP_N = int(os.getenv("MOMO_FLOW_TOP_N", "200"))
FLOW_MAX_ALERTS_PER_SCAN = int(os.getenv("MOMO_FLOW_MAX_ALERTS_PER_SCAN", "3"))

TV_SCAN_URL = os.getenv("MOMO_FLOW_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = int(os.getenv("MOMO_FLOW_TV_TIMEOUT", "12"))

DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
FLOW_STATE_FILE = os.path.join(DATA_DIR, "momo_flow_state.json")
FLOW_LAST_ALERT_FILE = os.path.join(DATA_DIR, "momo_flow_last_alert.json")

# Rolling volume memory (per symbol)
FLOW_VOL_ROLL_N = int(os.getenv("MOMO_FLOW_VOL_ROLL_N", "10"))

# Hard cap: pct > cap ise mesaj atma (anti-spam)
FLOW_PCT_CAP = float(os.getenv("MOMO_FLOW_PCT_CAP", "6.00"))

# Per-symbol anti-spam
FLOW_COOLDOWN_SEC = int(os.getenv("MOMO_FLOW_COOLDOWN_SEC", "1200"))  # 20 min
FLOW_STEPUP_PCT = float(os.getenv("MOMO_FLOW_STEPUP_PCT", "0.40"))  # cooldown içindeyken ancak bu kadar artarsa

# Global anti-spam (saatlik limit)
FLOW_HOURLY_LIMIT = int(os.getenv("MOMO_FLOW_HOURLY_LIMIT", "24"))  # saat başı max mesaj
FLOW_HOURLY_WINDOW_SEC = 3600

# ==========================
# TARGET SYSTEM (SPARK -> ROCKET)
# ==========================
# 1) SPARK = hedef bandın (0.30-0.80). Ama "hacim kıpırdattı mı?" teyidiyle
FLOW_PCT_SPARK_MIN = float(os.getenv("MOMO_FLOW_PCT_SPARK_MIN", "0.30"))
FLOW_PCT_SPARK_MAX = float(os.getenv("MOMO_FLOW_PCT_SPARK_MAX", "0.80"))
FLOW_SPARK_DELTA_MIN = float(os.getenv("MOMO_FLOW_SPARK_DELTA_MIN", "0.08"))
FLOW_VOL_SPIKE_SPARK_MIN = float(os.getenv("MOMO_FLOW_VOL_SPIKE_SPARK_MIN", "1.35"))

# 2) EARLY = 0.80+ üstü, ama hala "erken yakala" (0.80-1.20 civarı)
FLOW_PCT_EARLY_MIN = float(os.getenv("MOMO_FLOW_PCT_EARLY_MIN", "0.80"))
FLOW_EARLY_DELTA_MIN = float(os.getenv("MOMO_FLOW_EARLY_DELTA_MIN", "0.15"))
FLOW_VOL_SPIKE_EARLY_MIN = float(os.getenv("MOMO_FLOW_VOL_SPIKE_EARLY_MIN", "1.25"))

# 3) RADAR = 1.20+ (daha net)
FLOW_PCT_RADAR_MIN = float(os.getenv("MOMO_FLOW_PCT_RADAR_MIN", "1.20"))
FLOW_RADAR_DELTA_MIN = float(os.getenv("MOMO_FLOW_RADAR_DELTA_MIN", "0.25"))
FLOW_VOL_SPIKE_RADAR_MIN = float(os.getenv("MOMO_FLOW_VOL_SPIKE_RADAR_MIN", "1.20"))

# 4) ROCKET = 2.50+
FLOW_PCT_ROCKET_MIN = float(os.getenv("MOMO_FLOW_PCT_ROCKET_MIN", "2.50"))
FLOW_ROCKET_DELTA_MIN = float(os.getenv("MOMO_FLOW_ROCKET_DELTA_MIN", "0.55"))
FLOW_VOL_SPIKE_ROCKET_MIN = float(os.getenv("MOMO_FLOW_VOL_SPIKE_ROCKET_MIN", "1.60"))

LEVEL_RANK = {
    "SPARK": 1,
    "EARLY": 2,
    "RADAR": 3,
    "ROCKET": 4
}

# ======================
# SESSION HELPERS
# ======================
def _istanbul_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Istanbul"))
    except Exception:
        return datetime.now()


def _bist_session_open() -> bool:
    now = _istanbul_now()
    wd = now.weekday()  # 0=Pzt ... 6=Paz
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


# ==========================
# Defaults (isolated state)
# ==========================
def _default_flow_state() -> dict:
    return {
        "schema_version": "3.0",
        "system": "momo_flow_spark_radar_rocket",
        "telegram": {
            "momo_flow_chat_id": int(MOMO_FLOW_CHAT_ID) if MOMO_FLOW_CHAT_ID else None
        },
        "scan": {
            "interval_seconds": MOMO_FLOW_INTERVAL_MIN * 60,
            "last_scan_utc": None
        },
        "rate": {
            "window_start_utc": None,
            "sent_in_window": 0
        },
        "recent": {
            "by_symbol": {}
        }
    }


def _default_last_alert() -> dict:
    return {
        "schema_version": "3.0",
        "system": "momo_flow_spark_radar_rocket",
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
# Rolling helpers
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


# ==========================
# Rate limit (global hourly)
# ==========================
def _rate_window_ok(st: dict, now_ts: float) -> bool:
    rate = st.get("rate") or {}
    ws = _parse_utc_iso(rate.get("window_start_utc"))
    sent = int(rate.get("sent_in_window") or 0)

    if ws is None or (now_ts - ws) >= FLOW_HOURLY_WINDOW_SEC:
        rate["window_start_utc"] = _utc_now_iso()
        rate["sent_in_window"] = 0
        st["rate"] = rate
        return True

    return sent < FLOW_HOURLY_LIMIT


def _rate_window_inc(st: dict) -> None:
    rate = st.get("rate") or {}
    sent = int(rate.get("sent_in_window") or 0)
    rate["sent_in_window"] = sent + 1
    st["rate"] = rate


# ==========================
# Level selection
# ==========================
def _pick_level(pct: float, pct_delta: float, vol_spike: Optional[float]) -> Optional[str]:
    if pct > FLOW_PCT_CAP:
        return None

    vs = vol_spike if vol_spike is not None else 0.0

    # Highest first
    if pct >= FLOW_PCT_ROCKET_MIN and pct_delta >= FLOW_ROCKET_DELTA_MIN and vs >= FLOW_VOL_SPIKE_ROCKET_MIN:
        return "ROCKET"

    if pct >= FLOW_PCT_RADAR_MIN and pct_delta >= FLOW_RADAR_DELTA_MIN and vs >= FLOW_VOL_SPIKE_RADAR_MIN:
        return "RADAR"

    if pct >= FLOW_PCT_EARLY_MIN and pct_delta >= FLOW_EARLY_DELTA_MIN and vs >= FLOW_VOL_SPIKE_EARLY_MIN:
        return "EARLY"

    # SPARK band: 0.30 - 0.80
    if FLOW_PCT_SPARK_MIN <= pct <= FLOW_PCT_SPARK_MAX and pct_delta >= FLOW_SPARK_DELTA_MIN and vs >= FLOW_VOL_SPIKE_SPARK_MIN:
        return "SPARK"

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
    elif level == "RADAR":
        head = "📡 <b>MOMO FLOW – RADAR</b>"
        note = "🧠 <i>Mentor notu:</i> Akış netleşiyor. İzle + teyit et."
    elif level == "EARLY":
        head = "🟡 <b>MOMO FLOW – EARLY</b>"
        note = "🧠 <i>Mentor notu:</i> Kıvılcım büyüyor. İzleme listesi aç."
    else:
        head = "✨ <b>MOMO FLOW – SPARK</b>"
        note = "🧠 <i>Mentor notu:</i> Hacim kıpırdadı. Devam teyidi bekle."

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
# Decision (cooldown + step-up + level-up)
# ==========================
def _cooldown_ok(last_alert_ts: Optional[float], now_ts: float) -> bool:
    if last_alert_ts is None:
        return True
    return (now_ts - last_alert_ts) >= FLOW_COOLDOWN_SEC


def _should_alert(
    last_alert_map: dict,
    ticker: str,
    pct: float,
    level: str,
    message_hash: str,
    now_ts: float
) -> bool:
    entry = (last_alert_map.get(ticker) or {})
    last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
    last_pct = _safe_float(entry.get("last_alert_pct"))
    last_level = (entry.get("last_level") or "").upper()

    if entry.get("last_message_hash") == message_hash:
        return False

    # Cooldown bittiyse -> gönder
    if _cooldown_ok(last_ts, now_ts):
        return True

    # Cooldown içindeyken:
    # 1) Seviye yükseldiyse (SPARK->EARLY->RADAR->ROCKET) gönder
    if last_level in LEVEL_RANK and level in LEVEL_RANK:
        if LEVEL_RANK[level] > LEVEL_RANK[last_level]:
            return True

    # 2) Yüzde step-up olduysa gönder
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

    if sub in ("help", ""):
        txt = (
            "⚡️ MOMO FLOW – SPARK → ROCKET\n\n"
            "Komutlar:\n"
            "• /flow status        → durum\n"
            "• /flow test          → test mesajı\n"
            "• /flow check X       → tek hisse kontrol\n"
            "• /flow watch         → izleme listesi\n\n"
            f"Tarama: {MOMO_FLOW_INTERVAL_MIN} dk | TopN: {FLOW_TOP_N}\n"
            f"SPARK: {FLOW_PCT_SPARK_MIN:.2f}-{FLOW_PCT_SPARK_MAX:.2f}% | Δ≥{FLOW_SPARK_DELTA_MIN:.2f} | VSP≥{FLOW_VOL_SPIKE_SPARK_MIN:.2f}\n"
            f"EARLY: pct≥{FLOW_PCT_EARLY_MIN:.2f} | Δ≥{FLOW_EARLY_DELTA_MIN:.2f} | VSP≥{FLOW_VOL_SPIKE_EARLY_MIN:.2f}\n"
            f"RADAR: pct≥{FLOW_PCT_RADAR_MIN:.2f} | Δ≥{FLOW_RADAR_DELTA_MIN:.2f} | VSP≥{FLOW_VOL_SPIKE_RADAR_MIN:.2f}\n"
            f"ROCKET: pct≥{FLOW_PCT_ROCKET_MIN:.2f} | Δ≥{FLOW_ROCKET_DELTA_MIN:.2f} | VSP≥{FLOW_VOL_SPIKE_ROCKET_MIN:.2f}\n"
            f"Cooldown: {int(FLOW_COOLDOWN_SEC / 60)} dk | Max/scan: {FLOW_MAX_ALERTS_PER_SCAN}\n"
            f"Hourly limit: {FLOW_HOURLY_LIMIT}\n"
            "BIST kapalıysa FLOW susar."
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "status":
        st = _load_json(FLOW_STATE_FILE, _default_flow_state())
        la = _load_json(FLOW_LAST_ALERT_FILE, _default_last_alert())
        last_scan = ((st.get("scan") or {}).get("last_scan_utc")) or "n/a"
        n_alerts = len((la.get("last_alert_by_symbol") or {}))
        rate = st.get("rate") or {}
        txt = (
            "⚡️ FLOW STATUS\n\n"
            f"enabled: {int(MOMO_FLOW_ENABLED)}\n"
            f"chat_id: {MOMO_FLOW_CHAT_ID or 'n/a'}\n"
            f"session_open: {int(_bist_session_open())}\n"
            f"last_scan_utc: {last_scan}\n"
            f"tracked_alerts: {n_alerts}\n"
            f"cooldown(min): {int(FLOW_COOLDOWN_SEC / 60)}\n"
            f"interval(min): {MOMO_FLOW_INTERVAL_MIN}\n"
            f"hourly_limit: {FLOW_HOURLY_LIMIT}\n"
            f"sent_in_window: {int(rate.get('sent_in_window') or 0)}\n"
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
                text="⚡️ <b>MOMO FLOW</b>\n\nTest mesajı ✅",
                parse_mode=ParseMode.HTML
            )
            await update.effective_message.reply_text("Test gönderildi ✅")
        except Exception as e:
            await update.effective_message.reply_text(f"Test hata: {e}")
        return

    if sub == "check":
        if len(context.args) < 2:
            await update.effective_message.reply_text("Kullanım: /flow check VAKFN")
            return

        ticker = (context.args[1] or "").strip().upper()
        st = _load_json(FLOW_STATE_FILE, _default_flow_state())
        recent = (st.get("recent") or {}).get("by_symbol") or {}
        r = recent.get(ticker)

        if not r:
            await update.effective_message.reply_text(f"{ticker}: son taramalarda yok.")
            return

        pct = _safe_float(r.get("last_pct")) or 0.0
        delta = _safe_float(r.get("last_delta")) or 0.0
        vsp = _safe_float(r.get("last_vol_spike"))
        vol = _safe_float(r.get("last_volume")) or 0.0
        close = _safe_float(r.get("last_close")) or 0.0
        lvl = (r.get("last_level") or "n/a").upper()
        seen = r.get("last_seen_utc") or "n/a"

        vsp_txt = "n/a" if vsp is None else f"{vsp:.2f}x"

        txt = (
            f"⚡️ FLOW CHECK – {ticker}\n\n"
            f"AKIŞ: {pct:+.2f}%\n"
            f"İVME: {delta:+.2f}%\n"
            f"VOL SPIKE: {vsp_txt}\n"
            f"HACİM: {vol:,.0f}\n"
            f"FİYAT: {close:.2f}\n"
            f"SEVİYE: {lvl}\n"
            f"last_seen_utc: {seen}"
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "watch":
        st = _load_json(FLOW_STATE_FILE, _default_flow_state())
        recent = (st.get("recent") or {}).get("by_symbol") or {}

        items = []
        for sym, v in recent.items():
            pct = _safe_float(v.get("last_pct"))
            if pct is None:
                continue
            lvl = (v.get("last_level") or "-").upper()
            items.append((pct, sym, lvl))

        if not items:
            await update.effective_message.reply_text("📌 FLOW WATCH\n\nListe boş (state yok).")
            return

        items.sort(key=lambda x: x[0], reverse=True)
        top = items[:12]

        lines = [f"• {sym:<6} {pct:+.2f}%  [{lvl}]" for (pct, sym, lvl) in top]
        txt = "📌 FLOW WATCH – En yüksek akışlar\n\n" + "\n".join(lines)
        await update.effective_message.reply_text(txt)
        return

    await update.effective_message.reply_text("Bilinmeyen alt komut. /flow help")


def register_momo_flow(app: Application) -> None:
    app.add_handler(CommandHandler("flow", cmd_flow))


# ==========================
# Scheduled job
# ==========================
async def job_momo_flow_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
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

    if not rows:
        _save_json(FLOW_STATE_FILE, st)
        logger.info("FLOW: no rows")
        return

    candidates: List[Tuple[str, float, float, float, float, Optional[float], str]] = []

    for r in rows:
        ticker = (r.get("symbol") or "").strip().upper()
        pct = float(r.get("change_pct") or 0.0)
        vol = float(r.get("volume") or 0.0)
        close = float(r.get("close") or 0.0)

        if not ticker or vol <= 0:
            continue

        prev = recent.get(ticker) or {}
        prev_pct = _safe_float(prev.get("last_pct"))
        prev_vols = prev.get("vols") or []

        pct_delta = pct - (prev_pct if prev_pct is not None else pct)
        vol_spike = _compute_vol_spike(prev_vols, vol)

        # memory update (always)
        base_mem = {
            "last_pct": pct,
            "last_delta": pct_delta,
            "last_vol_spike": vol_spike,
            "last_volume": vol,
            "last_close": close,
            "vols": _roll_append(prev_vols, vol, FLOW_VOL_ROLL_N),
            "last_seen_utc": _utc_now_iso()
        }

        # Hard cap: mesaj yok ama hafıza var
        if pct > FLOW_PCT_CAP:
            base_mem["last_level"] = None
            recent[ticker] = base_mem
            continue

        level = _pick_level(pct, pct_delta, vol_spike)
        base_mem["last_level"] = level
        recent[ticker] = base_mem

        if not level:
            continue

        candidates.append((ticker, pct, pct_delta, vol, close, vol_spike, level))

    st.setdefault("recent", {})
    st["recent"]["by_symbol"] = recent
    _save_json(FLOW_STATE_FILE, st)

    if not candidates:
        logger.info("FLOW: no candidates")
        return

    # Priority: higher level, higher pct_delta, higher vol_spike, higher pct
    def _prio(x: Tuple[str, float, float, float, float, Optional[float], str]) -> Tuple[int, float, float, float]:
        _ticker, _pct, _delta, _vol, _close, _vsp, _lvl = x
        rank = LEVEL_RANK.get(_lvl, 0)
        vsp = _vsp if _vsp is not None else 0.0
        return (rank, _delta, vsp, _pct)

    candidates.sort(key=_prio, reverse=True)

    sent = 0
    for (ticker, pct, pct_delta, vol, close, vol_spike, level) in candidates:
        if sent >= FLOW_MAX_ALERTS_PER_SCAN:
            break

        # Global hourly limit
        if not _rate_window_ok(st, now_ts):
            break

        msg = _format_flow_message(ticker, pct, pct_delta, vol, close, level, vol_spike)
        mh = _hash_message(msg)

        if not _should_alert(last_alert_by_symbol, ticker, pct, level, mh, now_ts):
            continue

        try:
            await context.bot.send_message(
                chat_id=MOMO_FLOW_CHAT_ID,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            sent += 1
            _rate_window_inc(st)
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
    _save_json(FLOW_STATE_FILE, st)

    if sent > 0:
        logger.info("FLOW: sent=%d", sent)
    else:
        logger.info("FLOW: no alerts")
