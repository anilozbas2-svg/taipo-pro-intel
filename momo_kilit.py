import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict

import requests
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, ContextTypes, Application

logger = logging.getLogger("MOMO_KILIT")

# ==========================
# KILIT CONFIG (env)
# ==========================
MOMO_KILIT_ENABLED = os.getenv("MOMO_KILIT_ENABLED", "1").strip() == "1"
MOMO_KILIT_CHAT_ID = os.getenv("MOMO_KILIT_CHAT_ID", "").strip()

MOMO_KILIT_INTERVAL_MIN = int(os.getenv("MOMO_KILIT_INTERVAL_MIN", "1"))
MOMO_KILIT_SCORE_MIN = int(os.getenv("MOMO_KILIT_SCORE_MIN", "75"))
MOMO_KILIT_COOLDOWN_SEC = int(os.getenv("MOMO_KILIT_COOLDOWN_SEC", "2700"))
MOMO_KILIT_MAX_ALERTS_PER_SCAN = int(os.getenv("MOMO_KILIT_MAX_ALERTS_PER_SCAN", "1"))
MOMO_KILIT_WINDOW_MIN = int(os.getenv("MOMO_KILIT_WINDOW_MIN", "15"))

# ✅ Separate envs for KILIT (do NOT reuse FLOW envs)
TV_SCAN_URL = os.getenv("MOMO_KILIT_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = int(os.getenv("MOMO_KILIT_TV_TIMEOUT", "12"))

DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"

KILIT_STATE_FILE = os.path.join(DATA_DIR, "momo_kilit_state.json")
KILIT_LAST_ALERT_FILE = os.path.join(DATA_DIR, "momo_kilit_last_alert.json")

# PRIME watchlist dosyası (mevcut PRIME modülünle uyum için aynı klasörde tutuyoruz)
PRIME_WATCHLIST_FILE = os.path.join(DATA_DIR, "momo_prime_watchlist.json")

# ==========================
# ALTIN ADAY (DIP + TOPLAMA) CONFIG
# ==========================
KILIT_DIP_LOOKBACK_DAYS = int(os.getenv("KILIT_DIP_LOOKBACK_DAYS", "120"))
KILIT_DIP_BAND_MAX = float(os.getenv("KILIT_DIP_BAND_MAX", "0.22"))

KILIT_ACCUM_SHORT_DAYS = int(os.getenv("KILIT_ACCUM_SHORT_DAYS", "10"))
KILIT_ACCUM_LONG_DAYS = int(os.getenv("KILIT_ACCUM_LONG_DAYS", "30"))
KILIT_ACCUM_VOL_RATIO_MIN = float(os.getenv("KILIT_ACCUM_VOL_RATIO_MIN", "1.25"))
KILIT_ACCUM_NEG_DAYS_MAX = int(os.getenv("KILIT_ACCUM_NEG_DAYS_MAX", "4"))

# ======================
# SESSION HELPERS
# ======================
def _bist_session_open() -> bool:
    now = datetime.now()  # Render TR saatinde
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
        logger.exception("KILIT load_json error: %s", e)
        return default


def _save_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.exception("KILIT save_json error: %s", e)


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
def _default_kilit_state() -> dict:
    return {
        "schema_version": "2.0",
        "system": "momo_kilit_altin_aday",
        "telegram": {
            "momo_kilit_chat_id": int(MOMO_KILIT_CHAT_ID) if MOMO_KILIT_CHAT_ID else None
        },
        "scan": {
            "interval_seconds": MOMO_KILIT_INTERVAL_MIN * 60,
            "last_scan_utc": None
        },
        "rules": {
            "score_min": MOMO_KILIT_SCORE_MIN,
            "cooldown_seconds": MOMO_KILIT_COOLDOWN_SEC,
            "max_alerts_per_scan": MOMO_KILIT_MAX_ALERTS_PER_SCAN,
            "window_min": MOMO_KILIT_WINDOW_MIN,
            "dip_lookback_days": KILIT_DIP_LOOKBACK_DAYS,
            "dip_band_max": KILIT_DIP_BAND_MAX,
            "accum_short_days": KILIT_ACCUM_SHORT_DAYS,
            "accum_long_days": KILIT_ACCUM_LONG_DAYS
        },
        "history": {}
    }


def _default_last_alert() -> dict:
    return {
        "schema_version": "2.0",
        "system": "momo_kilit_altin_aday",
        "cooldown_seconds": MOMO_KILIT_COOLDOWN_SEC,
        "last_alert_by_symbol": {}
    }


def _default_watchlist() -> dict:
    return {
        "schema_version": "1.0",
        "system": "momo_prime_watchlist",
        "updated_utc": None,
        "symbols": []
    }


# ==========================
# Watchlist helpers
# ==========================
def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
    if ":" in s:
        s = s.split(":")[-1].strip()
    return s


def _load_watchlist_symbols() -> List[str]:
    wl = _load_json(PRIME_WATCHLIST_FILE, _default_watchlist())
    syms = wl.get("symbols") or []
    out: List[str] = []
    seen: set = set()
    for x in syms:
        s = _normalize_symbol(str(x))
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _remove_from_watchlist(symbol: str) -> None:
    symbol = _normalize_symbol(symbol)
    wl = _load_json(PRIME_WATCHLIST_FILE, _default_watchlist())
    syms = wl.get("symbols") or []

    new_syms: List[str] = []
    for x in syms:
        s = _normalize_symbol(str(x))
        if not s:
            continue
        if s == symbol:
            continue
        new_syms.append(s)

    wl["symbols"] = new_syms
    wl["updated_utc"] = _utc_now_iso()
    _save_json(PRIME_WATCHLIST_FILE, wl)


# ==========================
# TradingView scan (by tickers list)
# ==========================
def _tv_scan_rows_for_symbols(symbols: List[str]) -> List[dict]:
    if not symbols:
        return []

    tickers = [f"BIST:{_normalize_symbol(s)}" for s in symbols if _normalize_symbol(s)]
    if not tickers:
        return []

    payload = {
        "filter": [
            {"left": "market_cap_basic", "operation": "nempty"},
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"}
        ],
        "options": {"lang": "tr"},
        "symbols": {"tickers": tickers, "query": {"types": []}},
        "columns": ["name", "change", "volume", "close"],
        "sort": {"sortBy": "change", "sortOrder": "desc"},
        "range": [0, max(0, len(tickers) - 1)]
    }

    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        out: List[dict] = []

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
        logger.error("KILIT TV scan error: %s", e)
        return []


# ==========================
# KILIT scoring helpers
# ==========================
def _prune_history(samples: List[dict], now_ts: float, window_min: int) -> List[dict]:
    keep_after_window = now_ts - (window_min * 60)
    keep_after_lookback = now_ts - (KILIT_DIP_LOOKBACK_DAYS * 86400)
    keep_after = min(keep_after_window, keep_after_lookback)

    out: List[dict] = []
    for s in samples:
        ts = float(s.get("ts") or 0.0)
        if ts >= keep_after:
            out.append(s)
    out.sort(key=lambda x: float(x.get("ts") or 0.0))
    return out


def _safe_mean(vals: List[float]) -> float:
    if not vals:
        return 0.0
    return sum(vals) / float(len(vals))


def _dip_band_ratio(closes: List[float], lookback: int) -> Optional[float]:
    if len(closes) < max(10, lookback // 6):
        return None
    tail = closes[-lookback:] if len(closes) >= lookback else closes[:]
    lo = min(tail)
    hi = max(tail)
    if hi <= lo:
        return None
    return (tail[-1] - lo) / (hi - lo)


def _accumulation_ok(vols: List[float], pcts: List[float]) -> Tuple[bool, Dict[str, str]]:
    if len(vols) < (KILIT_ACCUM_LONG_DAYS + 2) or len(pcts) < (KILIT_ACCUM_SHORT_DAYS + 2):
        return False, {"TOPLAMA": "YOK"}

    short_vol = vols[-KILIT_ACCUM_SHORT_DAYS:]
    long_vol = vols[-KILIT_ACCUM_LONG_DAYS:]

    sv = _safe_mean([v for v in short_vol if v > 0.0])
    lv = _safe_mean([v for v in long_vol if v > 0.0])
    if lv <= 0.0:
        return False, {"TOPLAMA": "YOK"}

    vol_ratio = sv / lv

    short_pcts = pcts[-KILIT_ACCUM_SHORT_DAYS:]
    neg_days = 0
    for x in short_pcts:
        if x < 0.0:
            neg_days += 1

    ok = (vol_ratio >= KILIT_ACCUM_VOL_RATIO_MIN) and (neg_days <= KILIT_ACCUM_NEG_DAYS_MAX)

    if ok:
        return True, {"TOPLAMA": "VAR"}
    return False, {"TOPLAMA": "ZAYIF"}


def _compute_kilit_score(samples: List[dict]) -> Tuple[int, Dict[str, str]]:
    if len(samples) < 3:
        return 0, {
            "DEVAM": "YOK",
            "CEKILME": "BILINMIYOR",
            "IVME": "BASLAMADI",
            "TUTAMAMA": "BILINMIYOR",
            "DIP": "BILINMIYOR",
            "TOPLAMA": "YOK"
        }

    closes = [float(s.get("close") or 0.0) for s in samples]
    vols = [float(s.get("vol") or 0.0) for s in samples]
    pcts = [float(s.get("pct") or 0.0) for s in samples]

    latest_close = closes[-1]
    latest_vol = vols[-1]
    latest_pct = pcts[-1]

    prev_vols = vols[:-1]
    prev_pcts = pcts[:-1]

    mean_prev_vol = _safe_mean([v for v in prev_vols if v > 0.0])
    vol_spike = 0.0
    if mean_prev_vol > 0.0:
        vol_spike = latest_vol / mean_prev_vol

    # 1) Hacim devamlılığı
    if mean_prev_vol <= 0.0:
        vol_cont_ratio = 0.0
    else:
        above = 0
        tail = vols[-5:]
        for v in tail:
            if v >= mean_prev_vol * 0.9:
                above += 1
        vol_cont_ratio = above / float(len(tail))

    # 2) Mini ivme: son iki tarama arası pct artışı
    pct_delta = latest_pct - float(prev_pcts[-1] if prev_pcts else 0.0)

    # 3) Geri çekilme zayıflığı: pencere içi zirveden max drawdown
    peak = max(closes) if closes else 0.0
    dd = 0.0
    if peak > 0.0:
        dd = (peak - latest_close) / peak

    # 4) Kırılım benzeri: hacim artıyor + ivme pozitif
    break_like = (vol_spike >= 1.4) and (pct_delta >= 0.15)

    # --- ALTIN ADAY components ---
    dip_ratio = _dip_band_ratio(closes, KILIT_DIP_LOOKBACK_DAYS)
    acc_ok, acc_tag = _accumulation_ok(vols, pcts)

    # --- Score weights ---
    # Base score (80) + Dip (20) + Toplama (20) => theoretical max 120, then clipped to 100
    score = 0.0

    # Hacim devamlılığı: 30
    score += 30.0 * max(0.0, min(1.0, vol_cont_ratio))

    # Geri çekilme zayıflığı: 25 (dd 0% => 1.0, dd 1.2%+ => 0)
    dd_norm = 1.0 - max(0.0, min(1.0, dd / 0.012))
    score += 25.0 * dd_norm

    # Mini ivme: 25 (pct_delta 0.00 => 0, 0.20+ => 1.0)
    ivme_norm = max(0.0, min(1.0, pct_delta / 0.20))
    score += 25.0 * ivme_norm

    # Hacim spike + ivme: 20
    vol_norm = 0.0
    if vol_spike > 1.0:
        vol_norm = max(0.0, min(1.0, (vol_spike - 1.0) / 0.8))
    score += 20.0 * vol_norm * (1.0 if pct_delta > 0.0 else 0.4)

    # 5) DIP teyidi: 20 puan
    dip_score = 0.0
    dip_tag = "BILINMIYOR"
    if dip_ratio is not None:
        if dip_ratio <= KILIT_DIP_BAND_MAX:
            dip_score = 20.0
            dip_tag = "DIPTE"
        elif dip_ratio <= (KILIT_DIP_BAND_MAX + 0.10):
            dip_score = 10.0
            dip_tag = "YAKIN"
        else:
            dip_score = 0.0
            dip_tag = "UZAK"
    score += dip_score

    # 6) TOPLAMA teyidi: 20 puan
    top_score = 0.0
    if acc_ok:
        top_score = 20.0
    elif acc_tag.get("TOPLAMA") == "ZAYIF":
        top_score = 8.0
    score += top_score

    score_int = int(round(max(0.0, min(100.0, score))))

    # --- Etiketler (sayısal yok) ---
    dev_tag = "ZAYIF"
    if vol_cont_ratio >= 0.8:
        dev_tag = "GUCLU"
    elif vol_cont_ratio >= 0.5:
        dev_tag = "ORTA"

    cek_tag = "SERT"
    if dd <= 0.004:
        cek_tag = "ZAYIF"
    elif dd <= 0.008:
        cek_tag = "ORTA"

    ivme_tag = "BASLAMADI"
    if pct_delta >= 0.25:
        ivme_tag = "HIZLANDI"
    elif pct_delta >= 0.12:
        ivme_tag = "BASLADI"

    # ✅ fixed semantics: break_like => "KIRIYOR" (not "TUTAMIYOR")
    tut_tag = "NORMAL"
    if break_like:
        tut_tag = "KIRIYOR"
    else:
        if vol_spike >= 1.3 and pct_delta <= 0.05:
            tut_tag = "TOPLUYOR"

    tags = {
        "DEVAM": dev_tag,
        "CEKILME": cek_tag,
        "IVME": ivme_tag,
        "TUTAMAMA": tut_tag,
        "DIP": dip_tag,
        "TOPLAMA": acc_tag.get("TOPLAMA", "n/a")
    }
    return score_int, tags


def _score_level(score: int) -> str:
    if score >= 85:
        return "ALTIN ADAY – COK GUCLU"
    if score >= 75:
        return "ALTIN ADAY"
    if score >= 70:
        return "KILIT ACIYOR (ZAYIF ALTIN)"
    return "ZAYIF"


def _score_badge(score: int) -> str:
    if score >= 85:
        return "COK YUKSEK"
    if score >= 75:
        return "YUKSEK"
    if score >= 70:
        return "ORTA"
    return "DUSUK"


# ==========================
# Decision / cooldown
# ==========================
def _cooldown_ok(last_ts: Optional[float], now_ts: float) -> bool:
    if last_ts is None:
        return True
    return (now_ts - last_ts) >= float(MOMO_KILIT_COOLDOWN_SEC)


def _should_alert(last_alert_map: dict, ticker: str, message_hash: str, now_ts: float) -> bool:
    entry = (last_alert_map.get(ticker) or {})
    last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
    if not _cooldown_ok(last_ts, now_ts):
        return False
    if entry.get("last_message_hash") == message_hash:
        return False
    return True


# ==========================
# Message formatting (NO numeric metrics)
# ==========================
def _format_kilit_message(ticker: str, score: int, tags: Dict[str, str]) -> str:
    level = _score_level(score)
    badge = _score_badge(score)

    tag_lines = [
        f"• <b>DEVAM:</b> {tags.get('DEVAM', 'n/a')}",
        f"• <b>CEKILME:</b> {tags.get('CEKILME', 'n/a')}",
        f"• <b>IVME:</b> {tags.get('IVME', 'n/a')}",
        f"• <b>TUTAMAMA:</b> {tags.get('TUTAMAMA', 'n/a')}",
        f"• <b>DIP:</b> {tags.get('DIP', 'n/a')}",
        f"• <b>TOPLAMA:</b> {tags.get('TOPLAMA', 'n/a')}",
    ]

    msg = (
        "🔓 <b>MOMO KILIT</b>\n\n"
        f"<b>HISSE:</b> {ticker}\n"
        f"<b>SKOR:</b> {badge}\n"
        f"<b>SEVIYE:</b> {level}\n\n"
        + "\n".join(tag_lines) +
        "\n\n"
        "🧠 <i>Mentor notu:</i> Dip + toplama teyidi gelmeden agresif girme. Izle + disiplin.\n"
        f"⏱ {datetime.now().strftime('%H:%M')}"
    )
    return msg


def _kilit_message_hash_key(ticker: str, score: int, tags: Dict[str, str]) -> str:
    # Stable hash key (time-independent)
    base = (
        f"{ticker}|{_score_level(score)}|"
        f"{tags.get('DEVAM','')}|{tags.get('CEKILME','')}|"
        f"{tags.get('IVME','')}|{tags.get('TUTAMAMA','')}|"
        f"{tags.get('DIP','')}|{tags.get('TOPLAMA','')}"
    )
    return _hash_message(base)


# ==========================
# /kilit command
# ==========================
async def cmd_kilit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sub = ""
    if context.args:
        sub = (context.args[0] or "").strip().lower()

    if sub in ("help", ""):
        txt = (
            "🔓 MOMO KILIT (ALTIN ADAY)\n\n"
            "Komutlar:\n"
            "• /kilit status  → KILIT durum\n"
            "• /kilit test    → test mesaji\n\n"
            "Notlar:\n"
            "• KILIT sadece PRIME watchlist icinden tarar.\n"
            "• Seans disinda otomatik susar.\n"
            "• Mesajlar etiketsel gelir (sayisal metrik basmaz).\n"
            "• DIP + TOPLAMA teyidi ile ALTIN ADAY secmeye calisir."
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "status":
        st = _load_json(KILIT_STATE_FILE, _default_kilit_state())
        la = _load_json(KILIT_LAST_ALERT_FILE, _default_last_alert())
        last_scan = ((st.get("scan") or {}).get("last_scan_utc")) or "n/a"
        n_alerts = len((la.get("last_alert_by_symbol") or {}))
        wl = _load_watchlist_symbols()
        txt = (
            "🔓 KILIT STATUS\n\n"
            f"enabled: {int(MOMO_KILIT_ENABLED)}\n"
            f"chat_id: {MOMO_KILIT_CHAT_ID or 'n/a'}\n"
            f"watchlist_count: {len(wl)}\n"
            f"last_scan_utc: {last_scan}\n"
            f"tracked_alerts: {n_alerts}\n"
            f"cooldown(min): {int(MOMO_KILIT_COOLDOWN_SEC / 60)}\n"
            f"window(min): {MOMO_KILIT_WINDOW_MIN}\n"
            f"score_min: {MOMO_KILIT_SCORE_MIN}\n"
            f"dip_lookback_days: {KILIT_DIP_LOOKBACK_DAYS}\n"
            f"dip_band_max: {KILIT_DIP_BAND_MAX}\n"
            f"accum_short_days: {KILIT_ACCUM_SHORT_DAYS}\n"
            f"accum_long_days: {KILIT_ACCUM_LONG_DAYS}\n"
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "test":
        if not MOMO_KILIT_CHAT_ID:
            await update.effective_message.reply_text("MOMO_KILIT_CHAT_ID yok. Env ayarla.")
            return
        try:
            await context.bot.send_message(
                chat_id=MOMO_KILIT_CHAT_ID,
                text="🔓 <b>MOMO KILIT</b>\n\nTest mesaji ✅",
                parse_mode=ParseMode.HTML
            )
            await update.effective_message.reply_text("Test gonderildi ✅")
        except Exception as e:
            await update.effective_message.reply_text(f"Test hata: {e}")
        return

    await update.effective_message.reply_text("Bilinmeyen alt komut. /kilit help")


def register_momo_kilit(app: Application) -> None:
    app.add_handler(CommandHandler("kilit", cmd_kilit))


# ==========================
# Scheduled job
# ==========================
async def job_momo_kilit_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _bist_session_open():
        return
    if not MOMO_KILIT_ENABLED:
        return
    if not MOMO_KILIT_CHAT_ID:
        return

    now_ts = time.time()

    watch_syms = _load_watchlist_symbols()
    if not watch_syms:
        return

    st = _load_json(KILIT_STATE_FILE, _default_kilit_state())
    la = _load_json(KILIT_LAST_ALERT_FILE, _default_last_alert())
    last_alert_by_symbol = la.get("last_alert_by_symbol") or {}
    history = (st.get("history") or {})

    rows = _tv_scan_rows_for_symbols(watch_syms)

    st["scan"]["last_scan_utc"] = _utc_now_iso()
    _save_json(KILIT_STATE_FILE, st)

    if not rows:
        logger.info("KILIT: no rows")
        return

    window_min = int(MOMO_KILIT_WINDOW_MIN)
    for r in rows:
        ticker = _normalize_symbol(str(r.get("symbol") or ""))
        if not ticker:
            continue

        close = float(r.get("close") or 0.0)
        vol = float(r.get("volume") or 0.0)
        pct = float(r.get("change_pct") or 0.0)

        samples = history.get(ticker) or []
        samples.append({
            "ts": now_ts,
            "close": close,
            "vol": vol,
            "pct": pct
        })
        samples = _prune_history(samples, now_ts, window_min)
        history[ticker] = samples

    st["history"] = history
    _save_json(KILIT_STATE_FILE, st)

    candidates: List[Tuple[str, int, Dict[str, str]]] = []
    for ticker in watch_syms:
        t = _normalize_symbol(ticker)
        samples = history.get(t) or []
        score, tags = _compute_kilit_score(samples)
        if score >= int(MOMO_KILIT_SCORE_MIN):
            candidates.append((t, score, tags))

    if not candidates:
        logger.info("KILIT: no candidates")
        return

    candidates.sort(key=lambda x: x[1], reverse=True)

    sent = 0
    for (ticker, score, tags) in candidates:
        if sent >= int(MOMO_KILIT_MAX_ALERTS_PER_SCAN):
            break

        msg = _format_kilit_message(ticker, score, tags)
        mh = _kilit_message_hash_key(ticker, score, tags)

        if not _should_alert(last_alert_by_symbol, ticker, mh, now_ts):
            continue

        try:
            await context.bot.send_message(
                chat_id=MOMO_KILIT_CHAT_ID,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            sent += 1
        except Exception as e:
            logger.error("KILIT send error: %s", e)
            continue

        last_alert_by_symbol[ticker] = {
            "last_alert_utc": _utc_now_iso(),
            "last_message_hash": mh,
            "level": _score_level(score)
        }

        # 🔥 Spam sıfır: KILIT attıysa watchlist’ten çıkar
        _remove_from_watchlist(ticker)

    la["last_alert_by_symbol"] = last_alert_by_symbol
    _save_json(KILIT_LAST_ALERT_FILE, la)

    if sent > 0:
        logger.info("KILIT: sent=%d", sent)
    else:
        logger.info("KILIT: no alerts")
