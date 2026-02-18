import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict, Any

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
MOMO_KILIT_SCORE_MIN = int(os.getenv("MOMO_KILIT_SCORE_MIN", "70"))
MOMO_KILIT_COOLDOWN_SEC = int(os.getenv("MOMO_KILIT_COOLDOWN_SEC", "2700"))
MOMO_KILIT_MAX_ALERTS_PER_SCAN = int(os.getenv("MOMO_KILIT_MAX_ALERTS_PER_SCAN", "1"))
MOMO_KILIT_WINDOW_MIN = int(os.getenv("MOMO_KILIT_WINDOW_MIN", "15"))

# Per-symbol cooldown (mentor kararı: watchlist'ten silme yok, cooldown var)
KILIT_SYMBOL_COOLDOWN_MIN = int(os.getenv("KILIT_SYMBOL_COOLDOWN_MIN", "240"))  # 240 dk = 4 saat

# Seans saatleri (env)
BIST_OPEN_HM = os.getenv("BIST_OPEN_HM", "10:00").strip()
BIST_CLOSE_HM = os.getenv("BIST_CLOSE_HM", "18:10").strip()

# ✅ Separate envs for KILIT (do NOT reuse FLOW envs)
TV_SCAN_URL = os.getenv("MOMO_KILIT_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = int(os.getenv("MOMO_KILIT_TV_TIMEOUT", "12"))

DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"

KILIT_STATE_FILE = os.path.join(DATA_DIR, "momo_kilit_state.json")
KILIT_LAST_ALERT_FILE = os.path.join(DATA_DIR, "momo_kilit_last_alert.json")

# PRIME watchlist dosyası (mevcut PRIME modülünle uyum için aynı klasörde)
PRIME_WATCHLIST_FILE = os.path.join(DATA_DIR, "momo_prime_watchlist.json")

# ======================
# TIME / SESSION HELPERS
# ======================
def _istanbul_now() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Istanbul"))
    except Exception:
        # Fallback: sistem saati
        return datetime.now()

def _hm_to_minutes(hm: str, default_min: int) -> int:
    try:
        parts = (hm or "").strip().split(":")
        if len(parts) != 2:
            return default_min
        h = int(parts[0])
        m = int(parts[1])
        return h * 60 + m
    except Exception:
        return default_min

def _bist_session_open() -> bool:
    now = _istanbul_now()
    wd = now.weekday()  # 0=Pzt ... 6=Paz
    if wd >= 5:
        return False
    hm = now.hour * 60 + now.minute
    open_min = _hm_to_minutes(BIST_OPEN_HM, 10 * 60)
    close_min = _hm_to_minutes(BIST_CLOSE_HM, 18 * 60 + 10)
    return open_min <= hm <= close_min

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
        "schema_version": "1.1",
        "system": "momo_kilit",
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
            "symbol_cooldown_min": KILIT_SYMBOL_COOLDOWN_MIN,
            "max_alerts_per_scan": MOMO_KILIT_MAX_ALERTS_PER_SCAN,
            "window_min": MOMO_KILIT_WINDOW_MIN,
            "bist_open_hm": BIST_OPEN_HM,
            "bist_close_hm": BIST_CLOSE_HM
        },
        "history": {}
    }

def _default_last_alert() -> dict:
    return {
        "schema_version": "1.1",
        "system": "momo_kilit",
        "cooldown_seconds": MOMO_KILIT_COOLDOWN_SEC,
        "symbol_cooldown_min": KILIT_SYMBOL_COOLDOWN_MIN,
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
    # TradingView "BIST:ASELS" gibi gelirse
    if ":" in s:
        s = s.split(":")[-1].strip()
    # Bazı durumlarda "ASELS / ..." gibi ekstralar gelebilir
    for sep in ["/", " ", "\t"]:
        if sep in s:
            s = s.split(sep)[0].strip()
    # Son güvenlik
    s = s.replace(".", "").strip()
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

# Mentor kararı: KİLİT attı diye watchlist'ten çıkarmıyoruz.
# (İstersen ileride tekrar açarız.)
def _remove_from_watchlist(symbol: str) -> None:
    _ = symbol
    return

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
# KILIT scoring (0-100)
# ==========================
def _prune_history(samples: List[dict], now_ts: float, window_min: int) -> List[dict]:
    keep_after = now_ts - (window_min * 60)
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

def _close_trend_bonus(closes: List[float]) -> float:
    """
    Close trend teyidi:
    - son 3 close art arda artıyorsa +0.20
    - son 4 close art arda artıyorsa +0.35
    - zigzag 0
    - düşüş baskınsa -0.10
    """
    if len(closes) < 4:
        return 0.0

    tail3 = closes[-3:]
    inc3 = (tail3[0] <= tail3[1] <= tail3[2])

    tail4 = closes[-4:]
    inc4 = (tail4[0] <= tail4[1] <= tail4[2] <= tail4[3])

    dec3 = (tail3[0] >= tail3[1] >= tail3[2])

    if inc4:
        return 0.35
    if inc3:
        return 0.20
    if dec3:
        return -0.10
    return 0.0

def _compute_kilit_score(samples: List[dict]) -> Tuple[int, Dict[str, str]]:
    if len(samples) < 3:
        return 0, {
            "DEVAM": "YOK",
            "CEKILME": "BILINMIYOR",
            "IVME": "BASLAMADI",
            "TUTAMAMA": "BILINMIYOR"
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

    # 2) Mini ivme: son iki tarama arası pct artışı (yardımcı)
    pct_delta = latest_pct - float(prev_pcts[-1] if prev_pcts else 0.0)

    # 2b) Close trend teyidi (asıl)
    trend_bonus = _close_trend_bonus(closes)

    # 3) Geri çekilme zayıflığı: pencere içi zirveden max drawdown
    peak = max(closes) if closes else 0.0
    dd = 0.0
    if peak > 0.0:
        dd = (peak - latest_close) / peak

    # 4) Kırılım benzeri: hacim artıyor + ivme pozitif
    break_like = (vol_spike >= 1.4) and ((pct_delta + (trend_bonus * 100.0)) >= 0.15)

    # --- Score weights ---
    score = 0.0

    # Hacim devamlılığı: 30
    score += 30.0 * max(0.0, min(1.0, vol_cont_ratio))

    # Geri çekilme zayıflığı: 25 (dd 0% => 1.0, dd 1.2%+ => 0)
    dd_norm = 1.0 - max(0.0, min(1.0, dd / 0.012))
    score += 25.0 * dd_norm

    # Mini ivme: 25 -> pct_delta yardımcı + trend teyidi
    # pct_delta 0.00 => 0, 0.20+ => 1.0
    ivme_norm = max(0.0, min(1.0, pct_delta / 0.20))
    # trend_bonus (0.35 max) ile çarpan: +%0..+%35
    ivme_with_trend = max(0.0, min(1.0, ivme_norm + trend_bonus))
    score += 25.0 * ivme_with_trend

    # Hacim spike + ivme: 20
    vol_norm = 0.0
    if vol_spike > 1.0:
        vol_norm = max(0.0, min(1.0, (vol_spike - 1.0) / 0.8))
    # Trend pozitifse destekle
    trend_factor = 1.0 if trend_bonus > 0 else 0.7
    score += 20.0 * vol_norm * (1.0 if pct_delta > 0.0 else 0.4) * trend_factor

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

    # ivme etiketi trend ile beslensin
    ivme_tag = "BASLAMADI"
    eff = pct_delta + (trend_bonus * 0.30)  # trend etkisini küçük tut
    if eff >= 0.25:
        ivme_tag = "HIZLANDI"
    elif eff >= 0.12:
        ivme_tag = "BASLADI"

    # ✅ fixed semantics: break_like => "KIRIYOR"
    tut_tag = "NORMAL"
    if break_like:
        tut_tag = "KIRIYOR"
    else:
        if vol_spike >= 1.3 and eff <= 0.05:
            tut_tag = "TOPLUYOR"

    tags = {
        "DEVAM": dev_tag,
        "CEKILME": cek_tag,
        "IVME": ivme_tag,
        "TUTAMAMA": tut_tag
    }
    return score_int, tags

def _score_level(score: int) -> str:
    if score >= 80:
        return "KİLİT AÇILDI (ROCKET YAKIN)"
    if score >= 70:
        return "KİLİT AÇILIYOR"
    return "ZAYIF"

def _score_badge(score: int) -> str:
    if score >= 80:
        return "ÇOK YÜKSEK"
    if score >= 70:
        return "YÜKSEK"
    return "DÜŞÜK"

# ==========================
# Decision / cooldown
# ==========================
def _cooldown_ok(last_ts: Optional[float], now_ts: float, cooldown_sec: int) -> bool:
    if last_ts is None:
        return True
    return (now_ts - last_ts) >= float(cooldown_sec)

def _should_alert(
    last_alert_map: dict,
    ticker: str,
    message_hash: str,
    now_ts: float
) -> bool:
    entry = (last_alert_map.get(ticker) or {})

    # 1) Mesaj hash aynıysa gönderme
    if entry.get("last_message_hash") == message_hash:
        return False

    # 2) Genel cooldown
    last_ts = _parse_utc_iso(entry.get("last_alert_utc"))
    if not _cooldown_ok(last_ts, now_ts, int(MOMO_KILIT_COOLDOWN_SEC)):
        return False

    # 3) Symbol cooldown (mentor A)
    sym_ts = _parse_utc_iso(entry.get("last_symbol_alert_utc"))
    sym_cd_sec = int(KILIT_SYMBOL_COOLDOWN_MIN) * 60
    if not _cooldown_ok(sym_ts, now_ts, sym_cd_sec):
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
        f"• <b>ÇEKİLME:</b> {tags.get('CEKILME', 'n/a')}",
        f"• <b>İVME:</b> {tags.get('IVME', 'n/a')}",
        f"• <b>TUTAMAMA:</b> {tags.get('TUTAMAMA', 'n/a')}",
    ]

    msg = (
        "🔓 <b>MOMO KİLİT</b>\n\n"
        f"<b>HİSSE:</b> {ticker}\n"
        f"<b>SKOR:</b> {badge}\n"
        f"<b>SEVİYE:</b> {level}\n\n"
        + "\n".join(tag_lines) +
        "\n\n"
        "🧠 <i>Mentor notu:</i> Kilit açılıyor. Takip + disiplin.\n"
        f"⏱ {_istanbul_now().strftime('%H:%M')}"
    )
    return msg

def _kilit_message_hash_key(ticker: str, score: int, tags: Dict[str, str]) -> str:
    # Stable hash key (time-independent)
    base = (
        f"{ticker}|{_score_level(score)}|"
        f"{tags.get('DEVAM','')}|{tags.get('CEKILME','')}|"
        f"{tags.get('IVME','')}|{tags.get('TUTAMAMA','')}"
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
            "🔓 MOMO KİLİT\n\n"
            "Komutlar:\n"
            "• /kilit status          → KİLİT durum\n"
            "• /kilit test            → test mesajı\n"
            "• /kilit watchlist       → watchlist özeti\n"
            "• /kilit check ASELS     → tek hisse debug\n\n"
            "Notlar:\n"
            "• KİLİT sadece PRIME watchlist içinden tarar.\n"
            "• Seans dışında otomatik susar.\n"
            "• Mesajlar etiketsel gelir (sayısal metrik basmaz).\n"
            f"• Seans: {BIST_OPEN_HM}-{BIST_CLOSE_HM} (TR)\n"
            f"• Symbol cooldown: {KILIT_SYMBOL_COOLDOWN_MIN} dk"
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
            "🔓 KİLİT STATUS\n\n"
            f"enabled: {int(MOMO_KILIT_ENABLED)}\n"
            f"chat_id: {MOMO_KILIT_CHAT_ID or 'n/a'}\n"
            f"session_open: {int(_bist_session_open())}\n"
            f"watchlist_count: {len(wl)}\n"
            f"last_scan_utc: {last_scan}\n"
            f"tracked_alerts: {n_alerts}\n"
            f"cooldown(min): {int(MOMO_KILIT_COOLDOWN_SEC / 60)}\n"
            f"symbol_cooldown(min): {KILIT_SYMBOL_COOLDOWN_MIN}\n"
            f"window(min): {MOMO_KILIT_WINDOW_MIN}\n"
            f"bist: {BIST_OPEN_HM}-{BIST_CLOSE_HM} TR\n"
        )
        await update.effective_message.reply_text(txt)
        return

    if sub == "watchlist":
        wl = _load_watchlist_symbols()
        if not wl:
            await update.effective_message.reply_text("📌 PRIME watchlist boş.")
            return
        head = f"📌 PRIME WATCHLIST ({len(wl)})\n\n"
        lines = []
        for s in wl[:20]:
            lines.append(f"• {s}")
        tail = ""
        if len(wl) > 20:
            tail = f"\n\n(+{len(wl) - 20} daha)"
        await update.effective_message.reply_text(head + "\n".join(lines) + tail)
        return

    if sub == "check":
        if len(context.args) < 2:
            await update.effective_message.reply_text("Kullanım: /kilit check ASELS")
            return
        ticker = _normalize_symbol(context.args[1])

        st = _load_json(KILIT_STATE_FILE, _default_kilit_state())
        history = (st.get("history") or {})
        samples = history.get(ticker) or []

        if not samples or len(samples) < 3:
            await update.effective_message.reply_text(f"{ticker}: yeterli örnek yok (min 3).")
            return

        score, tags = _compute_kilit_score(samples)
        last_seen_ts = float(samples[-1].get("ts") or 0.0)
        last_seen = datetime.fromtimestamp(last_seen_ts).strftime("%H:%M:%S")

        txt = (
            f"🔎 KİLİT CHECK – {ticker}\n\n"
            f"score_badge: {_score_badge(score)}\n"
            f"level: {_score_level(score)}\n"
            f"DEVAM: {tags.get('DEVAM')}\n"
            f"ÇEKİLME: {tags.get('CEKILME')}\n"
            f"İVME: {tags.get('IVME')}\n"
            f"TUTAMAMA: {tags.get('TUTAMAMA')}\n\n"
            f"last_seen: {last_seen}"
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
                text="🔓 <b>MOMO KİLİT</b>\n\nTest mesajı ✅",
                parse_mode=ParseMode.HTML
            )
            await update.effective_message.reply_text("Test gönderildi ✅")
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

    st.setdefault("scan", {})
    st["scan"]["last_scan_utc"] = _utc_now_iso()

    if not rows:
        _save_json(KILIT_STATE_FILE, st)
        logger.info("KILIT: no rows")
        return

    window_min = int(MOMO_KILIT_WINDOW_MIN)

    # Update history memory
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

    # Score candidates (watchlist order independent)
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

        # Alert memory update
        last_alert_by_symbol[ticker] = {
            "last_alert_utc": _utc_now_iso(),
            "last_symbol_alert_utc": _utc_now_iso(),
            "last_message_hash": mh,
            "level": _score_level(score)
        }

        # Mentor kararı: watchlist'ten çıkarma yok (cooldown ile yönetiyoruz)
        # _remove_from_watchlist(ticker)

    la["last_alert_by_symbol"] = last_alert_by_symbol
    _save_json(KILIT_LAST_ALERT_FILE, la)

    if sent > 0:
        logger.info("KILIT: sent=%d", sent)
    else:
        logger.info("KILIT: no alerts")
