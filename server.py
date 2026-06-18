"""
Backend для виджета «Таблица фьючерсов» Т-Инвестиции.
Использует REST API T-Invest (без SDK).
Токен читается из переменной окружения TINKOFF_INVEST_TOKEN.

Серверное кэширование v5:
- Фоновый поток обновляет данные по активным инструментам
- Все пользователи получают данные из общего кэша
- Лимит: 18 инструментов (ограничение API: 600 запросов/мин)
- Интервал: 2 сек (аукцион) / 60 сек (обычное время)
"""
import logging
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, request, send_from_directory
import requests
import urllib3

# Отключаем предупреждения об SSL (для локального тестирования)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== Конфигурация ==========
# Лимиты T-Invest API: 600 запросов/мин на сервис котировок
# При 4 сек интервале: 15 обновлений/мин × N инструментов ≤ 600 → N ≈ 40
# С небольшим запасом используем 40 инструментов
MAX_CACHED_INSTRUMENTS = 40
BACKGROUND_INTERVAL_AUCTION = 4      # 4 секунды во время аукциона
BACKGROUND_INTERVAL_NORMAL = 3600    # 60 минут вне аукциона
ACTIVE_INSTRUMENT_TTL = 300          # 5 минут - инструмент считается активным

# ========== Кэширование ==========
# TTL в секундах
CACHE_TTL_FUTURES = 300  # 5 минут - список фьючерсов редко меняется
CACHE_TTL_CANDLES = 60   # 60 секунд - свечи обновляются фоновым потоком

_cache = {}
_cache_lock = threading.Lock()

# ========== Серверный кэш данных ==========
# Хранит данные стакана и свечей для активных инструментов
_server_cache = {
    "orderbook": {},      # {instrument_id: {data, updated_at}}
    "candles": {},        # {instrument_id: {data, updated_at}}
    "active": {},         # {instrument_id: last_requested_at}
}
_server_cache_lock = threading.Lock()
_background_thread = None
_background_running = False

# ========== Статистика запросов ==========
STATS_WINDOW_SECONDS = 300  # 5 минут
_stats = {
    "requests": [],  # [(timestamp, endpoint, session_id), ...]
    "sessions": {},  # {session_id: last_seen_timestamp}
}
_stats_lock = threading.Lock()

# ========== Очередь браузерных уведомлений ==========
_notify_queue = []   # [{"id": str, "msg": str, "ts": float}]
_notify_lock = threading.Lock()


def _record_request(endpoint, session_id=None):
    """Записать запрос в статистику."""
    now = time.time()
    with _stats_lock:
        _stats["requests"].append((now, endpoint, session_id))
        if session_id:
            _stats["sessions"][session_id] = now
        # Очистка старых записей
        cutoff = now - STATS_WINDOW_SECONDS
        _stats["requests"] = [(t, e, s) for t, e, s in _stats["requests"] if t > cutoff]
        _stats["sessions"] = {s: t for s, t in _stats["sessions"].items() if t > cutoff}


def _get_stats():
    """Получить статистику за последние 5 минут."""
    now = time.time()
    cutoff = now - STATS_WINDOW_SECONDS
    with _stats_lock:
        recent_requests = [(t, e, s) for t, e, s in _stats["requests"] if t > cutoff]
        active_sessions = {s: t for s, t in _stats["sessions"].items() if t > cutoff}
    
    # Группировка по endpoint
    by_endpoint = {}
    for _, endpoint, _ in recent_requests:
        by_endpoint[endpoint] = by_endpoint.get(endpoint, 0) + 1
    
    return {
        "total_requests_5min": len(recent_requests),
        "unique_sessions_5min": len(active_sessions),
        "requests_by_endpoint": by_endpoint,
        "window_seconds": STATS_WINDOW_SECONDS,
    }


def _cache_get(key):
    """Получить значение из кэша, если не истёк TTL."""
    with _cache_lock:
        item = _cache.get(key)
        if item is None:
            return None
        value, expires_at = item
        if time.time() > expires_at:
            del _cache[key]
            return None
        return value


def _cache_set(key, value, ttl_seconds):
    """Сохранить значение в кэш с TTL."""
    with _cache_lock:
        _cache[key] = (value, time.time() + ttl_seconds)


# ========== Серверный кэш: управление активными инструментами ==========

def _mark_instrument_active(instrument_id):
    """Отметить инструмент как активный (запрошен пользователем)."""
    now = time.time()
    with _server_cache_lock:
        _server_cache["active"][instrument_id] = now


def _get_active_instruments():
    """Получить список активных инструментов (запрошенных за последние 5 минут)."""
    now = time.time()
    cutoff = now - ACTIVE_INSTRUMENT_TTL
    with _server_cache_lock:
        active = {k: v for k, v in _server_cache["active"].items() if v > cutoff}
        _server_cache["active"] = active
        # Сортируем по времени последнего запроса (недавние первыми)
        sorted_ids = sorted(active.keys(), key=lambda x: active[x], reverse=True)
        return sorted_ids[:MAX_CACHED_INSTRUMENTS]


def _get_cached_orderbook(instrument_id):
    """Получить данные стакана из серверного кэша."""
    with _server_cache_lock:
        item = _server_cache["orderbook"].get(instrument_id)
        if item:
            return item.get("data")
    return None


def _set_cached_orderbook(instrument_id, data):
    """Сохранить данные стакана в серверный кэш."""
    with _server_cache_lock:
        _server_cache["orderbook"][instrument_id] = {
            "data": data,
            "updated_at": time.time(),
        }


def _get_cached_candle(instrument_id):
    """Получить данные свечи из серверного кэша."""
    with _server_cache_lock:
        item = _server_cache["candles"].get(instrument_id)
        if item:
            return item.get("data")
    return None


def _set_cached_candle(instrument_id, data):
    """Сохранить данные свечи в серверный кэш."""
    with _server_cache_lock:
        _server_cache["candles"][instrument_id] = {
            "data": data,
            "updated_at": time.time(),
        }


def _get_cache_stats():
    """Статистика серверного кэша."""
    with _server_cache_lock:
        return {
            "active_instruments": len(_server_cache["active"]),
            "cached_orderbooks": len(_server_cache["orderbook"]),
            "cached_candles": len(_server_cache["candles"]),
            "max_instruments": MAX_CACHED_INSTRUMENTS,
        }


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Загружаем переменные из env_vars.txt (если есть)
def _load_env_from_file():
    """Читаем токен из env_vars.txt в родительской папке или текущей."""
    for path in ["env_vars.txt", "../env_vars.txt", "../../env_vars.txt"]:
        try:
            full_path = os.path.join(os.path.dirname(__file__), path)
            if os.path.exists(full_path):
                with open(full_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, _, value = line.partition("=")
                            key = key.strip()
                            value = value.strip()
                            if key and value and not os.environ.get(key):
                                os.environ[key] = value
                                logger.info("Loaded %s from %s", key, path)
        except Exception as e:
            logger.debug("Could not read %s: %s", path, e)

_load_env_from_file()

app = Flask(__name__, static_folder="static", static_url_path="")


# ========== Фоновый поток обновления данных ==========

def _background_update_loop():
    """Фоновый поток: обновляет данные по активным инструментам."""
    global _background_running
    logger.info("Background update thread started")
    
    while _background_running:
        try:
            # Получаем список активных инструментов
            active_ids = _get_active_instruments()
            
            if active_ids:
                base_url = _get_api_url()
                headers = _get_headers()
                
                # Определяем интервал (аукцион или нет)
                auction_info = _is_auction_time()
                is_auction = auction_info.get("is_any_auction", False)
                
                logger.info("Background update: %d instruments, auction=%s", 
                           len(active_ids), is_auction)
                
                # Обновляем данные для каждого инструмента
                for instrument_id in active_ids:
                    if not _background_running:
                        break
                    
                    try:
                        # Обновляем стакан (всегда)
                        orderbook_data = _fetch_orderbook_direct(
                            instrument_id, base_url, headers, depth=50
                        )
                        if orderbook_data and "error" not in orderbook_data:
                            # Во время аукциона не кэшируем пустой стакан (нет цены) — чтобы следующий запрос попробовал снова
                            if not is_auction or orderbook_data.get("auction_price") is not None:
                                _set_cached_orderbook(instrument_id, orderbook_data)
                        
                        # Обновляем свечи (реже, раз в минуту достаточно)
                        candle_data = _fetch_5min_candle_direct(
                            instrument_id, base_url, headers
                        )
                        if candle_data is not None:
                            _set_cached_candle(instrument_id, candle_data)
                        
                        # Небольшая пауза между запросами, чтобы не превысить лимит
                        time.sleep(0.05)
                        
                    except Exception as e:
                        logger.warning("Background update error for %s: %s", 
                                      instrument_id, e)
                
                # Определяем интервал до следующего обновления
                interval = BACKGROUND_INTERVAL_AUCTION if is_auction else BACKGROUND_INTERVAL_NORMAL
                logger.debug("Next background update in %d seconds", interval)
                
                # Спим с проверкой флага остановки
                for _ in range(int(interval * 10)):
                    if not _background_running:
                        break
                    time.sleep(0.1)
            else:
                # Нет активных инструментов — спим дольше
                time.sleep(5)
                
        except Exception as e:
            logger.exception("Background update loop error: %s", e)
            time.sleep(5)
    
    logger.info("Background update thread stopped")


def _start_background_thread():
    """Запустить фоновый поток обновления."""
    global _background_thread, _background_running
    
    if _background_thread is not None and _background_thread.is_alive():
        return
    
    _background_running = True
    _background_thread = threading.Thread(target=_background_update_loop, daemon=True)
    _background_thread.start()
    logger.info("Background thread started")


def _stop_background_thread():
    """Остановить фоновый поток."""
    global _background_running
    _background_running = False
    logger.info("Background thread stop requested")

# T-Invest API REST endpoints
API_URL_PROD = "https://invest-public-api.tbank.ru/rest"
API_URL_SANDBOX = "https://sandbox-invest-public-api.tbank.ru/rest"


def _get_api_url():
    use_sandbox = os.environ.get("SANDBOX", "1").strip() in ("1", "true", "yes")
    return API_URL_SANDBOX if use_sandbox else API_URL_PROD


def _get_token_from_request():
    """Получить токен из заголовка X-API-Tokens (список через запятую) или из env.
    Если токенов несколько — выбирает следующий по кругу (round-robin)."""
    header = request.headers.get("X-API-Tokens", "").strip()
    if header:
        tokens = [t.strip() for t in header.split(",") if t.strip()]
        if tokens:
            # Round-robin: выбираем токен по текущей секунде
            idx = int(time.time()) % len(tokens)
            return tokens[idx]
    return os.environ.get("TINKOFF_INVEST_TOKEN", "").strip()


def _get_headers(token=None):
    if token is None:
        token = os.environ.get("TINKOFF_INVEST_TOKEN", "").strip()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _quotation_to_float(q) -> float:
    """Quotation dict (units + nano) -> float."""
    if not q:
        return 0.0
    units = int(q.get("units", 0) or 0)
    nano = int(q.get("nano", 0) or 0)
    return units + nano / 1e9


@app.route("/")
def index():
    """Главная страница.
    
    Без параметра ?profile=... показываем заглушку, чтобы доступ был только по
    именным ссылкам вида /?profile=nikita.
    """
    ALLOWED_PROFILES = {"damir", "ruslan", "kolya", "vanya", "nikita", "anton", "alex"}
    profile = request.args.get("profile", "").strip().lower()
    if not profile or profile not in ALLOWED_PROFILES:
        # Простая заглушка без виджета и без настроек
        return (
            """
<!DOCTYPE html>
<html lang="ru">
  <head>
    <meta charset="UTF-8">
    <title>Виджет — доступ по личным ссылкам</title>
    <style>
      body {
        margin: 0;
        padding: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
        background: #14161c;
        color: #e6e8ec;
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 100vh;
      }
      .card {
        background: #1f2229;
        border-radius: 12px;
        padding: 24px 28px;
        max-width: 420px;
        box-shadow: 0 20px 40px rgba(0,0,0,0.45);
        border: 1px solid #333844;
      }
      h1 {
        margin: 0 0 12px;
        font-size: 20px;
      }
      p {
        margin: 0 0 8px;
        font-size: 14px;
        line-height: 1.5;
        color: #9a9faf;
      }
      .note {
        margin-top: 12px;
        font-size: 12px;
        color: #6f7484;
      }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>Доступ к виджету по личным ссылкам</h1>
      <p>Чтобы открыть виджет аукциона, используйте персональный URL с профилем.</p>
      <p>Попросите свою личную ссылку.</p>
    </div>
  </body>
</html>
""",
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )
    # Есть profile — отдаём обычный виджет
    return send_from_directory(app.static_folder, "index.html")


# Список акций (спот), которые нужно показывать
SPOT_TICKERS = ["SBER", "GAZP", "LKOH", "NVTK", "GMKN", "VTBR", "PLZL",
                "ROSN", "TATN", "YNDX", "MGNT", "AFLT", "ALRS", "MTSS",
                "NLMK", "MAGN", "CHMF", "POLY", "PHOR", "IRAO"]

# Маппинг: тикер акции → префикс тикера фьючерса на MOEX
# Фьючи ищутся по вхождению префикса в тикер (SBER → SBERМ25, SBERH25 и т.д.)
# ПРОВЕРЬ и поправь список под реальные тикеры в T-Invest!
BLUE_CHIP_FUTURES_MAP = {
    "SBER":  "SBRF",   # Сбербанк (SBRF-6.26, SBERF)
    "GAZP":  "GAZRF",  # Газпром  (GAZRF, GAZR-6.26)
    "LKOH":  "LKOH",   # ЛУКОЙЛ   (LKOH-6.26)
    "NVTK":  "NVTK",   # Новатэк
    "GMKN":  "GMKN",   # Норникель
    "VTBR":  "VTBR",   # ВТБ
    "PLZL":  "PLZL",   # Полюс Золото
    "ROSN":  "ROSN",   # Роснефть
    "TATN":  "TATN",   # Татнефть
    "YNDX":  "YNDX",   # Яндекс
    "MGNT":  "MGNT",   # Магнит
    "AFLT":  "AFLT",   # Аэрофлот
    "ALRS":  "ALRS",   # АЛРОСА
    "MTSS":  "MTSS",   # МТС
    "NLMK":  "NLMK",   # НЛМК
    "MAGN":  "MAGN",   # ММК
    "CHMF":  "CHMF",   # Северсталь
    "POLY":  "POLY",   # Полиметалл
    "PHOR":  "PHOR",   # ФосАгро
    "IRAO":  "IRAO",   # Интер РАО
}


@app.route("/api/spot_prices")
def api_spot_prices():
    """Текущие цены и % изменение от дневного закрытия для акций голубых фишек.
    Используется для мониторинга расхождения с фьючерсами во время аукциона.
    """
    token = _get_token_from_request()
    if not token:
        return jsonify({"error": "TINKOFF_INVEST_TOKEN not set"}), 503

    base_url = _get_api_url()
    headers = _get_headers(token)

    # Берём кэшированный список инструментов (загружается через /api/futures)
    instruments_cache = _cache_get("instruments_list")
    if instruments_cache is None:
        return jsonify({"error": "instruments not loaded, open /api/futures first"}), 503

    # Фильтруем только акции из нашего списка
    spot_instruments = [
        i for i in instruments_cache
        if i.get("instrument_type") == "shares"
        and i.get("ticker", "").upper() in [t.upper() for t in SPOT_TICKERS]
    ]

    if not spot_instruments:
        return jsonify({"stocks": [], "mapping": BLUE_CHIP_FUTURES_MAP})

    # Батч-запрос последних цен сразу для всех акций
    uids = [i.get("instrument_uid") or i.get("figi") for i in spot_instruments if i.get("instrument_uid") or i.get("figi")]
    last_prices_map = {}
    try:
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices"
        resp = requests.post(url, headers=headers, json={"instrumentId": uids}, timeout=15, verify=False)
        resp.raise_for_status()
        for lp in resp.json().get("lastPrices", []):
            uid = lp.get("instrumentUid") or lp.get("figi", "")
            price = _quotation_to_float(lp.get("price"))
            if uid and price:
                last_prices_map[uid] = price
    except Exception as e:
        logger.warning("GetLastPrices failed: %s", e)

    result = []
    for inst in spot_instruments:
        ticker = inst.get("ticker", "")
        uid = inst.get("instrument_uid") or inst.get("figi", "")
        last_price = last_prices_map.get(uid)

        # Дневное закрытие (кэшируется на 5 мин)
        daily_close = _fetch_daily_close(uid, base_url, headers)

        change_pct = None
        if last_price and daily_close and daily_close != 0:
            change_pct = round((last_price - daily_close) / daily_close * 100, 2)

        result.append({
            "ticker": ticker,
            "name": inst.get("name", ticker),
            "instrument_uid": uid,
            "last_price": round(last_price, 4) if last_price else None,
            "daily_close": round(daily_close, 4) if daily_close else None,
            "change_pct": change_pct,
            "futures_prefix": BLUE_CHIP_FUTURES_MAP.get(ticker.upper(), ticker),
        })

    return jsonify({"stocks": result, "mapping": BLUE_CHIP_FUTURES_MAP})


@app.route("/api/stats")
def api_stats():
    """Статистика запросов и кэша за последние 5 минут."""
    stats = _get_stats()
    cache_stats = _get_cache_stats()
    
    # Информация о фоновом потоке
    background_info = {
        "running": _background_running,
        "interval_auction": BACKGROUND_INTERVAL_AUCTION,
        "interval_normal": BACKGROUND_INTERVAL_NORMAL,
        "max_instruments": MAX_CACHED_INSTRUMENTS,
    }
    
    return jsonify({
        **stats,
        "cache": cache_stats,
        "background": background_info,
    })


@app.route("/api/futures")
def api_futures():
    """Список фьючерсов + избранных акций (спот) для настроек. Кэшируется на 5 минут."""
    session_id = request.args.get("session_id") or request.headers.get("X-Session-ID")
    _record_request("/api/futures", session_id)
    token = _get_token_from_request()
    if not token:
        logger.warning("TINKOFF_INVEST_TOKEN not set")
        return jsonify({"error": "TINKOFF_INVEST_TOKEN not set"}), 503

    # Проверяем кэш
    cache_key = "instruments_list"
    cached = _cache_get(cache_key)
    if cached is not None:
        logger.debug("instruments list from cache")
        return jsonify({"futures": cached, "cached": True})

    base_url = _get_api_url()
    headers = _get_headers(token)
    items = []

    # 1. Загружаем фьючерсы
    try:
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures"
        resp = requests.post(url, headers=headers, json={}, timeout=30, verify=False)
        resp.raise_for_status()
        data = resp.json()
        for inv in data.get("instruments", []):
            items.append({
                "figi": inv.get("figi", ""),
                "ticker": inv.get("ticker", ""),
                "name": inv.get("name") or inv.get("ticker", ""),
                "instrument_uid": inv.get("uid", "") or inv.get("figi", ""),
                "instrument_type": "futures",
            })
        logger.info("futures count=%s", len(items))
    except Exception as e:
        logger.exception("Error loading futures: %s", e)

    # 2. Загружаем только избранные акции (спот)
    try:
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares"
        resp = requests.post(url, headers=headers, json={}, timeout=30, verify=False)
        resp.raise_for_status()
        data = resp.json()
        shares_count = 0
        spot_tickers_upper = [t.upper() for t in SPOT_TICKERS]
        for inv in data.get("instruments", []):
            ticker = inv.get("ticker", "")
            # Только акции из списка SPOT_TICKERS
            if ticker.upper() in spot_tickers_upper:
                items.append({
                    "figi": inv.get("figi", ""),
                    "ticker": ticker,
                    "name": inv.get("name") or ticker,
                    "instrument_uid": inv.get("uid", "") or inv.get("figi", ""),
                    "instrument_type": "shares",
                })
                shares_count += 1
        logger.info("shares (spot) count=%s", shares_count)
    except Exception as e:
        logger.exception("Error loading shares: %s", e)

    logger.info("total instruments=%s (fresh)", len(items))
    _cache_set(cache_key, items, CACHE_TTL_FUTURES)
    return jsonify({"futures": items})


def _last_completed_5min_close(candles):
    """Из списка 5-минутных свечей вернуть close последней по времени завершённой (isComplete=true).
    Свечи сортируем по полю time, т.к. API может вернуть в произвольном порядке.
    """
    if not candles:
        return None
    completed = [c for c in candles if c.get("isComplete", False)]
    if not completed:
        completed = candles
    completed.sort(key=lambda c: c.get("time") or "")
    last_candle = completed[-1]
    close_price = _quotation_to_float(last_candle.get("close"))
    return round(close_price, 4) if close_price else None


def _fetch_5min_candle_direct(instrument_id, base_url, headers):
    """Получить цену закрытия последней 5-минутной свечи (без кэширования).
    
    Окно 3 часа (UTC), чтобы захватить последнюю завершённую 5-минутку даже перед аукционом.
    Источник: T-Invest API GetCandles, последняя по времени завершённая свеча (isComplete=true).
    """
    try:
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(hours=24)
        
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
        payload = {
            "instrumentId": instrument_id,
            "from": from_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": to_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": "CANDLE_INTERVAL_5_MIN",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15, verify=False)
        resp.raise_for_status()
        data = resp.json()
        candles = data.get("candles", [])
        return _last_completed_5min_close(candles)
    except Exception as e:
        logger.warning("_fetch_5min_candle_direct %s: %s", instrument_id, e)
        return None


def _fetch_5min_candle_close(instrument_id, base_url, headers):
    """Получить цену закрытия последней завершённой 5-минутной свечи.
    
    Сначала проверяет серверный кэш, затем старый кэш, затем делает запрос.
    """
    # Проверяем серверный кэш (заполняется фоновым потоком)
    cached_candle = _get_cached_candle(instrument_id)
    if cached_candle is not None:
        return cached_candle, True
    
    # Проверяем старый кэш
    cache_key = f"candle_5min_{instrument_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached, True

    try:
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(hours=24)
        
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
        payload = {
            "instrumentId": instrument_id,
            "from": from_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": to_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": "CANDLE_INTERVAL_5_MIN",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15, verify=False)
        resp.raise_for_status()
        data = resp.json()
        candles = data.get("candles", [])
        result = _last_completed_5min_close(candles)
        _cache_set(cache_key, result, CACHE_TTL_CANDLES)
        return result, False
    except Exception as e:
        logger.warning("get_5min_candle %s: %s", instrument_id, e)
        return None, False


CACHE_TTL_DAILY = 300  # 5 минут — дневное закрытие меняется редко


def _fetch_daily_close(instrument_id, base_url, headers):
    """Цена закрытия последней завершённой дневной свечи (для колонки «Цена д»).
    Кэш 5 мин. Fallback — closePrice из стакана, если свечей нет.
    """
    cache_key = f"candle_daily_{instrument_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(days=10)
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
        payload = {
            "instrumentId": instrument_id,
            "from": from_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": to_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": "CANDLE_INTERVAL_DAY",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15, verify=False)
        resp.raise_for_status()
        data = resp.json()
        candles = data.get("candles", [])
        completed = [c for c in candles if c.get("isComplete", False)]
        if not completed:
            return None
        completed.sort(key=lambda c: c.get("time") or "")
        last_day = completed[-1]
        close_price = _quotation_to_float(last_day.get("close"))
        result = round(close_price, 4) if close_price else None
        _cache_set(cache_key, result, CACHE_TTL_DAILY)
        return result
    except Exception as e:
        logger.debug("_fetch_daily_close %s: %s", instrument_id, e)
        return None


def _fetch_candles_for_instrument(instrument_id, base_url, headers, from_ts, to_ts):
    """Получить свечи для одного инструмента (с кэшированием)."""
    cache_key = f"candles_{instrument_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached, True  # (result, is_cached)

    try:
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
        payload = {
            "instrumentId": instrument_id,
            "from": from_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": to_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": "CANDLE_INTERVAL_DAY",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30, verify=False)
        resp.raise_for_status()
        data = resp.json()
        candles = data.get("candles", [])

        if not candles:
            result = {
                "instrument_id": instrument_id,
                "name": instrument_id,
                "close": None,
                "open": None,
                "change_pct": None,
                "error": "no data",
            }
        elif len(candles) < 2:
            # Только одна свеча (сегодняшняя) - нет вчерашних данных
            today = candles[-1]
            open_price = _quotation_to_float(today.get("open"))
            close_price = _quotation_to_float(today.get("close"))
            result = {
                "instrument_id": instrument_id,
                "name": instrument_id,
                "close": round(close_price, 4) if close_price else None,
                "open": round(open_price, 4) if open_price else None,
                "change_pct": None,
            }
        else:
            # Вчерашняя свеча (предпоследняя) и сегодняшняя (последняя)
            yesterday = candles[-2]
            today = candles[-1]
            yesterday_close = _quotation_to_float(yesterday.get("close"))
            today_open = _quotation_to_float(today.get("open"))
            change_pct = None
            if yesterday_close and yesterday_close != 0:
                change_pct = round((today_open - yesterday_close) / yesterday_close * 100, 2)
            result = {
                "instrument_id": instrument_id,
                "name": instrument_id,
                "close": round(yesterday_close, 4) if yesterday_close else None,
                "open": round(today_open, 4) if today_open else None,
                "change_pct": change_pct,
            }
        
        _cache_set(cache_key, result, CACHE_TTL_CANDLES)
        return result, False
    except requests.RequestException as e:
        logger.warning("get_candles %s: %s", instrument_id, e)
        return {
            "instrument_id": instrument_id,
            "name": instrument_id,
            "close": None,
            "open": None,
            "change_pct": None,
            "error": str(e),
        }, False


@app.route("/api/table")
def api_table():
    """Данные для таблицы: по списку instrument_id — последняя 5-минутная свеча (close) + цена аукциона + отклонение + лоты."""
    token = _get_token_from_request()
    if not token:
        return jsonify({"error": "TINKOFF_INVEST_TOKEN not set"}), 503

    ids_param = request.args.get("ids", "")
    if not ids_param:
        return jsonify({"rows": []})
    instrument_ids = [x.strip() for x in ids_param.split(",") if x.strip()]

    base_url = _get_api_url()
    headers = _get_headers(token)

    rows = []
    cached_count = 0
    for instrument_id in instrument_ids:
        # Получаем цену закрытия последней 5-минутной свечи
        candle_5min_close, is_cached = _fetch_5min_candle_close(instrument_id, base_url, headers)
        
        # Получаем данные стакана для цены аукциона и лотов
        orderbook, _ = _fetch_orderbook(instrument_id, base_url, headers, depth=10)
        
        auction_price = orderbook.get("auction_price")
        
        # Отклонение от 5-минутной свечи до цены аукциона
        change_pct = None
        if candle_5min_close and auction_price and candle_5min_close != 0:
            change_pct = round((auction_price - candle_5min_close) / candle_5min_close * 100, 2)
        
        result = {
            "instrument_id": instrument_id,
            "name": instrument_id,
            "close": candle_5min_close,
            "candle_5min_close": candle_5min_close,
            "open": auction_price,
            "auction_price": auction_price,
            "change_pct": change_pct,
            "total_lots": orderbook.get("total_lots") or orderbook.get("auction_lots"),
            "imbalance": orderbook.get("imbalance"),
        }
        
        rows.append(result)
        if is_cached:
            cached_count += 1

    logger.info("table: total=%d, cached=%d, fresh=%d", len(rows), cached_count, len(rows) - cached_count)
    return jsonify({"rows": rows})


def _is_auction_time(instrument_type=None):
    """Проверить, сейчас ли время аукциона (по московскому времени).
    
    Расписание аукционов:
    - Акции (shares): 6:50-7:00 (открытие), 18:40-18:45 (закрытие), 18:45-18:50 (частичный)
    - Фьючерсы (futures): 8:50-9:00 (открытие)
    
    Args:
        instrument_type: 'shares', 'futures' или None (проверяет все аукционы)
    """
    now_utc = datetime.now(timezone.utc)
    moscow_offset = timedelta(hours=3)
    now_msk = now_utc + moscow_offset
    
    time_minutes = now_msk.hour * 60 + now_msk.minute
    
    # Аукционы для акций (shares)
    shares_auctions = [
        {"start": 6 * 60 + 50, "end": 7 * 60, "type": "opening", "name": "Акции: открытие"},
        {"start": 18 * 60 + 40, "end": 18 * 60 + 45, "type": "closing", "name": "Акции: закрытие"},
        {"start": 18 * 60 + 45, "end": 18 * 60 + 50, "type": "partial", "name": "Акции: частичный"},
    ]
    
    # Аукционы для фьючерсов (futures)
    futures_auctions = [
        {"start": 8 * 60 + 50, "end": 9 * 60, "type": "opening", "name": "Фьючерсы: открытие"},
    ]
    
    def check_auctions(auctions):
        for auction in auctions:
            if auction["start"] <= time_minutes < auction["end"]:
                return auction
        return None
    
    active_shares = check_auctions(shares_auctions)
    active_futures = check_auctions(futures_auctions)
    
    # Определяем активный аукцион в зависимости от типа инструмента
    if instrument_type == "shares":
        active = active_shares
    elif instrument_type == "futures":
        active = active_futures
    else:
        # Если тип не указан, возвращаем любой активный аукцион
        active = active_shares or active_futures
    
    is_any_auction = active_shares is not None or active_futures is not None
    
    return {
        "is_auction": active is not None,
        "is_any_auction": is_any_auction,
        "auction_type": active["type"] if active else None,
        "auction_name": active["name"] if active else None,
        "shares_auction": active_shares["name"] if active_shares else None,
        "futures_auction": active_futures["name"] if active_futures else None,
        "moscow_time": now_msk.strftime("%H:%M:%S"),
    }


def _calculate_auction_price(bids, asks, reference_price=None):
    """Рассчитать цену сведения аукциона по правилам биржи (MOEX-style).

    Алгоритм:
      1. Кумулятивный bid (сверху вниз) и ask (снизу вверх)
      2. На каждой цене executed = min(cum_bid, cum_ask)
      3. Равновесный диапазон — все цены, где executed == max(executed)
      4. В диапазоне отфильтровать по min |imbalance|
      5. Выбор цены:
         - дисбаланс везде в bid (cum_bid > cum_ask) → ВЕРХНЯЯ цена диапазона
         - дисбаланс везде в ask (cum_ask > cum_bid) → НИЖНЯЯ цена диапазона
         - смешанный знак или 0 → ближайшая к reference_price
           (или середина диапазона, если ref нет)

    Args:
        bids, asks: списки заявок из API T-Invest
        reference_price: цена-ориентир для случая нулевого дисбаланса
                        (обычно last_price или предыдущий close)

    Returns:
        tuple: (auction_price, executed_lots, imbalance, imbalance_direction)
        - auction_price: цена сведения
        - executed_lots: количество лотов, которые исполнятся
        - imbalance: лоты, которые НЕ исполнятся (модуль)
        - imbalance_direction: 'bid' / 'ask' / None
    """
    # ===== Граничные случаи =====
    if not bids and not asks:
        return None, 0, 0, None
    if bids and not asks:
        best_bid_price = _quotation_to_float(bids[0].get("price"))
        total_bid_lots = sum(int(b.get("quantity", 0)) for b in bids)
        return (
            round(best_bid_price, 2) if best_bid_price else None,
            0,
            total_bid_lots,
            "bid",
        )
    if asks and not bids:
        best_ask_price = _quotation_to_float(asks[0].get("price"))
        total_ask_lots = sum(int(a.get("quantity", 0)) for a in asks)
        return (
            round(best_ask_price, 2) if best_ask_price else None,
            0,
            total_ask_lots,
            "ask",
        )

    # ===== Парсинг и группировка по уровням =====
    _round = lambda x: round(x, 4)
    bid_by_price = {}
    for b in bids:
        p = _round(_quotation_to_float(b.get("price")))
        bid_by_price[p] = bid_by_price.get(p, 0) + int(b.get("quantity", 0))
    ask_by_price = {}
    for a in asks:
        p = _round(_quotation_to_float(a.get("price")))
        ask_by_price[p] = ask_by_price.get(p, 0) + int(a.get("quantity", 0))

    all_prices = sorted(set(list(bid_by_price.keys()) + list(ask_by_price.keys())))
    if not all_prices:
        return None, 0, 0, None

    # ===== Кумулятивные объёмы =====
    cumulative_bid = {}
    running = 0
    for price in reversed(all_prices):
        running += bid_by_price.get(price, 0)
        cumulative_bid[price] = running

    cumulative_ask = {}
    running = 0
    for price in all_prices:
        running += ask_by_price.get(price, 0)
        cumulative_ask[price] = running

    # ===== На каждой цене считаем executed и raw imbalance =====
    levels = []  # (price, cum_bid, cum_ask, executed, imbalance_signed)
    for price in all_prices:
        cb = cumulative_bid.get(price, 0)
        ca = cumulative_ask.get(price, 0)
        executed = min(cb, ca)
        imb = cb - ca   # знак: + → bid, − → ask, 0 → баланс
        levels.append((price, cb, ca, executed, imb))

    max_executed = max(l[3] for l in levels)
    if max_executed == 0:
        # Стаканы не пересекаются вообще
        return None, 0, 0, None

    # Шаг 1: равновесный диапазон
    equilibrium = [l for l in levels if l[3] == max_executed]

    # Шаг 2: минимальный |imbalance| в диапазоне
    min_abs_imb = min(abs(l[4]) for l in equilibrium)
    candidates = [l for l in equilibrium if abs(l[4]) == min_abs_imb]

    # Шаг 3: выбор цены по направлению дисбаланса
    signs = set()
    for l in candidates:
        if l[4] > 0:
            signs.add(1)
        elif l[4] < 0:
            signs.add(-1)
        else:
            signs.add(0)

    if signs == {1}:
        # Везде bid преобладает — берём максимальную цену
        chosen = max(candidates, key=lambda l: l[0])
    elif signs == {-1}:
        # Везде ask преобладает — берём минимальную цену
        chosen = min(candidates, key=lambda l: l[0])
    else:
        # Смешанный знак или нулевой дисбаланс → используем reference_price
        if reference_price and reference_price > 0:
            chosen = min(candidates, key=lambda l: abs(l[0] - reference_price))
        else:
            # Без референса — середина диапазона (берём центральный кандидат)
            sorted_c = sorted(candidates, key=lambda l: l[0])
            chosen = sorted_c[len(sorted_c) // 2]

    best_price, _cb, _ca, best_executed, raw_imb = chosen
    best_imbalance = abs(raw_imb)
    if raw_imb > 0:
        best_direction = 'bid'
    elif raw_imb < 0:
        best_direction = 'ask'
    else:
        best_direction = None

    return (
        round(best_price, 2),
        best_executed,
        best_imbalance,
        best_direction,
    )


def _fetch_orderbook_direct(instrument_id, base_url, headers, depth=50):
    """Получить стакан для инструмента (без кэширования).
    
    Args:
        depth: глубина стакана (1, 10, 20, 30, 40, 50). По умолчанию 50 для точного расчёта.
    """
    try:
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetOrderBook"
        payload = {
            "instrumentId": instrument_id,
            "depth": depth,
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15, verify=False)
        resp.raise_for_status()
        data = resp.json()
        
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        
        best_bid = _quotation_to_float(bids[0].get("price")) if bids else None
        best_ask = _quotation_to_float(asks[0].get("price")) if asks else None
        
        total_bid_lots = sum(int(b.get("quantity", 0)) for b in bids)
        total_ask_lots = sum(int(a.get("quantity", 0)) for a in asks)
        
        last_price = _quotation_to_float(data.get("lastPrice"))
        close_price = _quotation_to_float(data.get("closePrice"))
        ref_for_auction = last_price or close_price or None
        auction_price, executed_lots, imbalance, imbalance_direction = _calculate_auction_price(bids, asks, ref_for_auction)
        # Дневное закрытие: из дневных свечей API, иначе closePrice стакана
        daily_close_price = _fetch_daily_close(instrument_id, base_url, headers)
        if daily_close_price is None:
            daily_close_price = close_price
        
        # Получаем цену закрытия последней 5-минутной свечи
        candle_5min_close = _get_cached_candle(instrument_id)
        if candle_5min_close is None:
            candle_5min_close = _fetch_5min_candle_direct(instrument_id, base_url, headers)
        
        reference_price = candle_5min_close if candle_5min_close else daily_close_price
        deviation_pct = None
        if auction_price and reference_price and reference_price != 0:
            deviation_pct = round((auction_price - reference_price) / reference_price * 100, 2)
        
        return {
            "instrument_id": instrument_id,
            "best_bid": round(best_bid, 4) if best_bid else None,
            "best_ask": round(best_ask, 4) if best_ask else None,
            "spread": round(best_ask - best_bid, 4) if (best_bid and best_ask) else None,
            "auction_price": round(auction_price, 2) if auction_price else None,
            "executed_lots": executed_lots,
            "imbalance": imbalance,
            "imbalance_direction": imbalance_direction,
            "last_price": round(last_price, 4) if last_price else None,
            "close_price": round(reference_price, 4) if reference_price else None,
            "daily_close_price": round(daily_close_price, 4) if daily_close_price else None,
            "candle_5min_close": candle_5min_close,
            "deviation_pct": deviation_pct,
            "total_bid_lots": total_bid_lots,
            "total_ask_lots": total_ask_lots,
            "total_lots": executed_lots,
            "orderbook_depth": len(bids),
        }
    except Exception as e:
        logger.warning("_fetch_orderbook_direct %s: %s", instrument_id, e)
        return {"instrument_id": instrument_id, "error": str(e)}


def _fetch_orderbook(instrument_id, base_url, headers, depth=50):
    """Получить стакан для инструмента.
    
    Сначала проверяет серверный кэш (заполняется фоновым потоком),
    затем старый кэш, затем делает прямой запрос.
    
    Args:
        depth: глубина стакана (1, 10, 20, 30, 40, 50). По умолчанию 50 для точного расчёта.
    """
    # Проверяем серверный кэш (заполняется фоновым потоком)
    cached_orderbook = _get_cached_orderbook(instrument_id)
    # Во время аукциона не используем кэш с пустой ценой — даём шанс получить свежий стакан
    if cached_orderbook is not None:
        if _is_auction_time().get("is_any_auction") and cached_orderbook.get("auction_price") is None:
            cached_orderbook = None
        else:
            return cached_orderbook, True
    
    # Проверяем старый кэш (TTL 2 сек)
    cache_key = f"orderbook_{instrument_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        if _is_auction_time().get("is_any_auction") and cached.get("auction_price") is None:
            cached = None
        else:
            return cached, True
    
    try:
        url = f"{base_url}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetOrderBook"
        payload = {
            "instrumentId": instrument_id,
            "depth": depth,
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15, verify=False)
        resp.raise_for_status()
        data = resp.json()
        
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        
        best_bid = _quotation_to_float(bids[0].get("price")) if bids else None
        best_ask = _quotation_to_float(asks[0].get("price")) if asks else None
        
        # Суммарный объём заявок (API может отдавать только видимую часть айсбергов)
        total_bid_lots = sum(int(b.get("quantity", 0)) for b in bids)
        total_ask_lots = sum(int(a.get("quantity", 0)) for a in asks)
        
        # Рассчитываем цену аукциона через кумулятивные объёмы
        last_price = _quotation_to_float(data.get("lastPrice"))
        close_price = _quotation_to_float(data.get("closePrice"))
        ref_for_auction = last_price or close_price or None
        auction_price, executed_lots, imbalance, imbalance_direction = _calculate_auction_price(bids, asks, ref_for_auction)
        daily_close_price = _fetch_daily_close(instrument_id, base_url, headers)
        if daily_close_price is None:
            daily_close_price = close_price
        
        # Получаем цену закрытия последней 5-минутной свечи для расчёта отклонения
        candle_5min_close, _ = _fetch_5min_candle_close(instrument_id, base_url, headers)
        reference_price = candle_5min_close if candle_5min_close else daily_close_price
        deviation_pct = None
        if auction_price and reference_price and reference_price != 0:
            deviation_pct = round((auction_price - reference_price) / reference_price * 100, 2)
        
        result = {
            "instrument_id": instrument_id,
            "best_bid": round(best_bid, 4) if best_bid else None,
            "best_ask": round(best_ask, 4) if best_ask else None,
            "spread": round(best_ask - best_bid, 4) if (best_bid and best_ask) else None,
            "auction_price": round(auction_price, 2) if auction_price else None,
            "executed_lots": executed_lots,  # Лоты, которые исполнятся
            "imbalance": imbalance,  # Лоты, которые НЕ исполнятся
            "imbalance_direction": imbalance_direction,  # 'bid' или 'ask'
            "last_price": round(last_price, 4) if last_price else None,
            "close_price": round(reference_price, 4) if reference_price else None,
            "daily_close_price": round(daily_close_price, 4) if daily_close_price else None,
            "candle_5min_close": candle_5min_close,
            "deviation_pct": deviation_pct,
            "total_bid_lots": total_bid_lots,
            "total_ask_lots": total_ask_lots,
            "total_lots": executed_lots,  # Лоты исполнения (для совместимости с фронтом)
            "orderbook_depth": len(bids),  # Для отладки
        }
        
        logger.debug("orderbook %s: price=%.2f, executed=%d, imbalance=%d (%s), depth=%d",
                    instrument_id, auction_price or 0, executed_lots, imbalance, 
                    imbalance_direction or 'none', len(bids))
        
        # Кэш на 2 секунды; во время аукциона не кэшируем пустой стакан (нет цены)
        if not (_is_auction_time().get("is_any_auction") and result.get("auction_price") is None):
            _cache_set(cache_key, result, 2)
        return result, False
    except Exception as e:
        logger.warning("get_orderbook %s: %s", instrument_id, e)
        return {
            "instrument_id": instrument_id,
            "error": str(e),
        }, False


@app.route("/api/orderbook")
def api_orderbook():
    """Данные стакана для аукциона. Отклонение считается от последней 5-минутной свечи.
    
    Серверное кэширование v5:
    - Инструменты отмечаются как активные при запросе
    - Фоновый поток обновляет данные по активным инструментам
    - Лимит: 40 инструментов (ограничение API и интервал 4 сек)
    """
    session_id = request.args.get("session_id") or request.headers.get("X-Session-ID")
    _record_request("/api/orderbook", session_id)
    token = _get_token_from_request()
    if not token:
        return jsonify({"error": "TINKOFF_INVEST_TOKEN not set"}), 503

    ids_param = request.args.get("ids", "")
    if not ids_param:
        return jsonify({"rows": [], "auction": _is_auction_time()})

    instrument_ids = [x.strip() for x in ids_param.split(",") if x.strip()]

    # Отмечаем инструменты как активные (для фонового обновления)
    for instrument_id in instrument_ids:
        _mark_instrument_active(instrument_id)

    # Запускаем фоновый поток, если ещё не запущен
    _start_background_thread()

    base_url = _get_api_url()
    headers = _get_headers(token)
    
    rows = []
    cached_count = 0
    for instrument_id in instrument_ids:
        result, is_cached = _fetch_orderbook(instrument_id, base_url, headers)
        if result.get("error"):
            candle_5min, _ = _fetch_5min_candle_close(instrument_id, base_url, headers)
            daily_close = _fetch_daily_close(instrument_id, base_url, headers)
            result["candle_5min_close"] = candle_5min
            result["daily_close_price"] = round(daily_close, 4) if daily_close else None
            result["close_price"] = round(candle_5min or daily_close or 0, 4) if (candle_5min or daily_close) else None
        rows.append(result)
        if is_cached:
            cached_count += 1
    
    # Информация об аукционах (общая для всех типов)
    auction_info = _is_auction_time()
    cache_stats = _get_cache_stats()
    
    # Предупреждение о лимите
    limit_warning = None
    if len(instrument_ids) > MAX_CACHED_INSTRUMENTS:
        limit_warning = f"Превышен лимит: выбрано {len(instrument_ids)}, максимум {MAX_CACHED_INSTRUMENTS}. Лишние инструменты не будут обновляться в реальном времени."
    
    logger.info("orderbook: total=%d, cached=%d, fresh=%d, auction=%s", 
                len(rows), cached_count, len(rows) - cached_count, 
                auction_info.get("is_any_auction"))
    
    return jsonify({
        "rows": rows, 
        "auction": auction_info,
        "cache_stats": cache_stats,
        "limit_warning": limit_warning,
    })


@app.route("/api/notify/push", methods=["POST", "GET"])
def notify_push():
    """Добавить уведомление в очередь. Параметр msg — текст уведомления."""
    msg = (request.args.get("msg") or (request.json or {}).get("msg", "")).strip()
    if not msg:
        return jsonify({"error": "msg required"}), 400
    with _notify_lock:
        _notify_queue.append({
            "id": str(time.time()),
            "msg": msg,
            "ts": time.time(),
        })
    logger.info("notify_push: %s", msg)
    return jsonify({"ok": True})


@app.route("/api/notify/poll")
def notify_poll():
    """Фронтенд забирает уведомления и удаляет их из очереди."""
    with _notify_lock:
        items = list(_notify_queue)
        _notify_queue.clear()
    return jsonify({"notifications": items})


def main():
    port = int(os.environ.get("PORT", "5000"))
    sandbox = "sandbox" if os.environ.get("SANDBOX", "1").strip() in ("1", "true", "yes") else "prod"
    logger.info("Starting server port=%s mode=%s", port, sandbox)
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()
