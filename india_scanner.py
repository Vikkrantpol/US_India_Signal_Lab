"""
╔══════════════════════════════════════════════════════════════╗
║         INDIA PRO STOCK SCANNER  —  NSE ONLY                ║
║         Powered by Fyers API + yfinance fundamentals         ║
╠══════════════════════════════════════════════════════════════╣
║  Primary gates and scoring rules are loaded from a local     ║
║  secure conditions file and are not embedded in tracked code.║
║                                                              ║
║  Runtime still evaluates those rules locally during scans.   ║
╚══════════════════════════════════════════════════════════════╝

Setup:
    pip install fyers-apiv3 yfinance pandas numpy requests

Usage:
    1. Optionally add FYERS credentials to .env
    2. python india_scanner.py
    3. Open dashboard.html in browser at http://localhost:8000
"""

import os, json, time, random, math, requests, shutil, gc, atexit
import errno
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
from threading import Lock

from env_utils import load_env_file
from scan_conditions_loader import load_scan_conditions
from scan_state import ScanStateStore

try:
    import resource
except Exception:
    resource = None

try:
    from curl_cffi import requests as curl_requests
except Exception:
    curl_requests = None

# ── Fyers imports ─────────────────────────────────────────────────────────────
try:
    from fyers_apiv3 import fyersModel
    FYERS_AVAILABLE = True
except ImportError:
    print("⚠  fyers-apiv3 not installed. Run: pip install fyers-apiv3")
    print("   Falling back to yfinance for price data.")
    FYERS_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG — SET THESE
# ══════════════════════════════════════════════════════════════════════════════
ENV_FILE = Path(__file__).with_name(".env")
load_env_file(ENV_FILE, override=False)

FYERS_CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()
FYERS_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()

OUTPUT_JSON   = "india_results.json"
STATE_DB      = "scan_state.db"
ARCHIVE_DB    = "scan_mega_history.db"
LEGACY_SCANNED_LOG = "india_scanned.txt"
CHUNK_SIZE    = 15
DELAY_BETWEEN = 0.8
CHUNK_PAUSE   = 6
RETRY_DELAY   = 30
MAX_RETRIES   = 3
MAX_WORKERS   = 4
LOG_DATE_FORMAT = '%Y-%m-%d'
RESCAN_AFTER_DAYS = 3
PRIMARY_CODES = ('P1', 'P2', 'P3', 'P4', 'P5')
RUN_DATE = datetime.now().strftime(LOG_DATE_FORMAT)
TODAY = datetime.now().date()
SNAPSHOT_KEEP = 7
FD_SOFT_TARGET = 4096

SCAN_CONDITIONS    = load_scan_conditions("INDIA")
PRIMARY_CONFIG     = SCAN_CONDITIONS["primary"]
SCORING_CONFIG     = SCAN_CONDITIONS["scoring"]
MIN_TURNOVER_CR    = float(PRIMARY_CONFIG["min_turnover_cr"] or 0)
HIGH_LOOKBACK      = int(PRIMARY_CONFIG["high_lookback"])
PRICE_FROM_HIGH    = float(PRIMARY_CONFIG["price_from_high"])
TURNOVER_WINDOW    = int(PRIMARY_CONFIG["turnover_window"])
EMA21_PERIOD, EMA50_PERIOD, EMA200_PERIOD = [int(v) for v in PRIMARY_CONFIG["ema_periods"]]
NEAR_HIGH_PCT      = float(SCORING_CONFIG["near_high_pct"])
VOLUME_SPIKE_RATIO = float(SCORING_CONFIG["volume_spike_ratio"])
EPS_GROWTH_MIN     = float(SCORING_CONFIG["eps_growth_min"])
REVENUE_GROWTH_MIN = float(SCORING_CONFIG["revenue_growth_min"])
UPPER_CIRCUIT_PCT  = float(SCORING_CONFIG["upper_circuit_pct"] or 0)
DELIVERY_PCT_MIN   = float(SCORING_CONFIG["delivery_pct_min"] or 0)
YF_SHARED_SESSION = None

RESET = "\033[0m"
CYAN = "\033[38;5;51m"
ORANGE = "\033[38;5;214m"
DIM = "\033[2m"
BOLD = "\033[1m"

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def sma(series, period):
    return series.rolling(period).mean()

def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))

def safe_float(val):
    try:
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except:
        return None

def clean_nan(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    return obj

def cr(val):
    """Convert raw value to Crores string."""
    if val is None: return 'N/A'
    return f"₹{val/1e7:.1f}Cr"

def raise_file_limit():
    if resource is None:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = min(max(soft, FD_SOFT_TARGET), hard)
        if target > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
            print(f"Open-file limit: {soft} -> {target}")
    except Exception as exc:
        print(f"⚠ Could not raise open-file limit: {exc}")

def configure_yfinance():
    cache_dir = Path('.cache') / 'yfinance'
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        yf.set_tz_cache_location(str(cache_dir))
    except Exception as exc:
        print(f"⚠ yfinance cache setup issue: {exc}")
    try:
        network_config = getattr(getattr(yf, "config", None), "network", None)
        if network_config is not None and hasattr(network_config, "retries"):
            network_config.retries = 2
        else:
            yf.set_config(retries=2)
    except Exception:
        pass
    try:
        yf.config.debug.hide_exceptions = True
        yf_logger = logging.getLogger('yfinance')
        yf_logger.setLevel(logging.CRITICAL)
        yf_logger.propagate = False
    except Exception:
        pass

def init_yf_session():
    global YF_SHARED_SESSION
    if curl_requests is None:
        return
    try:
        YF_SHARED_SESSION = curl_requests.Session(impersonate='chrome')
    except Exception as exc:
        print(f"⚠ yfinance session setup issue: {exc}")
        YF_SHARED_SESSION = None

def close_yf_session():
    global YF_SHARED_SESSION
    if YF_SHARED_SESSION is None:
        return
    try:
        YF_SHARED_SESSION.close()
    except Exception:
        pass
    YF_SHARED_SESSION = None

def close_ticker_sessions(stock):
    if stock is None:
        return
    try:
        history_obj = getattr(stock, '_price_history', None)
        history_session = getattr(history_obj, 'session', None)
        if history_session is not None and history_session is not YF_SHARED_SESSION:
            history_session.close()
    except Exception:
        pass
    try:
        stock_session = getattr(stock, 'session', None)
        if stock_session is not None and stock_session is not YF_SHARED_SESSION:
            stock_session.close()
    except Exception:
        pass

raise_file_limit()
configure_yfinance()
init_yf_session()
atexit.register(close_yf_session)

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — GET TICKER LIST (NSE ONLY)
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 65)
print("  INDIA PRO STOCK SCANNER  —  NSE ONLY")
print("=" * 65)

NSE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}

all_symbols = {}   # symbol -> {name, exchange, fyers}

# NSE equity list
print("\n📡 Fetching NSE equity list...")
try:
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=10)
    r = session.get(
        "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
        headers=NSE_HEADERS, timeout=20
    )
    nse_df = pd.read_csv(pd.io.common.StringIO(r.text))
    nse_df.columns = [c.strip() for c in nse_df.columns]
    sym_col  = next((c for c in nse_df.columns if 'symbol' in c.lower()), nse_df.columns[0])
    name_col = next((c for c in nse_df.columns if 'name' in c.lower()), nse_df.columns[1])
    for _, row in nse_df.iterrows():
        sym = str(row[sym_col]).strip()
        name = str(row[name_col]).strip() if name_col in row.index else sym
        if sym and sym != 'nan':
            all_symbols[sym] = {'name': name, 'exchange': 'NSE', 'fyers': f"NSE:{sym}-EQ"}
    print(f"   ✅ NSE: {len(all_symbols)} symbols loaded")
except Exception as e:
    print(f"   ❌ NSE fetch failed: {e}")

# Fallback: Nifty 500 if main fetch failed
if len(all_symbols) < 100:
    print("📡 Falling back to Nifty 500...")
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=10)
        r = session.get(
            "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
            headers=NSE_HEADERS, timeout=20
        )
        df = pd.read_csv(pd.io.common.StringIO(r.text))
        for _, row in df.iterrows():
            sym = str(row.get('Symbol', '')).strip()
            name = str(row.get('Company Name', sym)).strip()
            if sym:
                all_symbols[sym] = {'name': name, 'exchange': 'NSE', 'fyers': f"NSE:{sym}-EQ"}
        print(f"   ✅ Nifty 500 fallback: {len(all_symbols)} symbols")
    except Exception as e:
        print(f"   ❌ Nifty 500 fallback failed: {e}")

tickers = sorted(all_symbols.keys())
print(f"\n📊 Total unique symbols: {len(tickers)}")

def primary_fail_codes(failed_value):
    if not failed_value:
        return []
    return [code.strip() for code in failed_value.split('|') if code.strip() in PRIMARY_CODES]

def should_rescan(entry):
    if entry is None:
        return True
    if (entry.get('score') or 0) >= 3:
        return True
    if len(primary_fail_codes(entry.get('failed', ''))) == 1:
        return True
    if entry.get('date') is None:
        return True
    return (TODAY - entry['date']).days > RESCAN_AFTER_DAYS

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — INIT FYERS
# ══════════════════════════════════════════════════════════════════════════════
fyers = None
if FYERS_AVAILABLE and FYERS_CLIENT_ID and FYERS_ACCESS_TOKEN:
    try:
        fyers = fyersModel.FyersModel(
            client_id=FYERS_CLIENT_ID,
            token=FYERS_ACCESS_TOKEN,
            log_path=""
        )
        # Test connection
        profile = fyers.get_profile()
        if profile.get('code') == 200:
            print(f"✅ Fyers connected: {profile['data']['name']}")
        else:
            print(f"⚠  Fyers auth issue: {profile.get('message')}. Falling back to yfinance.")
            fyers = None
    except Exception as e:
        print(f"⚠  Fyers init failed: {e}. Falling back to yfinance.")
        fyers = None
else:
    print("ℹ  Using yfinance for price data (FYERS credentials not configured)")

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — RESUME SUPPORT
# ══════════════════════════════════════════════════════════════════════════════
state_store = ScanStateStore(STATE_DB, market='IN', log_date_format=LOG_DATE_FORMAT)
archive_store = ScanStateStore(ARCHIVE_DB, market='IN', log_date_format=LOG_DATE_FORMAT)
legacy_migration = state_store.migrate_legacy_log(LEGACY_SCANNED_LOG)
if legacy_migration.get('migrated'):
    print(
        f"🗄️ Migrated {legacy_migration['entries']} legacy India scan entries "
        f"for {legacy_migration['tickers']} symbols into {STATE_DB}"
    )
archive_legacy_migration = archive_store.migrate_legacy_log(LEGACY_SCANNED_LOG)
if archive_legacy_migration.get('migrated'):
    print(
        f"🗄️ Archived {archive_legacy_migration['entries']} legacy India scan entries "
        f"for {archive_legacy_migration['tickers']} symbols into {ARCHIVE_DB}"
    )
elif not archive_store.has_market_history() and state_store.has_market_history():
    archive_backfill = 0
    for row in state_store.load_history_rows():
        archive_store.record_scan(
            ticker=row['ticker'],
            score=row['score'],
            failed=row['failed'],
            run_id=None,
            scan_date=row['scan_date'],
            scanned_at=row['scanned_at'],
            source=row.get('source') or 'state_backfill',
            payload=json.loads(row['payload_json']) if row.get('payload_json') else None,
        )
        archive_backfill += 1
    if archive_backfill:
        print(f"🗄️ Backfilled {archive_backfill} India historical scan events into {ARCHIVE_DB}")

scan_history = state_store.load_latest_state()
scanned = set(scan_history)
if scan_history:
    print(f"🔁 Resuming — loaded scan history for {len(scan_history)} symbols")

if os.path.exists(OUTPUT_JSON):
    with open(OUTPUT_JSON) as f:
        results = json.load(f).get('stocks', [])
    print(f"📂 Loaded {len(results)} existing results")
else:
    results = []

remaining = [t for t in tickers if should_rescan(scan_history.get(t))]
chunks    = [remaining[i:i+CHUNK_SIZE] for i in range(0, len(remaining), CHUNK_SIZE)]
print(f"⏳ Remaining: {len(remaining)} symbols | {len(chunks)} chunks\n")

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — HISTORICAL DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════
def fetch_history_fyers(fyers_sym, days=400):
    """Fetch daily OHLCV from Fyers API."""
    date_to   = datetime.now()
    date_from = date_to - timedelta(days=days)
    data = {
        "symbol":    fyers_sym,
        "resolution": "D",
        "date_from": date_from.strftime("%Y-%m-%d"),
        "date_to":   date_to.strftime("%Y-%m-%d"),
        "cont_flag": "1"
    }
    resp = fyers.history(data=data)
    if resp.get('code') != 200 or not resp.get('candles'):
        return None
    candles = resp['candles']
    df = pd.DataFrame(candles, columns=['ts','open','high','low','close','volume'])
    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    df.set_index('ts', inplace=True)
    return df

def fetch_history_yfinance(symbol, exchange='NSE', days=400):
    """Fetch daily OHLCV from yfinance as fallback."""
    suffix = '.NS' if exchange == 'NSE' else '.BO'
    tk = None
    try:
        tk = yf.Ticker(symbol + suffix, session=YF_SHARED_SESSION) if YF_SHARED_SESSION else yf.Ticker(symbol + suffix)
        hist = tk.history(period='2y', interval='1d', timeout=20)
        if hist.empty or len(hist) < 50:
            return None
        hist.columns = [c.lower() for c in hist.columns]
        return hist[['open','high','low','close','volume']]
    finally:
        close_ticker_sessions(tk)

def fetch_quotes_fyers(fyers_sym):
    """Get real-time quote including upper/lower circuit."""
    try:
        resp = fyers.quotes({"symbols": fyers_sym})
        if resp.get('code') == 200 and resp.get('d'):
            q = resp['d'][0]['v']
            return {
                'upper_circuit': safe_float(q.get('upper_circuit_limit')),
                'lower_circuit': safe_float(q.get('lower_circuit_limit')),
                'ltp':           safe_float(q.get('lp')),
                'prev_close':    safe_float(q.get('prev_close_price')),
            }
    except:
        pass
    return {}

def fetch_delivery_nse(symbol):
    """Fetch delivery % from NSE API."""
    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=5)
        r = session.get(
            f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info",
            headers=NSE_HEADERS, timeout=8
        )
        data = r.json()
        delivery = data.get('marketDeptOrderBook', {}).get('tradeInfo', {}).get('deliveryToTradedQuantity')
        return safe_float(delivery)
    except:
        return None

def fetch_fundamentals_yfinance(symbol, exchange='NSE'):
    """Get EPS growth, revenue growth, PE etc from yfinance."""
    suffix = '.NS' if exchange == 'NSE' else '.BO'
    tk = None
    try:
        tk = yf.Ticker(symbol + suffix, session=YF_SHARED_SESSION) if YF_SHARED_SESSION else yf.Ticker(symbol + suffix)
        info = tk.info
        return {
            'pe':            safe_float(info.get('trailingPE')),
            'forward_pe':    safe_float(info.get('forwardPE')),
            'profit_margin': safe_float(info.get('profitMargins')),
            'eps_growth':    safe_float(info.get('earningsGrowth') or info.get('earningsQuarterlyGrowth')),
            'rev_growth':    safe_float(info.get('revenueGrowth')),
            'market_cap':    safe_float(info.get('marketCap')),
            'beta':          safe_float(info.get('beta')),
            'sector':        info.get('sector', 'N/A'),
            'industry':      info.get('industry', 'N/A'),
            'name':          info.get('longName') or info.get('shortName') or symbol,
        }
    except:
        return {}
    finally:
        close_ticker_sessions(tk)

results_lock = Lock()
scanned_lock = Lock()
print_lock = Lock()

def supports_color():
    return os.isatty(1) and os.environ.get("TERM", "") != "dumb"

def colorize(text, color, bold=False, dim=False):
    if not supports_color():
        return text
    codes = []
    if bold:
        codes.append("1")
    if dim:
        codes.append("2")
    codes.append(color[2:-1])
    return f"\033[{';'.join(codes)}m{text}{RESET}"

def format_bar(score, max_score):
    filled = colorize("★" * score, ORANGE, bold=True)
    empty = colorize("☆" * max(0, max_score - score), CYAN, dim=True)
    return f"{filled}{empty}"

def format_chunk_header(current, total):
    return (
        f"\n{colorize('===', CYAN, bold=True)} "
        f"{colorize('Chunk', ORANGE, bold=True)} "
        f"{colorize(str(current), ORANGE, bold=True)}/"
        f"{colorize(str(total), CYAN, bold=True)} "
        f"{colorize('===', CYAN, bold=True)}"
    )

def format_qualified_line(symbol, result):
    bar = format_bar(result["score"], result["max_score"])
    price_text = colorize(f"₹{result['price']}", CYAN, bold=True)
    score_text = colorize(f"{result['score']}/{result['max_score']}", CYAN, bold=True)
    symbol_text = colorize(symbol, ORANGE, bold=True)
    sigs = ", ".join(result["signals"]) or "qualified (no signals)"
    return f"  [{bar}] {symbol_text}: {score_text} | {price_text} | {sigs}"

def format_saved_line(total_results, qualifiers):
    saved_text = colorize(f"Saved {total_results} qualified stocks", CYAN, bold=True)
    qualifier_text = colorize(f"{qualifiers} with score ≥ 5", ORANGE, bold=True)
    return f"  {colorize('>>', CYAN, bold=True)} {saved_text} | {qualifier_text}"

def format_sleep_line(pause_seconds):
    return f"  {colorize('💤', CYAN, bold=True)} {colorize(f'Sleeping {pause_seconds}s...', CYAN, bold=True)}"

def safe_print(message):
    with print_lock:
        print(message)

atexit.register(state_store.close)
atexit.register(archive_store.close)

def set_ticker_result(symbol, result=None):
    with results_lock:
        results[:] = [existing for existing in results if existing.get('ticker') != symbol]
        if result is not None:
            results.append(result)

def mark_scanned(symbol, score=0, failed='', payload=None):
    with scanned_lock:
        scanned.add(symbol)
        scan_history[symbol] = {
            'ticker': symbol,
            'score': score,
            'failed': failed,
            'date': TODAY,
        }

    try:
        state_store.record_scan(
            ticker=symbol,
            score=score,
            failed=failed,
            run_id=run_id,
            scan_date=TODAY,
            payload=payload,
        )
    except Exception as exc:
        safe_print(f"  ⚠  {symbol}: scan state write failed ({exc})")
    try:
        archive_store.record_scan(
            ticker=symbol,
            score=score,
            failed=failed,
            run_id=archive_run_id,
            scan_date=TODAY,
            payload=payload,
        )
    except Exception as exc:
        safe_print(f"  ⚠  {symbol}: archive write failed ({exc})")

def save_daily_snapshot(output_json):
    output_path = Path(output_json)
    if not output_path.exists():
        return

    snapshot_path = output_path.with_name(f"{output_path.stem}_{RUN_DATE}.json")
    shutil.copyfile(output_path, snapshot_path)
    try:
        state_store.record_snapshot(
            snapshot_date=RUN_DATE,
            file_path=str(snapshot_path),
            run_id=run_id,
        )
    except Exception as exc:
        safe_print(f"  ⚠  snapshot state write failed: {exc}")
    try:
        archive_store.record_snapshot(
            snapshot_date=RUN_DATE,
            file_path=str(snapshot_path),
            run_id=archive_run_id,
        )
    except Exception as exc:
        safe_print(f"  ⚠  snapshot archive write failed: {exc}")

    snapshots = []
    prefix = f"{output_path.stem}_"
    for candidate in output_path.parent.glob(f"{output_path.stem}_*.json"):
        name = candidate.name
        if not name.startswith(prefix) or not name.endswith('.json'):
            continue
        date_str = name[len(prefix):-5]
        try:
            snap_date = datetime.strptime(date_str, LOG_DATE_FORMAT).date()
        except Exception:
            continue
        snapshots.append((snap_date, candidate))

    snapshots.sort(key=lambda item: item[0], reverse=True)
    for _, old_path in snapshots[SNAPSHOT_KEEP:]:
        try:
            old_path.unlink()
        except Exception:
            pass

def save_payload_atomic(payload):
    tmp = OUTPUT_JSON + '.tmp'
    for attempt in range(3):
        try:
            with open(tmp, 'w') as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, OUTPUT_JSON)
            return True
        except OSError as exc:
            if exc.errno == errno.EMFILE and attempt < 2:
                gc.collect()
                time.sleep(0.3 * (attempt + 1))
                continue
            safe_print(f"  ⚠  save failed: {exc}")
            return False
    return False

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 5 — SCORE ONE TICKER
# ══════════════════════════════════════════════════════════════════════════════
def score_ticker(symbol):
    meta     = all_symbols.get(symbol, {})
    exchange = meta.get('exchange', 'NSE')
    fyers_sym = meta.get('fyers', f"NSE:{symbol}-EQ")

    # ── Fetch OHLCV history ───────────────────────────────────────────────────
    hist = None
    if fyers:
        hist = fetch_history_fyers(fyers_sym)
    if hist is None:
        hist = fetch_history_yfinance(symbol, exchange)
    if hist is None or len(hist) < 60:
        return None

    close  = hist['close']
    high   = hist['high']
    volume = hist['volume']
    price  = float(close.iloc[-1])
    closes_30d = [round(float(x), 2) for x in close.tail(30).tolist()]

    # ── PRIMARY CONDITIONS (all 5 must pass) ─────────────────────────────────
    lookback   = min(HIGH_LOOKBACK, len(high))
    high_250   = float(high.iloc[-lookback:].max())
    ema21_val  = float(ema(close, EMA21_PERIOD).iloc[-1])
    ema50_val  = float(ema(close, EMA50_PERIOD).iloc[-1])

    # P4: long-period indicator needs enough history
    ema200_val = float(ema(close, EMA200_PERIOD).iloc[-1]) if len(close) >= EMA200_PERIOD else None

    # P5: configured turnover floor
    turnover     = close * volume
    avg_turnover = float(sma(turnover, TURNOVER_WINDOW).iloc[-1])   # in ₹
    avg_turnover_cr = avg_turnover / 1e7               # convert to Crores

    # Apply filters
    failed_conditions = []
    if price < high_250 * PRICE_FROM_HIGH:
        failed_conditions.append(('P1', f"P1 price ₹{price:.2f} < configured floor ₹{high_250 * PRICE_FROM_HIGH:.2f}"))
    if price < ema21_val:
        failed_conditions.append(('P2', f"P2 price ₹{price:.2f} < EMA{EMA21_PERIOD} ₹{ema21_val:.2f}"))
    if price < ema50_val:
        failed_conditions.append(('P3', f"P3 price ₹{price:.2f} < EMA{EMA50_PERIOD} ₹{ema50_val:.2f}"))
    if ema200_val and price < ema200_val:
        failed_conditions.append(('P4', f"P4 price ₹{price:.2f} < EMA{EMA200_PERIOD} ₹{ema200_val:.2f}"))
    if avg_turnover_cr < MIN_TURNOVER_CR:
        failed_conditions.append(('P5', f"P5 turnover ₹{avg_turnover_cr:.2f}Cr < configured floor ₹{MIN_TURNOVER_CR}Cr"))
    if failed_conditions:
        return {
            '_failed': failed_conditions[0][1],
            '_failed_codes': [code for code, _ in failed_conditions],
        }

    # ── SCORING ───────────────────────────────────────────────────────────────
    score   = 0
    signals = []
    details = {}

    # T1: configured near-high band
    pct_from_high = (high_250 - price) / high_250 * 100
    details['52W High']        = round(high_250, 2)
    details['% From 52W High'] = round(pct_from_high, 2)
    if pct_from_high <= NEAR_HIGH_PCT:
        score += 1
        signals.append('Near 52W High')

    # T2: configured short-trend confirmation
    details['EMA21'] = round(ema21_val, 2)
    if price > ema21_val:
        score += 1
        signals.append('Above EMA21')

    # T3: configured medium-trend confirmation
    details['EMA50'] = round(ema50_val, 2)
    if price > ema50_val:
        score += 1
        signals.append('Above EMA50')

    # T4: configured long-trend confirmation
    if ema200_val:
        details['EMA200'] = round(ema200_val, 2)
        if price > ema200_val:
            score += 1
            signals.append('Above EMA200')
    else:
        details['EMA200'] = 'N/A'

    # T5: configured activity expansion check
    avg_vol   = float(sma(volume, TURNOVER_WINDOW).iloc[-1])
    last_vol  = float(volume.iloc[-1])
    vol_ratio = round(last_vol / avg_vol, 2) if avg_vol > 0 else 0
    details['Volume Ratio']    = vol_ratio
    details['Avg Volume 20D']  = int(avg_vol)
    details['Latest Volume']   = int(last_vol)
    details['Avg Turnover Cr'] = round(avg_turnover_cr, 1)
    if vol_ratio >= VOLUME_SPIKE_RATIO:
        score += 1
        signals.append(f'Volume Spike ({vol_ratio}x)')

    # T6: Near Upper Circuit — needs real-time quote from Fyers
    circuit_data  = {}
    near_circuit  = False
    upper_circuit = None
    if fyers:
        circuit_data  = fetch_quotes_fyers(fyers_sym)
        upper_circuit = circuit_data.get('upper_circuit')
        if upper_circuit and upper_circuit > 0:
            pct_to_circuit = (upper_circuit - price) / upper_circuit * 100
            details['Upper Circuit'] = round(upper_circuit, 2)
            details['% To Circuit']  = round(pct_to_circuit, 2)
            if pct_to_circuit <= UPPER_CIRCUIT_PCT:
                score += 1
                signals.append(f'Near Upper Circuit ({pct_to_circuit:.1f}%)')
                near_circuit = True
        else:
            details['Upper Circuit'] = 'N/A'
    else:
        details['Upper Circuit'] = 'N/A (Fyers not connected)'

    # T7: configured delivery floor (NSE only)
    delivery_pct = fetch_delivery_nse(symbol)
    details['Delivery %'] = f"{delivery_pct:.1f}%" if delivery_pct else 'N/A'
    if delivery_pct and delivery_pct >= DELIVERY_PCT_MIN:
        score += 1
        signals.append(f'Delivery {delivery_pct:.1f}%')

    # RSI (display only)
    rsi_s = calc_rsi(close)
    rsi   = round(float(rsi_s.iloc[-1]), 1) if not rsi_s.isna().iloc[-1] else None
    details['RSI'] = rsi

    # 52W Low
    low_52w = float(high.iloc[-lookback:].min()) if len(high) >= lookback else float(high.min())

    # ── FUNDAMENTALS (yfinance) ───────────────────────────────────────────────
    fund = fetch_fundamentals_yfinance(symbol, exchange)

    # F1: configured EPS growth floor
    eps_growth = fund.get('eps_growth')
    details['EPS Growth'] = f"{round(eps_growth*100,1)}%" if eps_growth is not None else 'N/A'
    if eps_growth is not None and eps_growth > EPS_GROWTH_MIN:
        score += 1
        signals.append(f'EPS Growth {round(eps_growth*100,1)}%')

    # F2: configured revenue growth floor
    rev_growth = fund.get('rev_growth')
    details['Revenue Growth'] = f"{round(rev_growth*100,1)}%" if rev_growth is not None else 'N/A'
    if rev_growth is not None and rev_growth > REVENUE_GROWTH_MIN:
        score += 1
        signals.append(f'Rev Growth {round(rev_growth*100,1)}%')

    # Market cap in Crores
    market_cap_raw = fund.get('market_cap')
    market_cap_cr  = round(market_cap_raw / 1e7, 1) if market_cap_raw else None

    # Categorise
    cap_cat = 'N/A'
    if market_cap_cr:
        if market_cap_cr < 5000:     cap_cat = 'Smallcap'
        elif market_cap_cr < 20000:  cap_cat = 'Midcap'
        else:                        cap_cat = 'Largecap'

    # Profit margin %
    pm = fund.get('profit_margin')
    margin_pct = round(pm * 100, 1) if pm is not None else None

    return {
        'ticker':        symbol,
        'name':          fund.get('name', meta.get('name', symbol)),
        'exchange':      exchange,
        'sector':        fund.get('sector', 'N/A'),
        'industry':      fund.get('industry', 'N/A'),
        'price':         round(price, 2),
        'closes_30d':    closes_30d,
        'market_cap_cr': market_cap_cr,
        'cap_category':  cap_cat,
        'pe':            round(fund.get('pe') or 0, 2) or None,
        'forward_pe':    round(fund.get('forward_pe') or 0, 2) or None,
        'profit_margin': margin_pct,
        'beta':          fund.get('beta'),
        'high_52w':      round(high_250, 2),
        'low_52w':       round(low_52w, 2),
        'avg_turnover_cr': round(avg_turnover_cr, 1),
        'score':         score,
        'max_score':     9,
        'primary_checks': {'P1': True, 'P2': True, 'P3': True, 'P4': True, 'P5': True},
        'signals':       signals,
        'details':       details,
        'market':        'IN',
        'currency':      '₹',
        'scanned_at':    datetime.now().strftime('%Y-%m-%d %H:%M'),
    }

def process_ticker(symbol):
    try:
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                result = score_ticker(symbol)

                if result and '_failed' not in result:
                    set_ticker_result(symbol, result)
                    safe_print(format_qualified_line(symbol, result))
                    mark_scanned(symbol, score=result['score'], failed='', payload=result)
                elif result and '_failed' in result:
                    set_ticker_result(symbol)
                    safe_print(f"  [─────────] {symbol}: failed primary conditions")
                    mark_scanned(
                        symbol,
                        score=0,
                        failed='|'.join(result.get('_failed_codes', [])),
                        payload={
                            'status': 'failed_primary',
                            'message': result.get('_failed'),
                            'failed_codes': result.get('_failed_codes', []),
                        },
                    )
                else:
                    set_ticker_result(symbol)
                    safe_print(f"  [─────────] {symbol}: failed primary conditions")
                    mark_scanned(
                        symbol,
                        score=0,
                        failed='NO_DATA',
                        payload={'status': 'no_data'},
                    )
                break

            except Exception as e:
                err = str(e)
                lower_err = err.lower()
                retryable = (
                    '429' in err or
                    'too many' in lower_err or
                    'rate limit' in lower_err or
                    'nonetype' in lower_err or
                    'unable to open database file' in lower_err
                )
                if retryable:
                    retries += 1
                    if retries <= MAX_RETRIES:
                        wait_s = RETRY_DELAY if ('429' in err or 'too many' in lower_err) else min(5 * retries, RETRY_DELAY)
                        safe_print(f"  ⚠  {symbol}: transient fetch error — waiting {wait_s}s (retry {retries}/{MAX_RETRIES})")
                        gc.collect()
                        time.sleep(wait_s)
                        continue
                else:
                    safe_print(f"  ✗  {symbol}: {err}")
                mark_scanned(
                    symbol,
                    score=0,
                    failed='ERROR',
                    payload={'status': 'error', 'message': err},
                )
                break
    finally:
        time.sleep(random.uniform(DELAY_BETWEEN, DELAY_BETWEEN + 0.3))

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 6 — MAIN SCAN LOOP
# ══════════════════════════════════════════════════════════════════════════════
total_chunks = len(chunks)
try:
    run_id = state_store.start_run(run_date=RUN_DATE)
except Exception as exc:
    safe_print(f"  ⚠  state run start failed ({exc})")
    run_id = None
try:
    archive_run_id = archive_store.start_run(run_date=RUN_DATE)
except Exception as exc:
    safe_print(f"  ⚠  archive run start failed ({exc})")
    archive_run_id = None

for chunk_num, chunk in enumerate(chunks):
    print(format_chunk_header(chunk_num + 1, total_chunks))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_ticker, symbol) for symbol in chunk]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                safe_print(f"  ⚠  worker error: {exc}")

    # ── Atomic save ───────────────────────────────────────────────────────────
    payload = clean_nan({
        'generated_at':  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_scanned': len(scanned),
        'total_results': len(results),
        'market':        'IN',
        'stocks':        sorted(results, key=lambda x: x['score'], reverse=True),
    })
    save_payload_atomic(payload)

    qualifiers = len([r for r in results if r['score'] >= 5])
    print(format_saved_line(len(results), qualifiers))

    if chunk_num < total_chunks - 1:
        print(format_sleep_line(CHUNK_PAUSE))
        time.sleep(CHUNK_PAUSE)

try:
    save_daily_snapshot(OUTPUT_JSON)
except Exception as exc:
    safe_print(f"  ⚠  snapshot save failed ({exc})")
try:
    state_store.finish_run(
        run_id,
        status='completed',
        total_scanned=len(scanned),
        total_results=len(results),
    )
except Exception as exc:
    safe_print(f"  ⚠  state run finalization failed ({exc})")
try:
    archive_store.finish_run(
        archive_run_id,
        status='completed',
        total_scanned=len(scanned),
        total_results=len(results),
    )
except Exception as exc:
    safe_print(f"  ⚠  archive run finalization failed ({exc})")

# ── Final ─────────────────────────────────────────────────────────────────────
print(f"\n{'='*65}")
print(f"  ✅  INDIA SCAN COMPLETE")
print(f"{'='*65}")
print(f"  Total scanned      : {len(scanned)}")
print(f"  Passed conditions  : {len(results)}")
print(f"  Score = 9          : {len([r for r in results if r['score'] == 9])}")
print(f"  Score >= 7         : {len([r for r in results if r['score'] >= 7])}")
print(f"  Score >= 5         : {len([r for r in results if r['score'] >= 5])}")
print(f"\n  Open dashboard.html → http://localhost:8000/dashboard.html")
