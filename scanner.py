"""
╔══════════════════════════════════════════════════════════════╗
║         US STOCK MOMENTUM + FUNDAMENTAL SCANNER  v3          ║
║         Reads: us_stocks_100m_10b_full.csv                   ║
║         Output: scanner_results.json                         ║
╠══════════════════════════════════════════════════════════════╣
║  Primary gates and scoring rules are loaded from a local     ║
║  secure conditions file and are not embedded in tracked code.║
║                                                              ║
║  Runtime still evaluates those rules locally during scans.   ║
╚══════════════════════════════════════════════════════════════╝

Speed:
  Static fields (name, sector, PE, margin, beta, 52W hi/lo) → read from CSV
  yfinance called ONLY for:
    - 1yr daily OHLCV  → EMAs, volume, RSI, live price
    - earningsGrowth / revenueGrowth  (not in CSV)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json, time, random, os, shutil, gc, atexit
import errno
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

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

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_CSV     = 'us_stocks_100m_10b_full.csv'
OUTPUT_JSON   = 'scanner_results.json'
STATE_DB      = 'scan_state.db'
ARCHIVE_DB    = 'scan_mega_history.db'
LEGACY_SCANNED_LOG = 'scanner_scanned.txt'
CHUNK_SIZE    = 20
DELAY_BETWEEN = 0.6
CHUNK_PAUSE   = 5
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

RESET = '\033[0m'
CYAN = '\033[38;5;51m'
ORANGE = '\033[38;5;214m'
DIM = '\033[2m'
BOLD = '\033[1m'

SCAN_CONDITIONS    = load_scan_conditions("US")
PRIMARY_CONFIG     = SCAN_CONDITIONS["primary"]
SCORING_CONFIG     = SCAN_CONDITIONS["scoring"]
HIGH_LOOKBACK      = int(PRIMARY_CONFIG["high_lookback"])
PRICE_FROM_HIGH    = float(PRIMARY_CONFIG["price_from_high"])
TURNOVER_WINDOW    = int(PRIMARY_CONFIG["turnover_window"])
EMA21_PERIOD, EMA50_PERIOD, EMA200_PERIOD = [int(v) for v in PRIMARY_CONFIG["ema_periods"]]
MIN_TURNOVER_M     = float(PRIMARY_CONFIG["min_turnover_m"] or 0)
NEAR_HIGH_PCT      = float(SCORING_CONFIG["near_high_pct"])
VOLUME_SPIKE_RATIO = float(SCORING_CONFIG["volume_spike_ratio"])
EPS_GROWTH_MIN     = float(SCORING_CONFIG["eps_growth_min"])
REVENUE_GROWTH_MIN = float(SCORING_CONFIG["revenue_growth_min"])

# ── Load CSV ──────────────────────────────────────────────────────────────────
print("=" * 62)
print("  US STOCK MOMENTUM + FUNDAMENTAL SCANNER  v3")
print("  (Primary conditions + EMA filter)")
print("=" * 62)

if not os.path.exists(INPUT_CSV):
    print(f"ERROR: {INPUT_CSV} not found. Run the market-cap downloader first.")
    raise SystemExit

df_csv = pd.read_csv(INPUT_CSV)
df_csv.columns = [c.strip() for c in df_csv.columns]
col_map = {c.lower(): c for c in df_csv.columns}

def col(name):
    return col_map.get(name.lower())

ticker_col = col('ticker') or df_csv.columns[0]
df_csv = df_csv.set_index(ticker_col)

all_tickers = [
    t for t in df_csv.index.tolist()
    if isinstance(t, str) and t.replace('-', '').isalpha() and len(t) <= 5
]
print(f"\nLoaded {len(all_tickers)} tickers from {INPUT_CSV}")
print(f"CSV columns: {list(df_csv.columns)}\n")

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

raise_file_limit()
configure_yfinance()

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

# ── Resume ────────────────────────────────────────────────────────────────────
state_store = ScanStateStore(STATE_DB, market='US', log_date_format=LOG_DATE_FORMAT)
archive_store = ScanStateStore(ARCHIVE_DB, market='US', log_date_format=LOG_DATE_FORMAT)
legacy_migration = state_store.migrate_legacy_log(LEGACY_SCANNED_LOG)
if legacy_migration.get('migrated'):
    print(
        f"Migrated {legacy_migration['entries']} legacy US scan entries "
        f"for {legacy_migration['tickers']} tickers into {STATE_DB}"
    )
archive_legacy_migration = archive_store.migrate_legacy_log(LEGACY_SCANNED_LOG)
if archive_legacy_migration.get('migrated'):
    print(
        f"Archived {archive_legacy_migration['entries']} legacy US scan entries "
        f"for {archive_legacy_migration['tickers']} tickers into {ARCHIVE_DB}"
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
        print(f"Backfilled {archive_backfill} US historical scan events into {ARCHIVE_DB}")

scan_history = state_store.load_latest_state()
scanned = set(scan_history)
if scan_history:
    print(f"Resuming — loaded scan history for {len(scan_history)} tickers")

if os.path.exists(OUTPUT_JSON):
    with open(OUTPUT_JSON) as f:
        try:
            results = json.load(f).get('stocks', [])
        except Exception:
            results = []
    print(f"Loaded {len(results)} existing results")
else:
    results = []

remaining = [t for t in all_tickers if should_rescan(scan_history.get(t))]
chunks    = [remaining[i:i+CHUNK_SIZE] for i in range(0, len(remaining), CHUNK_SIZE)]
print(f"Remaining: {len(remaining)} tickers | {len(chunks)} chunks\n")

# ── Helpers ───────────────────────────────────────────────────────────────────
def ema(series, period):
    """Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series, period=14):
    """RSI indicator."""
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))

def clean_for_json(obj):
    """Recursively replace NaN/Inf with None for valid JSON."""
    if isinstance(obj, float):
        return None if (obj != obj or obj == float('inf') or obj == float('-inf')) else obj
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(i) for i in obj]
    return obj

def safe_float(val):
    try:
        f = float(val)
        return None if (np.isnan(f) or np.isinf(f)) else f
    except Exception:
        return None

def from_csv(ticker, *keys):
    if ticker not in df_csv.index:
        return None
    row = df_csv.loc[ticker]
    for k in keys:
        c = col(k)
        if c and c in row.index:
            return safe_float(row[c])
    return None

def str_from_csv(ticker, *keys):
    if ticker not in df_csv.index:
        return 'N/A'
    row = df_csv.loc[ticker]
    for k in keys:
        c = col(k)
        if c and c in row.index:
            v = row[c]
            return str(v) if pd.notna(v) else 'N/A'
    return 'N/A'

results_lock = Lock()
scanned_lock = Lock()
print_lock = Lock()
YF_SHARED_SESSION = None

def supports_color():
    return os.isatty(1) and os.environ.get('TERM', '') != 'dumb'

def colorize(text, color, bold=False, dim=False):
    if not supports_color():
        return text
    codes = []
    if bold:
        codes.append('1')
    if dim:
        codes.append('2')
    codes.append(color[2:-1])
    return f"\033[{';'.join(codes)}m{text}{RESET}"

def format_bar(score, max_score):
    filled = colorize('★' * score, ORANGE, bold=True)
    empty = colorize('☆' * max(0, max_score - score), CYAN, dim=True)
    return f"{filled}{empty}"

def format_chunk_header(current, total):
    return (
        f"{colorize('===', CYAN, bold=True)} "
        f"{colorize('Chunk', ORANGE, bold=True)} "
        f"{colorize(str(current), ORANGE, bold=True)}/"
        f"{colorize(str(total), CYAN, bold=True)} "
        f"{colorize('===', CYAN, bold=True)}"
    )

def format_qualified_line(ticker, result):
    bar = format_bar(result['score'], result['max_score'])
    price_text = colorize(f"${result['price']}", CYAN, bold=True)
    score_text = colorize(f"{result['score']}/{result['max_score']}", CYAN, bold=True)
    ticker_text = colorize(ticker, ORANGE, bold=True)
    sigs = ', '.join(result['signals']) or 'qualified (no signals)'
    return f"  [{bar}] {ticker_text}: {score_text} | {price_text} | {sigs}"

def format_saved_line(total_results, qualifiers):
    saved_text = colorize(f"Saved {total_results} qualified stocks", CYAN, bold=True)
    qualifier_text = colorize(f"{qualifiers} with score ≥ 5", ORANGE, bold=True)
    return f"  {colorize('>>', CYAN, bold=True)} {saved_text} | {qualifier_text}"

def format_sleep_line(pause_seconds):
    return f"  {colorize('💤', CYAN, bold=True)} {colorize(f'Sleeping {pause_seconds}s...', CYAN, bold=True)}"

def safe_print(message):
    with print_lock:
        print(message)

def init_yf_session():
    global YF_SHARED_SESSION
    if curl_requests is None:
        return
    try:
        YF_SHARED_SESSION = curl_requests.Session(impersonate='chrome')
    except Exception as exc:
        safe_print(f"⚠ yfinance session setup issue: {exc}")
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

init_yf_session()
atexit.register(close_yf_session)
atexit.register(state_store.close)
atexit.register(archive_store.close)

def set_ticker_result(ticker, result=None):
    with results_lock:
        results[:] = [existing for existing in results if existing.get('ticker') != ticker]
        if result is not None:
            results.append(result)

def mark_scanned(ticker, score=0, failed='', payload=None):
    with scanned_lock:
        scanned.add(ticker)
        scan_history[ticker] = {
            'ticker': ticker,
            'score': score,
            'failed': failed,
            'date': TODAY,
        }

    try:
        state_store.record_scan(
            ticker=ticker,
            score=score,
            failed=failed,
            run_id=run_id,
            scan_date=TODAY,
            payload=payload,
        )
    except Exception as exc:
        safe_print(f"  ⚠  {ticker}: scan state write failed ({exc})")
    try:
        archive_store.record_scan(
            ticker=ticker,
            score=score,
            failed=failed,
            run_id=archive_run_id,
            scan_date=TODAY,
            payload=payload,
        )
    except Exception as exc:
        safe_print(f"  ⚠  {ticker}: archive write failed ({exc})")

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

# ── Score one ticker ──────────────────────────────────────────────────────────
def score_ticker(ticker):
    # ── 1. Pull static fields from CSV (zero API cost) ────────────────────────
    name       = str_from_csv(ticker, 'company name', 'name')
    sector     = str_from_csv(ticker, 'sector')
    industry   = str_from_csv(ticker, 'industry')
    csv_cap_b  = from_csv(ticker, 'market cap (b)', 'market cap(b)', 'market_cap_b')
    csv_pe     = from_csv(ticker, 'pe ratio', 'pe_ratio', 'trailing pe')
    csv_fwd_pe = from_csv(ticker, 'forward pe', 'forward_pe')
    csv_margin = from_csv(ticker, 'profit margin', 'profit_margin')
    csv_52hi   = from_csv(ticker, '52w high', '52w_high')
    csv_52lo   = from_csv(ticker, '52w low',  '52w_low')
    csv_beta   = from_csv(ticker, 'beta')

    # ── 2. Fetch live OHLCV history ────────────────────────────────────────────
    stock = yf.Ticker(ticker, session=YF_SHARED_SESSION) if YF_SHARED_SESSION else yf.Ticker(ticker)
    hist  = stock.history(period='2y', interval='1d', timeout=20)   # enough history for long-period indicators

    if hist.empty or len(hist) < 60:
        close_ticker_sessions(stock)
        return None

    close  = hist['Close']
    high   = hist['High']
    volume = hist['Volume']
    price  = float(close.iloc[-1])
    closes_30d = [round(float(x), 2) for x in close.tail(30).tolist()]

    # ── PRIMARY CONDITIONS (gate — stock must pass all 5) ─────────────────────
    lookback   = min(HIGH_LOOKBACK, len(high))
    high_250   = float(high.iloc[-lookback:].max())

    ema21_val  = float(ema(close, EMA21_PERIOD).iloc[-1])
    ema50_val  = float(ema(close, EMA50_PERIOD).iloc[-1])
    ema200_val = float(ema(close, EMA200_PERIOD).iloc[-1]) if len(close) >= EMA200_PERIOD else None

    # P5: configured turnover floor
    turnover       = close * volume
    avg_turnover   = float(turnover.rolling(TURNOVER_WINDOW).mean().iloc[-1])
    avg_turnover_m = avg_turnover / 1_000_000          # in $millions

    # Apply all 5 gates — return None if any fails
    failed_conditions = []
    if price < high_250 * PRICE_FROM_HIGH:
        failed_conditions.append(('P1', f"P1 price ${price:.2f} < configured floor ${high_250 * PRICE_FROM_HIGH:.2f}"))
    if price < ema21_val:
        failed_conditions.append(('P2', f"P2 price ${price:.2f} < EMA{EMA21_PERIOD} ${ema21_val:.2f}"))
    if price < ema50_val:
        failed_conditions.append(('P3', f"P3 price ${price:.2f} < EMA{EMA50_PERIOD} ${ema50_val:.2f}"))
    if ema200_val and price < ema200_val:
        failed_conditions.append(('P4', f"P4 price ${price:.2f} < EMA{EMA200_PERIOD} ${ema200_val:.2f}"))
    if avg_turnover_m < MIN_TURNOVER_M:
        failed_conditions.append(('P5', f"P5 turnover ${avg_turnover_m:.2f}M < configured floor ${MIN_TURNOVER_M}M"))
    if failed_conditions:
        close_ticker_sessions(stock)
        return {
            '_failed': failed_conditions[0][1],
            '_failed_codes': [code for code, _ in failed_conditions],
        }

    # ── SCORING ───────────────────────────────────────────────────────────────
    score   = 0
    signals = []
    details = {}

    # Store all condition values in details for dashboard panel
    details['EMA21']          = round(ema21_val, 2)
    details['EMA50']          = round(ema50_val, 2)
    details['EMA200']         = round(ema200_val, 2) if ema200_val else 'N/A'
    details['Avg Turnover $M'] = round(avg_turnover_m, 1)

    # T1: configured near-high band
    live_52hi    = float(close.rolling(252).max().iloc[-1])
    high_52w     = max(live_52hi, csv_52hi or 0)
    pct_from_high = (high_52w - price) / high_52w * 100
    details['52W High']        = round(high_52w, 2)
    details['% From 52W High'] = round(pct_from_high, 2)
    if pct_from_high <= NEAR_HIGH_PCT:
        score += 1
        signals.append('Near 52W High')

    # T2: configured short-trend confirmation
    if price > ema21_val:
        score += 1
        signals.append('Above EMA21')

    # T3: configured medium-trend confirmation
    if price > ema50_val:
        score += 1
        signals.append('Above EMA50')

    # T4: configured long-trend confirmation
    if ema200_val and price > ema200_val:
        score += 1
        signals.append('Above EMA200')

    # T5: configured activity expansion check
    avg_vol   = float(volume.rolling(TURNOVER_WINDOW).mean().iloc[-1])
    last_vol  = float(volume.iloc[-1])
    vol_ratio = round(last_vol / avg_vol, 2) if avg_vol > 0 else 0
    details['Volume Ratio']   = vol_ratio
    details['Avg Volume 20D'] = int(avg_vol)
    details['Latest Volume']  = int(last_vol)
    if vol_ratio >= VOLUME_SPIKE_RATIO:
        score += 1
        signals.append(f'Volume Spike ({vol_ratio}x)')

    # RSI (display only)
    rsi_s = calc_rsi(close)
    rsi   = round(float(rsi_s.iloc[-1]), 1) if not rsi_s.isna().iloc[-1] else None
    details['RSI'] = rsi

    # ── Fundamental signals (need .info) ──────────────────────────────────────
    info = stock.info

    # F1: configured EPS growth floor
    eps_growth = safe_float(info.get('earningsGrowth') or info.get('earningsQuarterlyGrowth'))
    details['EPS Growth'] = f"{round(eps_growth*100,1)}%" if eps_growth is not None else 'N/A'
    if eps_growth is not None and eps_growth > EPS_GROWTH_MIN:
        score += 1
        signals.append(f'EPS Growth {round(eps_growth*100,1)}%')

    # F2: configured revenue growth floor
    rev_growth = safe_float(info.get('revenueGrowth'))
    details['Revenue Growth'] = f"{round(rev_growth*100,1)}%" if rev_growth is not None else 'N/A'
    if rev_growth is not None and rev_growth > REVENUE_GROWTH_MIN:
        score += 1
        signals.append(f'Rev Growth {round(rev_growth*100,1)}%')

    # 52W low
    live_52lo = float(close.rolling(252).min().iloc[-1])
    low_52w   = min(live_52lo, csv_52lo) if csv_52lo else live_52lo

    # Profit margin: CSV stores as decimal (0.12) → convert to %
    margin_pct = None
    if csv_margin is not None:
        margin_pct = round(csv_margin * 100, 1) if abs(csv_margin) <= 1 else round(csv_margin, 1)

    result = {
        'ticker':          ticker,
        'name':            name if name != 'N/A' else ticker,
        'sector':          sector,
        'industry':        industry,
        'price':           round(price, 2),
        'closes_30d':      closes_30d,
        'market_cap_b':    round(csv_cap_b, 3)  if csv_cap_b  else None,
        'pe':              round(csv_pe, 2)      if csv_pe     else None,
        'forward_pe':      round(csv_fwd_pe, 2)  if csv_fwd_pe else None,
        'profit_margin':   margin_pct,
        'beta':            round(csv_beta, 3)    if csv_beta   else None,
        'high_52w':        round(high_52w, 2),
        'low_52w':         round(low_52w, 2),
        'avg_turnover_m':  round(avg_turnover_m, 1),
        'score':           score,
        'max_score':       7,
        'primary_checks':  {'P1': True, 'P2': True, 'P3': True, 'P4': True, 'P5': True},
        'signals':         signals,
        'details':         details,
        'market':          'US',
        'currency':        '$',
        'scanned_at':      datetime.now().strftime('%Y-%m-%d %H:%M'),
    }
    close_ticker_sessions(stock)
    return result

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

def process_ticker(ticker):
    try:
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                result = score_ticker(ticker)
                if result and '_failed' not in result:
                    set_ticker_result(ticker, result)
                    safe_print(format_qualified_line(ticker, result))
                    mark_scanned(ticker, score=result['score'], failed='', payload=result)
                elif result and '_failed' in result:
                    set_ticker_result(ticker)
                    safe_print(f"  [───────] {ticker}: ✗ {result['_failed']}")
                    mark_scanned(
                        ticker,
                        score=0,
                        failed='|'.join(result.get('_failed_codes', [])),
                        payload={
                            'status': 'failed_primary',
                            'message': result.get('_failed'),
                            'failed_codes': result.get('_failed_codes', []),
                        },
                    )
                else:
                    set_ticker_result(ticker)
                    safe_print(f"  [───────] {ticker}: insufficient history")
                    mark_scanned(
                        ticker,
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
                        safe_print(f"  ⚠  {ticker}: transient fetch error — waiting {wait_s}s (retry {retries}/{MAX_RETRIES})")
                        gc.collect()
                        time.sleep(wait_s)
                        continue
                else:
                    safe_print(f"  ✗  {ticker}: {err}")
                mark_scanned(
                    ticker,
                    score=0,
                    failed='ERROR',
                    payload={'status': 'error', 'message': err},
                )
                break
    finally:
        time.sleep(random.uniform(DELAY_BETWEEN, DELAY_BETWEEN + 0.2))

# ── Main scan loop ────────────────────────────────────────────────────────────
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
        futures = [executor.submit(process_ticker, ticker) for ticker in chunk]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                safe_print(f"  ⚠  worker error: {exc}")

    # ── Atomic save ────────────────────────────────────────────────────────────
    payload = clean_for_json({
        'generated_at':  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_scanned': len(scanned),
        'total_results': len(results),
        'market':        'US',
        'stocks': sorted([r for r in results if '_failed' not in r], key=lambda x: x['score'], reverse=True),
    })
    save_payload_atomic(payload)

    hi = len([r for r in results if r['score'] >= 5])
    print(format_saved_line(len(results), hi))

    if chunk_num < total_chunks - 1:
        print(f"{format_sleep_line(CHUNK_PAUSE)}\n")
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

# ── Final summary ─────────────────────────────────────────────────────────────
print(f"\n{'='*62}")
print(f"  SCAN COMPLETE")
print(f"{'='*62}")
print(f"  Total scanned      : {len(scanned)}")
print(f"  Passed conditions  : {len(results)}")
print(f"  Score = 7          : {len([r for r in results if r['score'] == 7])}")
print(f"  Score >= 6         : {len([r for r in results if r['score'] >= 6])}")
print(f"  Score >= 5         : {len([r for r in results if r['score'] >= 5])}")
print(f"  Score >= 4         : {len([r for r in results if r['score'] >= 4])}")
print(f"\n  Open dashboard.html → http://localhost:8000/dashboard.html")
