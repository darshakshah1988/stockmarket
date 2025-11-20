import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import schedule

# ---------------- CONFIG ----------------
SYMBOL = "BTCUSD"                # BTCUSD perpetual pair
RESOLUTION = "5m"                # 5-minute candles
FETCH_LIMIT = 200                # number of candles to fetch
ST_LENGTH = 10                   # Supertrend period
ST_MULT1 = 1                     # Supertrend multiplier 1
ST_MULT2 = 3                     # Supertrend multiplier 2
VOL_ROLL = 20                    # rolling volume window
VOL_THRESHOLD_RATIO = 0.8        # volume filter (0.8 × avg)
# ----------------------------------------

# === Fetch BTCUSD data ===
def get_btcusd_data(limit=FETCH_LIMIT, resolution=RESOLUTION):
    """Fetch historical candle data from Delta Exchange (requires start & end)."""
    url = "https://api.delta.exchange/v2/history/candles"

    # each 5m candle = 300 seconds
    candle_sec = 300
    total_seconds = candle_sec * limit
    end_time = int(datetime.now(timezone.utc).timestamp())
    start_time = end_time - total_seconds

    params = {
        "symbol": SYMBOL,
        "resolution": resolution,
        "start": start_time,
        "end": end_time,
        "limit": limit
    }

    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        raise ValueError(f"HTTP {r.status_code}: {r.text}")

    data = r.json().get("result", [])
    if not data:
        raise ValueError(f"No data returned from API: {r.text}")

    df = pd.DataFrame(data)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
    df = df.sort_values('time').reset_index(drop=True)
    return df

# === Manual Supertrend Calculation ===
def add_supertrend_manual(df, length=ST_LENGTH, multiplier=ST_MULT2):
    """Manually compute Supertrend and trend direction."""
    df = df.copy()
    hl2 = (df['high'] + df['low']) / 2.0

    # True Range (TR)
    df['previous_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['previous_close']).abs()
    df['tr3'] = (df['low'] - df['previous_close']).abs()
    df['TR'] = df[['tr1','tr2','tr3']].max(axis=1)

    # ATR (simple rolling mean)
    df['ATR'] = df['TR'].rolling(window=length, min_periods=1).mean()

    # Basic bands
    df['basic_upper'] = hl2 + multiplier * df['ATR']
    df['basic_lower'] = hl2 - multiplier * df['ATR']

    # Final bands
    df['final_upper'] = df['basic_upper']
    df['final_lower'] = df['basic_lower']
    for i in range(1, len(df)):
        if (df['basic_upper'].iloc[i] < df['final_upper'].iloc[i-1]) or (df['previous_close'].iloc[i] > df['final_upper'].iloc[i-1]):
            df.at[i, 'final_upper'] = df['basic_upper'].iloc[i]
        else:
            df.at[i, 'final_upper'] = df['final_upper'].iloc[i-1]

        if (df['basic_lower'].iloc[i] > df['final_lower'].iloc[i-1]) or (df['previous_close'].iloc[i] < df['final_lower'].iloc[i-1]):
            df.at[i, 'final_lower'] = df['basic_lower'].iloc[i]
        else:
            df.at[i, 'final_lower'] = df['final_lower'].iloc[i-1]

    st_col = f"supertrend_{length}_{multiplier}"
    trend_col = f"in_uptrend_{length}_{multiplier}"
    df[st_col] = np.nan
    df[trend_col] = True

    df.at[0, st_col] = df['final_upper'].iloc[0] if df['close'].iloc[0] <= df['final_upper'].iloc[0] else df['final_lower'].iloc[0]
    df.at[0, trend_col] = df['close'].iloc[0] > df['final_upper'].iloc[0]

    for i in range(1, len(df)):
        if df['close'].iloc[i] <= df['final_upper'].iloc[i]:
            df.at[i, trend_col] = False
            df.at[i, st_col] = df['final_upper'].iloc[i]
        else:
            df.at[i, trend_col] = True
            df.at[i, st_col] = df['final_lower'].iloc[i]

    drop_cols = ['previous_close','tr1','tr2','tr3','TR','ATR','basic_upper','basic_lower','final_upper','final_lower']
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)
    return df

# === Generate signal ===
def generate_signal(df):
    col1 = f"in_uptrend_{ST_LENGTH}_{ST_MULT1}"
    col2 = f"in_uptrend_{ST_LENGTH}_{ST_MULT2}"

    last = df.iloc[-1]
    vol = last['volume']
    avg_vol = df['volume'].rolling(window=VOL_ROLL, min_periods=1).mean().iloc[-1]

    if vol < VOL_THRESHOLD_RATIO * avg_vol:
        return "No Signal (Low Volume)"

    up1 = bool(last[col1])
    up2 = bool(last[col2])

    if up1 and up2:
        return "BUY Signal 🚀"
    elif not up1 and not up2:
        return "SELL Signal 🔻"
    else:
        return "No Clear Signal"

# === Master function ===
def run_signal_check():
    try:
        df = get_btcusd_data()
        df = add_supertrend_manual(df, length=ST_LENGTH, multiplier=ST_MULT1)
        df = add_supertrend_manual(df, length=ST_LENGTH, multiplier=ST_MULT2)
        signal = generate_signal(df)

        last_time = df['time'].iloc[-1].strftime("%Y-%m-%d %H:%M:%S")
        last_close = df['close'].iloc[-1]
        last_vol = df['volume'].iloc[-1]

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{now}] Candle: {last_time} | Close: {last_close:.2f} | Vol: {last_vol:.6f} -> {signal}")

    except Exception as e:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{now}] Error: {e}")

# === Schedule every 1 minute ===
if __name__ == "__main__":
    print("BTCUSD Supertrend Signal Bot (5m chart) — running...\n")
    run_signal_check()
    schedule.every(1).minutes.do(run_signal_check)

    while True:
        schedule.run_pending()
        time.sleep(1)
