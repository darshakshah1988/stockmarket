from datetime import datetime, timezone, timedelta
import requests
import pandas as pd

def get_btcusd_data(limit=200, resolution="5m"):
    """Fetch candle data from Delta Exchange (now requires start & end timestamps)."""
    url = "https://api.delta.exchange/v2/history/candles"

    # Each candle is 5 minutes = 300 seconds
    candle_sec = 300
    total_seconds = candle_sec * limit

    # End time = now (UTC)
    end_time = int(datetime.now(timezone.utc).timestamp())
    start_time = end_time - total_seconds

    params = {
        "symbol": "BTCUSD",      # ✅ Works for BTCUSD perpetual pair
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

df = get_btcusd_data(limit=5)
print(df.head())
