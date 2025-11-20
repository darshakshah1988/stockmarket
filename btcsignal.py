import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import schedule
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import platform

# === SOUND ALERT ===
def play_sound():
    try:
        if platform.system() == "Windows":
            import winsound
            winsound.Beep(1000, 800)
        elif platform.system() == "Darwin":  # macOS
            os.system('afplay /System/Library/Sounds/Glass.aiff')
        else:  # Linux
            os.system('paplay /usr/share/sounds/freedesktop/stereo/complete.oga 2>/dev/null || echo -e "\a"')
    except Exception as e:
        print(f"⚠️ Sound error: {e}")


# ---------------- CONFIG ----------------
SYMBOL = "BTCUSD"
RESOLUTION = "5m"
FETCH_LIMIT = 200
ST_LENGTH = 10
ST_MULT1 = 1
ST_MULT2 = 3
VOL_ROLL = 20
VOL_THRESHOLD_RATIO = 0.8
VOLUME_SPIKE_RATIO = 1.5      # Major spike threshold (150% of avg)
VOLUME_DROP_RATIO = 0.5       # Major decline threshold (50% of avg)
GOOGLE_SHEET_NAME = "DeltaSignals"
SERVICE_ACCOUNT_FILE = "service_account.json"
TELEGRAM_TOKEN = "7982399371:AAF50tDrYpUMiYZg2KWGpO4_waVXfhW2QN0"    # Replace
TELEGRAM_CHAT_ID = "-1003237685950"             # Replace
SEND_TELEGRAM = True
STATE_FILE = "last_signal_state.json"
# ----------------------------------------


def init_google_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1

    if not sheet.row_values(1):
        sheet.append_row([
            "timestamp_utc", "candle_time", "close", "volume",
            "signal", "entry", "stop_loss", "target1", "target2"
        ])
    return sheet


def send_telegram_message(message: str):
    if not SEND_TELEGRAM:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")


def load_last_signal():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {"last_signal": None}
    return {"last_signal": None}


def save_last_signal(signal):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_signal": signal}, f)


def get_btcusd_data(limit=FETCH_LIMIT, resolution=RESOLUTION):
    url = "https://api.delta.exchange/v2/history/candles"
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
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    df = df.sort_values('time').reset_index(drop=True)
    return df


def add_supertrend_manual(df, length=ST_LENGTH, multiplier=ST_MULT2):
    df = df.copy()
    hl2 = (df['high'] + df['low']) / 2.0
    df['previous_close'] = df['close'].shift(1)
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['previous_close']).abs()
    df['tr3'] = (df['low'] - df['previous_close']).abs()
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=length, min_periods=1).mean()
    df['basic_upper'] = hl2 + multiplier * df['ATR']
    df['basic_lower'] = hl2 - multiplier * df['ATR']
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

    return df


def generate_signal(df):
    col1 = f"in_uptrend_{ST_LENGTH}_{ST_MULT1}"
    col2 = f"in_uptrend_{ST_LENGTH}_{ST_MULT2}"
    st1 = f"supertrend_{ST_LENGTH}_{ST_MULT1}"
    st2 = f"supertrend_{ST_LENGTH}_{ST_MULT2}"

    last = df.iloc[-1]
    vol = last['volume']
    avg_vol = df['volume'].rolling(window=VOL_ROLL, min_periods=1).mean().iloc[-1]
    atr = df['TR'].rolling(window=ST_LENGTH).mean().iloc[-1]

    up1, up2 = bool(last[col1]), bool(last[col2])
    close = last['close']

    if up1 and up2:
        stop_loss = min(df[st1].iloc[-1], df[st2].iloc[-1])
        target1 = close + 1.5 * atr
        target2 = close + 3 * atr
        return "BUY Signal 🚀", vol, close, stop_loss, target1, target2, avg_vol

    elif not up1 and not up2:
        stop_loss = max(df[st1].iloc[-1], df[st2].iloc[-1])
        target1 = close - 1.5 * atr
        target2 = close - 3 * atr
        return "SELL Signal 🔻", vol, close, stop_loss, target1, target2, avg_vol

    else:
        return "No Clear Signal", vol, None, None, None, None, avg_vol


def run_signal_check(sheet=None):
    try:
        df = get_btcusd_data()
        df = add_supertrend_manual(df, length=ST_LENGTH, multiplier=ST_MULT1)
        df = add_supertrend_manual(df, length=ST_LENGTH, multiplier=ST_MULT2)
        signal, vol, entry, sl, t1, t2, avg_vol = generate_signal(df)

        last_state = load_last_signal()
        last_signal = last_state.get("last_signal")

        last_time = df['time'].iloc[-1].strftime("%Y-%m-%d %H:%M:%S")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        volume_spike = vol > avg_vol * VOLUME_SPIKE_RATIO
        volume_drop = vol < avg_vol * VOLUME_DROP_RATIO

        if signal in ["BUY Signal 🚀", "SELL Signal 🔻"] and signal == last_signal:
            print(f"⏸️ No new signal — same as last ({signal})")
            return

        if "BUY" in signal:
            msg = (
                f"🚀 <b>BTC/USDT BUY SIGNAL</b>\n\n"
                f"🕒 Candle: {last_time}\n💰 Entry: ${entry:.2f}\n"
                f"🛑 Stop-Loss: ${sl:.2f}\n🎯 Target 1: ${t1:.2f}\n🎯 Target 2: ${t2:.2f}\n"
                f"📊 Volume: {vol:.2f} (avg: {avg_vol:.2f})"
            )
        elif "SELL" in signal:
            msg = (
                f"🔻 <b>BTC/USDT SELL SIGNAL</b>\n\n"
                f"🕒 Candle: {last_time}\n💰 Entry: ${entry:.2f}\n"
                f"🛑 Stop-Loss: ${sl:.2f}\n🎯 Target 1: ${t1:.2f}\n🎯 Target 2: ${t2:.2f}\n"
                f"📊 Volume: {vol:.2f} (avg: {avg_vol:.2f})"
            )
        else:
            msg = f"⚪ <b>No Clear Signal for BTC/USDT</b>\n\n🕒 {last_time}\n📊 Volume: {vol:.2f}"

        # Log every signal regardless of volume
        sheet.append_row([now, last_time, entry or "-", vol, signal, entry, sl, t1, t2])

        if volume_spike:
            send_telegram_message(msg + "\n🔥 <b>Major Volume Spike Detected!</b>")
            play_sound()
            save_last_signal(signal)
            print("📈 Major Volume Spike Alert Sent!")
        elif volume_drop:
            send_telegram_message(msg + "\n⚠️ <b>Major Volume Decline Detected!</b>")
            play_sound()
            save_last_signal(signal)
            print("📉 Major Volume Drop Alert Sent!")
        elif signal in ["BUY Signal 🚀", "SELL Signal 🔻"]:
            send_telegram_message(msg)
            play_sound()
            save_last_signal(signal)
            print("✅ Trade Signal Sent!")
        else:
            print("ℹ️ No new trading signal.")

    except Exception as e:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{now}] Error: {e}")


if __name__ == "__main__":
    print("🚀 BTCUSD Supertrend Bot (15-Min Schedule + Volume Alerts) running...\n")
    sheet = init_google_sheet()
    run_signal_check(sheet)
    schedule.every(15).minutes.do(run_signal_check, sheet=sheet)

    while True:
        schedule.run_pending()
        time.sleep(5)
