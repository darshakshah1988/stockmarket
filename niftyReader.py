import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import schedule
import gspread
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
DHAN_ACCESS_TOKEN = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJwYXJ0bmVySWQiOiIiLCJkaGFuQ2xpZW50SWQiOiIyNTExMTA1NzcwIiwid2ViaG9va1VybCI6IiIsImlzcyI6ImRoYW4iLCJleHAiOjE3NjUzNDM1NTR9.1WoGSj_OcCMojbSNUfsjw_F0QSG-i2US8ldg257fvDjKB9Fkr8sjxBXJNKz2k0Dkp3lCLAfrOFAyvTfNK-7OUQ"   # Replace with your Dhan API token
NIFTY_SYMBOL = "NSE_INDEX|Nifty 50"            # Dhan instrument identifier
RESOLUTION_MIN = 15
FETCH_LIMIT = 200
ST_LENGTH = 10
ST_MULT1 = 1
ST_MULT2 = 3
VOL_ROLL = 20
GOOGLE_SHEET_NAME = "Nifty50Signals"
SERVICE_ACCOUNT_FILE = "service_account.json"
TELEGRAM_TOKEN = "7982399371:AAF50tDrYpUMiYZg2KWGpO4_waVXfhW2QN0"    # Replace
TELEGRAM_CHAT_ID = "-1003460917857"
SEND_TELEGRAM = True
# ----------------------------------------

# === Google Sheets ===
def init_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open(GOOGLE_SHEET_NAME).sheet1
    if not sheet.row_values(1):
        sheet.append_row([
            "timestamp_utc", "candle_time", "close", "volume",
            "signal", "entry", "stop_loss", "target1", "target2"
        ])
    return sheet


# === Telegram ===
def send_telegram_message(message: str):
    if not SEND_TELEGRAM:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")


# === Fetch NIFTY Data from Dhan API ===
def get_nifty_data(limit=FETCH_LIMIT, resolution=RESOLUTION_MIN):
    try:
        url = "https://api.dhan.co/v2/charts/instrument"
        headers = {
            "accept": "application/json",
            "access-token": DHAN_ACCESS_TOKEN,
        }
        params = {
            "symbol": NIFTY_SYMBOL,
            "resolution": resolution,
            "limit": limit
        }
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}: {r.text}")

        data = r.json().get("data", [])
        if not data:
            raise ValueError("No data returned from Dhan API")

        df = pd.DataFrame(data)
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.rename(columns={
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume"
        })
        df = df[["time", "open", "high", "low", "close", "volume"]]
        df = df.sort_values("time").reset_index(drop=True)
        return df
    except Exception as e:
        raise ValueError(f"⚠️ Failed to fetch NIFTY 50 data: {e}")


# === Supertrend Calculation ===
def add_supertrend_manual(df, length=ST_LENGTH, multiplier=ST_MULT2):
    df = df.copy().reset_index(drop=True)
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.rolling(window=length, min_periods=1).mean()
    hl2 = (high + low) / 2.0
    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr
    final_upper, final_lower = basic_upper.copy(), basic_lower.copy()

    for i in range(1, len(df)):
        if basic_upper.iloc[i] < final_upper.iloc[i-1] or close.iloc[i-1] > final_upper.iloc[i-1]:
            final_upper.iloc[i] = basic_upper.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i-1]
        if basic_lower.iloc[i] > final_lower.iloc[i-1] or close.iloc[i-1] < final_lower.iloc[i-1]:
            final_lower.iloc[i] = basic_lower.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i-1]

    in_uptrend = pd.Series(True, index=df.index)
    supertrend = pd.Series(np.nan, index=df.index)

    for i in range(1, len(df)):
        if close.iloc[i] > final_upper.iloc[i]:
            in_uptrend.iloc[i] = True
            supertrend.iloc[i] = final_lower.iloc[i]
        elif close.iloc[i] < final_lower.iloc[i]:
            in_uptrend.iloc[i] = False
            supertrend.iloc[i] = final_upper.iloc[i]
        else:
            in_uptrend.iloc[i] = in_uptrend.iloc[i-1]
            supertrend.iloc[i] = final_lower.iloc[i] if in_uptrend.iloc[i] else final_upper.iloc[i]

    df[f"supertrend_{length}_{multiplier}"] = supertrend
    df[f"in_uptrend_{length}_{multiplier}"] = in_uptrend
    return df


# === Signal Generation ===
def generate_signal(df):
    col1 = f"in_uptrend_{ST_LENGTH}_{ST_MULT1}"
    col2 = f"in_uptrend_{ST_LENGTH}_{ST_MULT2}"
    last, prev = df.iloc[-1], df.iloc[-2]
    vol = last["volume"]
    avg_vol = df["volume"].rolling(window=VOL_ROLL, min_periods=1).mean().iloc[-1]

    up1, up2 = bool(last[col1]), bool(last[col2])
    prev_up1, prev_up2 = bool(prev[col1]), bool(prev[col2])
    atr = (df["high"] - df["low"]).rolling(window=ST_LENGTH).mean().iloc[-1]
    entry = last["close"]

    if up1 and up2 and not (prev_up1 and prev_up2):
        sl, t1, t2 = entry - 1.5 * atr, entry + 1.5 * atr, entry + 3 * atr
        return "BUY Signal 🚀", vol, entry, sl, t1, t2
    elif not up1 and not up2 and (prev_up1 and prev_up2):
        sl, t1, t2 = entry + 1.5 * atr, entry - 1.5 * atr, entry - 3 * atr
        return "SELL Signal 🔻", vol, entry, sl, t1, t2
    else:
        return "No Clear Signal", vol, None, None, None, None


# === Main Function ===
def run_signal_check(sheet=None):
    try:
        df = get_nifty_data()
        df = add_supertrend_manual(df, ST_LENGTH, ST_MULT1)
        df = add_supertrend_manual(df, ST_LENGTH, ST_MULT2)
        signal, vol, entry, sl, t1, t2 = generate_signal(df)

        last_close = df["close"].iloc[-1]
        last_time = df["time"].iloc[-1].strftime("%Y-%m-%d %H:%M")
        avg_vol = df["volume"].rolling(VOL_ROLL).mean().iloc[-1]
        vol_spike = vol > 1.5 * avg_vol
        vol_drop = vol < 0.5 * avg_vol

        msg = (
            f"<b>📊 NIFTY 50 Supertrend Alert</b>\n"
            f"🕒 <b>{last_time}</b>\n"
            f"💰 Close: <b>{last_close:.2f}</b>\n"
            f"📈 Signal: <b>{signal}</b>\n"
            f"📊 Volume: <b>{vol:.0f}</b> (avg: {avg_vol:.0f})\n"
        )

        if entry:
            msg += f"🎯 Entry: <b>{entry:.2f}</b>\n🛑 Stop Loss: <b>{sl:.2f}</b>\n🎯 T1: <b>{t1:.2f}</b>\n🎯 T2: <b>{t2:.2f}</b>\n"

        if vol_spike:
            msg += "\n🔥 <b>Major Volume Spike!</b>"
        elif vol_drop:
            msg += "\n⚠️ <b>Major Volume Decline!</b>"

        print(msg)
        sheet.append_row([datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                          last_time, last_close, vol, signal, entry, sl, t1, t2])
        send_telegram_message(msg)
        print("✅ Logged and alert sent.")
    except Exception as e:
        print(f"⚠️ Error: {e}")


# === Scheduler ===
if __name__ == "__main__":
    print("📈 NIFTY 50 Supertrend Bot (Dhan API) — running every 15 minutes...\n")
    sheet = init_google_sheet()
    run_signal_check(sheet)
    schedule.every(15).minutes.do(run_signal_check, sheet=sheet)
    while True:
        schedule.run_pending()
        time.sleep(10)
