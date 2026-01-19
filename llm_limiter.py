import time
import threading
from datetime import datetime, timedelta

# Limits
GROQ_RPM_LIMIT = 28
GROQ_TPM_LIMIT = 5500  # approx tokens per minute free tier
GROQ_TDM_LIMIT = 450000

REQUEST_INTERVAL = 60 / GROQ_RPM_LIMIT

# Global state
last_request_time = 0
tokens_used_this_minute = 0
tokens_used_this_day = 0
minute_window_start = time.time()
day_window_start = datetime.now()
rate_lock = threading.Lock()

def groq_reset_daily_rate():
    global tokens_used_this_day, day_window_start
    tokens_used_this_day = 0
    day_window_start = datetime.now()

def groq_rate_limiter(estimated_tokens=300):
    """
    Thread-safe limiter for both RPM and TPM.
    """
    global last_request_time, tokens_used_this_minute, minute_window_start, tokens_used_this_day

    with rate_lock:
        now = time.time()
        now_dt = datetime.now()

        if now_dt - day_window_start >= timedelta(days = 1):
            groq_reset_daily_rate()
        
        if tokens_used_this_day + estimated_tokens > GROQ_TDM_LIMIT:
            seconds_until_reset = (
                (day_window_start + timedelta(days = 1)) - now_dt
            ).total_seconds()

            time.sleep(seconds_until_reset)

            groq_reset_daily_rate()

        # --- Reset token window every 60 seconds ---
        if now - minute_window_start >= 60:
            tokens_used_this_minute = 0
            minute_window_start = now

        # --- TPM throttle ---
        if tokens_used_this_minute + estimated_tokens > GROQ_TPM_LIMIT:
            sleep_time = 60 - (now - minute_window_start)
            if sleep_time > 0:
                time.sleep(sleep_time)
            tokens_used_this_minute = 0
            minute_window_start = time.time()

        # --- RPM throttle ---
        elapsed = now - last_request_time
        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)

        # --- Update globals AFTER sleeping ---
        last_request_time = time.time()
        tokens_used_this_minute += estimated_tokens