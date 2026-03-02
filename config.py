import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN")
SUPABASE_URL      = os.getenv("SUPABASE_URL")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY")
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", "your_webhook_secret")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "5500688913")
BOT_USERNAME      = os.getenv("BOT_USERNAME", "TeleKartBot")  # your bot username without @
SETU_CLIENT_ID    = os.getenv("SETU_CLIENT_ID", "")           # optional: Setu UPI webhook
SETU_SECRET       = os.getenv("SETU_SECRET", "")