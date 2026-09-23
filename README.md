# Telegram Bot

A Telegram bot built with Python, supporting buyer/seller role separation, multi-language support, and payment handling.

## Features

- Buyer and owner/seller role separation with dedicated permissions
- Multi-language support
- Payment integration
- Secure webhook-based updates
- Database integration for persistent storage

## Tech Stack

- **Language:** Python 3.11
- **Database:** Supabase
- **Bot Framework:** python-telegram-bot (or relevant library)
- **Deployment:** Webhook-based

## Project Structure
├── bot.py # Main bot logic and command handlers
├── config.py # Configuration and environment variables
├── db.py # Database connection and queries
├── languages.py # Multi-language text/translations
├── payments.py # Payment processing logic
├── webhook.py # Webhook setup for receiving Telegram updates
├── requirements.txt # Python dependencies
└── .python-version # Python version pin


## Setup

1. Clone the repository
```bash
   git clone https://github.com/siddarthasunkara/telegrambot.git
   cd telegrambot
```

2. Install dependencies
```bash
   pip install -r requirements.txt
```

3. Set up environment variables (bot token, database credentials, payment API keys) in a `.env` file or your environment.

4. Run the bot
```bash
   python bot.py
```

## Security

- Buyer and owner roles are separated to prevent unauthorized access to admin/seller functions.
- Sensitive credentials are handled via environment variables, not hardcoded.

## License

This project is open for personal/educational use.
