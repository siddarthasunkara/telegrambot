"""
webhook.py — Auto-onboarding via Tally.so form submission.

How it works:
1. Client pays you and fills your Tally form.
2. Tally sends a POST request to this webhook URL.
3. This script reads the form data, creates a client in Supabase,
   and sends the client their live shop link on Telegram.

Run this separately: python webhook.py
Deploy on Render as a second service, or run alongside bot.py using threads.
"""

from flask import Flask, request, jsonify
import telebot
from config import TELEGRAM_TOKEN, WEBHOOK_SECRET, BOT_USERNAME
from db import create_client
import re
import json

app = Flask(__name__)
bot = telebot.TeleBot(TELEGRAM_TOKEN)


def slugify(name):
    """Convert shop name to URL-safe slug. e.g. 'Priya Cakes' → 'priya-cakes'"""
    name = name.lower().strip()
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'[\s_]+', '-', name)
    return name


def parse_tally_data(payload):
    """
    Parse Tally.so webhook payload.
    Tally sends fields as a list under payload['data']['fields'].
    Map field labels to values.
    """
    fields = {}
    try:
        for field in payload["data"]["fields"]:
            label = field.get("label", "").strip().lower()
            value = field.get("value", "")
            # For file uploads, Tally gives a list of URLs
            if isinstance(value, list) and len(value) > 0:
                value = value[0] if isinstance(value[0], str) else value
            fields[label] = value
    except Exception as e:
        print(f"Error parsing Tally data: {e}")
    return fields


@app.route("/webhook/tally", methods=["POST"])
def tally_webhook():
    # Optional: verify secret token
    secret = request.args.get("secret")
    if secret != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.json
    if not payload:
        return jsonify({"error": "No data"}), 400

    fields = parse_tally_data(payload)

    # Expected Tally field labels (must match exactly what you put in Tally form):
    # "business name", "telegram username", "upi id", "language", "products"
    # Products should be a JSON string like:
    # [{"id":1,"name":"Choco Cake","price":299,"photo":"url"}]

    business_name    = fields.get("business name", "").strip()
    owner_telegram   = fields.get("telegram id", "").strip()   # numeric Telegram user ID
    upi_id           = fields.get("upi id", "").strip()
    language         = fields.get("language", "english").strip().lower()
    contact          = fields.get("contact", "").strip()
    products_raw     = fields.get("products", "[]")

    if not business_name or not owner_telegram:
        return jsonify({"error": "Missing required fields: business name + telegram id"}), 400

    try:
        owner_telegram_id = int(owner_telegram)
    except ValueError:
        return jsonify({"error": "telegram id must be a numeric Telegram user ID"}), 400

    slug = slugify(business_name)
    # Collision guard: append number if slug taken
    base_slug = slug
    for n in range(2, 100):
        from db import get_client_by_slug
        if not get_client_by_slug(slug):
            break
        slug = f"{base_slug}-{n}"

    # Parse products JSON if it's a string
    if isinstance(products_raw, str):
        try:
            products = json.loads(products_raw)
        except Exception:
            products = []
    else:
        products = products_raw

    # Create client in Supabase
    client = create_client(
        name=business_name,
        owner_telegram=owner_telegram_id,
        slug=slug,
        upi_id=upi_id,
        language=language,
        contact=contact
    )

    if not client:
        return jsonify({"error": "Failed to create client"}), 500

    shop_link = f"https://t.me/{BOT_USERNAME}?start={slug}"

    # Send the client their shop link via Telegram (uses numeric ID — reliable)
    try:
        bot.send_message(
            owner_telegram_id,
            f"🎉 *Your shop is live!*\n\n"
            f"Shop Name: *{business_name}*\n"
            f"Your Shop Link: {shop_link}\n\n"
            f"Share this link with your customers. They tap it, browse, and order — all inside Telegram!\n\n"
            f"*Owner Commands:*\n"
            f"/orders — See pending orders\n"
            f"/stats — See revenue and totals\n"
            f"/broadcast — Message all your customers",
            parse_mode="Markdown"
        )
    except Exception as e:
        print(f"Could not send Telegram message to {owner_telegram_id}: {e}")

    return jsonify({"success": True, "shop_link": shop_link}), 200


@app.route("/", methods=["GET"])
def health():
    return "Apna Shop Webhook is running.", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)