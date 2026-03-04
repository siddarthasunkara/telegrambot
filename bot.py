# ═══════════════════════════════════════════════════════
# TeleKart — Main Bot
# Architecture: Render + Supabase, sessions in DB
# ═══════════════════════════════════════════════════════

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import TELEGRAM_TOKEN, ADMIN_TELEGRAM_ID, BOT_USERNAME
from db import (
    get_client_by_slug, get_client_by_id, get_client_by_owner,
    get_products, get_product_by_id, add_product, edit_product, delete_product,
    get_categories, get_variants, add_variant, delete_variants,
    create_order, update_order_status, update_payment_status,
    get_orders_by_client, get_orders_by_customer, get_order_by_id,
    get_client_stats, create_client, update_client,
    get_session, upsert_session, update_session, delete_session,
    get_customer_profile, save_customer_profile,
    save_review, get_shop_rating, get_product_rating,
    get_all_customers_of_client,
    flag_blocked_customer, is_customer_blocked,
    decrement_stock, restore_stock
)
from payments import build_payment_message, build_qr_bytes, build_upi_string, QR_AVAILABLE
from languages import t, language_picker_keyboard, LANGUAGE_OPTIONS
import time
import re
import urllib.parse
import json
from datetime import datetime, timezone, timedelta

import sys
if not TELEGRAM_TOKEN:
    print("❌ FATAL: TELEGRAM_TOKEN env var missing. Exiting.")
    sys.exit(1)

bot = telebot.TeleBot(TELEGRAM_TOKEN, num_threads=4)

DIV = "━━━━━━━━━━━━━━━"

# ── Checkout states ──
STATE_IDLE     = "idle"
STATE_ADDRESS  = "address"
STATE_PHONE    = "phone"
STATE_DATE     = "date"

# ── Admin step states (in-memory only for owner flows) ──
admin_sessions      = {}


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════

def fmt(price):
    try:
        p = float(price)
        return int(p) if p == int(p) else round(p, 2)
    except Exception:
        return price

def md(text):
    """Escape user-supplied text for Telegram Markdown v1 so special chars don't break parsing."""
    if not text:
        return ""
    # In Markdown v1 these chars open/close formatting: * _ ` [
    for ch in ("*", "_", "`", "["):
        text = str(text).replace(ch, f"\\{ch}")
    return text

def get_ist():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def is_shop_open(shop):
    o, c = shop.get("open_time"), shop.get("close_time")
    if not o or not c:
        return True
    now = get_ist().strftime("%H:%M")
    if o <= c:
        return o <= now <= c        # normal hours e.g. 09:00–21:00
    else:
        return now >= o or now <= c # midnight-spanning e.g. 22:00–02:00

def validate_date(date_str, min_lead_hours=0):
    """Validate date string. Optionally enforce min_lead_hours ahead of now."""
    now_ist = get_ist()
    today   = now_ist.date()
    s       = date_str.strip().lower()
    quick_words = [
        "today", "now", "asap", "urgent", "fast", "quick", "express",
        "jaldi", "abhi", "aaj", "ee roju", "inni",
        "tomorrow", "kal", "repu", "naale", "nale",
        "day after", "parso",
    ]
    if s in quick_words:
        # quick words only valid if no lead-time requirement
        return min_lead_hours == 0
    for fmt_str in ["%d %b", "%d/%m", "%d-%m-%Y", "%d %B", "%d-%m-%y",
                    "%d-%m", "%d.%m", "%d %b %Y", "%d %B %Y"]:
        try:
            parsed = datetime.strptime(s, fmt_str)
            if parsed.year == 1900:
                parsed = parsed.replace(year=today.year)
            if parsed.date() < today:
                parsed = parsed.replace(year=today.year + 1)
            if parsed.date() < today:
                return False
            if min_lead_hours:
                # treat parsed date as start-of-day; check hours from now
                parsed_dt = datetime(parsed.year, parsed.month, parsed.day,
                                     tzinfo=now_ist.tzinfo)
                hours_ahead = (parsed_dt - now_ist).total_seconds() / 3600
                if hours_ahead < min_lead_hours:
                    return False
            return True
        except ValueError:
            continue
    return False

def safe_send(chat_id, text, **kwargs):
    for attempt in [0, 1, 2, 4]:
        try:
            time.sleep(attempt)
            return bot.send_message(chat_id, text, **kwargs)
        except telebot.apihelper.ApiTelegramException as e:
            err = str(e).lower()
            if "blocked" in err or "deactivated" in err or "chat not found" in err:
                flag_blocked_customer(chat_id)
                return None
            if "429" in str(e):
                wait = 5
                try:
                    wait = int(str(e).split("retry after ")[-1].strip())
                except Exception:
                    pass
                time.sleep(wait + attempt)
            else:
                print(f"Telegram error {chat_id}: {e}")
                return None
    return None

def try_edit(chat_id, message_id, text, **kwargs):
    try:
        bot.edit_message_text(text, chat_id, message_id, **kwargs)
    except Exception:
        bot.send_message(chat_id, text, **kwargs)

def home_kb(chat_id=None):
    cart  = get_cart(chat_id) if chat_id else []
    n     = sum(i.get("quantity", 1) for i in cart)
    lang  = get_lang(chat_id) if chat_id else "english"
    cart_label = t(lang, "cart")
    clabel = f"{cart_label} ({n})" if n else cart_label
    m = InlineKeyboardMarkup(row_width=2)
    m.add(
        InlineKeyboardButton(t(lang, "browse"),    callback_data="browse"),
        InlineKeyboardButton(clabel,               callback_data="cart")
    )
    m.add(
        InlineKeyboardButton(t(lang, "my_orders"), callback_data="my_orders"),
        InlineKeyboardButton(t(lang, "about"),     callback_data="about")
    )
    m.add(InlineKeyboardButton(t(lang, "change_language"), callback_data="change_language"))
    return m

def cancel_kb(chat_id=None):
    lang = get_lang(chat_id) if chat_id else "english"
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton(t(lang, "cancel_checkout"), callback_data="cancel_checkout"))
    return m

def build_welcome_text(shop):
    rating, rcount = get_shop_rating(shop["id"])
    parts = []
    if shop.get("tagline"):
        parts.append(shop["tagline"])
    if rating:
        parts.append(f"⭐ {rating}/5  ({rcount} reviews)")
    if shop.get("shop_hours"):
        parts.append(f"🕐 {shop['shop_hours']}")
    dc = shop.get("delivery_charge", 0)
    parts.append(f"🚚 {'Free delivery' if not dc else f'₹{int(dc)} delivery charge'}")
    return (
        f"🙏 *Welcome to {shop['name']}!*\n"
        f"{DIV}\n"
        f"{chr(10).join(parts)}\n"
        f"{DIV}\n"
        f"What would you like today?"
    )

def send_welcome(chat_id, shop, reply_markup=None):
    """Send welcome — with shop logo/banner if available, else plain text."""
    text = build_welcome_text(shop)
    photo = shop.get("shop_photo", "")
    if photo:
        try:
            bot.send_photo(
                chat_id, photo,
                caption=text,
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
            return
        except Exception as e:
            print(f"Failed to send shop photo: {e}")
    # Fallback — plain text welcome
    bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=reply_markup)

def parse_slug_from_input(text):
    """
    Extract shop slug from various input formats:
    - https://t.me/TeleKartBot?start=my-shop  → my-shop
    - t.me/TeleKartBot?start=my-shop          → my-shop
    - my-shop                                  → my-shop
    """
    text = text.strip()
    # Full URL or t.me link
    match = re.search(r'[?&]start=([a-zA-Z0-9\-]+)', text)
    if match:
        return match.group(1)
    # Just the slug
    if re.match(r'^[a-zA-Z0-9\-]+$', text):
        return text
    return None

# ── Session helpers ──────────────────────────────────

def load_session(chat_id):
    """Load session from DB. Returns dict or None."""
    return get_session(chat_id)

def session_shop_id(chat_id):
    s = load_session(chat_id)
    return s["shop_id"] if s else None

def get_state(chat_id):
    s = load_session(chat_id)
    return s["state"] if s else STATE_IDLE

def set_state(chat_id, state):
    update_session(chat_id, state=state)

def get_cart(chat_id):
    s = load_session(chat_id)
    if not s:
        return []
    cart = s.get("cart") or []
    if isinstance(cart, str):
        try:
            cart = json.loads(cart)
        except Exception:
            cart = []
    return cart

def set_cart(chat_id, cart):
    update_session(chat_id, cart=cart)

def add_to_cart_db(chat_id, item):
    cart = get_cart(chat_id)
    # Check if same product+variant already in cart
    for c in cart:
        if c["product_id"] == item["product_id"] and c.get("variant_id") == item.get("variant_id"):
            c["quantity"] = c.get("quantity", 1) + 1
            set_cart(chat_id, cart)
            return
    item["quantity"] = 1
    cart.append(item)
    set_cart(chat_id, cart)

def get_shop_for_session(chat_id):
    """Load the shop linked to this session — uses db.py singleton client."""
    s = load_session(chat_id)
    if not s or not s.get("shop_id"):
        return None
    return get_client_by_id(s["shop_id"])

def get_lang(chat_id):
    """Get the customer's chosen language from session. Defaults to 'english'."""
    s = load_session(chat_id)
    return (s or {}).get("language") or "english"

# ── Cart display helpers ─────────────────────────────

def build_cart_display(cart, delivery_charge=0):
    if not cart:
        return "", 0, 0
    subtotal = sum(
        float(item["price"]) * int(item.get("quantity", 1))
        for item in cart
    )
    total = subtotal + float(delivery_charge or 0)
    lines = ""
    for item in cart:
        qty  = item.get("quantity", 1)
        name = item["name"]
        if item.get("variant_label"):
            name += f" ({item['variant_label']})"
        item_total = float(item["price"]) * qty
        if qty > 1:
            lines += f"• {name} ×{qty} — ₹{fmt(item_total)}\n"
        else:
            lines += f"• {name} — ₹{fmt(item_total)}\n"
    return lines, subtotal, total

def cart_keyboard(cart):
    m = InlineKeyboardMarkup(row_width=2)
    m.add(
        InlineKeyboardButton("✅ Checkout",   callback_data="checkout"),
        InlineKeyboardButton("🗑 Clear Cart", callback_data="clear_cart")
    )
    seen = set()
    for item in cart:
        key = f"{item['product_id']}_{item.get('variant_id','')}"
        if key not in seen:
            seen.add(key)
            label = item["name"]
            if item.get("variant_label"):
                label += f" ({item['variant_label']})"
            m.add(InlineKeyboardButton(
                f"➖ {label} ×{item.get('quantity',1)}",
                callback_data=f"remove_{item['product_id']}_{item.get('variant_id','')}"
            ))
    m.add(InlineKeyboardButton("🛍 Add More", callback_data="browse"))
    return m


# ═══════════════════════════════════════════════════════
# /start — ENTRY POINT
# ═══════════════════════════════════════════════════════

@bot.message_handler(commands=['start'])
def start(message):
    args  = message.text.split(maxsplit=1)
    slug  = args[1].strip() if len(args) > 1 else None

    # Cancel any in-progress checkout
    existing = load_session(message.chat.id)
    if existing:
        update_session(message.chat.id, state=STATE_IDLE)

    if not slug:
        # Check if this is a registered owner
        owner_shop = get_client_by_owner(message.chat.id)
        if owner_shop:
            shop_link = f"https://t.me/{BOT_USERNAME}?start={owner_shop['slug']}"
            bot.send_message(
                message.chat.id,
                f"👋 *Welcome back, {owner_shop['name']} owner!*\n\n"
                f"🔗 Your shop link:\n`{shop_link}`\n\nUse /help for all commands.",
                parse_mode="Markdown"
            )
        else:
            bot.send_message(
                message.chat.id,
                "👋 *Welcome to TeleKart!*\n\n"
                "If you're a *customer* — use your shop's link to get started.\n"
                "If you're a *shop owner* — type /help",
                parse_mode="Markdown"
            )
        return

    shop = get_client_by_slug(slug)
    if not shop:
        bot.send_message(
            message.chat.id,
            "❌ Shop not found.\n\nPlease use a valid shop link.\n\n"
            "_Tip: Ask the shop owner to send you the correct link._",
            parse_mode="Markdown"
        )
        return

    if not is_shop_open(shop):
        # Use language from existing session (if any); default english for brand-new visitors
        lang_for_closed = (existing or {}).get("language") or "english"
        closed_m = InlineKeyboardMarkup()
        closed_m.add(InlineKeyboardButton("ℹ️ About Shop", callback_data="about"))
        bot.send_message(
            message.chat.id,
            t(lang_for_closed, "shop_closed",
              shop_name=shop["name"],
              open_time=shop.get("open_time",""),
              close_time=shop.get("close_time",""))
            + f"\n\nSee you during business hours! 🙏",
            parse_mode="Markdown", reply_markup=closed_m
        )
        return

    # Preserve cart if returning to same shop
    saved_cart = []
    if existing and existing.get("shop_id") == shop["id"]:
        saved_cart = get_cart(message.chat.id)

    upsert_session(
        chat_id=message.chat.id,
        shop_id=shop["id"],
        cart=saved_cart,
        state=STATE_IDLE,
        address=existing.get("address","") if existing else "",
        phone=existing.get("phone","") if existing else "",
        delivery_date=""
    )

    # First-time visitor — show language picker before welcome
    # Session row is guaranteed to exist now (upsert above), so set_language can safely update it
    is_first_visit = not existing or not existing.get("language")
    if is_first_visit:
        bot.send_message(
            message.chat.id,
            "🌐 *Choose your language / भाषा चुनें / భాష ఎంచుకోండి:*",
            parse_mode="Markdown",
            reply_markup=language_picker_keyboard()
        )
        return  # wait for lang_ callback, then show welcome

    # Welcome back hint if cart has items
    lang = get_lang(message.chat.id)
    if saved_cart:
        n = sum(i.get("quantity", 1) for i in saved_cart)
        items_word = "items" if n > 1 else "item"
        bot.send_message(
            message.chat.id,
            t(lang, "welcome_back_cart", n=n, word=items_word),
            parse_mode="Markdown"
        )
    send_welcome(message.chat.id, shop, reply_markup=home_kb(message.chat.id))


# ── Handle pasted shop links in chat ────────────────

@bot.message_handler(func=lambda msg:
    msg.text and (
        f"t.me/{BOT_USERNAME}" in msg.text or
        f"t.me/{BOT_USERNAME.lower()}" in msg.text.lower()
    ) and get_state(msg.chat.id) == STATE_IDLE
)
def handle_pasted_link(message):
    slug = parse_slug_from_input(message.text)
    if not slug:
        return
    # Simulate /start with slug
    message.text = f"/start {slug}"
    start(message)


# ═══════════════════════════════════════════════════════
# HOME
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "home")
def go_home(call):
    bot.answer_callback_query(call.id)
    set_state(call.message.chat.id, STATE_IDLE)
    shop = get_shop_for_session(call.message.chat.id)
    if not shop:
        bot.send_message(call.message.chat.id,
                         "⚠️ Session expired. Please open your shop link again.")
        return
    # If shop has a logo, can't edit (photo vs text mismatch), send fresh
    if shop.get("shop_photo"):
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        send_welcome(call.message.chat.id, shop, reply_markup=home_kb(call.message.chat.id))
    else:
        try_edit(call.message.chat.id, call.message.message_id,
                 build_welcome_text(shop), parse_mode="Markdown", reply_markup=home_kb(call.message.chat.id))


# ═══════════════════════════════════════════════════════
# LANGUAGE SELECTION
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data.startswith("lang_"))
def set_language(call):
    lang = call.data.split("_", 1)[1]   # e.g. "telugu"
    if lang not in ("english","hindi","telugu","kannada","tamil","malayalam"):
        bot.answer_callback_query(call.id, "Unknown language.")
        return

    # Always upsert — guarantees language is saved regardless of session state
    upsert_session(call.message.chat.id, language=lang)
    bot.answer_callback_query(call.id, t(lang, "language_set"))

    # Now show welcome in chosen language
    shop = get_shop_for_session(call.message.chat.id)
    if not shop:
        bot.send_message(call.message.chat.id,
                         "⚠️ Please open your shop link again.",
                         parse_mode="Markdown")
        return

    # Delete the language picker message
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass

    send_welcome(call.message.chat.id, shop, reply_markup=home_kb(call.message.chat.id))


@bot.callback_query_handler(func=lambda c: c.data == "change_language")
def change_language(call):
    bot.answer_callback_query(call.id)
    lang = get_lang(call.message.chat.id)
    bot.send_message(
        call.message.chat.id,
        t(lang, "choose_language"),
        parse_mode="Markdown",
        reply_markup=language_picker_keyboard()
    )


# ═══════════════════════════════════════════════════════
# ABOUT
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "about")
def about_shop(call):
    bot.answer_callback_query(call.id)
    shop = get_shop_for_session(call.message.chat.id)
    if not shop:
        bot.send_message(call.message.chat.id, "Please open the shop link first.")
        return

    rating, rcount = get_shop_rating(shop["id"])
    dc = shop.get("delivery_charge", 0)
    text = f"🏪 *{shop['name']}*\n{DIV}\n"
    if rating:
        text += f"⭐ {rating}/5  ({rcount} reviews)\n"
    else:
        text += "⭐ No reviews yet\n"
    if shop.get("tagline"):
        text += f"💬 {shop['tagline']}\n"
    if shop.get("shop_hours"):
        text += f"🕐 {shop['shop_hours']}\n"
    if shop.get("delivery_areas"):
        text += f"📍 Delivers to: {shop['delivery_areas']}\n"
    text += f"🚚 Delivery: {'Free' if not dc else f'₹{int(dc)}'}\n"
    if shop.get("contact"):
        text += f"📞 {shop['contact']}\n"
    text += DIV

    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("🛍 Browse Products", callback_data="browse"))
    m.add(InlineKeyboardButton("🏠 Back to Menu",    callback_data="home"))
    try_edit(call.message.chat.id, call.message.message_id,
             text, parse_mode="Markdown", reply_markup=m)


# ═══════════════════════════════════════════════════════
# BROWSE — categories first
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "browse")
def browse(call):
    bot.answer_callback_query(call.id)   # answer immediately — Telegram 10s timeout
    shop = get_shop_for_session(call.message.chat.id)
    if not shop:
        bot.send_message(call.message.chat.id, "Please open the shop link first.")
        return

    cats = get_categories(shop["id"])
    if cats:
        m = InlineKeyboardMarkup(row_width=2)
        for cat in cats:
            prods = get_products(shop["id"], category=cat)
            avail = sum(1 for p in prods if p.get("stock",99) > 0)
            if avail == 0:
                continue
            m.add(InlineKeyboardButton(
                f"{cat}  ({avail} items)",
                callback_data=f"cat_{cat}"
            ))
        m.add(InlineKeyboardButton("🏠 Menu",     callback_data="home"))
        bot.send_message(
            call.message.chat.id,
            f"🛍 *{shop['name']}*\n{DIV}\nChoose a category 👇",
            parse_mode="Markdown", reply_markup=m
        )
    else:
        products = get_products(shop["id"])
        if not products:
            m = InlineKeyboardMarkup()
            m.add(InlineKeyboardButton("🏠 Menu", callback_data="home"))
            bot.send_message(call.message.chat.id,
                             t(get_lang(call.message.chat.id), "no_products"),
                             reply_markup=m)
            return
        _send_product_list(call.message.chat.id, products, shop["name"])


@bot.callback_query_handler(func=lambda c: c.data.startswith("cat_"))
def browse_category(call):
    bot.answer_callback_query(call.id)
    shop = get_shop_for_session(call.message.chat.id)
    if not shop:
        bot.send_message(call.message.chat.id, "Session expired.")
        return

    cat = call.data[4:]
    if cat == "ALL":
        products = get_products(shop["id"])
        title = "📋 *All Products*"
    else:
        products = get_products(shop["id"], category=cat)
        title = f"🛍 *{cat}*"

    if not products:
        bot.send_message(call.message.chat.id,
                         "No products in this category right now.")
        return

    bot.send_message(call.message.chat.id,
                     f"{title}\n{DIV}\nTap *Add to Cart* on any item 👇",
                     parse_mode="Markdown")
    _send_product_list(call.message.chat.id, products, shop["name"])


def _send_product_list(chat_id, products, shop_name):
    available    = [p for p in products if p.get("stock", 99) > 0]
    out_of_stock = [p for p in products if p.get("stock", 99) == 0]

    for p in available:
        _send_product_card(chat_id, p)
    for p in out_of_stock:
        _send_product_card(chat_id, p)

    m = InlineKeyboardMarkup(row_width=2)
    m.add(
        InlineKeyboardButton("🛒 View Cart", callback_data="cart"),
        InlineKeyboardButton("🏠 Menu",      callback_data="home")
    )
    bot.send_message(chat_id,
                     "👆 That's everything!\n\nTap *View Cart* to checkout.",
                     parse_mode="Markdown", reply_markup=m)


def _send_product_card(chat_id, product):
    pid   = product["id"]
    stock = product.get("stock", 99)
    rating, rcount = get_product_rating(pid)

    caption = f"*{product['name']}*"
    if product.get("category"):
        caption += f"  •  _{product['category']}_"

    # Rating line
    if rating:
        caption += f"\n⭐ {rating}/5  ({rcount} sold)"

    # Price
    caption += f"\n💰 ₹{fmt(product['price'])}"
    if product.get("description"):
        caption += f"\n_{product['description']}_"

    if stock == 0:
        caption += "\n\n❌ *Out of Stock*"
    elif 0 < stock <= 3:
        caption += f"\n\n⚠️ *Only {stock} left!*"

    # Check for variants
    variants = get_variants(pid)

    m = InlineKeyboardMarkup()
    if stock == 0:
        m.add(InlineKeyboardButton("🔔 Notify Me", callback_data=f"notify_{pid}"))
    elif variants:
        # Show variant buttons in rows of 3
        row = []
        for v in variants:
            v_stock = v.get("stock", 99)
            if v_stock == 0:
                row.append(InlineKeyboardButton(
                    f"❌ {v['label']}",
                    callback_data=f"vout_{pid}"
                ))
            else:
                row.append(InlineKeyboardButton(
                    f"{v['label']} — ₹{fmt(v['price'])}",
                    callback_data=f"addv_{pid}_{v['id']}"
                ))
            if len(row) == 3:
                m.row(*row)
                row = []
        if row:
            m.row(*row)
    else:
        m.add(InlineKeyboardButton(
            f"🛒 Add to Cart — ₹{fmt(product['price'])}",
            callback_data=f"add_{pid}"
        ))

    if product.get("photo"):
        try:
            bot.send_photo(chat_id, product["photo"], caption=caption,
                           parse_mode="Markdown", reply_markup=m)
            return
        except Exception as e:
            print(f"Photo failed {pid}: {e}")
    bot.send_message(chat_id, caption, parse_mode="Markdown", reply_markup=m)


# ── Notify out of stock ──────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data.startswith("notify_"))
def notify_me(call):
    shop = get_shop_for_session(call.message.chat.id)
    contact = shop.get("contact", "") if shop else ""
    msg = "Contact the shop to be notified 📞"
    if contact:
        msg += f": {contact}"
    bot.answer_callback_query(call.id, msg, show_alert=True)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vout_"))
def variant_out(call):
    shop = get_shop_for_session(call.message.chat.id)
    contact = shop.get("contact", "") if shop else ""
    msg = "❌ This size/weight is out of stock."
    if contact:
        msg += f"\nContact shop: {contact}"
    bot.answer_callback_query(call.id, msg, show_alert=True)


# ═══════════════════════════════════════════════════════
# ADD TO CART — product without variants
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data.startswith("add_"))
def add_to_cart(call):
    s = load_session(call.message.chat.id)
    if not s:
        bot.answer_callback_query(call.id, "Session expired. Open shop link again.", show_alert=True)
        return

    product_id = int(call.data[4:])
    product    = get_product_by_id(product_id)
    if not product:
        bot.answer_callback_query(call.id, "Product not found.")
        return
    if product.get("stock", 99) == 0:
        bot.answer_callback_query(call.id, "❌ Out of stock.", show_alert=True)
        return

    add_to_cart_db(call.message.chat.id, {
        "product_id":    product_id,
        "variant_id":    None,
        "name":          product["name"],
        "variant_label": "",
        "price":         float(product["price"])
    })

    cart  = get_cart(call.message.chat.id)
    total = sum(float(i["price"]) * i.get("quantity",1) for i in cart)
    n     = sum(i.get("quantity",1) for i in cart)
    item_qty = next((i.get("quantity",1) for i in cart if i["product_id"]==product_id and not i.get("variant_id")), 1)
    qty_label = f" ×{item_qty}" if item_qty > 1 else ""
    bot.answer_callback_query(
        call.id,
        f"✅ {product['name']}{qty_label} in cart!\n🛒 {n} item{'s' if n>1 else ''} | ₹{fmt(total)}"
    )


# ═══════════════════════════════════════════════════════
# ADD TO CART — product WITH variant
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data.startswith("addv_"))
def add_variant_to_cart(call):
    s = load_session(call.message.chat.id)
    if not s:
        bot.answer_callback_query(call.id, "Session expired.", show_alert=True)
        return

    parts      = call.data.split("_")
    product_id = int(parts[1])
    variant_id = int(parts[2])

    product  = get_product_by_id(product_id)
    variants = get_variants(product_id)
    variant  = next((v for v in variants if v["id"] == variant_id), None)

    if not product or not variant:
        bot.answer_callback_query(call.id, "Item not found.")
        return
    if variant.get("stock", 99) == 0:
        bot.answer_callback_query(call.id, "❌ This size is out of stock.", show_alert=True)
        return

    add_to_cart_db(call.message.chat.id, {
        "product_id":    product_id,
        "variant_id":    variant_id,
        "name":          product["name"],
        "variant_label": variant["label"],
        "price":         float(variant["price"])
    })

    cart  = get_cart(call.message.chat.id)
    total    = sum(float(i["price"]) * i.get("quantity",1) for i in cart)
    n        = sum(i.get("quantity",1) for i in cart)
    item_qty = next((i.get("quantity",1) for i in cart if i.get("variant_id")==variant["id"]), 1)
    qty_label = f" ×{item_qty}" if item_qty > 1 else ""
    bot.answer_callback_query(
        call.id,
        f"✅ {product['name']} ({variant['label']}){qty_label} in cart!\n🛒 {n} items | ₹{fmt(total)}"
    )


# ═══════════════════════════════════════════════════════
# CART
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "cart")
def view_cart(call):
    bot.answer_callback_query(call.id)
    s = load_session(call.message.chat.id)
    if not s:
        bot.send_message(call.message.chat.id, "Please open the shop link first.")
        return

    cart = get_cart(call.message.chat.id)
    if not cart:
        m = InlineKeyboardMarkup()
        m.add(InlineKeyboardButton("🛍 Browse Products", callback_data="browse"))
        m.add(InlineKeyboardButton("🏠 Menu",            callback_data="home"))
        try_edit(call.message.chat.id, call.message.message_id,
                 t(get_lang(call.message.chat.id), "empty_cart"),
                 parse_mode="Markdown", reply_markup=m)
        return

    shop = get_shop_for_session(call.message.chat.id)
    dc   = shop.get("delivery_charge", 0) if shop else 0
    lines, subtotal, total = build_cart_display(cart, dc)

    text = f"🛒 *Your Cart*\n{DIV}\n{lines}{DIV}\n💰 Subtotal: ₹{fmt(subtotal)}\n"
    if dc:
        text += f"🚚 Delivery: ₹{int(dc)}\n"
    text += f"💳 *Total: ₹{fmt(total)}*"

    try_edit(call.message.chat.id, call.message.message_id,
             text, parse_mode="Markdown", reply_markup=cart_keyboard(cart))


# ── Remove item ──────────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data.startswith("remove_"))
def remove_item(call):
    s = load_session(call.message.chat.id)
    if not s:
        bot.answer_callback_query(call.id)
        return

    parts      = call.data.split("_", 2)
    product_id = int(parts[1])
    variant_id = int(parts[2]) if parts[2].isdigit() else None

    cart = get_cart(call.message.chat.id)
    removed_name = None
    for i, item in enumerate(cart):
        if item["product_id"] == product_id and item.get("variant_id") == variant_id:
            if item.get("quantity", 1) > 1:
                item["quantity"] -= 1
            else:
                cart.pop(i)
            removed_name = item["name"]
            if item.get("variant_label"):
                removed_name += f" ({item['variant_label']})"
            break

    set_cart(call.message.chat.id, cart)

    if removed_name:
        bot.answer_callback_query(call.id, f"➖ {removed_name} removed")
    else:
        bot.answer_callback_query(call.id)

    if not cart:
        m = InlineKeyboardMarkup()
        m.add(InlineKeyboardButton("🛍 Browse", callback_data="browse"))
        m.add(InlineKeyboardButton("🏠 Menu",   callback_data="home"))
        try_edit(call.message.chat.id, call.message.message_id,
                 "🛒 Cart is empty now 😊", reply_markup=m)
        return

    shop = get_shop_for_session(call.message.chat.id)
    dc   = shop.get("delivery_charge", 0) if shop else 0
    lines, subtotal, total = build_cart_display(cart, dc)
    text = f"🛒 *Your Cart*\n{DIV}\n{lines}{DIV}\n💰 Subtotal: ₹{fmt(subtotal)}\n"
    if dc:
        text += f"🚚 Delivery: ₹{int(dc)}\n"
    text += f"💳 *Total: ₹{fmt(total)}*"
    try_edit(call.message.chat.id, call.message.message_id,
             text, parse_mode="Markdown", reply_markup=cart_keyboard(cart))


# ── Clear cart ───────────────────────────────────────

@bot.callback_query_handler(func=lambda c: c.data == "clear_cart")
def clear_cart(call):
    set_cart(call.message.chat.id, [])
    bot.answer_callback_query(call.id, t(get_lang(call.message.chat.id), "cart_cleared"))
    lang = get_lang(call.message.chat.id)
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton(t(lang, "browse"),     callback_data="browse"))
    m.add(InlineKeyboardButton(t(lang, "back_to_menu"), callback_data="home"))
    try_edit(call.message.chat.id, call.message.message_id,
             t(lang, "cart_cleared") + " 😊", reply_markup=m)


# ═══════════════════════════════════════════════════════
# CHECKOUT — state machine
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "checkout")
def checkout(call):
    bot.answer_callback_query(call.id)
    s    = load_session(call.message.chat.id)
    cart = get_cart(call.message.chat.id)
    if not s or not cart:
        bot.send_message(call.message.chat.id, t(get_lang(call.message.chat.id), "empty_cart"))
        return

    shop = get_shop_for_session(call.message.chat.id)
    if shop and not is_shop_open(shop):
        bot.send_message(
            call.message.chat.id,
            f"🔴 {shop['name']} is currently closed. Please try again during business hours."
        )
        return

    # Check saved profile
    profile = get_customer_profile(call.message.chat.id)
    if profile and profile.get("address") and profile.get("phone"):
        m = InlineKeyboardMarkup()
        addr_preview = profile['address'][:35] + ("…" if len(profile['address']) > 35 else "")
        m.add(InlineKeyboardButton(
            f"✅ Use saved: {addr_preview}",
            callback_data="use_saved_address"
        ))
        m.add(InlineKeyboardButton("📝 Enter new address", callback_data="new_address"))
        m.add(InlineKeyboardButton("❌ Cancel", callback_data="cancel_checkout"))
        lang = get_lang(call.message.chat.id)
        bot.send_message(
            call.message.chat.id,
            f"📦 *Checkout*\n\n{DIV}\n"
            f"📍 Saved address found!\n\n"
            f"*{profile['address']}*\n"
            f"📱 {profile['phone']}\n\n"
            f"Use this or enter a new one?",
            parse_mode="Markdown", reply_markup=m
        )
    else:
        set_state(call.message.chat.id, STATE_ADDRESS)
        lang = get_lang(call.message.chat.id)
        bot.send_message(
            call.message.chat.id,
            f"📦 *Checkout*\n\n{DIV}\n" + t(lang, "enter_address"),
            parse_mode="Markdown", reply_markup=cancel_kb(call.message.chat.id)
        )


@bot.callback_query_handler(func=lambda c: c.data == "use_saved_address")
def use_saved_address(call):
    profile = get_customer_profile(call.message.chat.id)
    if not profile:
        bot.answer_callback_query(call.id)
        return
    bot.answer_callback_query(call.id)
    update_session(call.message.chat.id,
                   address=profile["address"],
                   phone=profile["phone"],
                   state=STATE_DATE)
    # Flag in session DB: reused saved address, don't overwrite on order
    update_session(call.message.chat.id, used_saved_address=True)
    lang = get_lang(call.message.chat.id)
    bot.send_message(
        call.message.chat.id,
        f"✅ Using saved address.\n\n" + t(lang, "enter_delivery_date"),
        parse_mode="Markdown", reply_markup=cancel_kb(call.message.chat.id)
    )


@bot.callback_query_handler(func=lambda c: c.data == "new_address")
def new_address(call):
    bot.answer_callback_query(call.id)
    set_state(call.message.chat.id, STATE_ADDRESS)
    lang = get_lang(call.message.chat.id)
    bot.send_message(
        call.message.chat.id,
        t(lang, "enter_address"),
        parse_mode="Markdown", reply_markup=cancel_kb(call.message.chat.id)
    )


@bot.callback_query_handler(func=lambda c: c.data == "cancel_checkout")
def cancel_checkout(call):
    set_state(call.message.chat.id, STATE_IDLE)
    bot.answer_callback_query(call.id, "Checkout cancelled")
    m = InlineKeyboardMarkup(row_width=2)
    lang = get_lang(call.message.chat.id)
    m.add(
        InlineKeyboardButton(t(lang, "back_to_cart"), callback_data="cart"),
        InlineKeyboardButton(t(lang, "back_to_menu"), callback_data="home")
    )
    lang = get_lang(call.message.chat.id)
    try_edit(call.message.chat.id, call.message.message_id,
             t(lang, "checkout_cancelled"), reply_markup=m)


# ── State handlers ───────────────────────────────────

@bot.message_handler(func=lambda msg: get_state(msg.chat.id) == STATE_ADDRESS)
def get_address(message):
    lang = get_lang(message.chat.id)
    if not message.text or len(message.text.strip()) < 15:
        bot.send_message(message.chat.id,
                         t(lang, "invalid_address"),
                         reply_markup=cancel_kb(message.chat.id))
        return
    update_session(message.chat.id,
                   address=message.text.strip(),
                   state=STATE_PHONE)
    bot.send_message(
        message.chat.id,
        t(lang, "enter_phone"),
        parse_mode="Markdown", reply_markup=cancel_kb(message.chat.id)
    )


@bot.message_handler(func=lambda msg: get_state(msg.chat.id) == STATE_PHONE)
def get_phone(message):
    phone = re.sub(r'[^\d]', '', message.text or "")
    lang = get_lang(message.chat.id)
    if len(phone) < 10 or phone[0] not in "6789":
        bot.send_message(message.chat.id,
                         t(lang, "invalid_phone"),
                         reply_markup=cancel_kb(message.chat.id))
        return
    update_session(message.chat.id,
                   phone=phone,
                   state=STATE_DATE)
    bot.send_message(
        message.chat.id,
        t(lang, "enter_delivery_date"),
        parse_mode="Markdown", reply_markup=cancel_kb(message.chat.id)
    )


@bot.message_handler(func=lambda msg: get_state(msg.chat.id) == STATE_DATE)
def get_date(message):
    date_input = (message.text or "").strip()
    shop       = get_shop_for_session(message.chat.id)
    min_hours  = shop.get("min_lead_hours", 0) if shop else 0

    if not validate_date(date_input, min_lead_hours=min_hours):
        if min_hours:
            err = f"⚠️ This shop needs *{min_hours}h* advance notice. Enter a date at least {min_hours} hours from now:"
        else:
            err = t(get_lang(message.chat.id), "invalid_date")
        bot.send_message(message.chat.id, err,
                         parse_mode="Markdown", reply_markup=cancel_kb(message.chat.id))
        return

    update_session(message.chat.id,
                   delivery_date=date_input,
                   state=STATE_IDLE)
    _show_order_summary(message.chat.id)


def _show_order_summary(chat_id):
    s    = load_session(chat_id)
    shop = get_shop_for_session(chat_id)
    cart = get_cart(chat_id)
    if not s or not shop or not cart:
        return
    # Re-check shop hours at final confirmation — customer may have started checkout before closing
    if not is_shop_open(shop):
        lang = get_lang(chat_id)
        bot.send_message(chat_id,
                         t(lang, "shop_closed", shop_name=shop["name"],
                           open_time=shop.get("open_time",""),
                           close_time=shop.get("close_time","")),
                         parse_mode="Markdown")
        update_session(chat_id, state=STATE_IDLE)
        return

    dc = shop.get("delivery_charge", 0)
    lines, subtotal, total = build_cart_display(cart, dc)

    text = (
        f"📋 *Order Summary*\n{DIV}\n{lines}{DIV}\n"
        f"💰 Subtotal: ₹{fmt(subtotal)}\n"
    )
    if dc:
        text += f"🚚 Delivery: ₹{int(dc)}\n"
    text += (
        f"💳 *Total: ₹{fmt(total)}*\n{DIV}\n"
        f"📍 {md(s.get('address',''))}\n"
        f"📱 {md(s.get('phone',''))}\n"
        f"🗓 Delivery: {md(s.get('delivery_date',''))}\n"
        f"{DIV}\n\n"
        f"💳 *How would you like to pay?*"
    )

    m = InlineKeyboardMarkup()
    if shop.get("upi_id"):
        m.add(InlineKeyboardButton(
            "📲 Pay via UPI / GPay / PhonePe",
            callback_data="pay_upi"
        ))
    m.add(InlineKeyboardButton(
        "💵 Cash on Delivery (COD)",
        callback_data="pay_cod"
    ))
    m.add(InlineKeyboardButton("✏️ Change Details", callback_data="checkout"))
    m.add(InlineKeyboardButton("🏠 Menu",           callback_data="home"))
    bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=m)


@bot.callback_query_handler(func=lambda c: c.data == "pay_upi")
def pay_upi(call):
    bot.answer_callback_query(call.id)
    _place_order(call, payment_method="upi")


@bot.callback_query_handler(func=lambda c: c.data == "pay_cod")
def pay_cod(call):
    bot.answer_callback_query(call.id)
    _place_order(call, payment_method="cod")


# ═══════════════════════════════════════════════════════
# PLACE ORDER — called by pay_upi or pay_cod
# ═══════════════════════════════════════════════════════

_last_order_time = {}  # in-memory cache; session DB is the durable fallback

def _place_order(call, payment_method="upi"):
    """Internal: called after customer picks payment method."""
    now      = time.time()
    # Fast in-memory check first
    last_mem = _last_order_time.get(call.message.chat.id, 0)
    if now - last_mem < 15:
        bot.answer_callback_query(call.id, "⏳ Please wait before placing another order.", show_alert=True)
        return
    # DB-backed check — survives bot restarts
    s_rl = load_session(call.message.chat.id)
    last_db = float((s_rl or {}).get("last_order_time") or 0)
    if now - last_db < 15:
        bot.answer_callback_query(call.id, "⏳ Please wait before placing another order.", show_alert=True)
        return
    _last_order_time[call.message.chat.id] = now
    update_session(call.message.chat.id, last_order_time=str(now))
    s    = load_session(call.message.chat.id)
    cart = get_cart(call.message.chat.id)
    if not s or not cart:
        bot.answer_callback_query(call.id, "Cart is empty.")
        return

    shop = get_shop_for_session(call.message.chat.id)
    if not shop:
        bot.answer_callback_query(call.id, "Session error. Please start again.")
        return

    if is_customer_blocked(call.message.chat.id):
        bot.answer_callback_query(call.id, "Unable to place order.", show_alert=True)
        return

    bot.answer_callback_query(call.id, "⏳ Placing your order...")

    dc       = float(shop.get("delivery_charge", 0))
    subtotal = sum(float(i["price"]) * i.get("quantity",1) for i in cart)
    total    = subtotal + dc
    address  = s.get("address") or "Not provided"
    phone    = s.get("phone")   or "Not provided"
    date     = s.get("delivery_date") or "Not provided"
    cname    = call.from_user.first_name or "Customer"
    prefix   = "".join([w[0].upper() for w in shop["name"].split()[:2]])

    order = create_order(
        client_id=shop["id"],
        customer_name=cname,
        customer_telegram_id=call.message.chat.id,
        cart_items=cart,
        subtotal=subtotal,
        delivery_charge=dc,
        total=total,
        address=address,
        phone=phone,
        delivery_date=date,
        shop_prefix=prefix
    )

    if not order:
        bot.send_message(call.message.chat.id,
                         "❌ Something went wrong. Please try again.")
        return

    order_ref  = order.get("order_ref", f"#ORD-{order['id'][:6].upper()}")
    items_text = ", ".join([
        f"{i['name']}{' ('+i['variant_label']+')' if i.get('variant_label') else ''}"
        for i in cart
    ])

    # Decrement stock for each item
    for item in cart:
        decrement_stock(
            product_id=item.get("product_id"),
            variant_id=item.get("variant_id"),
            qty=int(item.get("quantity", 1))
        )

    # Save customer profile — only if they entered a NEW address (not using saved)
    s_fresh = load_session(call.message.chat.id)
    if not (s_fresh or {}).get("used_saved_address"):
        save_customer_profile(call.message.chat.id, cname, phone, address)
    update_session(call.message.chat.id, used_saved_address=False)  # clear flag

    # ── Payment section ──────────────────────────────
    pay_badge = "💵 Cash on Delivery" if payment_method == "cod" else "📲 UPI / GPay"

    upi_string = None   # defined here, set below if UPI
    if payment_method == "cod":
        pay_section = (
            f"💵 *Payment: Cash on Delivery*\n\n"
            f"Please keep *₹{fmt(total)}* ready at delivery.\n"
            f"No advance payment needed."
        )
    else:
        upi_id = shop.get("upi_id", "")
        if upi_id:
            pay_section, upi_string = build_payment_message(upi_id, fmt(total), order_ref, shop["name"])
        else:
            pay_section = "Please contact the shop for payment details."
            upi_string  = None

    # ── Owner notification ────────────────────────────
    oid = order["id"]
    cid = call.message.chat.id
    om  = InlineKeyboardMarkup(row_width=2)
    om.add(
        InlineKeyboardButton("✅ Confirm", callback_data=f"oconfirm|{oid}|{cid}"),
        InlineKeyboardButton("❌ Cancel",  callback_data=f"ocancel|{oid}|{cid}")
    )
    pending_count = len(get_orders_by_client(shop["id"], status="pending"))
    ist_now = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime("%I:%M %p")
    safe_send(
        shop["owner_telegram"],
        f"🔔 *New Order {order_ref}!* ({pending_count} pending) · {ist_now}\n{DIV}\n"
        f"👤 {md(cname)} | 📱 {md(phone)}\n"
        f"📦 {md(items_text)}\n"
        f"💰 ₹{fmt(subtotal)} + 🚚 ₹{int(dc)} = *₹{fmt(total)}*\n"
        f"📍 {md(address)}\n🗓 {md(date)}\n"
        f"💳 *{pay_badge}*\n{DIV}",
        parse_mode="Markdown", reply_markup=om
    )

    # ── Customer receipt ──────────────────────────────
    m = InlineKeyboardMarkup(row_width=2)
    m.add(
        InlineKeyboardButton("📦 Track Order",  callback_data="my_orders"),
        InlineKeyboardButton("🛍 Order More",   callback_data="browse")
    )
    m.add(InlineKeyboardButton("🏠 Back to Menu", callback_data="home"))

    order_receipt = (
        f"🎉 *Order Placed!*\n{DIV}\n"
        f"🧾 *{order_ref}*\n"
        f"🏪 {shop['name']}\n"
        f"📦 {md(items_text)}\n"
        f"💰 Subtotal: ₹{fmt(subtotal)}\n"
        f"🚚 Delivery: ₹{int(dc)}\n"
        f"💳 *Total: ₹{fmt(total)}*\n"
        f"📍 {md(address)}\n🗓 {md(date)}\n"
        f"{DIV}"
    )
    bot.send_message(call.message.chat.id, order_receipt,
                     parse_mode="Markdown", reply_markup=m)

    # ── UPI Payment ────────────────────────────────────
    if payment_method == "upi" and shop.get("upi_id"):
        # Show UPI ID clearly + QR code if available
        # Note: Telegram inline buttons cannot open UPI apps directly (upi:// blocked)
        # Best experience: show QR code (customer scans in any UPI app) + UPI ID to copy
        upi_id_val = shop.get("upi_id","")
        upi_caption = (
            f"{pay_section}\n\n"
            f"📋 *UPI ID:* `{upi_id_val}`\n"
            f"_Open any UPI app → Scan QR or pay to UPI ID above_"
        )
        qr_buf = build_qr_bytes(upi_string) if upi_string else None
        if qr_buf:
            try:
                bot.send_photo(call.message.chat.id, qr_buf,
                               caption=upi_caption, parse_mode="Markdown")
            except Exception:
                bot.send_message(call.message.chat.id, upi_caption, parse_mode="Markdown")
        else:
            bot.send_message(call.message.chat.id, upi_caption, parse_mode="Markdown")
    elif payment_method == "cod":
        bot.send_message(call.message.chat.id, pay_section, parse_mode="Markdown")

    # Mark payment status based on method
    update_payment_status(order["id"], "paid" if payment_method == "upi" else "pending")

    # Clear cart
    set_cart(call.message.chat.id, [])


# ═══════════════════════════════════════════════════════
# MY ORDERS
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data == "my_orders")
def my_orders(call):
    bot.answer_callback_query(call.id)
    s = load_session(call.message.chat.id)
    if not s:
        bot.send_message(call.message.chat.id, "Please open the shop link first.")
        return

    orders = get_orders_by_customer(call.message.chat.id, s["shop_id"])
    if not orders:
        m = InlineKeyboardMarkup()
        m.add(InlineKeyboardButton("🛍 Start Shopping", callback_data="browse"))
        m.add(InlineKeyboardButton("🏠 Menu",           callback_data="home"))
        bot.send_message(call.message.chat.id,
                         t(get_lang(call.message.chat.id), "no_orders"),
                         parse_mode="Markdown", reply_markup=m)
        return

    icons = {"pending":"⏳","confirmed":"✅","delivered":"🎉","cancelled":"❌"}
    lang  = get_lang(call.message.chat.id)
    text  = t(lang, "my_orders_header") + f"\n{DIV}\n"
    m     = InlineKeyboardMarkup()

    for o in orders[:10]:
        items_str = ", ".join([
            f"{i['name']}{' ('+i['variant_label']+')' if i.get('variant_label') else ''}"
            for i in o.get("items", [])
        ]) or "—"
        ref  = o.get("order_ref", o["id"][:6])
        icon = icons.get(o["status"], "•")
        text += f"{icon} *{ref}*\n   {items_str}\n   ₹{fmt(o['total'])} • {o['status'].title()}\n\n"
        if o["status"] == "pending":
            m.add(InlineKeyboardButton(f"❌ Cancel {ref}", callback_data=f"customer_cancel|{o['id']}"))
        elif o["status"] in ["confirmed","delivered"]:
            m.add(InlineKeyboardButton(f"🔄 Reorder {ref}", callback_data=f"reorder_{o['id']}"))

    m.add(InlineKeyboardButton("🏠 Back to Menu", callback_data="home"))
    bot.send_message(call.message.chat.id, text, parse_mode="Markdown", reply_markup=m)


@bot.callback_query_handler(func=lambda c: c.data.startswith("customer_cancel|"))
def customer_cancel_order(call):
    _, order_id = call.data.split("|", 1)
    order = get_order_by_id(order_id)
    if not order or order.get("status") != "pending":
        bot.answer_callback_query(call.id, "This order can no longer be cancelled.", show_alert=True)
        return
    update_order_status(order_id, "cancelled")
    # Restore stock for every item
    for item in order.get("items", []):
        restore_stock(
            product_id=item.get("product_id"),
            variant_id=item.get("variant_id"),
            qty=int(item.get("quantity", 1))
        )
    bot.answer_callback_query(call.id, "✅ Order cancelled.")
    ref = order.get("order_ref", order_id[:6])
    # Notify owner with item details
    s = load_session(call.message.chat.id)
    if s:
        shop = get_shop_for_session(call.message.chat.id)
        if shop:
            items_str = ", ".join([
                f"{i['name']}{' ('+i.get('variant_label','')+')' if i.get('variant_label') else ''}"
                for i in order.get("items", [])
            ]) or "—"
            safe_send(shop["owner_telegram"],
                      f"ℹ️ Customer cancelled order *{ref}*\n📦 {md(items_str)}\n💰 ₹{fmt(order.get('total',0))}",
                      parse_mode="Markdown")
    bot.send_message(call.message.chat.id,
                     f"✅ Order *{ref}* has been cancelled.",
                     parse_mode="Markdown")


@bot.callback_query_handler(func=lambda c: c.data.startswith("reorder_"))
def reorder(call):
    s = load_session(call.message.chat.id)
    if not s:
        bot.answer_callback_query(call.id, "Session expired.", show_alert=True)
        return

    order_id = call.data[len("reorder_"):]
    order    = get_order_by_id(order_id)
    if not order or not order.get("items"):
        bot.answer_callback_query(call.id, "Could not find that order.", show_alert=True)
        return

    # Rebuild cart — skip deleted or out-of-stock products
    new_cart = []
    skipped  = []
    for i in order["items"]:
        pid = i.get("product_id")
        if not pid:
            skipped.append(i["name"])
            continue
        product = get_product_by_id(pid)
        if not product or not product.get("is_active", True):
            skipped.append(i["name"])
            continue
        if product.get("stock", 99) == 0:
            skipped.append(i["name"] + " (out of stock)")
            continue
        new_cart.append({
            "product_id":    pid,
            "variant_id":    i.get("variant_id"),
            "name":          i["name"],
            "variant_label": i.get("variant_label",""),
            "price":         float(product["price"]),  # current price, not old
            "quantity":      int(i.get("quantity",1))
        })

    if not new_cart:
        bot.answer_callback_query(call.id, "❌ All items from this order are unavailable.", show_alert=True)
        return

    set_cart(call.message.chat.id, new_cart)
    skip_note = f"\n\n⚠️ Skipped: {', '.join(skipped)}" if skipped else ""
    bot.answer_callback_query(call.id, f"✅ {len(new_cart)} item(s) added to cart!")

    lines, subtotal, _ = build_cart_display(new_cart)
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("✅ Checkout Now",   callback_data="checkout"))
    m.add(InlineKeyboardButton("🛍 Add More Items", callback_data="browse"))
    bot.send_message(
        call.message.chat.id,
        f"🔄 *Reorder Ready!*\n{DIV}\n{lines}{DIV}\nAdded to cart!{skip_note}",
        parse_mode="Markdown", reply_markup=m
    )


# ═══════════════════════════════════════════════════════
# OWNER: CONFIRM / CANCEL ORDER
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data.startswith("oconfirm|"))
def owner_confirm(call):
    _, order_id, cid = call.data.split("|")
    customer_id = int(cid)
    order = get_order_by_id(order_id)
    if not order or order.get("status") != "pending":
        bot.answer_callback_query(call.id, "Already processed!", show_alert=True)
        return
    update_order_status(order_id, "confirmed")
    bot.answer_callback_query(call.id, "✅ Confirmed!")
    try:
        bot.edit_message_reply_markup(call.message.chat.id,
                                      call.message.message_id, reply_markup=None)
    except Exception:
        pass

    # Give owner a "Mark as Delivered" button directly
    dm = InlineKeyboardMarkup()
    dm.add(InlineKeyboardButton(
        "🎉 Mark as Delivered",
        callback_data=f"odelivered|{order_id}|{customer_id}"
    ))
    bot.send_message(
        call.message.chat.id,
        "✅ *Order confirmed!* Customer notified.\n\n"
        "👇 Tap below when you've delivered it:",
        parse_mode="Markdown", reply_markup=dm
    )
    safe_send(customer_id,
              t(get_lang(customer_id), "order_confirmed_customer"),
              parse_mode="Markdown")


@bot.callback_query_handler(func=lambda c: c.data.startswith("ocancel|"))
def owner_cancel(call):
    _, order_id, cid = call.data.split("|")
    customer_id = int(cid)

    order = get_order_by_id(order_id)
    if not order or order.get("status") == "cancelled":
        bot.answer_callback_query(call.id, "Already cancelled.", show_alert=True)
        return

    update_order_status(order_id, "cancelled")
    # Restore stock for every item
    for item in order.get("items", []):
        restore_stock(
            product_id=item.get("product_id"),
            variant_id=item.get("variant_id"),
            qty=int(item.get("quantity", 1))
        )
    bot.answer_callback_query(call.id, "❌ Cancelled.")
    try:
        bot.edit_message_reply_markup(call.message.chat.id,
                                      call.message.message_id, reply_markup=None)
    except Exception:
        pass
    bot.send_message(call.message.chat.id, "❌ Order cancelled. Customer notified.")
    shop = get_client_by_owner(call.message.chat.id)
    contact_line = f"\n📞 {shop['contact']}" if shop and shop.get("contact") else ""
    safe_send(customer_id,
              t(get_lang(customer_id), "order_cancelled_customer") + contact_line,
              parse_mode="Markdown")


# ═══════════════════════════════════════════════════════
# OWNER: Mark Delivered — via button (primary) or /delivered command
# ═══════════════════════════════════════════════════════

@bot.callback_query_handler(func=lambda c: c.data.startswith("odelivered|"))
def owner_delivered_btn(call):
    """Owner taps 'Mark as Delivered' button after confirming order."""
    _, order_id, cid = call.data.split("|")
    customer_id = int(cid)
    order = get_order_by_id(order_id)
    if not order or order.get("status") == "delivered":
        bot.answer_callback_query(call.id, "Already delivered!", show_alert=True)
        return
    bot.answer_callback_query(call.id, "🎉 Marked as delivered!")
    try:
        bot.edit_message_reply_markup(call.message.chat.id,
                                      call.message.message_id, reply_markup=None)
    except Exception:
        pass

    _do_mark_delivered(call.message.chat.id, order_id, customer_id)


def _do_mark_delivered(owner_chat_id, order_id, customer_id):
    """Shared logic — marks order delivered, sends review request to customer."""
    order = get_order_by_id(order_id)
    if not order:
        bot.send_message(owner_chat_id, "❌ Order not found.")
        return

    update_order_status(order_id, "delivered")

    # Get shop name for message
    shop = get_client_by_owner(owner_chat_id)
    shop_name = shop["name"] if shop else "the shop"
    ref = order.get("order_ref", order_id[:6])

    bot.send_message(owner_chat_id,
                     f"🎉 *Order {ref} marked as delivered!*\n\n"
                     f"Customer will be asked to rate their order.",
                     parse_mode="Markdown")

    # Send review request to customer — one per product
    items = order.get("items", [])
    if items:
        for item in items:
            pid = item.get("product_id")
            if not pid:
                continue
            name = item["name"]
            if item.get("variant_label"):
                name += f" ({item['variant_label']})"
            m = InlineKeyboardMarkup(row_width=5)
            m.add(
                InlineKeyboardButton("1⭐", callback_data=f"review|{order_id}|{pid}|1"),
                InlineKeyboardButton("2⭐", callback_data=f"review|{order_id}|{pid}|2"),
                InlineKeyboardButton("3⭐", callback_data=f"review|{order_id}|{pid}|3"),
                InlineKeyboardButton("4⭐", callback_data=f"review|{order_id}|{pid}|4"),
                InlineKeyboardButton("5⭐", callback_data=f"review|{order_id}|{pid}|5")
            )
            cid_int = int(customer_id)
            safe_send(cid_int,
                      t(get_lang(cid_int), "order_delivered_customer") + f"\n\n{DIV}\n" +
                      t(get_lang(cid_int), "rate_product", product_name=name) + f"\n{DIV}",
                      parse_mode="Markdown", reply_markup=m)
    else:
        # Fallback — overall shop rating
        m = InlineKeyboardMarkup(row_width=5)
        m.add(
            InlineKeyboardButton("1⭐", callback_data=f"review|{order_id}|0|1"),
            InlineKeyboardButton("2⭐", callback_data=f"review|{order_id}|0|2"),
            InlineKeyboardButton("3⭐", callback_data=f"review|{order_id}|0|3"),
            InlineKeyboardButton("4⭐", callback_data=f"review|{order_id}|0|4"),
            InlineKeyboardButton("5⭐", callback_data=f"review|{order_id}|0|5")
        )
        cid_int = int(customer_id)
        safe_send(cid_int,
                  t(get_lang(cid_int), "order_delivered_customer") + f"\n\n{DIV}\n" +
                  t(get_lang(cid_int), "rate_product", product_name=shop_name) + f"\n{DIV}",
                  parse_mode="Markdown", reply_markup=m)


@bot.message_handler(commands=['delivered'])
def mark_delivered(message):
    """Fallback command — owner can type /delivered TK-001 if button is gone."""
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    args = message.text.split()
    if len(args) < 2:
        # No ref given — show list of confirmed orders with buttons
        orders = get_orders_by_client(shop["id"], status="confirmed")
        if not orders:
            bot.send_message(message.chat.id, "No confirmed orders to mark as delivered.")
            return
        text = f"📦 *Confirmed Orders*\n{DIV}\nTap to mark delivered:\n\n"
        m    = InlineKeyboardMarkup()
        for o in orders:
            ref = o.get("order_ref", o["id"][:6])
            cid = o.get("customer_telegram", "0")
            text += f"• {ref} — {md(o['customer_name'])}\n"
            m.add(InlineKeyboardButton(
                f"🎉 {ref} Delivered",
                callback_data=f"odelivered|{o['id']}|{cid}"
            ))
        bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=m)
        return

    ref    = args[1].upper().replace("#","")
    orders = get_orders_by_client(shop["id"], status="confirmed")
    order  = next((o for o in orders
                   if o.get("order_ref","").replace("#","") == ref), None)
    if not order:
        bot.send_message(message.chat.id,
                         f"❌ Order #{ref} not found.\n\nUse /orders to check.")
        return

    cid = order.get("customer_telegram", "0")
    _do_mark_delivered(message.chat.id, order["id"], cid)


@bot.callback_query_handler(func=lambda c: c.data.startswith("review|"))
def handle_review(call):
    # format: review|order_id|product_id|rating
    _, order_id, pid_str, rating_str = call.data.split("|")
    product_id = int(pid_str) if pid_str != "0" else None
    rating     = int(rating_str)

    # Get shop from order
    order = get_order_by_id(order_id)
    if not order:
        bot.answer_callback_query(call.id)
        return

    save_review(
        client_id=order["client_id"],
        customer_telegram=call.message.chat.id,
        order_id=order_id,
        rating=rating,
        product_id=product_id
    )

    stars = "⭐" * rating
    msgs  = {
        1: "Thanks for the feedback. We'll do better! 🙏",
        2: "Thanks! We'll improve. 🙏",
        3: "Thanks! We'll serve you better next time 😊",
        4: "Great! Thanks for the kind rating 🙏",
        5: "Wow, 5 stars! You made our day! 🎉🙏"
    }
    bot.answer_callback_query(call.id, f"{stars} — Thank you!")
    try:
        bot.edit_message_reply_markup(call.message.chat.id,
                                      call.message.message_id, reply_markup=None)
    except Exception:
        pass
    rm = InlineKeyboardMarkup(row_width=2)
    lang = get_lang(call.message.chat.id)
    rm.add(
        InlineKeyboardButton(t(lang, "start_shopping"), callback_data="browse"),
        InlineKeyboardButton(t(lang, "my_orders"),      callback_data="my_orders")
    )
    bot.send_message(call.message.chat.id,
                     f"{stars}\n\n{msgs.get(rating, t(lang, 'review_thanks'))}",
                     reply_markup=rm)


# ═══════════════════════════════════════════════════════
# OWNER: /stats  /orders  /broadcast
# ═══════════════════════════════════════════════════════

# ── Owner shortcut callbacks (from inline buttons) ──────

@bot.callback_query_handler(func=lambda c: c.data == "owner_stats")
def owner_stats_cb(call):
    bot.answer_callback_query(call.id)
    call.message.text = "/stats"
    stats(call.message)

@bot.callback_query_handler(func=lambda c: c.data == "owner_shoplink")
def owner_shoplink_cb(call):
    bot.answer_callback_query(call.id)
    shop = get_client_by_owner(call.message.chat.id)
    if not shop:
        bot.send_message(call.message.chat.id, "❌ No shop found.")
        return
    link = f"https://t.me/{BOT_USERNAME}?start={shop['slug']}"
    bot.send_message(call.message.chat.id,
                     f"🔗 *Your Shop Link:*\n`{link}`\n\nShare this with your customers!",
                     parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "owner_orders")
def owner_orders_cb(call):
    bot.answer_callback_query(call.id)
    call.message.text = "/orders"
    orders_cmd(call.message)

@bot.callback_query_handler(func=lambda c: c.data == "owner_broadcast")
def owner_broadcast_cb(call):
    bot.answer_callback_query(call.id)
    call.message.text = "/broadcast"
    broadcast_start(call.message)

@bot.callback_query_handler(func=lambda c: c.data == "owner_viewproducts")
def owner_viewproducts_cb(call):
    bot.answer_callback_query(call.id)
    call.message.text = "/viewproducts"
    view_products(call.message)

@bot.callback_query_handler(func=lambda c: c.data == "owner_addproduct")
def owner_addproduct_cb(call):
    bot.answer_callback_query(call.id)
    call.message.text = "/addproduct"
    add_product_start(call.message)

@bot.callback_query_handler(func=lambda c: c.data == "owner_editproduct")
def owner_editproduct_cb(call):
    bot.answer_callback_query(call.id)
    call.message.text = "/editproduct"
    edit_product_start(call.message)

@bot.callback_query_handler(func=lambda c: c.data == "owner_deleteproduct")
def owner_deleteproduct_cb(call):
    bot.answer_callback_query(call.id)
    call.message.text = "/deleteproduct"
    delete_product_start(call.message)


@bot.message_handler(commands=['stats'])
def stats(message):
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    data          = get_client_stats(shop["id"])
    rating, rcount = get_shop_rating(shop["id"])
    sm = InlineKeyboardMarkup(row_width=2)
    sm.add(
        InlineKeyboardButton("📦 Pending Orders", callback_data="owner_orders"),
        InlineKeyboardButton("📣 Broadcast",      callback_data="owner_broadcast")
    )
    sm.add(InlineKeyboardButton("🏷 View Products", callback_data="owner_viewproducts"))
    bot.send_message(
        message.chat.id,
        f"📊 *{shop['name']} Dashboard*\n{DIV}\n"
        f"📦 Total Orders: *{data['total_orders']}*\n"
        f"⏳ Pending: *{data['pending']}*\n"
        f"🎉 Delivered: *{data['delivered']}*\n"
        f"💰 Revenue: *₹{fmt(data['total_revenue'])}*\n"
        f"⭐ Rating: *{f'{rating}/5 ({rcount} reviews)' if rating else 'No reviews yet'}*\n"
        f"{DIV}",
        parse_mode="Markdown", reply_markup=sm
    )


@bot.message_handler(commands=['orders'])
def orders_cmd(message):
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    pending   = get_orders_by_client(shop["id"], status="pending")
    confirmed = get_orders_by_client(shop["id"], status="confirmed")
    all_orders = pending + confirmed
    if not all_orders:
        bot.send_message(message.chat.id, "🎉 No active orders right now!")
        return
    # Send each order as its own message with action buttons
    header = f"📦 *Active Orders — {len(pending)} pending · {len(confirmed)} confirmed*\n{DIV}"
    bot.send_message(message.chat.id, header, parse_mode="Markdown")

    for o in all_orders:
        items_str = ", ".join([
            f"{i['name']}{' ('+i.get('variant_label','')+')' if i.get('variant_label') else ''}"
            for i in o.get("items", [])
        ]) or "—"
        ref  = o.get("order_ref", o["id"][:6])
        icon = "⏳" if o["status"] == "pending" else "✅"
        ts   = ""
        if o.get("created_at"):
            try:
                dt  = datetime.fromisoformat(o["created_at"].replace("Z", "+00:00"))
                ist = dt + timedelta(hours=5, minutes=30)
                ts  = f" · {ist.strftime('%d %b %I:%M%p')}"
            except Exception:
                pass

        text = (
            f"{icon} *{ref}*{ts}\n"
            f"👤 {md(o['customer_name'])} | 📱 {md(o.get('phone', '—'))}\n"
            f"📦 {md(items_str)}\n"
            f"💰 ₹{fmt(o['total'])} | 🗓 {md(o.get('delivery_date', '—'))}\n"
            f"📍 {md(o.get('address', '—'))}"
        )

        oid = o["id"]
        cid = o.get("customer_telegram", "0")
        om  = InlineKeyboardMarkup(row_width=2)

        if o["status"] == "pending":
            om.add(
                InlineKeyboardButton("✅ Confirm", callback_data=f"oconfirm|{oid}|{cid}"),
                InlineKeyboardButton("❌ Cancel",  callback_data=f"ocancel|{oid}|{cid}")
            )
        elif o["status"] == "confirmed":
            om.add(
                InlineKeyboardButton("🎉 Mark Delivered", callback_data=f"odelivered|{oid}|{cid}")
            )

        bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=om)



@bot.message_handler(commands=['broadcast'])
def broadcast_start(message):
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    bot.send_message(message.chat.id,
                     "📣 *Broadcast*\n\nType your message below.\n"
                     "It will be sent to all your customers.\n\n"
                     "_Tip: New products, offers, festival specials!_",
                     parse_mode="Markdown")
    bot.register_next_step_handler(message, do_broadcast, shop)

def do_broadcast(message, shop):
    customers = get_all_customers_of_client(shop["id"])
    if not customers:
        bot.send_message(message.chat.id, "No customers yet!")
        return
    admin_sessions[message.chat.id] = admin_sessions.get(message.chat.id, {})
    bcast_text = message.text or (message.caption if message.photo else "")
    admin_sessions[message.chat.id]["broadcast_text"] = bcast_text
    admin_sessions[message.chat.id]["broadcast_photo"] = message.photo[-1].file_id if message.photo else None
    admin_sessions[message.chat.id]["broadcast_shop"] = shop
    m = InlineKeyboardMarkup()
    m.add(
        InlineKeyboardButton(f"\u2705 Send to {len(customers)} customers", callback_data="bc_confirm"),
        InlineKeyboardButton("\u274c Cancel", callback_data="bc_cancel")
    )
    bot.send_message(
        message.chat.id,
        f"\U0001f4e3 *Preview:*\n{DIV}\n{bcast_text or '(photo — no caption)'}\n{DIV}\n\nSend to *{len(customers)}* customers?",
        parse_mode="Markdown", reply_markup=m
    )

@bot.callback_query_handler(func=lambda c: c.data == "bc_confirm")
def bc_confirm(call):
    s    = admin_sessions.get(call.message.chat.id, {})
    text = s.get("broadcast_text")
    shop = s.get("broadcast_shop")
    if not shop:
        bot.answer_callback_query(call.id, "Session lost. Try /broadcast again.")
        return
    # text can be None for photo-only broadcasts — that's valid
    bot.answer_callback_query(call.id, "\U0001f4e4 Sending...")
    customers = get_all_customers_of_client(shop["id"])
    success = 0
    for c in customers:
        if is_customer_blocked(c["customer_telegram"]):
            continue
        photo_id = s.get("broadcast_photo")
        if photo_id:
            try:
                bot.send_photo(int(c["customer_telegram"]), photo_id,
                               caption=f"📢 *{md(shop['name'])}*\n{DIV}\n{md(text)}" if text else f"📢 *{md(shop['name'])}*",
                               parse_mode="Markdown")
                success += 1
                continue
            except Exception:
                pass
        if text and safe_send(int(c["customer_telegram"]),
                     f"📢 *{md(shop['name'])}*\n{DIV}\n{md(text)}",
                     parse_mode="Markdown"):
            success += 1
        elif not text:
            success += 1  # photo-only already counted above
        time.sleep(0.05)
    bot.send_message(call.message.chat.id,
                     f"\u2705 *Done!* Sent to {success}/{len(customers)} customers.",
                     parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "bc_cancel")
def bc_cancel(call):
    admin_sessions.pop(call.message.chat.id, None)
    bot.answer_callback_query(call.id, "Broadcast cancelled.")
    bot.send_message(call.message.chat.id, "\u274c Broadcast cancelled.")


# ═══════════════════════════════════════════════════════
# OWNER: PRODUCT MANAGEMENT
# ═══════════════════════════════════════════════════════

@bot.message_handler(commands=['viewproducts'])
def view_products(message):
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    products = get_products(shop["id"], include_inactive=False)
    if not products:
        bot.send_message(message.chat.id, "📦 No products yet.\n\nUse /addproduct!")
        return
    text = f"📦 *Your Products ({len(products)})*\n{DIV}\n"
    for p in products:
        stock  = p.get("stock",99)
        sl     = "❌ Out of Stock" if stock == 0 else f"✅ {stock} in stock"
        cat    = f" [{p['category']}]" if p.get("category") else ""
        photo  = "📷" if p.get("photo") else "🚫"
        rating, rcount = get_product_rating(p["id"])
        stars  = f"⭐{rating}" if rating else "—"
        # Check variants
        variants = get_variants(p["id"])
        var_str  = f" | {len(variants)} variants" if variants else ""
        text += (
            f"*ID {p['id']}* — {p['name']}{cat}\n"
            f"   ₹{fmt(p['price'])} | {sl} | {photo} | {stars}{var_str}\n\n"
        )
    vm = InlineKeyboardMarkup(row_width=3)
    vm.add(
        InlineKeyboardButton("➕ Add",    callback_data="owner_addproduct"),
        InlineKeyboardButton("✏️ Edit",   callback_data="owner_editproduct"),
        InlineKeyboardButton("🗑 Delete", callback_data="owner_deleteproduct")
    )
    text += f"{DIV}"
    for i in range(0, len(text), 4000):
        bot.send_message(message.chat.id, text[i:i+4000], parse_mode="Markdown",
                         reply_markup=vm if i+4000 >= len(text) else None)


@bot.message_handler(commands=['addproduct'])
def add_product_start(message):
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    admin_sessions[message.chat.id] = {"shop": shop, "new_product": {}}
    bot.send_message(
        message.chat.id,
        f"➕ *Add New Product*\n{DIV}\n*Step 1 of 7* — Product name:",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(message, ap_name)

def ap_name(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /addproduct")
        return
    if not message.text or not message.text.strip():
        bot.send_message(message.chat.id, "⚠️ Please *type* the product name (no photos here):",
                         parse_mode="Markdown")
        bot.register_next_step_handler(message, ap_name)
        return
    s["new_product"]["name"] = message.text.strip()
    bot.send_message(message.chat.id,
                     "*Step 2 of 7* — Base price? _(e.g. 299)_\n\n"
                     "_If you have variants (sizes/weights), enter the base/starting price._",
                     parse_mode="Markdown")
    bot.register_next_step_handler(message, ap_price)

def ap_price(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /addproduct")
        return
    try:
        price = float(re.sub(r'[^\d.]', '', message.text))
        if price <= 0: raise ValueError
    except (ValueError, TypeError):
        bot.send_message(message.chat.id, "⚠️ Invalid. Enter a number like 299:")
        bot.register_next_step_handler(message, ap_price)
        return
    s["new_product"]["price"] = price
    bot.send_message(message.chat.id,
                     "*Step 3 of 7* — Category?\n_(e.g. Cakes, Cookies, Dresses)_\nOr type *skip*",
                     parse_mode="Markdown")
    bot.register_next_step_handler(message, ap_category)

def ap_category(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /addproduct")
        return
    s["new_product"]["category"] = "" if (message.text or "").lower()=="skip" else (message.text or "").strip()
    bot.send_message(message.chat.id,
                     "*Step 4 of 7* — Description?\n_Short description or type *skip*_",
                     parse_mode="Markdown")
    bot.register_next_step_handler(message, ap_description)

def ap_description(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /addproduct")
        return
    s["new_product"]["description"] = "" if (message.text or "").lower()=="skip" else (message.text or "").strip()
    bot.send_message(message.chat.id,
                     "*Step 5 of 7* — Stock quantity?\n_Type 99 for unlimited_",
                     parse_mode="Markdown")
    bot.register_next_step_handler(message, ap_stock)

def ap_stock(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /addproduct")
        return
    try:
        stock = int(re.sub(r'[^\d]', '', message.text))
    except (ValueError, TypeError):
        bot.send_message(message.chat.id, "⚠️ Invalid. Enter a number like 10:")
        bot.register_next_step_handler(message, ap_stock)
        return
    s["new_product"]["stock"] = stock
    bot.send_message(
        message.chat.id,
        "*Step 6 of 7* — Does this product have sizes or weights?\n\n"
        "_Examples:_\n"
        "• _500g, 1kg, 2kg_\n"
        "• _S, M, L, XL_\n"
        "• _Eggless, With Egg_\n\n"
        "Type variants as: `label:price, label:price`\n"
        "Example: `500g:299, 1kg:499, 2kg:849`\n\n"
        "Or type *skip* if no variants.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(message, ap_variants)

def ap_variants(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /addproduct")
        return
    if (message.text or "").lower() == "skip":
        s["new_product"]["variants"] = []
    else:
        variants = []
        order    = 0
        if not message.text:
            bot.send_message(message.chat.id, "⚠️ Please type variant text or *skip*:", parse_mode="Markdown")
            bot.register_next_step_handler(message, ap_variants)
            return
        for part in message.text.split(","):
            part = part.strip()
            if ":" in part:
                try:
                    label, price = part.split(":", 1)
                    variants.append({
                        "label":      label.strip(),
                        "price":      float(re.sub(r'[^\d.]', '', price)),
                        "sort_order": order
                    })
                    order += 1
                except Exception:
                    pass
        if not variants:
            bot.send_message(message.chat.id,
                             "⚠️ Invalid format.\nUse: `500g:299, 1kg:499`\nOr type *skip*:",
                             parse_mode="Markdown")
            bot.register_next_step_handler(message, ap_variants)
            return
        s["new_product"]["variants"] = variants

    bot.send_message(
        message.chat.id,
        "*Step 7 of 7* — Send a product photo 📸\n\n"
        "_Open gallery and send the photo._\n_Or type *skip*_",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(message, ap_photo)

def ap_photo(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /addproduct")
        return
    if message.photo:
        photo_id = message.photo[-1].file_id
    elif message.text and message.text.lower() == "skip":
        photo_id = ""
    else:
        bot.send_message(message.chat.id,
                         "⚠️ Send a *photo* or type *skip*:", parse_mode="Markdown")
        bot.register_next_step_handler(message, ap_photo)
        return

    p    = s["new_product"]
    shop = s["shop"]

    result = add_product(
        client_id=shop["id"],
        name=p["name"],
        price=p["price"],
        description=p.get("description",""),
        photo=photo_id,
        category=p.get("category",""),
        stock=p.get("stock",99)
    )

    if result and p.get("variants"):
        for v in p["variants"]:
            add_variant(
                product_id=result["id"],
                client_id=shop["id"],
                label=v["label"],
                price=v["price"],
                stock=p.get("stock",99),
                sort_order=v.get("sort_order",0)
            )

    price    = fmt(p["price"])
    cat_line = f"🏷 Category: {p['category']}\n" if p.get("category") else ""
    var_line = ""
    if p.get("variants"):
        var_line = "📐 Variants:\n"
        for v in p["variants"]:
            var_line += f"   • {v['label']} — ₹{fmt(v['price'])}\n"

    bot.send_message(
        message.chat.id,
        f"✅ *Product added!*\n{DIV}\n"
        f"🏷 {p['name']}\n"
        f"💰 ₹{price}\n"
        f"{cat_line}{var_line}"
        f"📦 Stock: {p.get('stock',99)}\n"
        f"📷 Photo: {'✅' if photo_id else '❌'}\n"
        f"{DIV}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("➕ Add Another", callback_data="owner_addproduct"),
            InlineKeyboardButton("📦 View All",    callback_data="owner_viewproducts")
        )
    )


@bot.message_handler(commands=['editproduct'])
def edit_product_start(message):
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    products = get_products(shop["id"])
    if not products:
        bot.send_message(message.chat.id, "No products. Use /addproduct first.")
        return
    text = f"✏️ *Edit Product*\n{DIV}\nSend the product ID:\n\n"
    for p in products:
        text += f"*ID {p['id']}* — {p['name']} — ₹{fmt(p['price'])}\n"
    admin_sessions[message.chat.id] = {"shop": shop}
    bot.send_message(message.chat.id, text, parse_mode="Markdown")
    bot.register_next_step_handler(message, ep_id)

def ep_id(message):
    try:
        pid = int(message.text.strip())
    except (ValueError, TypeError):
        bot.send_message(message.chat.id, "⚠️ Invalid ID. Send a number.")
        return
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /editproduct")
        return
    s["edit_id"] = pid
    bot.send_message(
        message.chat.id,
        f"✏️ *Editing ID {pid}*\n{DIV}\n"
        f"Send what to change:\n\n"
        f"• `name: New Name`\n• `price: 350`\n"
        f"• `category: Cakes`\n• `description: text`\n"
        f"• `stock: 15`\n\n"
        f"To update variants type:\n"
        f"`variants: 500g:299, 1kg:499`\n\n"
        f"Or send a *new photo* 📸",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(message, ep_edit)

def ep_edit(message):
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /editproduct")
        return

    if message.photo:
        edit_product(s["shop"]["id"], s["edit_id"], photo=message.photo[-1].file_id)
        bot.send_message(message.chat.id, "✅ *Photo updated!*\n\n/viewproducts",
                         parse_mode="Markdown")
        return

    text = message.text or ""
    if text.lower().startswith("variants:"):
        raw      = text[9:].strip()
        variants = []
        order    = 0
        for part in raw.split(","):
            part = part.strip()
            if ":" in part:
                try:
                    label, price = part.split(":", 1)
                    variants.append({
                        "label":      label.strip(),
                        "price":      float(re.sub(r'[^\d.]', '', price)),
                        "sort_order": order
                    })
                    order += 1
                except Exception:
                    pass
        if variants:
            delete_variants(s["edit_id"])
            for v in variants:
                add_variant(s["edit_id"], s["shop"]["id"],
                            v["label"], v["price"], sort_order=v["sort_order"])
            bot.send_message(message.chat.id,
                             f"✅ *Variants updated!*\n\n/viewproducts",
                             parse_mode="Markdown")
        else:
            bot.send_message(message.chat.id,
                             "⚠️ Invalid format. Use: `500g:299, 1kg:499`",
                             parse_mode="Markdown")
        return

    try:
        field, value = text.split(":", 1)
        field = field.strip().lower()
        value = value.strip()
        if field not in ["name","price","description","stock","category"]:
            bot.send_message(message.chat.id,
                             "⚠️ Invalid field. Use: name, price, category, description, stock")
            return
        if field == "price":
            value = float(re.sub(r'[^\d.]', '', value))
        elif field == "stock":
            value = int(re.sub(r'[^\d]', '', value))
        edit_product(s["shop"]["id"], s["edit_id"], **{field: value})
        bot.send_message(message.chat.id,
                         f"✅ *{field.title()}* updated.\n\n/viewproducts",
                         parse_mode="Markdown")
    except Exception:
        bot.send_message(message.chat.id,
                         "⚠️ Format: `field: value`\nExample: `price: 350`",
                         parse_mode="Markdown")


@bot.message_handler(commands=['deleteproduct'])
def delete_product_start(message):
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    products = get_products(shop["id"])
    if not products:
        bot.send_message(message.chat.id, "No products to delete.")
        return
    text = f"🗑 *Delete Product*\n{DIV}\nSend the ID:\n\n"
    for p in products:
        text += f"*ID {p['id']}* — {p['name']}\n"
    admin_sessions[message.chat.id] = {"shop": shop}
    bot.send_message(message.chat.id, text, parse_mode="Markdown")
    bot.register_next_step_handler(message, dp_confirm)

def dp_confirm(message):
    try:
        pid = int(message.text.strip())
    except (ValueError, TypeError):
        bot.send_message(message.chat.id, "⚠️ Invalid ID.")
        return
    s = admin_sessions.get(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "⚠️ Session lost. Please start again with /deleteproduct")
        return
    # Show product name + confirmation button
    product = get_product_by_id(pid)
    if not product or product.get("client_id") != s["shop"]["id"]:
        bot.send_message(message.chat.id, "❌ Product not found.")
        return
    s["delete_id"] = pid
    m = InlineKeyboardMarkup()
    m.add(
        InlineKeyboardButton(f"⚠️ Yes, Delete '{product['name']}'", callback_data=f"dp_yes_{pid}"),
        InlineKeyboardButton("❌ Cancel", callback_data="dp_no")
    )
    bot.send_message(message.chat.id,
                     f"⚠️ *Are you sure?*\n\nDelete *{product['name']}* (ID {pid})?\n\nThis cannot be undone.",
                     parse_mode="Markdown", reply_markup=m)

@bot.callback_query_handler(func=lambda c: c.data.startswith("dp_yes_"))
def dp_yes(call):
    s = admin_sessions.get(call.message.chat.id, {})
    pid = int(call.data.split("_")[-1])
    shop = s.get("shop")
    if not shop:
        bot.answer_callback_query(call.id, "Session lost.")
        return
    delete_product(shop["id"], pid)
    bot.answer_callback_query(call.id, "✅ Deleted!")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except Exception:
        pass
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("📦 View Products", callback_data="owner_viewproducts"))
    bot.send_message(call.message.chat.id, f"✅ Product deleted.", reply_markup=m)

@bot.callback_query_handler(func=lambda c: c.data == "dp_no")
def dp_no(call):
    bot.answer_callback_query(call.id, "Cancelled.")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    except Exception:
        pass
    bot.send_message(call.message.chat.id, "❌ Delete cancelled.")


# ═══════════════════════════════════════════════════════
# OWNER: /help
# ═══════════════════════════════════════════════════════

@bot.message_handler(commands=['setlogo'])
def set_logo_start(message):
    """Owner sends shop logo/banner photo."""
    shop = get_client_by_owner(message.chat.id)
    if not shop:
        bot.send_message(message.chat.id, "❌ You don't have a shop registered.")
        return
    bot.send_message(
        message.chat.id,
        f"🖼 *Set Shop Logo / Banner*\n\n{DIV}\n"
        "Send your shop photo now.\n\n"
        "_Tip: Use a square logo or wide banner (1280×640px looks best)_\n\n"
        "This will be shown to every customer when they open your shop.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(message, set_logo_save, shop)

def set_logo_save(message, shop):
    if not message.photo:
        if message.text and message.text.lower() == "remove":
            update_client(shop["id"], shop_photo="")
            bot.send_message(message.chat.id,
                             "✅ Shop logo removed.\n\nCustomers will see text welcome now.")
            return
        bot.send_message(message.chat.id,
                         "⚠️ Please send a *photo*.\nOr type *remove* to clear the logo.",
                         parse_mode="Markdown")
        bot.register_next_step_handler(message, set_logo_save, shop)
        return

    photo_id = message.photo[-1].file_id
    update_client(shop["id"], shop_photo=photo_id)
    bot.send_message(
        message.chat.id,
        "✅ *Shop logo saved!*\n\n"
        "Customers will see this photo when they open your shop.\n\n"
        "_To change it, just run /setlogo again._\n"
        "_To remove it, run /setlogo and type 'remove'._",
        parse_mode="Markdown"
    )


@bot.message_handler(commands=['cancel'])
def cancel_cmd(message):
    """Global cancel — gets customer out of any stuck checkout state."""
    s = load_session(message.chat.id)
    if not s:
        bot.send_message(message.chat.id, "Nothing to cancel.")
        return
    state = s.get("state", STATE_IDLE)
    if state == STATE_IDLE:
        bot.send_message(message.chat.id, "You're not in the middle of anything. Use your shop link to start.")
        return
    update_session(message.chat.id, state=STATE_IDLE)
    shop = get_shop_for_session(message.chat.id)
    shop_name = shop["name"] if shop else "the shop"
    m = InlineKeyboardMarkup()
    m.add(InlineKeyboardButton("🛒 Back to Cart", callback_data="cart"))
    m.add(InlineKeyboardButton("🏠 Menu",         callback_data="home"))
    bot.send_message(
        message.chat.id,
        t(get_lang(message.chat.id), "checkout_cancelled"),

        reply_markup=m
    )


@bot.message_handler(commands=['help'])
def help_cmd(message):
    hm = InlineKeyboardMarkup(row_width=2)
    hm.add(
        InlineKeyboardButton("📊 Stats",       callback_data="owner_stats"),
        InlineKeyboardButton("📦 Orders",      callback_data="owner_orders")
    )
    hm.add(
        InlineKeyboardButton("🏷 Products",    callback_data="owner_viewproducts"),
        InlineKeyboardButton("📣 Broadcast",   callback_data="owner_broadcast")
    )
    hm.add(
        InlineKeyboardButton("➕ Add Product", callback_data="owner_addproduct"),
        InlineKeyboardButton("🔗 Shop Link",   callback_data="owner_shoplink")
    )
    bot.send_message(
        message.chat.id,
        f"🛍 *TeleKart — Owner Commands*\n{DIV}\n"
        f"📦 /orders · /stats · /delivered TK-001\n"
        f"📣 /broadcast\n"
        f"🏷 /viewproducts · /addproduct · /editproduct · /deleteproduct\n"
        f"🖼 /setlogo\n"
        f"{DIV}\n_Tap a button below to get started quickly:_",
        parse_mode="Markdown", reply_markup=hm
    )


# ═══════════════════════════════════════════════════════
# CATCH-ALL
# ═══════════════════════════════════════════════════════

@bot.message_handler(func=lambda msg: get_state(msg.chat.id) == STATE_IDLE)
def unknown_message(message):
    # Check if it looks like a shop link
    if message.text and ("t.me/" in message.text or "telegram.me/" in message.text):
        handle_pasted_link(message)
        return

    s = load_session(message.chat.id)
    if s:
        bot.send_message(message.chat.id,
                         "😊 Use the buttons below:",
                         reply_markup=home_kb(message.chat.id))
    else:
        bot.send_message(message.chat.id,
                         "👋 Use your shop link to get started,\nor type /help if you're a shop owner.")


# ═══════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════

while True:
    try:
        print("✅ TeleKart Bot is running...")
        bot.polling(none_stop=True, interval=0, timeout=20)
    except Exception as e:
        print(f"❌ Polling crashed: {e}. Restarting in 5s...")
        time.sleep(5)