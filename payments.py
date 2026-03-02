# ═══════════════════════════════════════════════════════
# TeleKart — Payment Layer
# Strategy:
#   1. Send QR code image (works in every app, scan it)
#   2. Show UPI ID + amount clearly to copy-paste
#   3. Inline buttons that open GPay / PhonePe / Paytm directly
# ═══════════════════════════════════════════════════════

import urllib.parse
import io

try:
    import qrcode
    QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False


def build_upi_string(upi_id, amount, order_ref, shop_name):
    """
    Standard UPI intent string.
    upi://pay?pa=ID&pn=NAME&am=AMOUNT&cu=INR&tn=NOTE
    Used for QR code AND app deep links.
    """
    note = urllib.parse.quote(str(order_ref))
    name = urllib.parse.quote(str(shop_name)[:20])   # UPI spec: name max 20 chars
    return (
        f"upi://pay?pa={upi_id}"
        f"&pn={name}"
        f"&am={amount}"
        f"&cu=INR"
        f"&tn={note}"
    )


def build_qr_bytes(upi_string):
    """
    Generate a QR code PNG from the UPI string.
    Returns BytesIO ready for bot.send_photo(), or None if qrcode not installed.
    """
    if not QR_AVAILABLE:
        return None
    try:
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=4,
        )
        qr.add_data(upi_string)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        buf.name = "payment_qr.png"
        return buf
    except Exception as e:
        print(f"QR generation error: {e}")
        return None


def build_payment_message(upi_id, amount, order_ref, shop_name):
    """
    Returns (caption_text, upi_string).
    caption_text → shown with QR photo or as standalone message.
    upi_string   → used to generate QR + inline app buttons.
    """
    try:
        int_amount = int(float(amount)) if float(amount) == int(float(amount)) else amount
    except Exception:
        int_amount = amount

    upi_string = build_upi_string(upi_id, amount, order_ref, shop_name)

    caption = (
        f"💳 *Pay ₹{int_amount} — {order_ref}*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📷 *Scan the QR* with any UPI app\n\n"
        f"✏️ *Or copy UPI ID manually:*\n"
        f"`{upi_id}`\n"
        f"Amount: `₹{int_amount}`\n"
        f"Note: `{order_ref}`\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⚠️ After paying, wait for confirmation.\n"
        f"_Do NOT pay twice. Screenshot is NOT proof._"
    )

    return caption, upi_string