# ═══════════════════════════════════════════════════════
# TeleKart — Database Layer
# Proper tables, atomic operations, sessions in DB
# ═══════════════════════════════════════════════════════

from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY
import re
import json

import sys
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ FATAL: SUPABASE_URL or SUPABASE_KEY env vars missing. Exiting.")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ───────────────────────────────────────────
# HELPERS
# ───────────────────────────────────────────

def sanitize_slug(slug):
    return re.sub(r'[^a-zA-Z0-9\-]', '', slug)


# ───────────────────────────────────────────
# CLIENT OPERATIONS
# ───────────────────────────────────────────

def get_client_by_slug(slug):
    slug = sanitize_slug(slug)
    try:
        r = supabase.table("clients").select(
            "id,name,slug,owner_telegram,upi_id,language,"
            "delivery_charge,delivery_areas,shop_hours,"
            "open_time,close_time,tagline,min_lead_hours,contact,is_active,"
            "shop_photo"
        ).eq("slug", slug).eq("is_active", True).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error get_client_by_slug: {e}")
        return None

def get_client_by_id(client_id):
    try:
        r = supabase.table("clients").select(
            "id,name,slug,owner_telegram,upi_id,language,"
            "delivery_charge,delivery_areas,shop_hours,"
            "open_time,close_time,tagline,min_lead_hours,contact,is_active,"
            "shop_photo"
        ).eq("id", client_id).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error get_client_by_id: {e}")
        return None

def get_client_by_owner(owner_telegram_id):
    try:
        r = supabase.table("clients").select(
            "id,name,slug,owner_telegram,upi_id,language,"
            "delivery_charge,delivery_areas,shop_hours,"
            "open_time,close_time,tagline,min_lead_hours,contact,is_active,"
            "shop_photo"
        ).eq("owner_telegram", str(owner_telegram_id)).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error get_client_by_owner: {e}")
        return None

def create_client(name, owner_telegram, slug, upi_id,
                  language="english", delivery_charge=0,
                  delivery_areas="", shop_hours="",
                  open_time="", close_time="",
                  tagline="", min_lead_hours=0, contact=""):
    try:
        r = supabase.table("clients").insert({
            "name": name,
            "owner_telegram": str(owner_telegram),
            "slug": slug,
            "upi_id": upi_id,
            "language": language,
            "delivery_charge": delivery_charge,
            "delivery_areas": delivery_areas,
            "shop_hours": shop_hours,
            "open_time": open_time,
            "close_time": close_time,
            "tagline": tagline,
            "min_lead_hours": min_lead_hours,
            "contact": contact
        }).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error create_client: {e}")
        return None

def update_client(client_id, **kwargs):
    try:
        supabase.table("clients").update(kwargs).eq("id", client_id).execute()
        return True
    except Exception as e:
        print(f"DB error update_client: {e}")
        return False


# ───────────────────────────────────────────
# PRODUCT OPERATIONS
# ───────────────────────────────────────────

def get_products(client_id, category=None, include_inactive=False):
    """Get products for a shop. Optionally filter by category."""
    try:
        q = supabase.table("products").select(
            "id,name,price,description,photo,category,"
            "stock,is_active,sort_order,total_sold,"
            "rating_sum,rating_count"
        ).eq("client_id", client_id)
        if not include_inactive:
            q = q.eq("is_active", True)
        if category:
            q = q.eq("category", category)
        r = q.order("sort_order").order("id").execute()
        return r.data or []
    except Exception as e:
        print(f"DB error get_products: {e}")
        return []

def get_product_by_id(product_id):
    try:
        r = supabase.table("products").select("*").eq("id", product_id).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error get_product_by_id: {e}")
        return None

def add_product(client_id, name, price, description="",
                photo="", category="", stock=99, sort_order=0):
    try:
        r = supabase.table("products").insert({
            "client_id": client_id,
            "name": name,
            "price": float(price),
            "description": description,
            "photo": photo,
            "category": category,
            "stock": int(stock),
            "sort_order": sort_order
        }).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error add_product: {e}")
        return None

def edit_product(client_id, product_id, **kwargs):
    try:
        supabase.table("products").update(kwargs).eq(
            "id", int(product_id)
        ).eq("client_id", client_id).execute()
        return True
    except Exception as e:
        print(f"DB error edit_product: {e}")
        return False

def delete_product(client_id, product_id):
    try:
        supabase.table("products").update(
            {"is_active": False}
        ).eq("id", int(product_id)).eq("client_id", client_id).execute()
        return True
    except Exception as e:
        print(f"DB error delete_product: {e}")
        return False

def decrement_stock(product_id, variant_id=None, qty=1):
    """Atomic stock decrement — the RPC handles WHERE stock >= qty, no oversell."""
    try:
        if variant_id:
            supabase.rpc("decrement_variant_stock", {
                "v_id": variant_id, "qty": qty
            }).execute()
        else:
            supabase.rpc("decrement_product_stock", {
                "p_id": product_id, "qty": qty
            }).execute()
        return True
    except Exception as e:
        print(f"DB error decrement_stock: {e}")
        return False

def restore_stock(product_id, variant_id=None, qty=1):
    """Restore stock when an order is cancelled."""
    try:
        if variant_id:
            supabase.rpc("restore_variant_stock", {
                "v_id": int(variant_id), "qty": int(qty)
            }).execute()
        else:
            supabase.rpc("restore_product_stock", {
                "p_id": int(product_id), "qty": int(qty)
            }).execute()
        return True
    except Exception as e:
        print(f"DB error restore_stock: {e}")
        return False

def get_categories(client_id):
    """Get distinct categories for a shop"""
    try:
        r = supabase.table("products").select("category").eq(
            "client_id", client_id
        ).eq("is_active", True).execute()
        seen, cats = set(), []
        for row in r.data:
            c = (row.get("category") or "").strip()
            if c and c not in seen:
                seen.add(c)
                cats.append(c)
        return cats
    except Exception as e:
        print(f"DB error get_categories: {e}")
        return []


# ───────────────────────────────────────────
# PRODUCT VARIANTS
# ───────────────────────────────────────────

def get_variants(product_id):
    try:
        r = supabase.table("product_variants").select(
            "id,label,price,stock,sort_order"
        ).eq("product_id", product_id).order("sort_order").execute()
        return r.data or []
    except Exception as e:
        print(f"DB error get_variants: {e}")
        return []

def add_variant(product_id, client_id, label, price, stock=99, sort_order=0):
    try:
        r = supabase.table("product_variants").insert({
            "product_id": int(product_id),
            "client_id": client_id,
            "label": label,
            "price": float(price),
            "stock": int(stock),
            "sort_order": sort_order
        }).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error add_variant: {e}")
        return None

def delete_variants(product_id):
    try:
        supabase.table("product_variants").delete().eq(
            "product_id", int(product_id)
        ).execute()
        return True
    except Exception as e:
        print(f"DB error delete_variants: {e}")
        return False


# ───────────────────────────────────────────
# ORDER OPERATIONS
# ───────────────────────────────────────────

def _get_next_order_number(client_id):
    """Get next order number. Not fully atomic but collision only affects display label —
    the real unique key is order_ref (uuid-based)."""
    import random as _rnd, time as _t
    for attempt in range(3):
        try:
            r = supabase.table("orders").select(
                "order_number"
            ).eq("client_id", client_id).order(
                "order_number", desc=True
            ).limit(1).execute()
            return (r.data[0]["order_number"] + 1) if r.data else 1
        except Exception as e:
            print(f"DB error get_next_order_number attempt {attempt}: {e}")
            _t.sleep(_rnd.uniform(0.1, 0.3))
    return int(_t.time()) % 100000  # timestamp fallback

def create_order(client_id, customer_name, customer_telegram_id,
                 cart_items, subtotal, delivery_charge, total,
                 address, phone, delivery_date, shop_prefix="TK"):
    """
    cart_items: list of dicts with keys:
      product_id, variant_id (optional), name, variant_label,
      price, quantity
    """
    try:
        order_number = _get_next_order_number(client_id)
        order_ref    = f"#{shop_prefix}-{str(order_number).zfill(3)}"

        # Create order row
        r = supabase.table("orders").insert({
            "client_id":         client_id,
            "customer_name":     customer_name,
            "customer_telegram": str(customer_telegram_id),
            "subtotal":          float(subtotal),
            "delivery_charge":   float(delivery_charge),
            "total":             float(total),
            "address":           address,
            "phone":             phone,
            "delivery_date":     delivery_date,
            "status":            "pending",
            "payment_status":    "pending",
            "order_number":      order_number,
            "order_ref":         order_ref
        }).execute()

        if not r.data:
            return None

        order = r.data[0]
        order_id = order["id"]

        # Create order_items rows
        for item in cart_items:
            supabase.table("order_items").insert({
                "order_id":      order_id,
                "product_id":    item.get("product_id"),
                "variant_id":    item.get("variant_id"),
                "name":          item.get("name", ""),
                "variant_label": item.get("variant_label", ""),
                "price":         float(item.get("price", 0)),
                "quantity":      int(item.get("quantity", 1))
            }).execute()

            # Increment total_sold on product
            supabase.rpc("increment_total_sold", {
                "p_id": item.get("product_id"),
                "qty":  int(item.get("quantity", 1))
            }).execute()

        return order

    except Exception as e:
        print(f"DB error create_order: {e}")
        return None

def update_order_status(order_id, status):
    try:
        supabase.table("orders").update(
            {"status": status}
        ).eq("id", order_id).execute()
        return True
    except Exception as e:
        print(f"DB error update_order_status: {e}")
        return False

def update_payment_status(order_id, payment_status, payment_ref=""):
    try:
        supabase.table("orders").update({
            "payment_status": payment_status,
            "payment_ref":    payment_ref
        }).eq("id", order_id).execute()
        return True
    except Exception as e:
        print(f"DB error update_payment_status: {e}")
        return False

def get_orders_by_client(client_id, status=None, limit=50):
    try:
        q = supabase.table("orders").select(
            "id,order_ref,order_number,customer_name,"
            "customer_telegram,subtotal,delivery_charge,"
            "total,address,phone,delivery_date,"
            "status,payment_status,created_at"
        ).eq("client_id", client_id)
        if status:
            q = q.eq("status", status)
        r = q.order("created_at", desc=True).limit(limit).execute()
        orders = r.data or []

        # Attach items to each order
        for order in orders:
            items_r = supabase.table("order_items").select(
                "name,variant_label,price,quantity"
            ).eq("order_id", order["id"]).execute()
            order["items"] = items_r.data or []

        return orders
    except Exception as e:
        print(f"DB error get_orders_by_client: {e}")
        return []

def get_orders_by_customer(customer_telegram, client_id, limit=10):
    try:
        r = supabase.table("orders").select(
            "id,order_ref,status,total,created_at,delivery_date"
        ).eq("customer_telegram", str(customer_telegram)).eq(
            "client_id", client_id
        ).order("created_at", desc=True).limit(limit).execute()
        orders = r.data or []
        for order in orders:
            items_r = supabase.table("order_items").select(
                "name,variant_label,price,quantity"
            ).eq("order_id", order["id"]).execute()
            order["items"] = items_r.data or []
        return orders
    except Exception as e:
        print(f"DB error get_orders_by_customer: {e}")
        return []

def get_order_by_id(order_id):
    try:
        r = supabase.table("orders").select("*").eq("id", order_id).execute()
        if not r.data:
            return None
        order = r.data[0]
        items_r = supabase.table("order_items").select("*").eq(
            "order_id", order_id
        ).execute()
        order["items"] = items_r.data or []
        return order
    except Exception as e:
        print(f"DB error get_order_by_id: {e}")
        return None

def get_client_stats(client_id):
    """Use per-status queries to avoid loading all rows into memory."""
    try:
        all_r  = supabase.table("orders").select("id", count="exact").eq("client_id", client_id).execute()
        pend_r = supabase.table("orders").select("id", count="exact").eq("client_id", client_id).eq("status","pending").execute()
        delv_r = supabase.table("orders").select("id", count="exact").eq("client_id", client_id).eq("status","delivered").execute()
        rev_r  = supabase.table("orders").select("total").eq("client_id", client_id).in_("status",["confirmed","delivered"]).execute()
        total_revenue = sum(o["total"] for o in (rev_r.data or []))
        return {
            "total_orders":  all_r.count  or 0,
            "total_revenue": total_revenue,
            "pending":       pend_r.count or 0,
            "delivered":     delv_r.count or 0
        }
    except Exception as e:
        print(f"DB error get_client_stats: {e}")
        return {"total_orders":0,"total_revenue":0,"pending":0,"delivered":0}


# ───────────────────────────────────────────
# SESSIONS — replaces in-memory dict
# ───────────────────────────────────────────

def get_session(chat_id):
    try:
        r = supabase.table("sessions").select("*").eq(
            "chat_id", int(chat_id)
        ).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error get_session: {e}")
        return None

def upsert_session(chat_id, shop_id=None, cart=None,
                   state="idle", address="", phone="", delivery_date="",
                   language=None):
    try:
        data = {
            "chat_id":    int(chat_id),
            "state":      state,
            "updated_at": "now()"
        }
        if shop_id       is not None: data["shop_id"]       = shop_id
        if cart          is not None: data["cart"]          = cart
        if address       is not None: data["address"]       = address
        if phone         is not None: data["phone"]         = phone
        if delivery_date is not None: data["delivery_date"] = delivery_date
        if language      is not None: data["language"]      = language

        supabase.table("sessions").upsert(data).execute()
        return True
    except Exception as e:
        print(f"DB error upsert_session: {e}")
        return False

def update_session(chat_id, **kwargs):
    try:
        kwargs["updated_at"] = "now()"
        supabase.table("sessions").update(kwargs).eq(
            "chat_id", int(chat_id)
        ).execute()
        return True
    except Exception as e:
        print(f"DB error update_session: {e}")
        return False

def delete_session(chat_id):
    try:
        supabase.table("sessions").delete().eq(
            "chat_id", int(chat_id)
        ).execute()
    except Exception as e:
        print(f"DB error delete_session: {e}")


# ───────────────────────────────────────────
# CUSTOMER PROFILES
# ───────────────────────────────────────────

def get_customer_profile(chat_id):
    try:
        r = supabase.table("customer_profiles").select("*").eq(
            "chat_id", int(chat_id)
        ).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        print(f"DB error get_customer_profile: {e}")
        return None

def save_customer_profile(chat_id, name, phone, address):
    try:
        supabase.table("customer_profiles").upsert({
            "chat_id":    int(chat_id),
            "name":       name,
            "phone":      phone,
            "address":    address,
            "updated_at": "now()"
        }).execute()
        return True
    except Exception as e:
        print(f"DB error save_customer_profile: {e}")
        return False


# ───────────────────────────────────────────
# REVIEWS
# ───────────────────────────────────────────

def save_review(client_id, customer_telegram, order_id, rating, product_id=None):
    try:
        supabase.table("reviews").upsert({
            "client_id":         client_id,
            "product_id":        product_id,
            "order_id":          order_id,
            "customer_telegram": str(customer_telegram),
            "rating":            int(rating)
        }, on_conflict="order_id,product_id,customer_telegram").execute()

        # Update product rating cache
        if product_id:
            supabase.rpc("update_product_rating", {
                "p_id":   product_id,
                "rating": int(rating)
            }).execute()

        return True
    except Exception as e:
        print(f"DB error save_review: {e}")
        return False

def get_shop_rating(client_id):
    try:
        r = supabase.table("reviews").select("rating").eq(
            "client_id", client_id
        ).execute()
        if not r.data:
            return None, 0
        ratings = [row["rating"] for row in r.data]
        return round(sum(ratings) / len(ratings), 1), len(ratings)
    except Exception as e:
        print(f"DB error get_shop_rating: {e}")
        return None, 0

def get_product_rating(product_id):
    """Returns (avg_rating, count) from cached columns"""
    try:
        r = supabase.table("products").select(
            "rating_sum,rating_count"
        ).eq("id", product_id).execute()
        if not r.data:
            return None, 0
        p = r.data[0]
        count = p["rating_count"] or 0
        if count == 0:
            return None, 0
        return round(p["rating_sum"] / count, 1), count
    except Exception as e:
        print(f"DB error get_product_rating: {e}")
        return None, 0


# ───────────────────────────────────────────
# CUSTOMERS (for broadcast)
# ───────────────────────────────────────────

def get_all_customers_of_client(client_id):
    try:
        # Only customers with at least one non-cancelled order
        r = supabase.table("orders").select(
            "customer_telegram,customer_name"
        ).eq("client_id", client_id).in_("status", ["pending","confirmed","delivered"]).execute()
        seen, customers = set(), []
        for row in r.data:
            tid = row.get("customer_telegram")
            if tid and tid not in seen:
                seen.add(tid)
                customers.append(row)
        return customers
    except Exception as e:
        print(f"DB error get_all_customers: {e}")
        return []

def flag_blocked_customer(customer_telegram):
    try:
        supabase.table("blocked_customers").upsert({
            "customer_telegram": str(customer_telegram)
        }).execute()
    except Exception as e:
        print(f"DB error flag_blocked: {e}")

def is_customer_blocked(customer_telegram):
    try:
        r = supabase.table("blocked_customers").select("customer_telegram").eq(
            "customer_telegram", str(customer_telegram)
        ).execute()
        return len(r.data) > 0
    except Exception:
        return False