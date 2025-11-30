import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+
import time
import os


BASE = os.environ["GOOMER_BASE_URL"] 
USER = "caixa"
PWD  = "1234"

API_BASE = "https://api.apolocontrol.com"
API_KEY = os.environ["APOLO_API_KEY"]  # nome que você quiser
GOOMER_BRANCH = os.environ["GOOMER_BRANCH"]

login_url  = f"{BASE}/api/v2/login"
orders_url = f"{BASE}/api/v2/orders"
tables_url = f"{BASE}/api/v2/tables"


def to_brasilia_time(utc_iso_str):
    dt_utc = datetime.fromisoformat(utc_iso_str.replace("Z", "+00:00"))
    dt_brt = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    return dt_brt.strftime("%Y-%m-%d %H:%M:%S")


def pending_to_brasilia(pending_list):
    if not pending_list:
        return None
    val = pending_list[0]
    if val is None:
        return None
    ts = val.split("_", 1)[0]
    dt_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    dt_brt = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    return dt_brt.strftime("%Y-%m-%d %H:%M:%S")


def login_session():
    s = requests.Session()
    s.auth = (USER, PWD)
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE,
        "Referer": f"{BASE}/goomer/login",
    }
    r = s.post(login_url, headers=headers, verify=False)
    r.raise_for_status()
    return s, headers


def calculate_last_hours():
    now_brt = datetime.now(ZoneInfo("America/Sao_Paulo"))
    hora = now_brt.hour

    if 1 <= hora < 6:
        return 0

    if hora < 1:
        inicio = now_brt.replace(hour=0, minute=0, second=0, microsecond=0)
        delta = now_brt - inicio
        return delta.total_seconds() / 3600

    if 6 <= hora < 11:
        inicio = now_brt.replace(hour=6, minute=0, second=0, microsecond=0)
        delta = now_brt - inicio
        return delta.total_seconds() / 3600

    return 1


def get_orders(s, headers, last_hours):
    params = {"last_hours": last_hours}
    r = s.get(orders_url, headers=headers, params=params, verify=False)
    r.raise_for_status()
    data = r.json()
    return data["response"]["orders"]


def get_cash_tabs(s, headers, last_hours):
    params = {"last_hours": last_hours}
    r = s.get(tables_url, headers=headers, params=params, verify=False)
    r.raise_for_status()
    data = r.json()
    tables = data["response"].get("tables", [])
    cash_codes = {t["code"] for t in tables}
    return cash_codes


def simplify_orders(orders, cash_codes):
    simplified = []
    for o in orders:
        if not o["products"]:
            continue

        tab = o["products"][0]["tab"]

        created_utc = o["created_at"]
        created_brt = to_brasilia_time(created_utc)

        tab_code = tab.get("code")
        is_cash = tab_code in cash_codes

        pending_brt = pending_to_brasilia(tab.get("pendingPayments"))

        tab_status = tab.get("status", o.get("status"))

        item = {
            "goomer_id": o["tab_id"],
            "created_at": created_brt,
            "status": tab_status,
            "code": tab_code,
            "taa_system": tab.get("taa_system"),
            "pendingPayments": pending_brt,
            "already_paid": o.get("already_paid", 0),
            "prod_total_cost": o.get("prod_total_cost", 0),
            "is_cash": is_cash,
        }
        simplified.append(item)
    return simplified


def send_to_api(pedidos):
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "cod_branch": GOOMER_BRANCH,
        "pedidos": pedidos
    }

    try:
        response = requests.post(
            f"{API_BASE}/api/goomer/pedidos",
            json=payload,
            headers=headers,
            timeout=30
        )

        print("STATUS:", response.status_code)
        print("BODY  :", response.text)

        if response.status_code == 201:
            result = response.json()
            saved = result.get("saved_new", 0)
            updated = result.get("updated_existing", 0)
            print(f"✅ Envio OK! saved={saved}, updated={updated}")
            return True
        else:
            print(f"❌ Erro: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"❌ Falha conexão: {e}")
        return False


FAST_INTERVAL = 10
REFRESH_INTERVAL = 30 * 60


if __name__ == "__main__":
    session, headers = login_session()
    last_refresh = 0
    last_fast_payload = None
    last_refresh_payload = None

    while True:
        now = time.time()

        try:
            last_hours = calculate_last_hours()
            if last_hours > 0:
                print(f"[FAST] Buscando pedidos dos últimos {last_hours:.2f} horas.")
                orders = get_orders(session, headers, last_hours)
                cash_codes = get_cash_tabs(session, headers, last_hours)
                simplified_orders = simplify_orders(orders, cash_codes)

                if simplified_orders and simplified_orders != last_fast_payload:
                    if send_to_api(simplified_orders):
                        last_fast_payload = simplified_orders

            if now - last_refresh >= REFRESH_INTERVAL:
                print("[REFRESH] Atualizando status dos últimos 12h...")
                orders_big = get_orders(session, headers, last_hours=12)
                cash_codes_big = get_cash_tabs(session, headers, last_hours=12)
                simplified_big = simplify_orders(orders_big, cash_codes_big)

                if simplified_big and simplified_big != last_refresh_payload:
                    if send_to_api(simplified_big):
                        last_refresh_payload = simplified_big

                last_refresh = now

        except Exception as e:
            print("Erro no ciclo:", e)

        time.sleep(FAST_INTERVAL)
