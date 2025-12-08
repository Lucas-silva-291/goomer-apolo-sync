import requests
import json
from datetime import datetime, timedelta
import time
import os
import logging

# ============================
# CONFIG LOGGING (REDUZ SPAM)
# ============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/var/log/goomer-sync.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ============================
# CONFIG
# ============================
BASE = os.environ["GOOMER_BASE_URL"]

# Listas de credenciais (ORDERS e TABLES) vindas de env
GOOMER_USERS_ORDERS = os.environ.get("GOOMER_USERS_ORDERS", "caixa:senha_orders").split(",")
GOOMER_USERS_TABLES = os.environ.get("GOOMER_USERS_TABLES", "Operador:senha_tables").split(",")

CRED_ORDERS = [{"user": u.split(":")[0], "pwd": u.split(":")[1]} for u in GOOMER_USERS_ORDERS]
CRED_TABLES = [{"user": u.split(":")[0], "pwd": u.split(":")[1]} for u in GOOMER_USERS_TABLES]

API_BASE = "https://api.apolocontrol.com"
API_KEY = os.environ["APOLO_API_KEY"]
GOOMER_BRANCH = os.environ["GOOMER_BRANCH"]

# URLs
orders_url = f"{BASE}/api/v2/orders"
tables_url = f"{BASE}/api/v2/tables"

# ============================
# FUNÇÕES DE DATA/HORA
# ============================
def utc_to_brasilia(dt_utc):
    return dt_utc - timedelta(hours=3)

def parse_iso_utc(ts):
    ts = ts.split("Z")[0]
    ts = ts.split("+")[0]
    ts = ts.split("-")[0] if ts.count("-") > 2 else ts
    if "." in ts:
        ts = ts.split(".", 1)[0]
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")

def to_brasilia_time(utc_iso_str):
    dt_utc = parse_iso_utc(utc_iso_str)
    dt_brt = utc_to_brasilia(dt_utc)
    return dt_brt.strftime("%Y-%m-%d %H:%M:%S")

def pending_to_brasilia(pending_list):
    if not pending_list:
        return None
    val = pending_list[0]
    if val is None:
        return None
    ts = val.split("_", 1)[0]
    dt_utc = parse_iso_utc(ts)
    dt_brt = utc_to_brasilia(dt_utc)
    return dt_brt.strftime("%Y-%m-%d %H:%M:%S")

# ============================
# SELEÇÃO DE CREDENCIAL POR ENDPOINT
# ============================
def select_credential_for(url, cred_list, desc):
    """
    Testa credenciais para um endpoint específico (orders ou tables).
    Retorna (user, pwd, headers) da primeira que funcionar (HTTP 200).
    """
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE,
        "Referer": f"{BASE}/goomer/login",
        "Accept": "application/json",
    }
    for cred in cred_list:
        user = cred["user"]
        pwd = cred["pwd"]
        try:
            logger.info(f"Testando credencial para {desc}: {user}")
            r = requests.get(
                url,
                params={"last_hours": 0.5},
                auth=(user, pwd),
                headers=headers,
                verify=False,
                timeout=15
            )
            r.raise_for_status()
            logger.info(f"✅ API autorizou {desc} com usuário: {user}")
            return user, pwd, headers
        except Exception as e:
            logger.warning(f"❌ API rejeitou {desc} com {user}: {e}")
    raise Exception(f"❌ Nenhuma credencial funcionou para {desc}!")

# ============================
# REQUISIÇÕES COM RETRY
# ============================
def requests_with_retry(method, url, user, pwd, headers=None, params=None, max_retries=3):
    for tentativa in range(max_retries):
        try:
            r = requests.request(
                method,
                url,
                auth=(user, pwd),
                headers=headers,
                params=params,
                verify=False,
                timeout=30
            )
            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning(f"Tentativa {tentativa+1}/{max_retries} falhou: {e}")
            if tentativa < max_retries - 1:
                sleep_time = 2 ** tentativa + 1
                logger.info(f"Aguardando {sleep_time}s antes de retry...")
                time.sleep(sleep_time)
    raise Exception(f"Falha após {max_retries} tentativas")

# ============================
# CÁLCULO DE JANELA DE BUSCA
# ============================
def calculate_last_hours():
    now_brt = datetime.now()
    hora = now_brt.hour

    if 1 <= hora < 6:
        return 0

    if hora < 1:
        inicio = now_brt.replace(hour=0, minute=0, second=0, microsecond=0)
        delta = now_brt - inicio
        return delta.total_seconds() / 3600.0

    if 6 <= hora < 11:
        inicio = now_brt.replace(hour=6, minute=0, second=0, microsecond=0)
        delta = now_brt - inicio
        return delta.total_seconds() / 3600.0

    return 1

# ============================
# BUSCAS NO GOOMER
# ============================
def get_orders(user, pwd, headers, last_hours):
    params = {"last_hours": last_hours}
    r = requests_with_retry("GET", orders_url, user, pwd, headers, params)
    data = r.json()
    return data["response"]["orders"]

def get_cash_tabs(user, pwd, headers, last_hours):
    params = {"last_hours": last_hours}
    r = requests_with_retry("GET", tables_url, user, pwd, headers, params)
    data = r.json()
    tables = data["response"].get("tables", [])
    return {t["code"] for t in tables}

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

# ============================
# ENVIO PARA API APOLO COM RETRY
# ============================
def send_to_api(pedidos):
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "cod_branch": GOOMER_BRANCH,
        "pedidos": pedidos
    }

    url = f"{API_BASE}/api/goomer/pedidos"

    for tentativa in range(3):
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=30
            )

            logger.info(f"STATUS: {response.status_code}")
            logger.debug(f"BODY: {response.text}")

            if response.status_code == 201:
                result = response.json()
                saved = result.get("saved_new", 0)
                updated = result.get("updated_existing", 0)
                logger.info(f"Envio OK! saved={saved}, updated={updated}")
                return True
            else:
                logger.error(f"Erro HTTP {response.status_code}: {response.text}")
                return False

        except Exception as e:
            logger.error(f"Tentativa {tentativa+1}/3 falhou: {e}")
            if tentativa < 2:
                time.sleep(2 ** tentativa + 1)

    logger.error("Falha após 3 tentativas na API Apolo")
    return False

# ============================
# LOOP PRINCIPAL
# ============================
FAST_INTERVAL = 10
REFRESH_INTERVAL = 30 * 60

if __name__ == "__main__":
    logger.info("=== Iniciando Goomer-Apolo Sync ===")

    # Seleciona credenciais separadas para ORDERS e TABLES
    user_orders, pwd_orders, headers_orders = select_credential_for(orders_url, CRED_ORDERS, "ORDERS")
    user_tables, pwd_tables, headers_tables = select_credential_for(tables_url, CRED_TABLES, "TABLES")

    last_refresh = 0
    last_fast_payload = None
    last_refresh_payload = None
    ciclo_count = 0
    erro_count = 0
    MAX_ERROS_CONSECUTIVOS = 10
    sleep_extra = 0

    while True:
        ciclo_count += 1
        now = time.time()
        sleep_extra = 0

        try:
            last_hours = calculate_last_hours()
            if last_hours > 0:
                logger.debug(f"[FAST Ciclo {ciclo_count}] Buscando últimos {last_hours:.2f}h")
                orders = get_orders(user_orders, pwd_orders, headers_orders, last_hours)
                cash_codes = get_cash_tabs(user_tables, pwd_tables, headers_tables, last_hours)
                simplified_orders = simplify_orders(orders, cash_codes)

                if simplified_orders and simplified_orders != last_fast_payload:
                    logger.info(f"NOVOS pedidos detectados ({len(simplified_orders)} itens)")
                    if send_to_api(simplified_orders):
                        last_fast_payload = simplified_orders
                        logger.info("Payload FAST atualizado com sucesso")

            if now - last_refresh >= REFRESH_INTERVAL:
                logger.info("[REFRESH] Atualizando status dos últimos 12h...")
                orders_big = get_orders(user_orders, pwd_orders, headers_orders, last_hours=12)
                cash_codes_big = get_cash_tabs(user_tables, pwd_tables, headers_tables, last_hours=12)
                simplified_big = simplify_orders(orders_big, cash_codes_big)

                if simplified_big and simplified_big != last_refresh_payload:
                    if send_to_api(simplified_big):
                        last_refresh_payload = simplified_big
                        logger.info("Payload REFRESH atualizado")

                last_refresh = now

            erro_count = 0

        except Exception as e:
            erro_count += 1
            logger.error(f"Erro no ciclo {ciclo_count}: {e}")

            if erro_count >= MAX_ERROS_CONSECUTIVOS:
                sleep_extra = 300
                logger.warning(f"{MAX_ERROS_CONSECUTIVOS} erros consecutivos. Aguardando {sleep_extra}s")

        sleep_time = FAST_INTERVAL + sleep_extra
        logger.debug(f"Aguardando {sleep_time}s até próximo ciclo")
        time.sleep(sleep_time)
