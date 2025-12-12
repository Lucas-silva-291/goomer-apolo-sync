import requests
import json
from datetime import datetime, timedelta
import time
import os
import logging
import socket
from urllib3.exceptions import InsecureRequestWarning

# desabilita warning de verify=False
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# ============================
# LOGGING
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
# IP LOCAL + BASE LOCAL
# ============================
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

LOCAL_IP = get_local_ip()
LOCAL_BASE = "http://" + LOCAL_IP + ":8081"

# BASE do Goomer: usa env se tiver, senão cai na LOCAL_BASE
BASE = os.environ.get("GOOMER_BASE_URL", LOCAL_BASE)

LOGIN_URL = BASE + "/api/v2/login"
ORDERS_URL = BASE + "/api/v2/orders"
TABLES_URL = BASE + "/api/v2/tables"

# ============================
# CONFIG APOLO
# ============================
API_BASE = "https://api.apolocontrol.com"
API_KEY = os.environ["APOLO_API_KEY"]
GOOMER_BRANCH = os.environ["GOOMER_BRANCH"]

# ============================
# SESSÃO AUTENTICADA NO GOOMER
# ============================
SESSION = requests.Session()

def goomer_login():
    """
    Faz um POST em /api/v2/login com Basic Auth (user/senha fixos ou de env)
    e guarda cookies na SESSION global.
    """
    username = os.environ.get("GOOMER_USER", "caixa")
    password = os.environ.get("GOOMER_PASS", "1234")

    logger.info("Fazendo login no Goomer em " + LOGIN_URL + " com usuário " + username)

    headers = {
        "Accept": "*/*",
        "Origin": BASE,
        "Referer": BASE + "/goomer/login",
        "X-Requested-With": "XMLHttpRequest",
    }

    # Requests monta o header Authorization: Basic ... para nós
    resp = SESSION.post(
        LOGIN_URL,
        headers=headers,
        auth=(username, password),
        verify=False,
        timeout=10,
    )
    resp.raise_for_status()
    logger.info("Login Goomer OK, status=" + str(resp.status_code))

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
# REQUISIÇÕES COM RETRY (USANDO SESSION)
# ============================
def session_request_with_retry(method, url, headers=None, params=None, max_retries=3):
    for tentativa in range(max_retries):
        try:
            r = SESSION.request(
                method,
                url,
                headers=headers,
                params=params,
                verify=False,
                timeout=30
            )
            if r.status_code == 401 and tentativa == 0:
                # tenta relogar uma vez se perdeu a sessão
                logger.warning("401 recebido; refazendo login no Goomer")
                goomer_login()
                continue

            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning(
                "Tentativa " + str(tentativa + 1) + "/" + str(max_retries) +
                " falhou em " + url + ": " + str(e)
            )
            if tentativa < max_retries - 1:
                sleep_time = 2 ** tentativa + 1
                logger.info("Aguardando " + str(sleep_time) + "s antes de retry...")
                time.sleep(sleep_time)
    raise Exception("Falha após " + str(max_retries) + " tentativas em " + url)

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
# BUSCAS NO GOOMER (ORDERS / TABLES)
# ============================
def get_orders(last_hours):
    params = {"last_hours": last_hours}
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json",
        "Origin": BASE,
        "Referer": BASE + "/goomer/login",
    }
    r = session_request_with_retry("GET", ORDERS_URL, headers=headers, params=params)
    data = r.json()
    return data["response"]["orders"]

def get_cash_tabs(last_hours):
    params = {"last_hours": last_hours}
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json",
        "Origin": BASE,
        "Referer": BASE + "/goomer/login",
    }
    r = session_request_with_retry("GET", TABLES_URL, headers=headers, params=params)
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
# ENVIO PARA API APOLO
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

    url = API_BASE + "/api/goomer/pedidos"

    for tentativa in range(3):
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=30
            )

            logger.info("STATUS: " + str(response.status_code))
            logger.debug("BODY: " + response.text)

            if response.status_code == 201:
                result = response.json()
                saved = result.get("saved_new", 0)
                updated = result.get("updated_existing", 0)
                logger.info("Envio OK! saved=" + str(saved) + ", updated=" + str(updated))
                return True
            else:
                logger.error("Erro HTTP " + str(response.status_code) + ": " + response.text)
                return False

        except Exception as e:
            logger.error("Tentativa " + str(tentativa+1) + "/3 falhou: " + str(e))
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
    logger.info("=== Iniciando Goomer-Apolo Sync (sessão única) ===")
    logger.info("IP local detectado: " + LOCAL_IP)
    logger.info("BASE em uso: " + BASE)

    # login inicial
    goomer_login()

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
                logger.debug("[FAST Ciclo " + str(ciclo_count) + "] Buscando últimos " + str(last_hours) + "h")
                orders = get_orders(last_hours)
                cash_codes = get_cash_tabs(last_hours)
                simplified_orders = simplify_orders(orders, cash_codes)

                if simplified_orders and simplified_orders != last_fast_payload:
                    logger.info("NOVOS pedidos detectados (" + str(len(simplified_orders)) + " itens)")
                    if send_to_api(simplified_orders):
                        last_fast_payload = simplified_orders
                        logger.info("Payload FAST atualizado com sucesso")

            if now - last_refresh >= REFRESH_INTERVAL:
                logger.info("[REFRESH] Atualizando status dos últimos 12h...")
                orders_big = get_orders(12)
                cash_codes_big = get_cash_tabs(12)
                simplified_big = simplify_orders(orders_big, cash_codes_big)

                if simplified_big and simplified_big != last_refresh_payload:
                    if send_to_api(simplified_big):
                        last_refresh_payload = simplified_big
                        logger.info("Payload REFRESH atualizado")

                last_refresh = now

            erro_count = 0

        except Exception as e:
            erro_count += 1
            logger.error("Erro no ciclo " + str(ciclo_count) + ": " + str(e))

            if erro_count >= MAX_ERROS_CONSECUTIVOS:
                sleep_extra = 300
                logger.warning(str(MAX_ERROS_CONSECUTIVOS) + " erros consecutivos. Aguardando " + str(sleep_extra) + "s")

        sleep_time = FAST_INTERVAL + sleep_extra
        logger.debug("Aguardando " + str(sleep_time) + "s até próximo ciclo")
        time.sleep(sleep_time)
