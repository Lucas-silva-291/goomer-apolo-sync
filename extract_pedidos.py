import json
import logging
import os
from typing import Any, Dict, List, Optional, Sequence

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_PRODUTOS_URL = os.getenv(
    "GOOMER_PRODUTOS_URL", "https://api.apolocontrol.com/api/goomer/pedidos-produtos"
)
DEFAULT_PRODUTOS_KEY = os.getenv("GOOMER_PRODUTOS_KEY", "goomer_prod_2025_xyz789")
COD_BRANCH = os.getenv("GOOMER_BRANCH", "0257")
DEFAULT_BATCH_SIZE = int(os.getenv("GOOMER_PRODUTOS_BATCH", "200") or "200")


def coletar_observacoes_do_produto(produto: Dict[str, Any]) -> List[str]:
    """Retorna lista das observações relevantes de um produto."""
    observacoes: List[str] = []

    for campo in ("observation", "observacao", "obs"):
        valor = produto.get(campo)
        if isinstance(valor, str) and valor.strip():
            observacoes.append(valor.strip())
        elif isinstance(valor, list):
            for item in valor:
                if isinstance(item, str) and item.strip():
                    observacoes.append(item.strip())

    opcoes = produto.get("options") or []
    if isinstance(opcoes, dict):
        opcoes_iteraveis = opcoes.values()
    elif isinstance(opcoes, list):
        opcoes_iteraveis = opcoes
    else:
        opcoes_iteraveis = []

    for opcao in opcoes_iteraveis:
        if not isinstance(opcao, dict):
            continue
        nome_opcao = opcao.get("name") or opcao.get("nome")
        if isinstance(nome_opcao, str) and nome_opcao.strip():
            observacoes.append(nome_opcao.strip())

    return observacoes


def extrair_pedidos_simplificados(pedidos_full: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pedidos_simplificados: List[Dict[str, Any]] = []

    for pedido in pedidos_full:
        tab_id = pedido.get("tab_id", "")
        numero_pedido = tab_id.split("_")[1] if "_" in tab_id else tab_id or "N/A"

        produtos_extraidos: List[Dict[str, Any]] = []
        for product in pedido.get("products", []):
            observacoes = coletar_observacoes_do_produto(product)
            produtos_extraidos.append(
                {
                    "produto": (product.get("name") or "").strip(),
                    "quantidade": product.get("quantity", 1),
                    "observacoes": observacoes,
                }
            )

        pedidos_simplificados.append(
            {
                "numero_pedido": numero_pedido,
                "produtos": produtos_extraidos,
            }
        )

    return pedidos_simplificados


def salvar_pedidos_json(pedidos: List[Dict[str, Any]], caminho: str = "pedidos.json") -> None:
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(pedidos, f, indent=2, ensure_ascii=False)
    logger.info("Arquivo %s salvo com %d pedidos", caminho, len(pedidos))


def enviar_para_api_produtos(
    pedidos: List[Dict[str, Any]],
    cod_branch: Optional[str] = None,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not pedidos:
        logger.info("Nenhum produto novo para enviar ao endpoint de produtos.")
        return None

    cod_branch = cod_branch or COD_BRANCH
    if not cod_branch:
        raise ValueError("cod_branch não definido. Configure GOOMER_BRANCH ou informe via parâmetro.")

    api_url = api_url or DEFAULT_PRODUTOS_URL
    if not api_url:
        raise ValueError("GOOMER_PRODUTOS_URL não configurada; informe via parâmetro ou variável de ambiente.")

    api_key = api_key or DEFAULT_PRODUTOS_KEY

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key,
    }
    payload = {
        "cod_branch": cod_branch,
        "pedidos": pedidos,
    }

    attempt_urls = [api_url]
    if api_url and not api_url.endswith("/"):
        attempt_urls.append(api_url + "/")

    last_exc: Optional[Exception] = None
    for idx, url in enumerate(attempt_urls):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            logger.info("Produtos enviados para %s. status=%s", url, response.status_code)
            return response.json()
        except requests.HTTPError as http_err:
            status_code = http_err.response.status_code if http_err.response else None
            if status_code == 404 and idx + 1 < len(attempt_urls):
                logger.warning(
                    "Endpoint %s retornou 404; tentando novamente com barra final...",
                    url,
                )
                last_exc = http_err
                continue
            last_exc = http_err
            break
        except Exception as exc:
            last_exc = exc
            break

    if last_exc:
        raise last_exc

    raise RuntimeError("Falha desconhecida ao enviar produtos para API de produtos.")


def _dividir_em_lotes(pedidos: List[Dict[str, Any]], tamanho_lote: int) -> Sequence[List[Dict[str, Any]]]:
    if tamanho_lote <= 0:
        return [pedidos]
    return [pedidos[i : i + tamanho_lote] for i in range(0, len(pedidos), tamanho_lote)]


def processar_pedidos_produtos(
    pedidos_full: Sequence[Dict[str, Any]],
    caminho_saida: str = "pedidos.json",
    enviar: bool = True,
    salvar_local: bool = False,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
    tamanho_lote: Optional[int] = None,
) -> List[Dict[str, Any]]:
    pedidos_simplificados = extrair_pedidos_simplificados(pedidos_full)
    if salvar_local:
        salvar_pedidos_json(pedidos_simplificados, caminho_saida)
    if enviar:
        try:
            lotes = _dividir_em_lotes(
                pedidos_simplificados,
                tamanho_lote or DEFAULT_BATCH_SIZE,
            )
            total_lotes = len(lotes)
            for indice, lote in enumerate(lotes, start=1):
                logger.info(
                    "Enviando lote %d/%d (%d pedidos)",
                    indice,
                    total_lotes,
                    len(lote),
                )
                resposta = enviar_para_api_produtos(
                    lote,
                    api_url=api_url,
                    api_key=api_key,
                )
                if resposta is not None:
                    logger.info("Resposta API Produtos (lote %d/%d): %s", indice, total_lotes, resposta)
        except Exception as exc:
            logger.error("Falha ao enviar produtos para API: %s", exc)
            raise
    return pedidos_simplificados


def carregar_pedidos_full(caminho: str = "pedidos_full.json") -> List[Dict[str, Any]]:
    with open(caminho, "r", encoding="utf-8") as f:
        return json.load(f)


def run_from_file(
    caminho_origem: str = "pedidos_full.json",
    caminho_saida: str = "pedidos.json",
    enviar: bool = True,
    salvar_local: bool = False,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
    tamanho_lote: Optional[int] = None,
) -> List[Dict[str, Any]]:
    pedidos_full = carregar_pedidos_full(caminho_origem)
    return processar_pedidos_produtos(
        pedidos_full,
        caminho_saida,
        enviar,
        salvar_local,
        api_url=api_url,
        api_key=api_key,
        tamanho_lote=tamanho_lote,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        resultado = run_from_file()
        logger.info("✅ Script concluído! %d pedidos processados", len(resultado))
    except Exception as err:
        logger.error("Erro ao processar pedidos: %s", err, exc_info=True)
        raise
