import datetime as dt
import json
import math
import os
import time
from typing import Dict, List, Tuple

import requests
from google.cloud import storage
from google.cloud import secretmanager
import google.cloud.logging

# =========================
# Configuración y clientes
# =========================

# URL base de la API 
BASE_URL = os.environ.get(
    "BASE_URL",
    "https://jtalent-practica-final-957248768795.europe-west3.run.app",
)

# Bucket RAW 
RAW_BUCKET = os.environ.get("RAW_BUCKET")
PROJECT = os.environ.get("PROJECT")

# Secreto con la API Key 
SECRET_NAME = "projects/157229630236/secrets/x-api-key/versions/1"


# Timeouts y reintentos
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "120"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
RETRY_BASE_SECONDS = float(os.environ.get("RETRY_BASE_SECONDS", "1.5"))

# Inicializa logging nativo de GCP 
_logging_client = google.cloud.logging.Client()
_logging_client.setup_logging()

# Clientes GCP
_storage = storage.Client()
_secrets = secretmanager.SecretManagerServiceClient()


# =========================
# Utilidades
# =========================

def log(severity: str, message: str, **kwargs):
    """
    Logging estructurado (jsonPayload) compatible con Cloud Logging.
    Uso: log("INFO","page_ingested", endpoint="customers", page=1, rows=100)
    """
    payload = {"severity": severity.upper(), "message": message}
    if kwargs:
        payload.update(kwargs)
    # print() con JSON para que Cloud Logging lo procese como jsonPayload
    print(json.dumps(payload, ensure_ascii=False))


def _read_secret() -> str:
    """
    Lee la última versión del secreto con la API key.
    """
    response = _secrets.access_secret_version(name=SECRET_NAME)
    return response.payload.data.decode("utf-8")


def _is_nan_like(v) -> bool:
    """
    True si v es NaN (float) o string "NaN"/"nan".
    """
    try:
        return (isinstance(v, float) and math.isnan(v)) or (
            isinstance(v, str) and v.strip().lower() == "nan"
        )
    except Exception:
        return False


def _clean_nan(obj):
    """
    Limpia NaN en estructuras arbitrarias (dict/list/escalares) → None.
    """
    if isinstance(obj, dict):
        return {k: _clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_nan(x) for x in obj]
    return None if _is_nan_like(obj) else obj


def _gcs_write_jsonlines(bucket: str, path: str, records: List[dict]) -> None:
    """
    Guarda una lista de objetos como NDJSON en GCS (una línea por objeto).
    Fuerza JSON estricto (allow_nan=False).
    """
    blob = _storage.bucket(bucket).blob(path)
    payload = "\n".join(
        json.dumps(r, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        for r in records
    )
    blob.upload_from_string(payload, content_type="application/json")


def _http_post_with_retry(url: str, headers: Dict[str, str]) -> Tuple[int, dict]:
    """
    POST con reintentos exponenciales ante fallos temporales.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, headers=headers, timeout=REQUEST_TIMEOUT)
            # Si el backend devuelve error 5xx, reintenta
            if resp.status_code >= 500:
                raise RuntimeError(
                    f"Server error {resp.status_code}: {resp.text[:300]}"
                )
            # Si no es JSON válido, esto lanzará
            body = resp.json()
            return resp.status_code, body

        except Exception as e:
            sleep_s = RETRY_BASE_SECONDS * (2 ** attempt)
            log(
                "WARNING",
                "ingest_request_retry",
                attempt=attempt + 1,
                sleep_s=sleep_s,
                url=url,
                error=str(e),
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"Max retries exceeded for {url}")


def ingest_endpoint(endpoint: str, run_date: str) -> Dict[str, int]:
    """
    Descarga paginado desde BASE_URL/<endpoint>?page=N y escribe:
    raw/<endpoint>/ingestion_date=YYYY-MM-DD/page=NNNNN.json
    Devuelve métricas agregadas de páginas/filas.
    """
    api_key = _read_secret()
    headers = {"x-api-key": api_key}
    page = 1
    total_rows = 0
    total_pages = 0
    endpoint_clean = endpoint.strip("/")

    while True:
        url = f"{BASE_URL}/{endpoint_clean}?page={page}"
        status, body = _http_post_with_retry(url, headers=headers)
        if status != 200:
            raise RuntimeError(f"HTTP {status} on {url}")

        # Estructura esperada: { status, message, data: { page, last, data: [] } }
        data_section = body.get("data") or {}
        current = int(data_section.get("page", page))
        last = int(data_section.get("last", page))
        records = data_section.get("data", [])
        if records is None:
            records = []

        if not isinstance(records, list):
            raise ValueError("Respuesta inesperada: 'data.data' no es un array.")

        # Saneo de NaN → null para JSON estricto
        records = _clean_nan(records)

        # Escritura NDJSON por página
        path = f"raw/{endpoint_clean}/ingestion_date={run_date}/page={current:05d}.json"
        _gcs_write_jsonlines(RAW_BUCKET, path, records)

        rows = len(records)
        total_rows += rows
        total_pages += 1

        log(
            "INFO",
            "page_ingested",
            endpoint=endpoint_clean,
            page=current,
            last=last,
            rows=rows,
            gcs_path=path,
        )

        if current >= last:
            break
        page += 1

    return {"pages": total_pages, "rows": total_rows}


def _parse_request_payload(request_json: dict) -> Tuple[List[str], dt.date, dt.date]:
    """
    Parsea 'start_date' y 'end_date'.
    - Si no se proporcionan fechas, se usa la fecha de hoy para ambas.
    - Si solo se proporciona start_date, end_date es igual a start_date.
    """
    today = dt.date.today()
    
    # Parsea las fechas, con valores por defecto robustos
    start_date_str = request_json.get("start_date")
    end_date_str = request_json.get("end_date")

    start_date = dt.datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else today
    end_date = dt.datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else start_date

    # Parsea los endpoints, con la lista completa por defecto
    endpoints = request_json.get("endpoints")
    if not endpoints:
        endpoints = [
            "customers", "customer_types", "addresses", "branches", "accounts",
            "account_types", "account_statuses", "loans", "loan_statuses",
            "transactions", "transaction_types",
        ]
        
    return endpoints, start_date, end_date


# =========================
# Entrypoints HTTP
# =========================

def ingest_http(request):
    """
    EntryPoint para Cloud Functions (2ª gen, HTTP).
    Body JSON opcional:
    {
      "endpoints": ["customers","accounts",...],
      "run_date": "YYYY-MM-DD"
    }
    """
    try:
        request_json = request.get_json(silent=True) or {}
        endpoints, start_date, end_date = _parse_request_payload(request_json)

        # Diccionario para almacenar los resultados por cada día procesado
        daily_results = {}
        
        # Bucle principal que itera desde start_date hasta end_date
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            log("INFO", "processing_date", date=date_str)
            
            results_for_day = {}
            for ep in endpoints:
                metrics = ingest_endpoint(ep, date_str)
                results_for_day[ep] = metrics
            
            daily_results[date_str] = results_for_day
            
            # Pasa al siguiente día
            current_date += dt.timedelta(days=1)

        log("INFO", "ingestion_finished", start_date=start_date.isoformat(), end_date=end_date.isoformat(), results=daily_results)
        resp_body = {"ok": True, "start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "results": daily_results}
        return (json.dumps(resp_body), 200, {"Content-Type": "application/json"})

    except Exception as e:
        log("ERROR", "ingestion_failed", error=str(e))
        resp_body = {"ok": False, "error": str(e)}
        return (json.dumps(resp_body), 500, {"Content-Type": "application/json"})



def run_ingest(request):
    """
    EntryPoint para Cloud Run (Gunicorn/WSGI).
    Reutiliza la misma lógica que ingest_http.
    """
    return ingest_http(request)
