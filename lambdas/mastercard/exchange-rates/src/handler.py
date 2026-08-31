"""
handler.py — Lambda real: itl-0004-itx-{env}-intchg-02-lmbd-mc-exchange-rates
================================================================================
Archivo:     lambdas/mastercard/exchange-rates/src/handler.py

Scrapea tipos de cambio históricos de Mastercard consultando la API pública
del conversor de moneda (mccom-services/currency-conversions/conversion-rates),
que no tiene equivalente oficial documentado — a diferencia de Visa, Mastercard
no expone una fuente oficial de tipos de cambio históricos. La API aplica
rate-limiting/bloqueo agresivo por IP, por lo que el scraping rota proxies
(ProxyManager) y espacia requests (PAUSE_MIN/PAUSE_MAX). Escribe a
`exchange-rates/brand=Mastercard/exchange_date=YYYY-MM-DD/` en s3-reference
— fuente cruda (solo códigos alfabéticos), enriquecida después con códigos
numéricos por el job glue-exchange-rates a `exchange-rates-glue/` (fuente
oficial del pipeline, ver decisions.md → "Por qué se migró la fuente de
tipo de cambio").

Arquitectura orquestador/worker/consolidador encadenada (evita el timeout
de 900s de un único Lambda al scrapear miles de pares de moneda por día):
  - orchestrator: valida fechas, arma la lista completa de pares de moneda,
    la divide en NUM_CHUNKS y dispara el primer worker (async) por cada
    fecha del rango.
  - worker: procesa un chunk de pares con un pool de hilos
    (ThreadPoolExecutor, MAX_WORKERS), usando ProxyManager para rotar
    proxies y banear los que fallan; guarda su resultado como un Parquet
    temporal en S3 y se auto-invoca (async) para el siguiente chunk —
    hasta agotar los chunks, momento en que dispara el consolidador.
  - consolidator: une todos los Parquets temporales de una fecha en un
    único Parquet final y borra los temporales.

Flujo:
1. orchestrator: generar rango de fechas, cargar pares de moneda, borrar
   Parquets existentes de esas fechas, dividir en chunks, invocar worker 0
2. worker: cargar y validar proxies, dividir su chunk en sub-chunks (uno
   por hilo), scrapear cada par con reintentos/backoff, guardar chunk
   temporal en S3, invocar el siguiente worker (o el consolidador si era
   el último chunk)
3. consolidator: leer todos los chunks temporales de la fecha, concatenar,
   escribir el Parquet final, borrar los temporales

Variables de entorno:
  S3_BUCKET      : bucket de referencia (default: itl-0004-itx-dev-intchg-02-s3-reference)
  S3_PREFIX      : prefix de salida (default: exchange-rates/brand=Mastercard)
  FUNCTION_NAME  : nombre de esta Lambda, usado para auto-invocarse en la cadena worker→worker→consolidator
  BEGIN_DATE     : fecha de inicio del scraping si no viene en el evento (default: hoy UTC)
  END_DATE       : fecha de fin del scraping si no viene en el evento (default: hoy UTC)

Archivos de recursos (deployment package, no en DynamoDB):
  resources/currencies.json      : catálogo de monedas (commiteado)
  resources/proxy_settings.json  : credenciales de proxy (NO commiteado, en .gitignore)
"""
import io
import os
import json
import time
import random
import logging
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from curl_cffi import requests

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =============================================================================
# CONFIGURATION
# =============================================================================

S3_BUCKET = os.environ.get("S3_BUCKET", "itl-0004-itx-dev-intchg-02-s3-reference")
S3_PREFIX = os.environ.get("S3_PREFIX", "exchange-rates/brand=Mastercard")
FUNCTION_NAME = os.environ.get(
    "FUNCTION_NAME", "itl-0004-itx-dev-intchg-02-lmbd-mc-exchange-rates"
)

CURRENT_UTC_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
BEGIN_DATE = os.environ.get("BEGIN_DATE", CURRENT_UTC_DATE)
END_DATE = os.environ.get("END_DATE", CURRENT_UTC_DATE)

NUM_CHUNKS = 10
MAX_WORKERS = 9
REQUEST_TIMEOUT = 15
PAUSE_MIN = 1.0
PAUSE_MAX = 1.3
PROXY_BAN_AFTER = 1  # consecutive failures before banning a proxy

REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": "https://www.mastercard.us/en-us/personal/get-support/convert-currency.html",
    "Origin": "https://www.mastercard.us",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

MASTERCARD_RATES_URL = (
    "https://www.mastercard.com/marketingservices/public/mccom-services/"
    "currency-conversions/conversion-rates"
)

DATE_FORMAT_INPUT = "%Y-%m-%d"
DATE_FORMAT_OUTPUT = "%m/%d/%Y"
DATE_FORMAT_FILE = "%Y%m%d"

# =============================================================================
# PROXY MANAGER
# Centralizes proxy state so failures from ANY thread count toward the ban limit.
# =============================================================================


class ProxyManager:
    """
    Pool de proxies thread-safe para el scraping de Mastercard. Centraliza el
    estado de fallas para que todos los hilos (ThreadPoolExecutor) cuenten
    hacia el mismo límite de baneo — un proxy que falla en distintos hilos
    igual acumula fallas hacia PROXY_BAN_AFTER, en vez de resetear el contador
    por hilo.
    """

    def __init__(self, proxies: list[dict]):
        """
        Inicializa el pool a partir de la lista de proxies ya cargados,
        descartando cualquiera que no venga con status="active".

        Args:
            proxies: Lista de dicts {"proxy": url, "status": "active"|...},
                típicamente el resultado de validate_proxies().

        Returns:
            None.
        """
        self._lock = threading.Lock()
        self._pool = [
            {"proxy": p["proxy"], "status": "active", "fails": 0}
            for p in proxies
            if p.get("status") == "active"
        ]

    def get_active(self) -> list[dict]:
        """
        Devuelve una copia de los proxies actualmente activos.

        Returns:
            Lista de dicts de proxy con status == "active".

        Ejemplo:
            proxy_manager.get_active()  # [{'proxy': 'http://...', 'status': 'active', 'fails': 0}, ...]
        """
        with self._lock:
            return [p for p in self._pool if p["status"] == "active"]

    def pick(self, index: int) -> dict | None:
        """
        Selecciona un proxy activo por round-robin, indexado por `index`
        (típicamente el índice del par de moneda dentro del sub-chunk del hilo).

        Args:
            index: Índice a partir del cual elegir (se aplica módulo la cantidad
                de proxies activos).

        Returns:
            Dict del proxy elegido, o None si no hay ningún proxy activo.

        Ejemplo:
            proxy_manager.pick(3)  # {'proxy': 'http://...', 'status': 'active', 'fails': 0}
        """
        with self._lock:
            active = [p for p in self._pool if p["status"] == "active"]
            if not active:
                return None
            return active[index % len(active)]

    def report_failure(self, proxy: dict) -> None:
        """
        Incrementa el contador de fallas de un proxy y lo banea globalmente
        (status→"inactive") si alcanza PROXY_BAN_AFTER fallas consecutivas.

        Args:
            proxy: Dict del proxy que falló (mismo objeto retornado por pick()).

        Returns:
            None.

        Ejemplo:
            proxy_manager.report_failure(proxy)
        """
        with self._lock:
            proxy["fails"] += 1
            if proxy["fails"] >= PROXY_BAN_AFTER and proxy["status"] == "active":
                proxy["status"] = "inactive"
                safe_url = self._mask_proxy_url(proxy["proxy"])
                logger.warning(
                    f"[ProxyManager] Proxy banned after {proxy['fails']} failures: {safe_url} | "
                    f"Active proxies remaining: {sum(1 for p in self._pool if p['status'] == 'active')}"
                )

    def report_success(self, proxy: dict) -> None:
        """
        Resetea el contador de fallas de un proxy tras una request exitosa.

        Args:
            proxy: Dict del proxy que tuvo éxito.

        Returns:
            None.

        Ejemplo:
            proxy_manager.report_success(proxy)
        """
        with self._lock:
            proxy["fails"] = 0

    @staticmethod
    def _mask_proxy_url(url: str) -> str:
        """
        Enmascara las credenciales embebidas en una URL de proxy
        (user:pass@host) para poder loguearla sin exponerlas.

        Args:
            url: URL completa del proxy, con o sin credenciales.

        Returns:
            URL con las credenciales reemplazadas por ***:***, o "***" si no se
            pudo parsear.

        Ejemplo:
            ProxyManager._mask_proxy_url("http://user:pass@1.2.3.4:8080")
            # "http://***:***@1.2.3.4:8080"
        """
        try:
            if "@" in url:
                protocol = url.split("://")[0]
                host_part = url.split("@")[-1]
                return f"{protocol}://***:***@{host_part}"
        except Exception:
            pass
        return "***"

    @property
    def total(self) -> int:
        """
        Cantidad total de proxies en el pool (activos + baneados).

        Returns:
            Entero con el tamaño total del pool.
        """
        return len(self._pool)

    @property
    def active_count(self) -> int:
        """
        Cantidad de proxies actualmente activos (no baneados).

        Returns:
            Entero con la cantidad de proxies activos.
        """
        with self._lock:
            return sum(1 for p in self._pool if p["status"] == "active")


# =============================================================================
# PROXY LOADING & VALIDATION
# =============================================================================


def load_proxy_settings() -> list[dict]:
    """
    Carga la lista de proxies activos desde el archivo del deployment
    package resources/proxy_settings.json (no versionado en git, contiene
    credenciales reales — ver decisions.md → "Por qué lmbd-mc-exchange-rates
    se reescribió con scraping vía proxies").

    Returns:
        Lista de dicts de proxy con status == "active", o lista vacía si el
        archivo no existe o no se pudo parsear (se loguea el error, no se
        relanza).

    Ejemplo:
        load_proxy_settings()  # [{'proxy': 'http://user:pass@host:port', 'status': 'active'}, ...]
    """
    try:
        with open("resources/proxy_settings.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        raw_proxies = data.get("proxy_settings", {}).get(
            "proxy_list_mastercard", []
        ) or data.get("proxy_settings", {}).get("proxy_list", [])
        active = [p for p in raw_proxies if p.get("status") == "active"]
        logger.info(f"[load_proxy_settings] {len(active)} active proxies loaded")
        return active

    except Exception as e:
        logger.error(f"[load_proxy_settings] Failed to load proxy file: {e}")
        return []


def validate_proxies(proxies: list[dict]) -> list[dict]:
    """
    Valida TODOS los proxies concurrentemente contra la API de Mastercard
    antes de iniciar el worker, para no perder tiempo de scraping real con
    proxies caídos. Descarta cualquier proxy que falle, dé timeout, o
    responda con un status distinto de 200.

    Args:
        proxies: Lista de dicts de proxy a validar (típicamente el resultado
            de load_proxy_settings()).

    Returns:
        Lista de dicts de proxy que pasaron la validación (subconjunto de
        proxies), o lista vacía si proxies está vacía.

    Ejemplo:
        validate_proxies(load_proxy_settings())  # solo los proxies que respondieron 200
    """
    if not proxies:
        return []

    logger.info(
        f"[validate_proxies] Launching concurrent validation for ALL {len(proxies)} proxies..."
    )
    valid_proxies = []

    test_params = {
        "exchange_date": datetime.now(timezone.utc).strftime(DATE_FORMAT_INPUT),
        "transaction_currency": "USD",
        "cardholder_billing_currency": "EUR",
        "bank_fee": "0",
        "transaction_amount": "1",
    }

    def check_proxy(proxy_dict: dict) -> dict | None:
        """
        Prueba un único proxy contra la API de Mastercard con una consulta de
        prueba (USD→EUR), devolviéndolo solo si responde 200 con contenido.

        Args:
            proxy_dict: Dict de un proxy a probar.

        Returns:
            El mismo proxy_dict si la prueba fue exitosa, o None si falló, dio
            timeout, o respondió con un status/código de error.

        Ejemplo:
            check_proxy({'proxy': 'http://...', 'status': 'active'})
        """
        proxy_url = proxy_dict["proxy"]
        # Enmascaramos la credencial para un log seguro
        safe_url = ProxyManager._mask_proxy_url(proxy_url)
        try:
            with requests.Session(impersonate="chrome120") as s:
                s.proxies = {"http": proxy_url, "https": proxy_url}
                resp = s.get(
                    MASTERCARD_RATES_URL,
                    params=test_params,
                    headers=REQUEST_HEADERS,
                    timeout=5,
                )

                if resp.status_code == 200 and resp.text.strip():
                    return proxy_dict
                else:
                    # Captura casos donde el proxy responde pero con códigos de error (ej. 403, 502)
                    logger.warning(
                        f"[validate_proxies] Proxy {safe_url} rejected connection | HTTP Status: {resp.status_code}"
                    )
        except Exception as e:
            # Captura caídas de red a nivel de socket (ej. Timeouts, Connection resets, Aborted)
            logger.warning(
                f"[validate_proxies] Proxy {safe_url} failed validation test | Details: {e}"
            )
        return None

    # Test de ping masivo e hilos concurrentes para aislar nodos funcionales
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = executor.map(check_proxy, proxies)
        for res in results:
            if res:
                valid_proxies.append(res)

    discarded = len(proxies) - len(valid_proxies)
    logger.info(
        f"[validate_proxies] Pre-flight complete | "
        f"Passed: {len(valid_proxies)} | Discarded: {discarded}"
    )

    return valid_proxies


# =============================================================================
# CURRENCY LIST
# =============================================================================


def fetch_currency_list() -> list[list[str]] | str:
    """
    Carga el catálogo de monedas desde resources/currencies.json (commiteado
    en el repo) y arma todos los pares posibles origen≠destino (matriz
    cruzada completa).

    Returns:
        Lista de pares [from_currency, to_currency], o el string "error" si
        no se pudo cargar/parsear el archivo (se loguea el error).

    Ejemplo:
        fetch_currency_list()  # [['USD', 'EUR'], ['USD', 'GBP'], ...]
    """
    try:
        with open("resources/currencies.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        currencies = [c["alphaCd"] for c in data["currencies"]]
        pairs = [[src, dst] for src in currencies for dst in currencies if src != dst]
        logger.info(
            f"[fetch_currency_list] {len(currencies)} currencies -> {len(pairs)} pairs"
        )
        return pairs

    except Exception as e:
        logger.error(f"[fetch_currency_list] Failed to load currencies: {e}")
        return "error"


# =============================================================================
# HELPERS
# =============================================================================


def generate_date_range(begin_date_str: str, end_date_str: str) -> list[str]:
    """
    Genera la lista de fechas (en formato de salida MM/DD/YYYY, el que
    espera la API de Mastercard) entre dos fechas de entrada YYYY-MM-DD,
    inclusive.

    Args:
        begin_date_str: Fecha de inicio en formato "YYYY-MM-DD".
        end_date_str: Fecha de fin en formato "YYYY-MM-DD".

    Returns:
        Lista de fechas en formato "MM/DD/YYYY".

    Raises:
        ValueError: si alguna de las fechas no tiene el formato esperado.

    Ejemplo:
        generate_date_range("2026-01-01", "2026-01-03")
        # ["01/01/2026", "01/02/2026", "01/03/2026"]
    """
    try:
        begin = datetime.strptime(begin_date_str, DATE_FORMAT_INPUT)
        end = datetime.strptime(end_date_str, DATE_FORMAT_INPUT)
        dates = [
            (begin + timedelta(days=i)).strftime(DATE_FORMAT_OUTPUT)
            for i in range((end - begin).days + 1)
        ]
        logger.info(
            f"[generate_date_range] {len(dates)} date(s) generated: {dates[0]} -> {dates[-1]}"
        )
        return dates
    except ValueError as e:
        logger.error(f"[generate_date_range] Invalid date format: {e}")
        raise


def split_into_chunks(items: list, num_chunks: int) -> list[list]:
    """
    Divide una lista en N chunks lo más parejos posible — el resto
    (remainder) se reparte de a uno entre los primeros chunks, para que
    ningún chunk tenga más de 1 elemento de diferencia respecto a los demás.

    Args:
        items: Lista a dividir (pares de moneda, o sub-chunks dentro de un
            worker).
        num_chunks: Cantidad de chunks a generar.

    Returns:
        Lista de num_chunks listas, con los elementos de items repartidos.

    Ejemplo:
        split_into_chunks([1, 2, 3, 4, 5], 2)  # [[1, 2, 3], [4, 5]]
    """
    try:
        chunk_size, remainder = divmod(len(items), num_chunks)
        chunks = []
        start = 0

        for i in range(num_chunks):
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append(items[start:end])
            start = end

        sizes = [len(c) for c in chunks]
        logger.info(
            f"[split_into_chunks] {len(items)} items -> {num_chunks} chunks | "
            f"min={min(sizes)} | max={max(sizes)}"
        )
        return chunks
    except Exception as e:
        logger.error(f"[split_into_chunks] Failed to split list: {e}")
        raise


def delete_existing_parquets(date_str: str) -> int:
    """
    Borra todos los Parquets bajo el prefix S3 de una fecha dada, antes de
    reprocesarla — evita mezclar chunks de una corrida anterior con la nueva.

    Args:
        date_str: Fecha en formato "YYYY-MM-DD", usada para construir el
            prefix {S3_PREFIX}/exchange_date={date_str}/.

    Returns:
        Cantidad de objetos borrados (0 si no había ninguno). Relanza
        cualquier excepción tras loguearla.

    Ejemplo:
        delete_existing_parquets("2026-01-03")  # 10
    """
    prefix = f"{S3_PREFIX}/exchange_date={date_str}/"
    try:
        s3 = boto3.client("s3")
        objects = []
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            objects.extend(page.get("Contents", []))

        if not objects:
            logger.info(
                f"[delete_existing_parquets] No existing files at s3://{S3_BUCKET}/{prefix}"
            )
            return 0

        s3.delete_objects(
            Bucket=S3_BUCKET, Delete={"Objects": [{"Key": o["Key"]} for o in objects]}
        )
        logger.info(
            f"[delete_existing_parquets] Deleted {len(objects)} file(s) from s3://{S3_BUCKET}/{prefix}"
        )
        return len(objects)

    except Exception as e:
        logger.error(
            f"[delete_existing_parquets] Failed to delete files at {prefix}: {e}"
        )
        raise


def save_chunk_to_s3(records: list[dict], date_str: str, chunk_id: int) -> str:
    """
    Serializa los registros de tipo de cambio de un chunk a Parquet y lo
    sube a S3 como archivo temporal (temp_chunks/), a la espera de que el
    consolidador los una. Descarta los registros con fx_rate vacío (pares
    que fallaron el scraping) antes de escribir.

    Args:
        records: Lista de dicts {from_currency, to_currency, fx_rate,
            creation_timestamp} del chunk (puede incluir registros fallidos
            con fx_rate="").
        date_str: Fecha en formato "YYYY-MM-DD".
        chunk_id: Número de chunk (1-indexado), usado en el nombre del
            archivo.

    Returns:
        S3 key del archivo temporal escrito (aunque no haya registros
        válidos, en cuyo caso no se sube nada pero igual se retorna el key
        calculado).

    Ejemplo:
        save_chunk_to_s3(records, "2026-01-03", 1)
        # "exchange-rates/brand=Mastercard/exchange_date=2026-01-03/temp_chunks/20260103_chunk_1.parquet"
    """
    file_date = datetime.strptime(date_str, DATE_FORMAT_INPUT).strftime(
        DATE_FORMAT_FILE
    )
    s3_key = (
        f"{S3_PREFIX}/exchange_date={date_str}/temp_chunks/{file_date}_chunk_{chunk_id}.parquet"
    )
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    valid_records = [r for r in records if r["fx_rate"] != ""]
    skipped_count = len(records) - len(valid_records)
    num_records = len(valid_records)
    
    if not valid_records:
        logger.warning(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | No valid records, skipping upload"
        )
        return s3_key

    try:
        table = pa.table(
            {
                "from_currency": [r["from_currency"] for r in valid_records],
                "to_currency": [r["to_currency"] for r in valid_records],
                "fx_rate": [r["fx_rate"] for r in valid_records],
                "creation_timestamp": [current_timestamp] * num_records,
            }
        )

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        logger.info(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | "
            f"written={len(valid_records)} | skipped={skipped_count} | "
            f"s3://{S3_BUCKET}/{s3_key}"
        )
        return s3_key

    except Exception as e:
        logger.error(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | Failed to upload parquet: {e}"
        )
        raise


def invoke_next_worker(date: str, chunks: list, chunk_index: int) -> None:
    """
    Invoca asincrónicamente (fire-and-forget) al siguiente eslabón de la
    cadena: el próximo worker si quedan chunks por procesar, o el
    consolidador si chunk_index ya superó la cantidad de chunks — así se
    evita el timeout de un único Lambda procesando todos los pares de moneda
    de una fecha.

    Args:
        date: Fecha en formato "MM/DD/YYYY" (formato interno de la cadena).
        chunks: Lista completa de chunks de pares de moneda.
        chunk_index: Índice del próximo chunk a procesar. Si es >= len(chunks),
            se invoca el consolidador en su lugar.

    Returns:
        None. Relanza cualquier excepción de la invocación tras loguearla.

    Ejemplo:
        invoke_next_worker("01/03/2026", chunks, chunk_index=1)
    """
    if chunk_index >= len(chunks):
        logger.info("[invoke_next_worker] All chunks processed. Invoking consolidator...")
        try:
            boto3.client("lambda").invoke(
                FunctionName=FUNCTION_NAME,
                InvocationType="Event",
                Payload=json.dumps({
                    "mode": "consolidator",
                    "date": date
                }),
            )
        except Exception as e:
            logger.error(f"[invoke_next_worker] Failed to invoke consolidator: {e}")
            raise
        return

    try:
        payload = {
            "mode": "worker",
            "date": date,
            "chunks": chunks,
            "chunk_index": chunk_index,
        }
        boto3.client("lambda").invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )
        logger.info(
            f"[invoke_next_worker] Worker invoked | "
            f"chunk_index={chunk_index} | chunk_id={chunk_index + 1}/{len(chunks)} | "
            f"pairs={len(chunks[chunk_index])}"
        )
    except Exception as e:
        logger.error(
            f"[invoke_next_worker] Failed to invoke worker at chunk_index={chunk_index}: {e}"
        )
        raise


# =============================================================================
# STEP 2: Process a sub-chunk of pairs inside a single thread
# =============================================================================


def process_sub_chunk(
    date: str,
    sub_chunk: list[list[str]],
    worker_id: int,
    proxy_manager: ProxyManager,
) -> list[dict]:
    """
    Procesa un sub-chunk de pares de moneda dentro de un único hilo,
    reutilizando una sola sesión HTTP (curl_cffi, impersonando Chrome 120
    para evadir fingerprinting básico). Por cada par: elige un proxy por
    round-robin (ProxyManager.pick), hace la consulta, reporta éxito/falla
    al ProxyManager, y espacia cada request con una pausa aleatoria
    (PAUSE_MIN–PAUSE_MAX) para no gatillar rate-limiting. Ante HTTP 403/429
    (bloqueo) o error de red (timeout/reset/aborted/connect), banea el
    proxy usado; ante cualquier otro fallo, el par queda con fx_rate="" sin
    reintentarse en esta pasada.

    Args:
        date: Fecha en formato "MM/DD/YYYY" (formato interno de la cadena).
        sub_chunk: Lista de pares [from_currency, to_currency] a scrapear en
            este hilo.
        worker_id: Número de hilo dentro del worker (solo para logging).
        proxy_manager: Pool de proxies compartido entre todos los hilos del
            worker.

    Returns:
        Lista de dicts {from_currency, to_currency, fx_rate,
        creation_timestamp}, uno por cada par de sub_chunk (con fx_rate=""
        para los que fallaron).

    Ejemplo:
        process_sub_chunk("01/03/2026", [["USD", "EUR"]], 1, proxy_manager)
        # [{'from_currency': 'USD', 'to_currency': 'EUR', 'fx_rate': 0.92, ...}]
    """
    date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)
    thread_records = []
    sub_chunk_total = len(sub_chunk)

    with requests.Session(impersonate="chrome120") as session:
        for idx, pair in enumerate(sub_chunk):
            from_curr, to_curr = pair

            params = {
                "exchange_date": date_str,
                "transaction_currency": from_curr,
                "cardholder_billing_currency": to_curr,
                "bank_fee": "0",
                "transaction_amount": "1",
            }

            empty_record = {
                "from_currency": from_curr,
                "to_currency": to_curr,
                "fx_rate": "",
                "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            proxy = proxy_manager.pick(idx)
            if proxy:
                session.proxies = {"http": proxy["proxy"], "https": proxy["proxy"]}
            else:
                session.proxies = {}
                logger.warning(
                    f"[Thread {worker_id}] No active proxies available — running without proxy"
                )

            try:
                response = session.get(
                    MASTERCARD_RATES_URL,
                    params=params,
                    headers=REQUEST_HEADERS,
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code in (403, 429):
                    logger.warning(
                        f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                        f"HTTP {response.status_code} (blocked) | {from_curr}->{to_curr} | {date_str}"
                    )
                    if proxy:
                        proxy_manager.report_failure(proxy)
                    thread_records.append(empty_record)
                    time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
                    continue

                if response.status_code != 200 or not response.text.strip():
                    logger.warning(
                        f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                        f"HTTP {response.status_code} empty/unexpected | {from_curr}->{to_curr}"
                    )
                    thread_records.append(empty_record)
                    time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
                    continue

                fx_rate = float(
                    str(response.json()["data"]["conversionRate"]).replace(",", "")
                )

                if proxy:
                    proxy_manager.report_success(proxy)

                thread_records.append(
                    {
                        "from_currency": from_curr,
                        "to_currency": to_curr,
                        "fx_rate": fx_rate,
                        "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                logger.info(
                    f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                    f"OK {from_curr}->{to_curr} | {date_str} | fx={fx_rate}"
                )

            except Exception as e:
                error_msg = str(e).lower()
                logger.error(
                    f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                    f"Error | {from_curr}->{to_curr} | {date_str} | {type(e).__name__}: {e}"
                )

                # Report proxy failure on network-level errors
                if proxy and any(
                    keyword in error_msg
                    for keyword in ("timeout", "reset", "aborted", "connect")
                ):
                    proxy_manager.report_failure(proxy)

                thread_records.append(empty_record)

            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

    return thread_records


# =============================================================================
# ORCHESTRATOR
# =============================================================================


def run_orchestrator(begin_date: str, end_date: str) -> dict:
    """
    Rol orquestador de la cadena: valida el rango de fechas, arma la lista
    completa de pares de moneda, borra los Parquets existentes de cada fecha
    (para no mezclar con una corrida anterior) y dispara la cadena de
    workers (chunk_index=0) para cada fecha del rango, de forma
    asincrónica.

    Args:
        begin_date: Fecha de inicio en formato "YYYY-MM-DD".
        end_date: Fecha de fin en formato "YYYY-MM-DD".

    Returns:
        Dict con statusCode, mode="orchestrator", chains (cantidad de
        fechas/cadenas iniciadas), total_pairs y dates. Relanza cualquier
        excepción tras loguearla.

    Ejemplo:
        run_orchestrator("2026-01-01", "2026-01-03")
        # {'statusCode': 200, 'mode': 'orchestrator', 'chains': 3, 'total_pairs': 380, ...}
    """
    logger.info(f"[ORCHESTRATOR] Starting | begin={begin_date} | end={end_date}")

    try:
        dates = generate_date_range(begin_date, end_date)
        pairs = fetch_currency_list()

        if isinstance(pairs, str):
            raise RuntimeError("Failed to retrieve currency list")

        chunks = split_into_chunks(pairs, NUM_CHUNKS)

        for date in dates:
            date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(
                DATE_FORMAT_INPUT
            )
            delete_existing_parquets(date_str)

            logger.info(
                f"[ORCHESTRATOR] Starting chain for {date} | {NUM_CHUNKS} chunks | {len(pairs)} pairs"
            )
            invoke_next_worker(date, chunks, chunk_index=0)

        logger.info(f"[ORCHESTRATOR] Done | {len(dates)} chain(s) started")
        return {
            "statusCode": 200,
            "mode": "orchestrator",
            "chains": len(dates),
            "total_pairs": len(pairs),
            "dates": dates,
        }

    except Exception as e:
        logger.error(f"[ORCHESTRATOR] Fatal error: {e}")
        raise


# =============================================================================
# WORKER
# =============================================================================


def run_worker(date: str, chunks: list, chunk_index: int) -> dict:
    """
    Rol worker de la cadena: procesa un único chunk de pares de moneda para
    una fecha. Carga y valida los proxies (nueva validación en cada
    invocación, porque cada worker es una ejecución de Lambda distinta),
    divide su chunk en sub-chunks (uno por hilo, hasta MAX_WORKERS hilos
    concurrentes vía ThreadPoolExecutor), junta los resultados de todos los
    hilos, los sube a S3 como Parquet temporal (save_chunk_to_s3), e invoca
    al siguiente worker de la cadena (o al consolidador si era el último
    chunk).

    Args:
        date: Fecha en formato "MM/DD/YYYY" (formato interno de la cadena).
        chunks: Lista completa de chunks de pares de moneda para esta fecha.
        chunk_index: Índice del chunk que le toca procesar a este worker
            (0-indexado).

    Returns:
        Dict con statusCode, mode="worker", chunk_id, records_ok,
        records_skip y s3_key. Relanza cualquier excepción tras loguearla.

    Ejemplo:
        run_worker("01/03/2026", chunks, chunk_index=0)
        # {'statusCode': 200, 'mode': 'worker', 'chunk_id': 1, 'records_ok': 36, ...}
    """
    chunk_id = chunk_index + 1
    pairs = chunks[chunk_index]
    total_pairs = len(pairs)

    logger.info(
        f"[WORKER {chunk_id}/{len(chunks)}] Starting | "
        f"date={date} | pairs={total_pairs}"
    )

    try:
        date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(
            DATE_FORMAT_INPUT
        )
        proxy_manager = ProxyManager(validate_proxies(load_proxy_settings()))

        logger.info(
            f"[WORKER {chunk_id}/{len(chunks)}] Proxy pool initialized | "
            f"total={proxy_manager.total} | active={proxy_manager.active_count}"
        )

        sub_chunks = split_into_chunks(pairs, MAX_WORKERS)
        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    process_sub_chunk, date, sub_chunk, i + 1, proxy_manager
                )
                for i, sub_chunk in enumerate(sub_chunks)
                if sub_chunk
            ]
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as e:
                    logger.error(f"[WORKER {chunk_id}/{len(chunks)}] Thread error: {e}")

        s3_key = save_chunk_to_s3(results, date_str, chunk_id)
        written_count = len([r for r in results if r["fx_rate"] != ""])
        skipped_count = len(results) - written_count

        logger.info(
            f"[WORKER {chunk_id}/{len(chunks)}] Done | date={date_str} | "
            f"written={written_count} | skipped={skipped_count} | "
            f"active_proxies={proxy_manager.active_count} | file={s3_key}"
        )

        invoke_next_worker(date, chunks, chunk_index=chunk_index + 1)

        return {
            "statusCode": 200,
            "mode": "worker",
            "chunk_id": chunk_id,
            "records_ok": written_count,
            "records_skip": skipped_count,
            "s3_key": s3_key,
        }

    except Exception as e:
        logger.error(
            f"[WORKER {chunk_id}/{len(chunks)}] Fatal error | date={date} | {e}"
        )
        raise

# =============================================================================
# CONSOLIDATOR
# =============================================================================

def run_consolidator(date: str) -> dict:
    """
    Rol consolidador de la cadena: une todos los Parquets temporales de una
    fecha (temp_chunks/) en un único Parquet final, y borra los temporales
    tras escribirlo — última etapa de la cadena
    orquestador→worker(s)→consolidador.

    Args:
        date: Fecha en formato "MM/DD/YYYY" (formato interno de la cadena).

    Returns:
        Dict con statusCode, mode="consolidator", date, total_records y
        final_file; o {"statusCode": 200, "message": "No data to
        consolidate"} si no había chunks temporales que consolidar. Relanza
        cualquier excepción tras loguearla.

    Ejemplo:
        run_consolidator("01/03/2026")
        # {'statusCode': 200, 'mode': 'consolidator', 'total_records': 380, ...}
    """
    date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)
    file_date = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_FILE)
    
    temp_prefix = f"{S3_PREFIX}/exchange_date={date_str}/temp_chunks/"
    final_s3_key = f"{S3_PREFIX}/exchange_date={date_str}/Mastercard_{file_date}.parquet"
    
    logger.info(f"[CONSOLIDATOR] Starting consolidation for {date_str}...")
    
    s3 = boto3.client("s3")
    tables = []
    objects_to_delete = []

    try:
        # 1. Listar y leer todos los chunks temporales
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=temp_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                objects_to_delete.append({"Key": key})
                
                # Descargar a memoria
                response = s3.get_object(Bucket=S3_BUCKET, Key=key)
                buffer = io.BytesIO(response['Body'].read())
                
                # Leer parquet y agregarlo a la lista
                table = pq.read_table(buffer)
                tables.append(table)

        if not tables:
            logger.warning(f"[CONSOLIDATOR] No temporary chunks found at {temp_prefix}. Skipping.")
            return {"statusCode": 200, "message": "No data to consolidate"}

        # 2. Unir todas las tablas en una sola
        consolidated_table = pa.concat_tables(tables)
        total_records = consolidated_table.num_rows

        # 3. Guardar el archivo consolidado final
        out_buffer = io.BytesIO()
        pq.write_table(consolidated_table, out_buffer)
        out_buffer.seek(0)
        
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=final_s3_key,
            Body=out_buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info(f"[CONSOLIDATOR] Successfully saved {total_records} records to s3://{S3_BUCKET}/{final_s3_key}")

        # 4. Limpiar los archivos temporales
        s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects_to_delete})
        logger.info(f"[CONSOLIDATOR] Cleaned up {len(objects_to_delete)} temporary chunk(s).")

        return {
            "statusCode": 200,
            "mode": "consolidator",
            "date": date_str,
            "total_records": total_records,
            "final_file": final_s3_key
        }

    except Exception as e:
        logger.error(f"[CONSOLIDATOR] Fatal error during consolidation: {e}")
        raise


# =============================================================================
# MAIN HANDLER
# =============================================================================


def lambda_handler(event: dict, context) -> dict:
    # logger.info(f"[lambda_handler] RAW EVENT: {json.dumps(event)}")
    """
    Punto de entrada de la Lambda lmbd-mc-exchange-rates. Despacha según el
    campo mode del evento al rol correspondiente de la cadena
    orquestador/worker/consolidador — la primera invocación (disparada
    externamente, ej. por un scheduler) es siempre mode="orchestrator"; las
    siguientes (worker, consolidator) se auto-invocan entre sí de forma
    asincrónica (ver invoke_next_worker).

    Args:
        event: Payload con mode ("orchestrator", "worker" o "consolidator")
            y los campos específicos de cada rol — begin_date/end_date
            (orchestrator, opcionales, default BEGIN_DATE/END_DATE),
            date+chunks+chunk_index (worker), date (consolidator).
        context: Contexto de ejecución de Lambda (no usado).

    Returns:
        Dict de resultado del rol invocado (ver run_orchestrator, run_worker,
        run_consolidator). Lanza ValueError si mode no es ninguno de los tres
        válidos, o KeyError si falta un campo requerido para el modo (date,
        chunks).

    Ejemplo:
        lambda_handler({'mode': 'orchestrator', 'begin_date': '2026-01-01',
                         'end_date': '2026-01-03'}, context)
    """
    mode = event.get("mode", "orchestrator")
    logger.info(f"[lambda_handler] Event received | mode={mode}")

    try:
        if mode == "orchestrator":
            begin_date = event.get("begin_date", BEGIN_DATE)
            end_date = event.get("end_date", END_DATE)
            return run_orchestrator(begin_date, end_date)

        if mode == "worker":
            date = event["date"]
            chunks = event["chunks"]
            chunk_index = event.get("chunk_index", 0)
            return run_worker(date, chunks, chunk_index)
     
        if mode == "consolidator":
            date = event["date"]
            return run_consolidator(date)

        raise ValueError(f"Unknown mode: '{mode}'. Use 'orchestrator', 'worker' or 'consolidator'.")

    except KeyError as e:
        logger.error(f"[lambda_handler] Missing required field in event: {e}")
        raise
    except ValueError as e:
        logger.error(f"[lambda_handler] Invalid event value: {e}")
        raise
    except Exception as e:
        logger.error(f"[lambda_handler] Fatal error: {e}")
        raise