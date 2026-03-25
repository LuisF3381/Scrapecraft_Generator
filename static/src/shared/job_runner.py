import argparse
import logging
import pandas as pd
import yaml
from datetime import datetime
from pathlib import Path
from src.shared.driver_config import create_driver
from src.shared.logger import setup_logger
from src.shared.storage import save_data, save_raw, cleanup_raw, load_raw  # load_raw solo para --reprocess
from config import global_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FLUJO ETL — vision general
#
#   FLUJO COMPLETO (skip_process=False):
#     run() → _run_full() → scrape()    → [scraper.py]   <- implementar aqui
#                         → save_raw()
#                         → process()   → [process.py]   <- implementar aqui
#                         → cleanup_raw()
#           → _save_output()
#
#   FLUJO SIN PROCESS (SKIP_PROCESS=True en settings.py):
#     run() → _run_full() → scrape()
#                         → save_raw()
#                         → load_raw()
#                         → cleanup_raw()
#           → _save_output()
#
#   FLUJO REPROCESS (--reprocess <sufijo>):
#     run() → _run_reprocess() → process()  → [process.py]
#           → _save_output()
#
#   Como data engineer solo debes implementar scraper.py y process.py.
#   app_job.py no requiere modificaciones: solo declara los imports del job.
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent.parent


def _validate_web_config(config: dict, job_name: str) -> None:
    """Valida que web_config.yaml tenga las claves y tipos requeridos."""
    required = {"url", "selectors", "waits"}
    missing = required - config.keys()
    if missing:
        raise ValueError(
            f"web_config.yaml del job '{job_name}' le faltan claves requeridas: {sorted(missing)}"
        )
    if not isinstance(config["selectors"], dict) or not config["selectors"]:
        raise ValueError(
            f"web_config.yaml del job '{job_name}': 'selectors' debe ser un dict no vacio."
        )
    if not isinstance(config["waits"], dict):
        raise ValueError(
            f"web_config.yaml del job '{job_name}': 'waits' debe ser un dict."
        )


def load_web_config(job_name: str) -> dict:
    """Carga y valida la configuracion de la web desde el archivo YAML del job."""
    path = _PROJECT_ROOT / "src" / job_name / "web_config.yaml"
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    _validate_web_config(config, job_name)
    logger.info(f"Configuracion cargada: {config['url']}")
    return config


# ---------------------------------------------------------------------------
# Flujos internos
# ---------------------------------------------------------------------------


def _run_full(scrape_fn, process_fn, settings, job_name: str, now: datetime, params: dict) -> list[dict]:
    """Flujo completo: scraping → raw → (proceso opcional).
    La limpieza de raw se realiza en run() despues de guardar el output."""
    logger.info("Iniciando scraper...")
    if params:
        logger.info(f"Parametros recibidos: {params}")

    web_config = load_web_config(job_name)
    driver = create_driver(settings.DRIVER_CONFIG)

    try:
        datos = scrape_fn(driver, web_config, params)
    finally:
        driver.quit()

    if not datos:
        raise RuntimeError("El scraper no retorno datos. Verifica la URL, los selectores o posible bloqueo.")

    # Normalizacion string-first: se construye el DataFrame una sola vez y se
    # reutiliza tanto para save_raw como para el procesamiento posterior.
    df_raw = pd.DataFrame(datos).fillna("").astype(str)
    del datos

    suffix = save_raw(df_raw, settings.RAW_CONFIG, global_settings.DATA_CONFIG, now)
    logger.info(f"Si el proceso falla, puedes reprocesar con: --reprocess {suffix}")

    if settings.SKIP_PROCESS:
        logger.info("skip_process=True: omitiendo process.py, usando raw directamente")
        processed = df_raw.to_dict(orient="records")
    else:
        processed = process_fn(df_raw)

    return processed


def _run_reprocess(suffix: str, process_fn, settings) -> list[dict]:
    """Flujo reprocess: omite el scraping y reprocesa un raw existente."""
    logger.info(f"Iniciando reprocesamiento: sufijo {suffix}")
    df = load_raw(
        suffix=suffix,
        raw_config=settings.RAW_CONFIG,
        data_config=global_settings.DATA_CONFIG,
    )
    return process_fn(df)


def _save_output(processed: list[dict], settings, now: datetime) -> dict[str, Path]:
    """Guarda los datos procesados en todos los formatos configurados.

    Returns:
        dict[str, Path]: Mapa de formato -> ruta del archivo guardado.
    """
    output_formats = settings.STORAGE_CONFIG.get("output_formats", ["csv"])
    paths: dict[str, Path] = {}
    for formato in output_formats:
        paths[formato] = save_data(processed, formato, global_settings.DATA_CONFIG, settings.STORAGE_CONFIG, now)
    logger.info("Proceso finalizado")
    return paths


# ---------------------------------------------------------------------------
# Punto de entrada generico (llamado desde app_job.py de cada job)
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace, scrape_fn, process_fn, settings, job_name: str, params: dict | None = None) -> dict[str, Path]:
    """
    Punto de entrada generico para cualquier job.

    Args:
        args:       Argumentos CLI (args.reprocess: str | None)
        scrape_fn:  Funcion scrape() del job
        process_fn: Funcion process() del job
        settings:   Modulo de configuracion del job
        job_name:   Nombre del job (nombre de la carpeta en src/)
        params:     Parametros del job definidos en el pipeline YAML (dict nativo)

    Returns:
        dict[str, Path]: Mapa de formato -> ruta del archivo guardado (ej: {"csv": Path(...)}).
    """
    now = datetime.now()
    setup_logger(job_name, now, **global_settings.LOG_CONFIG)

    params = params or {}

    output_paths: dict[str, Path] = {}
    try:
        if args.reprocess:
            processed = _run_reprocess(args.reprocess, process_fn, settings)
        else:
            processed = _run_full(scrape_fn, process_fn, settings, job_name, now, params)

        output_paths = _save_output(processed, settings, now)

    except Exception as e:
        logger.error(f"Error durante la ejecucion: {e}", exc_info=True)
        raise

    finally:
        # cleanup_raw corre siempre (exito o fallo) para que la politica de retencion
        # se aplique aunque el job falle. En --reprocess no hay raw nuevo que gestionar.
        if not args.reprocess:
            cleanup_raw(settings.RAW_CONFIG)

    return output_paths
