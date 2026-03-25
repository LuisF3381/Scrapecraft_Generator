import argparse
import importlib
import logging
import pandas as pd
import yaml
from datetime import datetime
from pathlib import Path
from src.shared import job_runner
from src.shared import logger as logger_module
from src.shared.storage import save_data, load_output, clear_latest, copy_to_latest, merge_logs_to_latest
from config import global_settings

# Logger del orquestador en namespace propio para que setup_logger() (que limpia "src")
# no elimine su handler entre jobs de un pipeline.
logger = logging.getLogger("orchestrator")

SUPPORTED_FORMATS = {"csv", "json", "xml", "xlsx"}


def _setup_console_handler() -> None:
    """Configura un handler de consola en el logger 'orchestrator'.
    Al usar un namespace separado de 'src', setup_logger() de cada job puede
    reemplazar los handlers de 'src' sin afectar los mensajes del orquestador."""
    orch_logger = logging.getLogger("orchestrator")
    orch_logger.setLevel(logging.INFO)
    orch_logger.propagate = False
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    orch_logger.addHandler(handler)


def get_available_jobs() -> list[str]:
    """Escanea src/ y retorna los jobs disponibles (carpetas con scraper.py)."""
    src_path = Path(__file__).parent
    return [
        entry.name
        for entry in src_path.iterdir()
        if entry.is_dir() and (entry / "scraper.py").is_file()
    ]


def _load_job_parts(job_name: str) -> tuple:
    """Importa y retorna (scrape_fn, process_fn, validate_fn, settings) del job indicado."""
    try:
        scraper  = importlib.import_module(f"src.{job_name}.scraper")
        process  = importlib.import_module(f"src.{job_name}.process")
        settings = importlib.import_module(f"src.{job_name}.settings")
    except ModuleNotFoundError:
        available = ", ".join(get_available_jobs()) or "ninguno"
        logger.error(f"Job '{job_name}' no encontrado. Jobs disponibles: {available}")
        raise SystemExit(1)

    try:
        validate = importlib.import_module(f"src.{job_name}.validate")
    except ModuleNotFoundError:
        logger.error(
            f"El job '{job_name}' no tiene validate.py. "
            f"Crea src/{job_name}/validate.py con la funcion validate(df: pd.DataFrame) -> list[str]."
        )
        raise SystemExit(1)

    return scraper.scrape, process.process, validate.validate, settings


def _make_args(job_name: str) -> argparse.Namespace:
    """Construye un Namespace de args para un job individual dentro de una serie."""
    return argparse.Namespace(
        job=job_name,
        pipeline=None,
        reprocess=None,
    )


# ---------------------------------------------------------------------------
# Consolidacion
# ---------------------------------------------------------------------------


def _validate_consolidation(job_entries: list[dict], consolidate_config: dict) -> None:
    """
    Valida antes de correr cualquier job que todos tengan el formato de
    consolidacion en su output_formats. Falla rapido con mensaje claro.
    """
    fmt = consolidate_config.get("format")
    if not fmt:
        logger.error("consolidate.format es obligatorio cuando consolidate.enabled es true.")
        raise SystemExit(1)

    if fmt not in SUPPORTED_FORMATS:
        logger.error(f"consolidate.format='{fmt}' no es valido. Formatos soportados: {sorted(SUPPORTED_FORMATS)}")
        raise SystemExit(1)

    module_name = consolidate_config.get("module")
    if not module_name:
        logger.error("consolidate.module es obligatorio cuando consolidate.enabled es true.")
        raise SystemExit(1)

    for entry in job_entries:
        job_name = entry["name"]
        try:
            settings = importlib.import_module(f"src.{job_name}.settings")
        except ModuleNotFoundError:
            continue  # El error de job no encontrado se manejara al ejecutarlo
        output_formats = settings.STORAGE_CONFIG.get("output_formats", ["csv"])
        if fmt not in output_formats:
            logger.error(
                f"Consolidacion activada con format='{fmt}', pero el job '{job_name}' "
                f"no incluye '{fmt}' en output_formats: {output_formats}. "
                f"Agrega '{fmt}' a STORAGE_CONFIG['output_formats'] en src/{job_name}/settings.py"
            )
            raise SystemExit(1)


def _run_consolidation(job_outputs: dict[str, Path], consolidate_config: dict) -> dict[str, Path]:
    """
    Carga el modulo consolidador, ejecuta consolidate() y guarda el resultado.

    Args:
        job_outputs:        Mapa job_name -> Path del archivo generado (formato de consolidacion).
        consolidate_config: Bloque 'consolidate' del pipeline YAML.

    Returns:
        dict[str, Path]: Mapa formato -> ruta del archivo consolidado guardado.
    """
    module_name = consolidate_config["module"]
    params = consolidate_config.get("params") or {}

    try:
        consolidator = importlib.import_module(f"src.consolidadores.{module_name}")
    except ModuleNotFoundError:
        logger.error(
            f"Consolidador '{module_name}' no encontrado. "
            f"Crea el modulo en src/consolidadores/{module_name}.py"
        )
        raise SystemExit(1)

    logger.info(f"\nIniciando consolidacion: {module_name}")
    logger.info(f"Fuentes: {list(job_outputs.keys())}")

    consolidation_fmt = consolidate_config["format"]
    job_dataframes = {
        job_name: load_output(filepath, consolidation_fmt, global_settings.DATA_CONFIG)
        for job_name, filepath in job_outputs.items()
    }

    result = consolidator.consolidate(job_dataframes, params)

    if not result:
        logger.warning("El consolidador no retorno datos.")
        return {}

    if hasattr(consolidator, "validate"):
        logger.info("Ejecutando validaciones del consolidador...")
        errors = consolidator.validate(pd.DataFrame(result))
        if errors:
            errors_str = "\n  - ".join(errors)
            raise ValueError(
                f"Validacion del consolidador fallida ({len(errors)} error(es)):\n  - {errors_str}"
            )
        logger.info("Validacion del consolidador exitosa")

    now = datetime.now()
    storage_config = consolidator.STORAGE_CONFIG
    output_formats = storage_config.get("output_formats", ["csv"])

    paths: dict[str, Path] = {}
    for output_fmt in output_formats:
        paths[output_fmt] = save_data(result, output_fmt, global_settings.DATA_CONFIG, storage_config, now)

    logger.info("Consolidacion finalizada")
    return paths


# ---------------------------------------------------------------------------
# Ejecucion en serie
# ---------------------------------------------------------------------------


def _run_series(job_entries: list[dict], consolidate_config: dict | None = None, pipeline_name: str | None = None) -> None:
    """
    Ejecuta una lista de jobs en serie.
    Cada entrada es un dict con claves: name (str), params (dict), reprocess (str|None).
    Si un job falla, registra el error y continua con el siguiente.
    Si consolidate_config esta activo y todos los jobs fueron exitosos, ejecuta la consolidacion.

    Gestion de latest/):
    - Sin consolidacion: cada job gestiona su propio latest/<job_name>/ de forma independiente.
    - Con consolidacion: se gestiona un unico latest/<pipeline_name>/ con el output consolidado
      y los logs de todos los jobs concatenados. Los jobs individuales no escriben en latest/.
    """
    is_consolidated = bool(consolidate_config and consolidate_config.get("enabled"))

    if is_consolidated:
        _validate_consolidation(job_entries, consolidate_config)
        pipeline_folder: str = pipeline_name or consolidate_config.get("module", "pipeline")
        clear_latest(pipeline_folder)
        collected_logs: list[Path] = []
    else:
        pipeline_folder = ""
        collected_logs = []

    total = len(job_entries)
    failed = []
    job_outputs: dict[str, Path] = {}
    consolidation_format: str = (consolidate_config or {}).get("format", "")

    for i, entry in enumerate(job_entries, start=1):
        job_name = entry["name"]
        params   = entry.get("params") or {}
        logger.info(f"\n[{i}/{total}] Iniciando job: {job_name}")

        try:
            scrape_fn, process_fn, validate_fn, settings = _load_job_parts(job_name)
            output_paths = job_runner.run(
                _make_args(job_name), scrape_fn, process_fn, validate_fn, settings, job_name,
                params=params,
                update_latest=not is_consolidated,
            )
            if consolidation_format and consolidation_format in output_paths:
                job_outputs[job_name] = output_paths[consolidation_format]
        except SystemExit:
            raise
        except Exception as e:
            logger.error(f"[{i}/{total}] ERROR en '{job_name}': {e}")
            failed.append(job_name)
        finally:
            if is_consolidated:
                logger_module.flush_log()
                if logger_module.current_log_path and logger_module.current_log_path not in collected_logs:
                    collected_logs.append(logger_module.current_log_path)

    logger.info(f"\n{'='*50}")
    logger.info(f"Serie finalizada: {total - len(failed)}/{total} jobs exitosos")
    if failed:
        logger.warning(f"Jobs con error: {', '.join(failed)}")

    if is_consolidated:
        if failed:
            logger.warning(
                f"Consolidacion omitida: {len(failed)} job(s) fallaron "
                f"({', '.join(failed)}). Todos los jobs deben ser exitosos."
            )
            merge_logs_to_latest(pipeline_folder, collected_logs)
        else:
            consolidator = importlib.import_module(f"src.consolidadores.{consolidate_config['module']}")
            base_filename = consolidator.STORAGE_CONFIG.get("filename")
            consolidation_paths: dict[str, Path] = {}
            try:
                consolidation_paths = _run_consolidation(job_outputs, consolidate_config)
            except SystemExit:
                raise
            except Exception as e:
                logger.error(f"ERROR en consolidacion: {e}")
            finally:
                copy_to_latest(pipeline_folder, consolidation_paths, None, base_filename)
                merge_logs_to_latest(pipeline_folder, collected_logs)


# ---------------------------------------------------------------------------
# Carga de pipeline
# ---------------------------------------------------------------------------


def _load_pipeline(path: str) -> tuple[list[dict], dict | None, str | None]:
    """
    Carga un pipeline YAML y retorna la lista de entradas de jobs y la
    configuracion de consolidacion (o None si no esta definida).

    Formato esperado:
        name: mi_pipeline           # opcional
        description: "..."          # opcional

        jobs:
          - name: books_to_scrape
            params:                 # opcional, dict nativo YAML
              categoria: mystery
              pagina: 1
            enabled: false          # opcional, omitir o poner true para ejecutar
          - name: viviendas_adonde

        consolidate:                # opcional
          enabled: true
          module: mi_consolidador   # src/consolidadores/mi_consolidador.py
          format: csv               # formato compartido por todos los jobs
          params: {}                # opcional
    """
    pipeline_path = Path(path)
    if not pipeline_path.is_file():
        logger.error(f"Pipeline '{path}' no encontrado.")
        raise SystemExit(1)

    with pipeline_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if "jobs" not in data or not isinstance(data["jobs"], list):
        logger.error(f"El pipeline '{path}' debe tener una clave 'jobs' con una lista de jobs.")
        raise SystemExit(1)

    pipeline_name: str | None = data.get("name") or None

    if pipeline_name:
        desc = f" — {data['description']}" if "description" in data else ""
        logger.info(f"Pipeline: {pipeline_name}{desc}")

    entries = []
    for item in data["jobs"]:
        if "name" not in item:
            logger.error("Cada job del pipeline debe tener un campo 'name'.")
            raise SystemExit(1)
        if item.get("enabled", True) is False:
            logger.info(f"Job '{item['name']}' desactivado (enabled: false), omitiendo.")
            continue
        entries.append({
            "name":   item["name"],
            "params": item.get("params") or {},
        })

    consolidate_config = data.get("consolidate") or None
    return entries, consolidate_config, pipeline_name


def main() -> None:
    _setup_console_handler()

    parser = argparse.ArgumentParser(description="ScrapeCraft - Web scraper multi-job")

    # --- Modos de ejecucion (mutuamente excluyentes) ---
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--job",
        metavar="JOB",
        help="Ejecutar un job individual (ej: books_to_scrape)"
    )
    mode_group.add_argument(
        "--pipeline",
        metavar="YAML",
        help="Ejecutar un pipeline definido en un archivo YAML (ej: config/pipelines/diario.yaml)"
    )

    # --- Opciones exclusivas de --job ---
    parser.add_argument(
        "--reprocess",
        metavar="SUFFIX",
        help="Solo con --job: reprocesar raw existente por sufijo (ej: 20260312_143052)"
    )

    # --- Utilidades ---
    parser.add_argument(
        "--list",
        action="store_true",
        help="Listar los jobs disponibles"
    )

    args = parser.parse_args()

    # --list
    if args.list:
        jobs = get_available_jobs()
        if jobs:
            print("Jobs disponibles:")
            for job in jobs:
                print(f"  - {job}")
        else:
            print("No se encontraron jobs. Crea uno en src/<nombre>/scraper.py")
        raise SystemExit(0)

    # --reprocess es exclusivo de --job
    if args.reprocess and not args.job:
        parser.error("--reprocess solo puede usarse junto a --job.")

    # --- Despacho segun modo ---

    if args.job:
        scrape_fn, process_fn, validate_fn, settings = _load_job_parts(args.job)
        job_runner.run(args, scrape_fn, process_fn, validate_fn, settings, args.job)

    elif args.pipeline:
        entries, consolidate_config, pipeline_name = _load_pipeline(args.pipeline)
        logger.info(f"Pipeline '{args.pipeline}': {len(entries)} job(s)")
        _run_series(entries, consolidate_config, pipeline_name)

    else:
        parser.error("Especifica un modo de ejecucion: --job o --pipeline. Usa --list para ver los jobs disponibles.")


if __name__ == "__main__":
    main()
