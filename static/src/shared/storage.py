import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults internos de formato
# Actuan como red de seguridad cuando format_config no esta definido en settings.py.
# En produccion, cada job debe declarar su propio format_config en STORAGE_CONFIG.
# ---------------------------------------------------------------------------

_FORMAT_DEFAULTS: dict[str, dict] = {
    "csv":  {"encoding": "utf-8", "separator": ";", "index": False},
    "json": {"indent": 2, "force_ascii": False, "orient": "records"},
    "xml":  {"root": "registros", "row": "registro", "encoding": "utf-8"},
    "xlsx": {"sheet_name": "Datos", "index": False},
}


def get_format_config(storage_config: dict, format: str) -> dict:
    """
    Retorna la configuracion del formato indicado desde storage_config["format_config"].
    Si no esta definida, usa los defaults internos del framework.
    """
    return storage_config.get("format_config", {}).get(format, _FORMAT_DEFAULTS.get(format, {}))


# ---------------------------------------------------------------------------
# Helpers privados de lectura / escritura (no usar directamente)
# ---------------------------------------------------------------------------

def _write_df(df: pd.DataFrame, filepath: Path, format: str, config: dict, stringify: bool = False) -> None:
    """
    Escribe un DataFrame en el formato indicado usando la config correspondiente.
    Usa escritura atomica: escribe en un .tmp y hace rename al nombre final.
    Garantiza que filepath nunca quede en estado corrupto/incompleto:
    o existe con datos validos, o no existe.

    Args:
        stringify: Si True, convierte todas las columnas a string antes de escribir.
                   Usar True para raw (preserva exactitud de datos brutos).
                   Usar False para output (preserva tipos numericos, fechas, etc.).
    """
    if stringify:
        df = df.fillna("").astype(str)

    tmp_path = filepath.with_suffix(filepath.suffix + ".tmp")
    try:
        if format == "csv":
            df.to_csv(tmp_path, index=config.get("index", False), encoding=config.get("encoding", "utf-8"), sep=config.get("separator", ","))
        elif format == "json":
            df.to_json(tmp_path, orient=config.get("orient", "records"), indent=config.get("indent", 2), force_ascii=config.get("force_ascii", False))
        elif format == "xml":
            df.to_xml(tmp_path, index=False, root_name=config.get("root", "registros"), row_name=config.get("row", "registro"), encoding=config.get("encoding", "utf-8"), parser="etree")
        elif format == "xlsx":
            df.to_excel(tmp_path, index=config.get("index", False), sheet_name=config.get("sheet_name", "Datos"))
        else:
            raise ValueError(f"Formato no soportado: {format}")

        # Rename atomico: reemplaza filepath solo si la escritura fue completa.
        # En Unix/Linux esta operacion es garantizadamente atomica (POSIX rename).
        # En Windows es best-effort pero sigue siendo mucho mas seguro que escribir directo.
        tmp_path.replace(filepath)

    except Exception:
        tmp_path.unlink(missing_ok=True)  # Limpiar el .tmp si algo fallo
        raise


def _read_df(filepath: Path, format: str, config: dict) -> pd.DataFrame:
    """Lee un archivo en el formato indicado usando la config correspondiente."""
    if format == "csv":
        df = pd.read_csv(filepath, encoding=config.get("encoding", "utf-8"), sep=config.get("separator", ","), dtype=str)
    elif format == "json":
        # dtype=str no es efectivo en read_json (pandas infiere tipos numericos antes de aplicarlo).
        # El .astype(str) posterior es quien garantiza la conversion a string.
        df = pd.read_json(filepath, orient=config.get("orient", "records")).astype(str)
    elif format == "xml":
        df = pd.read_xml(filepath, dtype=str, encoding=config.get("encoding", "utf-8"), parser="etree")
    elif format == "xlsx":
        df = pd.read_excel(filepath, dtype=str)
    else:
        raise ValueError(f"Formato no soportado: {format}")
    return df


def _parse_raw_timestamp(filepath: Path) -> datetime | None:
    """Extrae el sufijo YYYYMMDD_HHMMSS del nombre del archivo y lo convierte a datetime."""
    try:
        ts_str = "_".join(filepath.stem.split("_")[-2:])
        return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
    except ValueError:
        logger.warning(f"Archivo con nombre inesperado en raw, ignorando: {filepath.name}")
        return None


# ---------------------------------------------------------------------------
# API publica
# ---------------------------------------------------------------------------

def build_filepath(storage_config: dict, format: str, now: datetime | None = None) -> Path:
    """
    Construye la ruta del archivo segun el modo de nombrado configurado.

    Args:
        storage_config: Diccionario con configuracion de almacenamiento
        format: Formato de salida (csv, json, xml, xlsx)
        now: Momento de referencia para el nombre del archivo. Si es None se usa datetime.now().
             Pasar el mismo valor a todas las llamadas de un mismo run garantiza nombres coherentes.

    Returns:
        Path: Ruta completa del archivo a guardar
    """
    output_folder = Path(storage_config["output_folder"])
    filename: str = storage_config["filename"]
    naming_mode: str = storage_config["naming_mode"]

    now = now or datetime.now()
    date_str: str = now.strftime("%Y%m%d")
    timestamp_str: str = now.strftime("%Y%m%d_%H%M%S")

    if naming_mode == "overwrite":
        filepath = output_folder / f"{filename}.{format}"
    elif naming_mode == "date_suffix":
        filepath = output_folder / f"{filename}_{date_str}.{format}"
    elif naming_mode == "timestamp_suffix":
        filepath = output_folder / f"{filename}_{timestamp_str}.{format}"
    elif naming_mode == "date_folder":
        filepath = output_folder / date_str / f"{filename}.{format}"
    else:
        raise ValueError(f"Modo de nombrado no soportado: {naming_mode}")

    return filepath


def save_data(datos: list[dict], format: str, storage_config: dict, now: datetime | None = None) -> Path:
    """
    Guarda los datos en el formato y ubicacion especificados.
    La config del formato se extrae de storage_config["format_config"].

    Args:
        datos:          Lista de diccionarios con los datos a guardar
        format:         Formato de salida (csv, json, xml, xlsx)
        storage_config: Diccionario con configuracion de almacenamiento (incluye format_config)
        now:            Momento de referencia para el nombre del archivo (ver build_filepath)

    Returns:
        Path: Ruta del archivo guardado
    """
    config = get_format_config(storage_config, format)
    filepath = build_filepath(storage_config, format, now)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    _write_df(pd.DataFrame(datos), filepath, format, config)
    logger.info(f"Datos guardados en {filepath} ({len(datos)} registros)")
    return filepath


def save_raw(datos: pd.DataFrame, storage_config: dict, now: datetime | None = None) -> str:
    """
    Guarda los datos en bruto con sufijo timestamp.
    El formato del raw es output_formats[0]; la config se extrae de format_config.

    Args:
        datos:          DataFrame ya construido con los datos a guardar (string-first)
        storage_config: Diccionario con configuracion de almacenamiento (incluye raw_folder,
                        output_formats y format_config)
        now:            Momento de referencia para el sufijo. Si es None se usa datetime.now().
                        Pasar el mismo valor que a save_data() garantiza coherencia entre raw y output.

    Returns:
        str: Sufijo timestamp generado (ej: "20260312_143052")
    """
    raw_folder = Path(storage_config["raw_folder"])
    filename: str = storage_config["filename"]
    format: str = storage_config["output_formats"][0]
    config = get_format_config(storage_config, format)

    raw_folder.mkdir(parents=True, exist_ok=True)

    now = now or datetime.now()
    suffix: str = now.strftime("%Y%m%d_%H%M%S")
    filepath = raw_folder / f"{filename}_{suffix}.{format}"

    # stringify=False: el DataFrame ya llega normalizado a string desde job_runner
    _write_df(datos, filepath, format, config, stringify=False)
    logger.info(f"Raw guardado en {filepath} ({len(datos)} registros)")

    return suffix


def load_output(filepath: Path, format: str, storage_config: dict) -> pd.DataFrame:
    """
    Lee un archivo de output y lo retorna como DataFrame.
    Usado por el runner para cargar los outputs de cada job antes de consolidar.

    Args:
        filepath:       Ruta del archivo a leer
        format:         Formato del archivo (csv, json, xml, xlsx)
        storage_config: Diccionario con configuracion de almacenamiento del job (incluye format_config)

    Returns:
        pd.DataFrame con el contenido del archivo
    """
    config = get_format_config(storage_config, format)
    return _read_df(filepath, format, config)


def load_raw(suffix: str, storage_config: dict) -> pd.DataFrame:
    """
    Lee un archivo raw y lo retorna como DataFrame sin transformaciones.
    El formato se deriva de output_formats[0].

    Args:
        suffix:         Sufijo timestamp de la ejecucion (ej: "20260312_143052")
        storage_config: Diccionario con configuracion de almacenamiento (incluye raw_folder,
                        output_formats y format_config)

    Returns:
        pd.DataFrame: Datos del raw sin transformar
    """
    filename: str = storage_config["filename"]
    format: str = storage_config["output_formats"][0]
    raw_folder = Path(storage_config["raw_folder"])
    config = get_format_config(storage_config, format)
    filepath = raw_folder / f"{filename}_{suffix}.{format}"
    return _read_df(filepath, format, config)


def cleanup_raw(storage_config: dict) -> None:
    """
    Limpia archivos raw segun la politica de retencion configurada.

    Args:
        storage_config: Diccionario con configuracion de almacenamiento (incluye raw_folder,
                        output_formats y retention)
    """
    raw_folder = Path(storage_config["raw_folder"])
    filename: str = storage_config["filename"]
    format: str = storage_config["output_formats"][0]
    retention: dict = storage_config.get("retention", {"mode": "keep_all"})
    mode: str = retention.get("mode", "keep_all")

    if mode == "keep_all":
        return

    if not raw_folder.is_dir():
        return

    files: list[Path] = sorted(
        raw_folder.glob(f"{filename}_*.{format}"),
        key=lambda f: _parse_raw_timestamp(f) or datetime.min
    )

    if mode == "keep_last_n":
        value: int = retention["value"]
        if value == 0:
            files_to_delete: list[Path] = list(files)
        else:
            files_to_delete = files[:-value] if len(files) > value else []
    elif mode == "keep_days":
        value: int = retention["value"]
        cutoff: datetime = datetime.now() - timedelta(days=value)
        files_to_delete = [f for f in files if (ts := _parse_raw_timestamp(f)) is not None and ts < cutoff]
    else:
        raise ValueError(f"Modo de retencion no soportado: {mode}")

    for filepath in files_to_delete:
        try:
            filepath.unlink()
            logger.info(f"Raw eliminado: {filepath}")
        except OSError as e:
            logger.warning(f"No se pudo eliminar el raw {filepath}: {e}")


# ---------------------------------------------------------------------------
# Latest — espejo de la ultima ejecucion para consumo externo
# ---------------------------------------------------------------------------


def clear_latest(folder: str) -> None:
    """Borra y recrea latest/<folder>/ para garantizar un estado limpio antes de cada ejecucion."""
    latest_path = Path("latest") / folder
    if latest_path.exists():
        shutil.rmtree(latest_path)
    latest_path.mkdir(parents=True)


def copy_to_latest(folder: str, output_paths: dict[str, Path], log_path: Path | None, base_filename: str | None = None) -> None:
    """
    Copia los archivos de output y el log a latest/<folder>/.

    Args:
        folder:        Nombre de la subcarpeta en latest/ (job_name o pipeline_name).
        output_paths:  Mapa formato -> Path del archivo generado. Puede estar vacio si hubo fallo.
        log_path:      Ruta del log de la ejecucion. Se copia siempre como run.log.
        base_filename: Nombre base para renombrar los outputs (ej: "books" -> "books.csv").
                       Si es None se conserva el nombre original del archivo.
    """
    latest_path = Path("latest") / folder
    latest_path.mkdir(parents=True, exist_ok=True)

    for fmt, filepath in output_paths.items():
        if filepath.exists():
            dest_name = f"{base_filename}.{fmt}" if base_filename else filepath.name
            shutil.copy2(filepath, latest_path / dest_name)
            logger.info(f"Latest actualizado: {latest_path / dest_name}")

    if log_path and log_path.exists():
        shutil.copy2(log_path, latest_path / "run.log")
        logger.info(f"Log copiado a latest: {latest_path / 'run.log'}")


def merge_logs_to_latest(folder: str, log_paths: list[Path]) -> None:
    """
    Concatena multiples archivos de log en latest/<folder>/run.log.
    Inserta un separador con el nombre del archivo entre cada seccion.

    Args:
        folder:    Nombre de la subcarpeta en latest/ (pipeline_name).
        log_paths: Lista de rutas de log a concatenar, en orden de ejecucion.
    """
    latest_path = Path("latest") / folder
    latest_path.mkdir(parents=True, exist_ok=True)
    merged_log = latest_path / "run.log"

    with open(merged_log, "w", encoding="utf-8") as out:
        for log_path in log_paths:
            if log_path and log_path.exists():
                out.write(f"{'='*60}\n")
                out.write(f"LOG: {log_path.name}\n")
                out.write(f"{'='*60}\n")
                with open(log_path, "r", encoding="utf-8") as log_file:
                    shutil.copyfileobj(log_file, out)
                out.write("\n")

    logger.info(f"Log consolidado en: {merged_log}")
