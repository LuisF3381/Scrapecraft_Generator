import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers privados de lectura / escritura (no usar directamente)
# ---------------------------------------------------------------------------

def _write_df(df: pd.DataFrame, filepath: Path, format: str, config: dict, stringify: bool = False) -> None:
    """
    Escribe un DataFrame en el formato indicado usando la config correspondiente.

    Args:
        stringify: Si True, convierte todas las columnas a string antes de escribir.
                   Usar True para raw (preserva exactitud de datos brutos).
                   Usar False para output (preserva tipos numericos, fechas, etc.).
    """
    if stringify:
        df = df.fillna("").astype(str)
    if format == "csv":
        df.to_csv(filepath, index=config.get("index", False), encoding=config.get("encoding", "utf-8"), sep=config.get("separator", ","))
    elif format == "json":
        df.to_json(filepath, orient=config.get("orient", "records"), indent=config.get("indent", 2), force_ascii=config.get("force_ascii", False))
    elif format == "xml":
        df.to_xml(filepath, index=False, root_name=config.get("root", "registros"), row_name=config.get("row", "registro"), encoding=config.get("encoding", "utf-8"))
    elif format == "xlsx":
        df.to_excel(filepath, index=config.get("index", False), sheet_name=config.get("sheet_name", "Datos"))
    else:
        raise ValueError(f"Formato no soportado: {format}")


def _read_df(filepath: Path, format: str, config: dict) -> pd.DataFrame:
    """Lee un archivo en el formato indicado usando la config correspondiente."""
    if format == "csv":
        df = pd.read_csv(filepath, encoding=config.get("encoding", "utf-8"), sep=config.get("separator", ","), dtype=str)
    elif format == "json":
        # dtype=str no es efectivo en read_json (pandas infiere tipos numericos antes de aplicarlo).
        # El .astype(str) posterior es quien garantiza la conversion a string.
        df = pd.read_json(filepath, orient=config.get("orient", "records")).astype(str)
    elif format == "xml":
        df = pd.read_xml(filepath, dtype=str, encoding=config.get("encoding", "utf-8"))
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


def save_data(datos: list[dict], format: str, data_config: dict, storage_config: dict, now: datetime | None = None) -> Path:
    """
    Guarda los datos en el formato y ubicacion especificados.

    Args:
        datos:          Lista de diccionarios con los datos a guardar
        format:         Formato de salida (csv, json, xml, xlsx)
        data_config:    Diccionario con configuraciones de cada formato
        storage_config: Diccionario con configuracion de almacenamiento
        now:            Momento de referencia para el nombre del archivo (ver build_filepath)

    Returns:
        Path: Ruta del archivo guardado
    """
    if format not in data_config:
        raise ValueError(f"Formato no soportado: {format}. Disponibles: {list(data_config.keys())}")

    filepath = build_filepath(storage_config, format, now)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    _write_df(pd.DataFrame(datos), filepath, format, data_config[format])
    logger.info(f"Datos guardados en {filepath} ({len(datos)} registros)")
    return filepath


def save_raw(datos: pd.DataFrame, raw_config: dict, data_config: dict, now: datetime | None = None) -> str:
    """
    Guarda los datos en bruto con sufijo timestamp en el formato indicado por raw_config.

    Args:
        datos:       DataFrame ya construido con los datos a guardar (string-first)
        raw_config:  Diccionario con configuracion del raw
        data_config: Diccionario con configuraciones de formato (DATA_CONFIG)
        now:         Momento de referencia para el sufijo. Si es None se usa datetime.now().
                     Pasar el mismo valor que a save_data() garantiza coherencia entre raw y output.

    Returns:
        str: Sufijo timestamp generado (ej: "20260312_143052")
    """
    raw_folder = Path(raw_config["raw_folder"])
    filename: str = raw_config["filename"]
    format: str = raw_config["format"]

    if format not in data_config:
        raise ValueError(f"Formato raw no soportado: {format}. Disponibles: {list(data_config.keys())}")

    raw_folder.mkdir(parents=True, exist_ok=True)

    now = now or datetime.now()
    suffix: str = now.strftime("%Y%m%d_%H%M%S")
    filepath = raw_folder / f"{filename}_{suffix}.{format}"

    # stringify=False: el DataFrame ya llega normalizado a string desde job_runner
    _write_df(datos, filepath, format, data_config[format], stringify=False)
    logger.info(f"Raw guardado en {filepath} ({len(datos)} registros)")

    return suffix


def load_output(filepath: Path, format: str, data_config: dict) -> pd.DataFrame:
    """
    Lee un archivo de output y lo retorna como DataFrame.
    Usado por el runner para cargar los outputs de cada job antes de consolidar.

    Args:
        filepath:    Ruta del archivo a leer
        format:      Formato del archivo (csv, json, xml, xlsx)
        data_config: Diccionario con configuraciones de formato (DATA_CONFIG)

    Returns:
        pd.DataFrame con el contenido del archivo
    """
    if format not in data_config:
        raise ValueError(f"Formato no soportado: {format}. Disponibles: {list(data_config.keys())}")
    return _read_df(filepath, format, data_config[format])


def load_raw(suffix: str, raw_config: dict, data_config: dict) -> pd.DataFrame:
    """
    Lee un archivo raw y lo retorna como DataFrame sin transformaciones.

    Args:
        suffix:      Sufijo timestamp de la ejecucion (ej: "20260312_143052")
        raw_config:  Diccionario con configuracion del raw
        data_config: Diccionario con configuraciones de formato (DATA_CONFIG)

    Returns:
        pd.DataFrame: Datos del raw sin transformar
    """
    filename: str = raw_config["filename"]
    extension: str = raw_config["format"]
    filepath = Path(raw_config["raw_folder"]) / f"{filename}_{suffix}.{extension}"
    return _read_df(filepath, extension, data_config[extension])


def cleanup_raw(raw_config: dict) -> None:
    """
    Limpia archivos raw segun la politica de retencion configurada.

    Args:
        raw_config: Diccionario con configuracion del raw
    """
    raw_folder = Path(raw_config["raw_folder"])
    filename: str = raw_config["filename"]
    format: str = raw_config["format"]
    retention: dict = raw_config.get("retention", {"mode": "keep_all"})
    mode: str = retention.get("mode", "keep_all")

    if mode == "keep_all":
        return

    if not raw_folder.is_dir():
        return

    files: list[Path] = sorted(
        [
            f for f in raw_folder.iterdir()
            if f.name.startswith(f"{filename}_") and f.suffix == f".{format}"
        ],
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
