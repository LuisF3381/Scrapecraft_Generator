import logging
import os
from datetime import datetime
from pathlib import Path


current_log_path: Path | None = None


def setup_logger(job_name: str, now: datetime, log_folder: str = "log", level: str = "INFO") -> None:
    """
    Configura el logger raiz "src" con salida a archivo y consola.
    Debe llamarse una sola vez al inicio del proceso desde app_job.py.
    Los modulos hijos usan logging.getLogger(__name__) y propagan automaticamente.

    Args:
        job_name:   Nombre del job (se usa para nombrar el archivo de log)
        now:        Timestamp de inicio del job (mismo que se usa para raw y output)
        log_folder: Carpeta donde se guardan los logs
        level:      Nivel de logging (DEBUG, INFO, WARNING, ERROR)
    """
    global current_log_path

    os.makedirs(log_folder, exist_ok=True)

    log_file: str = os.path.join(log_folder, f"{job_name}_{now:%Y%m%d_%H%M%S}.log")
    current_log_path = Path(log_file)

    logger: logging.Logger = logging.getLogger("src")
    logger.setLevel(getattr(logging, level.upper()))
    logger.propagate = False

    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    formatter: logging.Formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler: logging.FileHandler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler: logging.StreamHandler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def flush_log() -> None:
    """Vacia los buffers de todos los handlers del logger 'src'."""
    for handler in logging.getLogger("src").handlers:
        handler.flush()
