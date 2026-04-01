import logging
import threading
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Thread-local storage — cada hilo mantiene su propio log path activo.
# Permite que multiples jobs corran en paralelo con archivos de log separados.
# ---------------------------------------------------------------------------

_local = threading.local()
_fh_lock = threading.Lock()
_file_handlers: dict[str, logging.FileHandler] = {}


class _ThreadLocalFileHandler(logging.Handler):
    """
    Handler que enruta cada LogRecord al archivo del hilo actual.

    En vez de escribir a un archivo fijo, consulta _local.log_path en cada
    emit() para saber a que archivo escribir. Esto permite:
    - Modo secuencial: un solo hilo, un solo archivo a la vez.
    - Modo paralelo:   cada hilo escribe en su propio archivo sin interferencias.

    Los FileHandlers abiertos se cachean en _file_handlers y se cierran
    explicitamente al llamar flush_log() al finalizar cada job.
    """

    def emit(self, record: logging.LogRecord) -> None:
        log_path = getattr(_local, "log_path", None)
        if log_path is None:
            return
        key = str(log_path)
        with _fh_lock:
            if key not in _file_handlers:
                fh = logging.FileHandler(log_path, encoding="utf-8")
                fh.setFormatter(self.formatter)
                _file_handlers[key] = fh
        try:
            _file_handlers[key].emit(record)
        except Exception:
            self.handleError(record)


_thread_local_file_handler = _ThreadLocalFileHandler()


# ---------------------------------------------------------------------------
# API publica
# ---------------------------------------------------------------------------


def setup_logger(job_name: str, now: datetime, log_folder: str = "log", level: str = "INFO") -> None:
    """
    Configura el logger "src" para el job indicado. Thread-safe.

    En modo secuencial: cada llamada actualiza _local.log_path al nuevo archivo.
    En modo paralelo:   cada hilo tiene su propio _local.log_path y escribe en
                        su propio archivo sin interferir con otros hilos.

    El enrutamiento al archivo correcto lo realiza _ThreadLocalFileHandler,
    que consulta _local.log_path dinamicamente en cada emit().

    Args:
        job_name:   Nombre del job (se usa para nombrar el archivo de log).
        now:        Timestamp de inicio del run (coherente con raw y output).
        log_folder: Carpeta donde se guardan los logs.
        level:      Nivel de logging (DEBUG, INFO, WARNING, ERROR).
    """
    log_dir = Path(log_folder)
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{job_name}_{now:%Y%m%d_%H%M%S}.log"
    _local.log_path = log_file

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    src_logger = logging.getLogger("src")
    src_logger.setLevel(numeric_level)
    src_logger.propagate = False

    # Instalar los handlers una sola vez (primera llamada).
    # En llamadas sucesivas (modo paralelo o siguiente job secuencial)
    # solo se actualiza _local.log_path — los handlers ya estan en su lugar.
    has_thread_local = any(isinstance(h, _ThreadLocalFileHandler) for h in src_logger.handlers)
    if not has_thread_local:
        for h in src_logger.handlers[:]:
            h.close()
            src_logger.removeHandler(h)

        _thread_local_file_handler.setFormatter(formatter)
        src_logger.addHandler(_thread_local_file_handler)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        src_logger.addHandler(ch)


def get_current_log_path() -> Path | None:
    """Retorna el Path del log activo en el hilo actual. None si no hay log configurado."""
    return getattr(_local, "log_path", None)


def flush_log() -> None:
    """
    Flushea y cierra el FileHandler del hilo actual.

    Debe llamarse al finalizar cada job para:
    1. Garantizar que todos los mensajes esten escritos en disco antes de
       copiar el log a latest/.
    2. Liberar el descriptor de archivo (evita leaks en pipelines largos).

    Despues de flush_log(), get_current_log_path() sigue retornando el path
    para que copy_to_latest() pueda leerlo.
    """
    log_path = getattr(_local, "log_path", None)
    if log_path is None:
        return
    key = str(log_path)
    with _fh_lock:
        fh = _file_handlers.pop(key, None)
    if fh:
        fh.flush()
        fh.close()
