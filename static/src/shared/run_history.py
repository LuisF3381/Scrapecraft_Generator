import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_HISTORY_FOLDER = Path("run_history")


def record_run(
    job_name: str,
    started_at: datetime,
    mode: str,
    status: str,
    raw_suffix: str | None,
    error: str | None,
    duration_s: float,
    outputs: list[str],
) -> None:
    """
    Registra el resultado de una ejecucion en run_history/<job_name>.jsonl.
    Cada linea del archivo es un objeto JSON independiente (formato JSON Lines).

    Args:
        job_name:   Nombre del job ejecutado.
        started_at: Momento de inicio del run.
        mode:       "scrape" para ejecucion normal, "reprocess" para --reprocess.
        status:     "success" si el job termino correctamente, "failed" si lanzo excepcion.
        raw_suffix: Sufijo del archivo raw generado (ej: "20260331_143052").
                    En modo "reprocess" es el sufijo del raw reprocesado.
                    None si el job fallo antes de generar el raw.
        error:      Mensaje de la excepcion capturada. None si status="success".
        duration_s: Duracion total del run en segundos.
        outputs:    Lista de rutas de los archivos de output generados. Vacia si fallo.
    """
    _HISTORY_FOLDER.mkdir(exist_ok=True)

    record = {
        "job":        job_name,
        "started_at": started_at.isoformat(),
        "mode":       mode,
        "status":     status,
        "raw_suffix": raw_suffix,
        "error":      error,
        "duration_s": round(duration_s, 1),
        "outputs":    outputs,
    }

    history_file = _HISTORY_FOLDER / f"{job_name}.jsonl"
    try:
        with open(history_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.debug(f"Historial actualizado: {history_file}")
    except OSError as e:
        # El historial es informativo — un fallo al escribirlo no debe abortar el job.
        logger.warning(f"No se pudo escribir en el historial de runs ({history_file}): {e}")
