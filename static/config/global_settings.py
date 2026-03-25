"""
Configuracion global de ScrapeCraft.
Aplica a todos los procesos independientemente del job que se ejecute.
"""
# =========================================================================
# ZONA DATA ENGINEER — modificar en casos excepcionales
# (encoding, separadores CSV, estructura XML, nivel de log, etc.)
# Los ajustes especificos de cada job van en src/<job>/settings.py
# =========================================================================

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================

LOG_CONFIG = {
    # Carpeta donde se guardan los logs
    "log_folder": "log",

    # Nivel de logging: DEBUG, INFO, WARNING, ERROR
    "level": "INFO"
}

# ============================================
# CONFIGURACIÓN DE DATOS (formatos de exportación)
# ============================================

DATA_CONFIG = {
    # Configuración para CSV
    "csv": {
        "encoding": "utf-8",
        "separator": ";",
        "index": False
    },

    # Configuración para JSON
    "json": {
        "indent": 2,
        "force_ascii": False,
        "orient": "records"
    },

    # Configuración para XML
    "xml": {
        "root": "registros",
        "row": "registro",
        "encoding": "utf-8"
    },

    # Configuración para Excel
    "xlsx": {
        "sheet_name": "Datos",
        "index": False
    }
}
