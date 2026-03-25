# ScrapeCraft Generator

Herramienta para generar proyectos [ScrapeCraft](../Plantilla-Scraping-SeleniumBase) completos desde cero mediante un wizard interactivo.

---

## Requisitos

- Python 3.11+
- Las dependencias del generador son minimas e independientes del proyecto generado:

```bash
pip install -r requirements.txt
```

| Dependencia | Uso |
|---|---|
| `jinja2` | Renderizado de templates por job |
| `pyyaml` | Lectura de `structure.yaml` |

---

## Uso

```bash
python generator.py
```

El script ejecuta un wizard en tres fases:

```
[1] Wizard   → recoge la configuracion paso a paso
[2] Resumen  → muestra el config completo y pide confirmacion
[3] Genera   → crea el proyecto en el destino indicado
```

### Ejemplo de sesion

```
==============================================================
  ScrapeCraft Generator
  Generador de proyectos ScrapeCraft
==============================================================

  [ Proyecto ]
  Nombre del proyecto (sera el nombre de la carpeta): mi_scraper
  Ruta destino donde crear el proyecto [C:/proyectos]:

  [ Jobs ]
  Cantidad de jobs (procesos de scraping): 2
  Nombre del job 1: productos
  Nombre del job 2: precios

  [ Pipeline ]
  Ejecutar los 2 jobs en serie (un unico pipeline con todos)? [S/n]: s
  Con consolidacion de resultados al final? [s/N]: n

==============================================================
  Resumen del proyecto
==============================================================
  Nombre    : mi_scraper
  Destino   : C:/proyectos/mi_scraper
  Jobs (2) :
              - productos
              - precios
  Pipeline  : Serial sin consolidacion
              → config/pipelines/pipeline.yaml
==============================================================

  Generar el proyecto con esta configuracion? [S/n]: s
```

---

## Estructura del proyecto generado

```
mi_scraper/
├── src/
│   ├── main.py                        # CLI dispatcher (framework)
│   ├── shared/                        # Motor ETL (framework, no modificar)
│   │   ├── job_runner.py
│   │   ├── storage.py
│   │   ├── driver_config.py
│   │   ├── logger.py
│   │   └── utils.py
│   ├── productos/                     # Job 1 (zona Data Engineer)
│   │   ├── scraper.py
│   │   ├── utils.py
│   │   ├── process.py
│   │   ├── settings.py
│   │   └── web_config.yaml
│   ├── precios/                       # Job 2 (zona Data Engineer)
│   │   └── ...
│   └── consolidadores/                # Solo si se eligio consolidacion
│       └── consolidador.py
├── config/
│   ├── global_settings.py
│   └── pipelines/
│       └── pipeline.yaml
├── tests/
│   ├── test_global.py
│   ├── test_pipelines.py
│   ├── productos/
│   │   └── test_productos.py
│   └── precios/
│       └── test_precios.py
├── output/                            # Datos procesados (generado en ejecucion)
├── raw/                               # Datos en bruto (generado en ejecucion)
├── log/                               # Logs de ejecucion (generado en ejecucion)
└── requirements.txt
```

---

## Escenarios de pipeline

El wizard genera los archivos de pipeline segun la eleccion del usuario:

| Configuracion | Archivos generados |
|---|---|
| Serial sin consolidacion | `config/pipelines/pipeline.yaml` |
| Serial con consolidacion | `config/pipelines/pipeline_consolidado.yaml` + `src/consolidadores/consolidador.py` |
| No serial (un pipeline por job) | `config/pipelines/<job>.yaml` por cada job |

---

## Zona Data Engineer

Cada job se genera con el **scaffold del framework pre-llenado** y la **logica de negocio vacia**, lista para implementar. Los archivos a completar son:

### `web_config.yaml` — URL y selectores

```yaml
url: ""    # TODO: URL objetivo

selectors:
  container: ""   # TODO: XPath del elemento que se repite por registro
  # Campo1: './/xpath/campo1'
  # Campo2: './/xpath/campo2'

waits:
  reconnect_attempts: 3
  after_load: 3
```

### `scraper.py` — Navegacion y extraccion

El framework inyecta `url`, `selectors` y `waits` automaticamente. El Data Engineer implementa la navegacion y la extraccion:

```python
def scrape(driver, web_config, params=None):
    params   = params or {}
    url      = web_config["url"]        # <- framework lo provee
    selectors = web_config["selectors"] # <- framework lo provee
    waits    = web_config["waits"]      # <- framework lo provee

    # TODO: driver.open(url) / driver.uc_open_with_reconnect(...)
    # TODO: items = driver.find_elements(By.XPATH, selectors["container"])
    # TODO: datos = [parse_record(item, selectors, i) for i, item in enumerate(items, 1)]

    datos: list[dict] = []
    return datos
```

### `utils.py` — Extraccion campo a campo

```python
def parse_record(item, selectors, index):
    registro = {"Numero": index}  # <- framework lo provee como convencion

    # TODO: registro["Campo"] = safe_get_text(item, selectors["Campo"])
    # TODO: registro["Enlace"] = safe_get_attr(item, selectors["Enlace"], "href")

    return registro
```

### `process.py` — Transformaciones y tipado

```python
def process(df):
    df = df.copy()

    # TODO: df["Precio_num"] = df["Precio"].str.replace("€","").astype(float)
    # TODO: df["Cantidad"] = df["Cantidad"].astype(int)

    return df.to_dict(orient="records")
```

### `settings.py` — Configuracion del job

Generado con todos los valores por defecto configurables. Las carpetas de `output/` y `raw/` se asignan automaticamente con el nombre del job:

```python
DRIVER_CONFIG  = { "headless": False, "undetected": True, ... }
STORAGE_CONFIG = { "output_folder": "output/mi_job", "naming_mode": "date_suffix", ... }
RAW_CONFIG     = { "raw_folder": "raw/mi_job", "retention": {"mode": "keep_last_n", "value": 5} }
SKIP_PROCESS   = False
```

---

## Ejecutar el proyecto generado

```bash
cd mi_scraper
pip install -r requirements.txt

# Ejecutar un job individual
python -m src.main --job productos

# Ejecutar el pipeline completo
python -m src.main --pipeline config/pipelines/pipeline.yaml

# Reprocesar raw existente sin re-scrapear
python -m src.main --job productos --reprocess 20260324_143052

# Listar jobs disponibles
python -m src.main --list
```

---

## Ejecutar los tests del proyecto generado

```bash
cd mi_scraper
pytest tests/ -v
```

Los tests validan automaticamente la estructura de configuracion de cada job (settings, web_config, driver) sin necesidad de modificacion.

---

## Estructura interna del generador

```
scrapecraft-generator/
├── generator.py        # Wizard + motor de generacion
├── structure.yaml      # Mapa declarativo de todos los archivos a generar
├── requirements.txt    # Dependencias del generador (jinja2, pyyaml)
├── static/             # Archivos del framework (se copian tal cual)
│   ├── src/
│   │   ├── main.py
│   │   └── shared/
│   ├── config/
│   │   └── global_settings.py
│   └── tests/
│       ├── test_global.py
│       └── test_pipelines.py
└── templates/          # Templates Jinja2 (se renderizan por job/config)
    ├── job/
    │   ├── scraper.py.j2
    │   ├── utils.py.j2
    │   ├── process.py.j2
    │   ├── settings.py.j2
    │   └── web_config.yaml.j2
    ├── pipelines/
    │   ├── pipeline_serial.yaml.j2
    │   ├── pipeline_serial_consolidado.yaml.j2
    │   └── pipeline_single.yaml.j2
    ├── consolidadores/
    │   └── consolidador.py.j2
    └── tests/
        └── test_job.py.j2
```

---

## Como adaptar el generador cuando cambia la plantilla

El generador esta disenado para adaptarse sin tocar `generator.py`. Todos los cambios se hacen en los archivos de contenido o en `structure.yaml`:

| Cambio en la plantilla | Que hacer en el generador |
|---|---|
| Nuevo archivo de framework (igual para todos los proyectos) | Copiar a `static/` + agregar una entrada `type: static` en `structure.yaml` |
| Nuevo archivo por job (varia segun el job) | Crear `.j2` en `templates/job/` + agregar entrada `type: template, repeat: per_job` en `structure.yaml` |
| Cambio en el contenido de un archivo de framework | Editar el archivo en `static/` |
| Cambio en el scaffold de un archivo por job | Editar el `.j2` correspondiente en `templates/job/` |
| Nueva carpeta de datos vacia | Agregar entrada `type: empty` en `structure.yaml` |
| Nueva condicion de generacion (ej: modo debug) | Agregar `condition: nueva_clave` en `structure.yaml` + manejar la clave en `_build_conditions()` de `generator.py` |

### Formato de una entrada en `structure.yaml`

```yaml
- type:      template          # static | template | empty
  src:       job/nuevo.py.j2   # relativo a static/ o templates/ segun el type
  dst:       src/{job_name}/nuevo.py  # relativo a la raiz del proyecto generado
  repeat:    per_job           # opcional: per_job para repetir por cada job
  condition: serial            # opcional: serial | serial_consolidado | not_serial
```
