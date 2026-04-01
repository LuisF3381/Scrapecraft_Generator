#!/usr/bin/env python3
"""
ScrapeCraft Generator
=====================
Genera un proyecto ScrapeCraft completo a partir de un wizard interactivo.

Uso:
    python generator.py

Flujo:
    1. Wizard  — recoge la configuracion del usuario
    2. Resumen — muestra el config y pide confirmacion
    3. Genera  — renderiza templates y copia archivos estaticos

Adaptabilidad:
    - Para agregar/quitar archivos del proyecto generado: editar structure.yaml
    - Para cambiar el contenido de un archivo de framework: editar static/<archivo>
    - Para cambiar el scaffold de un archivo por job: editar templates/job/<archivo>.j2
    - El codigo de este archivo NO necesita modificarse para esos casos.
"""

import os
import re
import shutil
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader

# ---------------------------------------------------------------------------
# Rutas internas del generador
# ---------------------------------------------------------------------------

GENERATOR_DIR  = Path(__file__).parent
STATIC_DIR     = GENERATOR_DIR / "static"
TEMPLATES_DIR  = GENERATOR_DIR / "templates"
STRUCTURE_FILE = GENERATOR_DIR / "structure.yaml"

SEP  = "=" * 62
SEP2 = "-" * 62


# ---------------------------------------------------------------------------
# Helpers de input
# ---------------------------------------------------------------------------

def _ask(prompt: str, default: str = None, valid: list = None) -> str:
    """Solicita un valor al usuario con validacion opcional."""
    suffix = f" [{default}]" if default else ""
    while True:
        raw = input(f"  {prompt}{suffix}: ").strip()
        value = raw if raw else default
        if not value:
            print("    ! Este campo es obligatorio.")
            continue
        if valid and value.lower() not in [v.lower() for v in valid]:
            print(f"    ! Opciones validas: {', '.join(valid)}")
            continue
        return value


def _ask_int(prompt: str, min_val: int = 1) -> int:
    """Solicita un entero con valor minimo."""
    while True:
        raw = input(f"  {prompt}: ").strip()
        try:
            value = int(raw)
            if value < min_val:
                print(f"    ! El valor minimo es {min_val}.")
                continue
            return value
        except ValueError:
            print("    ! Ingresa un numero entero.")


def _ask_yn(prompt: str, default: bool = True) -> bool:
    """Pregunta si/no con valor por defecto."""
    opts = "S/n" if default else "s/N"
    raw = input(f"  {prompt} [{opts}]: ").strip().lower()
    if not raw:
        return default
    return raw in ("s", "si", "y", "yes")


def _slugify(name: str) -> str:
    """Convierte un nombre a snake_case valido para Python."""
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"[^\w]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


# ---------------------------------------------------------------------------
# Wizard
# ---------------------------------------------------------------------------

def run_wizard() -> dict:
    """Ejecuta el wizard interactivo y retorna el diccionario de configuracion."""
    print(f"\n{SEP}")
    print("  ScrapeCraft Generator")
    print("  Generador de proyectos ScrapeCraft")
    print(SEP)

    # -- Proyecto --
    print("\n  [ Proyecto ]")
    project_name = _slugify(_ask("Nombre del proyecto (sera el nombre de la carpeta)"))
    while not project_name:
        print("    ! El nombre no puede estar vacio o contener solo caracteres especiales.")
        project_name = _slugify(_ask("Nombre del proyecto"))

    default_dest = str(GENERATOR_DIR.parent)
    dest_path = _ask("Ruta destino donde crear el proyecto", default=default_dest)

    # -- Jobs --
    print("\n  [ Jobs ]")
    n_jobs = _ask_int("Cantidad de jobs (procesos de scraping)", min_val=1)

    jobs = []
    for i in range(n_jobs):
        raw_name = _ask(f"Nombre del job {i + 1}")
        job_name = _slugify(raw_name)
        while not job_name:
            print("    ! El nombre no puede estar vacio o contener solo caracteres especiales.")
            raw_name = _ask(f"Nombre del job {i + 1}")
            job_name = _slugify(raw_name)
        if raw_name != job_name:
            print(f"    > Nombre normalizado a: '{job_name}'")
        jobs.append(job_name)

    # -- Pipeline --
    print("\n  [ Pipeline ]")
    if n_jobs == 1:
        # Con un solo job la eleccion no cambia el resultado practico,
        # pero se le pregunta igual para que el YAML generado sea coherente.
        serial = _ask_yn("¿Incluir el job en un pipeline?", default=True)
        consolidado = False
        parallel = False
    else:
        serial = _ask_yn(
            f"¿Ejecutar los {n_jobs} jobs en serie (un unico pipeline con todos)?",
            default=True,
        )
        consolidado = False
        parallel = False
        if serial:
            consolidado = _ask_yn("¿Con consolidacion de resultados al final?", default=False)
            parallel = _ask_yn(
                f"¿Ejecutar los {n_jobs} jobs en paralelo (simultaneamente)?",
                default=False,
            )

    return {
        "project_name": project_name,
        "dest_path":    dest_path,
        "jobs":         jobs,
        "serial":       serial,
        "consolidado":  consolidado,
        "parallel":     parallel,
    }


# ---------------------------------------------------------------------------
# Resumen y confirmacion
# ---------------------------------------------------------------------------

def show_summary(config: dict) -> bool:
    """Muestra el resumen del proyecto y retorna True si el usuario confirma."""
    project_root = Path(config["dest_path"]) / config["project_name"]

    print(f"\n{SEP}")
    print("  Resumen del proyecto")
    print(SEP)
    print(f"  Nombre    : {config['project_name']}")
    print(f"  Destino   : {project_root}")
    print(f"  Jobs ({len(config['jobs'])}) :")
    for job in config["jobs"]:
        print(f"              - {job}")

    if config["serial"]:
        modo = "Paralelo" if config["parallel"] else "Serial"
        if config["consolidado"]:
            print(f"  Pipeline  : {modo} con consolidacion")
            print("              → config/pipelines/pipeline_consolidado.yaml")
            print("              → src/consolidadores/consolidador.py")
        else:
            print(f"  Pipeline  : {modo} sin consolidacion")
            print("              → config/pipelines/pipeline.yaml")
    else:
        print("  Pipelines : Individual por job")
        for job in config["jobs"]:
            print(f"              → config/pipelines/{job}.yaml")

    print(SEP)
    return _ask_yn("\n  Generar el proyecto con esta configuracion?", default=True)


# ---------------------------------------------------------------------------
# Motor de generacion
# ---------------------------------------------------------------------------

def _build_conditions(config: dict) -> dict:
    """Construye el mapa de condiciones booleanas para filtrar entradas del structure."""
    serial      = config["serial"]
    consolidado = config["consolidado"]
    return {
        "serial":             serial and not consolidado,
        "serial_consolidado": serial and consolidado,
        "not_serial":         not serial,
    }


def _process_entry(entry: dict, project_root: Path, config: dict, env: Environment, conditions: dict) -> None:
    """Procesa una entrada del structure.yaml y genera el archivo correspondiente."""
    entry_type = entry["type"]
    condition  = entry.get("condition")
    repeat     = entry.get("repeat")

    # Evaluar condicion
    if condition and not conditions.get(condition, False):
        return

    if entry_type == "empty":
        dst = project_root / entry["dst"]
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.touch()
        print(f"  [dir]      {entry['dst']}")

    elif entry_type == "static":
        src = STATIC_DIR / entry["src"]
        dst = project_root / entry["dst"]
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        print(f"  [static]   {entry['dst']}")

    elif entry_type == "template":
        if repeat == "per_job":
            for job_name in config["jobs"]:
                context  = {**config, "job_name": job_name}
                dst_str  = entry["dst"].replace("{job_name}", job_name)
                dst      = project_root / dst_str
                dst.parent.mkdir(parents=True, exist_ok=True)
                content  = env.get_template(entry["src"]).render(**context)
                dst.write_text(content, encoding="utf-8")
                print(f"  [template] {dst_str}")
        else:
            dst     = project_root / entry["dst"]
            dst.parent.mkdir(parents=True, exist_ok=True)
            content = env.get_template(entry["src"]).render(**config)
            dst.write_text(content, encoding="utf-8")
            print(f"  [template] {entry['dst']}")


def generate_project(config: dict) -> None:
    """Lee structure.yaml y genera el proyecto completo en el destino configurado."""
    project_root = Path(config["dest_path"]) / config["project_name"]

    # Verificar existencia previa
    if project_root.exists():
        print(f"\n  ! La carpeta '{project_root}' ya existe.")
        if not _ask_yn("  Sobreescribir?", default=False):
            print("\n  Operacion cancelada.\n")
            return

    print(f"\n{SEP}")
    print("  Generando proyecto...")
    print(SEP2)

    # Cargar mapa de archivos
    with open(STRUCTURE_FILE, "r", encoding="utf-8") as f:
        structure = yaml.safe_load(f)

    # Configurar Jinja2
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False,
    )

    conditions = _build_conditions(config)

    for entry in structure["files"]:
        _process_entry(entry, project_root, config, env, conditions)

    _print_success(project_root, config)


def _print_success(project_root: Path, config: dict) -> None:
    """Muestra el mensaje de exito con los proximos pasos."""
    print(SEP2)
    print(f"\n  Proyecto generado correctamente en:")
    print(f"  {project_root}")
    print(f"\n{SEP}")
    print("  Proximos pasos")
    print(SEP2)
    print("  1. Instalar dependencias:")
    print(f"       cd \"{project_root}\"")
    print("       pip install -r requirements.txt")
    print()
    print("  2. Copiar variables de entorno:")
    print("       cp .env.example .env")
    print()
    print("  3. Verificar la configuracion:")
    print("       python -m pytest tests/ -v")
    print()
    print("  4. Ejecutar:")
    print(f"       python -m src.main --job {config['jobs'][0]}")
    if config["serial"]:
        pipeline = "pipeline_consolidado.yaml" if config["consolidado"] else "pipeline.yaml"
        print(f"       python -m src.main --pipeline config/pipelines/{pipeline}")
        if config["parallel"]:
            print("       (pipeline configurado en modo paralelo — todos los jobs corren a la vez)")
    print()
    print("  Manuales de referencia generados en el proyecto:")
    print("    MANUAL_DATA_ENGINEER.md  — guia para implementar scraper, process y settings")
    print("    MANUAL_GOBERNANZA.md     — guia para implementar validate.py")
    print(SEP)
    print()


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main() -> None:
    config = run_wizard()
    if show_summary(config):
        generate_project(config)
    else:
        print("\n  Operacion cancelada.\n")


if __name__ == "__main__":
    main()
