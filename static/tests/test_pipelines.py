import pytest
import yaml
from pathlib import Path

PIPELINES_DIR = Path("config/pipelines")
JOBS_DIR = Path("src")
CONSOLIDADORES_DIR = Path("src/consolidadores")
SUPPORTED_FORMATS = {"csv", "json", "xml", "xlsx"}


def _get_pipeline_files() -> list[Path]:
    return list(PIPELINES_DIR.glob("*.yaml"))


def _get_available_jobs() -> set[str]:
    return {
        entry.name
        for entry in JOBS_DIR.iterdir()
        if entry.is_dir() and (entry / "scraper.py").is_file()
    }


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class TestPipelineYAML:
    """Tests para validar todos los archivos YAML en config/pipelines/"""

    def test_at_least_one_pipeline_exists(self):
        """Verifica que existe al menos un pipeline en config/pipelines/."""
        files = _get_pipeline_files()
        assert len(files) > 0, f"No se encontraron archivos .yaml en {PIPELINES_DIR}"
        print(f"[OK] {len(files)} pipeline(s) encontrado(s): {[f.name for f in files]}")

    def test_pipelines_have_jobs_list(self):
        """Verifica que cada pipeline tiene una clave 'jobs' con una lista no vacia."""
        for path in _get_pipeline_files():
            data = _load(path)
            assert "jobs" in data, f"{path.name}: falta clave 'jobs'"
            assert isinstance(data["jobs"], list), f"{path.name}: 'jobs' debe ser una lista"
            assert len(data["jobs"]) > 0, f"{path.name}: 'jobs' no puede estar vacia"
            print(f"[OK] {path.name}: 'jobs' valido ({len(data['jobs'])} job(s))")

    def test_pipeline_jobs_have_name(self):
        """Verifica que cada job dentro de un pipeline tiene un campo 'name' valido."""
        for path in _get_pipeline_files():
            data = _load(path)
            for i, job in enumerate(data["jobs"]):
                assert "name" in job, f"{path.name}: job[{i}] no tiene campo 'name'"
                assert isinstance(job["name"], str) and job["name"].strip(), \
                    f"{path.name}: job[{i}].name debe ser una cadena no vacia"
            print(f"[OK] {path.name}: todos los jobs tienen 'name'")

    def test_pipeline_job_names_exist_in_src(self):
        """Verifica que los nombres de job del pipeline corresponden a jobs existentes en src/."""
        available = _get_available_jobs()
        for path in _get_pipeline_files():
            data = _load(path)
            for job in data["jobs"]:
                name = job.get("name", "")
                assert name in available, (
                    f"{path.name}: job '{name}' no existe en src/ "
                    f"(disponibles: {sorted(available)})"
                )
            print(f"[OK] {path.name}: todos los jobs existen en src/")

    def test_pipeline_params_are_dicts_if_present(self):
        """Verifica que params, si esta definido, es un dict nativo YAML (no un string)."""
        for path in _get_pipeline_files():
            data = _load(path)
            for job in data["jobs"]:
                if "params" in job and job["params"] is not None:
                    assert isinstance(job["params"], dict), (
                        f"{path.name}: job '{job.get('name')}' — 'params' debe ser un "
                        f"dict nativo YAML, no un string"
                    )
            print(f"[OK] {path.name}: params con formato correcto")

    def test_pipeline_enabled_is_bool_if_present(self):
        """Verifica que enabled, si esta definido, es un booleano."""
        for path in _get_pipeline_files():
            data = _load(path)
            for job in data["jobs"]:
                if "enabled" in job:
                    assert isinstance(job["enabled"], bool), (
                        f"{path.name}: job '{job.get('name')}' — 'enabled' debe ser "
                        f"true o false, no '{job['enabled']}'"
                    )
            print(f"[OK] {path.name}: enabled con formato correcto")

    def test_pipeline_metadata_types_if_present(self):
        """Verifica que name y description del pipeline, si estan definidos, son strings."""
        for path in _get_pipeline_files():
            data = _load(path)
            if "name" in data:
                assert isinstance(data["name"], str) and data["name"].strip(), \
                    f"{path.name}: 'name' del pipeline debe ser una cadena no vacia"
            if "description" in data:
                assert isinstance(data["description"], str), \
                    f"{path.name}: 'description' debe ser una cadena"
            print(f"[OK] {path.name}: metadatos validos")

    def test_consolidate_structure_if_present(self):
        """Verifica la estructura del bloque 'consolidate' cuando esta definido."""
        for path in _get_pipeline_files():
            data = _load(path)
            consolidate = data.get("consolidate")
            if consolidate is None:
                print(f"[OK] {path.name}: sin bloque consolidate (opcional)")
                continue

            assert isinstance(consolidate, dict), \
                f"{path.name}: 'consolidate' debe ser un dict"

            assert "enabled" in consolidate, \
                f"{path.name}: consolidate.enabled es obligatorio"
            assert isinstance(consolidate["enabled"], bool), \
                f"{path.name}: consolidate.enabled debe ser true o false"

            if not consolidate["enabled"]:
                print(f"[OK] {path.name}: consolidate desactivado (enabled: false)")
                continue

            assert "module" in consolidate and isinstance(consolidate["module"], str) and consolidate["module"].strip(), \
                f"{path.name}: consolidate.module debe ser una cadena no vacia cuando enabled es true"

            assert "format" in consolidate, \
                f"{path.name}: consolidate.format es obligatorio cuando enabled es true"
            assert consolidate["format"] in SUPPORTED_FORMATS, \
                f"{path.name}: consolidate.format='{consolidate['format']}' no valido. Soportados: {sorted(SUPPORTED_FORMATS)}"

            if "params" in consolidate and consolidate["params"] is not None:
                assert isinstance(consolidate["params"], dict), \
                    f"{path.name}: consolidate.params debe ser un dict nativo YAML"

            print(f"[OK] {path.name}: bloque consolidate valido")

    def test_consolidate_module_exists_if_enabled(self):
        """Verifica que el modulo consolidador exista en src/consolidadores/ cuando esta activado."""
        for path in _get_pipeline_files():
            data = _load(path)
            consolidate = data.get("consolidate")
            if not consolidate or not consolidate.get("enabled"):
                continue

            module_name = consolidate.get("module", "")
            module_path = CONSOLIDADORES_DIR / f"{module_name}.py"
            assert module_path.is_file(), (
                f"{path.name}: consolidador '{module_name}' no encontrado en "
                f"{CONSOLIDADORES_DIR}/ (esperado: {module_path})"
            )
            print(f"[OK] {path.name}: consolidador '{module_name}' existe")

    def test_consolidate_format_in_all_jobs_if_enabled(self):
        """Verifica que todos los jobs del pipeline incluyan el formato de consolidacion."""
        import importlib
        import sys

        for path in _get_pipeline_files():
            data = _load(path)
            consolidate = data.get("consolidate")
            if not consolidate or not consolidate.get("enabled"):
                continue

            fmt = consolidate.get("format")
            if not fmt:
                continue

            for job in data.get("jobs", []):
                if not job.get("enabled", True):
                    continue
                job_name = job.get("name", "")
                try:
                    settings = importlib.import_module(f"src.{job_name}.settings")
                except ModuleNotFoundError:
                    continue  # test_pipeline_job_names_exist_in_src ya cubre este caso
                output_formats = settings.STORAGE_CONFIG.get("output_formats", ["csv"])
                assert fmt in output_formats, (
                    f"{path.name}: consolidate.format='{fmt}' pero el job '{job_name}' "
                    f"no incluye '{fmt}' en output_formats: {output_formats}"
                )
            print(f"[OK] {path.name}: todos los jobs tienen format='{fmt}' en output_formats")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
