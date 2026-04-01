import pytest
from config import global_settings


class TestLogConfig:
    """Tests para validar LOG_CONFIG en global_settings.py"""

    def test_global_settings_has_log_config(self):
        """Verifica que global_settings.py tiene LOG_CONFIG."""
        assert hasattr(global_settings, 'LOG_CONFIG'), "Falta LOG_CONFIG en global_settings.py"
        assert isinstance(global_settings.LOG_CONFIG, dict), "LOG_CONFIG debe ser un diccionario"
        print("[OK] global_settings.py contiene LOG_CONFIG")

    def test_log_config_has_required_keys(self):
        """Verifica que LOG_CONFIG tiene las claves requeridas."""
        required_keys = ["log_folder", "level"]
        for key in required_keys:
            assert key in global_settings.LOG_CONFIG, f"Falta '{key}' en LOG_CONFIG"
        print("[OK] LOG_CONFIG tiene todas las claves requeridas")

    def test_log_config_level_is_valid(self):
        """Verifica que el nivel de logging es valido."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        level = global_settings.LOG_CONFIG["level"].upper()
        assert level in valid_levels, f"Nivel invalido: {level}. Debe ser uno de {valid_levels}"
        print(f"[OK] Nivel de logging valido: {level}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
