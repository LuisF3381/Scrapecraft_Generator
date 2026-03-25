import logging
from seleniumbase import Driver

logger: logging.Logger = logging.getLogger(__name__)


def create_driver(config: dict) -> Driver:
    """
    Crea y retorna un driver de SeleniumBase configurado.

    Args:
        config: Diccionario con las opciones del driver (headless, undetected, maximize,
                window_size, user_agent, proxy)

    Returns:
        Driver: Instancia del driver de SeleniumBase configurada.
    """
    driver_kwargs: dict = {
        "uc": config.get("undetected", True),
        "headless": config.get("headless", False),
    }

    if config.get("user_agent"):
        driver_kwargs["user_agent"] = config["user_agent"]

    if config.get("proxy"):
        driver_kwargs["proxy"] = config["proxy"]

    try:
        driver: Driver = Driver(**driver_kwargs)
    except Exception as e:
        raise RuntimeError(
            f"No se pudo inicializar el driver de SeleniumBase: {e}\n"
            "Verifica que Google Chrome este instalado y que el puerto no este bloqueado."
        ) from e

    logger.info("Driver inicializado correctamente")

    try:
        window_size = config.get("window_size")
        if window_size:
            width, height = window_size
            driver.set_window_size(width, height)
        elif config.get("maximize", True):
            driver.maximize_window()
    except Exception as e:
        driver.quit()
        raise RuntimeError(f"Error al configurar la ventana del driver: {e}") from e

    return driver
