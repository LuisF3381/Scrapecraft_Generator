from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException


def safe_get_text(element, xpath: str, fallback: str = "") -> str:
    """
    Extrae el texto de un sub-elemento dado su XPath relativo.
    Retorna `fallback` si el elemento no existe en lugar de lanzar una excepcion.

    Args:
        element:  Elemento padre desde el que se busca (WebElement de Selenium)
        xpath:    XPath relativo al elemento padre
        fallback: Valor a retornar si el elemento no se encuentra (por defecto "")

    Returns:
        Texto del elemento limpio, o `fallback` si no existe
    """
    try:
        text = element.find_element(By.XPATH, xpath).text
        return text.replace("\n", " | ").strip()
    except NoSuchElementException:
        return fallback


def safe_get_attr(element, xpath: str, attr: str, fallback: str = "") -> str:
    """
    Extrae el valor de un atributo HTML de un sub-elemento dado su XPath relativo.
    Retorna `fallback` si el elemento o el atributo no existe.

    Args:
        element:  Elemento padre desde el que se busca (WebElement de Selenium)
        xpath:    XPath relativo al elemento padre
        attr:     Nombre del atributo HTML a extraer (ej: "title", "class", "href")
        fallback: Valor a retornar si no se encuentra

    Returns:
        Valor del atributo, o `fallback` si no existe
    """
    try:
        value = element.find_element(By.XPATH, xpath).get_attribute(attr)
        return value.strip() if value else fallback
    except NoSuchElementException:
        return fallback
