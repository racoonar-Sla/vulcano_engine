"""
Módulo Sanitizador y Validador de Entradas CLI - Proyecto Vulcano (TP-2)
Desarrollado por: Integrante 1 (Ingeniero de Robustez, Excepciones y Sanitización)

Implementa el patrón de Validación en la Frontera Temprana (Early Boundary Validation).
Garantiza que ningún parámetro corrupto o fuera de rango ingrese a la aplicación.
"""

import argparse
import re
from urllib.parse import urlparse

# Patrón estricto para el nombre de colas SQS: vulcano-[a-z0-9-]+-queue
_QUEUE_NAME_PATTERN = re.compile(r"^vulcano-[a-z0-9-]+-queue$")

WAIT_TIME_MIN = 1
WAIT_TIME_MAX = 20
SECRET_KEY_MIN_LEN = 16


def parse_queue_name(value: str) -> str:
    """
    Valida mediante expresiones regulares que el nombre de la cola SQS cumpla
    con el patrón 'vulcano-[a-z0-9-]+-queue'.
    Lanza argparse.ArgumentTypeError si el formato es inválido.
    """
    clean_val = value.strip() if isinstance(value, str) else ""

    if not _QUEUE_NAME_PATTERN.match(clean_val):
        raise argparse.ArgumentTypeError(
            f"El nombre de la cola '{value}' es inválido. Debe cumplir con el patrón "
            f"'vulcano-[a-z0-9-]+-queue' (ejemplo: 'vulcano-events-queue')."
        )
    return clean_val


def parse_wait_time(value: str) -> int:
    """
    Sanitiza y valida que el tiempo de Long Polling (WaitTimeSeconds)
    sea un entero entre 1 y 20 segundos.
    Lanza argparse.ArgumentTypeError si no es numérico o está fuera de rango.
    """
    try:
        wait_time = int(value)
    except (TypeError, ValueError) as err:
        raise argparse.ArgumentTypeError(
            f"'{value}' no es un número entero válido para el tiempo de espera."
        ) from err

    if not WAIT_TIME_MIN <= wait_time <= WAIT_TIME_MAX:
        raise argparse.ArgumentTypeError(
            f"El tiempo de espera debe estar entre {WAIT_TIME_MIN} y {WAIT_TIME_MAX} "
            f"segundos (recibido: {wait_time})."
        )
    return wait_time


def parse_webhook_url(value: str) -> str:
    """
    Valida mediante urllib.parse que la URL del receptor Webhook sea un esquema
    HTTP o HTTPS válido y contenga un host.
    Lanza argparse.ArgumentTypeError si la URL no es válida.
    """
    clean_url = value.strip() if isinstance(value, str) else ""

    try:
        parsed = urlparse(clean_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("El esquema debe ser 'http' o 'https' y contener un host válido.")
    except Exception as err:
        raise argparse.ArgumentTypeError(
            f"La URL del Webhook '{value}' es inválida: {err}"
        ) from err

    return clean_url


def parse_secret_key(value: str) -> str:
    """
    Valida que la clave secreta compartida HMAC no esté vacía y cumpla
    con la longitud mínima de 16 caracteres.
    Lanza argparse.ArgumentTypeError si no cumple la regla de seguridad.
    """
    clean_key = value.strip() if isinstance(value, str) else ""

    if len(clean_key) < SECRET_KEY_MIN_LEN:
        raise argparse.ArgumentTypeError(
            f"La clave secreta HMAC debe tener al menos {SECRET_KEY_MIN_LEN} caracteres "
            f"(longitud recibida: {len(clean_key)})."
        )
    return clean_key
