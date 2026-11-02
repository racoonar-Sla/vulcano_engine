"""Funciones de firma y verificación HMAC-SHA256 para Vulcano Pay.

La firma protege la combinación exacta de:
    timestamp + b"." + cuerpo HTTP original (bytes)

Las excepciones de dominio se importan desde exceptions.py, módulo compartido
que debe aportar el Integrante 1.
"""

from __future__ import annotations

import hashlib
import hmac
import string
import time

from .exceptions import WebhookSignatureError, WebhookTimestampError


def generate_signature(secret: str, timestamp: int, payload_bytes: bytes) -> str:
    """Genera una firma HMAC-SHA256 hexadecimal.

    Args:
        secret: clave secreta compartida entre emisor y receptor.
        timestamp: instante de creación del webhook, en segundos Unix.
        payload_bytes: bytes originales del cuerpo HTTP.

    Returns:
        Firma hexadecimal de 64 caracteres.

    Raises:
        TypeError: si los argumentos no tienen los tipos esperados.
        ValueError: si la clave secreta está vacía.
    """
    if not isinstance(secret, str):
        raise TypeError("secret debe ser str.")
    if not secret:
        raise ValueError("La clave secreta HMAC no puede estar vacía.")
    if not isinstance(timestamp, int) or isinstance(timestamp, bool):
        raise TypeError("timestamp debe ser int.")
    if not isinstance(payload_bytes, bytes):
        raise TypeError("payload_bytes debe ser bytes.")

    # Es importante firmar los bytes exactos, sin volver a serializar el JSON.
    message_to_sign = f"{timestamp}.".encode("utf-8") + payload_bytes

    return hmac.new(
        key=secret.encode("utf-8"),
        msg=message_to_sign,
        digestmod=hashlib.sha256,
    ).hexdigest()


def _parse_signature_header(header_value: str) -> tuple[int, str]:
    """Extrae t y v1 de X-Webhook-Signature y valida su formato."""
    if not isinstance(header_value, str) or not header_value.strip():
        raise WebhookSignatureError(
            "La cabecera X-Webhook-Signature está ausente o vacía."
        )

    fields: dict[str, str] = {}
    for item in header_value.split(","):
        key, separator, value = item.strip().partition("=")
        key = key.strip()
        value = value.strip()

        if not separator or not key or not value:
            raise WebhookSignatureError(
                "La cabecera X-Webhook-Signature tiene un formato inválido."
            )
        if key in fields:
            raise WebhookSignatureError(
                f"La cabecera X-Webhook-Signature repite el campo '{key}'."
            )
        fields[key] = value

    timestamp_text = fields.get("t")
    received_signature = fields.get("v1")

    # El formato esperado es t=<segundos Unix>,v1=<64 caracteres hexadecimales>.
    if timestamp_text is None or not timestamp_text.isdecimal():
        raise WebhookSignatureError("El timestamp de la firma no es válido.")
    if received_signature is None:
        raise WebhookSignatureError("La firma v1 no está presente en la cabecera.")
    if len(received_signature) != 64 or any(
        character not in string.hexdigits for character in received_signature
    ):
        raise WebhookSignatureError(
            "La firma HMAC debe contener 64 caracteres hexadecimales."
        )

    return int(timestamp_text), received_signature


def verify_signature(
    secret: str,
    header_value: str,
    payload_bytes: bytes,
    tolerance_sec: int = 300,
) -> bool:
    """Verifica timestamp y firma HMAC-SHA256 de un webhook.

    Devuelve True cuando la firma es válida. Si algo falla, lanza una de las
    excepciones semánticas definidas por el proyecto.

    Args:
        secret: clave secreta compartida.
        header_value: valor de X-Webhook-Signature (t=...,v1=...).
        payload_bytes: cuerpo HTTP original, sin convertir a JSON.
        tolerance_sec: diferencia temporal máxima permitida; por defecto 300 s.

    Raises:
        WebhookSignatureError: cabecera ausente/malformada o firma incorrecta.
        WebhookTimestampError: timestamp fuera de la ventana permitida.
        ValueError: tolerancia temporal inválida.
    """
    if not isinstance(payload_bytes, bytes):
        raise TypeError("payload_bytes debe ser bytes.")
    if not isinstance(tolerance_sec, int) or isinstance(tolerance_sec, bool):
        raise ValueError("tolerance_sec debe ser un número entero.")
    if tolerance_sec < 0:
        raise ValueError("tolerance_sec no puede ser negativo.")

    timestamp, received_signature = _parse_signature_header(header_value)

    # Protección contra Replay Attacks: la petición debe ser reciente.
    current_time = int(time.time())
    time_difference = abs(current_time - timestamp)
    if time_difference > tolerance_sec:
        raise WebhookTimestampError(
            "Timestamp fuera de tolerancia "
            f"({time_difference}s > {tolerance_sec}s)."
        )

    expected_signature = generate_signature(secret, timestamp, payload_bytes)

    # Comparación de tiempo constante para reducir Timing Attacks.
    if not hmac.compare_digest(expected_signature, received_signature):
        raise WebhookSignatureError("La firma HMAC-SHA256 no coincide.")

    return True
