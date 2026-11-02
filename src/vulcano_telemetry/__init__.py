"""
API Pública del paquete vulcano_telemetry (Patrón Fachada).
Expone de forma centralizada las herramientas del motor de eventos y webhooks
para el Proyecto Vulcano Pay.
"""

# Importaciones relativas internas (usando el punto '.')
from .exceptions import (
    VulcanoError,
    SQSConnectionError,
    CorruptedMessageError,
    QueueTimeoutError,
    WebhookSignatureError,
    WebhookTimestampError,
    WebhookDeliveryError,
)
from .sanitizer import (
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)
from .core_sqs import (
    get_sqs_client,
    produce_event,
    consume_events,
    delete_event,
)
from .webhook_crypto import (
    generate_signature,
    verify_signature,
)
from .logging_engine import (
    setup_vulcano_observability,
    shutdown_vulcano_observability,
)

# Contrato explícito de la API pública
__all__ = [
    # Excepciones
    "VulcanoError",
    "SQSConnectionError",
    "CorruptedMessageError",
    "QueueTimeoutError",
    "WebhookSignatureError",
    "WebhookTimestampError",
    "WebhookDeliveryError",

    # Sanitizadores
    "parse_queue_name",
    "parse_secret_key",
    "parse_wait_time",
    "parse_webhook_url",

    # Mensajería SQS
    "get_sqs_client",
    "produce_event",
    "consume_events",
    "delete_event",

    # Criptografía
    "generate_signature",
    "verify_signature",

    # Observabilidad
    "setup_vulcano_observability",
    "shutdown_vulcano_observability",
]
