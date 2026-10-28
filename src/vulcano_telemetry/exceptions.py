"""
Módulo de Excepciones de Dominio - Proyecto Vulcano (TP-2)
Desarrollado por: Integrante 1 (Ingeniero de Robustez, Excepciones y Sanitización)

Define la jerarquía semántica de errores del sistema respetando la regla de oro
de la cátedra: todas las excepciones heredan estrictamente de Exception.
"""


class VulcanoError(Exception):
    """
    Excepción base para todos los errores del Proyecto Vulcano.
    Garantiza que cualquier fallo de dominio pueda ser capturado de forma unificada.
    """

# ============================================================================
# FAMILIA 1: ERRORES DE MENSAJERÍA Y CONEXIÓN CON SQS (LOCALSTACK)
# ============================================================================


class SQSConnectionError(VulcanoError):
    """
    Se lanza cuando ocurre una falla de red, autenticación o timeout al intentar
    interactuar con el servicio AWS SQS emulado en LocalStack.
    """


class QueueTimeoutError(VulcanoError):
    """
    Se lanza cuando se agota el tiempo de espera configurado al intentar
    recibir o procesar eventos desde la cola SQS.
    """


class CorruptedMessageError(VulcanoError):
    """
    Se lanza cuando un mensaje extraído de SQS está venenosamente corrupto,
    incompleto o no cumple con el formato JSON esperado.
    """

# ============================================================================
# FAMILIA 2: ERRORES DE WEBHOOKS HTTP Y SEGURIDAD CRIPTOGRÁFICA HMAC
# ============================================================================


class WebhookSignatureError(VulcanoError):
    """
    Se lanza cuando la firma HMAC-SHA256 enviada en la cabecera HTTP no
    coincide con la firma calculada localmente sobre los bytes crudos (raw_body).
    """


class WebhookTimestampError(VulcanoError):
    """
    Se lanza cuando la marca de tiempo (timestamp) de la petición HTTP está
    fuera de la ventana de tolerancia permitida, previniendo ataques de retransmisión.
    """


class WebhookDeliveryError(VulcanoError):
    """
    Se lanza cuando el despachador (dispatcher) no logra entregar exitosamente
    un evento al endpoint HTTP del cliente de destino tras agotar los reintentos.
    """
