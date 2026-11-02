"""
Módulo: core_sqs.py
Responsable: Integrante 2 Mauricio Jesus Goroso Romano (Ingeniero de Mensajería Asíncrona SQS)
Descripción: Cliente Boto3 SQS, productor de eventos, consumidor resiliente con
Long Polling y borrado explícito, apuntando a LocalStack (http://localhost:4566).

Requiere Python 3.11+ (uso de add_note()).
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, ReadTimeoutError

from .exceptions import CorruptedMessageError, QueueTimeoutError, SQSConnectionError

logger = logging.getLogger("vulcano_engine")

DEFAULT_ENDPOINT = "http://localhost:4566"
SQS_MAX_MESSAGES = 10   # límite de SQS por receive_message
SQS_MAX_WAIT_TIME = 20  # límite de SQS para Long Polling (segundos)


def get_sqs_client(endpoint_url: str = DEFAULT_ENDPOINT):
    """Crea el cliente Boto3 SQS configurado para LocalStack.

    El endpoint puede sobrescribirse con la variable SQS_ENDPOINT_URL.
    """
    endpoint_url = os.getenv("SQS_ENDPOINT_URL", endpoint_url)
    config = Config(
        connect_timeout=5,
        # debe ser mayor que el máximo WaitTimeSeconds para no cortar el Long Polling
        read_timeout=SQS_MAX_WAIT_TIME + 5,
        retries={"max_attempts": 3, "mode": "standard"},
    )
    return boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=config,
    )


def create_queue_with_dlq(
    sqs_client,
    queue_name: str = "vulcano-events-queue",
    dlq_name: str = "vulcano-events-dlq",
    max_receive_count: int = 3,
) -> tuple[str, str]:
    """Crea la cola principal y su Dead Letter Queue enlazadas con RedrivePolicy.

    Tras `max_receive_count` recepciones sin borrado, SQS mueve el mensaje a la DLQ.
    Devuelve (queue_url, dlq_url). create_queue es idempotente.
    """
    try:
        dlq_url = sqs_client.create_queue(QueueName=dlq_name)["QueueUrl"]
        dlq_arn = sqs_client.get_queue_attributes(
            QueueUrl=dlq_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        queue_url = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": max_receive_count}
                )
            },
        )["QueueUrl"]
        return queue_url, dlq_url

    except (ClientError, BotoCoreError) as err:
        sqs_err = SQSConnectionError("Error al crear la cola principal o la DLQ en SQS.")
        sqs_err.add_note(f"Queue: {queue_name} | DLQ: {dlq_name}")
        raise sqs_err from err


def send_event(
    sqs_client,
    queue_url: str,
    event_data: dict,
    trace_id: str | None = None,
    event_topic: str = "generic",
) -> str:
    """Publica un evento JSON en SQS con MessageAttributes TraceId, EventTopic y Timestamp.

    Devuelve el MessageId asignado por SQS.
    """
    # SQS rechaza StringValue vacío: normalizamos
    trace_id = (trace_id or "").strip() or str(uuid.uuid4())
    event_topic = (event_topic or "").strip() or "generic"
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(event_data),
            MessageAttributes={
                "TraceId": {"DataType": "String", "StringValue": trace_id},
                "EventTopic": {"DataType": "String", "StringValue": event_topic},
                "Timestamp": {"DataType": "String", "StringValue": timestamp},
            },
        )
        msg_id = response.get("MessageId", "N/A")
        logger.info(
            "Evento publicado en SQS. MessageId: %s",
            msg_id,
            extra={"message_id": msg_id, "trace_id": trace_id},
        )
        return msg_id

    except ReadTimeoutError as err:
        t_err = QueueTimeoutError("Timeout al enviar mensaje a SQS.")
        t_err.add_note(f"QueueUrl: {queue_url}")
        t_err.add_note(f"TraceId: {trace_id}")
        raise t_err from err
    except (ClientError, BotoCoreError, TypeError) as err:
        sqs_err = SQSConnectionError("Error al enviar mensaje a SQS.")
        sqs_err.add_note(f"QueueUrl: {queue_url}")
        sqs_err.add_note(f"TraceId: {trace_id}")
        raise sqs_err from err


def receive_events(
    sqs_client,
    queue_url: str,
    wait_time: int = 10,
    max_messages: int = 5,
    visibility_timeout: int = 30,
) -> list[dict]:
    """Consume mensajes con Long Polling (WaitTimeSeconds) y VisibilityTimeout.

    Devuelve solo los mensajes con JSON válido. Los mensajes veneno se registran
    y NO se borran: al agotar el maxReceiveCount de la RedrivePolicy, SQS los
    deriva automáticamente a la Dead Letter Queue. Esto evita que un mensaje
    corrupto interrumpa el procesamiento del resto del lote.
    """
    max_messages = max(1, min(max_messages, SQS_MAX_MESSAGES))
    wait_time = max(0, min(wait_time, SQS_MAX_WAIT_TIME))

    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            MessageAttributeNames=["All"],
        )
    except ReadTimeoutError as err:
        t_err = QueueTimeoutError("Timeout en Long Polling contra SQS.")
        t_err.add_note(f"QueueUrl: {queue_url}")
        raise t_err from err
    except (ClientError, BotoCoreError) as err:
        sqs_err = SQSConnectionError("Error en recepción de mensajes desde SQS.")
        sqs_err.add_note(f"QueueUrl: {queue_url}")
        raise sqs_err from err

    events = []
    for msg in response.get("Messages", []):
        message_id = msg.get("MessageId", "N/A")
        try:
            body = json.loads(msg["Body"])
        except (json.JSONDecodeError, KeyError, TypeError) as err:
            poison = CorruptedMessageError(f"Mensaje veneno detectado. MessageId: {message_id}")
            poison.add_note("No se borra: SQS lo derivará a la DLQ por RedrivePolicy.")
            poison.__cause__ = err
            logger.error(
                "Mensaje veneno detectado. MessageId: %s",
                message_id,
                exc_info=poison,
                extra={"message_id": message_id},
            )
            continue

        attrs = msg.get("MessageAttributes", {})
        events.append(
            {
                "receipt_handle": msg["ReceiptHandle"],
                "message_id": message_id,
                "body": body,
                "attributes": attrs,
                "trace_id": attrs.get("TraceId", {}).get("StringValue", "N/A"),
            }
        )
    return events


def delete_event(
    sqs_client,
    queue_url: str,
    receipt_handle: str,
    message_id: str = "N/A",
    trace_id: str = "N/A",
) -> None:
    """Elimina explícitamente un mensaje tras ser procesado con éxito."""
    try:
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info(
            "Mensaje eliminado de SQS. MessageId: %s",
            message_id,
            extra={"message_id": message_id, "trace_id": trace_id},
        )
    except (ClientError, BotoCoreError) as err:
        sqs_err = SQSConnectionError("Error al eliminar el mensaje de SQS.")
        sqs_err.add_note(f"ReceiptHandle: {receipt_handle}")
        sqs_err.add_note(f"MessageId: {message_id}")
        raise sqs_err from err


# Alias para compatibilidad con el código de referencia de la cátedra
produce_event = send_event
consume_events = receive_events
