"""Servidor HTTP receptor de webhooks para Vulcano Pay (FastAPI).

El módulo verifica la firma sobre los bytes originales antes de interpretar
el JSON. Después publica el evento en SQS mediante el módulo core_sqs del
Integrante 2.

Configuración por variables de entorno:
    VULCANO_WEBHOOK_SECRET: clave compartida con el emisor.
    VULCANO_QUEUE_URL: URL completa de la cola SQS de LocalStack.
    LOCALSTACK_ENDPOINT: endpoint de LocalStack (por defecto localhost:4566).
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError
from fastapi import FastAPI, HTTPException, Request, status
from starlette.concurrency import run_in_threadpool

from . import core_sqs
from .exceptions import (
    SQSConnectionError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from .webhook_crypto import verify_signature

logger = logging.getLogger("vulcano_engine")

_DEFAULT_QUEUE_URL = (
    "http://localhost:4566/000000000000/vulcano-events-queue"
)
_DEFAULT_LOCALSTACK_ENDPOINT = "http://localhost:4566"


def _resolve_publisher() -> Callable[..., Any]:
    """Obtiene la función de publicación acordada con el Integrante 2.

    La plantilla de referencia llama a produce_event(). La descripción del
    rol del Integrante 2 también menciona send_event(). Se aceptan ambos
    nombres, siempre que reciban (sqs_client, queue_url, event_data).
    """
    publisher = getattr(core_sqs, "produce_event", None)
    if publisher is None:
        publisher = getattr(core_sqs, "send_event", None)
    if not callable(publisher):
        raise RuntimeError(
            "core_sqs.py debe exponer produce_event() o send_event() "
            "con los argumentos (sqs_client, queue_url, event_data)."
        )
    return publisher


def _get_message_id(publish_result: Any) -> str:
    """Normaliza el resultado del productor a un identificador de mensaje."""
    if isinstance(publish_result, str):
        return publish_result or "N/A"
    if isinstance(publish_result, dict):
        value = publish_result.get("MessageId") or publish_result.get("message_id")
        return str(value) if value else "N/A"
    return str(publish_result) if publish_result is not None else "N/A"


def create_app(
    *,
    secret: str | None = None,
    queue_url: str | None = None,
    sqs_client: Any | None = None,
    publish_event: Callable[..., Any] | None = None,
) -> FastAPI:
    """Crea la aplicación FastAPI.

    Los parámetros opcionales permiten probar el receptor sin depender de un
    LocalStack real. En ejecución normal se toman los valores del entorno y
    se utiliza get_sqs_client() del módulo core_sqs.
    """
    app = FastAPI(title="Vulcano Webhook Receiver")
    client = sqs_client

    def get_secret() -> str:
        configured_secret = secret or os.getenv("VULCANO_WEBHOOK_SECRET")
        if not configured_secret:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="El servidor no tiene configurada la clave del webhook.",
            )
        return configured_secret

    def get_queue_url() -> str:
        configured_queue_url = queue_url or os.getenv(
            "VULCANO_QUEUE_URL", _DEFAULT_QUEUE_URL
        )
        if not configured_queue_url:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="El servidor no tiene configurada la cola SQS.",
            )
        return configured_queue_url

    def get_client() -> Any:
        nonlocal client
        if client is None:
            endpoint_url = os.getenv(
                "LOCALSTACK_ENDPOINT", _DEFAULT_LOCALSTACK_ENDPOINT
            )
            client = core_sqs.get_sqs_client(endpoint_url=endpoint_url)
        return client

    def get_publisher() -> Callable[..., Any]:
        return publish_event if publish_event is not None else _resolve_publisher()

    @app.post(
        "/webhook",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def receive_webhook(request: Request) -> dict[str, str]:
        """Autentica, interpreta y encola un webhook entrante."""
        # 1. Leer el cuerpo original. No llamar a request.json() todavía.
        raw_bytes = await request.body()

        # 2. Obtener la firma HTTP enviada por el emisor.
        signature_header = request.headers.get("X-Webhook-Signature", "")

        # 3. Verificar primero la firma y el timestamp, usando los raw bytes.
        try:
            verify_signature(
                get_secret(),
                signature_header,
                raw_bytes,
                tolerance_sec=300,
            )
        except (WebhookSignatureError, WebhookTimestampError) as err:
            logger.warning("Webhook rechazado por validación: %s", err)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Webhook no autenticado.",
            ) from err

        # 4. Solo después de autenticar, interpretar el cuerpo como JSON.
        try:
            payload = json.loads(raw_bytes)
        except (json.JSONDecodeError, UnicodeDecodeError) as err:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El cuerpo recibido no contiene un JSON válido.",
            ) from err

        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El evento debe ser un objeto JSON.",
            )

        # 5. Publicar en SQS. Boto3 es síncrono, por eso se ejecuta en un hilo.
        try:
            publish_result = await run_in_threadpool(
                get_publisher(),
                get_client(),
                get_queue_url(),
                payload,
            )
        except (SQSConnectionError, BotoCoreError) as err:
            logger.error("No se pudo publicar el webhook en SQS: %s", err)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No fue posible encolar el evento.",
            ) from err

        message_id = _get_message_id(publish_result)
        logger.info(
            "Webhook verificado y encolado correctamente.",
            extra={"message_id": message_id},
        )

        # 6. Respondemos 202 solo después de que la publicación tuvo éxito.
        return {
            "status": "accepted",
            "message_id": message_id,
        }

    return app


# Aplicación que Uvicorn importa al ejecutar el servidor.
app = create_app()
