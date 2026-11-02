"""
Punto de entrada CLI oficial para el operador de telemetría y webhooks de Vulcano.
Responsabilidad: Integrante 5 - Coordinador de Integración, Despachador Webhook y CLI Engine.

Integra los validadores, el despachador HTTP HMAC, el consumidor SQS y el
sistema de logs estructurados en un flujo ordenado y concurrente.
"""

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
import httpx
import uvicorn

# =======================================================================
# IMPORTACIONES DE MÓDULOS DEL EQUIPO (Patrón Fachada / Aislamiento)
# =======================================================================


# =======================================================================
# IMPORTACIONES DE MÓDULOS DEL EQUIPO (Patrón Fachada / Aislamiento)
# =======================================================================
from vulcano_telemetry.exceptions import (
    SQSConnectionError,
    WebhookSignatureError,
    WebhookTimestampError,
)
from vulcano_telemetry.sanitizer import (
    parse_queue_name,
    parse_secret_key,
    parse_wait_time,
    parse_webhook_url,
)
from vulcano_telemetry.core_sqs import get_sqs_client, receive_events, delete_event, produce_event
from vulcano_telemetry.logging_engine import setup_vulcano_observability, shutdown_vulcano_observability

logger = logging.getLogger("vulcano_engine")


# =======================================================================
# LÓGICA DE FIRMA Y DESPACHO HTTP (Responsabilidad del Integrante 5)
# =======================================================================
def generate_signature(secret: str, timestamp: int, payload_bytes: bytes) -> str:
    """Genera una firma HMAC-SHA256 sobre timestamp.payload_bytes."""
    message_to_sign = f"{timestamp}.".encode("utf-8") + payload_bytes
    return hmac.new(
        key=secret.encode("utf-8"),
        msg=message_to_sign,
        digestmod=hashlib.sha256,
    ).hexdigest()


async def async_dispatch_webhook(
    target_url: str,
    secret: str,
    payload_dict: dict
) -> None:
    """Firma un evento y lo envía al destino mediante HTTP POST asíncrono."""
    # Convertimos a JSON sin espacios para garantizar reproducibilidad en la firma
    payload_json = json.dumps(payload_dict, separators=(",", ":"), sort_keys=True)
    payload_bytes = payload_json.encode("utf-8")

    timestamp = int(time.time())
    signature = generate_signature(secret, timestamp, payload_bytes)

    headers = {
        "X-Webhook-Signature": f"t={timestamp},v1={signature}",
        "Content-Type": "application/json"
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(target_url, content=payload_bytes, headers=headers)

    # Validamos respuestas del receptor simulando propagación de errores de negocio
    if response.status_code == 401:
        raise WebhookSignatureError("El receptor HTTP rechazó la firma HMAC del webhook.")
    if response.status_code == 400:
        raise WebhookTimestampError("El receptor HTTP rechazó el timestamp (posible desincronización).")

    response.raise_for_status()
    logger.info("Webhook despachado correctamente al receptor HTTP.", extra={"target_url": target_url})


# =======================================================================
# CONSUMIDOR CONTINUO DE SQS (Long Polling y Despacho)
# =======================================================================
async def async_consume_sqs_event(
    client,
    queue_url: str,
    target_url: str,
    secret: str,
    wait_time: int,
    continuous: bool = False
) -> None:
    """Consume eventos de LocalStack SQS y los despacha por Webhook."""
    while True:
        # Boto3 es bloqueante, lo ejecutamos en un hilo secundario del Event Loop
        messages = await asyncio.to_thread(
            receive_events,
            client,
            queue_url,
            wait_time
        )

        if not messages:
            logger.info("Long Polling agotado. No hay mensajes en SQS.")
            if not continuous:
                break
            continue

        for message in messages:
            body = message["body"]

            # Despachamos el webhook hacia el destino
            await async_dispatch_webhook(target_url, secret, body)

            # Si el webhook fue exitoso (no hubo excepción), borramos el mensaje de SQS
            await asyncio.to_thread(
                delete_event,
                client,
                queue_url,
                message["receipt_handle"],
                message["message_id"],
                message.get("trace_id", "N/A")
            )

        if not continuous:
            break


# =======================================================================
# ORQUESTADOR DE TAREAS Y GRUPOS DE EXCEPCIONES (PEP 654)
# =======================================================================
async def run_dispatcher(
    client,
    queue_url: str,
    target_url: str,
    secret: str,
    wait_time: int,
    continuous: bool = False
) -> None:
    """Coordina tareas concurrentes y captura grupos de errores (except*)."""
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(
                async_consume_sqs_event(
                    client, queue_url, target_url, secret, wait_time, continuous
                )
            )

    except* WebhookSignatureError as errors:
        logger.error("Se detectaron fallos criptográficos (Firmas Rechazadas): %d", len(errors.exceptions))
        for err in errors.exceptions:
            logger.error("Detalle: %s", err)

    except* WebhookTimestampError as errors:
        logger.error("Se detectaron fallos temporales (Replay Attacks o Desincronización): %d", len(errors.exceptions))
        for err in errors.exceptions:
            logger.error("Detalle: %s", err)

    except* SQSConnectionError as errors:
        logger.error("Fallo de infraestructura con AWS SQS LocalStack: %d", len(errors.exceptions))
        for err in errors.exceptions:
            logger.error("Detalle: %s", err)


# =======================================================================
# ARGPARSE Y FLUJO PRINCIPAL
# =======================================================================
def build_parser() -> argparse.ArgumentParser:
    """Construye la interfaz de terminal inyectando los validadores del Integrante 1."""
    parser = argparse.ArgumentParser(
        prog="VulcanoCLI",
        description="Operador de Integración y Motor de Webhooks Vulcano."
    )

    # Validadores inyectados (Integrante 1)
    parser.add_argument(
        "--queue-name",
        type=parse_queue_name,
        default="vulcano-events-queue",
        help="Nombre de la cola SQS."
    )
    parser.add_argument(
        "--wait-time",
        type=parse_wait_time,
        default=10,
        help="Tiempo de Long Polling en SQS (1 a 20 segundos)."
    )
    parser.add_argument(
        "--webhook-url",
        type=parse_webhook_url,
        default="http://127.0.0.1:8000/webhook",
        help="URL destino para enviar los Webhooks."
    )
    parser.add_argument(
        "--secret-key",
        type=parse_secret_key,
        default="vulcano_secret_super_segura",
        help="Clave secreta HMAC compartida."
    )

    # Subcomandos
    subparsers = parser.add_subparsers(dest="command", required=True)

    producer_parser = subparsers.add_parser("produce-sqs", help="Publica un evento manual en SQS LocalStack.")
    producer_parser.add_argument("--event", default="test_event")
    producer_parser.add_argument("--id", type=int, default=999)

    subparsers.add_parser("start-receiver", help="Inicia el servidor receptor FastAPI local.")
    subparsers.add_parser("start-dispatcher", help="Inicia el despachador en modo demonio (consumo continuo).")

    return parser


def main() -> None:
    """Punto de entrada principal del orquestador Vulcano CLI."""
    parser = build_parser()
    args = parser.parse_args()

    # Variables globales compartidas (por ejemplo para el receptor local)
    os.environ["VULCANO_WEBHOOK_SECRET"] = args.secret_key
    queue_full_url = f"http://localhost:4566/000000000000/{args.queue_name}"
    os.environ["VULCANO_QUEUE_URL"] = queue_full_url

    # 1. INICIALIZACIÓN DE OBSERVABILIDAD (Integrante 4)
    setup_vulcano_observability()

    try:
        if args.command == "produce-sqs":
            logger.info("Modo: Publicación Manual SQS")
            client = get_sqs_client()
            payload = {"evento": args.event, "id": args.id}
            msg_id = produce_event(client, queue_full_url, payload)
            logger.info("Mensaje manual publicado exitosamente con ID: %s", msg_id)

        elif args.command == "start-receiver":
            logger.info("Modo: Receptor Webhook HTTP Local")
            # Carga dinámicamente la app FastAPI del Integrante 3
            uvicorn.run("vulcano_telemetry.webhook_receiver:app", host="127.0.0.1", port=8000, reload=False)

        elif args.command == "start-dispatcher":
            logger.info("Modo: Despachador de Eventos (Daemon)")
            client = get_sqs_client()
            asyncio.run(
                run_dispatcher(
                    client,
                    queue_full_url,
                    args.webhook_url,
                    args.secret_key,
                    args.wait_time,
                    continuous=True
                )
            )

    except SQSConnectionError as err:
        logger.critical("Fallo catastrófico en SQS: %s", err)
    except KeyboardInterrupt:
        logger.info("El operador ha cancelado la ejecución (Ctrl+C).")
    finally:
        # 2. LIMPIEZA DETERMINISTA (PEP 765 - Integrante 4)
        logger.info("Iniciando apagado ordenado de Vulcano Engine.")
        shutdown_vulcano_observability()


if __name__ == "__main__":
    main()
