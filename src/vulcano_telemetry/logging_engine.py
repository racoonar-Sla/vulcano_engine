"""
Motor de observabilidad asíncrona y telemetría estructurada.
Responsabilidad compartida:
- Formateo JSON estructurado y serialización de excepciones (TP-2).
- Desacoplamiento No Bloqueante (Colas) y Compresión GZIP Idempotente (Integrante 4).
"""

import gzip
import json
import logging
import os
import queue
import sys
import shutil
from datetime import datetime, timezone
from typing import Any, Dict
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener

# ============================================================================
# BLOQUE 1: FORMATEADOR JSON (Adaptado a Vulcano Pay - TP-2)
# ============================================================================


class AsyncJSONFormatter(logging.Formatter):
    """Formateador JSON para telemetría estructurada de Webhooks y SQS."""

    def _serialize_exception(self, exc: BaseException) -> Dict[str, Any]:
        """Estructura recursivamente excepciones, notas dinámicas y causas raíz."""
        exc_data: Dict[str, Any] = {
            "class": exc.__class__.__name__,
            "message": str(exc),
            "notes": getattr(exc, "__notes__", []),
        }

        # Soporte para ExceptionGroup (Python 3.11+)
        if isinstance(exc, ExceptionGroup):
            exc_data["nested_exceptions"] = [
                self._serialize_exception(nested_err)
                for nested_err in exc.exceptions
            ]
        # Soporte para encadenamiento raise ... from
        elif exc.__cause__:
            exc_data["cause"] = self._serialize_exception(exc.__cause__)

        return exc_data

    def format(self, record: logging.LogRecord) -> str:
        # Timestamp ISO 8601 UTC estricto exigido por TP-2
        dt_utc = datetime.fromtimestamp(record.created, tz=timezone.utc)

        log_payload: Dict[str, Any] = {
            "timestamp": dt_utc.isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "thread_name": record.threadName,
        }

        # EXIGENCIA TP-2: Inyección de campos de SQS y Webhooks (si existen en extra)
        for field in ["TraceId", "MessageId", "WebhookId"]:
            if hasattr(record, field):
                log_payload[field] = getattr(record, field)
            elif field in record.__dict__:
                log_payload[field] = record.__dict__[field]

        # Serialización del árbol de excepciones (Incluyendo WebhookTimestampError / SQSConnectionError)
        if record.exc_info:
            _, exc_value, _ = record.exc_info
            if exc_value:
                log_payload["exception_tree"] = self._serialize_exception(exc_value)
                log_payload["stack_trace"] = self.formatException(record.exc_info)

        # Captura dinámica de metadatos inyectados vía 'extra' no reservados
        reserved_fields = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "message", "taskName", "TraceId", "MessageId", "WebhookId"
        }

        for key, value in record.__dict__.items():
            if key not in reserved_fields and not key.startswith("_"):
                log_payload[key] = value

        return json.dumps(log_payload, ensure_ascii=False)


# ============================================================================
# BLOQUE 2: COMPRESIÓN EN CALIENTE GZIP (Responsabilidad del Integrante 4)
# ============================================================================
def gzip_namer(name: str) -> str:
    """Añade la extensión .gz al archivo rotado que alcanzó el límite de tamaño."""
    return name + ".gz"


def gzip_rotator(source: str, dest: str) -> None:
    """
    Comprime el archivo cerrado a GZIP de forma segura y borra el original.
    Garantiza idempotencia eliminando colisiones previas antes de escribir.
    """
    if os.path.exists(dest):
        try:
            os.remove(dest)
        except OSError as err:
            raise OSError(f"No se pudo limpiar colisión previa: {err}") from err

    try:
        with open(source, 'rb') as f_in:
            with gzip.open(dest, 'wb', compresslevel=9) as f_out:
                shutil.copyfileobj(f_in, f_out)

        if os.path.exists(source):
            os.remove(source)
    except Exception as err:
        raise OSError(f"Fallo crítico en compresión: {err}") from err


# Variable global privada que mantendrá vivo al Listener en segundo plano
# pylint: disable=invalid-name
_listener: QueueListener = None
# pylint: enable=invalid-name


# ============================================================================
# BLOQUE 3: PIPELINE ASÍNCRONO NO BLOQUEANTE (Responsabilidad del Integrante 4)
# ============================================================================
def setup_vulcano_observability(log_file="vulcano_events.log", level=logging.INFO) -> logging.Logger:
    """
    Configura el pipeline de logging que aísla la I/O (disco) del hilo principal
    usando una estructura Thread-Safe (queue.Queue).
    """
    # pylint: disable=global-statement
    global _listener

    logger = logging.getLogger("vulcano_engine")
    logger.setLevel(level)
    logger.propagate = False

    json_formatter = AsyncJSONFormatter()

    # 1. CREACIÓN DE LA COLA: Tamaño ilimitado (-1) para no perder eventos
    log_queue = queue.Queue(-1)

    # 2. MANEJADOR DE ENTRADA (Memoria RAM)
    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)

    # 3. MANEJADORES FÍSICOS (Salida): Consola y Archivo Rotativo
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)

    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=2 * 1024 * 1024,  # Límite estricto de 2 MB
        backupCount=3,             # Conserva máximo 3 archivos históricos
        encoding="utf-8"
    )

    file_handler.namer = gzip_namer
    file_handler.rotator = gzip_rotator
    file_handler.setFormatter(json_formatter)

    # 4. EL LISTENER (Hilo en segundo plano)
    _listener = QueueListener(log_queue, console_handler, file_handler, respect_handler_level=True)
    _listener.start()

    return logger


def shutdown_vulcano_observability() -> None:
    """
    Detiene ordenadamente el listener secundario.
    Debe llamarse obligatoriamente en el bloque finally (PEP 765).
    """
    if _listener is not None:
        _listener.stop()
