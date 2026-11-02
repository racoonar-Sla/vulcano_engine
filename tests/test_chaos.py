"""
Suite de Simulación de Caos para Proyecto Vulcano (TP-2).
Desarrollado por: Integrante 6 (Ingeniero de Simulación de Caos y Pruebas Forenses).

Responsabilidades:
1. Webhooks con firmas alteradas / claves incorrectas (401/403).
2. Webhooks con timestamps desincronizados (Replay Attacks).
3. Inyección de Mensajes Venenosos a SQS y verificación en DLQ.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# Añadir el directorio raíz del proyecto al sys.path para importaciones absolutas
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# =======================================================================
# CONFIGURACIÓN DEL ENTORNO Y RUTAS
# =======================================================================
DEFAULT_APP = PROJECT_ROOT / "src" / "app_operator.py"


def require(condition: bool, message: str) -> None:
    """Validador estándar para afirmar las condiciones del test."""
    if not condition:
        raise AssertionError(message)


def run_cli(*args: str, timeout_process: float = 15.0) -> subprocess.CompletedProcess[str]:
    """Ejecuta la CLI de Vulcano aislando el proceso."""
    command = [sys.executable, "-m", "src.app_operator"] + list(args)
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout_process,
        check=False,
    )


# =======================================================================
# PRUEBAS DE CAOS (WEBHOOKS)
# =======================================================================
def test_webhook_invalid_signature() -> None:
    """Fuerza un error de firma criptográfica y verifica el rechazo HTTP 401."""
    print("Iniciando prueba: Firma alterada / clave incorrecta (401)...")
    # Inyectamos una clave secreta que no coincide con la del servidor
    result = run_cli(
        "--secret-key", "clave_falsa_muy_corta",
        "start-dispatcher"
    )
    output = f"{result.stdout}\n{result.stderr}".lower()

    # Verificamos que la CLI haya reportado el fallo de validación
    require(
        "signature" in output or "firma" in output or "401" in output,
        f"El sistema no reportó el rechazo 401 por firma inválida.\nSalida: {output}"
    )
    print("[PASS] El servidor bloqueó la firma alterada con 401 Unauthorized.")


def test_webhook_replay_attack() -> None:
    """Fuerza un timestamp antiguo para gatillar WebhookTimestampError."""
    print("Iniciando prueba: Timestamp caducado (Replay Attack)...")

    # Inyectamos una desincronización artificial modificando el tiempo de Python
    # (Para el script CLI forzamos a que construya un timestamp de hace 1 hora)
    import time
    original_time = time.time
    time.time = lambda: original_time() - 3600  # -1 hora

    try:
        from src.app_operator import async_dispatch_webhook
        import asyncio

        # Test unitario directo a la función de despacho del Integrante 5
        payload = {"caos": "replay_attack"}
        try:
            asyncio.run(
                async_dispatch_webhook("http://127.0.0.1:8000/webhook", "vulcano_secret_super_segura", payload)
            )
        except Exception as err:
            err_str = str(type(err)).lower()
            require(
                "timestamp" in err_str or "webhooktimestamperror" in err_str,
                f"El servidor no devolvió WebhookTimestampError. Error obtenido: {err}"
            )
            print("[PASS] Replay Attack neutralizado por WebhookTimestampError.")
            return

        raise AssertionError("El servidor aceptó un webhook de hace 1 hora.")
    finally:
        time.time = original_time


# =======================================================================
# PRUEBA SQS: MENSAJE VENENOSO A DLQ
# =======================================================================
def test_poison_message_dlq() -> None:
    """Envía una carga dañada a SQS y certifica su derivación a la DLQ."""
    print("Iniciando prueba: Mensajes Venenosos a DLQ SQS LocalStack...")
    from src.vulcano_telemetry.core_sqs import get_sqs_client

    client = get_sqs_client()
    queue_url = "http://localhost:4566/000000000000/vulcano-events-queue"
    dlq_url = "http://localhost:4566/000000000000/vulcano-events-dlq"

    # 1. Enviamos un string que NO es JSON para romper el parser
    client.send_message(QueueUrl=queue_url, MessageBody="esto_no_es_un_json_rompe_todo")

    # 2. Ejecutamos el consumidor (que debería leerlo, fallar y NO borrarlo)
    from src.vulcano_telemetry.core_sqs import receive_events
    receive_events(client, queue_url, wait_time=2)

    # 3. Forzamos recepciones adicionales para agotar el maxReceiveCount (3 veces)
    receive_events(client, queue_url, wait_time=1)
    receive_events(client, queue_url, wait_time=1)
    receive_events(client, queue_url, wait_time=1)

    # 4. Verificamos que SQS lo movió automáticamente a la DLQ
    dlq_response = client.receive_message(
        QueueUrl=dlq_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2
    )

    messages = dlq_response.get("Messages", [])
    require(
        len(messages) > 0,
        "El mensaje venenoso no fue derivado a la Dead Letter Queue (DLQ)."
    )
    print("[PASS] Mensaje venenoso (Corrupted Payload) derivado a la DLQ exitosamente.")


# =======================================================================
# ORQUESTADOR
# =======================================================================
def main() -> int:
    """Ejecuta la suite de simulación de caos de Vulcano Pay."""
    parser = argparse.ArgumentParser(description="Suite de Simulación de Caos.")
    parser.parse_args()

    try:
        print("=== VULCANO PAY: INICIANDO SUITE DE CAOS ===")
        test_webhook_invalid_signature()
        test_webhook_replay_attack()
        test_poison_message_dlq()
        print("\nRESULTADO FINAL CAOS: PASS")
        return 0
    except (AssertionError, subprocess.TimeoutExpired) as error:
        print(f"\nRESULTADO FINAL CAOS: FAIL\n{error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
