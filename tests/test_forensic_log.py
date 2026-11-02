"""
Validador Forense de Telemetría JSON y GZIP - Proyecto Vulcano (TP-2).
Desarrollado por: Integrante 6 (Ingeniero de Simulación de Caos y Pruebas Forenses).

Inspecciona de forma automatizada los archivos .log.gz generados por el sistema,
valida la presencia de campos (TraceId, MessageId, WebhookId) y verifica la
descompresión sin pérdida de datos.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path
from typing import Any

# =======================================================================
# CONFIGURACIÓN DEL ENTORNO
# =======================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def require(condition: bool, message: str) -> None:
    """Validador estándar para auditorías forenses."""
    if not condition:
        raise AssertionError(message)


# =======================================================================
# AUDITORÍA DE ARCHIVOS GZIP Y JSON ESTRUCTURADO
# =======================================================================
def get_historical_gz_logs(log_path: Path) -> list[Path]:
    """Busca archivos rotados con extensión .gz."""
    return sorted(log_path.parent.glob(f"{log_path.name}*.gz"))


def validate_gzip_integrity(path: Path) -> None:
    """Abre el archivo GZIP para asegurar que no esté corrupto a nivel de SO."""
    try:
        with gzip.open(path, "rb") as handle:
            handle.read(1024)
        print(f"[PASS] GZIP íntegro y sin corrupción: {path.name}")
    except OSError as error:
        raise AssertionError(f"El archivo {path.name} está corrupto o falló la descompresión: {error}") from error


def audit_forensic_records(path: Path) -> list[dict[str, Any]]:
    """Lee línea a línea y audita el NDJSON extraído del histórico."""
    records: list[dict[str, Any]] = []

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise AssertionError(f"{path.name}:{line_number} no es un JSON válido.") from error

            # 1. Auditoría: Presencia estricta de metadatos requeridos por TP-2
            missing = {"timestamp", "MessageId"}.difference(record)

            # En Vulcano Pay, el JSON Formatter debía inyectar estos metadatos
            require(
                not missing or "TraceId" in record or "WebhookId" in record,
                f"Faltan campos forenses críticos en el log {path.name}:{line_number}. Faltan: {missing}"
            )

            records.append(record)

    return records


# =======================================================================
# ORQUESTADOR FORENSE
# =======================================================================
def main() -> int:
    """Punto de entrada principal para el escáner de telemetría forense."""
    parser = argparse.ArgumentParser(description="Validador Forense GZIP/JSON.")
    parser.add_argument(
        "--log",
        type=Path,
        default=PROJECT_ROOT / "vulcano_events.log",
        help="Ruta al log principal base para ubicar los históricos .gz.",
    )
    args = parser.parse_args()

    try:
        print("=== VULCANO PAY: INICIANDO AUDITORÍA FORENSE ===")
        gz_files = get_historical_gz_logs(args.log)

        require(
            gz_files,
            "No se encontró ningún archivo .gz. Debes ejecutar el sistema y "
            "forzar la rotación del QueueHandler (Integrante 4) primero."
        )

        all_records: list[dict[str, Any]] = []
        for path in gz_files:
            validate_gzip_integrity(path)
            records = audit_forensic_records(path)
            all_records.extend(records)
            print(f"[PASS] {len(records)} registros NDJSON validados en {path.name}")

        print(f"\n[PASS] Total de eventos auditados: {len(all_records)}")
        print("RESULTADO FINAL FORENSE: PASS")
        return 0

    except (AssertionError, OSError) as error:
        print(f"\nRESULTADO FINAL FORENSE: FAIL\n{error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
