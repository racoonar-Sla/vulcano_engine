# 🚀 Proyecto Vulcano: Sistema de Procesamiento de Eventos, Webhooks HMAC y Observabilidad Asíncrona (Vulcano Pay)

Este repositorio contiene la implementación oficial de **VulcanoEventEngine**, una plataforma financiera de grado industrial diseñada para notificar eventos de pago y liquidaciones a sistemas de terceros en tiempo real mediante **Webhooks HTTP**. 

La arquitectura desacopla la recepción y la emisión de eventos utilizando **AWS SQS** emulado localmente con **LocalStack** en el puerto `4566` (`http://localhost:4566`). El sistema garantiza alta disponibilidad, mitigación de ataques de tiempo (*Timing Attacks*) y retransmisión (*Replay Attacks*), tolerancia a picos masivos de tráfico y desvío automático de mensajes venenosos a la **Dead Letter Queue (DLQ)** (`vulcano-events-dlq`).

---

## 👥 Estructura del Equipo y Distribución de Roles

| Integrante | Rol Técnico | Responsabilidades Principales |
| :--- | :--- | :--- |
| **Integrante 1** | Ingeniero de Robustez, Excepciones y Sanitización - **Rafael Adán Cruz** | Jerarquía semántica de excepciones de dominio (`exceptions.py`) y sanitización declarativa en la frontera CLI con `argparse` (`sanitizer.py`). |
| **Integrante 2** | Ingeniero de Mensajería Asíncrona SQS (Boto3) - **Mauricio Jesús Goroso Romano** | Inicialización del cliente Boto3 en LocalStack (`http://localhost:4566`), productor de eventos (`produce_event`) y consumidor con *Long Polling* (`core_sqs.py`). |
| **Integrante 3** | Ingeniero de Criptografía HMAC y Servidor Receptor - **Roberto Ezequiel Nahuel Choque Martinez** | Verificación y firma criptográfica HMAC-SHA256 sobre bytes crudos (`raw_body`) (`webhook_crypto.py`) y servidor HTTP receptor de Webhooks en `/webhook` (`webhook_receiver.py`). |
| **Integrante 4** | Ingeniero de Almacenamiento y Observabilidad - **Javier Alejandro Quiroga** | Pipeline de logging no bloqueante (`QueueHandler`/`QueueListener`), formateador NDJSON y rotación Gzip idempotente a 2 MB (`logging_engine.py`). |
| **Integrante 5** | Coordinador de Integración y CLI Engine - **Noemí Barragan** | Punto de entrada CLI con subcomandos `argparse`, worker despachador, fachada del paquete (`__init__.py`) y captura `except*` con limpieza PEP 765 (`app_operator.py`). |
| **Integrante 6** | Ingeniero de Simulación de Caos y Pruebas Forenses **Rodrigo Tarqui Gutierrez** | Suite de inyección de caos y firmas corruptas (`test_chaos.py`) y auditoría forense automatizada de logs comprimidos `.gz` (`test_forensic_log.py`). |

---

## 📐 Diagrama de Arquitectura General

```mermaid
graph TD
    subgraph Entrada [1. Frontera CLI & Validaciones]
        A[app_operator.py - CLI Entrypoint] -->|Sanitiza entradas| B[sanitizer.py]
        B -->|Usa excepciones de dominio| C[exceptions.py]
    end

    subgraph Recepcion [2. Recepción Webhook HTTP]
        D[Emisor / Cliente] -->|HTTP POST /webhook| E[webhook_receiver.py]
        E -->|Valida HMAC sobre raw_body & Timestamp| F[webhook_crypto.py]
        E -->|202 Accepted & Encola| G[core_sqs.py - Boto3 SQS]
    end

    subgraph Mensajeria [3. AWS SQS en LocalStack]
        G -->|produce_event| H[(vulcano-events-queue)]
    end

    subgraph Despacho [4. Worker / Dispatcher SQS]
        I[app_operator.py / Dispatcher Worker] -->|consume_events - Long Polling| H
        I -->|Re-firma HMAC-SHA256 & HTTP POST| J[Servidor Cliente / Destino]
        I -.->|Poison Messages / Reintentos agotados| K[(vulcano-events-dlq - DLQ)]
    end

    subgraph Observabilidad [5. Logging Asíncrono NDJSON]
        A & E & I -->|Logs estructurados No Bloqueantes| L[logging_engine.py]
        L -->|QueueHandler en RAM| M[(queue.Queue Thread-Safe)]
        M -->|QueueListener| N[AsyncJSONFormatter]
        N -->|RotatingFileHandler & Gzip| O[triton_services.log.gz]
    end

    style Entrada fill:#f9f9f9,stroke:#333,stroke-width:2px;
    style Recepcion fill:#e8f4f8,stroke:#2980b9,stroke-width:2px;
    style Mensajeria fill:#fff3cd,stroke:#ffc107,stroke-width:2px;
    style Despacho fill:#eafaf1,stroke:#27ae60,stroke-width:2px;
    style Observabilidad fill:#fce4ec,stroke:#e91e63,stroke-width:2px;
```

---
## 📂 Estructura de Directorios del Proyecto

```text
vulcano_engine/
├── src/
│   ├── vulcano_telemetry/
│   │   ├── __init__.py              # Fachada del paquete y definición de __all__
│   │   ├── exceptions.py            # Excepciones de dominio (VulcanoError)
│   │   ├── sanitizer.py             # Validadores declarativos de argparse
│   │   ├── core_sqs.py              # Cliente Boto3 SQS y consumidor resiliente
│   │   ├── webhook_crypto.py        # Generación y verificación HMAC-SHA256
│   │   ├── webhook_receiver.py      # Servidor HTTP receptor de Webhooks
│   │   └── logging_engine.py        # QueueHandler + QueueListener + Gzip
│   └── app_operator.py              # Orquestador CLI principal
├── tests/
│   ├── test_chaos.py                # Suite de simulación de caos
│   └── test_forensic_log.py         # Auditoría forense de logs comprimidos
├── docker-compose.yml               # Configuración de LocalStack (puerto 4566)
├── requirements.txt                 # Dependencias del proyecto
└── README.md                        # Documentación 
```
---
## 🛠️ Requisitos Previos e Instalación

### 1\. Clonar el Repositorio

```
git clone https://github.com/racoonar-Sla/vulcano_engine.git
cd vulcano_engine

```

### 2\. Crear y Activar el Entorno Virtual (`.venv`)


* **En Windows (PowerShell):**

```
python -m venv .venv
.venv\Scripts\Activate.ps1
```
* **En Linux / macOS:**

```
python3 -m venv .venv
source .venv/bin/activate

```
### 3\. Instalar Dependencias

```
pip install --upgrade pip
pip install -r requirements.txt

```

### 4\. Inicializar LocalStack con Docker Desktop

Nos aseguramos de tener **Docker Desktop** activo y ejecutamos:

```
docker compose up -d

```

Verificamos la salud del servicio SQS emulado en: `http://localhost:4566/_localstack/health`

---
## 💻 Comandos de Ejecución y Guía de Pruebas

El programa debe ejecutarse siempre en modo módulo desde la raíz del proyecto para evitar errores de importación y protegernos contra *Local Shadowing*:

### Escenario A: Inicializar el Servidor Receptor de Webhooks

Inicia el servidor HTTP receptor en FastAPI que valida firmas HMAC sobre `raw_body` y responde inmediatamente con `202 Accepted`:

```
python -m src.app_operator start-receiver --secret-key "vulcano_secret_super_segura"

```

### Escenario B: Publicación Manual de Eventos a SQS

Publica un evento estructurado en la cola de LocalStack con atributos de mensaje (`TraceId`, `EventTopic`, `Timestamp`):

```
python -m src.app_operator produce-sqs --queue-name vulcano-events-queue --event payment.processed

```

### Escenario C: Iniciar el Worker Despachador de Eventos (Dispatcher)

Inicia el consumidor con *Long Polling* que re-firma el evento con HMAC y lo despacha al endpoint destino:

```
python -m src.app_operator start-dispatcher --queue-name vulcano-events-queue --wait-time 10

```

### Escenario D: Validación Temprana en la Frontera CLI (Sanitizador)

Prueba la sanitización declarativa de argumentos ingresando un nombre de cola fuera del patrón estricto `vulcano-[a-z0-9-]+-queue`:

```
python -m src.app_operator produce-sqs --queue-name cola_invalida

```

* **Comportamiento esperado**: La CLI aborta inmediatamente lanzando `argparse.ArgumentTypeError` con código de salida `2`.

---

## 🧪 Pruebas Automatizadas SRE (Suite Forense y Caos)

El proyecto incluye una suite automatizada de ingeniería de confiabilidad (SRE) para estrés concurrente y auditoría forense de logs:

### 1\. Simulación de Caos (Webhooks &amp; Poison Messages)

Prueba firmas alteradas (401), desincronización de timestamps (Replay Attacks) y derivación automática de mensajes venenosos a la DLQ (`vulcano-events-dlq`):

```
python tests/test_chaos.py

```

### 2\. Auditoría Forense de Logs GZIP / NDJSON

Descomprime y audita los archivos de log históricos `.gz` verificando que la estructura JSON, las marcas ISO 8601 UTC y los identificadores (`TraceId`, `MessageId`) permanezcan intactos:

```
python tests/test_forensic_log.py

```

---

## 📐 Estándar de Código Limpio y Linters (PEP 8)

Para comprobar de forma automatizada la calidad del software antes de cada entrega:

```
# Análisis estilístico PEP 8 con Flake8
flake8 src/ tests/

# Análisis lógico y puntuación de calidad con Pylint
pylint src/ tests/
```