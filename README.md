# 🤖 AI Assistant Microservices Architecture

An event-driven, production-ready AI Assistant platform built with **FastAPI**, **LangGraph**, **Apache Kafka (KRaft mode)**, and **Redis**.

This project implements a **Zero-Footprint, High-Performance Separation of Concerns Strategy**: the FastAPI web server handles lightweight database tracking and user connections, while a detached background worker process handles heavy AI graph calculations and LLM processing via **Ollama**.

---

## 🏗️ System Architecture

- **FastAPI App (`main.py`)**: Acts as the ultra-fast I/O traffic router. It drops user inputs into Kafka and streams generated tokens straight out of Redis via Server-Sent Events (SSE). It reads conversation history directly from the SQLite file without compiling machine learning graphs.
- **AI Worker (`worker.py`)**: Consumes prompts from Kafka, orchestrates the LangGraph state machine, interacts with local vector index files, and streams tokens to Redis.
- **Shared Storage**: A Docker-mounted SQLite file (`checkpoints.sqlite`) that keeps LangGraph states securely synced across containers with zero runtime overhead.

---

## ⚙️ Environment Configuration

Create a `.env` file in the root directory of your project:

```env
APP_NAME="Ai Assistant"
PORT=8000
HOST=0.0.0.0
APP_VERSION=1.0.0

# Network routing configurations
KAFKA_BOOTSTARP_SERVER=kafka:29092
KAFKA_PROMPT_REQUEST_TOPIC=llm_prompt_request
KAFKA_PROMPT_RESPONSE_TOPIC=llm_prompt_response
REDIS_URL=redis://redis:6379/0
OLLAMA_BASE_URL=http://host.docker.internal:11434
```

> **Note:** Keep the configuration key `KAFKA_BOOTSTARP_SERVER` spelled exactly as shown to match the internal configuration mapper typo (`BOOTSTARP`).

---

## 🚀 Local Deployment

### 1. Prerequisites

Ensure your local Linux host has Ollama installed, running, and accessible to the Docker bridge network. You can verify it is active by checking the service socket:

```bash
curl http://localhost:11434
```

### 2. Pre-stage Shared Memory

Docker Compose handles file volume bindings strictly. Initialize an empty checkpoint file on your host machine before starting up the containers so it isn't mistaken for a directory:

```bash
touch checkpoints.sqlite
```

### 3. Spin Up the Cluster

Launch your core platform components (FastAPI, Worker, Kafka KRaft, and Redis) concurrently:

```bash
docker-compose up --build -d
```

---

## 🧪 Testing Strategies

The repository is built around a Dual-Mode Testing Infrastructure governed by the `TEST_MODE` environment variable.

### 1. Live Integration Testing (100% Real, Zero Mocks)

To run your test suite against your real, active infrastructure containers without code interception, use Docker to execute your tests inside your live container environment:

```bash
docker-compose run --build -e TEST_MODE=real api pytest -v -s
```

What it does: Bypasses test patches entirely. Your test runner connects straight to your active Kafka broker at `kafka:29092` to process real message handshakes.

### 2. Isolated Verification (Mock Mode)

To run your test suite locally without needing any Docker containers or background services running on your laptop:

```bash
TEST_MODE=mock pytest tests/ -v -s
```

What it does: Simulates Kafka network calls in-memory, allowing you to instantly check code logic, endpoint behaviors, and router paths.

---

## 🔄 CI/CD Automation Pipeline

This project includes a production-grade automated pipeline using GitHub Actions (`.github/workflows/ci-cd.yml`).

Every time you execute a code update to your main branch, the pipeline will automatically:

1. Construct a temporary virtual machine in the cloud.
2. Inject your production environment layout variables.
3. Boot isolated Redis runtime layers.
4. Execute your test collection using `TEST_MODE=mock`.
5. Only if all tests pass: Connect safely to your live Linux deployment server over SSH, pull down the code changes, and execute a zero-downtime rolling restart (`docker-compose up --build -d`).
