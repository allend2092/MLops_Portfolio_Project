# MLOps Portfolio Project
## Real-World Log Anomaly Detection Using a Multi-GPU Home Lab AI Server

![AI Server](pictures/AI_computer.jpg)

---

## Overview

This project is an end-to-end **MLOps engineering demonstration** built around a realistic production-inspired environment:
my **multi-GPU home-lab AI server**.

Instead of relying on toy datasets, this project collects **real operational logs** from a live machine running:

- Local LLM inference (Ollama, Open-WebUI, HuggingFace GGUF models)
- Dockerized services
- GPU-accelerated workloads
- Linux system daemons
- High-power PSU + multi-fan cooling under varying thermal loads

These logs feed a complete ML-driven **anomaly detection pipeline**, similar to what real-world Data/AI/Platform teams use to monitor production AI services.

---

## Hardware Platform — *The AI Box*

| Component | Details |
|----------|---------|
| **CPU** | Intel Core i7-6950X |
| **Motherboard** | Gigabyte X99P-SLI-CF |
| **Memory** | 32 GB DDR4 |
| **GPUs** | NVIDIA RTX 3090 (24 GB) + RTX 3060 (12 GB) |
| **Total VRAM** | 36 GB |
| **PSU** | 1000W |
| **Cooling** | Dual tower coolers + case fans |
| **Workload** | Local LLM inference and GPU-heavy operations |

---

## Project Goal

Build a **production-style anomaly detection system** capable of identifying:

- abnormal GPU behavior  
- service failures  
- thermal anomalies  
- container restarts  
- unusual sequence patterns in logs  

---

# Current Pipeline Implementation

This repository now implements:

1. Remote ingestion of logs + GPU telemetry  
2. Normalization and preprocessing into a unified schema  
3. Production-style output artifacts: JSONL + Parquet  

---

## Ingestion Pipeline (SSH → AI Box → Raw Logs)

The ingestion runner is:

```bash
python -m pipeline.run_ingestion
```

This command:  
- SSH’s into the AI box (172.16.0.20) using Paramiko  
- Runs:  
```bash
journalctl --output=json          # systemd logs
docker ps + docker logs           # container logs
nvidia-smi --query-gpu=... --format=csv,noheader,nounits   # GPU metrics
```
- Writes raw JSONL artifacts under:  
```bash
data/ingested/systemd/systemd_logs_*.jsonl
data/ingested/docker/docker_logs_*.jsonl
data/ingested/gpu/gpu_metrics_*.jsonl
```
Each record is tagged with:  
- source = systemd | docker | gpu  
- host  
- plus additional metadata (units, container names, GPU fields)  


##  Normalization & Preprocessing (JSONL → Unified Events → Parquet)

Run:  
```bash
python -m src.preprocessing.parser
```

This performs:  

- Timestamp normalization (microseconds → ISO-8601)  
- Schema unification  
- Deduplication-friendly structured format  
  
Produces:  
- data/processed/combined_events.jsonl  
- data/processed/combined_events.parquet  
  
Example Unified Event (structure)
```bash
{
  "timestamp": "2025-12-06T17:20:30.123456+00:00",
  "source": "docker",
  "host": "AI-box",
  "category": "log",
  "subtype": "docker",
  "container_name": "open-webui",
  "message": "...",
}
```

## Quick Data Exploration (from Parquet)

Use pandas to verify everything. View it under the notebook directory.  
![Jupyter Notebook](pictures/jupyter_normalized_log_ingest.png)


## 🏗️ Project Phases (Full Roadmap)

Phase 1 — Log Collection + Exploration✔️ Done  
Phase 2 — Baseline ML Anomaly Detection  
Phase 3 — Training Pipeline  
Phase 4 — Model Registry + Experiment Tracking  
Phase 5 — Deployment (FastAPI + Docker)  
Phase 6 — Monitoring & Drift Detection  
Phase 7 — CI/CD (GitHub Actions)  


```mermaid
flowchart LR

    %% ========= DEV / MLOps NODE =========
    subgraph Dev["Dev / MLOps Node (172.16.0.10)"]
        Jupyter["Jupyter / VS Code / CLI"]
        Repo["Git Repo<br/>MLops_Portfolio_Project"]
        Pipeline["Ingestion Pipeline<br/>python -m pipeline.run_ingestion"]
        Preproc["Preprocessing & Validation<br/>(src/preprocessing)"]
        FeatEng["Feature Engineering<br/>(src/features)"]
        Train["Model Training & Evaluation<br/>(src/models)"]
        InferCode["Inference Service Code (FastAPI)<br/>(src/inference)"]
    end

    %% ========= AI BOX =========
    subgraph AI["AI Box (172.16.0.20)"]
        LLMs["LLM Workloads<br/>(Docker / Open WebUI)"]
        Sysd["systemd Journal<br/>(journalctl)"]
        DockerLogs["Docker Logs<br/>(docker logs)"]
        GPUMetrics["GPU Telemetry<br/>(nvidia-smi)"]
        InferSvc["Deployed Inference Service<br/>(FastAPI container)"]
    end

    %% ========= DATA & ARTIFACTS (ON DEV) =========
    subgraph Data["Data & Artifacts on Dev Node"]
        Raw["Raw Logs<br/>data/ingested/**"]
        Processed["Processed Logs<br/>data/processed/**"]
        Features["Training Features<br/>data/features/**"]
        Models["Model Artifacts<br/>models/**"]
    end

    %% ========= OPS / INTEGRATION =========
    subgraph Ops["Ops / Integration"]
        CI["GitHub Actions CI/CD"]
        Registry["Model Registry / Artifact Store"]
        Monitor["Monitoring & Drift Detection"]
    end

    %% ---- Dev workflow ----
    Jupyter --> Repo
    Repo --> Pipeline
    Repo --> Preproc
    Repo --> FeatEng
    Repo --> Train
    Repo --> InferCode

    %% ---- Ingestion over SSH (Dev -> AI box) ----
    Pipeline -- "SSH" --> Sysd
    Pipeline -- "SSH" --> DockerLogs
    Pipeline -- "SSH" --> GPUMetrics

    %% ---- Workloads generating logs on AI box ----
    LLMs --> Sysd
    LLMs --> DockerLogs
    LLMs --> GPUMetrics

    %% Inference service on AI box also produces logs + GPU usage
    InferSvc --> Sysd
    InferSvc --> DockerLogs
    InferSvc --> GPUMetrics

    %% ---- Data flow on Dev node ----
    Pipeline --> Raw
    Preproc --> Processed
    FeatEng --> Features
    Train --> Models
    Models --> Registry

    %% ---- Inference deployment path ----
    Repo --> CI
    CI --> Train
    CI --> InferSvc

    %% ---- Serving and monitoring loop ----
    Registry --> InferSvc
    InferSvc --> Monitor
    Monitor --> Train
```
