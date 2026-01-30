# ghTrader Architecture Review and Optimization Report

## 1. Current State Analysis

**Overview**: ghTrader is designed as an ambitious, "AI-first" quantitative trading system targeting SHFE markets. It aims to provide a complete lifecycle from data ingestion (TqSdk) to deep learning model training and execution.

**Key Components**:
*   **Data Layer**: "QuestDB-first" architecture. `main_l5` (Level 5 ticks) is the canonical dataset. Strict requirements for idempotent ingestion and provenance (manifests).
*   **Control Plane**: A heavy FastAPI-based web application (`src/ghtrader/control`) serving a "headless ops dashboard" via SSH. It includes a custom job runner, resource locking, and a full HTML/JS UI for monitoring and manual trading.
*   **Trading Engine**: A decoupled architecture separating `AccountGateway` (OMS/EMS) from `StrategyRunner` (Alpha). Communication is achieved via **file-based IPC** (polling `state.json` and `targets.json` in `runs/`).
*   **Research**: Extensive support for deep learning models (Transformers, DeepLOB) and a complex "Feature Store" design enforcing offline/online parity.

**Codebase Observations**:
*   `src/ghtrader/control/app.py` is massive (>160KB), indicating significant complexity in the UI/Dashboard layer.
*   `src/ghtrader/trading/strategy_runner.py` relies on file polling for inter-process communication, which introduces latency and potential locking issues.
*   The system attempts to solve "production-grade" problems (governance, strict lineage, drift detection) at the cost of high initial complexity.

## 2. Gap Analysis: ghTrader vs. Mature Quant Systems

| Feature | Mature System (e.g., KDB+/C++) | ghTrader (Current) | Gap / Issue |
| :--- | :--- | :--- | :--- |
| **Data Path** | Ticker Plant -> In-Memory RDB -> On-Disk HDB. Zero-copy where possible. | TqSdk -> QuestDB (ILP). | **Good**. QuestDB is a solid choice for the "HDB" equivalent. |
| **Execution IPC** | Shared Memory (Ring Buffers) or low-latency messaging (Aeron, ZMQ). | **File-based Polling** (JSON). | **Critical**. File I/O for high-frequency strategy updates is slow, fragile, and hard to debug (race conditions). |
| **Control Plane** | Lightweight monitoring (Grafana, TUI). Ops via CLI/Scripts. | **Heavy Web App**. Custom HTML/JS, Job Runner, Auth. | **Bloat**. The custom web UI is a massive maintenance burden and unnecessary for a "research-only" system. |
| **Feature Store** | Implicit in Q queries or specialized calc engine. Config-as-Code. | **Registry-heavy**. Strict schema, DB-stored metadata, parity enforcement. | **Over-engineered**. Adds high friction to research iteration. |
| **Live Trading** | Specialized OMS/EMS with hardware acceleration. | Python-based `AccountGateway` with TqSdk. | **Adequate** for research/paper, but the "Live" safety gates are complex for a system not yet trading. |

## 3. Optimization Proposal: The "Lean Research" Architecture

The goal is to strip away the "Enterprise" bloat and focus on the core value loop: **Data -> Alpha -> Execution**.

### 3.1 Architectural Changes

1.  **Kill the Web UI (Control Plane)**
    *   **Proposal**: Remove `src/ghtrader/control/templates`, `static`, and the complex route logic in `app.py`.
    *   **Replacement**:
        *   **CLI**: Enhance `ghtrader status`, `ghtrader jobs`, `ghtrader monitor` (TUI based on `rich` or `textual`).
        *   **Observability**: Expose Prometheus metrics from the Python processes. Use **Grafana** for dashboards (standard industry practice) instead of building a custom HTML dashboard.

2.  **Upgrade IPC to ZeroMQ (Execution)**
    *   **Proposal**: Replace file-based polling (`state.json`/`targets.json`) with **ZeroMQ (PUB/SUB and REQ/REP)**.
    *   **Benefit**: Sub-millisecond latency, no disk I/O bottlenecks, cleaner architecture (Gateway publishes ticks, Strategy subscribes).
    *   **Persistence**: Async logging to disk (JSONL) for audit, but not for the hot path.

3.  **Simplify Feature Store**
    *   **Proposal**: Move from "DB-stored Registry" to **Config-as-Code**.
    *   **Mechanism**: Feature definitions are Python classes/functions versioned in git. The "Feature Store" is just the materialized output in QuestDB. Parity is ensured by sharing the same Python code for offline (batch) and online (streaming) calculation.

4.  **Unify "Research" and "Live" Environments**
    *   **Proposal**: The `StrategyRunner` should be the *exact same process* for Backtest, Paper, and Live.
    *   **Mechanism**: Abstract the `DataProvider` and `ExecutionProvider`. In Backtest, these are mocks/simulators. In Live, they are ZMQ bridges to the Gateway.

## 4. Action Plan

### Phase 1: Decouple and Delete
1.  **Remove UI**: Delete `src/ghtrader/control/templates` and `src/ghtrader/control/static`.
2.  **Slim Down App**: Refactor `src/ghtrader/control/app.py` to be a simple API server (if needed for job orchestration) or just a library for the CLI.
3.  **Clean Dependencies**: Remove web-related dependencies if possible (or keep FastAPI just for the lightweight API).

### Phase 2: High-Performance IPC
1.  **Introduce ZMQ**: Add `pyzmq` dependency.
2.  **Refactor Gateway**: Change `AccountGateway` to PUBLISH market data on a ZMQ socket.
3.  **Refactor Strategy**: Change `StrategyRunner` to SUBSCRIBE to market data and SEND targets via ZMQ REQ/REP.
4.  **Remove File Polling**: Delete the `state.json` read/write loops in the hot path.

### Phase 3: Research Focus
1.  **Focus on `main_l5`**: Ensure the TqSdk -> QuestDB pipeline is rock solid. This is the foundation.
2.  **Standardize Metrics**: Implement the "Standardized JSON reports" for backtests, but keep them simple (PnL, Sharpe, Drawdown).
3.  **Grafana Setup**: Create a docker-compose for Grafana + Prometheus to replace the custom dashboard.

## 5. Summary

The current ghTrader architecture is "Enterprise-Ready" before it is "Research-Proven". It spends too much complexity on governance, UIs, and file-based safety mechanisms. By moving to **CLI + Grafana + ZMQ**, we reduce code volume, increase system performance, and allow researchers to focus on Alpha generation rather than maintaining a web application.
