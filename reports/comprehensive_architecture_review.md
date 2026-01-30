# Comprehensive Architecture Review & Optimization Plan

## 1. Executive Summary

The current `ghTrader` architecture is a **research-first** platform that has been extended to support live/paper trading via a file-based IPC mechanism. While this design is robust for batch jobs and offline research, it is **sub-optimal for live trading** due to high latency (polling + file I/O) and lack of real-time event propagation.

To meet the "tick/HFT granularity" requirement while preserving the Web UI, we propose a **hybrid architecture**:
1.  **Retain QuestDB** as the canonical store for historical data and research.
2.  **Upgrade the Trading Engine** to use **ZeroMQ (ZMQ)** for low-latency IPC.
3.  **Upgrade the Web UI** to use **WebSockets** for real-time updates, bridging the gap between the trading engine and the browser.

---

## 2. Current Architecture Analysis

### 2.1 Component Overview

| Component | Implementation | Communication | Status |
| :--- | :--- | :--- | :--- |
| **Control Plane (UI)** | FastAPI + Jinja2 + HTMX-style polling | Polls JSON files on disk | **Naive** |
| **Gateway** | Python Process (`tqsdk`) | Reads `targets.json`, Writes `state.json` | **High Latency** |
| **Strategy Runner** | Python Process (ML Inference) | Reads `state.json`, Writes `targets.json` | **High Latency** |
| **Data Layer** | QuestDB (via PGWire) | SQL Queries | **Solid** |

### 2.2 Critical Weaknesses

1.  **File-Based IPC (The "Polling Loop of Death")**:
    *   **Mechanism**: Gateway writes `state.json` -> Strategy polls file -> Strategy writes `targets.json` -> Gateway polls file.
    *   **Latency**: With a default 0.5s poll interval, the round-trip time (Tick -> Action) is **> 1.0 second**. This is unacceptable for tick-level strategies.
    *   **Contention**: Frequent atomic writes to the same files (`state.json`) cause lock contention and high disk I/O.

2.  **UI Disconnect**:
    *   The Web UI (`trading.js`) polls the FastAPI backend every 1s.
    *   The FastAPI backend reads the same `state.json` files from disk.
    *   **Result**: The UI is always 1-2 seconds behind reality.

3.  **Concurrency Limits**:
    *   The current `JobManager` and `LockStore` are file-backed (SQLite/JSON). While sufficient for low-frequency ops, they cannot handle high-throughput order management.

---

## 3. Optimization Plan

We propose a 3-phase upgrade to transform `ghTrader` into a low-latency system without rewriting the core logic.

### Phase 1: Low-Latency IPC (The "Nervous System")

**Goal**: Reduce Tick-to-Target latency from >1s to <10ms.

**Implementation**:
*   Replace File I/O with **ZeroMQ (pyzmq)**.
*   **Gateway**:
    *   Binds a `PUB` socket (e.g., `ipc:///tmp/gateway_pub.sock`) to broadcast `MarketTick` and `AccountState` events.
    *   Binds a `PULL` socket (e.g., `ipc:///tmp/gateway_cmd.sock`) to receive `TargetPosition` commands.
*   **Strategy Runner**:
    *   Connects `SUB` to Gateway's `PUB` socket (Event-driven, no polling).
    *   Connects `PUSH` to Gateway's `PULL` socket to send targets immediately.

**Code Changes**:
*   `src/ghtrader/tq/gateway.py`: Add ZMQ context. Replace `write_state()` with `socket.send_json()`.
*   `src/ghtrader/trading/strategy_runner.py`: Replace `read_json()` loop with `socket.recv_json()`.

### Phase 2: Real-Time Web UI (The "Bridge")

**Goal**: Make the UI feel "instant" and eliminate polling.

**Implementation**:
*   **Redis Pub/Sub**: Use Redis as a bridge between the ZMQ backend and the Web UI.
    *   Gateway/Strategy publish state updates to Redis channels (e.g., `updates:account:profile`).
*   **FastAPI WebSockets**:
    *   Add a WebSocket endpoint `/ws/trading`.
    *   On connection, the server subscribes to the relevant Redis channels and pushes updates to the browser.
*   **Frontend**:
    *   Refactor `trading.js` to use `new WebSocket()`.
    *   Replace `setInterval` polling with `ws.onmessage` handlers.

**Code Changes**:
*   `src/ghtrader/control/app.py`: Add `WebSocket` route.
*   `src/ghtrader/control/static/trading.js`: Switch to WebSocket.

### Phase 3: Unified Data Layer (The "Memory")

**Goal**: Ensure data consistency and fast access.

**Implementation**:
*   **Hot State (Redis)**: Store active orders, current positions, and latest ticks in Redis for <1ms access by the UI and Strategy.
*   **Cold State (QuestDB)**: Continue using QuestDB for historical ticks (`main_l5`) and trade logs.
*   **Persistence**: Gateway periodically flushes Redis state to QuestDB for permanent storage.

---

## 4. File-by-File Status & Action Items

| File Path | Current Role | Action Required |
| :--- | :--- | :--- |
| `src/ghtrader/control/app.py` | HTTP API | **Add WebSocket support.** |
| `src/ghtrader/control/static/trading.js` | Polling UI | **Refactor to WebSocket client.** |
| `src/ghtrader/tq/gateway.py` | File Writer | **Integrate ZMQ PUB/PULL.** |
| `src/ghtrader/trading/strategy_runner.py` | File Reader | **Integrate ZMQ SUB/PUSH.** |
| `src/ghtrader/questdb/client.py` | DB Client | No change (Keep for history). |
| `src/ghtrader/util/json_io.py` | File Helper | **Deprecate for hot path.** |

## 5. Conclusion

The "Web UI + File IPC" model is a bottleneck. By introducing **ZeroMQ** for inter-process communication and **WebSockets** for the UI, `ghTrader` can achieve the low latency required for tick-level research and trading, while keeping the user-friendly Control Plane intact.
