# NetDiff-Beam: Apache Beam Networking DB Port-Down Rate Pipeline

A portable, stateful **Apache Beam** pipeline for processing git-like incremental diffs from networking switch databases. It reconstructs chronological database state machines and calculates normalized **Port-Down Rates (`UP -> DOWN`)** per device for specific binary versions within a given query window.

This project has **zero proprietary/internal dependencies** and is fully executable both inside Google environments (`google3`) and on open-source execution engines (e.g., local `DirectRunner`, Apache Flink, or Google Cloud Dataflow).

---

## 📖 Table of Contents
- [Background & Use Case](#-background--use-case)
- [Architecture & Design Principles](#-architecture--design-principles)
- [Data Model & Extensible Schema](#-data-model--extensible-schema)
- [Pipeline Execution Flow](#-pipeline-execution-flow)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [Formula & Computation Logic](#-formula--computation-logic)

---

## 🔍 Background & Use Case

Network devices running SONiC or similar network operating systems continuously log operational state changes to a telemetry database. To optimize bandwidth and storage, state updates are written as **incremental diffs** (similar to Git commits):
1. **Base Snapshot (`is_base_snapshot=True`)**: Represents the complete state from an empty baseline ($t_0$).
2. **Incremental Diffs**: Subsequent transactions contain only changed tables/columns. If a table or column is omitted, it retains its value from the prior snapshot.

### **The Goal**
Compute the **Port-Down Rate (`UP -> DOWN`)** for each switch database (`db_id`), subject to:
* Filtering for a specific target binary/firmware version (`target_version`).
* Evaluating within a user-defined time window `[start_time, end_time]`.
* Normalizing the transition count by the **exact active time** the database ran that `target_version` inside the window.

---

## 🏗️ Architecture & Design Principles

Processing sequential state machine transitions inside a distributed stream/batch engine like Apache Beam poses a challenge: **PCollections are unordered across distributed workers**. 

Our architecture solves this while ensuring maximum efficiency and portability:

```
┌────────────────────────┐
│  Raw Input JSON Lines  │
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│ PruneAndProjectDiffsFn │  <-- Map-side Pruning & Projection
│      (ParDo Stage)     │      Strips unused tables/columns & out-of-window diffs
└───────────┬────────────┘
            │ PCollection[tuple[db_id, _SlimDiff]]
            ▼
┌────────────────────────┐
│      GroupByKey        │  <-- Single Network Shuffle
│    (Keyed by db_id)    │      Groups slim payloads per database
└───────────┬────────────┘
            │ PCollection[tuple[db_id, Iterable[_SlimDiff]]]
            ▼
┌────────────────────────┐
│ ComputeDbPortDownRate  │  <-- Chronological State Replay
│      (ParDo Stage)     │      Sorts timeline, tracks version & port state,
└───────────┬────────────┘      accrues active time & counts UP->DOWN transitions
            │
            ▼
┌────────────────────────┐
│ Output Rate Results    │  <-- Serialized JSON Lines (PortDownRateResult)
└────────────────────────┘
```

### **Core Optimization Strategies**
1. **Map-Side Projection & Filtering Before Shuffle**:
   - Diffs occurring strictly after `end_time` are dropped immediately.
   - Diffs occurring before `start_time` are **retained** to accurately reconstruct state entering the window.
   - Irrelevant tables (`VLAN`, `BGP_NEIGHBOR`, `ROUTE`) and non-essential `PORT` columns (`speed_gbps`, `mtu`, `mac_address`, etc.) are stripped away into a lightweight `_SlimDiff` object, reducing network shuffle payload size by orders of magnitude.
2. **Single-Pass Chronological Replay**:
   - `GroupByKey` groups all historical diffs by `db_id`.
   - Per key, diffs are sorted by `timestamp` (base snapshots applied first on timestamp ties).
   - A single-pass state machine tracks `current_version` and `port_state[port_name]`.

---

## 📊 Data Model & Extensible Schema (`schema.py`)

All models are defined using immutable Python dataclasses (`frozen=True`) for thread safety and portability.

### Key Features & Extensibility
* **`PortStatus`**: Strongly-typed enum (`UP` / `DOWN`) supporting case-insensitive string parsing via `PortStatus.from_raw()`.
* **Dynamic Attribute Capture (`attributes`)**: Any unexpected columns added to known tables (e.g., optical transceiver vendor data) are captured into an `attributes` mapping rather than breaking deserialization.
* **Forward Compatibility (`other_tables`)**: Unmodeled tables in incoming diffs are preserved in an `other_tables` dictionary to maintain forward compatibility.

### Sample Data Payload (`DatabaseDiff` JSON)

```json
{
  "db_id": "switch-tor-01.us-west-1",
  "timestamp": 1772438400.0,
  "is_base_snapshot": false,
  "commit_id": "c8f9d0a1b2c3",
  "parent_commit_id": "a1b2c3d4e5f6",
  "author": "port-health-monitor",
  "switch_role": "TOR",
  "tables": {
    "PORT": [
      {
        "port_name": "Ethernet1/1",
        "status": "DOWN",
        "speed_gbps": 400,
        "attributes": { "transceiver_vendor": "Finisar" }
      }
    ],
    "BINARY_VERSION": {
      "version": "v2.4.1-sonic"
    }
  }
}
```

---

## 📐 Formula & Computation Logic

For a database $D$ over interval $[T_{\text{start}}, T_{\text{end}}]$ with target version $V_{\text{target}}$:

$$\text{Port Down Rate} = \frac{\sum \text{Qualifying } (\text{UP} \rightarrow \text{DOWN} \text{ Transitions})}{\text{Total Active Seconds spent on } V_{\text{target}} \text{ in } [T_{\text{start}}, T_{\text{end}}]}$$

### A Port-Down Transition is Counted IF & ONLY IF:
1. The diff is **not** an initial base snapshot (`is_base_snapshot=False`).
2. The diff timestamp $t_i \in [T_{\text{start}}, T_{\text{end}}]$.
3. The database active version at $t_i$ equals $V_{\text{target}}$.
4. The port's prior state was `UP` and new state is `DOWN` (`port_name` lookup).

### Active Time Window Intersections
Time duration between successive diffs $[t_i, t_{i+1}]$ is clipped against $[T_{\text{start}}, T_{\text{end}}]$ using:

$$\Delta t = \max\Bigl(0,\, \min(t_{i+1}, T_{\text{end}}) - \max(t_i, T_{\text{start}})\Bigr)$$

If `active_time_seconds` $\le 0$, the database is omitted from final output.

---

## 📁 Project Structure

```
.
├── schema.py        # Extensible dataclasses for DatabaseDiff, PortDiff, BinaryVersion, etc.
├── pipeline.py      # Apache Beam PTransform and DoFn implementations
├── pipeline_main.py # Main function to trigger Apache Beam Pipeline
├── pipeline_test.py # Unit test 
├── BUILD
├── README.md        # Documentation and system architecture guide
```

---

## 🚀 Getting Started

### Prerequisites

* Python 3.10+
* `apache-beam`

```bash
pip install apache-beam
```

### Running the Pipeline locally

You can invoke `run_pipeline` directly or run it via a Python entrypoint script:

```python
from pipeline import run_pipeline

run_pipeline(
    input_path="path/to/diffs_*.jsonl",
    output_path="path/to/results",
    target_version="v2.4.1-sonic",
    start_time=1772400000.0,
    end_time=1772486400.0,
    beam_args=["--runner=DirectRunner"]
)
```

### Pipeline Output Sample (`PortDownRateResult`)

```json
{
  "db_id": "switch-tor-01.us-west-1",
  "port_down_rate": 0.000023148,
  "port_down_count": 2,
  "active_time_seconds": 86400.0
}
```
