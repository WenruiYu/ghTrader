---
name: system-gpu-info-polish
overview: Improve dashboard System GPU detection so it reliably shows nvidia-smi output on hosts with NVIDIA GPUs, even when PATH differs or the query form fails.
todos:
  - id: harden-gpu-info
    content: Update system_info.gpu_info() to reliably find/run nvidia-smi and surface failures.
    status: completed
  - id: test-gpu-info
    content: Add unit test that simulates nvidia-smi discovery/fallback and asserts /system displays GPU output.
    status: completed
---

# Dashboard System GPU info polish

### Problem

- The System page currently shows `nvidia-smi not available` even though `nvidia-smi` works in the shell.
- Root causes are usually:
- `nvidia-smi` not on the dashboard process PATH, or
- the query-form command failing/timeouting (current code returns `None` on any non-zero exit and hides stderr).

### Proposed changes

- **Harden GPU detection** in [`src/ghtrader/control/system_info.py`](/home/ops/ghTrader/src/ghtrader/control/system_info.py):
- Locate `nvidia-smi` via `shutil.which("nvidia-smi")` and common fallback paths like `/usr/bin/nvidia-smi`.
- Increase timeout slightly (e.g. 5s).
- Try the current CSV query first.
- If the CSV query fails, fall back to running plain `nvidia-smi` and display its stdout.
- If both fail, return a helpful diagnostic string (stderr/returncode/path) so the UI shows *why*.

- **UI rendering remains unchanged** in [`src/ghtrader/control/templates/system.html`](/home/ops/ghTrader/src/ghtrader/control/templates/system.html): it will automatically display the returned string in the `<pre>`.

- **Add a small unit test** (no real GPU required):
- In `tests/test_control_explorer_page.py` (or a new test file), monkeypatch `ghtrader.control.system_info.subprocess.run` + `shutil.which` to simulate:
- PATH-miss but `/usr/bin/nvidia-smi` exists
- query failure but plain `nvidia-smi` success
- Assert `/system` renders the expected GPU output string.

### Validation

- Run `pytest`.
- Restart `ghtrader dashboard` and confirm the System page shows GPU info matching `nvidia-smi` output.