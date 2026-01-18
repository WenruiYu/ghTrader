// ghTrader Trading Console (Gateway-first)
// Loaded by templates/trading.html. Uses window.ghTrader helpers from base.html.

(function () {
  const tabs = window.ghTrader.initTabs({ updateHash: true });
  const token = window.ghTrader && window.ghTrader.token ? String(window.ghTrader.token) : "";
  const tokenQs = token ? ("?token=" + encodeURIComponent(token)) : "";

  // Account profiles
  let accountsByProfile = {};
  let selectedAccountProfile = (function () {
    try {
      return String(localStorage.getItem("ghtrader_account_profile") || "default");
    } catch (_) {
      return "default";
    }
  })();

  let liveEnabled = null;

  // Selectors
  const accountProfileSelect = document.getElementById("accountProfileSelect");

  // Accounts form
  const accountUpsertForm = document.getElementById("accountUpsertForm");
  const accountFormProfile = document.getElementById("accountFormProfile");
  const accountFormBroker = document.getElementById("accountFormBroker");
  const accountFormAccountId = document.getElementById("accountFormAccountId");
  const accountFormPassword = document.getElementById("accountFormPassword");
  const accountFormClear = document.getElementById("accountFormClear");
  const brokerIdList = document.getElementById("brokerIdList");

  // Gateway panel
  const gatewayProfileEl = document.getElementById("gatewayProfile");
  const gatewayHealthEl = document.getElementById("gatewayHealth");
  const gatewayUpdatedAtEl = document.getElementById("gatewayUpdatedAt");
  const gatewayRawEl = document.getElementById("gatewayRaw");
  const gatewayRefreshBtn = document.getElementById("gatewayRefreshBtn");
  const gatewayDesiredForm = document.getElementById("gatewayDesiredForm");
  const gatewayModeEl = document.getElementById("gatewayMode");
  const gatewayExecutorEl = document.getElementById("gatewayExecutor");
  const gatewaySimAccountEl = document.getElementById("gatewaySimAccount");
  const gatewaySymbolsEl = document.getElementById("gatewaySymbols");
  const gatewayConfirmLiveEl = document.getElementById("gatewayConfirmLive");
  const gatewayMaxAbsPosEl = document.getElementById("gatewayMaxAbsPos");
  const gatewayMaxOrderSizeEl = document.getElementById("gatewayMaxOrderSize");
  const gatewayMaxOpsSecEl = document.getElementById("gatewayMaxOpsSec");
  const gatewayMaxDailyLossEl = document.getElementById("gatewayMaxDailyLoss");
  const gatewayEnforceTradingTimeEl = document.getElementById("gatewayEnforceTradingTime");
  const gatewayCmdCancelAll = document.getElementById("gatewayCmdCancelAll");
  const gatewayCmdFlatten = document.getElementById("gatewayCmdFlatten");
  const gatewayCmdDisarm = document.getElementById("gatewayCmdDisarm");

  // Strategy panel
  const strategyProfileEl = document.getElementById("strategyProfile");
  const strategyHealthEl = document.getElementById("strategyHealth");
  const strategyUpdatedAtEl = document.getElementById("strategyUpdatedAt");
  const strategyRawEl = document.getElementById("strategyRaw");
  const strategyRefreshBtn = document.getElementById("strategyRefreshBtn");
  const strategyDesiredForm = document.getElementById("strategyDesiredForm");
  const strategyModeEl = document.getElementById("strategyMode");
  const strategySymbolsEl = document.getElementById("strategySymbols");
  const strategyModelEl = document.getElementById("strategyModelName");
  const strategyHorizonEl = document.getElementById("strategyHorizon");
  const strategyThresholdUpEl = document.getElementById("strategyThresholdUp");
  const strategyThresholdDownEl = document.getElementById("strategyThresholdDown");
  const strategyPositionSizeEl = document.getElementById("strategyPositionSize");
  const strategyArtifactsDirEl = document.getElementById("strategyArtifactsDir");
  const strategyPollIntervalEl = document.getElementById("strategyPollIntervalSec");

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function fmtTs(ts) {
    if (!ts) return "--";
    try {
      return new Date(ts).toLocaleString();
    } catch (_) {
      return String(ts);
    }
  }

  function formatNumber(n) {
    if (n === null || n === undefined) return "--";
    const x = Number(n);
    if (!Number.isFinite(x)) return "--";
    return x.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function parseCsvSymbols(s) {
    return String(s || "")
      .split(",")
      .map((x) => String(x || "").trim())
      .filter((x) => x);
  }

  function setText(el, txt) {
    if (!el) return;
    el.textContent = String(txt || "--");
  }

  function modeDotClass(mode) {
    const m = String(mode || "idle").toLowerCase();
    if (m === "live_trade" || m === "live") return "status-dot-error";
    if (m === "sim" || m === "live_monitor") return "status-dot-warn";
    if (m === "paper") return "status-dot-ok";
    return "status-dot-idle";
  }

  function setSelectedAccountProfile(p) {
    selectedAccountProfile = String(p || "default").trim() || "default";
    try { localStorage.setItem("ghtrader_account_profile", selectedAccountProfile); } catch (_) {}
    if (accountProfileSelect) accountProfileSelect.value = selectedAccountProfile;
  }

  function updateAccountProfileStatus() {
    const row = accountsByProfile[selectedAccountProfile];
    const cfg = row ? (row.configured === true) : null;
    const el = document.getElementById("accountProfileConfigured");
    if (el) {
      if (cfg === null) el.textContent = "--";
      else el.textContent = cfg ? "configured" : "missing creds";
    }
    const acctEl = document.getElementById("tradingAccountIndicator");
    if (acctEl) {
      const dot = acctEl.querySelector(".status-dot");
      if (dot) {
        dot.className = "status-dot " + (cfg === false ? "status-dot-error" : (cfg === true ? "status-dot-ok" : "status-dot-unknown"));
      }
    }
  }

  async function loadBrokers() {
    if (!brokerIdList) return;
    try {
      const resp = await window.ghTrader.fetchApi("/api/brokers");
      if (!resp.ok) return;
      const data = await resp.json();
      if (!data || data.ok === false) return;
      const brokers = Array.isArray(data.brokers) ? data.brokers : [];
      brokerIdList.innerHTML = brokers.map((b) => '<option value="' + esc(b) + '"></option>').join("");
    } catch (_) {
      // ignore
    }
  }

  async function loadAccounts() {
    try {
      const resp = await window.ghTrader.fetchApi("/api/accounts");
      if (!resp.ok) return;
      const data = await resp.json();
      if (!data || data.ok === false) return;

      const rows = Array.isArray(data.profiles) ? data.profiles : [];
      accountsByProfile = {};
      const profiles = [];
      for (const r of rows) {
        if (!r || !r.profile) continue;
        const p = String(r.profile);
        profiles.push(p);
        accountsByProfile[p] = r;
      }

      if (profiles.length && profiles.indexOf(selectedAccountProfile) === -1) {
        setSelectedAccountProfile(profiles[0]);
      } else {
        setSelectedAccountProfile(selectedAccountProfile);
      }

      function profileLabel(p) {
        const r = accountsByProfile[p] || {};
        const parts = [String(p)];
        if (r.broker_id) parts.push(String(r.broker_id));
        if (r.account_id_masked) parts.push(String(r.account_id_masked));
        return parts.join(" — ");
      }

      if (accountProfileSelect) {
        accountProfileSelect.innerHTML = profiles.map((p) => '<option value="' + esc(p) + '">' + esc(profileLabel(p)) + "</option>").join("");
        accountProfileSelect.value = selectedAccountProfile;
      }

      updateAccountProfileStatus();

      const tbody = document.getElementById("accountsTbody");
      if (tbody) {
        if (!profiles.length) {
          tbody.innerHTML = '<tr><td colspan="8" class="muted">No profiles found</td></tr>';
        } else {
          tbody.innerHTML = profiles.map((p) => {
            const r = accountsByProfile[p] || {};
            const cfg = (r.configured === true);
            const brokerId = r.broker_id ? String(r.broker_id) : "--";
            const accMasked = r.account_id_masked ? String(r.account_id_masked) : "--";
            const gw = (r.gateway && typeof r.gateway === "object") ? r.gateway : {};
            const st = (r.strategy && typeof r.strategy === "object") ? r.strategy : {};
            const gwStatus = String(gw.status || "--");
            const stStatus = String(st.status || "--");
            const gwMode = String(gw.desired_mode || "");
            const stMode = String(st.desired_mode || "");

            function statusPill(status) {
              const s = String(status || "--");
              let cls = "pill";
              if (s === "running") cls = "pill pill-running";
              else if (s === "starting") cls = "pill pill-queued";
              else if (s === "degraded") cls = "pill pill-failed";
              else if (s === "desired_idle") cls = "pill";
              else if (s === "not_initialized") cls = "pill";
              return '<span class="' + cls + '">' + esc(s) + "</span>";
            }

            const gwCell = statusPill(gwStatus) + (gwMode ? (' <span class="muted text-xs">(' + esc(gwMode) + ')</span>') : "");
            const stCell = statusPill(stStatus) + (stMode ? (' <span class="muted text-xs">(' + esc(stMode) + ')</span>') : "");
            const verifyAt = r.verify && r.verify.verified_at ? String(r.verify.verified_at) : "";
            const verifyOk = (r.verify && r.verify.ok === true);
            const verifyErr = (r.verify && r.verify.error) ? String(r.verify.error) : "";
            const verifyPill = verifyAt
              ? (verifyOk
                ? '<span class="pill pill-succeeded" title="verified">ok</span>'
                : (verifyErr
                  ? ('<span class="pill pill-failed" title="' + esc(verifyErr) + '">error</span>')
                  : '<span class="pill">--</span>'))
              : '<span class="pill">--</span>';

            return [
              "<tr>",
              '<td class="mono">', esc(p), "</td>",
              '<td class="mono">', esc(brokerId), "</td>",
              '<td class="mono">', esc(accMasked), "</td>",
              "<td>", (cfg ? '<span class="pill pill-succeeded">yes</span>' : '<span class="pill pill-failed">no</span>'), "</td>",
              "<td>", gwCell, "</td>",
              "<td>", stCell, "</td>",
              '<td class="mono text-xs">', (verifyAt ? fmtTs(verifyAt) : "--"), " ", verifyPill, "</td>",
              '<td style="white-space:nowrap;">',
              '<button type="button" class="btn btn-sm btn-secondary" data-action="select-account" data-profile="', esc(p), '">Select</button> ',
              '<button type="button" class="btn btn-sm btn-secondary" data-action="verify-account" data-profile="', esc(p), '">Verify</button> ',
              '<button type="button" class="btn btn-sm btn-danger" data-action="remove-account" data-profile="', esc(p), '">Remove</button>',
              "</td>",
              "</tr>",
            ].join("");
          }).join("");
        }
      }
    } catch (_) {
      // ignore
    }
  }

  // Accounts table actions
  const accountsTbody = document.getElementById("accountsTbody");
  if (accountsTbody) {
    accountsTbody.addEventListener("click", async (ev) => {
      const t = ev.target;
      if (!t || !t.dataset) return;
      const action = String(t.dataset.action || "");
      const prof = String(t.dataset.profile || "");
      if (!action || !prof) return;

      if (action === "select-account") {
        setSelectedAccountProfile(prof);
        updateAccountProfileStatus();
        tabs.activate("status");
        await refreshAll();
        return;
      }

      if (action === "verify-account") {
        try {
          const out = await window.ghTrader.postJson("/api/accounts/enqueue-verify", { account_profile: prof });
          const jobId = (out.enqueued && out.enqueued[0]) ? out.enqueued[0] : "";
          window.ghTrader.toast(jobId ? ("Enqueued verify job: " + jobId) : "Enqueued verify job", "success");
          loadTradingJobs();
        } catch (e) {
          window.ghTrader.toast("Failed to enqueue verify: " + (e.message || e), "error");
        }
        return;
      }

      if (action === "remove-account") {
        if (!confirm("Remove account profile '" + prof + "'? This deletes it from runs/control/accounts.env.")) return;
        try {
          await window.ghTrader.postJson("/api/accounts/delete", { profile: prof });
          window.ghTrader.toast("Removed: " + prof, "success");
          if (selectedAccountProfile === prof) setSelectedAccountProfile("default");
          loadAccounts();
        } catch (e) {
          window.ghTrader.toast("Failed to remove: " + (e.message || e), "error");
        }
        return;
      }
    });
  }

  function clearAccountForm() {
    if (accountFormProfile) accountFormProfile.value = "";
    if (accountFormBroker) accountFormBroker.value = "";
    if (accountFormAccountId) accountFormAccountId.value = "";
    if (accountFormPassword) accountFormPassword.value = "";
  }
  if (accountFormClear) {
    accountFormClear.addEventListener("click", () => clearAccountForm());
  }
  if (accountUpsertForm) {
    accountUpsertForm.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const profile = accountFormProfile ? String(accountFormProfile.value || "").trim() : "";
      const brokerId = accountFormBroker ? String(accountFormBroker.value || "").trim() : "";
      const accountId = accountFormAccountId ? String(accountFormAccountId.value || "").trim() : "";
      const password = accountFormPassword ? String(accountFormPassword.value || "").trim() : "";

      if (!profile) {
        window.ghTrader.toast("Profile is required", "error");
        return;
      }
      if (!brokerId || !accountId || !password) {
        window.ghTrader.toast("Broker ID, account ID, and password are required", "error");
        return;
      }

      try {
        await window.ghTrader.postJson("/api/accounts/upsert", {
          profile: profile,
          broker_id: brokerId,
          account_id: accountId,
          password: password,
        });
        window.ghTrader.toast("Saved: " + profile, "success");
        if (accountFormPassword) accountFormPassword.value = "";
        loadAccounts();
      } catch (e) {
        window.ghTrader.toast("Failed to save: " + (e.message || e), "error");
      }
    });
  }

  async function loadConsoleStatus() {
    try {
      const u = "/api/trading/console/status?account_profile=" + encodeURIComponent(selectedAccountProfile);
      const resp = await window.ghTrader.fetchApi(u);
      if (!resp.ok) return;
      const data = await resp.json();
      if (!data || data.ok === false) return;

      // Live enabled
      liveEnabled = (data.live_enabled === true);

      const gw = data.gateway || {};
      const gwState = (gw.state && typeof gw.state === "object") ? gw.state : null;
      const gwLastSnap = gwState && gwState.last_snapshot && typeof gwState.last_snapshot === "object" ? gwState.last_snapshot : null;

      // Status bar
      const modeIndicator = document.getElementById("tradingModeIndicator");
      const gwMode = gwState && gwState.effective ? gwState.effective.mode : null;
      const gwDesiredWrap = (gw.desired && typeof gw.desired === "object") ? gw.desired : {};
      const gwDesired = (gwDesiredWrap.desired && typeof gwDesiredWrap.desired === "object") ? gwDesiredWrap.desired : gwDesiredWrap;
      const gwDesiredMode = gwDesired && gwDesired.mode ? String(gwDesired.mode) : null;
      const mode = gwMode || gwDesiredMode || "idle";
      if (modeIndicator) {
        const dot = modeIndicator.querySelector(".status-dot");
        const txt = modeIndicator.querySelector("span:last-child");
        if (txt) txt.textContent = "Mode: " + mode;
        if (dot) dot.className = "status-dot " + modeDotClass(mode);
      }
      const liveEl = document.getElementById("tradingLiveEnabledIndicator");
      if (liveEl) {
        const dot = liveEl.querySelector(".status-dot");
        const txt = liveEl.querySelector("span:last-child");
        if (txt) txt.textContent = "Live enabled: " + (liveEnabled ? "true" : "false");
        if (dot) dot.className = "status-dot " + (liveEnabled ? "status-dot-ok" : "status-dot-warn");
      }

      // KPIs
      const kpiMode = document.getElementById("kpiMode");
      const kpiModeDetail = document.getElementById("kpiModeDetail");
      if (kpiMode) kpiMode.textContent = String(mode || "idle");
      if (kpiModeDetail) {
        const gwJob = gw.active_job_id ? ("gateway job: " + gw.active_job_id) : "";
        const stJob = (data.strategy && data.strategy.active_job_id) ? ("strategy job: " + data.strategy.active_job_id) : "";
        const parts = [gwJob, stJob].filter(Boolean);
        kpiModeDetail.textContent = parts.length ? parts.join(" · ") : "No active process";
      }

      const acct = (gwLastSnap && gwLastSnap.account) ? gwLastSnap.account : null;
      const kpiBalance = document.getElementById("kpiBalance");
      const kpiEquity = document.getElementById("kpiEquity");
      if (kpiBalance) kpiBalance.textContent = acct ? formatNumber(acct.balance) : "--";
      if (kpiEquity) kpiEquity.textContent = acct ? ("Equity: " + formatNumber(acct.equity)) : "Equity: --";

      // PnL (best-effort from snapshot)
      const pnl = acct ? (acct.float_profit ?? acct.position_profit) : null;
      const kpiPnL = document.getElementById("kpiPnL");
      const kpiPnLPct = document.getElementById("kpiPnLPct");
      if (kpiPnL) kpiPnL.textContent = formatNumber(pnl);
      if (kpiPnLPct) {
        try {
          const bal = acct && acct.balance !== undefined ? Number(acct.balance) : null;
          const pp = (pnl !== null && pnl !== undefined) ? Number(pnl) : null;
          const pct = (bal && Number.isFinite(bal) && bal !== 0 && Number.isFinite(pp)) ? (pp / bal) * 100 : null;
          kpiPnLPct.textContent = (pct === null) ? "--" : (pct.toFixed(2) + "%");
        } catch (_) {
          kpiPnLPct.textContent = "--";
        }
      }

      // Last update time
      const lastUpdate = document.getElementById("tradingLastUpdate");
      if (lastUpdate) lastUpdate.textContent = "Last update: " + new Date().toLocaleTimeString();

      // Active run info: gateway + strategy
      const activeRunInfo = document.getElementById("activeRunInfo");
      if (activeRunInfo) {
        const st = data.strategy || {};
        const lines = [];

        lines.push('<div class="grid2">');
        lines.push('<div class="text-sm"><span class="muted">Gateway:</span> ' + esc(String(gw.component_status || gw.status || "--")) + '</div>');
        lines.push('<div class="text-sm"><span class="muted">Gateway mode:</span> ' + esc(String(gwMode || "--")) + '</div>');
        lines.push('<div class="text-sm"><span class="muted">Gateway job:</span> ' + (gw.active_job_id ? ('<a href="/jobs/' + esc(gw.active_job_id) + tokenQs + '">' + esc(gw.active_job_id) + '</a>') : "--") + '</div>');
        lines.push('<div class="text-sm"><span class="muted">Gateway error:</span> ' + esc(String(gw.error || "")) + '</div>');

        lines.push('<div class="text-sm"><span class="muted">Strategy:</span> ' + esc(String(st.status || "--")) + '</div>');
        lines.push('<div class="text-sm"><span class="muted">Strategy job:</span> ' + (st.active_job_id ? ('<a href="/jobs/' + esc(st.active_job_id) + tokenQs + '">' + esc(st.active_job_id) + '</a>') : "--") + '</div>');
        const lastTargets = (st.state && st.state.last_targets) ? st.state.last_targets : null;
        lines.push('<div class="text-sm"><span class="muted">Last targets:</span> ' + esc(lastTargets ? JSON.stringify(lastTargets) : "--") + '</div>');
        lines.push("</div>");

        activeRunInfo.innerHTML = lines.join("");
      }

      // Positions table
      const posTbody = document.getElementById("positionsTbody");
      const pos = (gwLastSnap && gwLastSnap.positions) ? gwLastSnap.positions : null;
      if (posTbody) {
        const keys = pos && typeof pos === "object" ? Object.keys(pos) : [];
        const rows = [];
        if (keys.length > 0) {
          keys.forEach((sym) => {
            const p = pos[sym] || {};
            if (p.error) return;
            const vlong = Number(p.volume_long || 0);
            const vshort = Number(p.volume_short || 0);
            const net = vlong - vshort;
            const dir = net > 0 ? "long" : (net < 0 ? "short" : "flat");
            const pnl2 = Number(p.float_profit_long || 0) + Number(p.float_profit_short || 0);
            rows.push([
              "<tr>",
              '<td class="mono text-xs">', esc(sym), "</td>",
              "<td>", esc(dir), "</td>",
              '<td class="mono">', String(net), "</td>",
              '<td class="muted">--</td>',
              '<td class="muted">--</td>',
              '<td class="mono">', esc(formatNumber(pnl2)), "</td>",
              "</tr>",
            ].join(""));
          });
        }
        posTbody.innerHTML = rows.length ? rows.join("") : '<tr><td colspan="6" class="muted">No positions</td></tr>';
      }

      // Risk metrics: gateway desired
      const risk = (gw.desired && gw.desired.desired) ? gw.desired.desired : null;
      if (risk) {
        const maxPos = risk.max_abs_position ?? risk.max_position;
        const maxOrder = risk.max_order_size;
        const maxOps = risk.max_ops_per_sec;
        const maxLoss = risk.max_daily_loss;
        const el1 = document.getElementById("riskMaxPos");
        const el2 = document.getElementById("riskMaxOrder");
        const el3 = document.getElementById("riskMaxOps");
        const el4 = document.getElementById("riskDailyLoss");
        if (el1) el1.textContent = (maxPos !== undefined && maxPos !== null) ? String(maxPos) : "--";
        if (el2) el2.textContent = (maxOrder !== undefined && maxOrder !== null) ? String(maxOrder) : "--";
        if (el3) el3.textContent = (maxOps !== undefined && maxOps !== null) ? String(maxOps) : "--";
        if (el4) el4.textContent = (maxLoss !== undefined && maxLoss !== null) ? String(maxLoss) : "--";
      }

      // Signals: prefer strategy recent events
      const sigTbody = document.getElementById("signalsTbody");
      if (sigTbody) {
        const st2 = data.strategy || {};
        const stState = st2.state || {};
        const events = Array.isArray(stState.recent_events) ? stState.recent_events : [];
        const rows = [];
        const sigEvents = events.filter((e) => e && (e.type === "target_change" || e.type === "signals_disabled")).slice(-50).reverse();
        for (const e of sigEvents) {
          const ts = e.ts || "";
          let sym = e.symbol || "--";
          let sig = "--";
          let conf = "--";
          if (e.type === "signals_disabled") {
            sig = "disabled";
            conf = String(e.reason || "");
          } else {
            // StrategyRunner target_change uses {targets:{...}}
            if (e.targets && typeof e.targets === "object") {
              const keys = Object.keys(e.targets);
              sym = keys.length ? keys[0] : "--";
              sig = "targets " + JSON.stringify(e.targets);
            } else {
              sig = "target_change";
            }
          }
          rows.push([
            "<tr>",
            '<td class="mono text-xs">', esc(fmtTs(ts)), "</td>",
            '<td class="mono text-xs">', esc(sym), "</td>",
            "<td>", esc(sig), "</td>",
            '<td class="mono">', esc(conf), "</td>",
            "</tr>",
          ].join(""));
        }
        sigTbody.innerHTML = rows.length ? rows.join("") : '<tr><td colspan="4" class="muted">No signals</td></tr>';
      }

      // Orders: prefer gateway snapshot
      const ordTbody = document.getElementById("ordersTbody");
      if (ordTbody) {
        const ordersAlive = (gwLastSnap && Array.isArray(gwLastSnap.orders_alive)) ? gwLastSnap.orders_alive : [];
        const rows = [];
        for (const o of ordersAlive.slice(0, 30)) {
          rows.push([
            "<tr>",
            '<td class="mono text-xs">--</td>',
            '<td class="mono text-xs">', esc(o.symbol || "--"), "</td>",
            "<td>", esc(o.direction || "--"), "</td>",
            '<td class="mono">', esc(String(o.volume_left ?? o.volume_orign ?? "--")), "</td>",
            '<td class="mono text-xs">ALIVE</td>',
            "</tr>",
          ].join(""));
        }
        ordTbody.innerHTML = rows.length ? rows.join("") : '<tr><td colspan="5" class="muted">No orders</td></tr>';
      }
    } catch (_) {
      // ignore
    }
  }

  async function loadGatewayStatus() {
    if (!gatewayProfileEl) return;
    setText(gatewayProfileEl, selectedAccountProfile);
    try {
      const u = "/api/gateway/status?account_profile=" + encodeURIComponent(selectedAccountProfile);
      const resp = await window.ghTrader.fetchApi(u);
      if (!resp.ok) {
        setText(gatewayHealthEl, "HTTP " + resp.status);
        return;
      }
      const data = await resp.json();
      if (!data || data.ok === false) return;

      if (data.exists === false) {
        setText(gatewayHealthEl, "not_initialized");
        setText(gatewayUpdatedAtEl, data.generated_at || "--");
        if (gatewayRawEl) gatewayRawEl.textContent = JSON.stringify(data, null, 2) + "\n\nTip: set Mode != idle and click Apply desired to create runs/gateway/account=<profile>/desired.json.";
        return;
      }

      const st = (data.state && typeof data.state === "object") ? data.state : {};
      const health = (st.health && typeof st.health === "object") ? st.health : {};
      const connected = (health.connected === true);
      const ok = (health.ok === true);
      setText(gatewayHealthEl, connected ? (ok ? "ok" : "degraded") : "offline");
      setText(gatewayUpdatedAtEl, st.updated_at || data.generated_at || "--");

      const desiredWrap = (data.desired && typeof data.desired === "object") ? data.desired : {};
      const desired = (desiredWrap.desired && typeof desiredWrap.desired === "object") ? desiredWrap.desired : desiredWrap;

      if (gatewayModeEl) gatewayModeEl.value = String(desired.mode || (st.effective && st.effective.mode) || "idle");
      if (gatewayExecutorEl) gatewayExecutorEl.value = String(desired.executor || (st.effective && st.effective.executor) || "targetpos");
      if (gatewaySimAccountEl) gatewaySimAccountEl.value = String(desired.sim_account || "tqsim");
      if (gatewaySymbolsEl) gatewaySymbolsEl.value = Array.isArray(desired.symbols) ? desired.symbols.join(",") : String(desired.symbols || "");
      if (gatewayConfirmLiveEl) gatewayConfirmLiveEl.value = String(desired.confirm_live || "");

      if (gatewayMaxAbsPosEl) gatewayMaxAbsPosEl.value = String(desired.max_abs_position || 1);
      if (gatewayMaxOrderSizeEl) gatewayMaxOrderSizeEl.value = String(desired.max_order_size || 1);
      if (gatewayMaxOpsSecEl) gatewayMaxOpsSecEl.value = String(desired.max_ops_per_sec || 10);
      if (gatewayMaxDailyLossEl) gatewayMaxDailyLossEl.value = (desired.max_daily_loss !== null && desired.max_daily_loss !== undefined) ? String(desired.max_daily_loss) : "";
      if (gatewayEnforceTradingTimeEl) gatewayEnforceTradingTimeEl.value = (desired.enforce_trading_time === false) ? "false" : "true";

      if (gatewayRawEl) gatewayRawEl.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      setText(gatewayHealthEl, "error");
      if (gatewayRawEl) gatewayRawEl.textContent = String(e && e.message ? e.message : e);
    }
  }

  async function loadStrategyStatus() {
    if (!strategyProfileEl) return;
    setText(strategyProfileEl, selectedAccountProfile);
    try {
      const u = "/api/strategy/status?account_profile=" + encodeURIComponent(selectedAccountProfile);
      const resp = await window.ghTrader.fetchApi(u);
      if (!resp.ok) {
        setText(strategyHealthEl, "HTTP " + resp.status);
        return;
      }
      const data = await resp.json();
      if (!data || data.ok === false) return;

      if (data.exists === false) {
        setText(strategyHealthEl, "not_initialized");
        setText(strategyUpdatedAtEl, data.generated_at || "--");
        if (strategyRawEl) strategyRawEl.textContent = JSON.stringify(data, null, 2) + "\n\nTip: set Mode=run and click Apply desired to create runs/strategy/account=<profile>/desired.json.";
        return;
      }

      setText(strategyHealthEl, String(data.status || "--"));
      const st = (data.state && typeof data.state === "object") ? data.state : {};
      setText(strategyUpdatedAtEl, st.updated_at || data.generated_at || "--");

      const desiredWrap = (data.desired && typeof data.desired === "object") ? data.desired : {};
      const desired = (desiredWrap.desired && typeof desiredWrap.desired === "object") ? desiredWrap.desired : desiredWrap;

      if (strategyModeEl) strategyModeEl.value = String(desired.mode || "idle");
      if (strategySymbolsEl) strategySymbolsEl.value = Array.isArray(desired.symbols) ? desired.symbols.join(",") : String(desired.symbols || "");
      if (strategyModelEl) strategyModelEl.value = String(desired.model_name || "xgboost");
      if (strategyHorizonEl) strategyHorizonEl.value = String(desired.horizon || 50);
      if (strategyThresholdUpEl) strategyThresholdUpEl.value = String(desired.threshold_up || 0.6);
      if (strategyThresholdDownEl) strategyThresholdDownEl.value = String(desired.threshold_down || 0.6);
      if (strategyPositionSizeEl) strategyPositionSizeEl.value = String(desired.position_size || 1);
      if (strategyArtifactsDirEl) strategyArtifactsDirEl.value = String(desired.artifacts_dir || "artifacts");
      if (strategyPollIntervalEl) strategyPollIntervalEl.value = String(desired.poll_interval_sec || 0.5);

      if (strategyRawEl) strategyRawEl.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      setText(strategyHealthEl, "error");
      if (strategyRawEl) strategyRawEl.textContent = String(e && e.message ? e.message : e);
    }
  }

  async function loadTradingJobs() {
    const container = document.getElementById("tradingJobsList");
    if (!container) return;
    try {
      const resp = await window.ghTrader.fetchApi("/api/jobs?limit=80");
      if (!resp.ok) return;
      const data = await resp.json();
      const jobs = Array.isArray(data.jobs) ? data.jobs : [];

      function jobKind(j) {
        const argv = Array.isArray(j.command) ? j.command : [];
        const idx = argv.indexOf("ghtrader.cli");
        const sub1 = (idx >= 0 && idx + 1 < argv.length) ? String(argv[idx + 1]) : "";
        const sub2 = (idx >= 0 && idx + 2 < argv.length) ? String(argv[idx + 2]) : "";
        if (sub1 === "gateway" && sub2 === "run") return "gateway";
        if (sub1 === "strategy" && sub2 === "run") return "strategy";
        return "";
      }

      const running = jobs.filter((j) => j && j.status === "running" && jobKind(j));
      if (!running.length) {
        container.innerHTML = '<div class="muted">No gateway/strategy jobs running</div>';
        return;
      }

      container.innerHTML = running.map((j) => {
        const kind = jobKind(j);
        const pill = (kind === "gateway") ? "pill pill-queued" : "pill pill-succeeded";
        const label = (kind === "gateway") ? "gateway" : "strategy";
        return [
          '<div class="metric-card" style="margin-bottom:8px;">',
          '<div class="metric-content">',
          '<div class="metric-label truncate" style="max-width:160px;">' + esc(j.title || label) + "</div>",
          '<div class="metric-value text-sm"><span class="' + pill + '">' + esc(label) + "</span></div>",
          "</div>",
          '<div style="display:flex; gap:6px;">',
          '<a href="/jobs/' + esc(j.id) + tokenQs + '" class="btn-sm btn-secondary">View</a>',
          '<button type="button" class="btn-sm btn-secondary" data-action="cancel-job" data-job-id="' + esc(j.id) + '">Stop</button>',
          "</div>",
          "</div>",
        ].join("");
      }).join("");
    } catch (_) {
      // ignore
    }
  }

  // Stop jobs (gateway/strategy)
  const tradingJobsList = document.getElementById("tradingJobsList");
  if (tradingJobsList) {
    tradingJobsList.addEventListener("click", async (ev) => {
      const t = ev.target;
      if (!t || !t.dataset || t.dataset.action !== "cancel-job") return;
      const jobId = t.dataset.jobId;
      if (!jobId) return;
      try {
        await window.ghTrader.postJson("/api/jobs/" + jobId + "/cancel", {});
        window.ghTrader.toast("Stop signal sent", "success");
        loadTradingJobs();
      } catch (e) {
        window.ghTrader.toast("Failed to stop: " + (e.message || e), "error");
      }
    });
  }

  async function loadStrategyRunHistory() {
    const tbody = document.getElementById("strategyRunHistoryTbody");
    if (!tbody) return;
    try {
      const resp = await window.ghTrader.fetchApi("/api/strategy/runs?limit=50");
      if (!resp.ok) return;
      const data = await resp.json();
      if (!data || data.ok === false) return;
      const runs = Array.isArray(data.runs) ? data.runs : [];
      const badge = document.getElementById("runCountBadge");
      if (badge) badge.textContent = runs.length ? runs.length : "--";
      if (!runs.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="muted">No strategy runs found</td></tr>';
        return;
      }
      tbody.innerHTML = runs.map((r) => {
        const rid = String(r.run_id || "");
        const ap = String(r.account_profile || "--");
        const model = String(r.model_name || "--");
        const h = String(r.horizon || "--");
        const created = fmtTs(r.created_at || "");
        const lastEv = fmtTs(r.last_event_ts || "");
        return [
          "<tr>",
          '<td class="mono">', esc(rid), "</td>",
          '<td class="mono">', esc(ap), "</td>",
          '<td class="mono">', esc(model), "</td>",
          '<td class="mono">', esc(h), "</td>",
          '<td class="mono text-xs">', esc(created), "</td>",
          '<td class="mono text-xs">', esc(lastEv), "</td>",
          "</tr>",
        ].join("");
      }).join("");
    } catch (_) {
      // ignore
    }
  }

  // Gateway desired submit
  if (gatewayDesiredForm) {
    gatewayDesiredForm.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const desired = {
        mode: gatewayModeEl ? String(gatewayModeEl.value || "idle") : "idle",
        executor: gatewayExecutorEl ? String(gatewayExecutorEl.value || "targetpos") : "targetpos",
        sim_account: gatewaySimAccountEl ? String(gatewaySimAccountEl.value || "tqsim") : "tqsim",
        symbols: parseCsvSymbols(gatewaySymbolsEl ? gatewaySymbolsEl.value : ""),
        confirm_live: gatewayConfirmLiveEl ? String(gatewayConfirmLiveEl.value || "").trim() : "",
        max_abs_position: gatewayMaxAbsPosEl ? Number(gatewayMaxAbsPosEl.value || 0) : 0,
        max_order_size: gatewayMaxOrderSizeEl ? Number(gatewayMaxOrderSizeEl.value || 1) : 1,
        max_ops_per_sec: gatewayMaxOpsSecEl ? Number(gatewayMaxOpsSecEl.value || 10) : 10,
        max_daily_loss: (gatewayMaxDailyLossEl && String(gatewayMaxDailyLossEl.value || "").trim()) ? Number(gatewayMaxDailyLossEl.value) : null,
        enforce_trading_time: (gatewayEnforceTradingTimeEl ? String(gatewayEnforceTradingTimeEl.value) : "true") !== "false",
      };
      try {
        await window.ghTrader.postJson("/api/gateway/desired", { account_profile: selectedAccountProfile, desired: desired });
        window.ghTrader.toast("Gateway desired updated", "success");
        await refreshAll();
      } catch (e) {
        window.ghTrader.toast("Failed to update gateway desired: " + (e.message || e), "error");
      }
    });
  }

  async function sendGatewayCommand(type, params) {
    try {
      const out = await window.ghTrader.postJson("/api/gateway/command", { account_profile: selectedAccountProfile, type: type, params: params || {} });
      const cid = out && out.command_id ? out.command_id : "";
      window.ghTrader.toast(cid ? ("Command sent: " + type + " (" + cid + ")") : ("Command sent: " + type), "success");
      await refreshAll();
    } catch (e) {
      window.ghTrader.toast("Failed to send gateway command: " + (e.message || e), "error");
    }
  }

  if (gatewayCmdCancelAll) {
    gatewayCmdCancelAll.addEventListener("click", () => {
      if (!confirm("Cancel ALL alive orders for profile '" + selectedAccountProfile + "'?")) return;
      sendGatewayCommand("cancel_all", {});
    });
  }
  if (gatewayCmdFlatten) {
    gatewayCmdFlatten.addEventListener("click", () => {
      if (!confirm("Flatten positions for profile '" + selectedAccountProfile + "'?")) return;
      sendGatewayCommand("flatten", {});
    });
  }
  if (gatewayCmdDisarm) {
    gatewayCmdDisarm.addEventListener("click", () => {
      if (!confirm("Disarm live: switch desired mode to live_monitor for profile '" + selectedAccountProfile + "'?")) return;
      sendGatewayCommand("disarm_live", {});
    });
  }

  if (gatewayRefreshBtn) gatewayRefreshBtn.addEventListener("click", () => loadGatewayStatus());

  // Strategy desired submit
  if (strategyDesiredForm) {
    strategyDesiredForm.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const desired = {
        mode: strategyModeEl ? String(strategyModeEl.value || "idle") : "idle",
        symbols: parseCsvSymbols(strategySymbolsEl ? strategySymbolsEl.value : ""),
        model_name: strategyModelEl ? String(strategyModelEl.value || "xgboost") : "xgboost",
        horizon: strategyHorizonEl ? Number(strategyHorizonEl.value || 50) : 50,
        threshold_up: strategyThresholdUpEl ? Number(strategyThresholdUpEl.value || 0.6) : 0.6,
        threshold_down: strategyThresholdDownEl ? Number(strategyThresholdDownEl.value || 0.6) : 0.6,
        position_size: strategyPositionSizeEl ? Number(strategyPositionSizeEl.value || 1) : 1,
        artifacts_dir: strategyArtifactsDirEl ? String(strategyArtifactsDirEl.value || "artifacts") : "artifacts",
        poll_interval_sec: strategyPollIntervalEl ? Number(strategyPollIntervalEl.value || 0.5) : 0.5,
      };
      try {
        await window.ghTrader.postJson("/api/strategy/desired", { account_profile: selectedAccountProfile, desired: desired });
        window.ghTrader.toast("Strategy desired updated", "success");
        await refreshAll();
      } catch (e) {
        window.ghTrader.toast("Failed to update strategy desired: " + (e.message || e), "error");
      }
    });
  }

  if (strategyRefreshBtn) strategyRefreshBtn.addEventListener("click", () => loadStrategyStatus());

  async function refreshAll() {
    await loadConsoleStatus();
    await loadGatewayStatus();
    await loadStrategyStatus();
    await loadTradingJobs();
    await loadStrategyRunHistory();
  }

  // Profile switching
  if (accountProfileSelect) {
    accountProfileSelect.addEventListener("change", async () => {
      setSelectedAccountProfile(accountProfileSelect.value);
      updateAccountProfileStatus();
      await refreshAll();
    });
  }

  // Refresh button
  const refreshBtn = document.getElementById("refreshTrading");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", async () => {
      await loadBrokers();
      await loadAccounts();
      await refreshAll();
      window.ghTrader.toast("Trading console refreshed", "info");
    });
  }

  // Initial load
  loadBrokers();
  loadAccounts();
  refreshAll();

  // Auto-refresh (lightweight)
  setInterval(loadConsoleStatus, 10000);
  setInterval(loadGatewayStatus, 10000);
  setInterval(loadStrategyStatus, 10000);
  setInterval(loadTradingJobs, 15000);
  setInterval(loadStrategyRunHistory, 30000);
  setInterval(loadAccounts, 30000);
})();

