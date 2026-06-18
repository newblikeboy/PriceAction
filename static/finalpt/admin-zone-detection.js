(function () {
  "use strict";

  const chartEl      = document.getElementById("zd-chart");
  const form         = document.getElementById("zd-load-form");
  if (!chartEl || !form || !window.LightweightCharts) return;

  const statusChip    = document.getElementById("zd-status-chip");
  const messageEl     = document.getElementById("zd-message");
  const titleEl       = document.getElementById("zd-chart-title");
  const counterEl     = document.getElementById("zd-candle-counter");
  const anchorCount   = document.getElementById("zd-anchor-count");
  const intradayCount = document.getElementById("zd-intraday-count");
  const loadBtn       = document.getElementById("zd-load-btn");
  const prevBtn       = document.getElementById("zd-prev-btn");
  const nextBtn       = document.getElementById("zd-next-btn");
  const playBtn       = document.getElementById("zd-play-btn");
  const resetBtn      = document.getElementById("zd-reset-btn");
  const speedInput    = document.getElementById("zd-speed-input");
  const zonesBody     = document.getElementById("zd-zones-body");

  // zone overlay layer
  const zoneLayer = document.createElement("div");
  zoneLayer.className = "chart-zone-layer";
  chartEl.appendChild(zoneLayer);

  // in-memory candle cache (populated once at load)
  let _anchorCandles = [];
  let _activeCandles = [];
  let _shownIndex    = 0;   // how many active candles are currently on the chart

  let loaded      = false;
  let playing     = false;
  let playTimer   = null;
  let inFlight    = false;
  let latestZones = [];
  let zoneRedrawTimer = null;

  // ── Chart ─────────────────────────────────────────────────────────────────
  const chart = LightweightCharts.createChart(chartEl, {
    autoSize: true,
    layout: { background: { type: "solid", color: "#ffffff" }, textColor: "#213047", fontFamily: "Sora, sans-serif" },
    grid: { vertLines: { color: "rgba(148,163,184,0.22)" }, horzLines: { color: "rgba(148,163,184,0.22)" } },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: { color: "rgba(21,70,194,0.75)", labelBackgroundColor: "#1546c2" },
      horzLine: { color: "rgba(21,70,194,0.75)", labelBackgroundColor: "#1546c2" },
    },
    rightPriceScale: { borderColor: "rgba(100,116,139,0.35)" },
    timeScale: { borderColor: "rgba(100,116,139,0.35)", timeVisible: true, secondsVisible: false },
  });

  const candleSeries = chart.addCandlestickSeries({
    upColor: "#16a34a", downColor: "#dc2626",
    borderUpColor: "#15803d", borderDownColor: "#b91c1c",
    wickUpColor: "#15803d", wickDownColor: "#b91c1c",
  });

  window.addEventListener("resize", function () { chart.applyOptions({ width: chartEl.clientWidth }); });

  // ── Helpers ───────────────────────────────────────────────────────────────
  function setText(el, v) { if (el) el.textContent = v; }

  function escapeHtml(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function postJson(url, body) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "application/json", "X-Requested-With": "fetch" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      return r.text().then(function (text) {
        var data = null;
        try { data = JSON.parse(text); } catch (_) {}
        if (!r.ok) throw new Error((data && data.detail) || text || "Request failed");
        if (!data) throw new Error("Empty response");
        return data;
      });
    });
  }

  // ── Candle management (local cache) ───────────────────────────────────────
  function showAnchorOnly() {
    candleSeries.setData(_anchorCandles.slice());
    _shownIndex = 0;
    chart.timeScale().fitContent();
  }

  function revealTo(newIndex) {
    if (newIndex > _shownIndex) {
      // fast path: append new candles one by one
      for (var i = _shownIndex; i < newIndex && i < _activeCandles.length; i++) {
        candleSeries.update(_activeCandles[i]);
      }
    } else if (newIndex < _shownIndex) {
      // backward: rebuild from memory
      var rebuild = _anchorCandles.concat(_activeCandles.slice(0, newIndex));
      candleSeries.setData(rebuild);
    }
    // newIndex === _shownIndex means nothing to do on candles
    _shownIndex = newIndex;
  }

  // ── Zone rendering ────────────────────────────────────────────────────────
  function chartPaneTop() {
    var chartRect = chartEl.getBoundingClientRect();
    var canvases  = Array.from(chartEl.querySelectorAll("canvas"));
    var best = canvases
      .map(function (c) { var r = c.getBoundingClientRect(); return { r: r, area: r.width * r.height }; })
      .filter(function (x) { return x.r.width > 100 && x.r.height > 100; })
      .sort(function (a, b) { return b.area - a.area; })[0];
    return best ? best.r.top - chartRect.top : 0;
  }

  function drawZones(zones) {
    latestZones = zones || [];
    zoneLayer.innerHTML = "";
    var paneTop = chartPaneTop();
    latestZones.forEach(function (zone) {
      var low  = Number(zone.low);
      var high = Number(zone.high);
      if (!isFinite(low) || !isFinite(high) || high <= low) return;
      var topCoord = candleSeries.priceToCoordinate(high);
      var botCoord = candleSeries.priceToCoordinate(low);
      if (topCoord == null || botCoord == null) return;

      var band   = document.createElement("div");
      var top    = paneTop + Math.min(topCoord, botCoord);
      var height = Math.max(3, Math.abs(botCoord - topCoord));

      band.className = zone.is_anchor ? "chart-zone-band" : "chart-zone-band chart-zone-intraday";
      band.style.top    = top + "px";
      band.style.height = height + "px";
      band.style.setProperty("--zone-color", zone.color || "#64748b");
      band.title = zone.name + "  " + low.toFixed(2) + " – " + high.toFixed(2) +
                   "  |  Score " + (zone.score || "--") + "  |  " + (zone.status || "") +
                   "  |  " + (zone.is_anchor ? "Anchor (prior session)" : "Intraday (today)");

      var label = document.createElement("span");
      label.className = "chart-zone-label";
      label.textContent = zone.name.slice(0, 20) + "  " + low.toFixed(0) + "-" + high.toFixed(0);
      band.appendChild(label);
      zoneLayer.appendChild(band);
    });
  }

  function scheduleZoneRedraw() {
    window.requestAnimationFrame(function () { drawZones(latestZones); });
    clearTimeout(zoneRedrawTimer);
    zoneRedrawTimer = setTimeout(function () { drawZones(latestZones); }, 80);
  }

  chart.timeScale().subscribeVisibleTimeRangeChange(scheduleZoneRedraw);
  chart.subscribeCrosshairMove(scheduleZoneRedraw);

  // ── Zone table ────────────────────────────────────────────────────────────
  function renderZoneTable(zones) {
    if (!zonesBody) return;
    if (!zones || zones.length === 0) {
      zonesBody.innerHTML = '<tr><td colspan="11" class="small">No zones yet.</td></tr>';
      return;
    }
    var sorted = zones.slice().sort(function (a, b) { return Number(b.score) - Number(a.score); });
    zonesBody.innerHTML = sorted.map(function (z) {
      var srcBadge = z.is_anchor
        ? '<span style="color:#1546c2;font-weight:600">Anchor</span>'
        : '<span style="color:#16a34a;font-weight:600">Intraday</span>';
      return "<tr>" +
        "<td>" + srcBadge + "</td>" +
        "<td class='truncate-cell' title='" + escapeHtml(z.zone_type) + "'>" + escapeHtml(z.name) + "</td>" +
        "<td>" + escapeHtml(z.low) + "</td>" +
        "<td>" + escapeHtml(z.high) + "</td>" +
        "<td><strong>" + escapeHtml(z.score) + "</strong></td>" +
        "<td>" + escapeHtml(z.status) + "</td>" +
        "<td>" + escapeHtml(z.touch_count) + "</td>" +
        "<td>" + escapeHtml(z.reaction_count) + "</td>" +
        "<td>" + escapeHtml(z.break_count) + "</td>" +
        "<td>" + escapeHtml(z.freshness_score) + "</td>" +
        "<td>" + escapeHtml(z.enhancer_total) + "</td>" +
        "</tr>";
    }).join("");
  }

  // ── Apply server response ─────────────────────────────────────────────────
  function applyLoad(data) {
    _anchorCandles = data.anchor_candles || [];
    _activeCandles = data.active_candles || [];
    _shownIndex    = 0;
    showAnchorOnly();
    applyMeta(data);
  }

  function applyStep(data) {
    var newIndex = data.current_candle_index || 0;
    revealTo(newIndex);
    applyMeta(data);
  }

  function applyMeta(data) {
    drawZones(data.zones || []);
    renderZoneTable(data.zones || []);
    scheduleZoneRedraw();

    var total   = data.total_active_candles || 0;
    var current = data.current_candle_index || 0;
    var done    = data.is_done;

    setText(counterEl,    current + " / " + total);
    setText(anchorCount,  "Anchor: "   + (data.anchor_zone_count || 0));
    setText(intradayCount,"Intraday: " + (data.intraday_zone_count || 0));
    setText(statusChip,   done ? "done" : "loaded");
    setText(titleEl,      (data.symbol || "") + "  " + (data.active_date || "") +
                          "  (active day)  —  " + (data.start_date || "") + " to " + (data.end_date || ""));
    setText(messageEl,    done ? "Reached end of active day." : "");

    if (nextBtn)  nextBtn.disabled  = !loaded || !!done;
    if (prevBtn)  prevBtn.disabled  = !loaded;
    if (resetBtn) resetBtn.disabled = !loaded;
    if (playBtn) {
      playBtn.disabled  = !loaded || !!done;
      playBtn.textContent = playing ? "Pause" : "Play";
    }
  }

  function setControls(isLoading) {
    if (loadBtn) {
      loadBtn.disabled    = isLoading;
      loadBtn.textContent = isLoading ? "Loading..." : "Create Zones";
    }
  }

  // ── Play loop (no server call for candles — they're cached) ───────────────
  function stopPlay() {
    playing = false;
    clearInterval(playTimer);
    playTimer = null;
    if (playBtn) playBtn.textContent = "Play";
  }

  function startPlay() {
    if (!loaded || inFlight) return;
    playing = true;
    if (playBtn) playBtn.textContent = "Pause";
    var speed = Math.max(150, parseInt(speedInput ? speedInput.value : 600, 10) || 600);
    playTimer = setInterval(function () {
      if (inFlight) return;
      inFlight = true;
      postJson("/api/admin/zone-detection/next", { count: 1 })
        .then(function (data) {
          inFlight = false;
          applyStep(data);
          if (data.is_done) stopPlay();
        })
        .catch(function (err) {
          inFlight = false;
          stopPlay();
          setText(messageEl, "Error: " + err.message);
        });
    }, speed);
  }

  // ── Events ────────────────────────────────────────────────────────────────
  form.addEventListener("submit", function (e) {
    e.preventDefault();
    stopPlay();
    loaded = false;
    var fd = new FormData(form);
    setControls(true);
    setText(messageEl, "Computing anchor zones from previous sessions...");
    setText(statusChip, "loading");

    postJson("/api/admin/zone-detection/load", {
      symbol:     fd.get("symbol") || "NIFTY",
      start_date: fd.get("start_date"),
      end_date:   fd.get("end_date"),
    }).then(function (data) {
      loaded = true;
      setControls(false);
      applyLoad(data);
      setText(messageEl, "Loaded — " + (data.anchor_zone_count || 0) +
              " anchor zone(s) from prior sessions, " +
              (data.active_candles ? data.active_candles.length : 0) +
              " active-day candles cached.");
    }).catch(function (err) {
      loaded = false;
      setControls(false);
      setText(statusChip, "error");
      setText(messageEl, "Error: " + err.message);
    });
  });

  if (nextBtn) {
    nextBtn.addEventListener("click", function () {
      if (inFlight || !loaded) return;
      inFlight = true;
      postJson("/api/admin/zone-detection/next", { count: 1 })
        .then(function (data) { inFlight = false; applyStep(data); })
        .catch(function (err) { inFlight = false; setText(messageEl, "Error: " + err.message); });
    });
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", function () {
      if (inFlight || !loaded) return;
      inFlight = true;
      postJson("/api/admin/zone-detection/previous", {})
        .then(function (data) { inFlight = false; applyStep(data); })
        .catch(function (err) { inFlight = false; setText(messageEl, "Error: " + err.message); });
    });
  }

  if (playBtn) {
    playBtn.addEventListener("click", function () {
      if (!loaded) return;
      playing ? stopPlay() : startPlay();
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener("click", function () {
      if (inFlight || !loaded) return;
      stopPlay();
      inFlight = true;
      postJson("/api/admin/zone-detection/reset", {})
        .then(function (data) { inFlight = false; applyStep(data); })
        .catch(function (err) { inFlight = false; setText(messageEl, "Error: " + err.message); });
    });
  }
}());
