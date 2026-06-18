(function () {
  "use strict";

  const chartEl = document.getElementById("admin-replay-chart");
  const form = document.getElementById("replay-load-form");
  if (!chartEl || !form || !window.LightweightCharts) {
    return;
  }

  const statusChip = document.getElementById("replay-status-chip");
  const messageEl = document.getElementById("replay-message");
  const titleEl = document.getElementById("replay-chart-title");
  const counterEl = document.getElementById("replay-candle-counter");
  const openStateEl = document.getElementById("replay-open-state");
  const loadBtn = document.getElementById("replay-load-btn");
  const prevBtn = document.getElementById("replay-prev-btn");
  const nextBtn = document.getElementById("replay-next-btn");
  const playBtn = document.getElementById("replay-play-btn");
  const resetBtn = document.getElementById("replay-reset-btn");
  const speedInput = document.getElementById("replay-speed-input");
  const tradesBody = document.getElementById("replay-trades-body");
  const skippedBody = document.getElementById("replay-skipped-body");
  const summaryNodes = {
    trades: document.getElementById("replay-summary-trades"),
    wins: document.getElementById("replay-summary-wins"),
    losses: document.getElementById("replay-summary-losses"),
    winRate: document.getElementById("replay-summary-win-rate"),
    points: document.getElementById("replay-summary-points"),
    average: document.getElementById("replay-summary-average"),
    r: document.getElementById("replay-summary-r"),
    currentPrice: document.getElementById("replay-current-price"),
  };

  const zoneLayer = document.createElement("div");
  zoneLayer.className = "chart-zone-layer";
  chartEl.appendChild(zoneLayer);

  let loaded = false;
  let playing = false;
  let requestInFlight = false;
  let playTimer = null;
  let renderTimer = null;
  let tradePriceLines = [];
  let latestZones = [];
  let latestCurrentTime = null;
  let zoneRedrawTimer = null;
  let activeSessionId = null;
  let lastVisibleCandles = 0;
  let forceFitNextUpdate = false;

  const chart = LightweightCharts.createChart(chartEl, {
    autoSize: true,
    layout: {
      background: { type: "solid", color: "#ffffff" },
      textColor: "#213047",
      fontFamily: "Sora, sans-serif",
    },
    grid: {
      vertLines: { color: "rgba(148, 163, 184, 0.22)" },
      horzLines: { color: "rgba(148, 163, 184, 0.22)" },
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: { color: "rgba(21, 70, 194, 0.75)", labelBackgroundColor: "#1546c2" },
      horzLine: { color: "rgba(21, 70, 194, 0.75)", labelBackgroundColor: "#1546c2" },
    },
    rightPriceScale: { borderColor: "rgba(100, 116, 139, 0.35)" },
    timeScale: {
      borderColor: "rgba(100, 116, 139, 0.35)",
      timeVisible: true,
      secondsVisible: false,
    },
  });

  const candleSeries = chart.addCandlestickSeries({
    upColor: "#16a34a",
    downColor: "#dc2626",
    borderUpColor: "#15803d",
    borderDownColor: "#b91c1c",
    wickUpColor: "#15803d",
    wickDownColor: "#b91c1c",
    priceLineColor: "#1546c2",
    priceLineVisible: true,
    lastValueVisible: true,
  });

  function setText(node, value) {
    if (node) {
      node.textContent = value;
    }
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function postJson(url, body) {
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Accept": "application/json", "X-Requested-With": "fetch" },
      body: JSON.stringify(body || {}),
    }).then(function (response) {
      return response.text().then(function (text) {
        let payload = null;
        if (text) {
          try {
            payload = JSON.parse(text);
          } catch (error) {
            payload = null;
          }
        }
        if (!response.ok) {
          const detail = payload && payload.detail ? payload.detail : text || "Replay request failed";
          throw new Error(detail);
        }
        if (!payload) {
          throw new Error("Replay response was empty or invalid");
        }
        return payload;
      });
    });
  }

  function setControls(payload) {
    const hasPayload = Boolean(payload);
    const done = hasPayload && Boolean(payload.is_done);
    [prevBtn, resetBtn, playBtn].forEach(function (button) {
      if (button) {
        button.disabled = !loaded;
      }
    });
    if (nextBtn) {
      nextBtn.disabled = !loaded || done;
    }
    if (playBtn) {
      playBtn.disabled = !loaded || done;
      playBtn.textContent = playing ? "Pause" : "Play";
    }
    if (loadBtn) {
      loadBtn.disabled = false;
      loadBtn.textContent = "Load Replay";
    }
  }

  function clearTradePriceLines() {
    tradePriceLines.forEach(function (line) {
      candleSeries.removePriceLine(line);
    });
    tradePriceLines = [];
  }

  function applyTradeLevels(levels) {
    clearTradePriceLines();
    (levels || []).forEach(function (level) {
      const price = Number(level.price);
      if (!Number.isFinite(price)) {
        return;
      }
      tradePriceLines.push(candleSeries.createPriceLine({
        price: price,
        color: level.color || "#64748b",
        lineWidth: 2,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: String(level.name || ""),
      }));
    });
  }

  function chartPaneOffsetTop() {
    const chartRect = chartEl.getBoundingClientRect();
    const canvases = Array.from(chartEl.querySelectorAll("canvas"));
    const paneCanvas = canvases
      .map(function (canvas) {
        const rect = canvas.getBoundingClientRect();
        return { rect: rect, area: rect.width * rect.height };
      })
      .filter(function (item) {
        return item.rect.width > 100 && item.rect.height > 100;
      })
      .sort(function (a, b) {
        return b.area - a.area;
      })[0];
    return paneCanvas ? paneCanvas.rect.top - chartRect.top : 0;
  }

  function scheduleZoneRedraw() {
    window.requestAnimationFrame(function () {
      applyZones(latestZones, latestCurrentTime);
    });
    window.clearTimeout(zoneRedrawTimer);
    zoneRedrawTimer = window.setTimeout(function () {
      applyZones(latestZones, latestCurrentTime);
    }, 80);
    window.setTimeout(function () {
      applyZones(latestZones, latestCurrentTime);
    }, 180);
  }

  function applyZones(zones, currentTime) {
    latestZones = zones || [];
    if (currentTime) latestCurrentTime = currentTime;
    zoneLayer.innerHTML = "";
    const paneOffsetTop = chartPaneOffsetTop();
    // Filter: anchor zones always visible; intraday zones only after they formed
    const visibleZones = latestZones.filter(function (zone) {
      if (!zone.formed_at) return true;
      if (!latestCurrentTime) return true;
      return zone.formed_at <= latestCurrentTime;
    });
    visibleZones.forEach(function (zone) {
      const low = Number(zone.low);
      const high = Number(zone.high);
      if (!Number.isFinite(low) || !Number.isFinite(high) || high <= low) {
        return;
      }
      const topCoordinate = candleSeries.priceToCoordinate(high);
      const bottomCoordinate = candleSeries.priceToCoordinate(low);
      if (!Number.isFinite(topCoordinate) || !Number.isFinite(bottomCoordinate)) {
        return;
      }
      const band = document.createElement("div");
      const top = paneOffsetTop + Math.min(topCoordinate, bottomCoordinate);
      const height = Math.max(3, Math.abs(bottomCoordinate - topCoordinate));
      // Focus zones (closest to price) = bolder; intraday zones = dashed; anchors = default
      band.className = zone.is_focus
        ? "chart-zone-band chart-zone-focus"
        : zone.is_anchor
        ? "chart-zone-band"
        : "chart-zone-band chart-zone-intraday";
      band.style.top = `${top}px`;
      band.style.height = `${height}px`;
      band.style.setProperty("--zone-color", zone.color || "#64748b");
      const origin = zone.is_anchor ? "prior session" : "today";
      band.title = `${zone.name || "ZONE"} ${low.toFixed(2)}-${high.toFixed(2)} | Score ${zone.score || "--"} | ${origin}${zone.is_focus ? " ★ FOCUS" : ""}`;

      const label = document.createElement("span");
      label.className = "chart-zone-label";
      const focusMark = zone.is_focus ? " ★" : "";
      label.textContent = `${String(zone.name || "ZONE").slice(0, 22)} ${low.toFixed(0)}-${high.toFixed(0)}${focusMark}`;
      band.appendChild(label);
      zoneLayer.appendChild(band);
    });
  }

  function renderTrades(trades) {
    if (!tradesBody) {
      return;
    }
    if (!Array.isArray(trades) || trades.length === 0) {
      tradesBody.innerHTML = '<tr><td colspan="14" class="small">No replay trades yet.</td></tr>';
      return;
    }
    tradesBody.innerHTML = trades.map(function (trade, index) {
      return `<tr>
        <td>${index + 1}</td>
        <td>${escapeHtml(trade.date)}</td>
        <td>${escapeHtml(trade.entry_time)}</td>
        <td>${escapeHtml(trade.exit_time || "--")}</td>
        <td>${escapeHtml(trade.direction)}</td>
        <td class="truncate-cell wide-truncate" title="${escapeHtml(trade.setup_type)}">${escapeHtml(trade.setup_type)}</td>
        <td>${escapeHtml(trade.entry_index_price)}</td>
        <td>${escapeHtml(trade.sl_index_price)}</td>
        <td>${escapeHtml(trade.target_index_price)}</td>
        <td>${escapeHtml(trade.exit_index_price || "--")}</td>
        <td>${escapeHtml(trade.result || trade.status || "--")}</td>
        <td>${escapeHtml(trade.points == null ? "" : trade.points)}</td>
        <td>${escapeHtml(trade.r_multiple == null ? "" : trade.r_multiple)}</td>
        <td class="truncate-cell reason-cell" title="${escapeHtml(trade.reason || "")}">${escapeHtml(trade.reason || "--")}</td>
      </tr>`;
    }).join("");
  }

  function renderSkipped(skipped) {
    if (!skippedBody) {
      return;
    }
    if (!Array.isArray(skipped) || skipped.length === 0) {
      skippedBody.innerHTML = '<tr><td colspan="5" class="small">No skipped replay signals yet.</td></tr>';
      return;
    }
    skippedBody.innerHTML = skipped.slice().reverse().map(function (item) {
      return `<tr>
        <td>${escapeHtml(item.date)}</td>
        <td>${escapeHtml(item.time)}</td>
        <td>${escapeHtml(item.direction)}</td>
        <td class="truncate-cell wide-truncate" title="${escapeHtml(item.setup)}">${escapeHtml(item.setup)}</td>
        <td class="truncate-cell reason-cell" title="${escapeHtml(item.reason)}">${escapeHtml(item.reason)}</td>
      </tr>`;
    }).join("");
  }

  function updateSummary(payload) {
    const summary = payload.summary || {};
    setText(summaryNodes.trades, summary.trades || 0);
    setText(summaryNodes.wins, summary.wins || 0);
    setText(summaryNodes.losses, summary.losses || 0);
    setText(summaryNodes.winRate, `${summary.win_rate || 0}%`);
    setText(summaryNodes.points, summary.total_points || 0);
    setText(summaryNodes.average, summary.average_points || 0);
    setText(summaryNodes.r, summary.total_R || 0);
    setText(summaryNodes.currentPrice, payload.current_price || "--");
    if (payload.open_trade) {
      setText(openStateEl, `open ${payload.open_trade.direction}`);
    } else if (payload.pending_signal) {
      setText(openStateEl, `pending ${payload.pending_signal.direction}`);
    } else {
      setText(openStateEl, "flat");
    }
  }

  function nextVisibleRange(previousRange, previousVisibleCandles, nextVisibleCandles) {
    if (!previousRange || !Number.isFinite(previousRange.from) || !Number.isFinite(previousRange.to)) {
      return null;
    }
    const previousLast = Math.max(0, Number(previousVisibleCandles || 0) - 1);
    const nextLast = Math.max(0, Number(nextVisibleCandles || 0) - 1);
    const width = previousRange.to - previousRange.from;
    if (!Number.isFinite(width) || width <= 0) {
      return previousRange;
    }
    const wasWatchingLatest = previousRange.to >= previousLast - 0.5;
    if (wasWatchingLatest) {
      return centeredVisibleRange(nextLast, width);
    }
    return previousRange;
  }

  function centeredVisibleRange(lastIndex, width) {
    const safeWidth = Math.max(8, Number(width || 60));
    const midpoint = Number(lastIndex || 0);
    return { from: midpoint - safeWidth / 2, to: midpoint + safeWidth / 2 };
  }

  function centerLatestCandle(payload) {
    const visibleCandles = Number(payload.visible_candles || 0);
    if (!visibleCandles) {
      return;
    }
    const currentRange = chart.timeScale().getVisibleLogicalRange();
    const currentWidth = currentRange && Number.isFinite(currentRange.to - currentRange.from)
      ? currentRange.to - currentRange.from
      : Math.max(40, Math.min(90, visibleCandles * 2));
    chart.timeScale().setVisibleLogicalRange(centeredVisibleRange(visibleCandles - 1, currentWidth));
  }

  function updateReplay(payload) {
    const previousRange = chart.timeScale().getVisibleLogicalRange();
    const previousVisibleCandles = lastVisibleCandles;
    const isNewSession = payload.session_id && payload.session_id !== activeSessionId;
    const canApplyDelta = Boolean(payload.is_delta && activeSessionId && payload.session_id === activeSessionId);
    loaded = true;
    if (canApplyDelta) {
      (payload.candles_delta || []).forEach(function (candle) {
        candleSeries.update(candle);
      });
    } else {
      candleSeries.setData(payload.candles || []);
    }
    candleSeries.setMarkers(payload.markers || []);
    if (isNewSession || forceFitNextUpdate || !previousRange) {
      chart.timeScale().fitContent();
      centerLatestCandle(payload);
    } else {
      const range = nextVisibleRange(previousRange, previousVisibleCandles, payload.visible_candles);
      if (range) {
        chart.timeScale().setVisibleLogicalRange(range);
      }
    }
    activeSessionId = payload.session_id || activeSessionId;
    lastVisibleCandles = Number(payload.visible_candles || 0);
    forceFitNextUpdate = false;
    applyZones(payload.zones || [], payload.current_time);
    scheduleZoneRedraw();
    applyTradeLevels(payload.trade_levels || []);
    renderTrades(payload.trades || []);
    renderSkipped(payload.skipped || []);
    updateSummary(payload);
    setText(statusChip, payload.is_done ? "complete" : "loaded");
    setText(titleEl, `${payload.symbol} ${payload.start_date} to ${payload.end_date}`);
    setText(counterEl, `${payload.visible_candles} / ${payload.total_candles}`);
    setText(messageEl, `Current candle: ${payload.current_time}`);
    setControls(payload);
    if (payload.is_done) {
      stopPlay();
    }
  }

  function replayFrameDelay() {
    const speed = Math.max(150, Number(speedInput && speedInput.value) || 700);
    return Math.max(60, speed);
  }

  function updateReplayTablesOnly(payload) {
    candleSeries.setMarkers(payload.markers || []);
    applyZones(payload.zones || [], payload.current_time);
    scheduleZoneRedraw();
    applyTradeLevels(payload.trade_levels || []);
    renderTrades(payload.trades || []);
    renderSkipped(payload.skipped || []);
    updateSummary(payload);
    setText(statusChip, payload.is_done ? "complete" : "loaded");
    setText(titleEl, `${payload.symbol} ${payload.start_date} to ${payload.end_date}`);
    setText(counterEl, `${payload.visible_candles} / ${payload.total_candles}`);
    setText(messageEl, `Current candle: ${payload.current_time}`);
    setControls(payload);
    if (payload.is_done) {
      stopPlay();
    }
  }

  function animateReplayDelta(payload) {
    const frames = Array.isArray(payload.frames) && payload.frames.length ? payload.frames : [payload];
    if (!playing || frames.length <= 1) {
      updateReplay(payload);
      return Promise.resolve();
    }
    let frameIndex = 0;
    loaded = true;

    return new Promise(function (resolve) {
      function drawOne() {
        if (!playing && frameIndex > 0) {
          requestInFlight = false;
          renderTimer = null;
          setControls(payload);
          resolve();
          return;
        }
        if (frameIndex >= frames.length) {
          renderTimer = null;
          requestInFlight = false;
          setControls(payload);
          if (payload.is_done) {
            stopPlay();
          }
          resolve();
          return;
        }
        const frame = frames[frameIndex];
        const previousRange = chart.timeScale().getVisibleLogicalRange();
        const previousVisibleCandles = lastVisibleCandles;
        const isNewSession = frame.session_id && frame.session_id !== activeSessionId;
        const delta = frame.candles_delta || [];
        if (!frame.is_delta || isNewSession || !activeSessionId) {
          updateReplay(frame);
        } else if (delta.length) {
          candleSeries.update(delta[delta.length - 1]);
          candleSeries.setMarkers(frame.markers || []);
          const range = nextVisibleRange(previousRange, previousVisibleCandles, frame.visible_candles);
          if (range) {
            chart.timeScale().setVisibleLogicalRange(range);
          }
          activeSessionId = frame.session_id || activeSessionId;
          lastVisibleCandles = Number(frame.visible_candles || 0);
          forceFitNextUpdate = false;
          applyZones(frame.zones || [], frame.current_time);
          scheduleZoneRedraw();
          applyTradeLevels(frame.trade_levels || []);
          renderTrades(frame.trades || []);
          renderSkipped(frame.skipped || []);
          updateSummary(frame);
          setText(statusChip, frame.is_done ? "complete" : "loaded");
          setText(titleEl, `${frame.symbol} ${frame.start_date} to ${frame.end_date}`);
          setText(counterEl, `${frame.visible_candles} / ${frame.total_candles}`);
          setText(messageEl, `Current candle: ${frame.current_time}`);
        } else {
          updateReplay(frame);
        }
        frameIndex += 1;
        if (frame.is_done) {
          stopPlay();
          requestInFlight = false;
          renderTimer = null;
          setControls(payload);
          resolve();
          return;
        }
        renderTimer = window.setTimeout(drawOne, replayFrameDelay());
      }
      drawOne();
    });
  }

  function stopPlay() {
    playing = false;
    if (playTimer) {
      window.clearInterval(playTimer);
      playTimer = null;
    }
    if (renderTimer) {
      window.clearTimeout(renderTimer);
      renderTimer = null;
    }
    if (playBtn) {
      playBtn.textContent = "Play";
    }
  }

  function replayStepCount() {
    if (!playing) {
      return 1;
    }
    const speed = Math.max(150, Number(speedInput && speedInput.value) || 700);
    if (speed <= 250) {
      return 8;
    }
    if (speed <= 500) {
      return 5;
    }
    if (speed <= 900) {
      return 3;
    }
    return 1;
  }

  function stepNext() {
    if (requestInFlight) {
      return;
    }
    if (!loaded || nextBtn.disabled) {
      stopPlay();
      return;
    }
    requestInFlight = true;
    nextBtn.disabled = true;
    postJson("/api/admin/replay/next", { count: replayStepCount() })
      .then(animateReplayDelta)
      .catch(function (error) {
        stopPlay();
        setText(messageEl, error.message);
        setControls(null);
      })
      .finally(function () {
        requestInFlight = false;
      });
  }

  form.addEventListener("submit", function (event) {
    event.preventDefault();
    stopPlay();
    activeSessionId = null;
    lastVisibleCandles = 0;
    forceFitNextUpdate = true;
    if (loadBtn) {
      loadBtn.disabled = true;
      loadBtn.textContent = "Loading";
    }
    const data = new FormData(form);
    postJson("/api/admin/replay/load", {
      symbol: data.get("symbol") || "NIFTY",
      start_date: data.get("start_date"),
      end_date: data.get("end_date"),
      context_trading_days: Number(data.get("context_trading_days") || 2),
    })
      .then(updateReplay)
      .catch(function (error) {
        loaded = false;
        setText(statusChip, "error");
        setText(messageEl, error.message);
        setControls(null);
      });
  });

  if (nextBtn) {
    nextBtn.addEventListener("click", stepNext);
  }

  if (prevBtn) {
    prevBtn.addEventListener("click", function () {
      stopPlay();
      postJson("/api/admin/replay/previous", {}).then(updateReplay).catch(function (error) {
        setText(messageEl, error.message);
      });
    });
  }

  if (resetBtn) {
    resetBtn.addEventListener("click", function () {
      stopPlay();
      forceFitNextUpdate = true;
      postJson("/api/admin/replay/reset", {}).then(updateReplay).catch(function (error) {
        setText(messageEl, error.message);
      });
    });
  }

  if (playBtn) {
    playBtn.addEventListener("click", function () {
      if (playing) {
        stopPlay();
        return;
      }
      playing = true;
      playBtn.textContent = "Pause";
      stepNext();
      playTimer = window.setInterval(stepNext, Math.max(150, Number(speedInput && speedInput.value) || 700));
    });
  }

  chart.timeScale().subscribeVisibleLogicalRangeChange(function () {
    if (loaded) {
      scheduleZoneRedraw();
    }
  });

  chart.timeScale().subscribeVisibleTimeRangeChange(function () {
    if (loaded) {
      scheduleZoneRedraw();
    }
  });

  ["wheel", "mousedown", "mousemove", "mouseup", "mouseleave", "touchmove", "touchend", "pointerdown", "pointermove", "pointerup"].forEach(function (eventName) {
    chartEl.addEventListener(eventName, scheduleZoneRedraw, { passive: true });
  });

  window.addEventListener("resize", function () {
    scheduleZoneRedraw();
  });

  setControls(null);
})();
