(function () {
  "use strict";

  const chartEl = document.getElementById("admin-live-chart");
  if (!chartEl || !window.LightweightCharts) {
    return;
  }

  const tfButtons = Array.from(document.querySelectorAll("[data-chart-timeframe]"));
  const crosshairEl = document.getElementById("chart-crosshair-readout");
  const liveMessageEl = document.getElementById("chart-live-message");
  let timeframe = "1m";
  let priceLines = [];
  let livePollTimer = null;
  let fullRefreshTimer = null;
  let liveRequestInFlight = false;
  let fullRequestInFlight = false;
  let hasLoaded = false;
  let hasFitInitialContent = false;
  let shouldFocusUnderlying = true;
  let userLockedView = false;
  let suppressRangeChange = false;
  let lastPayload = null;
  let lastCandles = [];
  let latestUnderlyingValue = null;

  function cacheKey() {
    return `priceAction.chart.${timeframe}.90`;
  }

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
      vertLine: {
        color: "rgba(21, 70, 194, 0.75)",
        labelBackgroundColor: "#1546c2",
      },
      horzLine: {
        color: "rgba(21, 70, 194, 0.75)",
        labelBackgroundColor: "#1546c2",
      },
    },
    rightPriceScale: {
      borderColor: "rgba(100, 116, 139, 0.35)",
    },
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

  function formatPrice(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toFixed(2) : "--";
  }

  function isMobileChart() {
    return window.matchMedia("(max-width: 760px)").matches;
  }

  function shortLevelName(name) {
    const mapping = {
      ROUND_NUMBER: "RN",
      SWING_HIGH: "SH",
      SWING_LOW: "SL",
      DAY_HIGH: "DH",
      DAY_LOW: "DL",
    };
    return mapping[name] || name;
  }

  function levelDistance(level) {
    const price = Number(level.price);
    if (!Number.isFinite(latestUnderlyingValue) || !Number.isFinite(price)) {
      return Number.MAX_SAFE_INTEGER;
    }
    return Math.abs(price - latestUnderlyingValue);
  }

  function levelsForViewport(levels) {
    if (!isMobileChart()) {
      return levels || [];
    }
    const core = new Set(["PDH", "PDL", "PDC", "ORH", "ORL"]);
    return (levels || [])
      .slice()
      .sort(function (a, b) {
        const aCore = core.has(a.name) ? 0 : 1;
        const bCore = core.has(b.name) ? 0 : 1;
        if (aCore !== bCore) {
          return aCore - bCore;
        }
        return levelDistance(a) - levelDistance(b);
      })
      .slice(0, 7);
  }

  function clearPriceLines() {
    priceLines.forEach(function (line) {
      candleSeries.removePriceLine(line);
    });
    priceLines = [];
  }

  function applyLevels(levels) {
    clearPriceLines();
    levelsForViewport(levels).forEach(function (level) {
      const price = Number(level.price);
      if (!Number.isFinite(price) || price <= 0) {
        return;
      }
      priceLines.push(
        candleSeries.createPriceLine({
          price: price,
          color: level.color || "#64748b",
          lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Dashed,
          axisLabelVisible: true,
          title: shortLevelName(String(level.name || "")),
        })
      );
    });
  }

  function normalizeCandles(rawCandles) {
    return (rawCandles || []).map(function (candle) {
      if (Array.isArray(candle)) {
        return {
          time: candle[0],
          open: candle[1],
          high: candle[2],
          low: candle[3],
          close: candle[4],
        };
      }
      return candle;
    });
  }

  function visibleBarsForTimeframe() {
    if (timeframe === "1m") {
      return 180;
    }
    if (timeframe === "5m") {
      return 120;
    }
    return 96;
  }

  function livePricePadding() {
    if (timeframe === "1m") {
      return 70;
    }
    if (timeframe === "5m") {
      return 110;
    }
    return 160;
  }

  function currentUnderlying(payload, candles) {
    const livePrice = payload && payload.live ? Number(payload.live.price) : NaN;
    if (Number.isFinite(livePrice) && livePrice > 0) {
      return livePrice;
    }
    const last = candles.length ? candles[candles.length - 1] : null;
    const close = last ? Number(last.close) : NaN;
    return Number.isFinite(close) && close > 0 ? close : null;
  }

  function mergeLiveCandle(candle) {
    if (!candle || !Number.isFinite(Number(candle.time))) {
      return;
    }
    const normalized = {
      time: Number(candle.time),
      open: Number(candle.open),
      high: Number(candle.high),
      low: Number(candle.low),
      close: Number(candle.close),
    };
    const last = lastCandles.length ? lastCandles[lastCandles.length - 1] : null;
    if (last && Number(last.time) === normalized.time) {
      last.open = normalized.open;
      last.high = normalized.high;
      last.low = normalized.low;
      last.close = normalized.close;
    } else {
      lastCandles.push(normalized);
      lastCandles.sort(function (a, b) {
        return Number(a.time) - Number(b.time);
      });
    }
    candleSeries.update(normalized);
  }

  function focusOnUnderlying(payload, candles) {
    const underlying = currentUnderlying(payload, candles);
    if (!underlying || !candles.length) {
      if (!hasFitInitialContent) {
        chart.timeScale().fitContent();
        hasFitInitialContent = true;
      }
      return;
    }
    const lastIndex = candles.length - 1;
    const visibleBars = visibleBarsForTimeframe();
    const leftWindow = Math.max(20, visibleBars);
    const rightPadding = isMobileChart() ? 8 : 16;
    suppressRangeChange = true;
    chart.timeScale().setVisibleLogicalRange({
      from: Math.max(0, lastIndex - leftWindow),
      to: lastIndex + rightPadding,
    });
    const padding = livePricePadding();
    try {
      candleSeries.priceScale().setVisibleRange({
        from: underlying - padding,
        to: underlying + padding,
      });
    } catch (error) {
      candleSeries.applyOptions({
        autoscaleInfoProvider: function () {
          return {
            priceRange: {
              minValue: underlying - padding,
              maxValue: underlying + padding,
            },
          };
        },
      });
    }
    window.setTimeout(function () {
      suppressRangeChange = false;
    }, 80);
    hasFitInitialContent = true;
    shouldFocusUnderlying = false;
  }

  function updateChart(payload) {
    const candles = normalizeCandles(payload.candles || []);
    lastPayload = payload;
    lastCandles = candles;
    latestUnderlyingValue = currentUnderlying(payload, candles);
    candleSeries.setData(candles);
    candleSeries.setMarkers(isMobileChart() ? [] : (payload.markers || []));
    if (shouldFocusUnderlying && !userLockedView) {
      focusOnUnderlying(payload, candles);
    } else if (!hasFitInitialContent) {
      chart.timeScale().fitContent();
      hasFitInitialContent = true;
    }
    applyLevels(payload.levels || []);
    const live = payload.live || {};
    setText(liveMessageEl, live.message || "Live status unavailable");
    hasLoaded = true;
  }

  function loadChart() {
    if (fullRequestInFlight) {
      return;
    }
    if (!hasLoaded) {
      try {
        const cached = window.sessionStorage.getItem(cacheKey());
        if (cached) {
          updateChart(JSON.parse(cached));
          setText(liveMessageEl, "Showing cached chart while refreshing...");
        } else {
          setText(liveMessageEl, "Loading candles...");
        }
      } catch (error) {
        setText(liveMessageEl, "Loading candles...");
      }
    }
    fullRequestInFlight = true;
    fetch(`/api/admin/live-chart?timeframe=${encodeURIComponent(timeframe)}&days=90&live=true`, {
      headers: { Accept: "application/json", "X-Requested-With": "fetch" },
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Chart data request failed");
        }
        return response.json();
      })
      .then(function (payload) {
        updateChart(payload);
        try {
          window.sessionStorage.setItem(cacheKey(), JSON.stringify(payload));
        } catch (error) {
          // Ignore storage quota or privacy-mode errors.
        }
      })
      .catch(function (error) {
        setText(liveMessageEl, error.message);
      })
      .finally(function () {
        fullRequestInFlight = false;
      });
  }

  function loadLiveUpdate() {
    if (!hasLoaded || liveRequestInFlight) {
      return;
    }
    liveRequestInFlight = true;
    fetch(`/api/admin/live-chart/update?timeframe=${encodeURIComponent(timeframe)}`, {
      headers: { Accept: "application/json", "X-Requested-With": "fetch" },
      cache: "no-store",
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Live candle request failed");
        }
        return response.json();
      })
      .then(function (payload) {
        if (payload.timeframe !== timeframe) {
          return;
        }
        if (payload.candle) {
          mergeLiveCandle(payload.candle);
          latestUnderlyingValue = Number(payload.candle.close);
        }
        if (lastPayload) {
          lastPayload.live = payload.live || lastPayload.live;
        }
        const live = payload.live || {};
        setText(liveMessageEl, live.message || "Live candle updated");
      })
      .catch(function (error) {
        setText(liveMessageEl, error.message);
      })
      .finally(function () {
        liveRequestInFlight = false;
      });
  }

  function setTimeframe(nextTimeframe) {
    timeframe = nextTimeframe;
    hasLoaded = false;
    hasFitInitialContent = false;
    shouldFocusUnderlying = true;
    userLockedView = false;
    tfButtons.forEach(function (button) {
      button.classList.toggle("is-active", button.dataset.chartTimeframe === timeframe);
    });
    loadChart();
  }

  function resizeChart() {
    const width = chartEl.clientWidth || 900;
    const cssHeight = parseFloat(window.getComputedStyle(chartEl).height);
    const stageHeight = chartEl.parentElement ? chartEl.parentElement.clientHeight : 0;
    const height = cssHeight || stageHeight || chartEl.clientHeight || 520;
    chart.applyOptions({ width: width, height: height });
  }

  function startPolling() {
    if (!hasLoaded) {
      loadChart();
    }
    resizeChart();
    if (!livePollTimer) {
      livePollTimer = window.setInterval(loadLiveUpdate, 1000);
    }
    if (!fullRefreshTimer) {
      fullRefreshTimer = window.setInterval(loadChart, 60000);
    }
  }

  function stopPolling() {
    if (livePollTimer) {
      window.clearInterval(livePollTimer);
      livePollTimer = null;
    }
    if (fullRefreshTimer) {
      window.clearInterval(fullRefreshTimer);
      fullRefreshTimer = null;
    }
  }

  tfButtons.forEach(function (button) {
    button.addEventListener("click", function () {
      setTimeframe(button.dataset.chartTimeframe || "1m");
    });
  });

  chart.subscribeCrosshairMove(function (param) {
    if (!param || !param.time) {
      setText(crosshairEl, "Crosshair: --");
      return;
    }
    const price = param.seriesData.get(candleSeries);
    if (!price) {
      setText(crosshairEl, "Crosshair: --");
      return;
    }
    const date = new Date(Number(param.time) * 1000);
    const stamp = date.toLocaleString("en-IN", {
      timeZone: "UTC",
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
    setText(
      crosshairEl,
      `Crosshair: ${stamp}  O ${formatPrice(price.open)}  H ${formatPrice(price.high)}  L ${formatPrice(price.low)}  C ${formatPrice(price.close)}`
    );
  });

  chart.timeScale().subscribeVisibleLogicalRangeChange(function () {
    if (!hasLoaded || suppressRangeChange) {
      return;
    }
    userLockedView = true;
    shouldFocusUnderlying = false;
  });

  window.addEventListener("resize", function () {
    resizeChart();
  });

  document.addEventListener("portal:viewchange", function (event) {
    if (event.detail && event.detail.view === "chart") {
      shouldFocusUnderlying = !userLockedView;
      window.setTimeout(startPolling, 50);
      window.setTimeout(resizeChart, 180);
      if (!userLockedView && lastPayload && lastCandles.length) {
        window.setTimeout(function () {
          focusOnUnderlying(lastPayload, lastCandles);
        }, 80);
      }
    } else {
      stopPolling();
    }
  });

  if (!chartEl.closest("[hidden]")) {
    startPolling();
  }
  window.addEventListener("beforeunload", function () {
    stopPolling();
  });
})();
