(function () {
  "use strict";

  const sidebarToggle = document.getElementById("sidebar-toggle");
  const sidebarClose = document.getElementById("sidebar-close");
  const sidebarOverlay = document.getElementById("sidebar-overlay");
  const profileWrap = document.getElementById("profile-wrap");
  const profileToggle = document.getElementById("profile-toggle-btn");
  const profileMenu = document.getElementById("profile-menu");
  const logoutLinks = Array.from(document.querySelectorAll('a[href="/logout"]'));
  const navButtons = Array.from(document.querySelectorAll("[data-view-target]"));
  const views = Array.from(document.querySelectorAll("[data-view]"));
  const backtestForm = document.getElementById("backtest-form");
  const backtestSubmit = document.getElementById("backtest-submit");
  const backtestStatusChip = document.getElementById("backtest-status-chip");
  const backtestProgressText = document.getElementById("backtest-progress-text");
  const backtestProgressFill = document.getElementById("backtest-progress-fill");
  const backtestCurrentStep = document.getElementById("backtest-current-step");
  const latestBacktestId = document.getElementById("latest-backtest-id");
  const latestBacktestStatus = document.getElementById("latest-backtest-status");
  const latestTotalTrades = document.getElementById("latest-total-trades");
  const latestWins = document.getElementById("latest-wins");
  const latestLosses = document.getElementById("latest-losses");
  const latestWinRate = document.getElementById("latest-win-rate");
  const latestTotalR = document.getElementById("latest-total-r");
  const latestAverageR = document.getElementById("latest-average-r");
  const latestSkipped = document.getElementById("latest-skipped");
  const latestBacktestError = document.getElementById("latest-backtest-error");
  const latestBacktestTradesBody = document.getElementById("latest-backtest-trades-body");
  const homeLiveExecutionToggle = document.getElementById("home-live-execution-toggle");
  const brokerForm = document.getElementById("broker-form");
  const brokerStatusChip = document.getElementById("broker-status-chip");
  const brokerServerStatus = document.getElementById("broker-server-status");
  const brokerSessionStatus = document.getElementById("broker-session-status");
  const brokerApiKeyStatus = document.getElementById("broker-api-key-status");
  const brokerLotSizeStatus = document.getElementById("broker-lot-size-status");
  const brokerClientIdInput = document.getElementById("broker-client-id-input");
  const brokerApiKeyInput = document.getElementById("broker-api-key-input");
  const brokerPinInput = document.getElementById("broker-pin-input");
  const brokerTotpInput = document.getElementById("broker-totp-input");
  const brokerLotCountInput = document.getElementById("broker-lot-count-input");
  const brokerTradingEnabledInput = document.getElementById("broker-trading-enabled-input");
  const brokerSaveBtn = document.getElementById("broker-save-btn");
  const brokerLoginBtn = document.getElementById("broker-login-btn");
  const brokerDisconnectBtn = document.getElementById("broker-disconnect-btn");
  const brokerMessage = document.getElementById("broker-message");
  let backtestPollTimer = null;
  let brokerLoaded = false;
  const viewStorageKey = `priceAction.activeView.${window.location.pathname}`;

  function isMobile() {
    return window.matchMedia("(max-width: 1024px)").matches;
  }

  function setSidebarOpen(open) {
    const shouldOpen = Boolean(open) && isMobile();
    document.body.classList.toggle("sidebar-open", shouldOpen);
    if (sidebarToggle) {
      sidebarToggle.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
    }
  }

  function setProfileOpen(open) {
    if (!profileWrap || !profileToggle || !profileMenu) {
      return;
    }
    const shouldOpen = Boolean(open);
    profileMenu.hidden = !shouldOpen;
    profileWrap.classList.toggle("is-open", shouldOpen);
    profileToggle.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
  }

  function setActiveView(name) {
    const target = String(name || "home").toLowerCase();
    const hasTarget = views.some(function (view) {
      return String(view.dataset.view || "").toLowerCase() === target;
    });
    const activeTarget = hasTarget ? target : "home";
    views.forEach(function (view) {
      const active = String(view.dataset.view || "").toLowerCase() === activeTarget;
      view.classList.toggle("is-active", active);
      view.hidden = !active;
    });
    navButtons.forEach(function (button) {
      const active = String(button.dataset.viewTarget || "").toLowerCase() === activeTarget;
      button.classList.toggle("is-active", active);
      if (active) {
        button.setAttribute("aria-current", "page");
      } else {
        button.removeAttribute("aria-current");
      }
    });
    try {
      window.localStorage.setItem(viewStorageKey, activeTarget);
      if (window.location.hash !== `#${activeTarget}`) {
        window.history.replaceState(null, "", `#${activeTarget}`);
      }
    } catch (error) {
      // Ignore storage errors in private browsing or restricted contexts.
    }
    setSidebarOpen(false);
    document.dispatchEvent(new CustomEvent("portal:viewchange", { detail: { view: activeTarget } }));
  }

  function initialView() {
    const query = new URLSearchParams(window.location.search);
    const queryView = query.get("view");
    const hashView = window.location.hash ? window.location.hash.slice(1) : "";
    let storedView = "";
    try {
      storedView = window.localStorage.getItem(viewStorageKey) || "";
    } catch (error) {
      storedView = "";
    }
    if (window.location.search.indexOf("backtest") >= 0) {
      return "backtest";
    }
    return queryView || hashView || storedView || "home";
  }

  navButtons.forEach(function (button) {
    button.addEventListener("click", function () {
      setActiveView(button.dataset.viewTarget || "home");
    });
  });
  if (sidebarToggle) {
    sidebarToggle.addEventListener("click", function () {
      setSidebarOpen(!document.body.classList.contains("sidebar-open"));
    });
  }
  if (sidebarClose) {
    sidebarClose.addEventListener("click", function () {
      setSidebarOpen(false);
    });
  }
  if (sidebarOverlay) {
    sidebarOverlay.addEventListener("click", function () {
      setSidebarOpen(false);
    });
  }
  if (profileToggle) {
    profileToggle.addEventListener("click", function (event) {
      event.stopPropagation();
      setProfileOpen(profileMenu ? profileMenu.hidden : true);
    });
  }
  document.addEventListener("click", function (event) {
    if (!profileWrap || !profileMenu || profileMenu.hidden) {
      return;
    }
    if (event.target instanceof Node && !profileWrap.contains(event.target)) {
      setProfileOpen(false);
    }
  });
  window.addEventListener("resize", function () {
    if (!isMobile()) {
      setSidebarOpen(false);
    }
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      setSidebarOpen(false);
      setProfileOpen(false);
    }
  });

  logoutLinks.forEach(function (link) {
    link.addEventListener("click", function (event) {
      event.preventDefault();
      window.location.href = "/logout";
    });
  });

  function setText(node, value) {
    if (node) {
      node.textContent = value;
    }
  }

  function setBrokerMessage(message, isError) {
    if (!brokerMessage) {
      return;
    }
    brokerMessage.textContent = message || "";
    brokerMessage.classList.toggle("error-text", Boolean(isError));
  }

  function setBrokerBusy(busy) {
    [brokerSaveBtn, brokerLoginBtn, brokerDisconnectBtn].forEach(function (button) {
      if (button) {
        button.disabled = Boolean(busy);
      }
    });
  }

  function renderBrokerStatus(payload) {
    const broker = payload && payload.broker ? payload.broker : payload || {};
    brokerLoaded = true;
    const serverOn = Boolean(broker.server_execution_enabled);
    const userOn = Boolean(broker.trading_enabled);
    const connected = Boolean(broker.connected && broker.has_access_token);
    const fullyOn = serverOn && userOn && connected;
    setText(brokerStatusChip, fullyOn ? "Live Ready" : connected ? "Connected" : "Not Connected");
    setText(brokerServerStatus, serverOn ? "Enabled" : "Disabled");
    setText(brokerSessionStatus, connected ? "Connected" : "Disconnected");
    setText(brokerApiKeyStatus, broker.api_key_masked || "Not saved");
    setText(brokerLotSizeStatus, String(broker.default_lot_size || "--"));
    if (brokerClientIdInput) {
      brokerClientIdInput.value = broker.client_id || "";
    }
    if (brokerLotCountInput) {
      brokerLotCountInput.value = String(broker.lot_count || 1);
    }
    if (brokerTradingEnabledInput) {
      brokerTradingEnabledInput.checked = userOn;
    }
    if (homeLiveExecutionToggle) {
      homeLiveExecutionToggle.classList.toggle("is-on", fullyOn);
      homeLiveExecutionToggle.classList.toggle("is-off", !fullyOn);
      homeLiveExecutionToggle.textContent = fullyOn ? "On" : "Off";
      homeLiveExecutionToggle.setAttribute("aria-checked", fullyOn ? "true" : "false");
    }
  }

  function loadBrokerStatus(force) {
    if (!brokerForm || (brokerLoaded && !force)) {
      return Promise.resolve();
    }
    return fetch("/api/user/broker/angel-one", {
      headers: { "Accept": "application/json", "X-Requested-With": "fetch" },
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Broker status request failed");
        }
        return response.json();
      })
      .then(renderBrokerStatus)
      .catch(function (error) {
        setBrokerMessage(error.message, true);
      });
  }

  function postBroker(url, body) {
    setBrokerBusy(true);
    setBrokerMessage("", false);
    const options = {
      method: "POST",
      headers: { "Accept": "application/json", "X-Requested-With": "fetch" },
    };
    if (body) {
      options.headers["Content-Type"] = "application/json";
      options.body = JSON.stringify(body);
    }
    return fetch(url, options)
      .then(function (response) {
        if (!response.ok) {
          return response.json().then(function (payload) {
            throw new Error(payload.detail || "Broker request failed");
          });
        }
        return response.json();
      })
      .then(function (payload) {
        renderBrokerStatus(payload);
        return payload;
      })
      .finally(function () {
        setBrokerBusy(false);
      });
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function renderBacktestTrades(trades) {
    if (!latestBacktestTradesBody) {
      return;
    }
    if (!Array.isArray(trades) || trades.length === 0) {
      latestBacktestTradesBody.innerHTML = '<tr><td colspan="16" class="small">No trades for the latest backtest run yet.</td></tr>';
      return;
    }
    latestBacktestTradesBody.innerHTML = trades.map(function (trade, index) {
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
        <td>${escapeHtml(trade.exit_reason || "--")}</td>
        <td>${escapeHtml(trade.result || "--")}</td>
        <td>${escapeHtml(trade.points == null ? "" : trade.points)}</td>
        <td>${escapeHtml(trade.r_multiple == null ? "" : trade.r_multiple)}</td>
        <td>${escapeHtml(trade.setup_score == null ? "" : trade.setup_score)}</td>
        <td class="truncate-cell reason-cell" title="${escapeHtml(trade.reason || "")}">${escapeHtml(trade.reason || "--")}</td>
      </tr>`;
    }).join("");
  }

  function formatPercent(value) {
    const number = Number(value || 0);
    return `${Math.max(0, Math.min(100, number)).toFixed(2)}%`;
  }

  function updateBacktestUi(payload) {
    const run = payload && payload.latest ? payload.latest : null;
    if (!run) {
      return;
    }
    const status = String(run.status || "idle");
    const progress = Number(run.progress_pct || 0);
    const summary = run.summary || {};
    setText(backtestStatusChip, status);
    setText(backtestProgressText, formatPercent(progress));
    if (backtestProgressFill) {
      backtestProgressFill.style.width = formatPercent(progress);
    }
    setText(backtestCurrentStep, run.current_step || "");
    setText(latestBacktestId, run.id ? `#${run.id}` : "#--");
    setText(latestBacktestStatus, status);
    setText(latestTotalTrades, String(summary.total_trades || 0));
    setText(latestWins, String(summary.wins || 0));
    setText(latestLosses, String(summary.losses || 0));
    setText(latestWinRate, `${summary.win_rate || 0}%`);
    setText(latestTotalR, String(summary.total_points || 0));
    setText(latestAverageR, String(summary.average_points || 0));
    setText(latestSkipped, String(run.skipped_count || 0));
    setText(latestBacktestError, run.error_message || "");
    renderBacktestTrades(run.trades || []);
    if (backtestSubmit) {
      backtestSubmit.disabled = status === "running";
      backtestSubmit.textContent = status === "running" ? "Backtest Running" : "Run DB Backtest";
    }
    if (status !== "running" && backtestPollTimer) {
      window.clearInterval(backtestPollTimer);
      backtestPollTimer = null;
    }
  }

  function pollBacktest() {
    if (!backtestForm) {
      return;
    }
    fetch("/api/backtest/latest", {
      headers: { "Accept": "application/json", "X-Requested-With": "fetch" },
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Backtest status request failed");
        }
        return response.json();
      })
      .then(updateBacktestUi)
      .catch(function () {
        if (backtestPollTimer) {
          window.clearInterval(backtestPollTimer);
          backtestPollTimer = null;
        }
      });
  }

  function startBacktestPolling() {
    pollBacktest();
    if (!backtestPollTimer) {
      backtestPollTimer = window.setInterval(pollBacktest, 1500);
    }
  }

  if (backtestForm) {
    backtestForm.addEventListener("submit", function (event) {
      event.preventDefault();
      setActiveView("backtest");
      if (backtestSubmit) {
        backtestSubmit.disabled = true;
        backtestSubmit.textContent = "Starting Backtest";
      }
      fetch(backtestForm.action, {
        method: "POST",
        body: new FormData(backtestForm),
        headers: { "Accept": "application/json", "X-Requested-With": "fetch" },
      })
        .then(function (response) {
          if (!response.ok) {
            return response.json().then(function (body) {
              throw new Error(body.detail || "Backtest could not be started");
            });
          }
          return response.json();
        })
        .then(function (payload) {
          updateBacktestUi({ active: true, latest: payload.backtest });
          startBacktestPolling();
        })
        .catch(function (error) {
          setText(backtestCurrentStep, error.message);
          if (backtestSubmit) {
            backtestSubmit.disabled = false;
            backtestSubmit.textContent = "Run DB Backtest";
          }
        });
    });
    startBacktestPolling();
  }

  if (brokerForm) {
    brokerForm.addEventListener("submit", function (event) {
      event.preventDefault();
      postBroker("/api/user/broker/angel-one", {
        client_id: brokerClientIdInput ? brokerClientIdInput.value.trim() : "",
        api_key: brokerApiKeyInput ? brokerApiKeyInput.value.trim() : "",
        pin: brokerPinInput ? brokerPinInput.value.trim() : "",
        totp_secret: brokerTotpInput ? brokerTotpInput.value.trim() : "",
        trading_enabled: brokerTradingEnabledInput ? brokerTradingEnabledInput.checked : false,
        lot_count: brokerLotCountInput ? Number(brokerLotCountInput.value || 1) : 1,
      })
        .then(function () {
          setBrokerMessage("Broker profile saved.", false);
          if (brokerApiKeyInput) {
            brokerApiKeyInput.value = "";
          }
          if (brokerPinInput) {
            brokerPinInput.value = "";
          }
          if (brokerTotpInput) {
            brokerTotpInput.value = "";
          }
        })
        .catch(function (error) {
          setBrokerMessage(error.message, true);
        });
    });
  }

  if (brokerLoginBtn) {
    brokerLoginBtn.addEventListener("click", function () {
      postBroker("/api/user/broker/angel-one/login")
        .then(function () {
          setBrokerMessage("Angel One login connected.", false);
        })
        .catch(function (error) {
          setBrokerMessage(error.message, true);
        });
    });
  }

  if (brokerDisconnectBtn) {
    brokerDisconnectBtn.addEventListener("click", function () {
      postBroker("/api/user/broker/angel-one/disconnect")
        .then(function () {
          setBrokerMessage("Angel One session disconnected.", false);
        })
        .catch(function (error) {
          setBrokerMessage(error.message, true);
        });
    });
  }

  document.addEventListener("portal:viewchange", function (event) {
    if (event.detail && event.detail.view === "broker") {
      loadBrokerStatus(false);
    }
  });

  setActiveView(initialView());
  loadBrokerStatus(true);
})();
