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
  let backtestPollTimer = null;
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

  setActiveView(initialView());
})();
