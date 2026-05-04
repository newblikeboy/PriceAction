(function () {
  "use strict";

  const authStateEl = document.getElementById("auth-state");
  const authLogEl = document.getElementById("auth-log");
  const signupForm = document.getElementById("signup-form");
  const loginForm = document.getElementById("login-form");
  const meBtn = document.getElementById("me-btn");
  const logoutBtn = document.getElementById("logout-btn");
  const authModal = document.getElementById("auth-modal");
  const authModalCloseBtn = document.getElementById("auth-modal-close");
  const authModalBackdrop = document.querySelector("[data-auth-close]");
  const authOpenButtons = Array.from(document.querySelectorAll("[data-auth-open]"));
  const authViewButtons = Array.from(document.querySelectorAll("[data-auth-view]"));
  const adminAuthModal = document.getElementById("admin-auth-modal");
  const adminAuthModalCloseBtn = document.getElementById("admin-auth-modal-close");
  const adminAuthModalBackdrop = document.querySelector("[data-admin-close]");
  const adminAuthOpenButtons = Array.from(document.querySelectorAll("[data-admin-open]"));
  const adminLoginForm = document.getElementById("admin-login-form");
  const adminEmailEl = document.getElementById("admin-email");
  const adminPasswordEl = document.getElementById("admin-password");
  const adminAuthLogEl = document.getElementById("admin-auth-log");
  const navToggleBtn = document.getElementById("nav-toggle");
  const topNavMenu = document.getElementById("topnav-menu");

  const suFullName = document.getElementById("su-fullname");
  const suEmail = document.getElementById("su-email");
  const suMobile = document.getElementById("su-mobile");
  const suPassword = document.getElementById("su-password");
  const suConfirmPassword = document.getElementById("su-confirm-password");
  const liEmail = document.getElementById("li-email");
  const liPassword = document.getElementById("li-password");

  let currentUser = null;
  let currentAuthView = "signup";

  function setLog(message) {
    authLogEl.textContent = String(message || "");
  }

  function setAdminLog(message) {
    if (!adminAuthLogEl) {
      return;
    }
    adminAuthLogEl.textContent = String(message || "");
  }

  async function requestJson(path, options) {
    const opts = options || {};
    const headers = Object.assign({}, opts.headers || {});
    const config = {
      method: opts.method || "GET",
      headers: headers,
      cache: "no-store",
      credentials: "same-origin",
    };
    if (opts.data !== undefined) {
      headers["content-type"] = "application/json";
      config.body = JSON.stringify(opts.data);
    }
    const res = await fetch(path, config);
    const text = await res.text();
    let body = {};
    try {
      body = text ? JSON.parse(text) : {};
    } catch (_err) {
      body = {};
    }
    if (!res.ok) {
      throw new Error(body.detail || body.message || `HTTP ${res.status}`);
    }
    return body;
  }

  function renderAuthState() {
    if (!authStateEl) {
      return;
    }
    if (currentUser && (currentUser.email || currentUser.username)) {
      authStateEl.textContent = `Logged in: ${currentUser.email || currentUser.username}`;
      return;
    }
    authStateEl.textContent = "Not logged in";
  }

  function setAuthView(view) {
    const next = String(view || "").toLowerCase() === "login" ? "login" : "signup";
    currentAuthView = next;
    const showSignup = next === "signup";
    signupForm.classList.toggle("is-hidden", !showSignup);
    loginForm.classList.toggle("is-hidden", showSignup);
    authViewButtons.forEach(function (button) {
      const isActive = String(button.dataset.authView || "") === next;
      button.classList.toggle("is-active", isActive);
    });
  }

  function openAuthModal(view) {
    if (!authModal) {
      return;
    }
    setAuthView(view || currentAuthView);
    authModal.hidden = false;
    document.body.style.overflow = "hidden";
  }

  function closeAuthModal() {
    if (!authModal) {
      return;
    }
    authModal.hidden = true;
    document.body.style.overflow = "";
  }

  function openAdminModal() {
    if (!adminAuthModal) {
      return;
    }
    adminAuthModal.hidden = false;
    document.body.style.overflow = "hidden";
    setAdminLog("Admin login ready");
  }

  function closeAdminModal() {
    if (!adminAuthModal) {
      return;
    }
    adminAuthModal.hidden = true;
    document.body.style.overflow = "";
  }

  function setNavOpen(isOpen) {
    if (!navToggleBtn || !topNavMenu) {
      return;
    }
    topNavMenu.classList.toggle("is-open", isOpen);
    navToggleBtn.setAttribute("aria-expanded", isOpen ? "true" : "false");
  }

  async function checkSession() {
    try {
      const res = await requestJson("/auth/me");
      currentUser = res.user || null;
      renderAuthState();
      if (currentUser && (currentUser.full_name || currentUser.email || currentUser.username)) {
        const name = currentUser.full_name || currentUser.email || currentUser.username;
        setLog(`Welcome back ${name}.`);
      } else {
        setLog("Session valid.");
      }
    } catch (_err) {
      currentUser = null;
      renderAuthState();
    }
  }

  signupForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    try {
      const fullName = String(suFullName.value || "").trim();
      const email = String(suEmail.value || "").trim().toLowerCase();
      const mobileNumber = String(suMobile.value || "").trim();
      const password = String(suPassword.value || "");
      const confirmPassword = String(suConfirmPassword.value || "");
      if (password !== confirmPassword) {
        setLog("Signup failed: password and confirm password do not match.");
        return;
      }
      await requestJson("/auth/signup", {
        method: "POST",
        data: {
          full_name: fullName,
          email: email,
          mobile_number: mobileNumber,
          password: password,
          confirm_password: confirmPassword,
        },
      });
      setLog("Signup successful. You can now login.");
      liEmail.value = email;
      setAuthView("login");
    } catch (err) {
      setLog(`Signup failed: ${err.message}`);
    }
  });

  loginForm.addEventListener("submit", async function (event) {
    event.preventDefault();
    try {
      const email = String(liEmail.value || "").trim().toLowerCase();
      const password = String(liPassword.value || "");
      const res = await requestJson("/auth/login", {
        method: "POST",
        data: { email: email, password: password, role: "user" },
      });
      currentUser = res.user || null;
      renderAuthState();
      setLog("Login successful. Redirecting to console...");
      window.location.href = "/ui";
    } catch (err) {
      setLog(`Login failed: ${err.message}`);
    }
  });

  if (adminLoginForm) {
    adminLoginForm.addEventListener("submit", async function (event) {
      event.preventDefault();
      try {
        const email = String((adminEmailEl && adminEmailEl.value) || "").trim().toLowerCase();
        const password = String((adminPasswordEl && adminPasswordEl.value) || "");
        const res = await requestJson("/auth/login", {
          method: "POST",
          data: { email: email, password: password, role: "admin" },
        });
        currentUser = res.user || null;
        renderAuthState();
        setAdminLog("Admin login successful. Redirecting to CRM...");
        window.location.href = "/ui/admin";
      } catch (err) {
        setAdminLog(`Admin login failed: ${err.message}`);
      }
    });
  }

  if (meBtn) {
    meBtn.addEventListener("click", async function () {
      await checkSession();
    });
  }

  if (logoutBtn) {
    logoutBtn.addEventListener("click", async function () {
      try {
        await requestJson("/auth/logout", { method: "POST" });
      } catch (_err) {
        // Continue local logout even if token expired.
      }
      currentUser = null;
      renderAuthState();
      setLog("Logged out.");
      window.location.href = "/";
    });
  }

  authOpenButtons.forEach(function (button) {
    button.addEventListener("click", function () {
      openAuthModal(String(button.dataset.authOpen || ""));
    });
  });

  authViewButtons.forEach(function (button) {
    button.addEventListener("click", function () {
      setAuthView(String(button.dataset.authView || ""));
    });
  });

  adminAuthOpenButtons.forEach(function (button) {
    button.addEventListener("click", function () {
      openAdminModal();
    });
  });

  if (navToggleBtn && topNavMenu) {
    navToggleBtn.addEventListener("click", function () {
      const willOpen = !topNavMenu.classList.contains("is-open");
      setNavOpen(willOpen);
    });

    topNavMenu.querySelectorAll("a, button").forEach(function (item) {
      item.addEventListener("click", function () {
        if (window.matchMedia("(max-width: 719px)").matches) {
          setNavOpen(false);
        }
      });
    });

    window.addEventListener("resize", function () {
      if (window.matchMedia("(min-width: 720px)").matches) {
        setNavOpen(false);
      }
    });
  }

  if (authModalCloseBtn) {
    authModalCloseBtn.addEventListener("click", closeAuthModal);
  }

  if (authModalBackdrop) {
    authModalBackdrop.addEventListener("click", closeAuthModal);
  }

  if (adminAuthModalCloseBtn) {
    adminAuthModalCloseBtn.addEventListener("click", closeAdminModal);
  }

  if (adminAuthModalBackdrop) {
    adminAuthModalBackdrop.addEventListener("click", closeAdminModal);
  }

  window.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") {
      return;
    }
    if (authModal && !authModal.hidden) {
      closeAuthModal();
    }
    if (adminAuthModal && !adminAuthModal.hidden) {
      closeAdminModal();
    }
  });

  setAuthView("signup");
  renderAuthState();
  checkSession();
})();
