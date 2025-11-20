(async function () {
    const statsEl = document.getElementById("stats");
    const refreshBtn = document.getElementById("refresh-btn");
    const startBtn = document.getElementById("start-worker-btn");
    const stopBtn = document.getElementById("stop-worker-btn");
    const enqueueForm = document.getElementById("enqueue-form");
    const enqueueUrl = document.getElementById("enqueue-url");
    const activityFeed = document.getElementById("activity-feed");

    // Toast helper
    function showToast(msg, type="info") {
        const toast = document.getElementById("toast");
        toast.textContent = msg;
        toast.className = "toast " + type;
        toast.style.opacity = 1;
        setTimeout(() => toast.style.opacity = 0, 3000);
    }

    async function fetchStats() {
        try {
            const res = await fetch("/api/stats", { credentials: "same-origin" });
            const j = await res.json();
            const s = j.stats || j;
            statsEl.innerHTML = `
                <strong>Total:</strong> ${s.total_links || s.total || 0} &nbsp;
                <strong>Pending:</strong> ${s.pending || 0} &nbsp;
                <strong>Posted:</strong> ${s.posted || s.sent || 0} &nbsp;
                <strong>Clicks:</strong> ${s.clicks_total || 0}
                <div style="margin-top:.5rem;"><small>Last posted: ${s.last_posted_at || "never"}</small></div>
            `;
        } catch (e) {
            console.error(e);
            statsEl.innerHTML = "<p>Error fetching stats</p>";
        }
    }

    async function fetchActivity() {
        try {
            const res = await fetch("/api/posts", { credentials: "same-origin" });
            const posts = await res.json();
            activityFeed.innerHTML = posts.map(p => `
                <li>
                  ${p.status === "posted" ? "✅" : "⏳"} 
                  <a href="/r/${p.id}" target="_blank">${p.url}</a>
                  <span class="source">(${p.source})</span>
                  <span class="time">${p.posted_at || p.created_at}</span>
                </li>
            `).join("");
        } catch (e) {
            console.error(e);
            activityFeed.innerHTML = "<li>Error loading activity</li>";
        }
    }

    async function fetchWorkerStatus() {
        try {
            const res = await fetch("/api/worker/status", { credentials: "same-origin" });
            const j = await res.json();
            const statusEl = document.getElementById("worker-status");
            if (j.running) {
                statusEl.textContent = "Running ✅";
                statusEl.style.color = "#00ffae";
                startBtn.classList.add("active");
                stopBtn.classList.remove("active");
            } else {
                statusEl.textContent = "Stopped ❌";
                statusEl.style.color = "#ff4d4d";
                startBtn.classList.remove("active");
                stopBtn.classList.add("active");
            }
        } catch (e) {

              console.error(e);
    }

    // Control actions
    async function doRefresh(btn) {
        btn.disabled = true;
        try {
            const res = await fetch("/refresh", { method: "POST", credentials: "same-origin" });
            await res.json();
            showToast("Refresh queued ✅", "success");
        } catch (e) {
            console.error(e);
            showToast("Refresh failed ❌", "error");
        } finally {
            btn.disabled = false;
            fetchStats(); fetchActivity(); fetchWorkerStatus();
        }
    }

    async function doStart(btn) {
        btn.disabled = true;
        try {
            const res = await fetch("/start", { method: "POST", credentials: "same-origin" });
            await res.json();
            showToast("Worker start requested ▶️", "success");
        } catch (e) {
            console.error(e);
            showToast("Worker start failed ❌", "error");
        } finally {
            btn.disabled = false;
            fetchStats(); fetchWorkerStatus();
        }
    }

    async function doStop(btn) {
        btn.disabled = true;
        try {
            const res = await fetch("/stop", { method: "POST", credentials: "same-origin" });
            await res.json();
            showToast("Worker stop requested ⏹", "info");
        } catch (e) {
            console.error(e);
            showToast("Worker stop failed ❌", "error");
        } finally {
            btn.disabled = false;
            fetchStats(); fetchWorkerStatus();
        }
    }

    async function doEnqueue(btn) {
        const url = enqueueUrl.value.trim();
        if (!url) return showToast("URL required", "error");
        btn.disabled = true;
        try {
            const payload = { url: url, source: "manual" };
            const res = await fetch("/enqueue", {
                method: "POST",
                credentials: "same-origin",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            await res.json();
            showToast("Link enqueued ➕", "success");
            enqueueUrl.value = "";
        } catch (e) {
            console.error(e);
            showToast("Enqueue failed ❌", "error");
        } finally {
            btn.disabled = false;
            fetchStats(); fetchActivity();
        }
    }

    // Wire main buttons
    refreshBtn && refreshBtn.addEventListener("click", () => doRefresh(refreshBtn));
    startBtn && startBtn.addEventListener("click", () => doStart(startBtn));
    stopBtn && stopBtn.addEventListener("click", () => doStop(stopBtn));
    enqueueForm && enqueueForm.addEventListener("submit", ev => {
        ev.preventDefault();
        doEnqueue(document.getElementById("enqueue-btn"));
    });

    // Wire dock buttons (mirror actions)
    const dockRefresh = document.querySelector(".control-dock #refresh-btn");
    const dockStart = document.querySelector(".control-dock #start-worker-btn");
    const dockStop = document.querySelector(".control-dock #stop-worker-btn");
    const dockEnqueue = document.querySelector(".control-dock #enqueue-btn");

    dockRefresh && dockRefresh.addEventListener("click", () => doRefresh(dockRefresh));
    dockStart && dockStart.addEventListener("click", () => doStart(dockStart));
    dockStop && dockStop.addEventListener("click", () => doStop(dockStop));
    dockEnqueue && dockEnqueue.addEventListener("click", () => doEnqueue(dockEnqueue));

    // Initial load + periodic refresh
    fetchStats();
    fetchActivity();
    fetchWorkerStatus();
    setInterval(() => { fetchStats(); fetchActivity(); fetchWorkerStatus(); }, 30000);
})();
      
