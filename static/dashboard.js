// static/dashboard.js
(async function () {
    const statsEl = document.getElementById("stats");
    const refreshBtn = document.getElementById("refresh-btn");
    const startBtn = document.getElementById("start-worker-btn");
    const stopBtn = document.getElementById("stop-worker-btn");
    const enqueueForm = document.getElementById("enqueue-form");
    const enqueueUrl = document.getElementById("enqueue-url");
    const activityFeed = document.getElementById("activity-feed");

    // Chart.js setup
    const clickCtx = document.getElementById("clickChart").getContext("2d");
    const sourceCtx = document.getElementById("sourceChart").getContext("2d");

    const clickChart = new Chart(clickCtx, {
        type: "line",
        data: { labels: [], datasets: [{ label: "Clicks", data: [], borderColor: "#00ffe0" }] },
        options: { responsive: true, plugins: { legend: { labels: { color: "#fff" } } } }
    });

    const sourceChart = new Chart(sourceCtx, {
        type: "doughnut",
        data: { labels: [], datasets: [{ data: [], backgroundColor: ["#00ffe0","#0077ff","#ff4d4d"] }] },
        options: { responsive: true, plugins: { legend: { labels: { color: "#fff" } } } }
    });

    function showLoading() {
        statsEl.innerHTML = "<p>Loading stats...</p>";
    }

    async function fetchStats() {
        showLoading();
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
            // update click chart
            clickChart.data.labels.push(new Date().toLocaleTimeString());
            clickChart.data.datasets[0].data.push(s.clicks_total || 0);
            if (clickChart.data.labels.length > 20) {
                clickChart.data.labels.shift();
                clickChart.data.datasets[0].data.shift();
            }
            clickChart.update();
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

    async function fetchSourcesSummary() {
        try {
            const res = await fetch("/api/failed_summary", { credentials: "same-origin" });
            const summary = await res.json();
            const sources = summary.sources || {};
            sourceChart.data.labels = Object.keys(sources);
            sourceChart.data.datasets[0].data = Object.values(sources);
            sourceChart.update();
        } catch (e) {
            console.error(e);
        }
    }

    // Wire buttons (same as before, but replace alerts with toast if you added it)
    refreshBtn && refreshBtn.addEventListener("click", async function () {
        refreshBtn.disabled = true;
        try {
            const res = await fetch("/refresh", { method: "POST", credentials: "same-origin" });
            const j = await res.json();
            showToast("Refresh queued ✅", "success");
        } catch (e) {
            console.error(e);
            showToast("Refresh failed ❌", "error");
        } finally {
            refreshBtn.disabled = false;
            fetchStats(); fetchActivity(); fetchSourcesSummary();
        }
    });

    startBtn && startBtn.addEventListener("click", async function () {
        startBtn.disabled = true;
        try {
            const res = await fetch("/start", { method: "POST", credentials: "same-origin" });
            showToast("Worker start requested ▶️", "success");
        } catch (e) {
            console.error(e);
            showToast("Worker start failed ❌", "error");
        } finally {
            startBtn.disabled = false;
            fetchStats();
        }
    });

    stopBtn && stopBtn.addEventListener("click", async function () {
        stopBtn.disabled = true;
        try {
            const res = await fetch("/stop", { method: "POST", credentials: "same-origin" });
            showToast("Worker stop requested ⏹", "info");
        } catch (e) {
            console.error(e);
            showToast("Worker stop failed ❌", "error");
        } finally {
            stopBtn.disabled = false;
            fetchStats();
        }
    });

    enqueueForm && enqueueForm.addEventListener("submit", async function (ev) {
        ev.preventDefault();
        const url = enqueueUrl.value.trim();
        if (!url) return showToast("URL required", "error");
        document.getElementById("enqueue-btn").disabled = true;
        try {
            const payload = { url: url, source: "manual" };
            const res = await fetch("/enqueue", {
                method: "POST",
                credentials: "same-origin",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            const j = await res.json();
            showToast("Link enqueued ➕", "success");
            enqueueUrl.value = "";
        } catch (e) {
            console.error(e);
            showToast("Enqueue failed ❌", "error");
        } finally {
            document.getElementById("enqueue-btn").disabled = false;
            fetchStats(); fetchActivity();
        }
    });

    // initial fetch and periodic refresh
    fetchStats();
    fetchActivity();
    fetchSourcesSummary();
    setInterval(() => { fetchStats(); fetchActivity(); fetchSourcesSummary(); }, 30000);

    // Toast helper
    function showToast(msg, type="info") {
        const toast = document.getElementById("toast");
        toast.textContent = msg;
        toast.className = "toast " + type;
        toast.style.opacity = 1;
        setTimeout(() => toast.style.opacity = 0, 3000);
    }
})();
