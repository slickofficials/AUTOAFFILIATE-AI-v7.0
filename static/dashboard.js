// static/dashboard.js
(async function () {
    const statsEl = document.getElementById("stats");
    const refreshBtn = document.getElementById("refresh-btn");
    const startBtn = document.getElementById("start-worker-btn");
    const stopBtn = document.getElementById("stop-worker-btn");
    const enqueueForm = document.getElementById("enqueue-form");
    const enqueueUrl = document.getElementById("enqueue-url");

    function showLoading() {
        statsEl.innerHTML = "<p>Loading stats...</p>";
    }

    async function fetchStats() {
        showLoading();
        try {
            const res = await fetch("/api/stats", { credentials: "same-origin" });
            const j = await res.json();
            if (!j.ok) {
                statsEl.innerHTML = "<p>Unable to fetch stats</p>";
                return;
            }
            const s = j.stats;
            statsEl.innerHTML = `
                <strong>Total links:</strong> ${s.total} &nbsp;
                <strong>Pending:</strong> ${s.pending} &nbsp;
                <strong>Sent:</strong> ${s.sent} &nbsp;
                <strong>Failed:</strong> ${s.failed}
                <div style="margin-top:.5rem;"><small>Last posted: ${s.last_posted_at || "never"}</small></div>
            `;
        } catch (e) {
            console.error(e);
            statsEl.innerHTML = "<p>Error fetching stats</p>";
        }
    }

    // Wire buttons
    refreshBtn && refreshBtn.addEventListener("click", async function () {
        refreshBtn.disabled = true;
        try {
            const res = await fetch("/api/refresh", { method: "POST", credentials: "same-origin" });
            const j = await res.json();
            if (res.ok) alert("Refresh completed: " + JSON.stringify(j.result || j));
            else alert("Refresh error: " + JSON.stringify(j));
        } catch (e) {
            console.error(e);
            alert("Refresh request failed");
        } finally {
            refreshBtn.disabled = false;
            fetchStats();
        }
    });

    startBtn && startBtn.addEventListener("click", async function () {
        startBtn.disabled = true;
        try {
            const res = await fetch("/api/worker/start", { method: "POST", credentials: "same-origin" });
            const j = await res.json();
            if (res.ok) alert("Worker start requested");
            else alert("Worker start failed: " + JSON.stringify(j));
        } catch (e) {
            console.error(e);
            alert("Worker start request failed");
        } finally {
            startBtn.disabled = false;
            fetchStats();
        }
    });

    stopBtn && stopBtn.addEventListener("click", async function () {
        stopBtn.disabled = true;
        try {
            const res = await fetch("/api/worker/stop", { method: "POST", credentials: "same-origin" });
            const j = await res.json();
            if (res.ok) alert("Worker stop requested");
            else alert("Worker stop failed: " + JSON.stringify(j));
        } catch (e) {
            console.error(e);
            alert("Worker stop request failed");
        } finally {
            stopBtn.disabled = false;
            fetchStats();
        }
    });

    enqueueForm && enqueueForm.addEventListener("submit", async function (ev) {
        ev.preventDefault();
        const url = enqueueUrl.value.trim();
        if (!url) return alert("URL required");
        document.getElementById("enqueue-btn").disabled = true;
        try {
            const payload = { url: url, source: "manual" };
            const res = await fetch("/api/enqueue", {
                method: "POST",
                credentials: "same-origin",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });
            const j = await res.json();
            if (res.status === 201 || j.ok) {
                alert("Enqueued: " + (j.result?.url || url));
                enqueueUrl.value = "";
            } else {
                alert("Enqueue failed: " + JSON.stringify(j));
            }
        } catch (e) {
            console.error(e);
            alert("Enqueue request failed");
        } finally {
            document.getElementById("enqueue-btn").disabled = false;
            fetchStats();
        }
    });

    // initial fetch and periodic refresh
    fetchStats();
    setInterval(fetchStats, 30000);
})();
