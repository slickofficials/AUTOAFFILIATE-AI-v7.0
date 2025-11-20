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
