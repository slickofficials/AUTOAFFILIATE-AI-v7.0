// dashboard.js — Production-ready for AutoAffiliate dashboard

document.addEventListener("DOMContentLoaded", () => {
    const refreshBtn = document.getElementById("refresh-btn");
    const startWorkerBtn = document.getElementById("start-worker-btn");
    const stopWorkerBtn = document.getElementById("stop-worker-btn");
    const statsSection = document.getElementById("stats");
    const enqueueForm = document.getElementById("enqueue-form");
    const enqueueInput = document.getElementById("enqueue-url");
    const enqueueBtn = document.getElementById("enqueue-btn");

    // Helper: Fetch wrapper with error logging
    const apiFetch = async (url, options = {}) => {
        try {
            const res = await fetch(url, options);
            const data = await res.json();
            return data;
        } catch (err) {
            console.error(`API call failed: ${url}`, err);
            alert(`API call failed: ${err}`);
        }
    };

    // --- Update stats ---
    const updateStats = async () => {
        const data = await apiFetch("/api/stats");
        if (!data) return;
        statsSection.innerHTML = `
            <p>Total posts: ${data.total}</p>
            <p>Posted: ${data.posted}</p>
            <p>Pending: ${data.pending}</p>
        `;
    };

    // --- Refresh all sources ---
    refreshBtn.addEventListener("click", async () => {
        refreshBtn.disabled = true;
        refreshBtn.innerText = "Refreshing...";
        const res = await apiFetch("/api/refresh", { method: "POST" });
        console.log("Refresh result:", res);
        refreshBtn.disabled = false;
        refreshBtn.innerText = "Refresh Sources";
        updateStats();
    });

    // --- Start worker ---
    startWorkerBtn.addEventListener("click", async () => {
        startWorkerBtn.disabled = true;
        startWorkerBtn.innerText = "Starting...";
        const res = await apiFetch("/api/worker/start", { method: "POST" });
        console.log("Worker start:", res);
        startWorkerBtn.disabled = false;
        startWorkerBtn.innerText = "Start Worker";
        updateStats();
    });

    // --- Stop worker ---
    stopWorkerBtn.addEventListener("click", async () => {
        stopWorkerBtn.disabled = true;
        stopWorkerBtn.innerText = "Stopping...";
        const res = await apiFetch("/api/worker/stop", { method: "POST" });
        console.log("Worker stop:", res);
        stopWorkerBtn.disabled = false;
        stopWorkerBtn.innerText = "Stop Worker";
        updateStats();
    });

    // --- Enqueue single URL ---
    enqueueForm.addEventListener("submit", async (e) => {
        e.preventDefault();
        const url = enqueueInput.value.trim();
        if (!url) return alert("Please enter a URL");
        enqueueBtn.disabled = true;
        enqueueBtn.innerText = "Enqueuing...";
        const res = await apiFetch("/api/enqueue", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ url }),
        });
        console.log("Enqueue result:", res);
        enqueueBtn.disabled = false;
        enqueueBtn.innerText = "Add URL";
        enqueueInput.value = "";
        updateStats();
    });

    // --- Auto-refresh stats every 15s ---
    setInterval(updateStats, 15000);
    updateStats();
});
