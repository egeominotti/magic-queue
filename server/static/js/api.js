// ============================================================================
// FlashQ Dashboard - API Functions
// ============================================================================

window.DashboardAPI = (function() {
    const API = window.location.origin;

    return {
        API,

        async fetchStats() {
            try {
                const res = await fetch(`${API}/stats`);
                const data = await res.json();
                return data.ok ? data.data : null;
            } catch (e) {
                console.error('Stats error:', e);
                return null;
            }
        },

        async fetchMetrics() {
            try {
                const res = await fetch(`${API}/metrics`);
                const data = await res.json();
                return data.ok ? data.data : null;
            } catch (e) {
                console.error('Metrics error:', e);
                return null;
            }
        },

        async fetchQueues() {
            try {
                const res = await fetch(`${API}/queues`);
                const data = await res.json();
                return data.ok ? (data.data || []) : [];
            } catch (e) {
                console.error('Queues error:', e);
                return [];
            }
        },

        async fetchJobs(params = {}) {
            const { search, queue, state, limit = 20, offset = 0 } = params;

            if (search) {
                try {
                    const res = await fetch(`${API}/jobs/${search}`);
                    const data = await res.json();
                    return data.ok && data.data ? [data.data] : [];
                } catch (e) {
                    return [];
                }
            }

            let url = `${API}/jobs?limit=${limit}&offset=${offset}`;
            if (queue) url += `&queue=${encodeURIComponent(queue)}`;
            if (state) url += `&state=${encodeURIComponent(state)}`;

            try {
                const res = await fetch(url);
                const data = await res.json();
                return data.ok ? (data.data || []) : [];
            } catch (e) {
                return [];
            }
        },

        async fetchJobDetail(id) {
            try {
                const res = await fetch(`${API}/jobs/${id}`);
                const data = await res.json();
                return data.ok ? data.data : null;
            } catch (e) {
                console.error('Job detail error:', e);
                return null;
            }
        },

        async fetchCrons() {
            try {
                const res = await fetch(`${API}/crons`);
                const data = await res.json();
                return data.ok ? (data.data || []) : [];
            } catch (e) {
                console.error('Crons error:', e);
                return [];
            }
        },

        async fetchWorkers() {
            try {
                const res = await fetch(`${API}/workers`);
                const data = await res.json();
                return data.ok ? (data.data || []) : [];
            } catch (e) {
                console.error('Workers error:', e);
                return [];
            }
        },

        async fetchMetricsHistory() {
            try {
                const res = await fetch(`${API}/metrics/history`);
                const data = await res.json();
                return data.ok ? (data.data || []) : [];
            } catch (e) {
                console.error('Metrics history error:', e);
                return [];
            }
        },

        async fetchSettings() {
            try {
                const res = await fetch(`${API}/settings`);
                const data = await res.json();
                return data.ok ? data.data : {};
            } catch (e) {
                return {};
            }
        },

        // Queue actions
        async pauseQueue(name) {
            await fetch(`${API}/queues/${encodeURIComponent(name)}/pause`, { method: 'POST' });
        },

        async resumeQueue(name) {
            await fetch(`${API}/queues/${encodeURIComponent(name)}/resume`, { method: 'POST' });
        },

        async retryDlq(name) {
            await fetch(`${API}/queues/${encodeURIComponent(name)}/dlq/retry`, { method: 'POST' });
        },

        async setRateLimit(name, limit) {
            await fetch(`${API}/queues/${encodeURIComponent(name)}/rate-limit`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ limit: parseInt(limit) })
            });
        },

        async clearRateLimit(name) {
            await fetch(`${API}/queues/${encodeURIComponent(name)}/rate-limit`, { method: 'DELETE' });
        },

        async setConcurrency(name, limit) {
            await fetch(`${API}/queues/${encodeURIComponent(name)}/concurrency`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ limit: parseInt(limit) })
            });
        },

        async clearConcurrency(name) {
            await fetch(`${API}/queues/${encodeURIComponent(name)}/concurrency`, { method: 'DELETE' });
        },

        // Job actions
        async cancelJob(id) {
            await fetch(`${API}/jobs/${id}/cancel`, { method: 'POST' });
        },

        async retryJob(id) {
            await fetch(`${API}/jobs/${id}/fail`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Manual retry' })
            });
        },

        // Cron actions
        async createCron(name, params) {
            const res = await fetch(`${API}/crons/${encodeURIComponent(name)}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(params)
            });
            return res.json();
        },

        async deleteCron(name) {
            await fetch(`${API}/crons/${encodeURIComponent(name)}`, { method: 'DELETE' });
        },

        // Settings actions
        async testDbConnection(url) {
            const res = await fetch(`${API}/settings/test-db`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url })
            });
            return res.json();
        },

        async saveDbSettings(url) {
            const res = await fetch(`${API}/settings/database`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url })
            });
            return res.json();
        },

        async saveAuthSettings(tokens) {
            const res = await fetch(`${API}/settings/auth`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tokens })
            });
            return res.json();
        },

        async saveQueueDefaults(settings) {
            const res = await fetch(`${API}/settings/queue-defaults`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(settings)
            });
            return res.json();
        },

        async saveCleanupSettings(settings) {
            const res = await fetch(`${API}/settings/cleanup`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(settings)
            });
            return res.json();
        },

        async runCleanup() {
            const res = await fetch(`${API}/settings/cleanup/run`, { method: 'POST' });
            return res.json();
        },

        async clearAllQueues() {
            const res = await fetch(`${API}/server/clear-queues`, { method: 'POST' });
            return res.json();
        },

        async clearAllDlq() {
            const res = await fetch(`${API}/server/clear-dlq`, { method: 'POST' });
            return res.json();
        },

        async clearCompletedJobs() {
            const res = await fetch(`${API}/server/clear-completed`, { method: 'POST' });
            return res.json();
        },

        async resetMetrics() {
            const res = await fetch(`${API}/server/reset-metrics`, { method: 'POST' });
            return res.json();
        },

        async shutdownServer() {
            const res = await fetch(`${API}/server/shutdown`, { method: 'POST' });
            return res.json();
        },

        async restartServer() {
            const res = await fetch(`${API}/server/restart`, { method: 'POST' });
            return res.json();
        },

        // Cluster APIs
        async fetchHealth() {
            try {
                const res = await fetch(`${API}/health`);
                const data = await res.json();
                return data.ok ? data.data : null;
            } catch (e) {
                console.error('Health error:', e);
                return null;
            }
        },

        async fetchClusterNodes() {
            try {
                const res = await fetch(`${API}/cluster/nodes`);
                const data = await res.json();
                return data.ok ? (data.data || []) : [];
            } catch (e) {
                console.error('Cluster nodes error:', e);
                return [];
            }
        }
    };
})();
