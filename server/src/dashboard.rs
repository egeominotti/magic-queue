use axum::{response::Html, routing::get, Router};

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MagicQueue Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    colors: {
                        primary: '#818cf8',
                        dark: {
                            900: '#0f172a',
                            800: '#1e293b',
                            700: '#334155',
                            600: '#475569',
                        }
                    }
                }
            }
        }
    </script>
</head>
<body class="bg-dark-900 text-slate-200 min-h-screen">
    <div class="max-w-7xl mx-auto px-4 py-8">
        <!-- Header -->
        <div class="flex items-center justify-between mb-8">
            <div class="flex items-center gap-3">
                <div class="w-10 h-10 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-lg flex items-center justify-center">
                    <svg class="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/>
                    </svg>
                </div>
                <h1 class="text-2xl font-bold text-white">MagicQueue</h1>
            </div>
            <div class="flex items-center gap-2">
                <span class="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium bg-green-500/10 text-green-400 border border-green-500/20">
                    <span class="w-2 h-2 bg-green-400 rounded-full animate-pulse"></span>
                    Live
                </span>
            </div>
        </div>

        <!-- Stats Cards -->
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700 hover:border-blue-500/50 transition-colors">
                <div class="flex items-center justify-between mb-3">
                    <span class="text-slate-400 text-sm font-medium">Queued Jobs</span>
                    <div class="w-8 h-8 bg-blue-500/10 rounded-lg flex items-center justify-center">
                        <svg class="w-4 h-4 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/>
                        </svg>
                    </div>
                </div>
                <p class="text-3xl font-bold text-blue-400" id="queued">-</p>
            </div>

            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700 hover:border-green-500/50 transition-colors">
                <div class="flex items-center justify-between mb-3">
                    <span class="text-slate-400 text-sm font-medium">Processing</span>
                    <div class="w-8 h-8 bg-green-500/10 rounded-lg flex items-center justify-center">
                        <svg class="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/>
                        </svg>
                    </div>
                </div>
                <p class="text-3xl font-bold text-green-400" id="processing">-</p>
            </div>

            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700 hover:border-yellow-500/50 transition-colors">
                <div class="flex items-center justify-between mb-3">
                    <span class="text-slate-400 text-sm font-medium">Delayed</span>
                    <div class="w-8 h-8 bg-yellow-500/10 rounded-lg flex items-center justify-center">
                        <svg class="w-4 h-4 text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>
                        </svg>
                    </div>
                </div>
                <p class="text-3xl font-bold text-yellow-400" id="delayed">-</p>
            </div>

            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700 hover:border-red-500/50 transition-colors">
                <div class="flex items-center justify-between mb-3">
                    <span class="text-slate-400 text-sm font-medium">Failed (DLQ)</span>
                    <div class="w-8 h-8 bg-red-500/10 rounded-lg flex items-center justify-center">
                        <svg class="w-4 h-4 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
                        </svg>
                    </div>
                </div>
                <p class="text-3xl font-bold text-red-400" id="dlq">-</p>
            </div>
        </div>

        <!-- Metrics Cards -->
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700">
                <span class="text-slate-400 text-sm font-medium">Total Pushed</span>
                <p class="text-2xl font-bold text-white mt-2" id="total_pushed">-</p>
            </div>
            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700">
                <span class="text-slate-400 text-sm font-medium">Total Completed</span>
                <p class="text-2xl font-bold text-green-400 mt-2" id="total_completed">-</p>
            </div>
            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700">
                <span class="text-slate-400 text-sm font-medium">Total Failed</span>
                <p class="text-2xl font-bold text-red-400 mt-2" id="total_failed">-</p>
            </div>
            <div class="bg-dark-800 rounded-xl p-5 border border-dark-700">
                <span class="text-slate-400 text-sm font-medium">Avg Latency</span>
                <p class="text-2xl font-bold text-indigo-400 mt-2" id="avg_latency">-</p>
            </div>
        </div>

        <!-- Queues Table -->
        <div class="bg-dark-800 rounded-xl border border-dark-700 overflow-hidden">
            <div class="px-6 py-4 border-b border-dark-700 flex items-center justify-between">
                <h2 class="text-lg font-semibold text-white">Queues</h2>
                <span class="text-xs text-slate-500">Auto-refresh every 2s</span>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full">
                    <thead>
                        <tr class="border-b border-dark-700">
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">Name</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">Pending</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">Processing</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">DLQ</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">Rate Limit</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">Concurrency</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">Status</th>
                            <th class="px-6 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider">Actions</th>
                        </tr>
                    </thead>
                    <tbody id="queues-table" class="divide-y divide-dark-700">
                        <tr>
                            <td colspan="8" class="px-6 py-8 text-center text-slate-500">
                                <div class="flex flex-col items-center gap-2">
                                    <svg class="w-8 h-8 text-slate-600 animate-spin" fill="none" viewBox="0 0 24 24">
                                        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                                        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                    </svg>
                                    <span>Loading queues...</span>
                                </div>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Footer -->
        <div class="mt-8 text-center text-slate-500 text-sm">
            MagicQueue Server &bull; High-performance job queue
        </div>
    </div>

    <script>
        const API_BASE = window.location.origin;

        async function fetchStats() {
            try {
                const res = await fetch(`${API_BASE}/stats`);
                const data = await res.json();
                if (data.ok) {
                    document.getElementById('queued').textContent = data.data.queued.toLocaleString();
                    document.getElementById('processing').textContent = data.data.processing.toLocaleString();
                    document.getElementById('delayed').textContent = data.data.delayed.toLocaleString();
                    document.getElementById('dlq').textContent = data.data.dlq.toLocaleString();
                }
            } catch (e) { console.error('Failed to fetch stats:', e); }
        }

        async function fetchMetrics() {
            try {
                const res = await fetch(`${API_BASE}/metrics`);
                const data = await res.json();
                if (data.ok) {
                    document.getElementById('total_pushed').textContent = data.data.total_pushed.toLocaleString();
                    document.getElementById('total_completed').textContent = data.data.total_completed.toLocaleString();
                    document.getElementById('total_failed').textContent = data.data.total_failed.toLocaleString();
                    document.getElementById('avg_latency').textContent = data.data.avg_latency_ms.toFixed(2) + ' ms';
                }
            } catch (e) { console.error('Failed to fetch metrics:', e); }
        }

        async function fetchQueues() {
            try {
                const res = await fetch(`${API_BASE}/queues`);
                const data = await res.json();
                const tbody = document.getElementById('queues-table');

                if (data.ok && data.data.length > 0) {
                    tbody.innerHTML = data.data.map(q => `
                        <tr class="hover:bg-dark-700/50 transition-colors">
                            <td class="px-6 py-4">
                                <span class="font-medium text-white">${q.name}</span>
                            </td>
                            <td class="px-6 py-4 text-slate-300">${q.pending}</td>
                            <td class="px-6 py-4 text-slate-300">${q.processing}</td>
                            <td class="px-6 py-4">
                                ${q.dlq > 0
                                    ? `<span class="text-red-400 font-medium">${q.dlq}</span>`
                                    : `<span class="text-slate-500">0</span>`
                                }
                            </td>
                            <td class="px-6 py-4 text-slate-300">${q.rate_limit || '<span class="text-slate-500">-</span>'}</td>
                            <td class="px-6 py-4 text-slate-300">${q.concurrency_limit || '<span class="text-slate-500">-</span>'}</td>
                            <td class="px-6 py-4">
                                ${q.paused
                                    ? `<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-yellow-500/10 text-yellow-400 border border-yellow-500/20">
                                        <span class="w-1.5 h-1.5 bg-yellow-400 rounded-full"></span>
                                        Paused
                                       </span>`
                                    : `<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-green-500/10 text-green-400 border border-green-500/20">
                                        <span class="w-1.5 h-1.5 bg-green-400 rounded-full"></span>
                                        Active
                                       </span>`
                                }
                            </td>
                            <td class="px-6 py-4">
                                <div class="flex items-center gap-2">
                                    ${q.paused
                                        ? `<button onclick="resumeQueue('${q.name}')" class="px-3 py-1.5 text-xs font-medium rounded-lg bg-green-500/10 text-green-400 hover:bg-green-500/20 border border-green-500/20 transition-colors">
                                            Resume
                                           </button>`
                                        : `<button onclick="pauseQueue('${q.name}')" class="px-3 py-1.5 text-xs font-medium rounded-lg bg-yellow-500/10 text-yellow-400 hover:bg-yellow-500/20 border border-yellow-500/20 transition-colors">
                                            Pause
                                           </button>`
                                    }
                                    ${q.dlq > 0
                                        ? `<button onclick="retryDlq('${q.name}')" class="px-3 py-1.5 text-xs font-medium rounded-lg bg-blue-500/10 text-blue-400 hover:bg-blue-500/20 border border-blue-500/20 transition-colors">
                                            Retry DLQ
                                           </button>`
                                        : ''
                                    }
                                </div>
                            </td>
                        </tr>
                    `).join('');
                } else if (data.ok) {
                    tbody.innerHTML = `
                        <tr>
                            <td colspan="8" class="px-6 py-12 text-center">
                                <div class="flex flex-col items-center gap-3">
                                    <div class="w-12 h-12 bg-dark-700 rounded-full flex items-center justify-center">
                                        <svg class="w-6 h-6 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"/>
                                        </svg>
                                    </div>
                                    <div>
                                        <p class="text-slate-400 font-medium">No queues yet</p>
                                        <p class="text-slate-500 text-sm">Push a job to create your first queue</p>
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `;
                }
            } catch (e) { console.error('Failed to fetch queues:', e); }
        }

        async function pauseQueue(name) {
            await fetch(`${API_BASE}/queues/${name}/pause`, { method: 'POST' });
            refresh();
        }

        async function resumeQueue(name) {
            await fetch(`${API_BASE}/queues/${name}/resume`, { method: 'POST' });
            refresh();
        }

        async function retryDlq(name) {
            await fetch(`${API_BASE}/queues/${name}/dlq/retry`, { method: 'POST' });
            refresh();
        }

        function refresh() {
            fetchStats();
            fetchMetrics();
            fetchQueues();
        }

        refresh();
        setInterval(refresh, 2000);
    </script>
</body>
</html>
"#;

pub fn dashboard_routes() -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/dashboard", get(dashboard))
}

async fn dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}
