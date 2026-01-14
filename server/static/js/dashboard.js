// ============================================================================
// FlashQ Dashboard - Main Controller
// ============================================================================

(function() {
    'use strict';

    const { formatTime, formatNumber, escapeHtml, getBadgeClass } = DashboardUtils;
    const API = DashboardAPI;
    const Charts = DashboardCharts;
    const Settings = DashboardSettings;

    // ========================================================================
    // State
    // ========================================================================

    const state = {
        currentSection: 'overview',
        stats: { queued: 0, processing: 0, delayed: 0, dlq: 0 },
        metrics: { total_pushed: 0, total_completed: 0, total_failed: 0, jobs_per_second: 0 },
        queues: [],
        jobs: [],
        crons: [],
        workers: [],
        metricsHistory: [],
        connected: false,
        selectedJobs: new Set(),
        jobPage: 0,
        chartRange: '5m',
        // Cluster state
        clusterEnabled: false,
        currentNodeId: null,
        isLeader: false,
        clusterNodes: []
    };

    let dashboardSocket = null;
    let reconnectTimeout = null;

    // ========================================================================
    // WebSocket Real-time Connection
    // ========================================================================

    function connectWebSocket() {
        if (dashboardSocket && dashboardSocket.readyState === WebSocket.OPEN) return;

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/dashboard`;

        dashboardSocket = new WebSocket(wsUrl);

        dashboardSocket.onopen = () => {
            state.connected = true;
            updateConnectionUI();
            console.log('Dashboard WebSocket connected');
        };

        dashboardSocket.onmessage = (e) => {
            try {
                const data = JSON.parse(e.data);
                // Update all state from WebSocket
                if (data.stats) {
                    state.stats = data.stats;
                    updateStatsUI();
                }
                if (data.metrics) {
                    state.metrics = data.metrics;
                    updateMetricsUI();
                }
                if (data.queues) {
                    state.queues = data.queues;
                    updateQueuesUI();
                    updateQueueFilterOptions();
                }
                if (data.workers) {
                    state.workers = data.workers;
                    updateWorkersUI();
                }
                if (data.metrics_history) {
                    state.metricsHistory = data.metrics_history;
                    Charts.updateCharts(state.metricsHistory, state.chartRange);
                }
                // Update timestamp
                document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
            } catch (err) {
                console.error('WebSocket message error:', err);
            }
        };

        dashboardSocket.onclose = () => {
            state.connected = false;
            updateConnectionUI();
            // Reconnect after 2 seconds
            reconnectTimeout = setTimeout(connectWebSocket, 2000);
        };

        dashboardSocket.onerror = () => {
            state.connected = false;
            updateConnectionUI();
        };
    }

    // ========================================================================
    // Data Fetching (for on-demand data like jobs, crons)
    // ========================================================================

    async function fetchStats() {
        // Stats now come from WebSocket, but keep for manual refresh
        const data = await API.fetchStats();
        if (data) {
            state.stats = data;
            updateStatsUI();
        }
    }

    async function fetchMetrics() {
        const data = await API.fetchMetrics();
        if (data) {
            state.metrics = data;
            updateMetricsUI();
        }
    }

    async function fetchQueues() {
        state.queues = await API.fetchQueues();
        updateQueuesUI();
        updateQueueFilterOptions();
    }

    async function fetchJobs() {
        const searchInput = document.getElementById('job-search');
        const search = searchInput?.value?.trim() || '';
        const queueFilter = document.getElementById('job-queue-filter')?.value || '';
        const stateFilter = document.getElementById('job-state-filter')?.value || '';

        state.jobs = await API.fetchJobs({
            search,
            queue: queueFilter,
            state: stateFilter,
            limit: 20,
            offset: state.jobPage * 20
        });
        updateJobsUI();
    }

    async function fetchCrons() {
        state.crons = await API.fetchCrons();
        updateCronsUI();
    }

    async function fetchWorkers() {
        state.workers = await API.fetchWorkers();
        updateWorkersUI();
    }

    async function fetchMetricsHistory() {
        state.metricsHistory = await API.fetchMetricsHistory();
        Charts.updateCharts(state.metricsHistory, state.chartRange);
    }

    async function fetchSettings() {
        const data = await API.fetchSettings();
        Settings.updateUI(data);
    }

    async function fetchClusterStatus() {
        const health = await API.fetchHealth();
        if (health) {
            state.clusterEnabled = health.cluster_enabled;
            state.currentNodeId = health.node_id;
            state.isLeader = health.is_leader;
        }
        if (state.clusterEnabled) {
            state.clusterNodes = await API.fetchClusterNodes();
        }
        updateClusterUI();
    }

    // ========================================================================
    // UI Updates
    // ========================================================================

    function updateStatsUI() {
        const s = state.stats;
        const total = Math.max(s.queued + s.processing + s.delayed + s.dlq, 1);

        document.getElementById('stat-queued').textContent = formatNumber(s.queued);
        document.getElementById('stat-processing').textContent = formatNumber(s.processing);
        document.getElementById('stat-delayed').textContent = formatNumber(s.delayed);
        document.getElementById('stat-dlq').textContent = formatNumber(s.dlq);

        document.getElementById('bar-queued').style.width = `${(s.queued / total) * 100}%`;
        document.getElementById('bar-processing').style.width = `${(s.processing / total) * 100}%`;
        document.getElementById('bar-delayed').style.width = `${(s.delayed / total) * 100}%`;
        document.getElementById('bar-dlq').style.width = `${(s.dlq / total) * 100}%`;

        Charts.updateDistribution(s);
    }

    function updateMetricsUI() {
        const m = state.metrics;
        document.getElementById('metric-pushed').textContent = formatNumber(m.total_pushed);
        document.getElementById('metric-completed').textContent = formatNumber(m.total_completed);
        document.getElementById('metric-failed').textContent = formatNumber(m.total_failed);
        document.getElementById('metric-throughput').textContent = (m.jobs_per_second?.toFixed(1) || '0') + '/s';
    }

    function updateQueuesUI() {
        const search = document.getElementById('queue-search')?.value?.toLowerCase() || '';
        const queues = search ? state.queues.filter(q => q.name.toLowerCase().includes(search)) : state.queues;

        // Overview queues (first 5)
        const overviewBody = document.getElementById('overview-queues');
        if (overviewBody) {
            overviewBody.innerHTML = queues.slice(0, 5).map(q => `
                <tr class="table-row">
                    <td class="px-6 py-4">
                        <div class="flex items-center gap-3">
                            <div class="w-2 h-2 rounded-full ${q.paused ? 'bg-amber-500' : 'bg-emerald-500'}"></div>
                            <span class="font-medium text-white">${escapeHtml(q.name)}</span>
                        </div>
                    </td>
                    <td class="px-6 py-4 text-slate-300 font-mono">${q.pending}</td>
                    <td class="px-6 py-4 text-slate-300 font-mono">${q.processing}</td>
                    <td class="px-6 py-4">${q.dlq > 0 ? `<span class="text-rose-400 font-medium">${q.dlq}</span>` : `<span class="text-slate-500">0</span>`}</td>
                    <td class="px-6 py-4"><span class="badge ${q.paused ? 'badge-paused' : 'badge-active'}">${q.paused ? 'Paused' : 'Active'}</span></td>
                    <td class="px-6 py-4 text-right">
                        <div class="flex items-center justify-end gap-2">
                            ${q.paused
                                ? `<button onclick="window.dashboard.resumeQueue('${escapeHtml(q.name)}')" class="btn btn-success text-xs py-1 px-2">Resume</button>`
                                : `<button onclick="window.dashboard.pauseQueue('${escapeHtml(q.name)}')" class="btn btn-ghost text-xs py-1 px-2">Pause</button>`}
                            ${q.dlq > 0 ? `<button onclick="window.dashboard.retryDlq('${escapeHtml(q.name)}')" class="btn btn-primary text-xs py-1 px-2">Retry DLQ</button>` : ''}
                        </div>
                    </td>
                </tr>
            `).join('') || '<tr><td colspan="6" class="px-6 py-12 text-center text-slate-500">No queues found</td></tr>';
        }

        // Full queues table
        const queuesBody = document.getElementById('queues-table');
        if (queuesBody) {
            queuesBody.innerHTML = queues.map(q => `
                <tr class="table-row">
                    <td class="px-6 py-4">
                        <div class="flex items-center gap-3">
                            <div class="w-2 h-2 rounded-full ${q.paused ? 'bg-amber-500' : 'bg-emerald-500'}"></div>
                            <span class="font-medium text-white">${escapeHtml(q.name)}</span>
                        </div>
                    </td>
                    <td class="px-6 py-4 text-slate-300 font-mono">${q.pending}</td>
                    <td class="px-6 py-4 text-slate-300 font-mono">${q.processing}</td>
                    <td class="px-6 py-4">${q.dlq > 0 ? `<span class="text-rose-400 font-medium">${q.dlq}</span>` : `<span class="text-slate-500">0</span>`}</td>
                    <td class="px-6 py-4 text-slate-400">${q.rate_limit || '-'}</td>
                    <td class="px-6 py-4 text-slate-400">${q.concurrency || '-'}</td>
                    <td class="px-6 py-4"><span class="badge ${q.paused ? 'badge-paused' : 'badge-active'}">${q.paused ? 'Paused' : 'Active'}</span></td>
                    <td class="px-6 py-4 text-right">
                        <div class="flex items-center justify-end gap-2">
                            ${q.paused
                                ? `<button onclick="window.dashboard.resumeQueue('${escapeHtml(q.name)}')" class="btn btn-success text-xs py-1 px-2">Resume</button>`
                                : `<button onclick="window.dashboard.pauseQueue('${escapeHtml(q.name)}')" class="btn btn-ghost text-xs py-1 px-2">Pause</button>`}
                            ${q.dlq > 0 ? `<button onclick="window.dashboard.retryDlq('${escapeHtml(q.name)}')" class="btn btn-primary text-xs py-1 px-2">Retry DLQ</button>` : ''}
                            <button onclick="window.dashboard.openQueueSettings('${escapeHtml(q.name)}')" class="btn btn-ghost text-xs py-1 px-2">
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
                            </button>
                        </div>
                    </td>
                </tr>
            `).join('') || '<tr><td colspan="8" class="px-6 py-12 text-center text-slate-500">No queues found</td></tr>';
        }
    }

    function updateJobsUI() {
        const jobsBody = document.getElementById('jobs-table');
        if (!jobsBody) return;

        jobsBody.innerHTML = state.jobs.map(j => `
            <tr class="table-row">
                <td class="px-4 py-3"><input type="checkbox" ${state.selectedJobs.has(j.id) ? 'checked' : ''} onchange="window.dashboard.toggleJob('${j.id}')" /></td>
                <td class="px-4 py-3 font-mono text-sm text-slate-300">${escapeHtml(j.id)}</td>
                <td class="px-4 py-3 font-medium text-white">${escapeHtml(j.queue)}</td>
                <td class="px-4 py-3"><span class="badge ${getBadgeClass(j.state)}">${escapeHtml(j.state)}</span></td>
                <td class="px-4 py-3 text-slate-300">${j.priority}</td>
                <td class="px-4 py-3 text-slate-300">${j.attempts}${j.max_attempts > 0 ? '/' + j.max_attempts : ''}</td>
                <td class="px-4 py-3 text-slate-400 text-sm">${formatTime(j.created_at)}</td>
                <td class="px-4 py-3 text-right"><button onclick="window.dashboard.fetchJobDetail('${j.id}')" class="btn btn-ghost text-xs py-1 px-2">View</button></td>
            </tr>
        `).join('') || '<tr><td colspan="8" class="px-6 py-12 text-center text-slate-500">No jobs found</td></tr>';

        document.getElementById('jobs-count').textContent = `${state.jobs.length} jobs`;
        document.getElementById('page-info').textContent = `Page ${state.jobPage + 1}`;
        document.getElementById('prev-page-btn').disabled = state.jobPage === 0;
        document.getElementById('next-page-btn').disabled = state.jobs.length < 20;
        updateBulkActionsUI();
    }

    function updateBulkActionsUI() {
        const bulkActions = document.getElementById('bulk-actions');
        const selectedCount = document.getElementById('selected-count');
        if (state.selectedJobs.size > 0) {
            bulkActions.classList.remove('hidden');
            selectedCount.textContent = state.selectedJobs.size;
        } else {
            bulkActions.classList.add('hidden');
        }
    }

    function updateCronsUI() {
        const cronsBody = document.getElementById('crons-table');
        if (!cronsBody) return;

        cronsBody.innerHTML = state.crons.map(c => `
            <tr class="table-row">
                <td class="px-6 py-4 font-medium text-white">${escapeHtml(c.name)}</td>
                <td class="px-6 py-4 text-slate-300">${escapeHtml(c.queue)}</td>
                <td class="px-6 py-4 font-mono text-sm text-slate-300">${escapeHtml(c.schedule)}</td>
                <td class="px-6 py-4 text-slate-300">${c.priority}</td>
                <td class="px-6 py-4 text-slate-400 text-sm">${formatTime(c.next_run)}</td>
                <td class="px-6 py-4 text-xs text-slate-400 max-w-xs truncate font-mono">${escapeHtml(JSON.stringify(c.data))}</td>
                <td class="px-6 py-4 text-right"><button onclick="window.dashboard.deleteCron('${escapeHtml(c.name)}')" class="btn btn-danger text-xs py-1 px-2">Delete</button></td>
            </tr>
        `).join('') || '<tr><td colspan="7" class="px-6 py-12 text-center text-slate-500">No cron jobs configured</td></tr>';
    }

    function updateWorkersUI() {
        const workersBody = document.getElementById('workers-table');
        if (!workersBody) return;

        const now = Date.now();
        workersBody.innerHTML = state.workers.map(w => {
            const isOnline = (now - w.last_heartbeat) < 30000;
            return `
                <tr class="table-row">
                    <td class="px-6 py-4 font-mono text-sm text-white">${escapeHtml(w.id)}</td>
                    <td class="px-6 py-4 text-slate-300">${w.queues?.join(', ') || '-'}</td>
                    <td class="px-6 py-4 text-slate-300">${w.concurrency}</td>
                    <td class="px-6 py-4 font-mono text-slate-300">${formatNumber(w.jobs_processed || 0)}</td>
                    <td class="px-6 py-4 text-slate-400 text-sm">${formatTime(w.last_heartbeat)}</td>
                    <td class="px-6 py-4"><span class="badge ${isOnline ? 'badge-active' : 'badge-failed'}">${isOnline ? 'Online' : 'Offline'}</span></td>
                </tr>
            `;
        }).join('') || '<tr><td colspan="6" class="px-6 py-12 text-center text-slate-500">No workers registered</td></tr>';
    }

    function updateConnectionUI() {
        const status = document.getElementById('connection-status');
        const text = document.getElementById('connection-text');
        if (status && text) {
            if (state.connected) {
                status.className = 'status-pulse online w-2 h-2 bg-emerald-500 rounded-full';
                text.className = 'text-xs text-emerald-400';
                text.textContent = 'Connected';
            } else {
                status.className = 'status-pulse offline w-2 h-2 bg-rose-500 rounded-full';
                text.className = 'text-xs text-rose-400';
                text.textContent = 'Disconnected';
            }
        }
    }

    function updateClusterUI() {
        const panel = document.getElementById('cluster-panel');
        const sidebarInfo = document.getElementById('sidebar-cluster-info');

        if (!state.clusterEnabled) {
            if (panel) panel.classList.add('hidden');
            if (sidebarInfo) sidebarInfo.classList.add('hidden');
            return;
        }

        // Show cluster panels
        if (panel) panel.classList.remove('hidden');
        if (sidebarInfo) sidebarInfo.classList.remove('hidden');

        // Update sidebar cluster info (always visible)
        const sidebarRole = document.getElementById('sidebar-node-role');
        const sidebarNodeId = document.getElementById('sidebar-node-id');
        const sidebarCount = document.getElementById('sidebar-cluster-count');
        if (sidebarRole) {
            sidebarRole.textContent = state.isLeader ? 'Leader' : 'Follower';
            sidebarRole.className = state.isLeader
                ? 'px-2 py-0.5 bg-emerald-500/20 text-emerald-400 rounded text-xs font-medium'
                : 'px-2 py-0.5 bg-slate-500/20 text-slate-400 rounded text-xs font-medium';
        }
        if (sidebarNodeId) sidebarNodeId.textContent = state.currentNodeId || 'unknown';
        if (sidebarCount) sidebarCount.textContent = state.clusterNodes.length;

        // Update overview panel current node info
        const roleEl = document.getElementById('current-node-role');
        const nodeIdEl = document.getElementById('current-node-id');
        if (roleEl) {
            roleEl.textContent = state.isLeader ? 'Leader' : 'Follower';
            roleEl.className = state.isLeader
                ? 'px-2.5 py-1 bg-emerald-500/20 text-emerald-400 rounded-full text-xs font-medium'
                : 'px-2.5 py-1 bg-slate-500/20 text-slate-400 rounded-full text-xs font-medium';
        }
        if (nodeIdEl) nodeIdEl.textContent = state.currentNodeId || 'unknown';

        // Render nodes grid
        const grid = document.getElementById('cluster-nodes-grid');
        if (grid) {
            grid.innerHTML = state.clusterNodes.map(node => {
                const isCurrentNode = node.node_id === state.currentNodeId;
                const lastHeartbeatAge = Date.now() - node.last_heartbeat;
                const isOnline = lastHeartbeatAge < 15000;
                return `
                    <div class="glass-light rounded-lg p-4 ${isCurrentNode ? 'ring-2 ring-primary-500/50' : ''}">
                        <div class="flex items-center justify-between mb-3">
                            <div class="flex items-center gap-2">
                                <span class="w-2 h-2 rounded-full ${isOnline ? 'bg-emerald-500' : 'bg-rose-500'}"></span>
                                <span class="font-medium text-white">${escapeHtml(node.node_id)}</span>
                            </div>
                            <span class="px-2 py-0.5 rounded text-xs ${node.is_leader ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-500/20 text-slate-400'}">
                                ${node.is_leader ? 'Leader' : 'Follower'}
                            </span>
                        </div>
                        <div class="space-y-1 text-sm">
                            <div class="flex justify-between">
                                <span class="text-slate-500">Host</span>
                                <span class="text-slate-300">${escapeHtml(node.host)}:${node.port}</span>
                            </div>
                            <div class="flex justify-between">
                                <span class="text-slate-500">Last Heartbeat</span>
                                <span class="text-slate-300">${Math.round(lastHeartbeatAge / 1000)}s ago</span>
                            </div>
                        </div>
                        ${isCurrentNode ? '<div class="mt-2 text-xs text-primary-400">← This node</div>' : ''}
                    </div>
                `;
            }).join('') || '<div class="text-slate-500">No nodes found</div>';
        }
    }

    function updateQueueFilterOptions() {
        const select = document.getElementById('job-queue-filter');
        if (!select) return;
        const currentValue = select.value;
        select.innerHTML = '<option value="">All Queues</option>' + state.queues.map(q => `<option value="${escapeHtml(q.name)}">${escapeHtml(q.name)}</option>`).join('');
        select.value = currentValue;
    }


    // ========================================================================
    // Navigation
    // ========================================================================

    function showSection(section) {
        state.currentSection = section;
        document.querySelectorAll('.nav-item').forEach(item => item.classList.remove('active'));
        document.getElementById(`nav-${section}`)?.classList.add('active');
        document.querySelectorAll('section[id^="section-"]').forEach(s => s.classList.add('hidden'));
        document.getElementById(`section-${section}`)?.classList.remove('hidden');

        const titles = {
            overview: ['Overview', 'Real-time system monitoring'],
            queues: ['Queues', 'Manage and monitor queues'],
            jobs: ['Jobs Browser', 'Search and manage jobs'],
            crons: ['Cron Jobs', 'Schedule recurring tasks'],
            workers: ['Workers', 'Monitor connected workers'],
            analytics: ['Analytics', 'Performance metrics and charts'],
            settings: ['Settings', 'Server configuration and preferences']
        };
        document.getElementById('page-title').textContent = titles[section]?.[0] || section;
        document.getElementById('page-subtitle').textContent = titles[section]?.[1] || '';

        if (section === 'jobs') fetchJobs();
        else if (section === 'crons') fetchCrons();
        else if (section === 'workers') fetchWorkers();
        else if (section === 'analytics') fetchMetricsHistory();
        else if (section === 'settings') fetchSettings();
    }

    // ========================================================================
    // Modals
    // ========================================================================

    async function showJobModal(id) {
        const job = await API.fetchJobDetail(id);
        if (!job) return;

        const modal = document.getElementById('job-modal');
        const content = document.getElementById('job-modal-content');
        if (!modal || !content) return;

        const j = job.job || job;
        content.innerHTML = `
            <div class="space-y-6">
                <div class="grid grid-cols-2 gap-4">
                    <div class="space-y-1"><label class="text-xs text-slate-500">Job ID</label><p class="font-mono text-white">${escapeHtml(j.id || '-')}</p></div>
                    <div class="space-y-1"><label class="text-xs text-slate-500">State</label><p><span class="badge ${getBadgeClass(job.state)}">${escapeHtml(job.state || '-')}</span></p></div>
                    <div class="space-y-1"><label class="text-xs text-slate-500">Queue</label><p class="text-white">${escapeHtml(j.queue || '-')}</p></div>
                    <div class="space-y-1"><label class="text-xs text-slate-500">Priority</label><p class="text-white">${j.priority ?? '-'}</p></div>
                    <div class="space-y-1"><label class="text-xs text-slate-500">Attempts</label><p class="text-white">${j.attempts ?? '-'}${j.max_attempts > 0 ? ' / ' + j.max_attempts : ''}</p></div>
                    <div class="space-y-1"><label class="text-xs text-slate-500">Created At</label><p class="text-white">${formatTime(j.created_at)}</p></div>
                </div>
                <div class="space-y-2"><label class="text-xs text-slate-500">Data</label><pre class="p-4 bg-dark-900 rounded-lg text-xs text-slate-300 overflow-x-auto font-mono">${escapeHtml(JSON.stringify(j.data || {}, null, 2))}</pre></div>
                ${job.result ? `<div class="space-y-2"><label class="text-xs text-slate-500">Result</label><pre class="p-4 bg-dark-900 rounded-lg text-xs text-emerald-400 overflow-x-auto font-mono">${escapeHtml(JSON.stringify(job.result, null, 2))}</pre></div>` : ''}
            </div>
        `;
        modal.classList.remove('hidden');
    }

    function openQueueSettings(name) {
        const queue = state.queues.find(q => q.name === name);
        if (!queue) return;

        const modal = document.getElementById('queue-modal');
        const content = document.getElementById('queue-modal-content');
        if (!modal || !content) return;

        content.innerHTML = `
            <div class="space-y-4">
                <div><label class="block text-sm font-medium text-slate-300 mb-2">Queue: ${escapeHtml(name)}</label></div>
                <div>
                    <label class="block text-xs font-medium text-slate-400 mb-2">Rate Limit (jobs/sec)</label>
                    <div class="flex gap-2">
                        <input id="queue-rate-limit" type="number" placeholder="No limit" class="input flex-1" value="${queue.rate_limit || ''}" />
                        <button onclick="window.dashboard.setRateLimit('${escapeHtml(name)}', document.getElementById('queue-rate-limit').value)" class="btn btn-primary">Set</button>
                        <button onclick="window.dashboard.clearRateLimit('${escapeHtml(name)}')" class="btn btn-ghost">Clear</button>
                    </div>
                </div>
                <div>
                    <label class="block text-xs font-medium text-slate-400 mb-2">Concurrency Limit</label>
                    <div class="flex gap-2">
                        <input id="queue-concurrency" type="number" placeholder="No limit" class="input flex-1" value="${queue.concurrency || ''}" />
                        <button onclick="window.dashboard.setConcurrency('${escapeHtml(name)}', document.getElementById('queue-concurrency').value)" class="btn btn-primary">Set</button>
                        <button onclick="window.dashboard.clearConcurrency('${escapeHtml(name)}')" class="btn btn-ghost">Clear</button>
                    </div>
                </div>
            </div>
        `;
        modal.classList.remove('hidden');
    }

    // ========================================================================
    // Actions
    // ========================================================================

    async function pauseQueue(name) { await API.pauseQueue(name); fetchQueues(); }
    async function resumeQueue(name) { await API.resumeQueue(name); fetchQueues(); }
    async function retryDlq(name) { await API.retryDlq(name); fetchQueues(); }

    async function setRateLimit(name, limit) {
        await API.setRateLimit(name, limit);
        fetchQueues();
        document.getElementById('queue-modal')?.classList.add('hidden');
    }

    async function clearRateLimit(name) {
        await API.clearRateLimit(name);
        fetchQueues();
        document.getElementById('queue-modal')?.classList.add('hidden');
    }

    async function setConcurrency(name, limit) {
        await API.setConcurrency(name, limit);
        fetchQueues();
        document.getElementById('queue-modal')?.classList.add('hidden');
    }

    async function clearConcurrency(name) {
        await API.clearConcurrency(name);
        fetchQueues();
        document.getElementById('queue-modal')?.classList.add('hidden');
    }

    async function bulkCancel() {
        for (const id of state.selectedJobs) await API.cancelJob(id);
        state.selectedJobs.clear();
        fetchJobs();
    }

    async function bulkRetry() {
        for (const id of state.selectedJobs) await API.retryJob(id);
        state.selectedJobs.clear();
        fetchJobs();
    }

    async function createCron() {
        const name = document.getElementById('cron-name')?.value?.trim();
        const queue = document.getElementById('cron-queue')?.value?.trim();
        const schedule = document.getElementById('cron-schedule')?.value?.trim();
        const priority = parseInt(document.getElementById('cron-priority')?.value) || 0;
        const dataStr = document.getElementById('cron-data')?.value?.trim() || '{}';

        if (!name || !queue || !schedule) { alert('Name, queue, and schedule are required'); return; }

        let data = {};
        try { data = JSON.parse(dataStr); } catch (e) { alert('Invalid JSON data'); return; }

        const result = await API.createCron(name, { queue, schedule, priority, data });
        if (result.ok) {
            ['cron-name', 'cron-queue', 'cron-schedule', 'cron-data'].forEach(id => document.getElementById(id).value = '');
            document.getElementById('cron-priority').value = '0';
            fetchCrons();
        } else {
            alert(result.error || 'Failed to create cron job');
        }
    }

    async function deleteCron(name) {
        if (!confirm(`Delete cron job "${name}"?`)) return;
        await API.deleteCron(name);
        fetchCrons();
    }

    function toggleJob(id) {
        if (state.selectedJobs.has(id)) state.selectedJobs.delete(id);
        else state.selectedJobs.add(id);
        updateJobsUI();
    }

    function toggleSelectAll() {
        const checkbox = document.getElementById('select-all-jobs');
        if (checkbox?.checked) state.jobs.forEach(j => state.selectedJobs.add(j.id));
        else state.selectedJobs.clear();
        updateJobsUI();
    }

    function clearSelection() { state.selectedJobs.clear(); document.getElementById('select-all-jobs').checked = false; updateJobsUI(); }
    function searchJobs() { state.jobPage = 0; fetchJobs(); }
    function clearJobFilters() { ['job-search', 'job-queue-filter', 'job-state-filter'].forEach(id => document.getElementById(id).value = ''); state.jobPage = 0; fetchJobs(); }
    function prevJobPage() { if (state.jobPage > 0) { state.jobPage--; fetchJobs(); } }
    function nextJobPage() { state.jobPage++; fetchJobs(); }
    function filterQueues() { updateQueuesUI(); }

    function setChartRange(range) {
        state.chartRange = range;
        ['5m', '15m', '1h'].forEach(r => {
            const btn = document.getElementById(`range-${r}`);
            if (btn) btn.className = r === range ? 'px-2 py-1 text-xs rounded bg-primary-500 text-white' : 'px-2 py-1 text-xs rounded bg-dark-700 text-slate-400 hover:bg-dark-600';
        });
        Charts.updateCharts(state.metricsHistory, state.chartRange);
    }

    function refreshData() {
        document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
        fetchStats(); fetchMetrics(); fetchQueues(); fetchMetricsHistory();
    }

    // ========================================================================
    // Initialize
    // ========================================================================

    function init() {
        // Connect WebSocket for real-time updates
        connectWebSocket();

        // Initial data fetch
        fetchMetricsHistory();
        fetchClusterStatus();

        // Initialize charts
        Charts.init();

        // Fetch analytics data every 5 seconds (charts need history)
        setInterval(() => {
            if (state.currentSection === 'analytics') {
                fetchMetricsHistory();
            }
        }, 5000);

        document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
    }

    // ========================================================================
    // Export Global API
    // ========================================================================

    const refreshCallback = () => { fetchStats(); fetchMetrics(); fetchQueues(); };

    window.dashboard = {
        pauseQueue, resumeQueue, retryDlq, setRateLimit, clearRateLimit, setConcurrency, clearConcurrency, openQueueSettings,
        fetchJobDetail: showJobModal, bulkCancel, bulkRetry, toggleJob,
        createCron, deleteCron,
        closeJobModal: () => document.getElementById('job-modal')?.classList.add('hidden'),
        closeQueueModal: () => document.getElementById('queue-modal')?.classList.add('hidden'),
        // Settings
        testDbConnection: () => Settings.testDbConnection(),
        saveDbSettings: () => Settings.saveDbSettings(),
        saveAuthSettings: () => Settings.saveAuthSettings(),
        toggleTokenVisibility: () => Settings.toggleTokenVisibility(),
        generateToken: () => Settings.generateToken(),
        copyToken: () => Settings.copyToken(),
        saveQueueDefaults: () => Settings.saveQueueDefaults(),
        saveCleanupSettings: () => Settings.saveCleanupSettings(),
        runCleanupNow: () => Settings.runCleanupNow(),
        clearAllQueues: () => Settings.clearAllQueues(refreshCallback),
        clearAllDlq: () => Settings.clearAllDlq(refreshCallback),
        clearCompletedJobs: () => Settings.clearCompletedJobs(refreshCallback),
        resetMetrics: () => Settings.resetMetrics(() => { fetchMetrics(); fetchMetricsHistory(); }),
        shutdownServer: () => Settings.shutdownServer(),
        restartServer: () => Settings.restartServer()
    };

    window.showSection = showSection;
    window.refreshData = refreshData;
    window.filterQueues = filterQueues;
    window.searchJobs = searchJobs;
    window.clearJobFilters = clearJobFilters;
    window.prevJobPage = prevJobPage;
    window.nextJobPage = nextJobPage;
    window.toggleSelectAll = toggleSelectAll;
    window.clearSelection = clearSelection;
    window.setChartRange = setChartRange;

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
    else init();
})();
