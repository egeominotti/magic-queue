// ============================================================================
// FlashQ Dashboard - Charts
// ============================================================================

window.DashboardCharts = (function() {
    let throughputChart = null;
    let distributionChart = null;
    let analyticsThroughputChart = null;
    let analyticsJobsChart = null;
    let analyticsCompletionChart = null;
    let analyticsLatencyChart = null;

    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                labels: { color: '#94a3b8', font: { size: 11 } }
            }
        },
        scales: {
            x: {
                grid: { color: 'rgba(148, 163, 184, 0.1)' },
                ticks: { color: '#64748b', font: { size: 10 } }
            },
            y: {
                grid: { color: 'rgba(148, 163, 184, 0.1)' },
                ticks: { color: '#64748b', font: { size: 10 } }
            }
        }
    };

    return {
        init() {
            // Throughput chart
            const throughputCtx = document.getElementById('throughput-chart')?.getContext('2d');
            if (throughputCtx) {
                throughputChart = new Chart(throughputCtx, {
                    type: 'line',
                    data: {
                        labels: [],
                        datasets: [{
                            label: 'Jobs/sec',
                            data: [],
                            borderColor: '#6366f1',
                            backgroundColor: 'rgba(99, 102, 241, 0.1)',
                            tension: 0.4,
                            fill: true
                        }]
                    },
                    options: { ...chartOptions, plugins: { legend: { display: false } } }
                });
            }

            // Distribution chart
            const distributionCtx = document.getElementById('distribution-chart')?.getContext('2d');
            if (distributionCtx) {
                distributionChart = new Chart(distributionCtx, {
                    type: 'doughnut',
                    data: {
                        labels: ['Queued', 'Processing', 'Delayed', 'Failed'],
                        datasets: [{
                            data: [0, 0, 0, 0],
                            backgroundColor: ['#3b82f6', '#10b981', '#f59e0b', '#f43f5e'],
                            borderWidth: 0
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                position: 'right',
                                labels: { color: '#94a3b8', font: { size: 11 }, padding: 15 }
                            }
                        },
                        cutout: '65%'
                    }
                });
            }

            // Analytics charts
            this.initAnalyticsCharts();
        },

        initAnalyticsCharts() {
            const throughputCtx = document.getElementById('analytics-throughput')?.getContext('2d');
            if (throughputCtx) {
                analyticsThroughputChart = new Chart(throughputCtx, {
                    type: 'line',
                    data: { labels: [], datasets: [{ label: 'Throughput', data: [], borderColor: '#6366f1', tension: 0.4 }] },
                    options: chartOptions
                });
            }

            const jobsCtx = document.getElementById('analytics-jobs')?.getContext('2d');
            if (jobsCtx) {
                analyticsJobsChart = new Chart(jobsCtx, {
                    type: 'line',
                    data: {
                        labels: [],
                        datasets: [
                            { label: 'Queued', data: [], borderColor: '#3b82f6', tension: 0.4 },
                            { label: 'Processing', data: [], borderColor: '#10b981', tension: 0.4 },
                            { label: 'Delayed', data: [], borderColor: '#f59e0b', tension: 0.4 }
                        ]
                    },
                    options: chartOptions
                });
            }

            const completionCtx = document.getElementById('analytics-completion')?.getContext('2d');
            if (completionCtx) {
                analyticsCompletionChart = new Chart(completionCtx, {
                    type: 'bar',
                    data: {
                        labels: [],
                        datasets: [
                            { label: 'Completed', data: [], backgroundColor: '#10b981' },
                            { label: 'Failed', data: [], backgroundColor: '#f43f5e' }
                        ]
                    },
                    options: chartOptions
                });
            }

            const latencyCtx = document.getElementById('analytics-latency')?.getContext('2d');
            if (latencyCtx) {
                analyticsLatencyChart = new Chart(latencyCtx, {
                    type: 'line',
                    data: { labels: [], datasets: [{ label: 'Latency (ms)', data: [], borderColor: '#8b5cf6', tension: 0.4 }] },
                    options: chartOptions
                });
            }
        },

        updateDistribution(stats) {
            if (!distributionChart) return;
            distributionChart.data.datasets[0].data = [stats.queued, stats.processing, stats.delayed, stats.dlq];
            distributionChart.update('none');
        },

        updateCharts(history, chartRange = '5m') {
            if (!history.length) return;

            // Filter by range
            const now = Date.now();
            const ranges = { '5m': 5*60*1000, '15m': 15*60*1000, '1h': 60*60*1000 };
            const cutoff = now - (ranges[chartRange] || ranges['5m']);
            const filteredHistory = history.filter(d => d.timestamp >= cutoff);

            if (!filteredHistory.length) return;

            const labels = filteredHistory.map(h => {
                const d = new Date(h.timestamp);
                return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            });

            // Throughput chart
            if (throughputChart) {
                throughputChart.data.labels = labels;
                throughputChart.data.datasets[0].data = filteredHistory.map(h => h.jobs_per_second || 0);
                throughputChart.update('none');
            }

            // Analytics charts
            if (analyticsThroughputChart) {
                analyticsThroughputChart.data.labels = labels;
                analyticsThroughputChart.data.datasets[0].data = filteredHistory.map(h => h.jobs_per_second || 0);
                analyticsThroughputChart.update('none');
            }

            if (analyticsJobsChart) {
                analyticsJobsChart.data.labels = labels;
                analyticsJobsChart.data.datasets[0].data = filteredHistory.map(h => h.queued || 0);
                analyticsJobsChart.data.datasets[1].data = filteredHistory.map(h => h.processing || 0);
                analyticsJobsChart.data.datasets[2].data = filteredHistory.map(h => h.delayed || 0);
                analyticsJobsChart.update('none');
            }

            if (analyticsCompletionChart) {
                analyticsCompletionChart.data.labels = labels;
                analyticsCompletionChart.data.datasets[0].data = filteredHistory.map(h => h.completed || 0);
                analyticsCompletionChart.data.datasets[1].data = filteredHistory.map(h => h.failed || 0);
                analyticsCompletionChart.update('none');
            }

            if (analyticsLatencyChart) {
                analyticsLatencyChart.data.labels = labels;
                analyticsLatencyChart.data.datasets[0].data = filteredHistory.map(h => h.avg_latency || 0);
                analyticsLatencyChart.update('none');
            }
        }
    };
})();
