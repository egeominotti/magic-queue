// ============================================================================
// FlashQ Dashboard - Utilities
// ============================================================================

window.DashboardUtils = {
    formatTime(ts) {
        if (!ts) return '-';
        const d = new Date(ts);
        return d.toLocaleTimeString() + ' ' + d.toLocaleDateString();
    },

    formatNumber(n) {
        if (n === null || n === undefined) return '-';
        return n.toLocaleString();
    },

    escapeHtml(str) {
        if (str === null || str === undefined) return '';
        const div = document.createElement('div');
        div.textContent = String(str);
        return div.innerHTML;
    },

    getBadgeClass(type) {
        const classes = {
            waiting: 'badge-waiting',
            active: 'badge-active',
            delayed: 'badge-delayed',
            completed: 'badge-completed',
            failed: 'badge-failed',
            paused: 'badge-paused'
        };
        return classes[type] || 'badge-waiting';
    }
};
