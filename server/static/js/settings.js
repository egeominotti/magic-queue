// ============================================================================
// FlashQ Dashboard - Settings
// ============================================================================

window.DashboardSettings = (function() {
    let serverStartTime = Date.now();

    return {
        updateUI(settings) {
            // Update uptime
            const uptimeMs = Date.now() - serverStartTime;
            const hours = Math.floor(uptimeMs / 3600000);
            const mins = Math.floor((uptimeMs % 3600000) / 60000);
            document.getElementById('settings-uptime').textContent = `${hours}h ${mins}m`;

            // Database status
            const dbIndicator = document.getElementById('db-status-indicator');
            const dbText = document.getElementById('db-status-text');
            if (settings.database_connected) {
                dbIndicator.className = 'w-2 h-2 rounded-full bg-emerald-500';
                dbText.textContent = 'Connected';
                dbText.className = 'text-sm text-emerald-400';
            } else if (settings.database_url) {
                dbIndicator.className = 'w-2 h-2 rounded-full bg-amber-500';
                dbText.textContent = 'Configured';
                dbText.className = 'text-sm text-amber-400';
            } else {
                dbIndicator.className = 'w-2 h-2 rounded-full bg-slate-500';
                dbText.textContent = 'Not configured';
                dbText.className = 'text-sm text-slate-400';
            }

            // Auth status
            const authIndicator = document.getElementById('auth-status-indicator');
            const authText = document.getElementById('auth-status-text');
            if (settings.auth_enabled) {
                authIndicator.className = 'w-2 h-2 rounded-full bg-emerald-500';
                authText.textContent = `${settings.auth_token_count || 0} tokens`;
                authText.className = 'text-sm text-emerald-400';
            } else {
                authIndicator.className = 'w-2 h-2 rounded-full bg-slate-500';
                authText.textContent = 'Disabled';
                authText.className = 'text-sm text-slate-400';
            }

            // Fill in values if available
            if (settings.database_url) {
                document.getElementById('settings-db-url').value = settings.database_url;
            }
            if (settings.pool_size) {
                document.getElementById('db-pool-size').textContent = settings.pool_size;
            }
            if (settings.active_connections !== undefined) {
                document.getElementById('db-active-conn').textContent = settings.active_connections;
            }
            if (settings.tcp_port) {
                document.getElementById('settings-tcp-port').textContent = settings.tcp_port;
            }
            if (settings.http_port) {
                document.getElementById('settings-http-port').textContent = settings.http_port;
            }
            if (settings.version) {
                document.getElementById('settings-version').textContent = settings.version;
            }
        },

        async testDbConnection() {
            const url = document.getElementById('settings-db-url').value.trim();
            if (!url) {
                alert('Please enter a database URL');
                return;
            }

            try {
                const data = await DashboardAPI.testDbConnection(url);
                if (data.ok) {
                    alert('Connection successful!');
                } else {
                    alert('Connection failed: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Connection test failed: ' + e.message);
            }
        },

        async saveDbSettings() {
            const url = document.getElementById('settings-db-url').value.trim();

            try {
                const data = await DashboardAPI.saveDbSettings(url);
                if (data.ok) {
                    alert('Database settings saved. Restart server to apply changes.');
                } else {
                    alert('Failed to save: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to save settings');
            }
        },

        async saveAuthSettings() {
            const tokens = document.getElementById('settings-auth-tokens').value.trim();
            const tokenArray = tokens ? tokens.split(',').map(t => t.trim()) : [];

            try {
                const data = await DashboardAPI.saveAuthSettings(tokenArray);
                if (data.ok) {
                    alert('Auth settings saved. Restart server to apply changes.');
                } else {
                    alert('Failed to save: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to save settings');
            }
        },

        toggleTokenVisibility() {
            const input = document.getElementById('settings-auth-tokens');
            input.type = input.type === 'password' ? 'text' : 'password';
        },

        generateToken() {
            const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            let token = '';
            for (let i = 0; i < 32; i++) {
                token += chars.charAt(Math.floor(Math.random() * chars.length));
            }
            document.getElementById('generated-token').value = token;
        },

        copyToken() {
            const token = document.getElementById('generated-token').value;
            if (token) {
                navigator.clipboard.writeText(token);
                alert('Token copied to clipboard!');
            }
        },

        async saveQueueDefaults() {
            const settings = {
                default_timeout: parseInt(document.getElementById('settings-default-timeout').value) || 30000,
                default_max_attempts: parseInt(document.getElementById('settings-default-attempts').value) || 3,
                default_backoff: parseInt(document.getElementById('settings-default-backoff').value) || 1000,
                default_ttl: parseInt(document.getElementById('settings-default-ttl').value) || 0
            };

            try {
                const data = await DashboardAPI.saveQueueDefaults(settings);
                if (data.ok) {
                    alert('Queue defaults saved!');
                } else {
                    alert('Failed to save: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to save settings');
            }
        },

        async saveCleanupSettings() {
            const settings = {
                max_completed_jobs: parseInt(document.getElementById('settings-max-completed').value) || 50000,
                max_job_results: parseInt(document.getElementById('settings-max-results').value) || 5000,
                cleanup_interval: parseInt(document.getElementById('settings-cleanup-interval').value) || 60,
                metrics_history_size: parseInt(document.getElementById('settings-metrics-history').value) || 1000
            };

            try {
                const data = await DashboardAPI.saveCleanupSettings(settings);
                if (data.ok) {
                    alert('Cleanup settings saved!');
                } else {
                    alert('Failed to save: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to save settings');
            }
        },

        async runCleanupNow() {
            try {
                const data = await DashboardAPI.runCleanup();
                if (data.ok) {
                    alert('Cleanup completed!');
                } else {
                    alert('Cleanup failed: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Cleanup failed');
            }
        },

        async clearAllQueues(refreshCallback) {
            if (!confirm('Are you sure you want to clear ALL queues? This will delete all pending jobs.')) return;

            try {
                const data = await DashboardAPI.clearAllQueues();
                if (data.ok) {
                    alert('All queues cleared!');
                    if (refreshCallback) refreshCallback();
                } else {
                    alert('Failed: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to clear queues');
            }
        },

        async clearAllDlq(refreshCallback) {
            if (!confirm('Are you sure you want to clear ALL dead letter queues?')) return;

            try {
                const data = await DashboardAPI.clearAllDlq();
                if (data.ok) {
                    alert('All DLQ cleared!');
                    if (refreshCallback) refreshCallback();
                } else {
                    alert('Failed: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to clear DLQ');
            }
        },

        async clearCompletedJobs(refreshCallback) {
            if (!confirm('Are you sure you want to clear all completed jobs?')) return;

            try {
                const data = await DashboardAPI.clearCompletedJobs();
                if (data.ok) {
                    alert('Completed jobs cleared!');
                    if (refreshCallback) refreshCallback();
                } else {
                    alert('Failed: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to clear completed jobs');
            }
        },

        async resetMetrics(refreshCallback) {
            if (!confirm('Are you sure you want to reset all metrics? This cannot be undone.')) return;

            try {
                const data = await DashboardAPI.resetMetrics();
                if (data.ok) {
                    alert('Metrics reset!');
                    if (refreshCallback) refreshCallback();
                } else {
                    alert('Failed: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Failed to reset metrics');
            }
        },

        async shutdownServer() {
            if (!confirm('Are you sure you want to shutdown the server? You will need to restart it manually.')) return;

            try {
                await DashboardAPI.shutdownServer();
                alert('Server is shutting down... Use "make restart" to restart.');
            } catch (e) {
                // Connection will likely fail as server is shutting down
                alert('Server shutdown initiated. Use "make restart" to restart.');
            }
        },

        async restartServer() {
            if (!confirm('Are you sure you want to restart the server? The dashboard will reconnect automatically.')) return;

            try {
                await DashboardAPI.restartServer();
                alert('Server is restarting... The page will attempt to reconnect in a few seconds.');
                // Attempt to reload after delay
                setTimeout(() => {
                    window.location.reload();
                }, 3000);
            } catch (e) {
                // Connection will likely fail as server is restarting
                alert('Server restart initiated. Reloading page in 3 seconds...');
                setTimeout(() => {
                    window.location.reload();
                }, 3000);
            }
        }
    };
})();
