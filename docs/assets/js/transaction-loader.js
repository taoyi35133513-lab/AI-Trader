// Transaction and Leaderboard Data Loader
// Loads transaction history and agent performance data

class TransactionLoader {
    constructor() {
        this.allTransactions = [];
        this.allEntries = [];
        this.leaderboardData = [];
    }

    // Load all transactions from all agents (trades only)
    async loadAllTransactions() {
        const config = window.configLoader;
        const dataLoader = window.dataLoader;
        const currentMarket = dataLoader.getMarket();
        const agents = config.getEnabledAgents(currentMarket);

        console.log(`[TransactionLoader] Loading transactions for ${agents.length} agents in ${currentMarket} market`);

        const promises = agents.map(agent => this.loadAgentTransactions(agent.folder, currentMarket));
        const results = await Promise.all(promises);

        // Flatten and sort by date (most recent first)
        this.allTransactions = results
            .flat()
            .sort((a, b) => new Date(b.date) - new Date(a.date));

        console.log(`[TransactionLoader] Loaded ${this.allTransactions.length} total transactions`);

        return this.allTransactions;
    }

    // Load ALL entries from all agents (including no_trade)
    async loadAllEntries() {
        const config = window.configLoader;
        const dataLoader = window.dataLoader;
        const currentMarket = dataLoader.getMarket();
        const agents = config.getEnabledAgents(currentMarket);

        const promises = agents.map(agent => this.loadAgentEntries(agent.folder, currentMarket));
        const results = await Promise.all(promises);

        this.allEntries = results
            .flat()
            .sort((a, b) => new Date(b.date) - new Date(a.date));

        console.log(`[TransactionLoader] Loaded ${this.allEntries.length} total entries (incl. no_trade)`);

        return this.allEntries;
    }

    // Load transactions for a single agent (trades only)
    async loadAgentTransactions(agentFolder, market = 'us') {
        const all = await this.loadAgentEntries(agentFolder, market);
        return all.filter(t => {
            if (!t.action || t.action === 'initial' || t.action === 'no_trade') return false;
            if (!t.amount || t.amount === 0) return false;
            if (!t.symbol || t.symbol === '') return false;
            return true;
        });
    }

    // Load ALL position entries for a single agent (including no_trade)
    // Also merges entries from the corresponding -live folder if it exists
    async loadAgentEntries(agentFolder, market = 'us') {
        const marketConfig = window.configLoader.getMarketConfig(market);
        const agentDataDir = marketConfig ? marketConfig.data_dir : 'agent_data';

        // Determine live folder name
        const isHourly = market === 'cn_hour';
        let liveFolder;
        if (isHourly && agentFolder.endsWith('-astock-hour')) {
            const base = agentFolder.slice(0, -'-astock-hour'.length);
            liveFolder = `${base}-live-astock-hour`;
        } else {
            liveFolder = `${agentFolder}-live`;
        }

        // Load from both backtest and live folders in parallel
        const [backtestEntries, liveEntries] = await Promise.all([
            this._loadEntriesFromPath(agentFolder, `data/${agentDataDir}/${agentFolder}/position/position.jsonl`),
            this._loadEntriesFromPath(agentFolder, `data/${agentDataDir}/${liveFolder}/position/position.jsonl`),
        ]);

        // Combine: live entries extend backtest data (dates don't overlap)
        // If there is date overlap, keep live entries and drop backtest for that date
        const liveDates = new Set(liveEntries.map(e => e.date));
        const combined = backtestEntries.filter(e => !liveDates.has(e.date)).concat(liveEntries);

        return combined
            .filter(t => t.action !== 'initial')
            .sort((a, b) => new Date(b.date) - new Date(a.date));
    }

    // Internal: parse a single position.jsonl file
    async _loadEntriesFromPath(agentFolder, positionPath) {
        try {
            const response = await fetch(positionPath + '?t=' + Date.now());
            if (!response.ok) return [];

            const text = await response.text();
            return text
                .trim()
                .split('\n')
                .filter(line => line.trim())
                .map(line => {
                    const data = JSON.parse(line);
                    return {
                        agentFolder: agentFolder,
                        date: data.date,
                        id: data.id,
                        action: data.this_action?.action || 'initial',
                        symbol: data.this_action?.symbol || '',
                        amount: data.this_action?.amount || 0,
                        positions: data.positions,
                        cash: data.CASH || 0
                    };
                });
        } catch (error) {
            return [];
        }
    }

    // Load agent's thinking/response for a specific transaction
    async loadAgentThinking(agentFolder, date, market = 'us') {
        try {
            const marketConfig = window.configLoader.getMarketConfig(market);
            const agentDataDir = marketConfig ? marketConfig.data_dir : 'agent_data';
            // Sanitize date for Windows compatibility (replace : with -)
            const safeDate = date.replace(/:/g, '-');
            const cacheBust = '?t=' + Date.now();
            const logPath = `data/${agentDataDir}/${agentFolder}/log/${safeDate}/log.jsonl`;
            let response = await fetch(logPath + cacheBust);

            // Fallback: try the corresponding -live folder
            if (!response.ok) {
                const isHourly = market === 'cn_hour';
                let liveFolder;
                if (isHourly && agentFolder.endsWith('-astock-hour')) {
                    const base = agentFolder.slice(0, -'-astock-hour'.length);
                    liveFolder = `${base}-live-astock-hour`;
                } else {
                    liveFolder = `${agentFolder}-live`;
                }
                const livePath = `data/${agentDataDir}/${liveFolder}/log/${safeDate}/log.jsonl`;
                response = await fetch(livePath + cacheBust);
            }

            // If log file doesn't exist in either location, return null
            if (!response.ok) {
                return null;
            }

            const text = await response.text();
            const lines = text.trim().split('\n').filter(line => line.trim());

            // Log files may contain entries from multiple runs for the same date.
            // Group into sessions (each starting with a user message), then pick
            // the last session that has actual assistant content.
            const sessions = []; // each: { startIdx, assistantContents[] }
            for (let i = 0; i < lines.length; i++) {
                try {
                    const data = JSON.parse(lines[i]);
                    if (!data.new_messages) continue;
                    const messages = Array.isArray(data.new_messages)
                        ? data.new_messages
                        : [data.new_messages];

                    if (messages.some(m => m.role === 'user')) {
                        sessions.push({ startIdx: i, assistantContents: [] });
                    }

                    if (sessions.length === 0) continue;
                    const currentSession = sessions[sessions.length - 1];
                    for (const msg of messages) {
                        if (msg.role === 'assistant') {
                            const content = msg.content.replace(/<FINISH_SIGNAL>/g, '').trim();
                            if (content) {
                                currentSession.assistantContents.push(content);
                            }
                        }
                    }
                } catch (e) {
                    console.warn(`Failed to parse line: ${lines[i]}`, e);
                }
            }

            // Pick the last session that has actual content; fall back to earlier sessions
            for (let i = sessions.length - 1; i >= 0; i--) {
                if (sessions[i].assistantContents.length > 0) {
                    return sessions[i].assistantContents.join('\n\n');
                }
            }

            return null;
        } catch (error) {
            console.warn(`Failed to load thinking for ${agentFolder} at ${date}:`, error);
            return null;
        }
    }

    // Calculate profit for a transaction
    async calculateTransactionProfit(transaction) {
        // For now, return null - will be calculated when price data is integrated
        // This would need: buy price at transaction time, sell price (if sell), or current price
        return null;
    }

    // Build leaderboard data
    async buildLeaderboard(allAgentsData) {
        const leaderboard = [];
        const currentMarket = window.dataLoader ? window.dataLoader.getMarket() : 'us';

        for (const [agentName, data] of Object.entries(allAgentsData)) {
            const assetHistory = data.assetHistory || [];
            const initialValue = assetHistory[0]?.value || 10000;
            const finalValue = assetHistory[assetHistory.length - 1]?.value || initialValue;
            const gain = finalValue - initialValue;
            const gainPercent = ((finalValue - initialValue) / initialValue) * 100;

            leaderboard.push({
                agentName: agentName,
                displayName: window.configLoader.getDisplayName(agentName, currentMarket),
                icon: window.configLoader.getIcon(agentName, currentMarket),
                color: window.configLoader.getColor(agentName, currentMarket),
                initialValue: initialValue,
                currentValue: finalValue,
                gain: gain,
                gainPercent: gainPercent,
                return: data.return || gainPercent
            });
        }

        // Sort by current value (descending)
        leaderboard.sort((a, b) => b.currentValue - a.currentValue);

        // Add rank
        leaderboard.forEach((item, index) => {
            item.rank = index + 1;
        });

        this.leaderboardData = leaderboard;
        return leaderboard;
    }

    // Get most recent N transactions
    getMostRecentTransactions(n = 100) {
        return this.allTransactions.slice(0, n);
    }

    // Format currency
    formatCurrency(value) {
        if (value === null || value === undefined) return 'N/A';
        const market = window.dataLoader?.getMarket() || 'us';
        const isCN = market.startsWith('cn');
        return new Intl.NumberFormat(isCN ? 'zh-CN' : 'en-US', {
            style: 'currency',
            currency: isCN ? 'CNY' : 'USD',
            minimumFractionDigits: 0,
            maximumFractionDigits: 0
        }).format(value);
    }

    // Format percent
    formatPercent(value) {
        if (value === null || value === undefined) return 'N/A';
        const sign = value >= 0 ? '+' : '';
        return `${sign}${value.toFixed(2)}%`;
    }

    // Format date/time
    formatDateTime(dateStr) {
        const date = new Date(dateStr);
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    // Get action icon
    getActionIcon(action) {
        return action === 'buy' ? '📈' : '📉';
    }

    // Get action color
    getActionColor(action) {
        return action === 'buy' ? 'var(--success)' : 'var(--danger)';
    }
}

// Create global instance
window.transactionLoader = new TransactionLoader();