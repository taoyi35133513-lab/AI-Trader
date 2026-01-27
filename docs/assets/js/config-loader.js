// Config Loader Utility
// Loads configuration from API instead of static YAML file

class ConfigLoader {
    constructor() {
        this.config = null;
        // Default API URL - can be overridden by URL parameter
        this.apiBaseUrl = this._getApiBaseUrl();
    }

    // Get API base URL from URL parameter or default
    _getApiBaseUrl() {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get('api') || 'http://localhost:8888';
    }

    // Load configuration from API
    async loadConfig() {
        if (this.config) {
            return this.config;
        }

        const configUrl = `${this.apiBaseUrl}/api/config/full`;
        console.log('Loading configuration from API:', configUrl);

        try {
            const response = await fetch(configUrl, {
                method: 'GET',
                signal: AbortSignal.timeout(5000)  // 5 second timeout
            });

            if (!response.ok) {
                throw new Error(`Failed to load config from API: ${response.status}`);
            }

            this.config = await response.json();
            console.log('Configuration loaded successfully from API:', this.config);
            return this.config;
        } catch (error) {
            console.error('Error loading configuration from API:', error);
            throw error;
        }
    }

    // Get all enabled agents for a specific market (or legacy global list)
    getEnabledAgents(marketId = null) {
        // If market ID provided, use market-specific agents
        if (marketId) {
            const marketConfig = this.getMarketConfig(marketId);
            if (marketConfig && marketConfig.agents) {
                return marketConfig.agents.filter(agent => agent.enabled !== false);
            }
        }

        // Fallback to legacy global agents list
        if (!this.config || !this.config.agents) {
            return [];
        }
        return this.config.agents.filter(agent => agent.enabled !== false);
    }

    // Get all agent folders (enabled only) for a specific market
    getAgentFolders(marketId = null) {
        return this.getEnabledAgents(marketId).map(agent => agent.folder);
    }

    // Get agent configuration by folder name from market or legacy config
    getAgentConfig(folderName, marketId = null) {
        // If market ID provided, search in market-specific agents
        if (marketId) {
            const marketConfig = this.getMarketConfig(marketId);
            if (marketConfig && marketConfig.agents) {
                const agent = marketConfig.agents.find(a => a.folder === folderName);
                if (agent) return agent;
            }
        }

        // Fallback to legacy global agents list
        if (!this.config || !this.config.agents) {
            return null;
        }
        return this.config.agents.find(agent => agent.folder === folderName);
    }

    // Get display name for agent
    getDisplayName(folderName, marketId = null) {
        const agent = this.getAgentConfig(folderName, marketId);
        return agent ? agent.display_name : folderName;
    }

    // Get icon path for agent
    getIcon(folderName, marketId = null) {
        const agent = this.getAgentConfig(folderName, marketId);
        return agent ? agent.icon : './figs/stock.svg';
    }

    // Get color for agent
    getColor(folderName, marketId = null) {
        const agent = this.getAgentConfig(folderName, marketId);
        return agent ? agent.color : null;
    }

    // Get benchmark configuration
    getBenchmarkConfig() {
        if (!this.config || !this.config.benchmark) {
            return null;
        }
        return this.config.benchmark;
    }

    // Get data path configuration
    getDataPath() {
        if (!this.config || !this.config.data) {
            return './data';
        }
        return this.config.data.base_path;
    }

    // Get price file prefix
    getPriceFilePrefix() {
        if (!this.config || !this.config.data) {
            return 'daily_prices_';
        }
        return this.config.data.price_file_prefix;
    }

    // Get benchmark file name
    getBenchmarkFile() {
        if (!this.config || !this.config.data) {
            return 'Adaily_prices_QQQ.json';
        }
        return this.config.data.benchmark_file;
    }

    // Get chart configuration
    getChartConfig() {
        if (!this.config || !this.config.chart) {
            return {
                default_scale: 'linear',
                max_ticks: 15,
                point_radius: 0,
                point_hover_radius: 7,
                border_width: 3,
                tension: 0.42
            };
        }
        return this.config.chart;
    }

    // Get UI configuration
    getUIConfig() {
        if (!this.config || !this.config.ui) {
            return {
                initial_value: 100000,
                max_recent_trades: 20,
                date_formats: {
                    hourly: 'MM/DD HH:mm',
                    daily: 'YYYY-MM-DD'
                }
            };
        }
        return this.config.ui;
    }

    // Check if an agent is enabled
    isAgentEnabled(folderName) {
        const agent = this.getAgentConfig(folderName);
        return agent ? agent.enabled : false;
    }

    // Get all agents (including disabled ones)
    getAllAgents() {
        if (!this.config || !this.config.agents) {
            return [];
        }
        return this.config.agents;
    }

    // Get market configuration
    getMarketConfig(marketId) {
        if (!this.config || !this.config.markets) {
            return null;
        }
        return this.config.markets[marketId];
    }

    // Get all enabled markets
    getEnabledMarkets() {
        console.log('[getEnabledMarkets] Config:', this.config);
        if (!this.config || !this.config.markets) {
            console.log('[getEnabledMarkets] No config or markets found');
            return {};
        }
        const enabledMarkets = {};
        for (const [key, market] of Object.entries(this.config.markets)) {
            console.log(`[getEnabledMarkets] Market ${key}: enabled=${market.enabled}`);
            if (market.enabled !== false) {
                enabledMarkets[key] = market;
            }
        }
        console.log('[getEnabledMarkets] Enabled markets:', Object.keys(enabledMarkets));
        return enabledMarkets;
    }

    // Get API configuration
    getApiConfig() {
        if (!this.config || !this.config.api) {
            return {
                enabled: true,
                base_url: this.apiBaseUrl,
                fallback_to_files: false
            };
        }
        return this.config.api;
    }

    // Check if API mode is enabled
    isApiEnabled() {
        const apiConfig = this.getApiConfig();
        return apiConfig.enabled === true;
    }

    // Get API base URL
    getApiBaseUrl() {
        const apiConfig = this.getApiConfig();
        return apiConfig.base_url || this.apiBaseUrl;
    }

    // Check if fallback to files is enabled
    isApiFallbackEnabled() {
        const apiConfig = this.getApiConfig();
        return apiConfig.fallback_to_files !== false;
    }
}

// Create a global instance
window.configLoader = new ConfigLoader();
