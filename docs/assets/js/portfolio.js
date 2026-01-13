// Portfolio Analysis Page
// Detailed view of individual agent portfolios

const dataLoader = new DataLoader();
let allAgentsData = {};
let currentAgent = null;
let allocationChart = null;

// Store data for search functionality
let currentHoldingsData = [];
let currentTradesData = [];

// Store current filter state
let currentHoldingsDate = null;  // null means latest
let currentTradeStartDate = null;
let currentTradeEndDate = null;

// Load data and refresh UI
async function loadDataAndRefresh() {
    showLoading();

    try {
        // Load all agents data
        console.log('Loading all agents data...');
        allAgentsData = await dataLoader.loadAllAgentsData();
        console.log('Data loaded:', allAgentsData);

        // Populate agent selector
        populateAgentSelector();

        // Load first agent by default
        const firstAgent = Object.keys(allAgentsData)[0];
        if (firstAgent) {
            currentAgent = firstAgent;
            await loadAgentPortfolio(firstAgent);
        }

    } catch (error) {
        console.error('Error loading data:', error);
        alert('Failed to load portfolio data. Please check console for details.');
    } finally {
        hideLoading();
    }
}

// Update market buttons visibility based on enabled markets in config
function updateMarketButtonsVisibility() {
    const config = window.configLoader.config;
    if (!config || !config.markets) return;

    const usBtn = document.getElementById('usMarketBtn');
    const cnBtn = document.getElementById('cnMarketBtn');
    const granularityWrapper = document.getElementById('granularityWrapper');

    // Check if US market is enabled
    const usEnabled = config.markets.us && config.markets.us.enabled !== false;
    // Check if any CN market is enabled (cn or cn_hour)
    const cnEnabled = (config.markets.cn && config.markets.cn.enabled !== false) ||
                      (config.markets.cn_hour && config.markets.cn_hour.enabled !== false);

    // Show/hide US market button
    if (usBtn) {
        usBtn.style.display = usEnabled ? '' : 'none';
    }

    // Show/hide CN market button
    if (cnBtn) {
        cnBtn.style.display = cnEnabled ? '' : 'none';
    }

    // Check if only one granularity is enabled for CN
    const cnDailyEnabled = config.markets.cn && config.markets.cn.enabled !== false;
    const cnHourlyEnabled = config.markets.cn_hour && config.markets.cn_hour.enabled !== false;

    // If only one CN granularity is enabled, hide the granularity wrapper
    if (granularityWrapper) {
        if (!cnEnabled || (cnDailyEnabled && !cnHourlyEnabled) || (!cnDailyEnabled && cnHourlyEnabled)) {
            granularityWrapper.classList.add('hidden');
        }
    }

    console.log(`Market buttons visibility - US: ${usEnabled}, CN: ${cnEnabled} (daily: ${cnDailyEnabled}, hourly: ${cnHourlyEnabled})`);
}

// Initialize the page
async function init() {
    // Set up event listeners first
    setupEventListeners();

    // Load config first to determine enabled markets
    await window.configLoader.loadConfig();

    // Get enabled markets and set initial market to first enabled one
    const enabledMarkets = window.configLoader.getEnabledMarkets();
    const enabledMarketIds = Object.keys(enabledMarkets);

    if (enabledMarketIds.length > 0) {
        // Set dataLoader to first enabled market
        const firstEnabledMarket = enabledMarketIds[0];
        dataLoader.setMarket(firstEnabledMarket);
        console.log(`Initial market set to: ${firstEnabledMarket} (first enabled market)`);
    }

    // Update market buttons visibility based on config
    updateMarketButtonsVisibility();

    // Load initial data
    await loadDataAndRefresh();

    // Initialize UI state
    updateMarketUI();
}

// Populate agent selector dropdown
function populateAgentSelector() {
    const select = document.getElementById('agentSelect');
    select.innerHTML = '';

    Object.keys(allAgentsData).forEach(agentName => {
        const option = document.createElement('option');
        option.value = agentName;
        // Use text only for dropdown options (HTML select doesn't support images well)
        option.textContent = dataLoader.getAgentDisplayName(agentName);
        select.appendChild(option);
    });
}

// Load and display portfolio for selected agent
async function loadAgentPortfolio(agentName) {
    showLoading();

    try {
        currentAgent = agentName;
        const data = allAgentsData[agentName];

        // Reset filters when switching agents
        resetFilters();

        // Update date pickers with agent's date range
        updateHoldingsDatePicker(agentName);
        updateTradeDatePickers(agentName);

        // Update performance metrics
        updateMetrics(data);

        // Update holdings table
        await updateHoldingsTable(agentName);

        // Update allocation chart
        await updateAllocationChart(agentName);

        // Update trade history
        updateTradeHistory(agentName);

    } catch (error) {
        console.error('Error loading portfolio:', error);
    } finally {
        hideLoading();
    }
}

// Reset all filters
function resetFilters() {
    // Reset search inputs
    const holdingsSearch = document.getElementById('holdingsSearch');
    const tradeSearch = document.getElementById('tradeSearch');
    if (holdingsSearch) holdingsSearch.value = '';
    if (tradeSearch) tradeSearch.value = '';

    // Reset date inputs
    const holdingsDate = document.getElementById('holdingsDate');
    const tradeStartDate = document.getElementById('tradeStartDate');
    const tradeEndDate = document.getElementById('tradeEndDate');
    if (holdingsDate) holdingsDate.value = '';
    if (tradeStartDate) tradeStartDate.value = '';
    if (tradeEndDate) tradeEndDate.value = '';

    // Reset state
    currentHoldingsDate = null;
    currentTradeStartDate = null;
    currentTradeEndDate = null;
}

// Update performance metrics
function updateMetrics(data) {
    const totalAsset = data.currentValue;
    const totalReturn = data.return;
    const latestPosition = data.positions && data.positions.length > 0 ? data.positions[data.positions.length - 1] : null;
    const cashPosition = latestPosition && latestPosition.positions ? latestPosition.positions.CASH || 0 : 0;
    const totalTrades = data.positions ? data.positions.filter(p => p.this_action).length : 0;

    document.getElementById('totalAsset').textContent = dataLoader.formatCurrency(totalAsset);
    document.getElementById('totalReturn').textContent = dataLoader.formatPercent(totalReturn);
    document.getElementById('totalReturn').className = `metric-value ${totalReturn >= 0 ? 'positive' : 'negative'}`;
    document.getElementById('cashPosition').textContent = dataLoader.formatCurrency(cashPosition);
    document.getElementById('totalTrades').textContent = totalTrades;
}

// Get holdings for a specific date
function getHoldingsForDate(agentName, targetDate) {
    const data = allAgentsData[agentName];
    if (!data || !data.positions || data.positions.length === 0) {
        return null;
    }

    // Find the position at or before the target date
    let targetPosition = null;
    for (let i = data.positions.length - 1; i >= 0; i--) {
        const position = data.positions[i];
        const posDate = position.date.split(' ')[0]; // Get date part only
        if (posDate <= targetDate) {
            targetPosition = position;
            break;
        }
    }

    if (!targetPosition || !targetPosition.positions) {
        return null;
    }

    return targetPosition.positions;
}

// Get available date range for an agent
function getAgentDateRange(agentName) {
    const data = allAgentsData[agentName];
    if (!data || !data.assetHistory || data.assetHistory.length === 0) {
        return { min: null, max: null };
    }

    const firstDate = data.assetHistory[0].date.split(' ')[0];
    const lastDate = data.assetHistory[data.assetHistory.length - 1].date.split(' ')[0];

    return { min: firstDate, max: lastDate };
}

// Update date picker constraints
function updateHoldingsDatePicker(agentName) {
    const dateInput = document.getElementById('holdingsDate');
    if (!dateInput) return;

    const { min, max } = getAgentDateRange(agentName);
    if (min && max) {
        dateInput.min = min;
        dateInput.max = max;
        // Set default value to max (latest) if not already set
        if (!dateInput.value) {
            dateInput.value = max;
        }
    }
}

// Update holdings table
async function updateHoldingsTable(agentName, searchQuery = '', targetDate = null) {
    console.log(`[updateHoldingsTable] Loading holdings for: ${agentName}, date: ${targetDate || 'latest'}`);

    const data = allAgentsData[agentName];
    if (!data || !data.assetHistory || data.assetHistory.length === 0) {
        currentHoldingsData = [];
        return;
    }

    // Determine which date to use
    let useDate = targetDate;
    let holdings;

    if (targetDate) {
        // Get holdings for specific date
        holdings = getHoldingsForDate(agentName, targetDate);
    } else {
        // Get current (latest) holdings
        holdings = dataLoader.getCurrentHoldings(agentName);
        useDate = data.assetHistory[data.assetHistory.length - 1].date.split(' ')[0];
    }

    console.log(`[updateHoldingsTable] Holdings for ${useDate}:`, holdings);
    const tableBody = document.getElementById('holdingsTableBody');
    tableBody.innerHTML = '';

    if (!holdings) {
        console.log(`[updateHoldingsTable] No holdings found`);
        currentHoldingsData = [];
        renderHoldingsTable([], 0);
        return;
    }

    // Get all stocks with non-zero holdings
    const stocks = Object.entries(holdings)
        .filter(([symbol, shares]) => symbol !== 'CASH' && shares > 0);

    // Calculate total value based on prices at that date
    let totalValue = holdings.CASH || 0;
    const holdingsData = await Promise.all(
        stocks.map(async ([symbol, shares]) => {
            const price = await dataLoader.getClosingPrice(symbol, useDate);
            const marketValue = price ? shares * price : 0;
            totalValue += marketValue;
            return { symbol, shares, price, marketValue };
        })
    );

    // Update totalValue in each holding
    holdingsData.forEach(h => h.totalValue = totalValue);
    holdingsData.sort((a, b) => b.marketValue - a.marketValue);

    // Add cash to holdings data
    if (holdings.CASH > 0) {
        holdingsData.push({
            symbol: 'CASH',
            shares: null,
            price: null,
            marketValue: holdings.CASH,
            totalValue,
            isCash: true
        });
    }

    // Store for search functionality
    currentHoldingsData = holdingsData;

    // Filter by search query
    const filteredData = searchQuery
        ? holdingsData.filter(h => h.symbol.toLowerCase().includes(searchQuery.toLowerCase()))
        : holdingsData;

    // Render holdings table
    renderHoldingsTable(filteredData, totalValue);
}

// Render holdings table with filtered data
function renderHoldingsTable(holdingsData, totalValue) {
    const tableBody = document.getElementById('holdingsTableBody');
    tableBody.innerHTML = '';

    // Separate stock holdings and cash
    const stockHoldings = holdingsData.filter(h => !h.isCash);
    const cashHolding = holdingsData.find(h => h.isCash);

    // Create table rows for stocks
    stockHoldings.forEach(holding => {
        const weight = (holding.marketValue / totalValue * 100).toFixed(2);
        const row = document.createElement('tr');
        row.innerHTML = `
            <td class="symbol">${holding.symbol}</td>
            <td>${holding.shares}</td>
            <td>${dataLoader.formatCurrency(holding.price || 0)}</td>
            <td>${dataLoader.formatCurrency(holding.marketValue)}</td>
            <td>${weight}%</td>
        `;
        tableBody.appendChild(row);
    });

    // Add cash row if present in filtered results
    if (cashHolding) {
        const cashWeight = (cashHolding.marketValue / totalValue * 100).toFixed(2);
        const cashRow = document.createElement('tr');
        cashRow.innerHTML = `
            <td class="symbol">CASH</td>
            <td>-</td>
            <td>-</td>
            <td>${dataLoader.formatCurrency(cashHolding.marketValue)}</td>
            <td>${cashWeight}%</td>
        `;
        tableBody.appendChild(cashRow);
    }

    // If no holdings data, show a message
    if (holdingsData.length === 0) {
        const noDataRow = document.createElement('tr');
        noDataRow.innerHTML = `
            <td colspan="5" style="text-align: center; color: var(--text-muted); padding: 2rem;">
                No holdings data available
            </td>
        `;
        tableBody.appendChild(noDataRow);
    }
}

// Filter holdings by search query
function filterHoldings(searchQuery) {
    if (!currentHoldingsData.length) return;

    const totalValue = currentHoldingsData[0]?.totalValue || 0;
    const filteredData = searchQuery
        ? currentHoldingsData.filter(h => h.symbol.toLowerCase().includes(searchQuery.toLowerCase()))
        : currentHoldingsData;

    renderHoldingsTable(filteredData, totalValue);
}

// Update allocation chart (pie chart)
async function updateAllocationChart(agentName) {
    const holdings = dataLoader.getCurrentHoldings(agentName);
    if (!holdings) return;

    const data = allAgentsData[agentName];
    const latestDate = data.assetHistory[data.assetHistory.length - 1].date;

    // Calculate market values
    const allocations = [];

    for (const [symbol, shares] of Object.entries(holdings)) {
        if (symbol === 'CASH') {
            if (shares > 0) {
                allocations.push({ label: 'CASH', value: shares });
            }
        } else if (shares > 0) {
            const price = await dataLoader.getClosingPrice(symbol, latestDate);
            if (price) {
                allocations.push({ label: symbol, value: shares * price });
            }
        }
    }

    // Sort by value and take top 10, combine rest as "Others"
    allocations.sort((a, b) => b.value - a.value);

    const topAllocations = allocations.slice(0, 10);
    const othersValue = allocations.slice(10).reduce((sum, a) => sum + a.value, 0);

    if (othersValue > 0) {
        topAllocations.push({ label: 'Others', value: othersValue });
    }

    // Destroy existing chart
    if (allocationChart) {
        allocationChart.destroy();
    }

    // Create new chart
    const ctx = document.getElementById('allocationChart').getContext('2d');
    const colors = [
        '#00d4ff', '#00ffcc', '#ff006e', '#ffbe0b', '#8338ec',
        '#3a86ff', '#fb5607', '#06ffa5', '#ff006e', '#ffbe0b', '#8338ec'
    ];

    allocationChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: topAllocations.map(a => a.label),
            datasets: [{
                data: topAllocations.map(a => a.value),
                backgroundColor: colors,
                borderWidth: 2,
                borderColor: '#1a2238'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: '#a0aec0',
                        padding: 15,
                        font: {
                            size: 12
                        }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(26, 34, 56, 0.95)',
                    titleColor: '#00d4ff',
                    bodyColor: '#fff',
                    borderColor: '#2d3748',
                    borderWidth: 1,
                    padding: 12,
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = dataLoader.formatCurrency(context.parsed);
                            const total = context.dataset.data.reduce((sum, v) => sum + v, 0);
                            const percentage = ((context.parsed / total) * 100).toFixed(1);
                            return `${label}: ${value} (${percentage}%)`;
                        }
                    }
                }
            }
        }
    });
}

// Update trade history timeline
function updateTradeHistory(agentName, searchQuery = '') {
    const trades = dataLoader.getTradeHistory(agentName);

    if (trades.length === 0) {
        currentTradesData = [];
        const timeline = document.getElementById('tradeTimeline');
        timeline.innerHTML = '<p style="color: var(--text-muted);">No trade history available.</p>';
        return;
    }

    // Store all trades for search functionality (not just recent 20)
    currentTradesData = trades;

    // Filter by search query
    const filteredTrades = searchQuery
        ? trades.filter(t =>
            t.symbol.toLowerCase().includes(searchQuery.toLowerCase()) ||
            t.action.toLowerCase().includes(searchQuery.toLowerCase())
          )
        : trades;

    // Render trades
    renderTradeHistory(filteredTrades);
}

// Render trade history with filtered data
function renderTradeHistory(trades) {
    const timeline = document.getElementById('tradeTimeline');
    timeline.innerHTML = '';

    if (trades.length === 0) {
        timeline.innerHTML = '<p style="color: var(--text-muted);">No matching trades found.</p>';
        return;
    }

    // Show latest 50 trades when searching, 20 otherwise
    const displayTrades = trades.slice(0, 50);

    displayTrades.forEach(trade => {
        const tradeItem = document.createElement('div');
        tradeItem.className = 'trade-item';

        const icon = trade.action === 'buy' ? '📈' : '📉';
        const iconClass = trade.action === 'buy' ? 'buy' : 'sell';
        const actionText = trade.action === 'buy' ? 'Bought' : 'Sold';

        // Format the timestamp for hourly data
        let formattedDate = trade.date;
        if (trade.date.includes(':')) {
            const date = new Date(trade.date);
            formattedDate = date.toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        }

        tradeItem.innerHTML = `
            <div class="trade-icon ${iconClass}">${icon}</div>
            <div class="trade-details">
                <div class="trade-action">${actionText} ${trade.amount} shares of ${trade.symbol}</div>
                <div class="trade-meta">${formattedDate}</div>
            </div>
        `;

        timeline.appendChild(tradeItem);
    });

    // Show count if there are more trades
    if (trades.length > 50) {
        const moreInfo = document.createElement('p');
        moreInfo.style.cssText = 'color: var(--text-muted); text-align: center; margin-top: 1rem;';
        moreInfo.textContent = `Showing 50 of ${trades.length} matching trades`;
        timeline.appendChild(moreInfo);
    }
}

// Filter trades by search query and date range
function filterTrades(searchQuery = '', startDate = null, endDate = null) {
    if (!currentTradesData.length) return;

    let filteredTrades = currentTradesData;

    // Filter by date range
    if (startDate || endDate) {
        filteredTrades = filteredTrades.filter(t => {
            const tradeDate = t.date.split(' ')[0]; // Get date part only
            if (startDate && tradeDate < startDate) return false;
            if (endDate && tradeDate > endDate) return false;
            return true;
        });
    }

    // Filter by search query
    if (searchQuery) {
        filteredTrades = filteredTrades.filter(t =>
            t.symbol.toLowerCase().includes(searchQuery.toLowerCase()) ||
            t.action.toLowerCase().includes(searchQuery.toLowerCase())
        );
    }

    renderTradeHistory(filteredTrades);
}

// Update trade date picker constraints
function updateTradeDatePickers(agentName) {
    const startInput = document.getElementById('tradeStartDate');
    const endInput = document.getElementById('tradeEndDate');
    if (!startInput || !endInput) return;

    const { min, max } = getAgentDateRange(agentName);
    if (min && max) {
        startInput.min = min;
        startInput.max = max;
        endInput.min = min;
        endInput.max = max;
    }
}

// Apply current filters to holdings
async function applyHoldingsFilters() {
    if (!currentAgent) return;

    const searchQuery = document.getElementById('holdingsSearch')?.value.trim() || '';
    const dateInput = document.getElementById('holdingsDate');
    const targetDate = dateInput?.value || null;

    await updateHoldingsTable(currentAgent, searchQuery, targetDate);
}

// Apply current filters to trades
function applyTradeFilters() {
    const searchQuery = document.getElementById('tradeSearch')?.value.trim() || '';
    const startDate = document.getElementById('tradeStartDate')?.value || null;
    const endDate = document.getElementById('tradeEndDate')?.value || null;

    filterTrades(searchQuery, startDate, endDate);
}

// Update UI based on current market state
function updateMarketUI() {
    const currentMarket = dataLoader.getMarket();
    const usBtn = document.getElementById('usMarketBtn');
    const cnBtn = document.getElementById('cnMarketBtn');
    const granularityWrapper = document.getElementById('granularityWrapper');
    const dailyBtn = document.getElementById('dailyBtn');
    const hourlyBtn = document.getElementById('hourlyBtn');

    // Reset all active states
    if (usBtn) usBtn.classList.remove('active');
    if (cnBtn) cnBtn.classList.remove('active');
    if (dailyBtn) dailyBtn.classList.remove('active');
    if (hourlyBtn) hourlyBtn.classList.remove('active');

    if (currentMarket === 'us') {
        if (usBtn) usBtn.classList.add('active');
        if (granularityWrapper) granularityWrapper.classList.add('hidden');
    } else {
        // Both 'cn' and 'cn_hour' keep the main CN button active
        if (cnBtn) cnBtn.classList.add('active');
        if (granularityWrapper) granularityWrapper.classList.remove('hidden');
        
        if (currentMarket === 'cn_hour') {
            if (hourlyBtn) hourlyBtn.classList.add('active');
        } else {
            if (dailyBtn) dailyBtn.classList.add('active');
        }
    }
}

// Set up event listeners
function setupEventListeners() {
    document.getElementById('agentSelect').addEventListener('change', (e) => {
        loadAgentPortfolio(e.target.value);
    });

    // Market switching
    const usMarketBtn = document.getElementById('usMarketBtn');
    const cnMarketBtn = document.getElementById('cnMarketBtn');
    
    // Granularity switching
    const dailyBtn = document.getElementById('dailyBtn');
    const hourlyBtn = document.getElementById('hourlyBtn');

    if (usMarketBtn) {
        usMarketBtn.addEventListener('click', async () => {
            if (dataLoader.getMarket() !== 'us') {
                dataLoader.setMarket('us');
                updateMarketUI();
                await loadDataAndRefresh();
            }
        });
    }

    if (cnMarketBtn) {
        cnMarketBtn.addEventListener('click', async () => {
            const current = dataLoader.getMarket();
            // If not currently in any CN mode, switch to default CN (Hourly)
            if (current !== 'cn' && current !== 'cn_hour') {
                dataLoader.setMarket('cn_hour');
                updateMarketUI();
                await loadDataAndRefresh();
            }
        });
    }

    if (dailyBtn) {
        dailyBtn.addEventListener('click', async () => {
            if (dataLoader.getMarket() !== 'cn') {
                dataLoader.setMarket('cn');
                updateMarketUI();
                await loadDataAndRefresh();
            }
        });
    }

    if (hourlyBtn) {
        hourlyBtn.addEventListener('click', async () => {
            if (dataLoader.getMarket() !== 'cn_hour') {
                dataLoader.setMarket('cn_hour');
                updateMarketUI();
                await loadDataAndRefresh();
            }
        });
    }

    // Scroll to top button
    const scrollBtn = document.getElementById('scrollToTop');
    window.addEventListener('scroll', () => {
        if (window.pageYOffset > 300) {
            scrollBtn.classList.add('visible');
        } else {
            scrollBtn.classList.remove('visible');
        }
    });

    scrollBtn.addEventListener('click', () => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });

    // Search functionality for holdings
    const holdingsSearch = document.getElementById('holdingsSearch');
    if (holdingsSearch) {
        holdingsSearch.addEventListener('input', () => {
            applyHoldingsFilters();
        });
    }

    // Date picker for holdings
    const holdingsDate = document.getElementById('holdingsDate');
    if (holdingsDate) {
        holdingsDate.addEventListener('change', () => {
            applyHoldingsFilters();
        });
    }

    // Reset button for holdings date
    const holdingsDateReset = document.getElementById('holdingsDateReset');
    if (holdingsDateReset) {
        holdingsDateReset.addEventListener('click', () => {
            const dateInput = document.getElementById('holdingsDate');
            if (dateInput) dateInput.value = '';
            applyHoldingsFilters();
        });
    }

    // Search functionality for trade history
    const tradeSearch = document.getElementById('tradeSearch');
    if (tradeSearch) {
        tradeSearch.addEventListener('input', () => {
            applyTradeFilters();
        });
    }

    // Date pickers for trade history
    const tradeStartDate = document.getElementById('tradeStartDate');
    const tradeEndDate = document.getElementById('tradeEndDate');

    if (tradeStartDate) {
        tradeStartDate.addEventListener('change', () => {
            applyTradeFilters();
        });
    }

    if (tradeEndDate) {
        tradeEndDate.addEventListener('change', () => {
            applyTradeFilters();
        });
    }

    // Reset button for trade dates
    const tradeDateReset = document.getElementById('tradeDateReset');
    if (tradeDateReset) {
        tradeDateReset.addEventListener('click', () => {
            const startInput = document.getElementById('tradeStartDate');
            const endInput = document.getElementById('tradeEndDate');
            if (startInput) startInput.value = '';
            if (endInput) endInput.value = '';
            applyTradeFilters();
        });
    }
}

// Loading overlay controls
function showLoading() {
    document.getElementById('loadingOverlay').classList.remove('hidden');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.add('hidden');
}

// Initialize on page load
window.addEventListener('DOMContentLoaded', init);