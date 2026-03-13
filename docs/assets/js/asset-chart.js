// Asset Evolution Chart
// Main page visualization

const dataLoader = new DataLoader();
window.dataLoader = dataLoader; // Export to global for transaction-loader
let chartInstance = null;
let allAgentsData = {};
let isLogScale = false;

// Color palette for different agents
const agentColors = [
    '#4A90D9', // Finance Blue
    '#6CB4EE', // Light Blue
    '#E8596E', // Coral
    '#F0C254', // Warm Gold
    '#7C6BDB', // Soft Purple
    '#5BA0E5', // Sky Blue
    '#E87D4F', // Warm Orange
    '#4ECB8D'  // Mint
];

// Cache for loaded SVG images
const iconImageCache = {};

// Function to load SVG as image
function loadIconImage(iconPath) {
    return new Promise((resolve, reject) => {
        if (iconImageCache[iconPath]) {
            resolve(iconImageCache[iconPath]);
            return;
        }
        
        const img = new Image();
        img.onload = () => {
            iconImageCache[iconPath] = img;
            resolve(img);
        };
        img.onerror = reject;
        img.src = iconPath;
    });
}

// Update market subtitle based on current market
function updateMarketSubtitle() {
    console.log('[updateMarketSubtitle] Starting...');
    console.log('[updateMarketSubtitle] Current market:', dataLoader.getMarket());

    const marketConfig = dataLoader.getMarketConfig();
    console.log('[updateMarketSubtitle] Market config:', marketConfig);

    const subtitleElement = document.getElementById('marketSubtitle');
    console.log('[updateMarketSubtitle] Subtitle element:', subtitleElement);

    if (marketConfig && marketConfig.subtitle && subtitleElement) {
        subtitleElement.textContent = marketConfig.subtitle;
        console.log('Updated subtitle to:', marketConfig.subtitle);
    } else {
        console.warn('[updateMarketSubtitle] Missing required data:', {
            hasMarketConfig: !!marketConfig,
            hasSubtitle: marketConfig?.subtitle,
            hasElement: !!subtitleElement
        });
    }
}

// Load data and refresh UI
async function loadDataAndRefresh() {
    showLoading();

    try {
        // Ensure config is loaded first
        await dataLoader.initialize();

        // Update subtitle for the current market
        updateMarketSubtitle();

        // Load all agents data
        console.log('Loading all agents data...');
        allAgentsData = await dataLoader.loadAllAgentsData();
        console.log('Data loaded:', allAgentsData);

        // Preload all agent icons
        const agentNames = Object.keys(allAgentsData);
        const iconPromises = agentNames.map(agentName => {
            const iconPath = dataLoader.getAgentIcon(agentName);
            return loadIconImage(iconPath).catch(err => {
                console.warn(`Failed to load icon for ${agentName}:`, err);
            });
        });
        await Promise.all(iconPromises);
        console.log('Icons preloaded');

        // Destroy existing chart if it exists
        if (chartInstance) {
            console.log('Destroying existing chart...');
            chartInstance.destroy();
            chartInstance = null;
        }

        // Update stats
        updateStats();

        // Create chart
        createChart();

        // Create legend
        createLegend();

        // Create leaderboard and action flow
        await createLeaderboard();
        await createActionFlow();

    } catch (error) {
        console.error('Error loading data:', error);
        alert('Failed to load trading data. Please check console for details.');
    } finally {
        hideLoading();
    }
}

// Update market buttons visibility based on enabled markets in config
function updateMarketButtonsVisibility() {
    const config = window.configLoader.config;
    if (!config || !config.markets) return;

    const cnBtn = document.getElementById('cnMarketBtn');
    const granularityWrapper = document.getElementById('granularityWrapper');

    // CN market is always shown since we have data
    if (cnBtn) {
        cnBtn.style.display = '';
    }

    // Always show granularity wrapper for CN market (daily/hourly toggle)
    if (granularityWrapper) {
        granularityWrapper.classList.remove('hidden');
    }

    console.log('Market buttons visibility updated for CN market');
}

// Initialize the page
async function init() {
    console.log('[init] Starting initialization...');

    // Set up event listeners first
    setupEventListeners();

    // Load config first to determine enabled markets
    console.log('[init] Loading config...');
    await window.configLoader.loadConfig();
    console.log('[init] Config loaded:', window.configLoader.config);

    // Default to CN daily market (A-shares)
    dataLoader.setMarket('cn');
    console.log('[init] Initial market set to: cn');

    // Update market buttons visibility based on config
    console.log('[init] Updating market buttons visibility...');
    updateMarketButtonsVisibility();

    // Load initial data
    console.log('[init] Loading data...');
    await loadDataAndRefresh();

    // Initialize UI state
    console.log('[init] Updating market UI...');
    updateMarketUI();

    console.log('[init] Initialization complete. Current market:', dataLoader.getMarket());
}

// Update statistics cards
function updateStats() {
    const agentNames = Object.keys(allAgentsData);
    const agentCount = agentNames.length;

    // Calculate date range
    let minDate = null;
    let maxDate = null;

    agentNames.forEach(name => {
        const history = allAgentsData[name].assetHistory;
        if (history.length > 0) {
            const firstDate = history[0].date;
            const lastDate = history[history.length - 1].date;

            if (!minDate || firstDate < minDate) minDate = firstDate;
            if (!maxDate || lastDate > maxDate) maxDate = lastDate;
        }
    });

    // Find best performer (exclude benchmarks)
    let bestAgent = null;
    let bestReturn = -Infinity;
    const isBenchmarkName = (name) => name.includes('QQQ') || name.includes('SSE') || name.includes('上证');

    agentNames.forEach(name => {
        if (isBenchmarkName(name)) return; // Skip benchmarks
        const returnValue = allAgentsData[name].return;
        if (returnValue > bestReturn) {
            bestReturn = returnValue;
            bestAgent = name;
        }
    });

    // Update DOM (exclude benchmarks from count)
    const actualAgentCount = agentNames.filter(n => !isBenchmarkName(n)).length;
    document.getElementById('agent-count').textContent = actualAgentCount;

    // Format date range - compact single-line display
    const formatTradingPeriod = (startStr, endStr) => {
        if (!startStr || !endStr) return 'N/A';
        const s = new Date(startStr), e = new Date(endStr);
        const sMonth = s.toLocaleString('en-US', { month: 'short' });
        const eMonth = e.toLocaleString('en-US', { month: 'short' });
        // Same year: "Feb 25 – Mar 12, 2026"; different year: "Dec 1, 2025 – Jan 5, 2026"
        if (s.getFullYear() === e.getFullYear()) {
            return `${sMonth} ${s.getDate()} – ${eMonth} ${e.getDate()}, ${e.getFullYear()}`;
        }
        return `${sMonth} ${s.getDate()}, ${s.getFullYear()} – ${eMonth} ${e.getDate()}, ${e.getFullYear()}`;
    };

    document.getElementById('trading-period').textContent =
        formatTradingPeriod(minDate, maxDate);
    document.getElementById('best-performer').textContent = bestAgent ?
        dataLoader.getAgentDisplayName(bestAgent) : 'N/A';
    document.getElementById('avg-return').textContent = bestAgent ?
        dataLoader.formatPercent(bestReturn) : 'N/A';
}

// Create the main chart
function createChart() {
    const ctx = document.getElementById('assetChart').getContext('2d');

    // Collect all unique dates and sort them
    const allDates = new Set();
    Object.keys(allAgentsData).forEach(agentName => {
        allAgentsData[agentName].assetHistory.forEach(h => allDates.add(h.date));
    });
    const sortedDates = Array.from(allDates).sort();

    console.log('=== CHART DEBUG ===');
    console.log('Total unique dates:', sortedDates.length);
    console.log('Date range:', sortedDates[0], 'to', sortedDates[sortedDates.length - 1]);
    console.log('Agent names:', Object.keys(allAgentsData));

    const datasets = Object.keys(allAgentsData).map((agentName, index) => {
        const data = allAgentsData[agentName];
        let color, borderWidth, borderDash;

        // Special styling for benchmarks
        const isBenchmark = agentName.includes('QQQ') || agentName.includes('SSE') || agentName.includes('上证');
        if (isBenchmark) {
            color = dataLoader.getAgentBrandColor(agentName) || '#E87D4F';
            borderWidth = 2;
            borderDash = [5, 5]; // Dashed line for benchmark
        } else {
            color = dataLoader.getAgentBrandColor(agentName) || agentColors[index % agentColors.length];
            borderWidth = 3;
            borderDash = [];
        }

        console.log(`[DATASET ${index}] ${agentName} => COLOR: ${color}, isBenchmark: ${isBenchmark}`);

        // Create data points for all dates, filling missing dates with null
        const chartData = sortedDates.map(date => {
            const historyEntry = data.assetHistory.find(h => h.date === date);
            return {
                x: date,
                y: historyEntry ? historyEntry.value : null
            };
        });

        // Detect if we have hourly data (many data points with time component)
        const isHourlyData = sortedDates.length > 50 && sortedDates[0].includes(':');

        const datasetObj = {
            label: dataLoader.getAgentDisplayName(agentName),
            data: chartData,
            borderColor: color,
            backgroundColor: isBenchmark ? 'transparent' : createGradient(ctx, color), // Keep gradient for all
            borderWidth: borderWidth,
            borderDash: borderDash,
            tension: isHourlyData ? 0.45 : 0.4, // More smoothing for dense hourly data
            pointRadius: 0,
            pointHoverRadius: 7,
            pointHoverBackgroundColor: color,
            pointHoverBorderColor: '#fff',
            pointHoverBorderWidth: 3,
            fill: !isBenchmark, // Fill for all non-benchmark agents
            spanGaps: true, // Draw continuous lines even with missing data points
            segment: {
                borderColor: color,
            },
            agentName: agentName,
            agentIcon: dataLoader.getAgentIcon(agentName)
        };

        console.log(`[DATASET OBJECT ${index}] borderColor: ${datasetObj.borderColor}, pointHoverBackgroundColor: ${datasetObj.pointHoverBackgroundColor}`);

        return datasetObj;
    });

    // Sort datasets: benchmarks first (rendered underneath), then agents on top
    datasets.sort((a, b) => {
        const aIsBench = a.borderDash && a.borderDash.length > 0 ? 0 : 1;
        const bIsBench = b.borderDash && b.borderDash.length > 0 ? 0 : 1;
        return aIsBench - bIsBench;
    });

    // Create gradient for area fills
    function createGradient(ctx, color) {
        // Parse color and create gradient
        const gradient = ctx.createLinearGradient(0, 0, 0, 400);
        gradient.addColorStop(0, color + '30'); // 30% opacity at top
        gradient.addColorStop(0.5, color + '15'); // 15% opacity at middle
        gradient.addColorStop(1, color + '05'); // 5% opacity at bottom
        return gradient;
    }

    // Custom plugin to draw icons on chart lines with pulsing animation
    const iconPlugin = {
        id: 'iconLabels',
        afterDatasetsDraw: (chart) => {
            const ctx = chart.ctx;
            const now = Date.now();

            chart.data.datasets.forEach((dataset, datasetIndex) => {
                const meta = chart.getDatasetMeta(datasetIndex);
                if (!meta.hidden && dataset.data.length > 0) {
                    // Get the last non-null data point (benchmark may have trailing nulls)
                    let lastPoint = null;
                    for (let i = dataset.data.length - 1; i >= 0; i--) {
                        if (dataset.data[i].y !== null && meta.data[i]) {
                            lastPoint = meta.data[i];
                            break;
                        }
                    }

                    if (lastPoint) {
                        const x = lastPoint.x;
                        const y = lastPoint.y;

                        ctx.save();

                        // Calculate pulse animation values
                        const pulseSpeed = 1500; // milliseconds per cycle
                        const phase = ((now + datasetIndex * 300) % pulseSpeed) / pulseSpeed; // Offset each line
                        const pulse = Math.sin(phase * Math.PI * 2) * 0.5 + 0.5; // 0 to 1

                        // Draw animated ripple rings (outer glow effect)
                        for (let i = 0; i < 3; i++) {
                            const ripplePhase = ((now + datasetIndex * 300 + i * 500) % 2000) / 2000;
                            const rippleSize = 6 + ripplePhase * 20;
                            const rippleOpacity = (1 - ripplePhase) * 0.4;

                            ctx.strokeStyle = dataset.borderColor;
                            ctx.globalAlpha = rippleOpacity;
                            ctx.lineWidth = 2;
                            ctx.beginPath();
                            ctx.arc(x, y, rippleSize, 0, Math.PI * 2);
                            ctx.stroke();
                        }

                        ctx.globalAlpha = 1;

                        // Draw main pulsing point
                        const pointSize = 5 + pulse * 3;

                        // Outer glow
                        ctx.shadowColor = dataset.borderColor;
                        ctx.shadowBlur = 10 + pulse * 15;
                        ctx.fillStyle = dataset.borderColor;
                        ctx.beginPath();
                        ctx.arc(x, y, pointSize, 0, Math.PI * 2);
                        ctx.fill();

                        // Inner bright core
                        ctx.shadowBlur = 5;
                        ctx.fillStyle = '#ffffff';
                        ctx.beginPath();
                        ctx.arc(x, y, pointSize * 0.5, 0, Math.PI * 2);
                        ctx.fill();

                        // Reset shadow
                        ctx.shadowBlur = 0;

                        // Draw icon image with glow background (positioned to the right)
                        const iconSize = 30;
                        const iconX = x + 22;

                        // Icon background circle with glow
                        ctx.shadowColor = dataset.borderColor;
                        ctx.shadowBlur = 15;
                        ctx.fillStyle = dataset.borderColor;
                        ctx.beginPath();
                        ctx.arc(iconX, y, iconSize / 2, 0, Math.PI * 2);
                        ctx.fill();

                        // Reset shadow for icon
                        ctx.shadowBlur = 0;

                        // Draw icon image if loaded
                        if (iconImageCache[dataset.agentIcon]) {
                            const img = iconImageCache[dataset.agentIcon];
                            const imgSize = iconSize * 0.6; // Icon slightly smaller than circle
                            ctx.drawImage(img, iconX - imgSize/2, y - imgSize/2, imgSize, imgSize);
                        }

                        ctx.restore();
                    }
                }
            });

            // Request animation frame to continuously update the pulse effect
            requestAnimationFrame(() => {
                if (chartInstance && !chartInstance.destroyed) {
                    chartInstance.update('none'); // Update without animation to maintain smooth pulse
                }
            });
        }
    };

    console.log('Creating chart with', datasets.length, 'datasets');
    console.log('Datasets summary:', datasets.map(d => ({
        label: d.label,
        borderColor: d.borderColor,
        backgroundColor: typeof d.backgroundColor === 'string' ? d.backgroundColor : 'GRADIENT',
        dataPoints: d.data.filter(p => p.y !== null).length,
        borderWidth: d.borderWidth,
        fill: d.fill
    })));

    // DEBUG: Log the actual Chart.js config
    console.log('[CHART.JS CONFIG] About to create chart with datasets:', JSON.stringify(
        datasets.map(d => ({ label: d.label, borderColor: d.borderColor }))
    ));

    chartInstance = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            resizeDelay: 200,
            layout: {
                padding: {
                    right: 50,
                    top: 10,
                    bottom: 10
                }
            },
            interaction: {
                mode: 'index',
                intersect: false
            },
            elements: {
                line: {
                    borderJoinStyle: 'round',
                    borderCapStyle: 'round'
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    enabled: false,
                    external: function(context) {
                        // Custom HTML tooltip
                        const tooltipModel = context.tooltip;
                        let tooltipEl = document.getElementById('chartjs-tooltip');

                        // Create element on first render
                        if (!tooltipEl) {
                            tooltipEl = document.createElement('div');
                            tooltipEl.id = 'chartjs-tooltip';
                            tooltipEl.innerHTML = '<div class="tooltip-container"></div>';
                            document.body.appendChild(tooltipEl);
                        }

                        // Hide if no tooltip
                        if (tooltipModel.opacity === 0) {
                            tooltipEl.style.opacity = 0;
                            return;
                        }

                        // Set Text
                        if (tooltipModel.body) {
                            const dataPoints = tooltipModel.dataPoints || [];

                            // Sort data points by value at this time point (descending)
                            const sortedPoints = [...dataPoints].sort((a, b) => {
                                const valueA = a.parsed.y || 0;
                                const valueB = b.parsed.y || 0;
                                return valueB - valueA;
                            });

                            // Format title (date/time)
                            const titleLines = tooltipModel.title || [];
                            let titleHtml = '';
                            if (titleLines.length > 0) {
                                const dateStr = titleLines[0];
                                if (dateStr && dateStr.includes(':')) {
                                    const date = new Date(dateStr);
                                    titleHtml = date.toLocaleString('en-US', {
                                        month: 'short',
                                        day: 'numeric',
                                        year: 'numeric',
                                        hour: '2-digit',
                                        minute: '2-digit'
                                    });
                                } else {
                                    titleHtml = dateStr;
                                }
                            }

                            // Build body HTML with logos and ranked data
                            let innerHtml = `<div class="tooltip-title">${titleHtml}</div>`;
                            innerHtml += '<div class="tooltip-body">';

                            sortedPoints.forEach((dataPoint, index) => {
                                const dataset = dataPoint.dataset;
                                const agentName = dataset.agentName;
                                const displayName = dataset.label;
                                const value = dataPoint.parsed.y;
                                const icon = dataLoader.getAgentIcon(agentName);
                                const color = dataset.borderColor;

                                // Add ranking badge
                                const rankBadge = `<span class="rank-badge">#${index + 1}</span>`;

                                innerHtml += `
                                    <div class="tooltip-row">
                                        ${rankBadge}
                                        <img src="${icon}" class="tooltip-icon" alt="${displayName}">
                                        <span class="tooltip-label" style="color: ${color}">${displayName}</span>
                                        <span class="tooltip-value">${dataLoader.formatCurrency(value)}</span>
                                    </div>
                                `;
                            });

                            innerHtml += '</div>';

                            const container = tooltipEl.querySelector('.tooltip-container');
                            container.innerHTML = innerHtml;
                        }

                        const position = context.chart.canvas.getBoundingClientRect();
                        const tooltipWidth = tooltipEl.offsetWidth || 300;
                        const tooltipHeight = tooltipEl.offsetHeight || 200;

                        // Smart positioning to prevent overflow
                        let left = position.left + window.pageXOffset + tooltipModel.caretX;
                        let top = position.top + window.pageYOffset + tooltipModel.caretY;

                        // Offset to prevent covering the hover point
                        const offset = 15;
                        left += offset;
                        top -= offset;

                        // Check if tooltip would go off right edge
                        const viewportWidth = window.innerWidth;
                        const viewportHeight = window.innerHeight;

                        if (left + tooltipWidth > viewportWidth - 20) {
                            // Position to the left of the cursor instead
                            left = position.left + window.pageXOffset + tooltipModel.caretX - tooltipWidth - offset;
                        }

                        // Check if tooltip would go off bottom edge
                        if (top + tooltipHeight > viewportHeight - 20) {
                            top = viewportHeight - tooltipHeight - 20;
                        }

                        // Check if tooltip would go off top edge
                        if (top < 20) {
                            top = 20;
                        }

                        // Check if tooltip would go off left edge
                        if (left < 20) {
                            left = 20;
                        }

                        // Display, position, and set styles
                        tooltipEl.style.opacity = 1;
                        tooltipEl.style.position = 'absolute';
                        tooltipEl.style.left = left + 'px';
                        tooltipEl.style.top = top + 'px';
                        tooltipEl.style.pointerEvents = 'none';
                        tooltipEl.style.transition = 'opacity 0.2s ease, transform 0.2s ease';
                        tooltipEl.style.transform = 'translateZ(0)'; // GPU acceleration
                    }
                }
            },
            scales: {
                x: {
                    type: 'category',
                    labels: sortedDates,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.06)',
                        drawBorder: false,
                        lineWidth: 1
                    },
                    ticks: {
                        color: '#9AA0A6',
                        maxRotation: 45,
                        minRotation: 45,
                        autoSkip: true,
                        maxTicksLimit: 15,
                        font: {
                            size: 11
                        },
                        callback: function(value, index) {
                            // Format hourly timestamps for better readability
                            const dateStr = this.getLabelForValue(value);
                            if (!dateStr) return '';

                            // If it's an hourly timestamp (contains time)
                            if (dateStr.includes(':')) {
                                const date = new Date(dateStr);
                                // Show date and hour
                                const month = (date.getMonth() + 1).toString().padStart(2, '0');
                                const day = date.getDate().toString().padStart(2, '0');
                                const hour = date.getHours().toString().padStart(2, '0');
                                return `${month}/${day} ${hour}:00`;
                            }
                            return dateStr;
                        }
                    }
                },
                y: {
                    type: isLogScale ? 'logarithmic' : 'linear',
                    grid: {
                        color: 'rgba(255, 255, 255, 0.06)',
                        drawBorder: false,
                        lineWidth: 1
                    },
                    ticks: {
                        color: '#9AA0A6',
                        callback: function(value) {
                            return dataLoader.formatCurrency(value);
                        },
                        font: {
                            size: 11
                        }
                    }
                }
            }
        },
        plugins: [iconPlugin]
    });
}

// Create legend
function createLegend() {
    const legendContainer = document.getElementById('agentLegend');
    legendContainer.innerHTML = '';

    Object.keys(allAgentsData).forEach((agentName, index) => {
        const data = allAgentsData[agentName];
        let color, borderStyle;

        // Special styling for benchmarks
        const isBenchmark = agentName.includes('QQQ') || agentName.includes('SSE') || agentName.includes('上证');
        if (isBenchmark) {
            color = dataLoader.getAgentBrandColor(agentName) || '#E87D4F';
            borderStyle = 'dashed';
        } else {
            color = dataLoader.getAgentBrandColor(agentName) || agentColors[index % agentColors.length];
            borderStyle = 'solid';
        }

        console.log(`[LEGEND ${index}] ${agentName} => COLOR: ${color}, isBenchmark: ${isBenchmark}`);
        
        const returnValue = data.return;
        const returnClass = returnValue >= 0 ? 'positive' : 'negative';
        const iconPath = dataLoader.getAgentIcon(agentName);
        const brandColor = dataLoader.getAgentBrandColor(agentName);

        const legendItem = document.createElement('div');
        legendItem.className = 'legend-item';
        legendItem.innerHTML = `
            <div class="legend-icon" ${brandColor ? `style="background: ${brandColor}20;"` : ''}>
                <img src="${iconPath}" alt="${agentName}" class="legend-icon-img" />
            </div>
            <div class="legend-color" style="background: ${color}; border-style: ${borderStyle};"></div>
            <div class="legend-info">
                <div class="legend-name">${dataLoader.getAgentDisplayName(agentName)}</div>
                <div class="legend-return ${returnClass}">${dataLoader.formatPercent(returnValue)}</div>
            </div>
        `;

        legendContainer.appendChild(legendItem);
    });
}

// Toggle between linear and log scale
function toggleScale() {
    isLogScale = !isLogScale;

    const button = document.getElementById('toggle-log');
    button.textContent = isLogScale ? 'Log Scale' : 'Linear Scale';

    // Update chart
    if (chartInstance) {
        chartInstance.destroy();
    }
    createChart();
}

// Export chart data as CSV
function exportData() {
    let csv = 'Date,';

    // Header row with agent names
    const agentNames = Object.keys(allAgentsData);
    csv += agentNames.map(name => dataLoader.getAgentDisplayName(name)).join(',') + '\n';

    // Collect all unique dates
    const allDates = new Set();
    agentNames.forEach(name => {
        allAgentsData[name].assetHistory.forEach(h => allDates.add(h.date));
    });

    // Sort dates
    const sortedDates = Array.from(allDates).sort();

    // Data rows
    sortedDates.forEach(date => {
        const row = [date];
        agentNames.forEach(name => {
            const history = allAgentsData[name].assetHistory;
            const entry = history.find(h => h.date === date);
            row.push(entry ? entry.value.toFixed(2) : '');
        });
        csv += row.join(',') + '\n';
    });

    // Download CSV
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'aitrader_asset_evolution.csv';
    a.click();
    window.URL.revokeObjectURL(url);
}

// Update UI based on current market state
function updateMarketUI() {
    const currentMarket = dataLoader.getMarket();
    const cnBtn = document.getElementById('cnMarketBtn');
    const granularityWrapper = document.getElementById('granularityWrapper');
    const dailyBtn = document.getElementById('dailyBtn');
    const hourlyBtn = document.getElementById('hourlyBtn');

    // Reset active states
    if (cnBtn) cnBtn.classList.remove('active');
    if (dailyBtn) dailyBtn.classList.remove('active');
    if (hourlyBtn) hourlyBtn.classList.remove('active');

    // CN market is always active
    if (cnBtn) cnBtn.classList.add('active');
    if (granularityWrapper) granularityWrapper.classList.remove('hidden');

    if (currentMarket === 'cn_hour') {
        if (hourlyBtn) hourlyBtn.classList.add('active');
    } else {
        if (dailyBtn) dailyBtn.classList.add('active');
    }

    updateMarketSubtitle();
}

// Set up event listeners
function setupEventListeners() {
    document.getElementById('toggle-log').addEventListener('click', toggleScale);
    document.getElementById('export-chart').addEventListener('click', exportData);

    // Market switching
    const cnMarketBtn = document.getElementById('cnMarketBtn');

    // Granularity switching
    const dailyBtn = document.getElementById('dailyBtn');
    const hourlyBtn = document.getElementById('hourlyBtn');

    if (cnMarketBtn) {
        cnMarketBtn.addEventListener('click', async () => {
            const current = dataLoader.getMarket();
            // If not currently in any CN mode, switch to default CN (Daily)
            if (current !== 'cn' && current !== 'cn_hour') {
                dataLoader.setMarket('cn');
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

    // Window resize handler for chart responsiveness
    let resizeTimeout;
    const handleResize = () => {
        clearTimeout(resizeTimeout);
        resizeTimeout = setTimeout(() => {
            if (chartInstance) {
                console.log('Resizing chart...'); // Debug log
                chartInstance.resize();
                chartInstance.update('none'); // Force update without animation
            }
        }, 100); // Faster response
    };

    window.addEventListener('resize', handleResize);

    // Also handle orientation change for mobile
    window.addEventListener('orientationchange', handleResize);
}

// Create leaderboard
async function createLeaderboard() {
    const leaderboard = await window.transactionLoader.buildLeaderboard(allAgentsData);
    const container = document.getElementById('leaderboardList');
    container.innerHTML = '';

    leaderboard.forEach((item, index) => {
        const rankClass = index === 0 ? 'first' : index === 1 ? 'second' : index === 2 ? 'third' : '';
        const gainClass = item.gain >= 0 ? 'positive' : 'negative';

        const itemEl = document.createElement('div');
        itemEl.className = 'leaderboard-item';
        itemEl.style.animationDelay = `${index * 0.05}s`;
        itemEl.innerHTML = `
            <div class="leaderboard-rank ${rankClass}">#${item.rank}</div>
            <div class="leaderboard-icon">
                <img src="${item.icon}" alt="${item.displayName}">
            </div>
            <div class="leaderboard-info">
                <div class="leaderboard-name">${item.displayName}</div>
                <div class="leaderboard-value">${window.transactionLoader.formatCurrency(item.currentValue)}</div>
            </div>
            <div class="leaderboard-gain">
                <div class="gain-amount ${gainClass}">${window.transactionLoader.formatCurrency(item.gain)}</div>
                <div class="gain-percent ${gainClass}">${window.transactionLoader.formatPercent(item.gainPercent)}</div>
            </div>
        `;

        // Click to show prompt modal
        itemEl.addEventListener('click', () => showPromptModal(item.agentName, item.displayName));
        container.appendChild(itemEl);
    });
}

// Prompt modal: fetch and display agent's latest system prompt
async function showPromptModal(agentName, displayName) {
    const overlay = document.getElementById('promptModalOverlay');
    const title = document.getElementById('promptModalTitle');
    const body = document.getElementById('promptModalBody');
    const dateEl = document.getElementById('promptModalDate');

    title.textContent = displayName + ' — System Prompt';
    body.textContent = 'Loading...';
    dateEl.textContent = '';
    overlay.classList.add('active');

    try {
        const apiBase = window.configLoader.getApiBaseUrl();
        const market = window.dataLoader ? window.dataLoader.getMarket() : 'cn';
        // Try live agent name first (with -live suffix), then backtest name
        const liveAgent = market === 'cn_hour'
            ? agentName.replace(/-astock-hour$/, '') + '-live-astock-hour'
            : agentName + '-live';

        let data = null;
        for (const name of [liveAgent, agentName]) {
            try {
                const resp = await fetch(apiBase + '/api/logs/' + encodeURIComponent(name) + '/latest-prompt?market=' + (market === 'cn_hour' ? 'cn' : market));
                if (resp.ok) {
                    data = await resp.json();
                    break;
                }
            } catch (e) { /* try next */ }
        }

        if (data && data.system_prompt) {
            body.innerHTML = formatThinking(data.system_prompt);
            const dateStr = data.session_time
                ? data.session_date + ' ' + data.session_time
                : data.session_date;
            dateEl.textContent = 'Session: ' + dateStr;
        } else {
            body.textContent = '暂无 prompt 数据。下次交易执行后将自动记录。';
        }
    } catch (error) {
        body.textContent = '加载失败: ' + error.message;
    }
}

// Close prompt modal
document.addEventListener('DOMContentLoaded', function() {
    const overlay = document.getElementById('promptModalOverlay');
    const closeBtn = document.getElementById('promptModalClose');
    if (closeBtn) {
        closeBtn.addEventListener('click', function() { overlay.classList.remove('active'); });
    }
    if (overlay) {
        overlay.addEventListener('click', function(e) {
            if (e.target === overlay) overlay.classList.remove('active');
        });
    }
});

// Create action flow with pagination
let actionFlowState = {
    allTransactions: [],      // All entries (including no_trade)
    filteredTransactions: [], // Filtered/grouped transactions to display
    loadedCount: 0,
    pageSize: 20,
    maxTransactions: 100,
    isLoading: false,
    isFiltering: false,       // Flag to disable animations during filtering
    container: null,
    // Filter state
    searchQuery: '',
    startDate: null,
    endDate: null,
    hideNoTrade: false,        // Show no-trade entries by default
    modelFilter: ''            // Empty = all models
};

async function createActionFlow() {
    // Load all entries (including no_trade) for the action flow
    await window.transactionLoader.loadAllEntries();
    actionFlowState.allTransactions = window.transactionLoader.allEntries.slice(0, 500);
    actionFlowState.container = document.getElementById('actionList');
    actionFlowState.container.innerHTML = '';
    actionFlowState.loadedCount = 0;

    // Sync checkbox state
    const hideCheckbox = document.getElementById('hideNoTrade');
    if (hideCheckbox) {
        actionFlowState.hideNoTrade = hideCheckbox.checked;
    }

    // Populate model filter dropdown
    populateModelFilter();

    // Initialize date picker constraints
    initActionDatePickers();

    // Apply initial filters (no filters = show all)
    applyActionFilters();

    // Set up scroll listener
    setupScrollListener();

    // Set up filter event listeners
    setupActionFilterListeners();
}

// Populate model filter dropdown from available agents
function populateModelFilter() {
    const select = document.getElementById('actionModelFilter');
    if (!select) return;

    const currentMarket = dataLoader.getMarket();
    const agents = window.configLoader.getEnabledAgents(currentMarket);

    // Reset options - keep only the "All Models" default
    while (select.options.length > 1) {
        select.remove(1);
    }

    for (const agent of agents) {
        const option = document.createElement('option');
        option.value = agent.folder;
        option.textContent = agent.display_name || agent.folder;
        select.appendChild(option);
    }
}

// Flatpickr instances stored for reset
let fpStart = null;
let fpEnd = null;

// Initialize flatpickr date pickers
function initActionDatePickers() {
    const startInput = document.getElementById('actionStartDate');
    const endInput = document.getElementById('actionEndDate');
    if (!startInput || !endInput) return;

    // Compute date bounds from transactions
    let minDate = null;
    let maxDate = null;
    if (actionFlowState.allTransactions.length > 0) {
        const dates = actionFlowState.allTransactions.map(t => t.date.split(' ')[0]);
        minDate = dates[dates.length - 1];
        maxDate = dates[0];
    }

    const commonOpts = {
        dateFormat: 'Y-m-d',
        altInput: true,
        altFormat: 'M j',           // Display as "Mar 5"
        altInputClass: 'flatpickr-alt',
        theme: 'dark',
        disableMobile: true,
        minDate: minDate,
        maxDate: maxDate,
        monthSelectorType: 'static',
        animate: true,
    };

    // Destroy previous instances if re-initializing
    if (fpStart) { fpStart.destroy(); fpStart = null; }
    if (fpEnd) { fpEnd.destroy(); fpEnd = null; }

    fpStart = flatpickr(startInput, {
        ...commonOpts,
        placeholder: 'Start',
        onChange: function(selectedDates, dateStr) {
            actionFlowState.startDate = dateStr || null;
            // Update end picker min date
            if (fpEnd && dateStr) fpEnd.set('minDate', dateStr);
            applyActionFilters();
        }
    });

    fpEnd = flatpickr(endInput, {
        ...commonOpts,
        placeholder: 'End',
        onChange: function(selectedDates, dateStr) {
            actionFlowState.endDate = dateStr || null;
            // Update start picker max date
            if (fpStart && dateStr) fpStart.set('maxDate', dateStr);
            applyActionFilters();
        }
    });
}

// Debounce timer for search input
let actionFilterDebounceTimer = null;

// Debounced filter function
function debouncedApplyActionFilters(delay = 300) {
    if (actionFilterDebounceTimer) {
        clearTimeout(actionFilterDebounceTimer);
    }
    actionFilterDebounceTimer = setTimeout(() => {
        applyActionFilters();
    }, delay);
}

// Set up filter event listeners
function setupActionFilterListeners() {
    const searchInput = document.getElementById('actionSearch');
    const resetBtn = document.getElementById('actionDateReset');

    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            actionFlowState.searchQuery = e.target.value.trim();
            debouncedApplyActionFilters(250);
        });
    }

    // Date change is handled by flatpickr onChange callbacks in initActionDatePickers

    if (resetBtn) {
        resetBtn.addEventListener('click', () => {
            if (fpStart) { fpStart.clear(); fpStart.set('maxDate', fpStart.config.maxDate); }
            if (fpEnd) { fpEnd.clear(); fpEnd.set('minDate', fpEnd.config.minDate); }
            actionFlowState.startDate = null;
            actionFlowState.endDate = null;
            applyActionFilters();
        });
    }

    const modelSelect = document.getElementById('actionModelFilter');
    if (modelSelect) {
        modelSelect.addEventListener('change', (e) => {
            actionFlowState.modelFilter = e.target.value;
            applyActionFilters();
        });
    }

    const hideNoTradeCheckbox = document.getElementById('hideNoTrade');
    if (hideNoTradeCheckbox) {
        hideNoTradeCheckbox.addEventListener('change', (e) => {
            actionFlowState.hideNoTrade = e.target.checked;
            applyActionFilters();
        });
    }
}

// Group flat entries by agent + date into grouped objects
function groupTransactions(entries) {
    const map = new Map();
    for (const t of entries) {
        const key = `${t.agentFolder}|||${t.date}`;
        if (!map.has(key)) {
            map.set(key, { agentFolder: t.agentFolder, date: t.date, trades: [] });
        }
        // Only add actual trades (not no_trade) to the trades list
        if (t.action && t.action !== 'no_trade' && t.action !== 'initial' && t.amount > 0 && t.symbol) {
            map.get(key).trades.push({ action: t.action, symbol: t.symbol, amount: t.amount });
        }
    }
    return Array.from(map.values());
}

// Build Tencent Finance URL from A-share symbol
function buildStockUrl(symbol) {
    if (!symbol || !symbol.includes('.')) return null;
    const [code, suffix] = symbol.split('.');
    const exchange = suffix === 'SH' ? 'sh' : suffix === 'SZ' ? 'sz' : null;
    if (!exchange) return null;
    return `https://stockapp.finance.qq.com/mstats/#/detail/${exchange}/${code}`;
}

// Apply filters and refresh the action list
function applyActionFilters() {
    let filtered = actionFlowState.allTransactions;

    // Filter by model
    if (actionFlowState.modelFilter) {
        filtered = filtered.filter(t => t.agentFolder === actionFlowState.modelFilter);
    }

    // Filter by date range
    if (actionFlowState.startDate || actionFlowState.endDate) {
        filtered = filtered.filter(t => {
            const transDate = t.date.split(' ')[0];
            if (actionFlowState.startDate && transDate < actionFlowState.startDate) return false;
            if (actionFlowState.endDate && transDate > actionFlowState.endDate) return false;
            return true;
        });
    }

    // Group by agent + date first, then apply text search on groups
    // This ensures stock code search matches any trade within the group
    let groups = groupTransactions(filtered);

    // Filter by search query on grouped data
    if (actionFlowState.searchQuery) {
        const query = actionFlowState.searchQuery.toLowerCase();
        const currentMarket = dataLoader.getMarket();
        groups = groups.filter(g => {
            const agentMatch = g.agentFolder.toLowerCase().includes(query);
            const displayName = window.configLoader.getDisplayName(g.agentFolder, currentMarket) || '';
            const displayNameMatch = displayName.toLowerCase().includes(query);
            // Check if any trade in the group matches the symbol or action
            const tradeMatch = g.trades.some(t =>
                t.symbol.toLowerCase().includes(query) ||
                t.action.toLowerCase().includes(query)
            );
            return agentMatch || displayNameMatch || tradeMatch;
        });
    }

    // Optionally hide no-trade groups
    if (actionFlowState.hideNoTrade) {
        groups = groups.filter(g => g.trades.length > 0);
    }

    actionFlowState.filteredTransactions = groups.slice(0, actionFlowState.maxTransactions);

    // Preserve container height to prevent layout shift
    const currentHeight = actionFlowState.container.offsetHeight;
    if (currentHeight > 0) {
        actionFlowState.container.style.minHeight = currentHeight + 'px';
    }

    // Reset and reload
    actionFlowState.container.innerHTML = '';
    actionFlowState.loadedCount = 0;

    // Mark as filtering to disable animations
    actionFlowState.isFiltering = true;

    // Load initial batch
    loadMoreTransactions().then(() => {
        // Reset min-height after content is loaded
        setTimeout(() => {
            actionFlowState.container.style.minHeight = '';
            actionFlowState.isFiltering = false;
        }, 100);
    });
}

async function loadMoreTransactions() {
    if (actionFlowState.isLoading) return;

    // Use filtered (grouped) transactions
    const groups = actionFlowState.filteredTransactions;
    if (actionFlowState.loadedCount >= groups.length) return;
    if (actionFlowState.loadedCount >= actionFlowState.maxTransactions) return;

    actionFlowState.isLoading = true;

    // Show loading indicator
    showLoadingIndicator();

    // Calculate how many to load
    const startIndex = actionFlowState.loadedCount;
    const endIndex = Math.min(
        startIndex + actionFlowState.pageSize,
        groups.length,
        actionFlowState.maxTransactions
    );

    // Handle empty results
    if (groups.length === 0) {
        actionFlowState.isLoading = false;
        hideLoadingIndicator();
        showNoResultsMessage();
        return;
    }

    // Load this batch
    for (let i = startIndex; i < endIndex; i++) {
        const group = groups[i];
        const agentName = group.agentFolder;
        const currentMarket = dataLoader.getMarket();
        const displayName = window.configLoader.getDisplayName(agentName, currentMarket);
        const icon = window.configLoader.getIcon(agentName, currentMarket);

        // Load agent's thinking (once per group)
        const thinking = await window.transactionLoader.loadAgentThinking(agentName, group.date, currentMarket);

        const cardEl = document.createElement('div');
        cardEl.className = 'action-card' + (actionFlowState.isFiltering ? ' no-animation' : '');
        if (!actionFlowState.isFiltering) {
            cardEl.style.animationDelay = `${(i % actionFlowState.pageSize) * 0.03}s`;
        }

        // Build trades list HTML
        let tradesHTML;
        if (group.trades.length > 0) {
            tradesHTML = group.trades.map(t => {
                const stockUrl = buildStockUrl(t.symbol);
                const symbolHTML = stockUrl
                    ? `<a class="action-symbol" href="${stockUrl}" target="_blank" rel="noopener">${t.symbol}</a>`
                    : `<span class="action-symbol">${t.symbol}</span>`;
                return `<div class="action-trade-item">
                    <span class="action-type ${t.action}">${t.action}</span>
                    ${symbolHTML}
                    <span>&times;${t.amount}</span>
                </div>`;
            }).join('');
        } else {
            tradesHTML = `<div class="action-trade-item action-no-trade"><span class="action-type no-trade">no trade</span></div>`;
        }

        let cardHTML = `
            <div class="action-header">
                <div class="action-agent-icon">
                    <img src="${icon}" alt="${displayName}">
                </div>
                <div class="action-meta">
                    <div class="action-agent-name">${displayName}</div>
                </div>
                <div class="action-timestamp">${window.transactionLoader.formatDateTime(group.date)}</div>
                <button class="action-comment-btn" title="Add comment"
                    data-agent="${agentName}"
                    data-date="${group.date}">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
                    </svg>
                </button>
            </div>
            <div class="action-trades-list">${tradesHTML}</div>
        `;

        if (thinking !== null) {
            cardHTML += `
            <div class="action-body">
                <div class="action-thinking-label">
                    <span class="thinking-icon">🧠</span>
                    Agent Reasoning
                </div>
                <div class="action-thinking">${formatThinking(thinking)}</div>
            </div>
            `;
        }

        cardEl.innerHTML = cardHTML;

        // Remove the status note and loading indicator before adding new cards
        const existingNote = actionFlowState.container.querySelector('.transactions-status-note');
        if (existingNote) existingNote.remove();
        const existingLoader = actionFlowState.container.querySelector('.transactions-loading');
        if (existingLoader) existingLoader.remove();

        actionFlowState.container.appendChild(cardEl);
    }

    actionFlowState.loadedCount = endIndex;
    actionFlowState.isLoading = false;

    // Hide loading indicator and add status note
    hideLoadingIndicator();
    updateStatusNote();

    // Load comment states for newly added cards
    refreshCommentButtons();
}

function showLoadingIndicator() {
    // Remove existing indicator
    const existingLoader = actionFlowState.container.querySelector('.transactions-loading');
    if (existingLoader) {
        existingLoader.remove();
    }

    const loaderEl = document.createElement('div');
    loaderEl.className = 'transactions-loading';
    loaderEl.style.cssText = 'text-align: center; padding: 1.5rem; color: var(--accent); font-size: 0.9rem; font-weight: 500;';
    loaderEl.innerHTML = '⏳ Loading more transactions...';
    actionFlowState.container.appendChild(loaderEl);
}

function hideLoadingIndicator() {
    const existingLoader = actionFlowState.container.querySelector('.transactions-loading');
    if (existingLoader) {
        existingLoader.remove();
    }
}

function updateStatusNote() {
    // Remove existing note
    const existingNote = actionFlowState.container.querySelector('.transactions-status-note');
    if (existingNote) {
        existingNote.remove();
    }

    // Add new note
    const noteEl = document.createElement('div');
    noteEl.className = 'transactions-status-note';
    noteEl.style.cssText = 'text-align: center; padding: 1.5rem; color: var(--text-muted); font-size: 0.9rem;';

    const filteredCount = actionFlowState.filteredTransactions.length;
    const totalCount = actionFlowState.allTransactions.length;
    const loaded = actionFlowState.loadedCount;
    const hasFilters = actionFlowState.searchQuery || actionFlowState.startDate || actionFlowState.endDate || actionFlowState.modelFilter;

    if (hasFilters) {
        // Show filtered results info
        if (loaded >= filteredCount) {
            noteEl.textContent = `Showing all ${loaded} matching transactions (filtered from ${totalCount} total)`;
        } else {
            noteEl.textContent = `Loaded ${loaded} of ${filteredCount} matching transactions. Scroll down to load more...`;
        }
    } else {
        // No filters applied
        if (loaded >= actionFlowState.maxTransactions || loaded >= filteredCount) {
            if (totalCount > actionFlowState.maxTransactions) {
                noteEl.textContent = `Showing the most recent ${loaded} of ${totalCount} total transactions`;
            } else {
                noteEl.textContent = `Showing all ${loaded} recent transactions`;
            }
        } else {
            noteEl.textContent = `Loaded ${loaded} of ${Math.min(filteredCount, actionFlowState.maxTransactions)} transactions. Scroll down to load more...`;
        }
    }

    actionFlowState.container.appendChild(noteEl);
}

// Show no results message
function showNoResultsMessage() {
    const existingNote = actionFlowState.container.querySelector('.transactions-status-note');
    if (existingNote) {
        existingNote.remove();
    }

    const noteEl = document.createElement('div');
    noteEl.className = 'transactions-status-note';
    noteEl.style.cssText = 'text-align: center; padding: 2rem; color: var(--text-muted); font-size: 0.95rem;';
    noteEl.textContent = 'No matching transactions found. Try adjusting your filters.';
    actionFlowState.container.appendChild(noteEl);
}

function setupScrollListener() {
    const container = actionFlowState.container;
    let ticking = false;

    const checkScroll = () => {
        const scrollTop = container.scrollTop;
        const scrollHeight = container.scrollHeight;
        const clientHeight = container.clientHeight;

        // Trigger load when user is within 300px of bottom
        if (scrollHeight - (scrollTop + clientHeight) < 300) {
            if (!actionFlowState.isLoading &&
                actionFlowState.loadedCount < actionFlowState.maxTransactions &&
                actionFlowState.loadedCount < actionFlowState.filteredTransactions.length) {
                loadMoreTransactions();
            }
        }

        ticking = false;
    };

    // Listen to the container's scroll, not window scroll
    container.addEventListener('scroll', () => {
        if (!ticking) {
            window.requestAnimationFrame(() => {
                checkScroll();
            });
            ticking = true;
        }
    });
}

// Lightweight markdown renderer for agent reasoning
function formatThinking(text) {
    if (!text || !text.trim()) return '';

    // Escape HTML entities first to prevent XSS
    const esc = s => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const escaped = esc(text);

    // Split into blocks by double newlines
    const blocks = escaped.split(/\n\n+/).filter(b => b.trim());
    const html = [];

    for (const block of blocks) {
        const trimmed = block.trim();

        // Headings: ## or ###
        const headingMatch = trimmed.match(/^(#{1,4})\s+(.+)$/m);
        if (headingMatch && trimmed.split('\n').length === 1) {
            const level = Math.min(headingMatch[1].length + 2, 6); // h3-h6
            html.push(`<h${level} class="md-heading">${inlineMarkdown(headingMatch[2])}</h${level}>`);
            continue;
        }

        // Check if block is a list (lines starting with - or N.)
        const lines = trimmed.split('\n');
        const isUnorderedList = lines.every(l => /^\s*[-*]\s/.test(l.trim()) || !l.trim());
        const isOrderedList = lines.every(l => /^\s*\d+[.)]\s/.test(l.trim()) || !l.trim());

        if (isUnorderedList && lines.some(l => l.trim())) {
            const items = lines.filter(l => l.trim()).map(l =>
                `<li>${inlineMarkdown(l.trim().replace(/^[-*]\s+/, ''))}</li>`
            );
            html.push(`<ul class="md-list">${items.join('')}</ul>`);
            continue;
        }

        if (isOrderedList && lines.some(l => l.trim())) {
            const items = lines.filter(l => l.trim()).map(l =>
                `<li>${inlineMarkdown(l.trim().replace(/^\d+[.)]\s+/, ''))}</li>`
            );
            html.push(`<ol class="md-list">${items.join('')}</ol>`);
            continue;
        }

        // Regular paragraph (may contain inline list items mixed with text)
        html.push(`<p>${inlineMarkdown(trimmed.replace(/\n/g, '<br>'))}</p>`);
    }

    return html.join('');
}

// Render inline markdown: **bold**, *italic*, `code`, ~~strike~~
function inlineMarkdown(text) {
    return text
        .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
        .replace(/\*(.+?)\*/g, '<em>$1</em>')
        .replace(/`(.+?)`/g, '<code class="md-code">$1</code>')
        .replace(/~~(.+?)~~/g, '<del>$1</del>');
}

// Loading overlay controls
function showLoading() {
    document.getElementById('loadingOverlay').classList.remove('hidden');
}

function hideLoading() {
    document.getElementById('loadingOverlay').classList.add('hidden');
}

// ====================================================
// Trade Comments
// ====================================================

function getCommentApiBase() {
    return window.configLoader.getApiBaseUrl();
}

// Cache of loaded comments per agent: { agentName: [commentObj, ...] }
let _commentCache = {};
let _commentCacheTime = 0;
const COMMENT_CACHE_TTL = 30000; // 30s

// Load all comments for visible agents in a single batch, then update buttons
async function refreshCommentButtons() {
    const buttons = document.querySelectorAll('.action-comment-btn');
    if (buttons.length === 0) return;

    // Collect unique agent names
    const agents = new Set();
    buttons.forEach(btn => agents.add(btn.dataset.agent));

    const now = Date.now();
    const needsRefresh = now - _commentCacheTime > COMMENT_CACHE_TTL;

    if (needsRefresh) {
        const apiBase = getCommentApiBase();
        // Fetch comments per agent (one request per unique agent, not per button)
        const fetches = [...agents].map(async (agent) => {
            try {
                const resp = await fetch(`${apiBase}/api/trade-comments/${encodeURIComponent(agent)}?limit=200`);
                if (resp.ok) {
                    _commentCache[agent] = await resp.json();
                } else {
                    _commentCache[agent] = [];
                }
            } catch (e) {
                _commentCache[agent] = [];
            }
        });
        await Promise.all(fetches);
        _commentCacheTime = now;
    }

    // Update button states from cache — match by (agent, date) only
    buttons.forEach(btn => {
        const agent = btn.dataset.agent;
        const date = btn.dataset.date;
        const agentComments = _commentCache[agent] || [];
        const matches = agentComments.filter(c => c.trade_date === date);
        if (matches.length > 0) {
            btn.classList.add('has-comments');
            let badge = btn.querySelector('.comment-count-badge');
            if (!badge) {
                badge = document.createElement('span');
                badge.className = 'comment-count-badge';
                btn.appendChild(badge);
            }
            badge.textContent = matches.length;
        } else {
            btn.classList.remove('has-comments');
            const badge = btn.querySelector('.comment-count-badge');
            if (badge) badge.remove();
        }
    });
}

// Invalidate cache so next refresh fetches fresh data
function invalidateCommentCache() {
    _commentCacheTime = 0;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Build modal DOM safely using DOM APIs
function buildCommentModal(agentName, tradeDate) {
    const overlay = document.createElement('div');
    overlay.className = 'comment-modal-overlay';

    const modal = document.createElement('div');
    modal.className = 'comment-modal';

    // Header
    const header = document.createElement('div');
    header.className = 'comment-modal-header';
    const headerLeft = document.createElement('div');
    const h4 = document.createElement('h4');
    h4.textContent = 'Trade Comments';
    const info = document.createElement('div');
    info.className = 'comment-modal-trade-info';
    info.textContent = `${agentName} — ${tradeDate}`;
    headerLeft.appendChild(h4);
    headerLeft.appendChild(info);
    const closeBtn = document.createElement('button');
    closeBtn.className = 'comment-modal-close';
    closeBtn.textContent = '\u00d7';
    header.appendChild(headerLeft);
    header.appendChild(closeBtn);

    // Body
    const body = document.createElement('div');
    body.className = 'comment-modal-body';
    const emptyMsg = document.createElement('div');
    emptyMsg.className = 'comment-list-empty';
    emptyMsg.textContent = 'Loading...';
    body.appendChild(emptyMsg);

    // Footer
    const footer = document.createElement('div');
    footer.className = 'comment-modal-footer';
    const inputArea = document.createElement('div');
    inputArea.className = 'comment-input-area';
    const textarea = document.createElement('textarea');
    textarea.className = 'comment-textarea';
    textarea.placeholder = 'Write your comment...';
    textarea.rows = 3;
    const btnRow = document.createElement('div');
    btnRow.className = 'comment-btn-row';
    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'comment-btn comment-btn-cancel';
    cancelBtn.textContent = 'Cancel';
    const saveBtn = document.createElement('button');
    saveBtn.className = 'comment-btn comment-btn-save';
    saveBtn.textContent = 'Save';
    btnRow.appendChild(cancelBtn);
    btnRow.appendChild(saveBtn);
    inputArea.appendChild(textarea);
    inputArea.appendChild(btnRow);
    footer.appendChild(inputArea);

    modal.appendChild(header);
    modal.appendChild(body);
    modal.appendChild(footer);
    overlay.appendChild(modal);

    return { overlay, body, textarea, saveBtn, cancelBtn, closeBtn };
}

// Open comment modal — keyed by (agent, date)
async function openCommentModal(agentName, tradeDate) {
    const existing = document.querySelector('.comment-modal-overlay');
    if (existing) existing.remove();

    const { overlay, body, textarea, saveBtn, cancelBtn, closeBtn } = buildCommentModal(agentName, tradeDate);
    document.body.appendChild(overlay);

    // Close handlers
    closeBtn.addEventListener('click', () => overlay.remove());
    cancelBtn.addEventListener('click', () => overlay.remove());
    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) overlay.remove();
    });

    // Load existing comments
    await renderCommentList(body, agentName, tradeDate);

    // Save handler
    saveBtn.addEventListener('click', async () => {
        const text = textarea.value.trim();
        if (!text) return;
        saveBtn.disabled = true;
        try {
            const apiBase = getCommentApiBase();
            const resp = await fetch(`${apiBase}/api/trade-comments/`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    agent_name: agentName,
                    market: dataLoader.getMarket() === 'cn_hour' ? 'cn' : dataLoader.getMarket(),
                    trade_date: tradeDate,
                    ts_code: '',
                    action: '',
                    comment_text: text,
                }),
            });
            if (resp.ok) {
                textarea.value = '';
                await renderCommentList(body, agentName, tradeDate);
                invalidateCommentCache();
                refreshCommentButtons();
            }
        } finally {
            saveBtn.disabled = false;
        }
    });
}

function createCommentItemElement(c, container, agentName, tradeDate) {
    const item = document.createElement('div');
    item.className = 'comment-item';
    item.dataset.commentId = c.id;

    const itemHeader = document.createElement('div');
    itemHeader.className = 'comment-item-header';
    const timeSpan = document.createElement('span');
    timeSpan.className = 'comment-item-time';
    timeSpan.textContent = new Date(c.created_at).toLocaleString();
    const actionsDiv = document.createElement('div');
    actionsDiv.className = 'comment-item-actions';
    const editBtn = document.createElement('button');
    editBtn.className = 'edit-btn';
    editBtn.textContent = 'Edit';
    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'delete-btn';
    deleteBtn.textContent = 'Delete';
    actionsDiv.appendChild(editBtn);
    actionsDiv.appendChild(deleteBtn);
    itemHeader.appendChild(timeSpan);
    itemHeader.appendChild(actionsDiv);

    const textEl = document.createElement('div');
    textEl.className = 'comment-item-text';
    textEl.textContent = c.comment_text;

    item.appendChild(itemHeader);
    item.appendChild(textEl);

    // Edit handler
    editBtn.addEventListener('click', () => {
        const currentText = textEl.textContent;
        textEl.textContent = '';
        const editArea = document.createElement('textarea');
        editArea.className = 'comment-textarea comment-edit-textarea';
        editArea.value = currentText;
        const editBtnRow = document.createElement('div');
        editBtnRow.className = 'comment-btn-row';
        editBtnRow.style.marginTop = '0.5rem';
        const editCancel = document.createElement('button');
        editCancel.className = 'comment-btn comment-btn-cancel';
        editCancel.textContent = 'Cancel';
        const editSave = document.createElement('button');
        editSave.className = 'comment-btn comment-btn-save';
        editSave.textContent = 'Save';
        editBtnRow.appendChild(editCancel);
        editBtnRow.appendChild(editSave);
        textEl.appendChild(editArea);
        textEl.appendChild(editBtnRow);

        editCancel.addEventListener('click', () => {
            textEl.textContent = currentText;
        });
        editSave.addEventListener('click', async () => {
            const newText = editArea.value.trim();
            if (!newText) return;
            const apiBase = getCommentApiBase();
            const resp = await fetch(`${apiBase}/api/trade-comments/${c.id}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ comment_text: newText }),
            });
            if (resp.ok) {
                await renderCommentList(container, agentName, tradeDate);
                invalidateCommentCache();
                refreshCommentButtons();
            }
        });
    });

    // Delete handler
    deleteBtn.addEventListener('click', async () => {
        if (!confirm('Delete this comment?')) return;
        const apiBase = getCommentApiBase();
        const resp = await fetch(`${apiBase}/api/trade-comments/${c.id}`, { method: 'DELETE' });
        if (resp.ok) {
            await renderCommentList(container, agentName, tradeDate);
            invalidateCommentCache();
            refreshCommentButtons();
        }
    });

    return item;
}

async function renderCommentList(container, agentName, tradeDate) {
    const apiBase = getCommentApiBase();
    const params = new URLSearchParams({ trade_date: tradeDate });
    try {
        const resp = await fetch(`${apiBase}/api/trade-comments/${encodeURIComponent(agentName)}?${params}&limit=200`);
        if (!resp.ok) {
            container.textContent = '';
            const msg = document.createElement('div');
            msg.className = 'comment-list-empty';
            msg.textContent = 'Failed to load comments';
            container.appendChild(msg);
            return;
        }
        const comments = await resp.json();
        container.textContent = '';
        if (comments.length === 0) {
            const msg = document.createElement('div');
            msg.className = 'comment-list-empty';
            msg.textContent = 'No comments yet';
            container.appendChild(msg);
            return;
        }
        comments.forEach(c => {
            container.appendChild(createCommentItemElement(c, container, agentName, tradeDate));
        });
    } catch (e) {
        container.textContent = '';
        const msg = document.createElement('div');
        msg.className = 'comment-list-empty';
        msg.textContent = 'Failed to load comments';
        container.appendChild(msg);
    }
}

// Delegate click on comment buttons
document.addEventListener('click', (e) => {
    const btn = e.target.closest('.action-comment-btn');
    if (!btn) return;
    e.stopPropagation();
    openCommentModal(btn.dataset.agent, btn.dataset.date);
});

// Initialize on page load
window.addEventListener('DOMContentLoaded', init);