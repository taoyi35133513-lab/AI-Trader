/**
 * 交易记忆面板
 *
 * 右下角悬浮按钮 + 滑出面板，展示 Agent 交易记忆（策略/经验/复盘）。
 * 支持归档、压缩记忆等操作。
 */
(function () {
    'use strict';

    var API_BASE = (window.configLoader && configLoader.apiBaseUrl) ||
        new URLSearchParams(location.search).get('api') ||
        (location.port === '8888' ? '' : location.protocol + '//' + location.hostname + ':8888');

    /* ── DOM 构建 ───────────────────────────────────── */

    function createPanel() {
        // FAB button
        var fab = document.createElement('button');
        fab.className = 'memory-fab';
        fab.title = '交易记忆';
        fab.textContent = '\uD83E\uDDE0';
        document.body.appendChild(fab);

        // Overlay
        var overlay = document.createElement('div');
        overlay.className = 'memory-overlay';
        document.body.appendChild(overlay);

        // Panel
        var panel = document.createElement('div');
        panel.className = 'memory-panel';

        // Header
        var header = document.createElement('div');
        header.className = 'memory-panel-header';

        var headerLeft = document.createElement('div');
        headerLeft.className = 'memory-header-left';
        var titleIcon = document.createElement('span');
        titleIcon.className = 'memory-header-icon';
        titleIcon.textContent = '\uD83E\uDDE0';
        var titleText = document.createElement('span');
        titleText.className = 'memory-header-title';
        titleText.textContent = '\u4EA4\u6613\u8BB0\u5FC6';
        headerLeft.appendChild(titleIcon);
        headerLeft.appendChild(titleText);

        var agentSelect = document.createElement('select');
        agentSelect.className = 'memory-agent-select';
        agentSelect.id = 'memoryAgentSelect';

        var closeBtn = document.createElement('button');
        closeBtn.className = 'memory-panel-close';
        closeBtn.textContent = '\u00d7';

        header.appendChild(headerLeft);
        header.appendChild(agentSelect);
        header.appendChild(closeBtn);

        // Stats bar
        var statsBar = document.createElement('div');
        statsBar.className = 'memory-stats-bar';
        statsBar.id = 'memoryStatsBar';

        // Body (scrollable)
        var body = document.createElement('div');
        body.className = 'memory-panel-body';
        body.id = 'memoryPanelBody';

        // Footer
        var footer = document.createElement('div');
        footer.className = 'memory-panel-footer';

        var consolidateBtn = document.createElement('button');
        consolidateBtn.className = 'memory-consolidate-btn';
        consolidateBtn.id = 'memoryConsolidateBtn';
        var consolidateIcon = document.createElement('span');
        consolidateIcon.textContent = '\uD83D\uDDDC\uFE0F';
        var consolidateText = document.createTextNode(' \u538B\u7F29\u8BB0\u5FC6');
        consolidateBtn.appendChild(consolidateIcon);
        consolidateBtn.appendChild(consolidateText);

        var totalCount = document.createElement('span');
        totalCount.className = 'memory-total-count';
        totalCount.id = 'memoryTotalCount';
        totalCount.textContent = '\u5171 0 \u6761';

        footer.appendChild(consolidateBtn);
        footer.appendChild(totalCount);

        panel.appendChild(header);
        panel.appendChild(statsBar);
        panel.appendChild(body);
        panel.appendChild(footer);
        document.body.appendChild(panel);

        return { fab: fab, overlay: overlay, panel: panel };
    }

    /* ── State ──────────────────────────────────────── */

    var els = null;
    var isOpen = false;
    var agents = [];
    var currentAgent = null;

    /* ── Helpers ────────────────────────────────────── */

    function detectAgent() {
        var agentSelect = document.getElementById('agentSelect');
        if (agentSelect && agentSelect.value) return agentSelect.value;
        var lb = document.querySelector('.leaderboard-name');
        if (lb) return lb.textContent.trim();
        return 'deepseek-chat-v3.2';
    }

    function detectMarket() {
        if (window.dataLoader && typeof dataLoader.getMarket === 'function') return dataLoader.getMarket();
        return 'cn';
    }

    /* ── Agent loading ─────────────────────────────── */

    async function loadAgents() {
        var market = detectMarket();
        var select = document.getElementById('memoryAgentSelect');
        if (!select) return;

        try {
            var resp = await fetch(API_BASE + '/api/config/full');
            var data = await resp.json();
            var mc = data.markets && data.markets[market];
            var agentList = mc && mc.agents ? mc.agents : [];
            agents = agentList.filter(function (a) { return a.enabled; }).map(function (a) {
                return { name: a.folder || a.name, display_name: a.display_name || a.folder };
            });

            if (agents.length === 0) {
                var det = detectAgent();
                agents = [{ name: det, display_name: det }];
            }
        } catch (e) {
            console.warn('Memory panel: failed to load agents:', e);
            var det = detectAgent();
            agents = [{ name: det, display_name: det }];
        }

        // Populate select
        while (select.firstChild) select.removeChild(select.firstChild);
        for (var i = 0; i < agents.length; i++) {
            var opt = document.createElement('option');
            opt.value = agents[i].name;
            opt.textContent = agents[i].display_name;
            select.appendChild(opt);
        }

        // Set current agent
        if (!currentAgent) {
            var detected = detectAgent();
            var found = agents.find(function (a) { return a.name === detected; });
            currentAgent = found ? found.name : (agents[0] ? agents[0].name : null);
        }
        if (currentAgent) select.value = currentAgent;
    }

    /* ── Stats loading ─────────────────────────────── */

    async function loadStats() {
        var statsBar = document.getElementById('memoryStatsBar');
        if (!statsBar || !currentAgent) return;

        while (statsBar.firstChild) statsBar.removeChild(statsBar.firstChild);

        try {
            var resp = await fetch(API_BASE + '/api/memory/stats?agent_name=' +
                encodeURIComponent(currentAgent) + '&market=' + encodeURIComponent(detectMarket()));
            var data = await resp.json();
            var stats = data.stats || {};

            var levels = [
                { key: 'reflection', label: 'L1 \u590D\u76D8', cls: 'memory-badge-l1' },
                { key: 'lesson', label: 'L2 \u7ECF\u9A8C', cls: 'memory-badge-l2' },
                { key: 'strategy', label: 'L3 \u7B56\u7565', cls: 'memory-badge-l3' }
            ];

            var total = 0;
            for (var i = 0; i < levels.length; i++) {
                var lv = levels[i];
                var st = stats[lv.key] || { active: 0, archived: 0 };
                total += st.active;

                var badge = document.createElement('span');
                badge.className = 'memory-stats-badge ' + lv.cls;
                badge.textContent = lv.label + ': ' + st.active;
                if (st.archived > 0) {
                    var archivedSpan = document.createElement('span');
                    archivedSpan.className = 'memory-badge-archived';
                    archivedSpan.textContent = ' (+' + st.archived + ')';
                    badge.appendChild(archivedSpan);
                }
                statsBar.appendChild(badge);
            }

            var totalEl = document.getElementById('memoryTotalCount');
            if (totalEl) totalEl.textContent = '\u5171 ' + total + ' \u6761';
        } catch (e) {
            var errBadge = document.createElement('span');
            errBadge.className = 'memory-stats-badge';
            errBadge.textContent = '\u52A0\u8F7D\u5931\u8D25';
            statsBar.appendChild(errBadge);
        }
    }

    /* ── Memory loading ────────────────────────────── */

    async function loadMemories() {
        var body = document.getElementById('memoryPanelBody');
        if (!body || !currentAgent) return;

        while (body.firstChild) body.removeChild(body.firstChild);

        // Loading state
        var loadingDiv = document.createElement('div');
        loadingDiv.className = 'memory-loading';
        var dotsDiv = document.createElement('div');
        dotsDiv.className = 'memory-loading-dots';
        for (var i = 0; i < 3; i++) dotsDiv.appendChild(document.createElement('span'));
        loadingDiv.appendChild(dotsDiv);
        var loadingText = document.createElement('span');
        loadingText.textContent = '\u52A0\u8F7D\u8BB0\u5FC6\u4E2D...';
        loadingDiv.appendChild(loadingText);
        body.appendChild(loadingDiv);

        try {
            var resp = await fetch(API_BASE + '/api/memory/active?agent_name=' +
                encodeURIComponent(currentAgent) + '&market=' + encodeURIComponent(detectMarket()));
            var data = await resp.json();
            var memories = data.memories || {};

            while (body.firstChild) body.removeChild(body.firstChild);

            // L3: Strategy
            renderStrategySection(body, memories.strategy);

            // L2: Lessons
            renderLessonsSection(body, memories.lessons || []);

            // L1: Reflections
            renderReflectionsSection(body, memories.reflections || []);

        } catch (e) {
            while (body.firstChild) body.removeChild(body.firstChild);
            var errDiv = document.createElement('div');
            errDiv.className = 'memory-error';
            errDiv.textContent = '\u52A0\u8F7D\u5931\u8D25: ' + e.message;
            body.appendChild(errDiv);
        }
    }

    function renderStrategySection(container, strategy) {
        var section = document.createElement('div');
        section.className = 'memory-section memory-section-l3';

        var title = document.createElement('div');
        title.className = 'memory-section-title';
        var icon = document.createElement('span');
        icon.className = 'memory-section-icon';
        icon.textContent = '\uD83C\uDFAF';
        var text = document.createTextNode(' \u7B56\u7565\u5907\u5FD8 (L3)');
        title.appendChild(icon);
        title.appendChild(text);
        section.appendChild(title);

        if (strategy && strategy.content) {
            var card = createMemoryCard(strategy, 'l3');
            section.appendChild(card);
        } else {
            var empty = document.createElement('div');
            empty.className = 'memory-empty';
            empty.textContent = '\u6682\u65E0\u7B56\u7565\u5907\u5FD8';
            section.appendChild(empty);
        }

        container.appendChild(section);
    }

    function renderLessonsSection(container, lessons) {
        var section = document.createElement('div');
        section.className = 'memory-section memory-section-l2';

        var title = document.createElement('div');
        title.className = 'memory-section-title';
        var icon = document.createElement('span');
        icon.className = 'memory-section-icon';
        icon.textContent = '\uD83D\uDCA1';
        var text = document.createTextNode(' \u4EA4\u6613\u7ECF\u9A8C (L2)');
        title.appendChild(icon);
        title.appendChild(text);

        var countBadge = document.createElement('span');
        countBadge.className = 'memory-section-count';
        countBadge.textContent = lessons.length.toString();
        title.appendChild(countBadge);
        section.appendChild(title);

        if (lessons.length === 0) {
            var empty = document.createElement('div');
            empty.className = 'memory-empty';
            empty.textContent = '\u6682\u65E0\u4EA4\u6613\u7ECF\u9A8C';
            section.appendChild(empty);
        } else {
            for (var i = 0; i < lessons.length; i++) {
                var card = createMemoryCard(lessons[i], 'l2');
                section.appendChild(card);
            }
        }

        container.appendChild(section);
    }

    function renderReflectionsSection(container, reflections) {
        var section = document.createElement('div');
        section.className = 'memory-section memory-section-l1';

        var title = document.createElement('div');
        title.className = 'memory-section-title';
        var icon = document.createElement('span');
        icon.className = 'memory-section-icon';
        icon.textContent = '\uD83D\uDD0D';
        var text = document.createTextNode(' \u6700\u8FD1\u590D\u76D8 (L1)');
        title.appendChild(icon);
        title.appendChild(text);

        var countBadge = document.createElement('span');
        countBadge.className = 'memory-section-count';
        countBadge.textContent = reflections.length.toString();
        title.appendChild(countBadge);
        section.appendChild(title);

        if (reflections.length === 0) {
            var empty = document.createElement('div');
            empty.className = 'memory-empty';
            empty.textContent = '\u6682\u65E0\u590D\u76D8\u8BB0\u5F55';
            section.appendChild(empty);
        } else {
            for (var i = 0; i < reflections.length; i++) {
                var card = createMemoryCard(reflections[i], 'l1');
                section.appendChild(card);
            }
        }

        container.appendChild(section);
    }

    function createMemoryCard(memory, level) {
        var card = document.createElement('div');
        card.className = 'memory-card memory-card-' + level;
        if (memory.id) card.dataset.memoryId = memory.id;

        // Date tag (if available)
        if (memory.source_dates && memory.source_dates.length > 0) {
            var dateTag = document.createElement('div');
            dateTag.className = 'memory-card-date';
            var dates = memory.source_dates;
            if (Array.isArray(dates)) {
                dateTag.textContent = dates.slice(0, 3).join(', ');
                if (dates.length > 3) {
                    var moreSpan = document.createElement('span');
                    moreSpan.className = 'memory-date-more';
                    moreSpan.textContent = ' +' + (dates.length - 3);
                    dateTag.appendChild(moreSpan);
                }
            } else {
                dateTag.textContent = String(dates);
            }
            card.appendChild(dateTag);
        }

        // Content
        var content = document.createElement('div');
        content.className = 'memory-card-content';
        content.textContent = memory.content || '';
        card.appendChild(content);

        // Tags (if available)
        if (memory.tags && memory.tags.length > 0) {
            var tagsDiv = document.createElement('div');
            tagsDiv.className = 'memory-card-tags';
            for (var i = 0; i < memory.tags.length && i < 5; i++) {
                var tag = document.createElement('span');
                tag.className = 'memory-tag';
                tag.textContent = memory.tags[i];
                tagsDiv.appendChild(tag);
            }
            card.appendChild(tagsDiv);
        }

        // Actions (appear on hover)
        if (memory.id) {
            var actions = document.createElement('div');
            actions.className = 'memory-card-actions';

            var archiveBtn = document.createElement('button');
            archiveBtn.className = 'memory-action-btn memory-archive-btn';
            archiveBtn.title = '\u5F52\u6863';
            archiveBtn.textContent = '\u2193';
            archiveBtn.addEventListener('click', function (e) {
                e.stopPropagation();
                archiveMemory(memory.id, card);
            });
            actions.appendChild(archiveBtn);

            card.appendChild(actions);
        }

        return card;
    }

    /* ── Actions ───────────────────────────────────── */

    async function archiveMemory(memoryId, cardEl) {
        try {
            var resp = await fetch(API_BASE + '/api/memory/archive/' + encodeURIComponent(memoryId), {
                method: 'POST'
            });
            if (resp.ok) {
                // Animate removal
                cardEl.style.opacity = '0';
                cardEl.style.transform = 'translateX(20px)';
                setTimeout(function () {
                    if (cardEl.parentNode) cardEl.parentNode.removeChild(cardEl);
                    loadStats();
                }, 300);
            } else {
                console.warn('Archive failed:', resp.status);
            }
        } catch (e) {
            console.warn('Archive error:', e);
        }
    }

    async function consolidateMemories() {
        var btn = document.getElementById('memoryConsolidateBtn');
        if (btn) {
            btn.disabled = true;
            btn.style.opacity = '0.5';
        }

        try {
            var resp = await fetch(API_BASE + '/api/memory/consolidate?agent_name=' +
                encodeURIComponent(currentAgent) + '&market=' + encodeURIComponent(detectMarket()), {
                method: 'POST'
            });

            if (resp.ok) {
                // Reload everything
                await loadStats();
                await loadMemories();
            } else {
                console.warn('Consolidate failed:', resp.status);
            }
        } catch (e) {
            console.warn('Consolidate error:', e);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.style.opacity = '';
            }
        }
    }

    /* ── Panel open/close ──────────────────────────── */

    function openPanel() {
        if (isOpen) return;
        isOpen = true;
        els.panel.classList.add('open');
        els.overlay.classList.add('open');
        els.fab.classList.add('hidden');

        // Reload agents (market may have changed)
        var prevAgent = currentAgent;
        currentAgent = null;
        loadAgents().then(function () {
            if (prevAgent) {
                var found = agents.find(function (a) { return a.name === prevAgent; });
                if (found) {
                    currentAgent = found.name;
                    var select = document.getElementById('memoryAgentSelect');
                    if (select) select.value = currentAgent;
                }
            }
            if (!currentAgent && agents.length > 0) {
                currentAgent = agents[0].name;
            }
            loadStats();
            loadMemories();
        });
    }

    function closePanel() {
        if (!isOpen) return;
        isOpen = false;
        els.panel.classList.remove('open');
        els.overlay.classList.remove('open');
        els.fab.classList.remove('hidden');
    }

    /* ── Init ──────────────────────────────────────── */

    function init() {
        els = createPanel();

        // Events
        els.fab.addEventListener('click', openPanel);
        els.overlay.addEventListener('click', closePanel);
        els.panel.querySelector('.memory-panel-close').addEventListener('click', closePanel);

        var agentSelect = document.getElementById('memoryAgentSelect');
        if (agentSelect) {
            agentSelect.addEventListener('change', function () {
                currentAgent = agentSelect.value;
                loadStats();
                loadMemories();
            });
        }

        var consolidateBtn = document.getElementById('memoryConsolidateBtn');
        if (consolidateBtn) {
            consolidateBtn.addEventListener('click', consolidateMemories);
        }

        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && isOpen) closePanel();
        });

        // Listen for agent changes on portfolio page
        var pageAgentSelect = document.getElementById('agentSelect');
        if (pageAgentSelect) {
            pageAgentSelect.addEventListener('change', function () {
                currentAgent = pageAgentSelect.value;
                var memSelect = document.getElementById('memoryAgentSelect');
                if (memSelect) memSelect.value = currentAgent;
                if (isOpen) {
                    loadStats();
                    loadMemories();
                }
            });
        }

        // Preload agents
        loadAgents();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
