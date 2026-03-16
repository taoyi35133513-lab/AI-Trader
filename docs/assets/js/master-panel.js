/**
 * 投资大师点评面板
 *
 * 右下角悬浮按钮 + 滑出面板，支持多大师 / 模型 / Agent 选择。
 * SSE 流式接收 LLM 点评。
 */
(function () {
    'use strict';

    const API_BASE = (window.configLoader && configLoader.apiBaseUrl) ||
        new URLSearchParams(location.search).get('api') ||
        (location.port === '8888' ? '' : location.protocol + '//' + location.hostname + ':8888');

    /* ── DOM 构建 ───────────────────────────────────── */

    function createPanel() {
        // FAB button
        const fab = document.createElement('button');
        fab.className = 'master-fab';
        fab.title = '投资大师点评';
        fab.textContent = '\uD83D\uDCA1';
        document.body.appendChild(fab);

        // Overlay
        const overlay = document.createElement('div');
        overlay.className = 'master-overlay';
        document.body.appendChild(overlay);

        // Panel
        const panel = document.createElement('div');
        panel.className = 'master-panel';

        // Header
        const header = document.createElement('div');
        header.className = 'master-panel-header';

        const avatarDiv = document.createElement('div');
        avatarDiv.className = 'master-panel-avatar';
        const avatarImg = document.createElement('img');
        avatarImg.src = '';
        avatarImg.alt = '';
        avatarDiv.appendChild(avatarImg);

        const titleDiv = document.createElement('div');
        titleDiv.className = 'master-panel-title';
        const nameSpan = document.createElement('span');
        nameSpan.className = 'master-panel-name';
        const descSpan = document.createElement('span');
        descSpan.className = 'master-panel-desc';
        titleDiv.appendChild(nameSpan);
        titleDiv.appendChild(descSpan);

        const closeBtn = document.createElement('button');
        closeBtn.className = 'master-panel-close';
        closeBtn.textContent = '\u00d7';

        header.appendChild(avatarDiv);
        header.appendChild(titleDiv);
        header.appendChild(closeBtn);

        // Pill selector area
        const pillArea = document.createElement('div');
        pillArea.className = 'master-pill-section';

        // Row: 大师
        pillArea.appendChild(createPillRow('master-pill-masters', '大师'));
        // Row: 模型
        pillArea.appendChild(createPillRow('master-pill-models', '模型'));
        // Row: Agent
        pillArea.appendChild(createPillRow('master-pill-agents', 'Agent'));

        // Body
        const body = document.createElement('div');
        body.className = 'master-panel-body';
        const content = document.createElement('div');
        content.className = 'master-panel-content';
        body.appendChild(content);

        // Footer
        const footer = document.createElement('div');
        footer.className = 'master-panel-footer';
        const footerInfo = document.createElement('span');
        footerInfo.className = 'master-panel-agent';
        const refreshBtn = document.createElement('button');
        refreshBtn.className = 'master-refresh-btn';
        refreshBtn.title = '重新生成';
        refreshBtn.textContent = '\u21BB';
        footer.appendChild(footerInfo);
        footer.appendChild(refreshBtn);

        panel.appendChild(header);
        panel.appendChild(pillArea);
        panel.appendChild(body);
        panel.appendChild(footer);
        document.body.appendChild(panel);

        return { fab, overlay, panel };
    }

    function createPillRow(groupId, label) {
        const row = document.createElement('div');
        row.className = 'master-pill-row';

        const lbl = document.createElement('span');
        lbl.className = 'master-pill-label';
        lbl.textContent = label;

        const group = document.createElement('div');
        group.className = 'master-pill-group';
        group.id = groupId;

        row.appendChild(lbl);
        row.appendChild(group);
        return row;
    }

    /* ── State ──────────────────────────────────────── */

    let els = null;
    let isOpen = false;
    let masters = [];
    let models = [];
    let agents = [];
    let currentMaster = null;
    let currentModel = null;
    let currentAgent = null;
    let abortController = null;
    let debounceTimer = null;

    /* ── Helpers ────────────────────────────────────── */

    function detectAgent() {
        const agentSelect = document.getElementById('agentSelect');
        if (agentSelect && agentSelect.value) return agentSelect.value;
        const lb = document.querySelector('.leaderboard-name');
        if (lb) return lb.textContent.trim();
        return 'deepseek-chat-v3.2';
    }

    function detectMarket() {
        if (window.dataLoader && typeof dataLoader.getMarket === 'function') return dataLoader.getMarket();
        return 'cn';
    }

    function renderMarkdownToElement(text, container) {
        while (container.firstChild) container.removeChild(container.firstChild);
        const lines = text.split('\n');
        let currentList = null;

        for (const line of lines) {
            const h3Match = line.match(/^### (.+)$/);
            const h2Match = line.match(/^## (.+)$/);
            const h1Match = line.match(/^# (.+)$/);
            const listMatch = line.match(/^[-*] (.+)$/);

            if (currentList && !listMatch) {
                container.appendChild(currentList);
                currentList = null;
            }

            if (h1Match) {
                const el = document.createElement('h2');
                appendInlineMarkdown(el, h1Match[1]);
                container.appendChild(el);
            } else if (h2Match) {
                const el = document.createElement('h3');
                appendInlineMarkdown(el, h2Match[1]);
                container.appendChild(el);
            } else if (h3Match) {
                const el = document.createElement('h4');
                appendInlineMarkdown(el, h3Match[1]);
                container.appendChild(el);
            } else if (listMatch) {
                if (!currentList) currentList = document.createElement('ul');
                const li = document.createElement('li');
                appendInlineMarkdown(li, listMatch[1]);
                currentList.appendChild(li);
            } else if (line.trim() === '') {
                container.appendChild(document.createElement('br'));
            } else {
                const p = document.createElement('p');
                appendInlineMarkdown(p, line);
                container.appendChild(p);
            }
        }
        if (currentList) container.appendChild(currentList);
    }

    function appendInlineMarkdown(parent, text) {
        const parts = text.split(/(\*\*[^*]+\*\*|\*[^*]+\*)/g);
        for (const part of parts) {
            if (part.startsWith('**') && part.endsWith('**')) {
                const strong = document.createElement('strong');
                strong.textContent = part.slice(2, -2);
                parent.appendChild(strong);
            } else if (part.startsWith('*') && part.endsWith('*')) {
                const em = document.createElement('em');
                em.textContent = part.slice(1, -1);
                parent.appendChild(em);
            } else {
                parent.appendChild(document.createTextNode(part));
            }
        }
    }

    /* ── Pill rendering ────────────────────────────── */

    function renderPills(groupId, items, selectedValue, onSelect) {
        const group = document.getElementById(groupId);
        if (!group) return;
        while (group.firstChild) group.removeChild(group.firstChild);

        for (const item of items) {
            const btn = document.createElement('button');
            btn.className = 'master-pill';
            btn.textContent = item.label;
            btn.dataset.value = item.value;
            if (item.value === selectedValue) btn.classList.add('selected');
            if (item.color) btn.style.setProperty('--pill-color', item.color);
            btn.addEventListener('click', function () {
                onSelect(item.value);
                // Update selected state
                const siblings = group.querySelectorAll('.master-pill');
                siblings.forEach(function (s) { s.classList.remove('selected'); });
                btn.classList.add('selected');
                debouncedGenerate();
            });
            group.appendChild(btn);
        }
    }

    function debouncedGenerate() {
        if (debounceTimer) clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function () {
            if (isOpen) generate();
        }, 300);
    }

    /* ── Panel open/close ──────────────────────────── */

    function openPanel() {
        if (isOpen) return;
        isOpen = true;
        els.panel.classList.add('open');
        els.overlay.classList.add('open');
        els.fab.classList.add('hidden');

        if (!currentMaster && masters.length > 0) {
            selectMaster(masters[0].id);
        }

        // Reload agents for current market (market may have changed since init)
        var prevAgent = currentAgent;
        currentAgent = null;
        loadAgents().then(function () {
            // Restore previous selection if still valid
            if (prevAgent) {
                var found = agents.find(function (a) { return a.name === prevAgent; });
                if (found) {
                    currentAgent = found.name;
                    renderAgentPills();
                }
            }
            generate();
        });
    }

    function closePanel() {
        if (!isOpen) return;
        isOpen = false;
        els.panel.classList.remove('open');
        els.overlay.classList.remove('open');
        els.fab.classList.remove('hidden');
        if (abortController) {
            abortController.abort();
            abortController = null;
        }
    }

    /* ── Master selection ──────────────────────────── */

    async function loadMasters() {
        try {
            const resp = await fetch(API_BASE + '/api/master-commentary/masters');
            const data = await resp.json();
            masters = data.masters || [];
            renderMasterPills();
            if (masters.length > 0) selectMaster(masters[0].id);
        } catch (e) {
            console.warn('Failed to load masters:', e);
        }
    }

    function renderMasterPills() {
        var items = masters.map(function (m) {
            return { value: m.id, label: m.name };
        });
        renderPills('master-pill-masters', items, currentMaster ? currentMaster.id : '', function (val) {
            selectMaster(val);
        });
    }

    function selectMaster(id) {
        var m = masters.find(function (x) { return x.id === id; });
        if (!m) return;
        currentMaster = m;

        var avatar = els.panel.querySelector('.master-panel-avatar img');
        avatar.src = m.avatar;
        avatar.alt = m.name;

        els.panel.querySelector('.master-panel-name').textContent = m.name;
        els.panel.querySelector('.master-panel-desc').textContent = m.description;
    }

    /* ── Model loading ─────────────────────────────── */

    async function loadModels() {
        try {
            var resp = await fetch(API_BASE + '/api/config/models');
            var data = await resp.json();
            models = (data.models || []).filter(function (m) { return m.enabled; });
            renderModelPills();
            if (!currentModel && models.length > 0) currentModel = models[0].name;
        } catch (e) {
            console.warn('Failed to load models:', e);
        }
    }

    function renderModelPills() {
        var items = models.map(function (m) {
            return { value: m.name, label: m.display_name || m.name, color: m.color };
        });
        renderPills('master-pill-models', items, currentModel || '', function (val) {
            currentModel = val;
        });
    }

    /* ── Agent loading ─────────────────────────────── */

    async function loadAgents() {
        var market = detectMarket();
        try {
            // Always fetch from API to ensure data is available
            var resp = await fetch(API_BASE + '/api/config/full');
            var data = await resp.json();
            var mc = data.markets && data.markets[market];
            var agentList = mc && mc.agents ? mc.agents : [];
            // Only show enabled agents
            agents = agentList.filter(function (a) { return a.enabled; }).map(function (a) {
                return { name: a.folder || a.name, display_name: a.display_name || a.folder };
            });

            // Still empty? Use detected agent
            if (agents.length === 0) {
                var det = detectAgent();
                agents = [{ name: det, display_name: det }];
            }
            renderAgentPills();
            if (!currentAgent && agents.length > 0) {
                var detected = detectAgent();
                var found = agents.find(function (a) { return a.name === detected; });
                currentAgent = found ? found.name : agents[0].name;
                renderAgentPills();
            }
        } catch (e) {
            console.warn('Failed to load agents:', e);
            var det = detectAgent();
            agents = [{ name: det, display_name: det }];
            currentAgent = det;
            renderAgentPills();
        }
    }

    function renderAgentPills() {
        var items = agents.map(function (a) {
            var label = a.display_name || a.name;
            // Truncate long names
            if (label.length > 16) label = label.substring(0, 14) + '..';
            return { value: a.name, label: label };
        });
        renderPills('master-pill-agents', items, currentAgent || '', function (val) {
            currentAgent = val;
        });
    }

    /* ── Footer update ─────────────────────────────── */

    function updateFooter() {
        var footerEl = els.panel.querySelector('.master-panel-agent');
        var parts = [];
        if (currentAgent) parts.push('Agent: ' + currentAgent);
        if (currentModel) parts.push('\u6A21\u578B: ' + currentModel);
        footerEl.textContent = parts.join(' | ');
    }

    /* ── SSE streaming ─────────────────────────────── */

    async function generate() {
        if (!currentMaster) return;

        var contentEl = els.panel.querySelector('.master-panel-content');
        var agentName = currentAgent || detectAgent();
        var market = detectMarket();

        updateFooter();

        // Loading state
        while (contentEl.firstChild) contentEl.removeChild(contentEl.firstChild);
        var loadingDiv = document.createElement('div');
        loadingDiv.className = 'master-loading';
        var dotsDiv = document.createElement('div');
        dotsDiv.className = 'master-loading-dots';
        for (var i = 0; i < 3; i++) dotsDiv.appendChild(document.createElement('span'));
        loadingDiv.appendChild(dotsDiv);
        var loadingText = document.createElement('span');
        loadingText.textContent = '\u6B63\u5728\u5206\u6790\u4EA4\u6613\u6570\u636E...';
        loadingDiv.appendChild(loadingText);
        contentEl.appendChild(loadingDiv);

        // Abort previous
        if (abortController) abortController.abort();
        abortController = new AbortController();

        var postBody = {
            agent_name: agentName,
            market: market,
            master_id: currentMaster.id,
        };
        if (currentModel) postBody.model_name = currentModel;

        try {
            var resp = await fetch(API_BASE + '/api/master-commentary/stream', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(postBody),
                signal: abortController.signal,
            });

            var reader = resp.body.getReader();
            var decoder = new TextDecoder();
            var fullText = '';
            while (contentEl.firstChild) contentEl.removeChild(contentEl.firstChild);

            while (true) {
                var result = await reader.read();
                if (result.done) break;

                var chunk = decoder.decode(result.value, { stream: true });
                var lines = chunk.split('\n');

                for (var j = 0; j < lines.length; j++) {
                    var line = lines[j];
                    if (!line.startsWith('data: ')) continue;
                    var payload = line.slice(6).trim();
                    if (payload === '[DONE]') continue;

                    try {
                        var data = JSON.parse(payload);
                        if (data.error) {
                            while (contentEl.firstChild) contentEl.removeChild(contentEl.firstChild);
                            var errDiv = document.createElement('div');
                            errDiv.className = 'master-error';
                            errDiv.textContent = data.error;
                            contentEl.appendChild(errDiv);
                            return;
                        }
                        if (data.content) {
                            fullText += data.content;
                            renderMarkdownToElement(fullText, contentEl);
                            var body = els.panel.querySelector('.master-panel-body');
                            body.scrollTop = body.scrollHeight;
                        }
                    } catch (_) { /* skip malformed */ }
                }
            }
        } catch (e) {
            if (e.name === 'AbortError') return;
            while (contentEl.firstChild) contentEl.removeChild(contentEl.firstChild);
            var errDiv2 = document.createElement('div');
            errDiv2.className = 'master-error';
            errDiv2.textContent = '\u8FDE\u63A5\u5931\u8D25: ' + e.message;
            contentEl.appendChild(errDiv2);
        }
    }

    /* ── Init ──────────────────────────────────────── */

    function init() {
        els = createPanel();

        // Events
        els.fab.addEventListener('click', openPanel);
        els.overlay.addEventListener('click', closePanel);
        els.panel.querySelector('.master-panel-close').addEventListener('click', closePanel);
        els.panel.querySelector('.master-refresh-btn').addEventListener('click', generate);

        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && isOpen) closePanel();
        });

        // Listen for agent changes on portfolio page
        var agentSelect = document.getElementById('agentSelect');
        if (agentSelect) {
            agentSelect.addEventListener('change', function () {
                currentAgent = agentSelect.value;
                renderAgentPills();
                if (isOpen) debouncedGenerate();
            });
        }

        // Load data in parallel
        loadMasters();
        loadModels();
        loadAgents();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
