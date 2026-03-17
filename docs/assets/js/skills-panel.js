/**
 * Skills 技能管理面板
 *
 * FAB 按钮(⚡) → 滑出面板 → 按类别展示技能卡片 → toggle 开关即时生效
 */
(function () {
    'use strict';

    var API_BASE = (window.configLoader && configLoader.apiBaseUrl) ||
        new URLSearchParams(location.search).get('api') ||
        (location.port === '8888' ? '' : location.protocol + '//' + location.hostname + ':8888');

    var panelOpen = false;
    var allSkills = {};
    var agentSkills = {};
    var currentAgent = null;
    var agents = [];

    var CATEGORY_LABELS = {
        strategy: { label: '交易策略', icon: '\uD83C\uDFAF' },
        analysis: { label: '分析工具', icon: '\uD83D\uDD2C' },
        risk: { label: '风控管理', icon: '\uD83D\uDEE1\uFE0F' },
    };

    function detectMarket() {
        if (window.dataLoader && typeof dataLoader.getMarket === 'function') {
            return dataLoader.getMarket();
        }
        return 'cn';
    }

    function createPanel() {
        // FAB
        var fab = document.createElement('button');
        fab.className = 'skills-fab';
        var fabIcon = document.createElement('span');
        fabIcon.style.fontSize = '1.4rem';
        fabIcon.textContent = '\u26A1';
        fab.appendChild(fabIcon);
        fab.title = '技能管理';
        fab.onclick = function () { panelOpen ? closePanel() : openPanel(); };
        document.body.appendChild(fab);

        // Panel
        var panel = document.createElement('div');
        panel.className = 'skills-panel';
        panel.id = 'skillsPanel';

        // Header
        var header = document.createElement('div');
        header.className = 'skills-panel-header';
        var title = document.createElement('span');
        title.className = 'skills-panel-title';
        title.textContent = '\u26A1 技能装备';
        var closeBtn = document.createElement('button');
        closeBtn.className = 'skills-panel-close';
        closeBtn.textContent = '\u00D7';
        closeBtn.onclick = closePanel;
        header.appendChild(title);
        header.appendChild(closeBtn);
        panel.appendChild(header);

        // Agent bar
        var agentBar = document.createElement('div');
        agentBar.className = 'skills-panel-agent-bar';
        agentBar.id = 'skillsAgentBar';
        panel.appendChild(agentBar);

        // Body
        var body = document.createElement('div');
        body.className = 'skills-panel-body';
        body.id = 'skillsPanelBody';
        panel.appendChild(body);

        document.body.appendChild(panel);
    }

    function openPanel() {
        panelOpen = true;
        document.getElementById('skillsPanel').classList.add('open');
        document.querySelector('.skills-fab').classList.add('active');
        loadAgents();
        loadSkills();
    }

    function closePanel() {
        panelOpen = false;
        document.getElementById('skillsPanel').classList.remove('open');
        document.querySelector('.skills-fab').classList.remove('active');
    }

    async function loadAgents() {
        try {
            var resp = await fetch(API_BASE + '/api/config/full');
            var data = await resp.json();
            var market = detectMarket();
            var marketKey = market === 'cn_hour' ? 'cn_hour' : 'cn';
            var marketData = data.markets && data.markets[marketKey];
            agents = [];
            if (marketData && marketData.agents) {
                marketData.agents.forEach(function (a) {
                    if (a.enabled !== false) {
                        agents.push({ name: a.folder || a.name, display: a.display_name || a.folder || a.name });
                    }
                });
            }
        } catch (e) {
            agents = [];
        }

        var bar = document.getElementById('skillsAgentBar');
        if (!bar) return;
        while (bar.firstChild) bar.removeChild(bar.firstChild);

        agents.forEach(function (a) {
            var pill = document.createElement('button');
            pill.className = 'skills-agent-pill' + (currentAgent === a.name ? ' selected' : '');
            pill.textContent = a.display;
            pill.onclick = function () {
                currentAgent = a.name;
                bar.querySelectorAll('.skills-agent-pill').forEach(function (p) { p.classList.remove('selected'); });
                pill.classList.add('selected');
                loadAgentSkills();
            };
            bar.appendChild(pill);
        });

        if (!currentAgent && agents.length > 0) {
            currentAgent = agents[0].name;
            if (bar.firstChild) bar.firstChild.classList.add('selected');
        }
        loadAgentSkills();
    }

    async function loadSkills() {
        try {
            var resp = await fetch(API_BASE + '/api/skills');
            var data = await resp.json();
            allSkills = data.skills || {};
        } catch (e) {
            allSkills = {};
        }
        renderSkills();
    }

    async function loadAgentSkills() {
        if (!currentAgent) return;
        try {
            var market = detectMarket();
            var resp = await fetch(API_BASE + '/api/skills/agent/' + encodeURIComponent(currentAgent) + '?market=' + market);
            var data = await resp.json();
            agentSkills[currentAgent] = data.skill_ids || [];
        } catch (e) {
            agentSkills[currentAgent] = [];
        }
        renderSkills();
    }

    function renderSkills() {
        var body = document.getElementById('skillsPanelBody');
        if (!body) return;
        while (body.firstChild) body.removeChild(body.firstChild);

        var activeIds = agentSkills[currentAgent] || [];
        var categories = ['strategy', 'analysis', 'risk'];

        categories.forEach(function (cat) {
            var skills = allSkills[cat];
            if (!skills || skills.length === 0) return;
            var catInfo = CATEGORY_LABELS[cat] || { label: cat, icon: '' };

            var section = document.createElement('div');
            section.className = 'skills-section';

            var sectionTitle = document.createElement('div');
            sectionTitle.className = 'skills-section-title';
            sectionTitle.textContent = catInfo.icon + ' ' + catInfo.label;
            section.appendChild(sectionTitle);

            var grid = document.createElement('div');
            grid.className = 'skills-grid';

            skills.forEach(function (skill) {
                var isActive = activeIds.indexOf(skill.id) !== -1;
                var card = document.createElement('div');
                card.className = 'skill-card' + (isActive ? ' active' : '');

                var cardHeader = document.createElement('div');
                cardHeader.className = 'skill-card-header';

                var iconSpan = document.createElement('span');
                iconSpan.className = 'skill-card-icon';
                iconSpan.textContent = skill.icon;
                cardHeader.appendChild(iconSpan);

                var nameSpan = document.createElement('span');
                nameSpan.className = 'skill-card-name';
                nameSpan.textContent = skill.name;
                cardHeader.appendChild(nameSpan);

                if (skill.has_tools) {
                    var badge = document.createElement('span');
                    badge.className = 'skill-tools-badge';
                    badge.textContent = 'MCP';
                    cardHeader.appendChild(badge);
                }

                var toggle = document.createElement('label');
                toggle.className = 'skill-toggle';
                var checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.checked = isActive;
                checkbox.onchange = function () { toggleSkill(skill.id, checkbox.checked); };
                var slider = document.createElement('span');
                slider.className = 'skill-slider';
                toggle.appendChild(checkbox);
                toggle.appendChild(slider);
                cardHeader.appendChild(toggle);

                card.appendChild(cardHeader);

                var desc = document.createElement('div');
                desc.className = 'skill-card-desc';
                desc.textContent = skill.description;
                card.appendChild(desc);

                grid.appendChild(card);
            });

            section.appendChild(grid);
            body.appendChild(section);
        });

        if (Object.keys(allSkills).length === 0) {
            var loading = document.createElement('div');
            loading.style.cssText = 'text-align:center;padding:2rem;opacity:0.5';
            loading.textContent = 'Loading skills...';
            body.appendChild(loading);
        }
    }

    async function toggleSkill(skillId, enabled) {
        if (!currentAgent) return;
        var market = detectMarket();
        try {
            if (enabled) {
                await fetch(API_BASE + '/api/skills/agent/' + encodeURIComponent(currentAgent) + '/' + skillId + '?market=' + market, { method: 'POST' });
            } else {
                await fetch(API_BASE + '/api/skills/agent/' + encodeURIComponent(currentAgent) + '/' + skillId + '?market=' + market, { method: 'DELETE' });
            }
            await loadAgentSkills();
        } catch (e) {
            console.error('Toggle skill failed:', e);
        }
    }

    function init() {
        var style = document.createElement('style');
        style.textContent = [
            '.skills-fab{position:fixed;bottom:200px;right:24px;width:48px;height:48px;border-radius:50%;background:var(--card-bg,#1a1a2e);border:1px solid var(--border-color,#2a2a4a);color:var(--text-primary,#e0e0e0);cursor:pointer;z-index:9998;box-shadow:0 4px 12px rgba(0,0,0,.3);transition:all .2s;display:flex;align-items:center;justify-content:center}',
            '.skills-fab:hover,.skills-fab.active{background:var(--accent-blue,#4a90d9);border-color:var(--accent-blue,#4a90d9)}',
            '.skills-panel{position:fixed;top:0;right:-420px;width:400px;height:100vh;background:var(--bg-primary,#0d0d1a);border-left:1px solid var(--border-color,#2a2a4a);z-index:9999;transition:right .3s ease;display:flex;flex-direction:column;overflow:hidden}',
            '.skills-panel.open{right:0}',
            '.skills-panel-header{display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid var(--border-color,#2a2a4a)}',
            '.skills-panel-title{font-size:1.1rem;font-weight:600;color:var(--text-primary,#e0e0e0)}',
            '.skills-panel-close{background:none;border:none;color:var(--text-secondary,#888);font-size:1.5rem;cursor:pointer;padding:0 4px}',
            '.skills-panel-agent-bar{display:flex;gap:6px;padding:10px 20px;border-bottom:1px solid var(--border-color,#2a2a4a);flex-wrap:wrap}',
            '.skills-agent-pill{padding:4px 12px;border-radius:14px;border:1px solid var(--border-color,#2a2a4a);background:transparent;color:var(--text-secondary,#888);font-size:.75rem;cursor:pointer;transition:all .15s}',
            '.skills-agent-pill.selected{background:var(--accent-blue,#4a90d9);color:#fff;border-color:var(--accent-blue,#4a90d9)}',
            '.skills-panel-body{flex:1;overflow-y:auto;padding:16px 20px}',
            '.skills-section{margin-bottom:20px}',
            '.skills-section-title{font-size:.85rem;font-weight:600;color:var(--text-secondary,#aaa);margin-bottom:10px}',
            '.skills-grid{display:flex;flex-direction:column;gap:8px}',
            '.skill-card{background:var(--card-bg,#1a1a2e);border:1px solid var(--border-color,#2a2a4a);border-radius:8px;padding:10px 14px;transition:all .15s}',
            '.skill-card.active{border-color:var(--accent-blue,#4a90d9);background:rgba(74,144,217,.08)}',
            '.skill-card-header{display:flex;align-items:center;gap:8px}',
            '.skill-card-icon{font-size:1.1rem}',
            '.skill-card-name{font-size:.85rem;font-weight:500;color:var(--text-primary,#e0e0e0);flex:1}',
            '.skill-card-desc{font-size:.72rem;color:var(--text-secondary,#888);margin-top:6px;line-height:1.4}',
            '.skill-tools-badge{font-size:.6rem;padding:2px 6px;border-radius:4px;background:rgba(74,144,217,.2);color:var(--accent-blue,#4a90d9);font-weight:600}',
            '.skill-toggle{position:relative;display:inline-block;width:36px;height:20px;flex-shrink:0}',
            '.skill-toggle input{opacity:0;width:0;height:0}',
            '.skill-slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:var(--border-color,#2a2a4a);border-radius:20px;transition:.2s}',
            '.skill-slider:before{content:"";position:absolute;height:16px;width:16px;left:2px;bottom:2px;background:#fff;border-radius:50%;transition:.2s}',
            '.skill-toggle input:checked+.skill-slider{background:var(--accent-blue,#4a90d9)}',
            '.skill-toggle input:checked+.skill-slider:before{transform:translateX(16px)}',
        ].join('\n');
        document.head.appendChild(style);
        createPanel();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
