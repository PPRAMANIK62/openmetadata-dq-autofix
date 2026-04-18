/**
 * DQ AutoFix - Diagnostic Console
 * Vanilla JavaScript application with safe DOM manipulation
 */

// ============================================
// STATE
// ============================================
const state = {
    failures: [],
    strategies: [],
    selectedFailureId: null,
    suggestion: null,
    suggestionError: null,
    isLoadingFailures: false,
    isLoadingDiagnosis: false,
    error: null,
    apiVersion: null
};

// ============================================
// API CLIENT
// ============================================
const API_BASE = '/api/v1';

const api = {
    async request(endpoint, options = {}) {
        try {
            const response = await fetch(`${API_BASE}${endpoint}`, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });
            
            if (!response.ok) {
                const error = await response.json().catch(() => ({}));
                throw new Error(error.detail || `HTTP ${response.status}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error(`API Error [${endpoint}]:`, error);
            throw error;
        }
    },

    async checkHealth() {
        return this.request('/health');
    },

    async getFailures() {
        return this.request('/failures');
    },

    async getStrategies() {
        return this.request('/strategies');
    },

    async getSuggestion(failureId) {
        // API expects camelCase: failureId (see SuggestRequest schema)
        return this.request('/suggest', {
            method: 'POST',
            body: JSON.stringify({ failureId: failureId })
        });
    }
};

// ============================================
// DOM HELPERS (Safe DOM manipulation)
// ============================================
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => document.querySelectorAll(selector);

function createElement(tag, className, attributes = {}) {
    const el = document.createElement(tag);
    if (className) el.className = className;
    Object.entries(attributes).forEach(([key, value]) => {
        if (key === 'text') {
            el.textContent = value;
        } else {
            el.setAttribute(key, value);
        }
    });
    return el;
}

function clearElement(el) {
    while (el.firstChild) {
        el.removeChild(el.firstChild);
    }
}

// ============================================
// RENDERING FUNCTIONS
// ============================================

function getConfidenceLevel(score) {
    if (score >= 0.8) return 'high';
    if (score >= 0.6) return 'medium';
    return 'low';
}

function getConfidenceLabel(score) {
    if (score >= 0.8) return 'Safe to apply';
    if (score >= 0.6) return 'Review first';
    return 'Manual review';
}

function renderConnectionStatus(connected, version) {
    const statusEl = $('#connection-status');
    statusEl.className = `connection-status ${connected ? 'connected' : 'disconnected'}`;
    statusEl.querySelector('.status-text').textContent = connected 
        ? `CONNECTED v${version}` 
        : 'DISCONNECTED';
}

function renderStats() {
    const total = state.failures.length;
    const fixable = state.failures.filter(f => {
        if (!state.strategies.length) return false;
        const testType = f.testDefinition;
        return state.strategies.some(s => 
            s.supportedTestTypes?.includes(testType)
        );
    }).length;
    const ready = state.failures.filter(f => {
        const testType = f.testDefinition;
        const strategy = state.strategies.find(s => 
            s.supportedTestTypes?.includes(testType)
        );
        return strategy && (f.confidence || 0) >= 0.8;
    }).length;

    const failuresEl = $('#stat-failures');
    const fixableEl = $('#stat-fixable');
    const readyEl = $('#stat-ready');
    
    if (failuresEl) failuresEl.textContent = total;
    if (fixableEl) fixableEl.textContent = fixable;
    if (readyEl) readyEl.textContent = ready;
}

function createFailureCard(failure, isSelected) {
    const card = createElement('div', 'failure-card', {
        tabindex: '0',
        role: 'button',
        'data-failure-id': failure.id,
        'data-selected': isSelected ? 'true' : 'false'
    });
    
    // Indicator
    const indicator = createElement('div', 'failure-indicator');
    card.appendChild(indicator);
    
    // Info section
    const info = createElement('div', 'failure-info');
    
    // API returns 'name' or 'displayName', not 'test_name'
    const testName = failure.displayName || failure.name || failure.id;
    const name = createElement('div', 'failure-name', { text: testName });
    info.appendChild(name);
    
    // API returns 'tableFqn' and 'columnName', not snake_case
    const tableName = failure.tableFqn ? failure.tableFqn.split('.').pop() : 'Unknown table';
    const columnName = failure.columnName || 'Unknown column';
    
    const meta = createElement('div', 'failure-meta');
    const tableSpan = createElement('span', '', { text: tableName });
    const separator = createElement('span', 'failure-meta-separator', { text: '→' });
    const columnSpan = createElement('span', '', { text: columnName });
    meta.appendChild(tableSpan);
    meta.appendChild(separator);
    meta.appendChild(columnSpan);
    info.appendChild(meta);
    
    card.appendChild(info);
    
    // Test type badge
    const testType = failure.testDefinition || 'unknown';
    const badge = createElement('span', 'failure-badge', { text: formatTestType(testType) });
    card.appendChild(badge);
    
    return card;
}

function formatTestType(testType) {
    // Convert OpenMetadata test definition names to readable labels
    const typeMap = {
        'columnValuesToNotBeNull': 'NULL',
        'columnValuesToBeUnique': 'UNIQUE',
        'columnValuesToMatchRegex': 'REGEX',
        'columnValuesToBeInSet': 'ENUM',
        'columnValuesToBeBetween': 'RANGE',
        'columnValueLengthsToBeBetween': 'LENGTH',
        'tableRowCountToBeBetween': 'ROWS',
        'tableColumnCountToBeBetween': 'COLS',
        'unknown': '?',
    };
    return typeMap[testType] || '?';
}

function renderFailures() {
    const container = $('#failures-list');
    clearElement(container);
    
    if (state.isLoadingFailures) {
        const loading = createElement('div', 'loading-state', { text: 'Loading failures...' });
        container.appendChild(loading);
        return;
    }
    
    if (state.error) {
        const error = createElement('div', 'empty-state', { text: state.error });
        container.appendChild(error);
        return;
    }
    
    if (!state.failures.length) {
        const empty = createElement('div', 'empty-state', { text: 'No failures found' });
        container.appendChild(empty);
        return;
    }
    
    state.failures.forEach(failure => {
        const isSelected = failure.id === state.selectedFailureId;
        const card = createFailureCard(failure, isSelected);
        
        card.addEventListener('click', () => selectFailure(failure.id));
        card.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                selectFailure(failure.id);
            }
        });
        
        container.appendChild(card);
    });
}

function createDataTable(rows, columns, diffMap = null, tableType = null) {
    if (!rows || !rows.length) {
        return createElement('p', 'empty-state', { text: 'No preview data' });
    }
    
    const cols = columns || Object.keys(rows[0]);
    
    const table = createElement('table', 'data-table');
    
    // Header
    const thead = createElement('thead');
    const headerRow = createElement('tr');
    cols.forEach(col => {
        const th = createElement('th', '', { text: col });
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    // Body
    const tbody = createElement('tbody');
    rows.slice(0, 5).forEach((row, rowIndex) => {
        const tr = createElement('tr');
        cols.forEach(col => {
            const value = row[col];
            const td = createElement('td');
            
            // Check if this cell is different
            const isDiff = diffMap && diffMap[rowIndex]?.has(col);
            
            if (value === null || value === undefined) {
                td.className = isDiff ? 'null-value diff-cell' : 'null-value';
                td.textContent = 'NULL';
            } else {
                const strValue = String(value);
                if (isDiff) {
                    td.className = tableType === 'after' ? 'diff-cell diff-added' : 'diff-cell diff-removed';
                    // Show whitespace visually for diff cells in "before" table
                    renderWithWhitespace(td, strValue, tableType);
                } else {
                    td.textContent = strValue;
                }
            }
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    
    return table;
}

function renderWithWhitespace(td, str, tableType) {
    // Show leading/trailing whitespace with visible markers in "before" table
    const leadingMatch = str.match(/^(\s*)/);
    const trailingMatch = str.match(/(\s*)$/);
    const leadingSpaces = leadingMatch ? leadingMatch[0] : '';
    const trailingSpaces = trailingMatch ? trailingMatch[0] : '';
    const middle = str.slice(leadingSpaces.length, str.length - (trailingSpaces.length || 0)) || str;
    
    // For "before" table, show dots for whitespace that will be removed
    if (tableType === 'before' && (leadingSpaces || trailingSpaces)) {
        if (leadingSpaces) {
            const marker = createElement('span', 'whitespace-marker', { text: '·'.repeat(leadingSpaces.length) });
            td.appendChild(marker);
        }
        
        const content = document.createTextNode(middle);
        td.appendChild(content);
        
        if (trailingSpaces) {
            const marker = createElement('span', 'whitespace-marker', { text: '·'.repeat(trailingSpaces.length) });
            td.appendChild(marker);
        }
    } else {
        td.textContent = str;
    }
}

function computeDiffMap(beforeRows, afterRows) {
    if (!beforeRows || !afterRows) return null;
    
    const diffMap = {};
    const cols = Object.keys(beforeRows[0] || afterRows[0] || {});
    const maxRows = Math.max(beforeRows.length, afterRows.length);
    
    for (let i = 0; i < Math.min(maxRows, 5); i++) {
        const beforeRow = beforeRows[i] || {};
        const afterRow = afterRows[i] || {};
        diffMap[i] = new Set();
        
        cols.forEach(col => {
            const beforeVal = beforeRow[col];
            const afterVal = afterRow[col];
            
            // Compare values (handle null/undefined)
            const beforeStr = beforeVal === null || beforeVal === undefined ? null : String(beforeVal);
            const afterStr = afterVal === null || afterVal === undefined ? null : String(afterVal);
            
            if (beforeStr !== afterStr) {
                diffMap[i].add(col);
            }
        });
    }
    
    return diffMap;
}

function createSQLBlock(sql) {
    const keywords = [
        'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL',
        'UPDATE', 'SET', 'INSERT', 'INTO', 'VALUES', 'DELETE', 'CREATE',
        'TABLE', 'INDEX', 'ALTER', 'DROP', 'JOIN', 'LEFT', 'RIGHT', 'INNER',
        'OUTER', 'ON', 'AS', 'DISTINCT', 'ORDER', 'BY', 'GROUP', 'HAVING',
        'LIMIT', 'OFFSET', 'UNION', 'ALL', 'CASE', 'WHEN', 'THEN', 'ELSE',
        'END', 'WITH', 'CTE', 'OVER', 'PARTITION', 'ROW_NUMBER', 'RANK',
        'COALESCE', 'NULLIF', 'CAST', 'BETWEEN', 'LIKE', 'EXISTS', 'TRUE', 'FALSE'
    ];
    
    const functions = [
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'NULLIF', 'CAST',
        'UPPER', 'LOWER', 'TRIM', 'LENGTH', 'SUBSTRING', 'CONCAT', 'NOW',
        'DATE', 'YEAR', 'MONTH', 'DAY', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE'
    ];
    
    const codeBlock = createElement('div', 'code-block');
    const pre = createElement('pre');
    
    // Tokenize and highlight SQL
    const tokens = tokenizeSQL(sql, keywords, functions);
    tokens.forEach(token => {
        const span = createElement('span', token.type ? `sql-${token.type}` : '');
        span.textContent = token.text;
        pre.appendChild(span);
    });
    
    codeBlock.appendChild(pre);
    return codeBlock;
}

function tokenizeSQL(sql, keywords, functions) {
    const tokens = [];
    let remaining = sql;
    
    const patterns = [
        { type: 'comment', regex: /^--[^\n]*/ },
        { type: 'string', regex: /^'[^']*'/ },
        { type: 'number', regex: /^\b\d+(?:\.\d+)?\b/ },
        { type: 'keyword', regex: new RegExp(`^\\b(${keywords.join('|')})\\b`, 'i') },
        { type: 'function', regex: new RegExp(`^\\b(${functions.join('|')})(?=\\s*\\()`, 'i') },
        { type: null, regex: /^[a-zA-Z_][a-zA-Z0-9_]*/ },
        { type: null, regex: /^\s+/ },
        { type: null, regex: /^[^\s\w']+/ }
    ];
    
    while (remaining.length > 0) {
        let matched = false;
        
        for (const pattern of patterns) {
            const match = remaining.match(pattern.regex);
            if (match) {
                tokens.push({ type: pattern.type, text: match[0] });
                remaining = remaining.slice(match[0].length);
                matched = true;
                break;
            }
        }
        
        if (!matched) {
            tokens.push({ type: null, text: remaining[0] });
            remaining = remaining.slice(1);
        }
    }
    
    return tokens;
}

function renderDiagnosis() {
    const container = $('#diagnosis-content');
    clearElement(container);
    
    if (!state.selectedFailureId) {
        const empty = createElement('div', 'empty-state');
        const p = createElement('p', '', { text: 'Select a failure to view diagnosis' });
        empty.appendChild(p);
        container.appendChild(empty);
        return;
    }
    
    if (state.isLoadingDiagnosis) {
        const loading = createElement('div', 'loading-state', { text: 'Analyzing failure...' });
        container.appendChild(loading);
        return;
    }
    
    if (!state.suggestion) {
        const empty = createElement('div', 'empty-state no-suggestion');
        
        // Icon
        const icon = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        icon.setAttribute('width', '48');
        icon.setAttribute('height', '48');
        icon.setAttribute('viewBox', '0 0 24 24');
        icon.setAttribute('fill', 'none');
        icon.setAttribute('stroke', 'currentColor');
        icon.setAttribute('stroke-width', '1.5');
        icon.classList.add('empty-icon');
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('cx', '12');
        circle.setAttribute('cy', '12');
        circle.setAttribute('r', '10');
        const line1 = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line1.setAttribute('x1', '12');
        line1.setAttribute('y1', '8');
        line1.setAttribute('x2', '12');
        line1.setAttribute('y2', '12');
        const line2 = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line2.setAttribute('x1', '12');
        line2.setAttribute('y1', '16');
        line2.setAttribute('x2', '12.01');
        line2.setAttribute('y2', '16');
        icon.appendChild(circle);
        icon.appendChild(line1);
        icon.appendChild(line2);
        empty.appendChild(icon);
        
        // Title
        const title = createElement('p', 'empty-title', { text: 'No fix available' });
        empty.appendChild(title);
        
        // Reason
        const reason = state.suggestionError || 'No applicable fix strategy found for this failure type';
        const desc = createElement('p', 'empty-description', { text: reason });
        empty.appendChild(desc);
        
        // Get the selected failure to show more context
        const failure = state.failures.find(f => f.id === state.selectedFailureId);
        if (failure) {
            const testType = failure.testDefinition || 'unknown';
            const hint = createElement('p', 'empty-hint', { 
                text: `Test type: ${testType}` 
            });
            empty.appendChild(hint);
        }
        
        container.appendChild(empty);
        return;
    }
    
    const { suggestion } = state;
    // SuggestResponse returns confidenceScore (camelCase)
    const confidence = suggestion.confidenceScore || 0;
    const level = getConfidenceLevel(confidence);
    const percentage = Math.round(confidence * 100);
    
    // Find strategy info - SuggestResponse returns 'strategy' (the name)
    const strategyInfo = state.strategies.find(s => s.name === suggestion.strategy);
    
    // Strategy Section
    const strategySection = createElement('div', 'strategy-section');
    const strategyName = createElement('div', 'strategy-name', { 
        text: suggestion.strategy || 'Unknown Strategy' 
    });
    const strategyDesc = createElement('div', 'strategy-description', { 
        text: suggestion.strategyDescription || strategyInfo?.description || '' 
    });
    strategySection.appendChild(strategyName);
    strategySection.appendChild(strategyDesc);
    container.appendChild(strategySection);
    
    // Confidence + Changes Summary Row
    const preview = suggestion.preview;
    const infoRow = createElement('div', 'info-row');
    
    // Confidence Display
    const confDisplay = createElement('div', 'confidence-display');
    
    const confHeader = createElement('div', 'confidence-header');
    const confLabel = createElement('span', 'confidence-label', { text: 'CONFIDENCE' });
    const confPct = createElement('span', `confidence-percentage ${level}`, { text: `${percentage}%` });
    confHeader.appendChild(confLabel);
    confHeader.appendChild(confPct);
    confDisplay.appendChild(confHeader);
    
    const confTrack = createElement('div', 'confidence-track-large');
    const confFill = createElement('div', `confidence-fill-large ${level}`);
    confFill.style.width = `${percentage}%`;
    confTrack.appendChild(confFill);
    confDisplay.appendChild(confTrack);
    
    const confLevel = createElement('div', `confidence-level ${level}`, { text: getConfidenceLabel(confidence) });
    confDisplay.appendChild(confLevel);
    
    infoRow.appendChild(confDisplay);
    
    // Changes summary - SuggestResponse.preview.changesSummary (camelCase)
    if (preview?.changesSummary) {
        const summary = createElement('div', 'changes-summary', { text: preview.changesSummary });
        infoRow.appendChild(summary);
    }
    
    container.appendChild(infoRow);
    
    // Preview section - SuggestResponse.preview.beforeSample/afterSample (camelCase)
    if (preview?.beforeSample || preview?.afterSample) {
        const previewSection = createElement('div', 'preview-section');
        const previewHeader = createElement('div', 'preview-header', { text: 'PREVIEW' });
        previewSection.appendChild(previewHeader);
        
        // Compute diffs between before and after
        const diffMap = computeDiffMap(preview.beforeSample, preview.afterSample);
        
        const previewGrid = createElement('div', 'preview-grid');
        
        // Before column
        const beforeCol = createElement('div', 'preview-column before');
        const beforeHeader = createElement('div', 'preview-column-header', { text: 'BEFORE' });
        beforeCol.appendChild(beforeHeader);
        const beforeWrapper = createElement('div', 'data-table-wrapper');
        beforeWrapper.appendChild(createDataTable(preview.beforeSample, null, diffMap, 'before'));
        beforeCol.appendChild(beforeWrapper);
        previewGrid.appendChild(beforeCol);
        
        // After column
        const afterCol = createElement('div', 'preview-column after');
        const afterHeader = createElement('div', 'preview-column-header', { text: 'AFTER' });
        afterCol.appendChild(afterHeader);
        const afterWrapper = createElement('div', 'data-table-wrapper');
        afterWrapper.appendChild(createDataTable(preview.afterSample, null, diffMap, 'after'));
        afterCol.appendChild(afterWrapper);
        previewGrid.appendChild(afterCol);
        
        previewSection.appendChild(previewGrid);
        container.appendChild(previewSection);
    }
    
    // Fix SQL section - SuggestResponse.fixSql (camelCase)
    if (suggestion.fixSql) {
        const sqlSection = createElement('div', 'sql-section');
        
        const sqlHeader = createElement('div', 'sql-header');
        const sqlLabel = createElement('span', 'sql-label', { text: 'FIX SQL' });
        sqlHeader.appendChild(sqlLabel);
        
        const copyBtn = createElement('button', 'btn', { 'aria-label': 'Copy SQL' });
        copyBtn.appendChild(createCopyIcon());
        const copyText = createElement('span', '', { text: 'COPY' });
        copyBtn.appendChild(copyText);
        copyBtn.addEventListener('click', () => copySQL(suggestion.fixSql));
        sqlHeader.appendChild(copyBtn);
        
        sqlSection.appendChild(sqlHeader);
        sqlSection.appendChild(createSQLBlock(suggestion.fixSql));
        container.appendChild(sqlSection);
    }
    
    // Rollback SQL section - same structure as FIX SQL but collapsible
    if (suggestion.rollbackSql) {
        const rollbackSection = createElement('div', 'sql-section rollback-section');
        
        const rollbackHeader = createElement('div', 'sql-header');
        
        // Clickable label with chevron
        const rollbackToggle = createElement('button', 'sql-label rollback-toggle', { 'aria-expanded': 'false' });
        const chevron = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        chevron.setAttribute('width', '8');
        chevron.setAttribute('height', '8');
        chevron.setAttribute('viewBox', '0 0 16 16');
        chevron.setAttribute('fill', 'currentColor');
        chevron.classList.add('rollback-chevron');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', 'M6 4l4 4-4 4');
        chevron.appendChild(path);
        rollbackToggle.appendChild(chevron);
        const toggleText = document.createTextNode(' ROLLBACK SQL');
        rollbackToggle.appendChild(toggleText);
        rollbackHeader.appendChild(rollbackToggle);
        
        const copyRollbackBtn = createElement('button', 'btn', { 'aria-label': 'Copy rollback SQL' });
        copyRollbackBtn.appendChild(createCopyIcon());
        const copyText = createElement('span', '', { text: 'COPY' });
        copyRollbackBtn.appendChild(copyText);
        copyRollbackBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            copySQL(suggestion.rollbackSql);
        });
        rollbackHeader.appendChild(copyRollbackBtn);
        
        rollbackSection.appendChild(rollbackHeader);
        
        const rollbackContent = createElement('div', 'rollback-content');
        rollbackContent.appendChild(createSQLBlock(suggestion.rollbackSql));
        rollbackSection.appendChild(rollbackContent);
        
        rollbackToggle.addEventListener('click', () => {
            const isExpanded = rollbackToggle.getAttribute('aria-expanded') === 'true';
            rollbackToggle.setAttribute('aria-expanded', !isExpanded);
            rollbackSection.classList.toggle('expanded', !isExpanded);
        });
        
        container.appendChild(rollbackSection);
    }
}

function createCopyIcon() {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', '14');
    svg.setAttribute('height', '14');
    svg.setAttribute('viewBox', '0 0 16 16');
    svg.setAttribute('fill', 'none');
    svg.setAttribute('stroke', 'currentColor');
    svg.setAttribute('stroke-width', '2');
    
    const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    rect.setAttribute('x', '5');
    rect.setAttribute('y', '5');
    rect.setAttribute('width', '9');
    rect.setAttribute('height', '9');
    rect.setAttribute('rx', '1');
    svg.appendChild(rect);
    
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', 'M2 11V3a1 1 0 0 1 1-1h8');
    svg.appendChild(path);
    
    return svg;
}

// ============================================
// EVENT HANDLERS
// ============================================

async function selectFailure(failureId) {
    if (state.selectedFailureId === failureId) return;
    
    state.selectedFailureId = failureId;
    state.suggestion = null;
    state.suggestionError = null;
    state.isLoadingDiagnosis = true;
    
    renderFailures();
    renderDiagnosis();
    
    try {
        state.suggestion = await api.getSuggestion(failureId);
        state.suggestionError = null;
    } catch (error) {
        console.error('Failed to get suggestion:', error);
        state.suggestion = null;
        state.suggestionError = error.message || 'Failed to analyze failure';
    } finally {
        state.isLoadingDiagnosis = false;
        renderDiagnosis();
    }
}

async function copySQL(sql) {
    if (!sql) return;
    
    try {
        await navigator.clipboard.writeText(sql);
        showToast('SQL copied to clipboard', 'success');
    } catch (error) {
        console.error('Failed to copy:', error);
        showToast('Failed to copy SQL');
    }
}

async function refresh() {
    state.isLoadingFailures = true;
    state.error = null;
    state.selectedFailureId = null;
    state.suggestion = null;
    
    renderFailures();
    renderDiagnosis();
    
    await loadData();
}

function showToast(message, type = '') {
    const toast = $('#toast');
    toast.textContent = message;
    toast.className = `toast ${type}`;
    
    requestAnimationFrame(() => {
        toast.classList.add('visible');
    });
    
    setTimeout(() => {
        toast.classList.remove('visible');
    }, 2500);
}

// ============================================
// KEYBOARD NAVIGATION
// ============================================

function setupKeyboardNavigation() {
    document.addEventListener('keydown', (e) => {
        // Arrow key navigation for failures
        if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
            const failureCards = $$('.failure-card');
            if (!failureCards.length) return;
            
            const currentIndex = Array.from(failureCards).findIndex(
                card => card.getAttribute('data-selected') === 'true'
            );
            
            let nextIndex;
            if (e.key === 'ArrowDown') {
                nextIndex = currentIndex < failureCards.length - 1 ? currentIndex + 1 : 0;
            } else {
                nextIndex = currentIndex > 0 ? currentIndex - 1 : failureCards.length - 1;
            }
            
            const nextCard = failureCards[nextIndex];
            if (nextCard) {
                const failureId = nextCard.getAttribute('data-failure-id');
                selectFailure(failureId);
                nextCard.focus();
                e.preventDefault();
            }
        }
        
        // Escape to deselect
        if (e.key === 'Escape' && state.selectedFailureId) {
            state.selectedFailureId = null;
            state.suggestion = null;
            renderFailures();
            renderDiagnosis();
        }
    });
}

// ============================================
// INITIALIZATION
// ============================================

async function loadData() {
    state.isLoadingFailures = true;
    renderFailures();
    
    try {
        // Load failures and strategies in parallel
        const [failuresResponse, strategiesResponse] = await Promise.all([
            api.getFailures(),
            api.getStrategies()
        ]);
        
        // API returns {data: [...], total: N} format
        state.failures = failuresResponse?.data || failuresResponse || [];
        state.strategies = strategiesResponse?.data || strategiesResponse || [];
        state.isLoadingFailures = false;
        
        renderStats();
        renderFailures();
    } catch (error) {
        console.error('loadData error:', error);
        state.error = 'Failed to load data. Is the API running?';
        state.isLoadingFailures = false;
        renderFailures();
    }
}

// ============================================
// RESIZABLE PANELS
// ============================================

function setupResizer() {
    const resizer = $('#resizer');
    const failuresPanel = $('.failures-panel');
    const main = $('.main');
    let isResizing = false;
    
    // Restore saved width from localStorage
    const savedWidth = localStorage.getItem('dq-autofix-panel-width');
    if (savedWidth) {
        failuresPanel.style.width = savedWidth;
    }
    
    // Mouse events
    resizer.addEventListener('mousedown', (e) => {
        isResizing = true;
        resizer.classList.add('dragging');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });
    
    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;
        
        const containerRect = main.getBoundingClientRect();
        const newWidth = e.clientX - containerRect.left;
        const percentage = (newWidth / containerRect.width) * 100;
        
        // Clamp between 20% and 60%
        const clampedPercentage = Math.min(60, Math.max(20, percentage));
        failuresPanel.style.width = `${clampedPercentage}%`;
    });
    
    document.addEventListener('mouseup', () => {
        if (isResizing) {
            isResizing = false;
            resizer.classList.remove('dragging');
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            
            // Save width to localStorage
            localStorage.setItem('dq-autofix-panel-width', failuresPanel.style.width);
        }
    });
    
    // Keyboard support for accessibility
    resizer.addEventListener('keydown', (e) => {
        const currentWidth = parseFloat(failuresPanel.style.width) || 35;
        const step = 2;
        
        if (e.key === 'ArrowLeft') {
            const newWidth = Math.max(20, currentWidth - step);
            failuresPanel.style.width = `${newWidth}%`;
            localStorage.setItem('dq-autofix-panel-width', failuresPanel.style.width);
            e.preventDefault();
        } else if (e.key === 'ArrowRight') {
            const newWidth = Math.min(60, currentWidth + step);
            failuresPanel.style.width = `${newWidth}%`;
            localStorage.setItem('dq-autofix-panel-width', failuresPanel.style.width);
            e.preventDefault();
        }
    });
    
    // Touch support for mobile/tablet
    resizer.addEventListener('touchstart', (e) => {
        isResizing = true;
        resizer.classList.add('dragging');
        e.preventDefault();
    }, { passive: false });
    
    document.addEventListener('touchmove', (e) => {
        if (!isResizing) return;
        
        const touch = e.touches[0];
        const containerRect = main.getBoundingClientRect();
        const newWidth = touch.clientX - containerRect.left;
        const percentage = (newWidth / containerRect.width) * 100;
        
        const clampedPercentage = Math.min(60, Math.max(20, percentage));
        failuresPanel.style.width = `${clampedPercentage}%`;
    }, { passive: true });
    
    document.addEventListener('touchend', () => {
        if (isResizing) {
            isResizing = false;
            resizer.classList.remove('dragging');
            localStorage.setItem('dq-autofix-panel-width', failuresPanel.style.width);
        }
    });
}

async function init() {
    // Check connection status
    try {
        const health = await api.checkHealth();
        state.apiVersion = health.version || '0.1.0';
        renderConnectionStatus(true, state.apiVersion);
    } catch (error) {
        renderConnectionStatus(false);
    }
    
    // Setup event listeners
    $('#refresh-btn').addEventListener('click', refresh);
    setupKeyboardNavigation();
    setupResizer();
    
    // Load initial data
    await loadData();
}

// Start the app
document.addEventListener('DOMContentLoaded', init);
