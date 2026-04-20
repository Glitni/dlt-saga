(function() {
  'use strict';

  // Theme init — must run before any rendering
  (function() {
    if (localStorage.getItem('saga-report-theme') === 'dark') {
      document.documentElement.setAttribute('data-theme', 'dark');
    }
  })();

  const D = REPORT_DATA;

  // ---- Lookup: pipeline_name -> pipeline config ----
  const pipelineMap = {};
  D.pipelines.forEach(p => { pipelineMap[p.pipeline_name] = p; });

  // ---- Utility functions ----
  function $(sel, ctx) { return (ctx || document).querySelector(sel); }
  function $$(sel, ctx) { return Array.from((ctx || document).querySelectorAll(sel)); }
  function h(tag, attrs, ...children) {
    const el = document.createElement(tag);
    if (attrs) Object.entries(attrs).forEach(([k, v]) => {
      if (v == null) return;
      if (k === 'className') el.className = v;
      else if (k === 'innerHTML') el.innerHTML = v;
      else if (k.startsWith('on')) el.addEventListener(k.slice(2).toLowerCase(), v);
      else el.setAttribute(k, v);
    });
    children.flat().forEach(c => {
      if (c == null) return;
      el.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    });
    return el;
  }

  function fmtNum(n) { return n == null ? '-' : n.toLocaleString(); }
  function fmtDuration(sec) {
    if (sec == null) return '-';
    if (sec < 60) return sec.toFixed(1) + 's';
    if (sec < 3600) return (sec / 60).toFixed(1) + 'm';
    return (sec / 3600).toFixed(1) + 'h';
  }
  function fmtDate(iso) {
    if (!iso) return '-';
    const d = new Date(iso);
    return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
      + ' ' + d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  }
  function fmtDateShort(iso) {
    if (!iso) return '-';
    const d = new Date(iso);
    return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
  }
  function statusBadge(status) {
    const cls = status === 'success' || status === 'completed' ? 'badge-success'
      : status === 'failed' ? 'badge-danger'
      : status === 'skipped' ? 'badge-neutral'
      : 'badge-warning';
    return '<span class="badge ' + cls + '">' + status + '</span>';
  }
  function tagBadges(tags) {
    return tags.map(t => '<span class="tag">' + t + '</span>').join('');
  }
  function escHtml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function tagsFor(name) {
    const p = pipelineMap[name];
    return p ? p.tags : [];
  }
  function dispositionFor(name) {
    const p = pipelineMap[name];
    return p ? p.write_disposition : '';
  }

  const allTags = [...new Set(D.pipelines.flatMap(p => p.tags))].sort();
  const allDispositions = [...new Set(D.pipelines.map(p => p.write_disposition))].sort();

  // ---- Merged ingest data: load_runs + failed orchestration tasks ----
  // Failed orchestration tasks represent ingest failures that never reach _saga_load_info.
  const orchRuns = D.orchestration_runs || [];

  // Build execution summaries
  const execMap = {};
  orchRuns.forEach(r => {
    if (!execMap[r.execution_id]) execMap[r.execution_id] = [];
    execMap[r.execution_id].push(r);
  });
  const execList = Object.entries(execMap).map(([id, tasks]) => {
    const failed = tasks.filter(t => t.status === 'failed').length;
    const completed = tasks.filter(t => t.status === 'completed').length;
    const pending = tasks.filter(t => t.status === 'pending').length;
    const running = tasks.filter(t => t.status === 'running').length;
    const timestamps = tasks.map(t => t.log_timestamp).filter(Boolean).sort();
    const startTimes = tasks.map(t => t.started_at).filter(Boolean).sort();
    const endTimes = tasks.map(t => t.completed_at).filter(Boolean).sort();
    let duration = null;
    if (startTimes.length && endTimes.length) {
      duration = (new Date(endTimes[endTimes.length - 1]) - new Date(startTimes[0])) / 1000;
    }
    return { execution_id: id, total: tasks.length, completed, failed, pending, running,
      timestamp: timestamps.length ? timestamps[0] : null, duration, tasks,
      meta: null };
  }).sort((a, b) => (b.timestamp || '') > (a.timestamp || '') ? 1 : -1);

  // Enrich executions with metadata from _saga_executions
  const execMetaMap = {};
  (D.executions || []).forEach(e => { execMetaMap[e.execution_id] = e; });
  execList.forEach(e => { e.meta = execMetaMap[e.execution_id] || null; });

  // Tag load_runs with execution_id by matching table_name + time window
  const orchTasksByTable = {};
  orchRuns.forEach(r => {
    if (!orchTasksByTable[r.table_name]) orchTasksByTable[r.table_name] = [];
    orchTasksByTable[r.table_name].push(r);
  });

  const taggedLoadRuns = D.load_runs.map(r => {
    const candidates = orchTasksByTable[r.table_name] || [];
    let matchedExecId = null;
    if (r.started_at && candidates.length) {
      const rStart = new Date(r.started_at).getTime();
      let bestDist = Infinity;
      candidates.forEach(c => {
        if (!c.started_at) return;
        const cStart = new Date(c.started_at).getTime();
        const cEnd = c.completed_at ? new Date(c.completed_at).getTime() : cStart + 3600000;
        // Load run should start within the orchestration task's window (with 5min buffer)
        if (rStart >= cStart - 300000 && rStart <= cEnd + 300000) {
          const dist = Math.abs(rStart - cStart);
          if (dist < bestDist) { bestDist = dist; matchedExecId = c.execution_id; }
        }
      });
    }
    return { ...r, execution_id: matchedExecId };
  });

  // Synthesize failed ingest runs from failed orchestration tasks
  const failedOrchIngest = orchRuns
    .filter(r => r.status === 'failed')
    .map(r => ({
      pipeline_name: r.pipeline_name,
      table_name: r.table_name,
      dataset_name: 'dlt_' + r.pipeline_group,
      destination_name: '',
      destination_type: '',
      row_count: 0,
      started_at: r.started_at || r.log_timestamp,
      finished_at: r.completed_at,
      first_run: false,
      duration_seconds: r.duration_seconds,
      status: 'failed',
      execution_id: r.execution_id,
      error_message: r.error_message,
    }));

  // Identify completed orchestration tasks with no matching load_run (skipped — no new rows)
  const matchedOrchKeys = new Set();
  taggedLoadRuns.forEach(r => {
    if (r.execution_id) matchedOrchKeys.add(r.execution_id + '|' + r.table_name);
  });
  const skippedOrchIngest = orchRuns
    .filter(r => r.status === 'completed' && !matchedOrchKeys.has(r.execution_id + '|' + r.table_name))
    .map(r => ({
      pipeline_name: r.pipeline_name,
      table_name: r.table_name,
      dataset_name: 'dlt_' + r.pipeline_group,
      destination_name: '',
      destination_type: '',
      row_count: 0,
      started_at: r.started_at || r.log_timestamp,
      finished_at: r.completed_at,
      first_run: false,
      duration_seconds: r.duration_seconds,
      status: 'skipped',
      execution_id: r.execution_id,
      error_message: null,
    }));

  // Merged list: load_runs + failed orch tasks + skipped orch tasks
  const allIngestRuns = [...taggedLoadRuns, ...failedOrchIngest, ...skippedOrchIngest]
    .sort((a, b) => (b.started_at || '') > (a.started_at || '') ? 1 : -1);

  // All unique execution_ids across merged ingest runs
  const allExecIds = [...new Set(allIngestRuns.map(r => r.execution_id).filter(Boolean))].sort().reverse();

  // ---- Navigation with browser history ----
  let currentTab = 'dashboard';
  let previousTab = 'pipelines';
  let suppressPush = false; // prevent pushState during popstate handling

  // External filter state for cross-tab navigation
  let pendingIngestExecFilter = null;
  let pendingIngestDateFilter = null;
  let pendingHistorizeDateFilter = null;
  let pendingOrchDateFilter = null;

  function showTab(tabId, opts) {
    opts = opts || {};
    if (tabId === 'pipeline-detail') previousTab = currentTab;
    currentTab = tabId;
    $$('.nav-link').forEach(l => l.classList.toggle('active', l.dataset.tab === tabId));
    $$('.tab-panel').forEach(p => p.classList.toggle('active', p.id === 'tab-' + tabId));
    if (!suppressPush) {
      const state = { tab: tabId };
      if (opts.pipeline) state.pipeline = opts.pipeline;
      history.pushState(state, '', '#' + tabId + (opts.pipeline ? '/' + encodeURIComponent(opts.pipeline) : ''));
    }
  }

  window.addEventListener('popstate', e => {
    if (e.state && e.state.tab) {
      suppressPush = true;
      if (e.state.tab === 'pipeline-detail' && e.state.pipeline) {
        showPipelineDetail(e.state.pipeline);
      } else {
        showTab(e.state.tab);
      }
      suppressPush = false;
    }
  });

  $$('.nav-link').forEach(link => {
    link.addEventListener('click', e => { e.preventDefault(); showTab(link.dataset.tab); });
  });

  // ---- Meta info ----
  $('#meta-info').innerHTML =
    '<div>Env: <strong>' + (D.environment || 'unknown') + '</strong></div>' +
    '<div>Database: <strong>' + (D.project || 'unknown') + '</strong></div>' +
    '<div>Period: last ' + D.days + ' days</div>' +
    '<div>Generated: ' + fmtDate(D.generated_at) + '</div>';

  // ---- Chart helpers ----
  function buildDayBuckets(numDays) {
    const now = new Date();
    const days = [];
    for (let i = numDays - 1; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      d.setHours(0, 0, 0, 0);
      days.push(d);
    }
    return days;
  }

  function bucketByDay(runs, days, tsField) {
    const field = tsField || 'started_at';
    const counts = new Array(days.length).fill(0);
    runs.forEach(r => {
      const ts = r[field];
      if (!ts) return;
      const rd = new Date(ts);
      rd.setHours(0, 0, 0, 0);
      const idx = days.findIndex(d => d.getTime() === rd.getTime());
      if (idx >= 0) counts[idx]++;
    });
    return counts;
  }

  function localDateStr(d) {
    return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
  }

  function buildStackedBarChart(label, days, successCounts, failCounts, successColor, failColor, onBarClick) {
    const max = Math.max(...successCounts.map((s, i) => s + failCounts[i]), 1);
    const chart = h('div', { className: 'mini-chart' });
    chart.appendChild(h('h3', null, label));
    const bars = h('div', { className: 'bar-chart' });
    const labels = h('div', { className: 'bar-labels' });
    successCounts.forEach((s, i) => {
      const f = failCounts[i];
      const total = s + f;
      const totalPct = (total / max) * 100;
      const clickable = onBarClick && total > 0;
      const bar = h('div', { className: 'bar bar-stack' + (clickable ? ' bar-clickable' : '') });
      bar.style.height = Math.max(totalPct, 2) + '%';
      if (clickable) bar.addEventListener('click', () => onBarClick(localDateStr(days[i])));
      if (f > 0) {
        const failSeg = h('div', { className: 'bar-segment' });
        failSeg.style.height = (f / total * 100) + '%';
        failSeg.style.background = failColor;
        failSeg.style.borderRadius = s > 0 ? '0' : '3px 3px 0 0';
        bar.appendChild(failSeg);
      }
      if (s > 0 || f === 0) {
        const succSeg = h('div', { className: 'bar-segment' });
        succSeg.style.height = (total > 0 ? s / total * 100 : 100) + '%';
        succSeg.style.background = successColor;
        succSeg.style.borderRadius = '3px 3px 0 0';
        bar.appendChild(succSeg);
      }
      bar.appendChild(h('div', { className: 'bar-tooltip' },
        fmtDateShort(days[i].toISOString()) + ': ' + s + ' ok, ' + f + ' failed'));
      bars.appendChild(bar);
      labels.appendChild(h('span', null, i % 2 === 0 ? fmtDateShort(days[i].toISOString()) : ''));
    });
    chart.appendChild(bars);
    chart.appendChild(labels);
    return chart;
  }

  // ---- Donut chart helper ----
  function buildDonutChart(label, segments, centerValue, centerSub) {
    // segments: [{ value, color, label }]
    const total = segments.reduce((s, seg) => s + seg.value, 0);
    const chart = h('div', { className: 'donut-chart' });
    chart.appendChild(h('h3', null, label));

    const size = 140, cx = 70, cy = 70, r = 52, stroke = 14;
    const circumference = 2 * Math.PI * r;
    let svgPaths = '';
    let offset = 0;
    if (total === 0) {
      svgPaths = '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="var(--border)" stroke-width="' + stroke + '"/>';
    } else {
      segments.forEach(seg => {
        if (seg.value === 0) return;
        const pct = seg.value / total;
        const dash = pct * circumference;
        const gap = circumference - dash;
        svgPaths += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" '
          + 'stroke="' + seg.color + '" stroke-width="' + stroke + '" '
          + 'stroke-dasharray="' + dash + ' ' + gap + '" '
          + 'stroke-dashoffset="' + (-offset) + '" '
          + 'transform="rotate(-90 ' + cx + ' ' + cy + ')"/>';
        offset += dash;
      });
    }

    const svg = h('div');
    svg.innerHTML = '<svg class="donut-svg" viewBox="0 0 ' + size + ' ' + size + '">'
      + svgPaths
      + '<text x="' + cx + '" y="' + cy + '" text-anchor="middle" dominant-baseline="central">'
      + '<tspan class="donut-center-text" x="' + cx + '" dy="-6">' + escHtml(String(centerValue)) + '</tspan>'
      + '<tspan class="donut-center-sub" x="' + cx + '" dy="18">' + escHtml(String(centerSub)) + '</tspan>'
      + '</text></svg>';
    chart.appendChild(svg.firstChild);

    const legend = h('div', { className: 'donut-legend' });
    segments.forEach(seg => {
      const item = h('div', { className: 'donut-legend-item' });
      const swatch = h('div', { className: 'donut-legend-swatch' });
      swatch.style.background = seg.color;
      item.appendChild(swatch);
      item.appendChild(document.createTextNode(seg.value + ' ' + seg.label));
      legend.appendChild(item);
    });
    chart.appendChild(legend);
    return chart;
  }

  // ---- Compute "currently failed" pipelines (last run was a failure) ----
  function getCurrentlyFailed(runs, nameField) {
    const lastByPipeline = {};
    // runs are already sorted desc by started_at, so first occurrence is the latest
    runs.forEach(r => {
      const name = r[nameField || 'pipeline_name'];
      if (!lastByPipeline[name]) lastByPipeline[name] = r;
    });
    return Object.values(lastByPipeline).filter(r => r.status === 'failed');
  }

  // ---- Sortable table helpers ----
  function attachSortHandlers(tableWrap, draw) {
    $$('th[data-col]', tableWrap).forEach(th => {
      th.addEventListener('click', () => draw(th.dataset.col));
    });
  }

  function sortIndicator(col, sortCol, sortAsc) {
    const sorted = sortCol === col;
    const arrow = sorted ? (sortAsc ? '&#9650;' : '&#9660;') : '&#9650;';
    return (sorted ? 'sorted' : '') + '" data-col="' + col + '">' +
      ' <span class="sort-arrow">' + arrow + '</span>';
  }

  function genericSort(arr, sortCol, sortAsc, getVal) {
    arr.sort((a, b) => {
      let va = getVal(a, sortCol), vb = getVal(b, sortCol);
      if (va == null) va = '';
      if (vb == null) vb = '';
      if (typeof va === 'string') { va = va.toLowerCase(); vb = (vb || '').toLowerCase(); }
      return sortAsc ? (va > vb ? 1 : va < vb ? -1 : 0) : (va < vb ? 1 : va > vb ? -1 : 0);
    });
  }

  // ---- Filter helpers ----
  function addTagFilter(filters) {
    const sel = h('select', { className: 'filter-input' });
    sel.appendChild(h('option', { value: '' }, 'All tags'));
    allTags.forEach(t => sel.appendChild(h('option', { value: t }, t)));
    filters.appendChild(sel);
    return sel;
  }
  function addDispositionFilter(filters) {
    const sel = h('select', { className: 'filter-input' });
    sel.appendChild(h('option', { value: '' }, 'All dispositions'));
    allDispositions.forEach(d => sel.appendChild(h('option', { value: d }, d)));
    filters.appendChild(sel);
    return sel;
  }

  function matchesTag(pipelineName, tagFilter) {
    if (!tagFilter) return true;
    return tagsFor(pipelineName).includes(tagFilter);
  }
  function matchesDisposition(pipelineName, dispFilter) {
    if (!dispFilter) return true;
    return dispositionFor(pipelineName) === dispFilter;
  }

  // ---- Pagination helpers ----
  const PAGE_SIZE = 50;

  function renderPagination(container, currentPage, totalItems, onPageChange) {
    const totalPages = Math.max(1, Math.ceil(totalItems / PAGE_SIZE));
    if (totalItems <= PAGE_SIZE) { container.innerHTML = ''; return; }
    const start = currentPage * PAGE_SIZE + 1;
    const end = Math.min((currentPage + 1) * PAGE_SIZE, totalItems);

    const wrap = h('div', { className: 'pagination' });
    const prevBtn = h('button', { className: 'pagination-btn',
      onClick: () => { if (currentPage > 0) onPageChange(currentPage - 1); } }, 'Prev');
    const nextBtn = h('button', { className: 'pagination-btn',
      onClick: () => { if (currentPage < totalPages - 1) onPageChange(currentPage + 1); } }, 'Next');
    if (currentPage === 0) prevBtn.disabled = true;
    if (currentPage >= totalPages - 1) nextBtn.disabled = true;
    const info = h('span', { className: 'pagination-info' },
      start + '–' + end + ' of ' + totalItems);
    wrap.appendChild(prevBtn);
    wrap.appendChild(info);
    wrap.appendChild(nextBtn);
    container.innerHTML = '';
    container.appendChild(wrap);
  }

  function paginateRows(rows, page) {
    return rows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  }

  // ---- Clear filters helper ----
  function addClearFiltersButton(filtersDiv, inputs, onClear) {
    const btn = h('button', { className: 'clear-filters-btn', onClick: () => {
      inputs.forEach(el => { el.value = ''; });
      onClear();
    } }, 'Clear filters');
    filtersDiv.appendChild(btn);
    return btn;
  }
  function updateClearButton(btn, inputs) {
    const hasFilter = inputs.some(el => el.value !== '');
    btn.style.display = hasFilter ? '' : 'none';
  }

  // ---- Date filter helper ----
  function addDateFilter(filtersDiv) {
    const input = h('input', { className: 'filter-input filter-date', type: 'date' });
    filtersDiv.appendChild(input);
    return input;
  }
  function matchesDate(isoTimestamp, dateVal) {
    if (!dateVal) return true;
    if (!isoTimestamp) return false;
    const d = new Date(isoTimestamp);
    const local = d.getFullYear() + '-' +
      String(d.getMonth() + 1).padStart(2, '0') + '-' +
      String(d.getDate()).padStart(2, '0');
    return local === dateVal;
  }

  // ---- Dashboard ----
  function renderDashboard() {
    const panel = $('#tab-dashboard');
    const totalPipelines = D.pipelines.length;
    const groups = [...new Set(D.pipelines.map(p => p.pipeline_group))];

    // Ingest stats (merged: load_runs + failed orch tasks + skipped)
    const ingestSkipped = allIngestRuns.filter(r => r.status === 'skipped').length;
    const ingestFailed = allIngestRuns.filter(r => r.status === 'failed').length;
    const ingestSuccess = allIngestRuns.filter(r => r.status === 'success').length;
    const totalIngest = ingestSuccess + ingestFailed; // exclude skipped from the ratio
    const totalRows = allIngestRuns.reduce((s, r) => s + (r.row_count || 0), 0);

    // Historize stats
    const totalHist = D.historize_runs.length;
    const histFailed = D.historize_runs.filter(r => r.status === 'failed').length;
    const histSuccess = totalHist - histFailed;

    // Orchestration stats (execution-level)
    const totalExecs = execList.length;
    const failedExecs = execList.filter(e => e.failed > 0).length;
    const cleanExecs = totalExecs - failedExecs;

    // Pipelines without any runs
    const pipelinesWithRuns = new Set(allIngestRuns.map(r => r.pipeline_name)).size;
    const pipelinesNoRuns = totalPipelines - pipelinesWithRuns;

    panel.innerHTML = '';
    panel.appendChild(h('h2', { className: 'page-title' }, 'Dashboard'));

    // Report context banner
    const banner = h('div', { className: 'report-banner' });
    banner.innerHTML =
      '<span>Environment: <strong>' + escHtml(D.environment || 'unknown') + '</strong></span>' +
      '<span>Database: <strong>' + escHtml(D.project || 'unknown') + '</strong></span>' +
      '<span>Period: <strong>Last ' + D.days + ' days</strong></span>' +
      '<span>Generated: <strong>' + fmtDate(D.generated_at) + '</strong></span>';
    panel.appendChild(banner);

    // Consolidated KPI row
    const kpiGrid = h('div', { className: 'kpi-grid' });
    const kpis = [
      { label: 'Pipelines', value: totalPipelines,
        sub: groups.length + ' group(s), ' + pipelinesNoRuns + ' without runs' },
    ];
    if (totalExecs > 0) {
      kpis.push(
        { label: 'Successful Orchestrations', value: cleanExecs + ' / ' + totalExecs,
          sub: failedExecs > 0 ? failedExecs + ' with failures' : 'all clean',
          subColor: failedExecs > 0 ? 'var(--danger)' : null }
      );
    }
    kpis.push(
      { label: 'Successful Ingest Runs', value: ingestSuccess + ' / ' + totalIngest,
        sub: (ingestFailed > 0 ? ingestFailed + ' failed' : 'all succeeded') + (ingestSkipped > 0 ? ', ' + ingestSkipped + ' skipped' : ''),
        subColor: ingestFailed > 0 ? 'var(--danger)' : null },
      { label: 'Rows Loaded', value: fmtNum(totalRows), sub: 'across all ingest runs' },
      { label: 'Successful Historize Runs', value: histSuccess + ' / ' + totalHist,
        sub: histFailed > 0 ? histFailed + ' failed' : 'all succeeded',
        subColor: histFailed > 0 ? 'var(--danger)' : null }
    );
    kpis.forEach(k => {
      const subEl = h('div', { className: 'kpi-sub' }, k.sub);
      if (k.subColor) subEl.style.color = k.subColor;
      const card = h('div', { className: 'kpi-card' },
        h('div', { className: 'kpi-label' }, k.label),
        h('div', { className: 'kpi-value' }, String(k.value)),
        subEl
      );
      kpiGrid.appendChild(card);
    });
    panel.appendChild(kpiGrid);

    // Currently failed pipelines (last run was a failure)
    const currentIngestFails = getCurrentlyFailed(allIngestRuns);
    const currentHistFails = getCurrentlyFailed(D.historize_runs);
    const currentlyFailedCount = new Set([
      ...currentIngestFails.map(r => r.pipeline_name),
      ...currentHistFails.map(r => r.pipeline_name),
    ]).size;
    const healthyCount = totalPipelines - currentlyFailedCount;

    // Stacked activity charts with pipeline state donut
    const chartSection = h('div', { className: 'section' });
    chartSection.appendChild(h('div', { className: 'section-header' }, 'Activity (Last 14 Days)'));
    const chartContainer = h('div', { className: 'chart-container' });
    const days14 = buildDayBuckets(14);

    // Pipeline state donut chart
    chartContainer.appendChild(buildDonutChart('Pipeline State', [
      { value: healthyCount, color: 'var(--success)', label: 'passing' },
      { value: currentlyFailedCount, color: 'var(--danger)', label: 'failing' },
    ], currentlyFailedCount > 0 ? currentlyFailedCount : totalPipelines,
       currentlyFailedCount > 0 ? 'failing' : 'all passing'));

    // Orchestration: clean vs failed executions per day
    if (totalExecs > 0) {
      const orchCleanByDay = new Array(14).fill(0);
      const orchFailByDay = new Array(14).fill(0);
      execList.forEach(e => {
        if (!e.timestamp) return;
        const rd = new Date(e.timestamp);
        rd.setHours(0, 0, 0, 0);
        const idx = days14.findIndex(d => d.getTime() === rd.getTime());
        if (idx >= 0) {
          if (e.failed > 0) orchFailByDay[idx]++;
          else orchCleanByDay[idx]++;
        }
      });
      chartContainer.appendChild(buildStackedBarChart('Orchestration Runs', days14,
        orchCleanByDay, orchFailByDay, 'var(--success)', 'var(--danger)', date => {
          pendingOrchDateFilter = date;
          showTab('orchestration');
          renderOrchestration();
        }));
    }

    // Ingest: success vs failed
    const ingestSuccessRuns = allIngestRuns.filter(r => r.status === 'success');
    const ingestFailedRuns = allIngestRuns.filter(r => r.status === 'failed');
    chartContainer.appendChild(buildStackedBarChart('Ingest Runs', days14,
      bucketByDay(ingestSuccessRuns, days14), bucketByDay(ingestFailedRuns, days14),
      'var(--success)', 'var(--danger)', date => {
        pendingIngestDateFilter = date;
        showTab('ingest-runs');
        renderIngestRuns();
      }));

    // Historize: completed vs failed
    chartContainer.appendChild(buildStackedBarChart('Historize Runs', days14,
      bucketByDay(D.historize_runs.filter(r => r.status === 'completed'), days14),
      bucketByDay(D.historize_runs.filter(r => r.status === 'failed'), days14),
      'var(--success)', 'var(--danger)', date => {
        pendingHistorizeDateFilter = date;
        showTab('historize-runs');
        renderHistorizeRuns();
      }));

    chartSection.appendChild(chartContainer);
    panel.appendChild(chartSection);

    // Pipeline groups breakdown
    const groupSection = h('div', { className: 'section' });
    groupSection.appendChild(h('div', { className: 'section-header' }, 'Pipeline Groups'));
    const groupTable = h('div', { className: 'table-wrap' });
    let gtHTML = '<table><tr><th>Group</th><th>Pipelines</th><th>Ingest</th><th>Historize</th><th>Tags</th></tr>';
    groups.sort().forEach(g => {
      const gp = D.pipelines.filter(p => p.pipeline_group === g);
      gtHTML += '<tr><td><strong>' + g + '</strong></td><td>' + gp.length +
        '</td><td>' + gp.filter(p=>p.ingest_enabled).length +
        '</td><td>' + gp.filter(p=>p.historize_enabled).length +
        '</td><td>' + tagBadges([...new Set(gp.flatMap(p=>p.tags))]) + '</td></tr>';
    });
    gtHTML += '</table>';
    groupTable.innerHTML = gtHTML;
    groupSection.appendChild(groupTable);
    panel.appendChild(groupSection);

    // Currently failing pipelines (last run was a failure)
    if (currentIngestFails.length) {
      const failSection = h('div', { className: 'section' });
      failSection.appendChild(h('div', { className: 'section-header' }, 'Currently Failing — Ingest'));
      const failTable = h('div', { className: 'table-wrap' });
      let ftHTML = '<table><tr><th>Pipeline</th><th>Table</th><th>Last Run</th><th>Error</th><th>Status</th></tr>';
      currentIngestFails.forEach(r => {
        const errMsg = r.error_message ? escHtml(r.error_message.substring(0, 120)) : '-';
        ftHTML += '<tr><td><span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' +
          escHtml(r.pipeline_name) + '</span></td><td>' + escHtml(r.table_name) +
          '</td><td>' + fmtDate(r.started_at) + '</td><td>' + errMsg +
          '</td><td>' + statusBadge(r.status) + '</td></tr>';
      });
      ftHTML += '</table>';
      failTable.innerHTML = ftHTML;
      failSection.appendChild(failTable);
      panel.appendChild(failSection);
    }

    if (currentHistFails.length) {
      const failSection = h('div', { className: 'section' });
      failSection.appendChild(h('div', { className: 'section-header' }, 'Currently Failing — Historize'));
      const failTable = h('div', { className: 'table-wrap' });
      let ftHTML = '<table><tr><th>Pipeline</th><th>Target</th><th>Snapshot</th><th>Last Run</th><th>Status</th></tr>';
      currentHistFails.forEach(r => {
        ftHTML += '<tr><td><span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' +
          escHtml(r.pipeline_name) + '</span></td><td>' + escHtml(r.target_table) +
          '</td><td>' + escHtml(r.snapshot_value) +
          '</td><td>' + fmtDate(r.started_at) + '</td><td>' + statusBadge(r.status) + '</td></tr>';
      });
      ftHTML += '</table>';
      failTable.innerHTML = ftHTML;
      failSection.appendChild(failTable);
      panel.appendChild(failSection);
    }
  }

  // ---- Pipeline Detail view ----
  function showPipelineDetail(pipelineName) {
    const panel = $('#tab-pipeline-detail');
    panel.innerHTML = '';
    showTab('pipeline-detail', { pipeline: pipelineName });

    const p = pipelineMap[pipelineName];

    // Header with back button
    const titleRow = h('div', { className: 'page-title-row' });
    const backBtn = h('button', { className: 'back-btn', onClick: () => showTab(previousTab) }, 'Back');
    titleRow.appendChild(backBtn);
    titleRow.appendChild(h('h2', { className: 'page-title' }, pipelineName));
    panel.appendChild(titleRow);

    if (p) {
      const infoGrid = h('div', { className: 'kpi-grid' });
      [
        { label: 'Group', value: p.pipeline_group },
        { label: 'Write Disposition', value: p.write_disposition },
        { label: 'Table', value: (p.schema_name ? p.schema_name + '.' : '') + (p.table_name || '') },
        { label: 'Implementation', value: p.adapter || 'default' },
      ].forEach(k => {
        infoGrid.appendChild(h('div', { className: 'kpi-card' },
          h('div', { className: 'kpi-label' }, k.label),
          h('div', { className: 'kpi-value-sm' }, k.value),
        ));
      });
      panel.appendChild(infoGrid);

      if (p.tags.length) {
        const tagDiv = h('div', { innerHTML: tagBadges(p.tags) });
        tagDiv.style.marginBottom = '24px';
        panel.appendChild(tagDiv);
      }
    }

    const pRuns = allIngestRuns.filter(r => r.pipeline_name === pipelineName);
    const pHistRuns = D.historize_runs.filter(r => r.pipeline_name === pipelineName);

    // Stats
    const statGrid = h('div', { className: 'kpi-grid' });
    const pSuccess = pRuns.filter(r => r.status === 'success');
    const totalIngestRows = pSuccess.reduce((s,r) => s + (r.row_count||0), 0);
    const avgDuration = pSuccess.length ? (pSuccess.reduce((s,r) => s + (r.duration_seconds||0), 0) / pSuccess.length) : 0;
    const pFailed = pRuns.filter(r => r.status === 'failed').length;
    const histFailCount = pHistRuns.filter(r => r.status === 'failed').length;
    [
      { label: 'Successful Ingest Runs', value: pSuccess.length + ' / ' + (pSuccess.length + pFailed) },
      { label: 'Total Rows Loaded', value: fmtNum(totalIngestRows) },
      { label: 'Avg Duration', value: fmtDuration(avgDuration) },
      { label: 'Historize Runs', value: pHistRuns.length + (histFailCount > 0 ? ' (' + histFailCount + ' failed)' : '') },
    ].forEach(k => {
      statGrid.appendChild(h('div', { className: 'kpi-card' },
        h('div', { className: 'kpi-label' }, k.label),
        h('div', { className: 'kpi-value-sm' }, String(k.value)),
      ));
    });
    panel.appendChild(statGrid);

    // Row count chart over time
    if (pSuccess.length > 1) {
      const chartSection = h('div', { className: 'section' });
      chartSection.appendChild(h('div', { className: 'section-header' }, 'Rows Per Run (Ingest)'));
      const chartContainer = h('div', { className: 'chart-container' });

      const recent = pSuccess.slice().sort((a,b) => (a.started_at||'') > (b.started_at||'') ? 1 : -1).slice(-30);
      const maxRows = Math.max(...recent.map(r => r.row_count || 0), 1);

      const chart = h('div', { className: 'mini-chart detail-chart' });
      chart.appendChild(h('h3', null, 'Rows loaded per run'));
      const bars = h('div', { className: 'bar-chart' });
      const barLabels = h('div', { className: 'bar-labels' });
      recent.forEach((r, i) => {
        const pct = ((r.row_count || 0) / maxRows) * 100;
        const bar = h('div', { className: 'bar' });
        bar.style.height = Math.max(pct, 2) + '%';
        bar.style.background = 'var(--accent)';
        bar.appendChild(h('div', { className: 'bar-tooltip' },
          fmtDate(r.started_at) + ': ' + fmtNum(r.row_count) + ' rows'));
        bars.appendChild(bar);
        barLabels.appendChild(h('span', null, i % 3 === 0 ? fmtDateShort(r.started_at) : ''));
      });
      chart.appendChild(bars);
      chart.appendChild(barLabels);
      chartContainer.appendChild(chart);

      const durChart = h('div', { className: 'mini-chart detail-chart' });
      durChart.appendChild(h('h3', null, 'Duration per run'));
      const maxDur = Math.max(...recent.map(r => r.duration_seconds || 0), 1);
      const durBars = h('div', { className: 'bar-chart' });
      const durLabels = h('div', { className: 'bar-labels' });
      recent.forEach((r, i) => {
        const pct = ((r.duration_seconds || 0) / maxDur) * 100;
        const bar = h('div', { className: 'bar' });
        bar.style.height = Math.max(pct, 2) + '%';
        bar.style.background = 'var(--info)';
        bar.appendChild(h('div', { className: 'bar-tooltip' },
          fmtDate(r.started_at) + ': ' + fmtDuration(r.duration_seconds)));
        durBars.appendChild(bar);
        durLabels.appendChild(h('span', null, i % 3 === 0 ? fmtDateShort(r.started_at) : ''));
      });
      durChart.appendChild(durBars);
      durChart.appendChild(durLabels);
      chartContainer.appendChild(durChart);

      chartSection.appendChild(chartContainer);
      panel.appendChild(chartSection);
    }

    // Recent ingest runs table
    if (pRuns.length) {
      const runSection = h('div', { className: 'section' });
      runSection.appendChild(h('div', { className: 'section-header' }, 'Ingest Run History'));
      const tw = h('div', { className: 'table-wrap' });
      let html = '<table><tr><th>Schema</th><th>Table</th><th>Rows</th><th>Started</th><th>Duration</th><th>Status</th></tr>';
      pRuns.slice(0, 100).forEach(r => {
        html += '<tr><td>' + escHtml(r.dataset_name) + '</td><td>' + escHtml(r.table_name) +
          '</td><td>' + fmtNum(r.row_count) + '</td><td>' + fmtDate(r.started_at) +
          '</td><td>' + fmtDuration(r.duration_seconds) + '</td><td>' + statusBadge(r.status) + '</td></tr>';
      });
      html += '</table>';
      tw.innerHTML = html;
      runSection.appendChild(tw);
      panel.appendChild(runSection);
    }

    // Historize runs table
    if (pHistRuns.length) {
      const histSection = h('div', { className: 'section' });
      histSection.appendChild(h('div', { className: 'section-header' }, 'Historize Run History'));
      const tw = h('div', { className: 'table-wrap' });
      let html = '<table><tr><th>Target</th><th>Snapshot</th><th>Changed</th><th>Deleted</th><th>Started</th><th>Duration</th><th>Status</th></tr>';
      pHistRuns.slice(0, 100).forEach(r => {
        html += '<tr><td>' + escHtml(r.target_table) + '</td><td>' + escHtml(r.snapshot_value) +
          '</td><td>' + fmtNum(r.new_or_changed_rows) + '</td><td>' + fmtNum(r.deleted_rows) +
          '</td><td>' + fmtDate(r.started_at) + '</td><td>' + fmtDuration(r.duration_seconds) +
          '</td><td>' + statusBadge(r.status) + '</td></tr>';
      });
      html += '</table>';
      tw.innerHTML = html;
      histSection.appendChild(tw);
      panel.appendChild(histSection);
    }

    if (!pRuns.length && !pHistRuns.length) {
      panel.appendChild(h('div', { className: 'section' },
        h('div', { className: 'empty-state' },
          h('h3', null, 'No runs recorded'),
          h('p', null, 'This pipeline has no ingest or historize run data in the selected time window.')
        )
      ));
    }
  }

  // Global click handler for pipeline links and execution links
  document.addEventListener('click', e => {
    const link = e.target.closest('.pipeline-link');
    if (link) { e.preventDefault(); showPipelineDetail(link.dataset.pipeline); return; }
    const execLink = e.target.closest('.exec-link');
    if (execLink) {
      e.preventDefault();
      pendingIngestExecFilter = execLink.dataset.execId;
      showTab('ingest-runs');
      renderIngestRuns();
    }
  });

  // ---- Pipelines tab ----
  function renderPipelines() {
    const panel = $('#tab-pipelines');
    panel.innerHTML = '';
    panel.appendChild(h('h2', { className: 'page-title' }, 'Pipeline Catalog'));

    const section = h('div', { className: 'section' });
    const filters = h('div', { className: 'filters' });
    const searchInput = h('input', { className: 'filter-input', type: 'text', placeholder: 'Search pipelines...' });
    const groupSelect = h('select', { className: 'filter-input' });
    groupSelect.appendChild(h('option', { value: '' }, 'All groups'));
    [...new Set(D.pipelines.map(p => p.pipeline_group))].sort().forEach(g => {
      groupSelect.appendChild(h('option', { value: g }, g));
    });
    const tagSelect = addTagFilter(filters);
    const dispSelect = addDispositionFilter(filters);
    filters.insertBefore(groupSelect, tagSelect);
    filters.insertBefore(searchInput, groupSelect);
    section.appendChild(filters);

    const filterInputs = [searchInput, groupSelect, tagSelect, dispSelect];
    const clearBtn = addClearFiltersButton(filters, filterInputs, () => { pipelinesPage = 0; draw(); });

    const tableWrap = h('div', { className: 'table-wrap' });
    const paginationWrap = h('div');
    section.appendChild(tableWrap);
    section.appendChild(paginationWrap);
    panel.appendChild(section);

    const lastRun = {};
    allIngestRuns.forEach(r => {
      if (!lastRun[r.pipeline_name] || r.started_at > lastRun[r.pipeline_name].started_at)
        lastRun[r.pipeline_name] = r;
    });

    let sortCol = 'pipeline_name', sortAsc = true;
    let pipelinesPage = 0;

    function draw(clickedCol) {
      if (clickedCol) {
        if (sortCol === clickedCol) sortAsc = !sortAsc;
        else { sortCol = clickedCol; sortAsc = true; }
        pipelinesPage = 0;
      }
      const query = searchInput.value.toLowerCase();
      const groupFilter = groupSelect.value;
      const tagFilter = tagSelect.value;
      const dispFilter = dispSelect.value;

      let filtered = D.pipelines.filter(p => {
        if (query && !p.pipeline_name.toLowerCase().includes(query) &&
            !p.tags.some(t => t.toLowerCase().includes(query))) return false;
        if (groupFilter && p.pipeline_group !== groupFilter) return false;
        if (tagFilter && !p.tags.includes(tagFilter)) return false;
        if (dispFilter && p.write_disposition !== dispFilter) return false;
        return true;
      });

      genericSort(filtered, sortCol, sortAsc, (item, col) => {
        if (col === 'last_run') return lastRun[item.pipeline_name] ? lastRun[item.pipeline_name].started_at || '' : '';
        if (col === 'last_rows') return lastRun[item.pipeline_name] ? lastRun[item.pipeline_name].row_count || 0 : 0;
        return item[col] || '';
      });

      const cols = [
        { key: 'pipeline_name', label: 'Pipeline' },
        { key: 'pipeline_group', label: 'Group' },
        { key: 'write_disposition', label: 'Disposition' },
        { key: 'tags', label: 'Tags', nosort: true },
        { key: 'last_run', label: 'Last Run' },
        { key: 'last_rows', label: 'Last Rows' },
      ];

      let html = '<table><tr>';
      cols.forEach(c => {
        if (c.nosort) { html += '<th>' + c.label + '</th>'; return; }
        html += '<th class="' + sortIndicator(c.key, sortCol, sortAsc) + c.label + '</th>';
      });
      html += '</tr>';

      if (!filtered.length) {
        html += '<tr><td colspan="' + cols.length + '"><div class="empty-state"><h3>No pipelines match</h3></div></td></tr>';
      }

      paginateRows(filtered, pipelinesPage).forEach(p => {
        const lr = lastRun[p.pipeline_name];
        const caps = [];
        if (p.ingest_enabled) caps.push('<span class="badge badge-info">ingest</span>');
        if (p.historize_enabled) caps.push('<span class="badge badge-success">historize</span>');

        html += '<tr>';
        html += '<td><span class="pipeline-link" data-pipeline="' + escHtml(p.pipeline_name) + '">' + escHtml(p.pipeline_name) + '</span></td>';
        html += '<td>' + p.pipeline_group + '</td>';
        html += '<td>' + p.write_disposition + ' ' + caps.join(' ') + '</td>';
        html += '<td>' + tagBadges(p.tags) + '</td>';
        html += '<td>' + (lr ? fmtDate(lr.started_at) : '<span class="badge badge-neutral">no runs</span>') + '</td>';
        html += '<td>' + (lr ? fmtNum(lr.row_count) : '-') + '</td>';
        html += '</tr>';
      });
      html += '</table>';
      tableWrap.innerHTML = html;
      attachSortHandlers(tableWrap, draw);
      renderPagination(paginationWrap, pipelinesPage, filtered.length, p => { pipelinesPage = p; draw(); });
      updateClearButton(clearBtn, filterInputs);
    }

    filterInputs.forEach(el => {
      el.addEventListener(el.tagName === 'INPUT' ? 'input' : 'change', () => { pipelinesPage = 0; draw(); });
    });
    draw();
  }

  // ---- Ingest Runs tab ----
  function renderIngestRuns() {
    const panel = $('#tab-ingest-runs');
    panel.innerHTML = '';
    panel.appendChild(h('h2', { className: 'page-title' }, 'Ingest Runs'));

    const section = h('div', { className: 'section' });
    const filters = h('div', { className: 'filters' });
    const searchInput = h('input', { className: 'filter-input', type: 'text', placeholder: 'Filter by pipeline...' });
    const datasetSelect = h('select', { className: 'filter-input' });
    datasetSelect.appendChild(h('option', { value: '' }, 'All schemas'));
    [...new Set(allIngestRuns.map(r => r.dataset_name).filter(Boolean))].sort().forEach(d => {
      datasetSelect.appendChild(h('option', { value: d }, d));
    });
    const statusSelect = h('select', { className: 'filter-input' });
    statusSelect.appendChild(h('option', { value: '' }, 'All statuses'));
    ['success', 'failed', 'skipped'].forEach(s => statusSelect.appendChild(h('option', { value: s }, s)));
    const execSelect = h('select', { className: 'filter-input' });
    execSelect.appendChild(h('option', { value: '' }, 'All executions'));
    allExecIds.forEach(id => {
      execSelect.appendChild(h('option', { value: id }, id.substring(0, 8) + '...'));
    });
    const tagSelect = addTagFilter(filters);
    const dispSelect = addDispositionFilter(filters);
    // Order: search, status, tag, schema, disposition, execution
    filters.insertBefore(datasetSelect, dispSelect);
    filters.insertBefore(tagSelect, datasetSelect);
    filters.insertBefore(statusSelect, tagSelect);
    filters.insertBefore(searchInput, statusSelect);
    filters.appendChild(execSelect);
    const ingestDateInput = addDateFilter(filters);
    filters.insertBefore(ingestDateInput, filters.firstChild);
    const ingestFilterInputs = [searchInput, datasetSelect, statusSelect, execSelect, tagSelect, dispSelect, ingestDateInput];
    const ingestClearBtn = addClearFiltersButton(filters, ingestFilterInputs, () => { ingestPage = 0; draw(); });
    section.appendChild(filters);

    // Apply pending filters from cross-tab navigation
    if (pendingIngestExecFilter) {
      execSelect.value = pendingIngestExecFilter;
      pendingIngestExecFilter = null;
    }
    if (pendingIngestDateFilter) {
      ingestDateInput.value = pendingIngestDateFilter;
      pendingIngestDateFilter = null;
    }

    const tableWrap = h('div', { className: 'table-wrap' });
    const paginationWrap = h('div');
    section.appendChild(tableWrap);
    section.appendChild(paginationWrap);
    panel.appendChild(section);

    let sortCol = 'started_at', sortAsc = false;
    let ingestPage = 0;

    function draw(clickedCol) {
      if (clickedCol) {
        if (sortCol === clickedCol) sortAsc = !sortAsc;
        else { sortCol = clickedCol; sortAsc = true; }
        ingestPage = 0;
      }
      const query = searchInput.value.toLowerCase();
      const dsFilter = datasetSelect.value;
      const statusFilter = statusSelect.value;
      const execFilter = execSelect.value;
      const tagFilter = tagSelect.value;
      const dispFilter = dispSelect.value;
      const dateFilter = ingestDateInput.value;

      let filtered = allIngestRuns.filter(r => {
        if (query && !r.pipeline_name.toLowerCase().includes(query) &&
            !r.table_name.toLowerCase().includes(query)) return false;
        if (dsFilter && r.dataset_name !== dsFilter) return false;
        if (statusFilter && r.status !== statusFilter) return false;
        if (execFilter && r.execution_id !== execFilter) return false;
        if (!matchesTag(r.pipeline_name, tagFilter)) return false;
        if (!matchesDisposition(r.pipeline_name, dispFilter)) return false;
        if (!matchesDate(r.started_at, dateFilter)) return false;
        return true;
      });

      genericSort(filtered, sortCol, sortAsc, (item, col) => item[col]);

      const cols = [
        { key: 'pipeline_name', label: 'Pipeline' },
        { key: 'dataset_name', label: 'Schema' },
        { key: 'table_name', label: 'Table' },
        { key: 'row_count', label: 'Rows' },
        { key: 'started_at', label: 'Started' },
        { key: 'duration_seconds', label: 'Duration' },
        { key: 'status', label: 'Status' },
        { key: 'error_message', label: 'Error' },
      ];

      let html = '<table><tr>';
      cols.forEach(c => { html += '<th class="' + sortIndicator(c.key, sortCol, sortAsc) + c.label + '</th>'; });
      html += '</tr>';

      if (!filtered.length) {
        html += '<tr><td colspan="' + cols.length + '"><div class="empty-state"><h3>No ingest runs found</h3><p>Run pipelines with <code>saga ingest</code> to generate data.</p></div></td></tr>';
      }

      paginateRows(filtered, ingestPage).forEach(r => {
        const errMsg = r.error_message ? escHtml(r.error_message.substring(0, 100)) : '-';
        html += '<tr>';
        html += '<td><span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' + escHtml(r.pipeline_name) + '</span></td>';
        html += '<td>' + escHtml(r.dataset_name || '') + '</td>';
        html += '<td>' + escHtml(r.table_name) + '</td>';
        html += '<td>' + fmtNum(r.row_count) + '</td>';
        html += '<td>' + fmtDate(r.started_at) + '</td>';
        html += '<td>' + fmtDuration(r.duration_seconds) + '</td>';
        html += '<td>' + statusBadge(r.status) + '</td>';
        html += '<td>' + errMsg + '</td>';
        html += '</tr>';
      });
      html += '</table>';
      tableWrap.innerHTML = html;
      attachSortHandlers(tableWrap, draw);
      renderPagination(paginationWrap, ingestPage, filtered.length, p => { ingestPage = p; draw(); });
      updateClearButton(ingestClearBtn, ingestFilterInputs);
    }

    ingestFilterInputs.forEach(el => {
      el.addEventListener(el.tagName === 'INPUT' ? 'input' : 'change', () => { ingestPage = 0; draw(); });
    });
    draw();
  }

  // ---- Historize Runs tab ----
  function renderHistorizeRuns() {
    const panel = $('#tab-historize-runs');
    panel.innerHTML = '';
    panel.appendChild(h('h2', { className: 'page-title' }, 'Historize Runs'));

    const section = h('div', { className: 'section' });
    const filters = h('div', { className: 'filters' });
    const searchInput = h('input', { className: 'filter-input', type: 'text', placeholder: 'Filter by pipeline...' });
    const statusSelect = h('select', { className: 'filter-input' });
    statusSelect.appendChild(h('option', { value: '' }, 'All statuses'));
    ['completed', 'failed'].forEach(s => statusSelect.appendChild(h('option', { value: s }, s)));
    const tagSelect = addTagFilter(filters);
    filters.insertBefore(statusSelect, tagSelect);
    filters.insertBefore(searchInput, statusSelect);
    const histDateInput = addDateFilter(filters);
    filters.insertBefore(histDateInput, filters.firstChild);
    const histFilterInputs = [searchInput, statusSelect, tagSelect, histDateInput];
    const histClearBtn = addClearFiltersButton(filters, histFilterInputs, () => { histPage = 0; draw(); });
    section.appendChild(filters);

    // Apply pending date filter from cross-tab navigation
    if (pendingHistorizeDateFilter) {
      histDateInput.value = pendingHistorizeDateFilter;
      pendingHistorizeDateFilter = null;
    }

    const tableWrap = h('div', { className: 'table-wrap' });
    const paginationWrap = h('div');
    section.appendChild(tableWrap);
    section.appendChild(paginationWrap);
    panel.appendChild(section);

    let sortCol = 'started_at', sortAsc = false;
    let histPage = 0;

    function draw(clickedCol) {
      if (clickedCol) {
        if (sortCol === clickedCol) sortAsc = !sortAsc;
        else { sortCol = clickedCol; sortAsc = true; }
        histPage = 0;
      }
      const query = searchInput.value.toLowerCase();
      const statusFilter = statusSelect.value;
      const tagFilter = tagSelect.value;
      const dateFilter = histDateInput.value;

      let filtered = D.historize_runs.filter(r => {
        if (query && !r.pipeline_name.toLowerCase().includes(query)) return false;
        if (statusFilter && r.status !== statusFilter) return false;
        if (!matchesTag(r.pipeline_name, tagFilter)) return false;
        if (!matchesDate(r.started_at, dateFilter)) return false;
        return true;
      });

      genericSort(filtered, sortCol, sortAsc, (item, col) => item[col]);

      const cols = [
        { key: 'pipeline_name', label: 'Pipeline' },
        { key: 'source_table', label: 'Source' },
        { key: 'target_table', label: 'Target' },
        { key: 'snapshot_value', label: 'Snapshot' },
        { key: 'new_or_changed_rows', label: 'Changed' },
        { key: 'deleted_rows', label: 'Deleted' },
        { key: 'started_at', label: 'Started' },
        { key: 'duration_seconds', label: 'Duration' },
        { key: 'status', label: 'Status' },
      ];

      let html = '<table><tr>';
      cols.forEach(c => { html += '<th class="' + sortIndicator(c.key, sortCol, sortAsc) + c.label + '</th>'; });
      html += '</tr>';

      if (!filtered.length) {
        html += '<tr><td colspan="' + cols.length + '"><div class="empty-state"><h3>No historize runs found</h3><p>Run pipelines with <code>saga historize</code> to generate data.</p></div></td></tr>';
      }

      paginateRows(filtered, histPage).forEach(r => {
        html += '<tr>';
        html += '<td><span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' + escHtml(r.pipeline_name) + '</span></td>';
        html += '<td>' + escHtml(r.source_table) + '</td>';
        html += '<td>' + escHtml(r.target_table) + '</td>';
        html += '<td>' + escHtml(r.snapshot_value) + '</td>';
        html += '<td>' + fmtNum(r.new_or_changed_rows) + '</td>';
        html += '<td>' + fmtNum(r.deleted_rows) + '</td>';
        html += '<td>' + fmtDate(r.started_at) + '</td>';
        html += '<td>' + fmtDuration(r.duration_seconds) + '</td>';
        html += '<td>' + statusBadge(r.status) + '</td>';
        html += '</tr>';
      });
      html += '</table>';
      tableWrap.innerHTML = html;
      attachSortHandlers(tableWrap, draw);
      renderPagination(paginationWrap, histPage, filtered.length, p => { histPage = p; draw(); });
      updateClearButton(histClearBtn, histFilterInputs);
    }

    histFilterInputs.forEach(el => {
      el.addEventListener(el.tagName === 'INPUT' ? 'input' : 'change', () => { histPage = 0; draw(); });
    });
    draw();
  }

  // ---- Orchestration tab ----
  function renderOrchestration() {
    const panel = $('#tab-orchestration');
    panel.innerHTML = '';
    panel.appendChild(h('h2', { className: 'page-title' }, 'Orchestration Runs'));

    if (!orchRuns.length) {
      panel.appendChild(h('div', { className: 'section' },
        h('div', { className: 'empty-state' },
          h('h3', null, 'No orchestration data'),
          h('p', null, 'No orchestration runs found. Orchestration data is recorded when using saga ingest --orchestrate.')
        )
      ));
      return;
    }

    // Collect unique filter values from execution metadata
    const allCriteria = [...new Set(
      execList.map(e => e.meta && e.meta.select_criteria).filter(Boolean)
    )].sort();
    const allCommands = [...new Set(
      execList.map(e => e.meta && e.meta.command).filter(Boolean)
    )].sort();

    // Execution summary section
    const summarySection = h('div', { className: 'section' });
    summarySection.appendChild(h('div', { className: 'section-header' }, 'Execution Summary'));
    const filters = h('div', { className: 'filters' });
    const statusSelect = h('select', { className: 'filter-input' });
    statusSelect.appendChild(h('option', { value: '' }, 'All executions'));
    statusSelect.appendChild(h('option', { value: 'failed' }, 'With failures'));
    statusSelect.appendChild(h('option', { value: 'clean' }, 'Clean only'));
    filters.appendChild(statusSelect);

    const commandSelect = h('select', { className: 'filter-input' });
    commandSelect.appendChild(h('option', { value: '' }, 'All commands'));
    allCommands.forEach(c => commandSelect.appendChild(h('option', { value: c }, c)));
    filters.appendChild(commandSelect);

    const criteriaSelect = h('select', { className: 'filter-input' });
    criteriaSelect.appendChild(h('option', { value: '' }, 'All selectors'));
    allCriteria.forEach(c => criteriaSelect.appendChild(h('option', { value: c }, c)));
    filters.appendChild(criteriaSelect);
    const orchDateInput = addDateFilter(filters);
    filters.insertBefore(orchDateInput, filters.firstChild);
    const orchFilterInputs = [statusSelect, commandSelect, criteriaSelect, orchDateInput];
    const orchClearBtn = addClearFiltersButton(filters, orchFilterInputs, () => { orchPage = 0; drawSummary(); });

    summarySection.appendChild(filters);

    // Apply pending date filter from cross-tab navigation
    if (pendingOrchDateFilter) {
      orchDateInput.value = pendingOrchDateFilter;
      pendingOrchDateFilter = null;
    }

    const summaryTable = h('div', { className: 'table-wrap' });
    const orchPaginationWrap = h('div');
    summarySection.appendChild(summaryTable);
    summarySection.appendChild(orchPaginationWrap);
    panel.appendChild(summarySection);

    let orchPage = 0;

    function drawSummary() {
      const filter = statusSelect.value;
      const commandFilter = commandSelect.value;
      const criteriaFilter = criteriaSelect.value;
      const dateFilter = orchDateInput.value;
      let filtered = execList;
      if (filter === 'failed') filtered = filtered.filter(e => e.failed > 0);
      if (filter === 'clean') filtered = filtered.filter(e => e.failed === 0);
      if (commandFilter) {
        filtered = filtered.filter(e =>
          e.meta && e.meta.command === commandFilter
        );
      }
      if (criteriaFilter) {
        filtered = filtered.filter(e =>
          e.meta && e.meta.select_criteria === criteriaFilter
        );
      }
      if (dateFilter) {
        filtered = filtered.filter(e => {
          const ts = e.meta ? e.meta.created_at : e.timestamp;
          return matchesDate(ts, dateFilter);
        });
      }

      const hasMeta = filtered.some(e => e.meta);
      const cols = ['Execution ID', 'Time', 'Command', 'Select', 'Tasks', 'Completed', 'Failed', 'Duration', 'Status'];
      let html = '<table><tr>';
      cols.forEach(c => { html += '<th>' + c + '</th>'; });
      html += '</tr>';
      if (!filtered.length) {
        html += '<tr><td colspan="' + cols.length + '"><div class="empty-state"><h3>No executions match</h3></div></td></tr>';
      }
      paginateRows(filtered, orchPage).forEach(e => {
        const shortId = e.execution_id.substring(0, 8);
        const status = e.failed > 0 ? 'failed' : e.running > 0 ? 'running' : e.pending > 0 ? 'pending' : 'completed';
        const m = e.meta;
        html += '<tr>';
        html += '<td><span class="exec-link" data-exec-id="' + escHtml(e.execution_id) + '" style="color:var(--accent-light);cursor:pointer;font-weight:600"><code>' + escHtml(shortId) + '</code></span></td>';
        html += '<td>' + fmtDate(m ? m.created_at : e.timestamp) + '</td>';
        html += '<td>' + (m ? escHtml(m.command) : '-') + '</td>';
        html += '<td>' + (m && m.select_criteria ? '<code>' + escHtml(m.select_criteria) + '</code>' : '<span style="color:var(--text-muted)">all</span>') + '</td>';
        html += '<td>' + e.total + '</td>';
        html += '<td>' + e.completed + '</td>';
        html += '<td>' + (e.failed > 0 ? '<strong style="color:var(--danger)">' + e.failed + '</strong>' : '0') + '</td>';
        html += '<td>' + fmtDuration(e.duration) + '</td>';
        html += '<td>' + statusBadge(status) + '</td></tr>';
      });
      html += '</table>';
      summaryTable.innerHTML = html;
      renderPagination(orchPaginationWrap, orchPage, filtered.length, p => { orchPage = p; drawSummary(); });
      updateClearButton(orchClearBtn, orchFilterInputs);
    }

    orchFilterInputs.forEach(el =>
      el.addEventListener('change', () => { orchPage = 0; drawSummary(); })
    );
    drawSummary();
  }

  // ---- Init ----
  renderDashboard();
  renderOrchestration();
  renderIngestRuns();
  renderHistorizeRuns();
  renderPipelines();

  // Theme toggle
  function updateThemeToggle() {
    const btn = document.getElementById('theme-toggle');
    if (!btn) return;
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    btn.textContent = isDark ? '☀ Light' : '⏾ Dark';
  }
  const themeToggleBtn = document.getElementById('theme-toggle');
  if (themeToggleBtn) {
    themeToggleBtn.addEventListener('click', () => {
      const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
      if (isDark) {
        document.documentElement.removeAttribute('data-theme');
        localStorage.setItem('saga-report-theme', 'light');
      } else {
        document.documentElement.setAttribute('data-theme', 'dark');
        localStorage.setItem('saga-report-theme', 'dark');
      }
      updateThemeToggle();
    });
  }
  updateThemeToggle();

  // Set initial history state and restore from hash if present
  const initHash = location.hash.slice(1);
  history.replaceState({ tab: 'dashboard' }, '', '#dashboard');
  if (initHash && initHash !== 'dashboard') {
    const parts = initHash.split('/');
    const tab = parts[0];
    if (tab === 'pipeline-detail' && parts[1]) {
      showPipelineDetail(decodeURIComponent(parts[1]));
    } else if ($('#tab-' + tab)) {
      showTab(tab);
    }
  }

})();
