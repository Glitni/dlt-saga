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

  // Expandable error cell: short messages render inline; long ones collapse
  // into a <details> the reader can expand to see the full text.
  const ERR_PREVIEW = 80;
  function errorCell(msg) {
    if (!msg) return '-';
    if (msg.length <= ERR_PREVIEW) {
      return '<span class="err-inline">' + escHtml(msg) + '</span>';
    }
    return '<details class="err-detail"><summary>' + escHtml(msg.substring(0, ERR_PREVIEW)) +
      '</summary><div class="err-full">' + escHtml(msg) + '</div></details>';
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

  // ---- Attribute an orchestration task to the ingest vs historize phase ----
  // A plan row carries one combined task status, not per-phase (see #161). We
  // infer the failing phase so the failure lands in the right place and we
  // don't emit a contradictory failed-ingest row when ingest actually
  // succeeded (the historize phase is what failed).
  const ingestSuccessKeys = new Set();
  taggedLoadRuns.forEach(r => {
    if (r.execution_id && r.status === 'success') {
      ingestSuccessKeys.add(r.execution_id + '|' + r.table_name);
    }
  });
  function pipelineCaps(name) {
    const p = pipelineMap[name];
    return { ingest: p ? p.ingest_enabled : true, historize: p ? p.historize_enabled : false };
  }
  function failurePhase(r) {
    const caps = pipelineCaps(r.pipeline_name);
    if (caps.historize && !caps.ingest) return 'historize';
    if (caps.ingest && !caps.historize) return 'ingest';
    // Both phases ran: a successful load_run means ingest succeeded → historize failed.
    return ingestSuccessKeys.has(r.execution_id + '|' + r.table_name) ? 'historize' : 'ingest';
  }

  const failedOrch = orchRuns.filter(r => r.status === 'failed');

  // Failed tasks whose failing phase is ingest → synthesized failed ingest runs.
  const failedOrchIngest = failedOrch
    .filter(r => failurePhase(r) === 'ingest')
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

  // Failed tasks whose failing phase is historize → synthesized failed historize runs.
  // The historize runner now writes a status='failed' row to _saga_historize_log,
  // so those failures already arrive via D.historize_runs. Skip synthesizing a
  // duplicate for any pipeline that already has a logged failure — the synthesized
  // entry is only a backstop for hard crashes that never reached the log write.
  const loggedHistorizeFailures = new Set(
    (D.historize_runs || [])
      .filter(r => r.status === 'failed')
      .map(r => r.pipeline_name)
  );
  const failedOrchHistorize = failedOrch
    .filter(r => failurePhase(r) === 'historize')
    .filter(r => !loggedHistorizeFailures.has(r.pipeline_name))
    .map(r => ({
      pipeline_name: r.pipeline_name,
      source_table: r.table_name,
      target_table: r.table_name,
      snapshot_value: '',
      new_or_changed_rows: 0,
      deleted_rows: 0,
      is_full_reprocess: false,
      started_at: r.started_at || r.log_timestamp,
      finished_at: r.completed_at,
      status: 'failed',
      duration_seconds: r.duration_seconds,
      error_message: r.error_message,
    }));

  // Completed tasks with no matching load_run = ingest "skipped" (no new rows),
  // but only for ingest-capable pipelines. A completed historize-only task has
  // no load_run by nature and lives in the historize log, not here.
  const matchedOrchKeys = new Set();
  taggedLoadRuns.forEach(r => {
    if (r.execution_id) matchedOrchKeys.add(r.execution_id + '|' + r.table_name);
  });
  const skippedOrchIngest = orchRuns
    .filter(r => r.status === 'completed'
      && pipelineCaps(r.pipeline_name).ingest
      && !matchedOrchKeys.has(r.execution_id + '|' + r.table_name))
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

  // Merged historize: completed runs from the log + synthesized failed-historize tasks.
  const allHistorizeRuns = [...D.historize_runs, ...failedOrchHistorize]
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

  // Single-series bar chart (e.g. rows loaded per day).
  // fmtVal formats the tooltip value; labelEvery controls x-axis label density.
  function buildBarChart(label, days, values, color, fmtVal, labelEvery) {
    const every = labelEvery || 2;
    const max = Math.max(...values, 1);
    const chart = h('div', { className: 'mini-chart' });
    chart.appendChild(h('h3', null, label));
    const bars = h('div', { className: 'bar-chart' });
    const labels = h('div', { className: 'bar-labels' });
    values.forEach((v, i) => {
      const pct = (v / max) * 100;
      const bar = h('div', { className: 'bar' });
      bar.style.height = Math.max(pct, 2) + '%';
      bar.style.background = color;
      bar.appendChild(h('div', { className: 'bar-tooltip' },
        fmtDateShort(days[i].toISOString()) + ': ' + (fmtVal ? fmtVal(v) : v)));
      bars.appendChild(bar);
      labels.appendChild(h('span', null, i % every === 0 ? fmtDateShort(days[i].toISOString()) : ''));
    });
    chart.appendChild(bars);
    chart.appendChild(labels);
    return chart;
  }

  // Sum a numeric field per day bucket.
  function sumByDay(runs, days, field, tsField) {
    const ts = tsField || 'started_at';
    const sums = new Array(days.length).fill(0);
    runs.forEach(r => {
      if (!r[ts]) return;
      const rd = new Date(r[ts]);
      rd.setHours(0, 0, 0, 0);
      const idx = days.findIndex(d => d.getTime() === rd.getTime());
      if (idx >= 0) sums[idx] += (r[field] || 0);
    });
    return sums;
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

  // ---- Column chooser (hide/show table columns, persisted per table) ----
  function loadHiddenCols(storeKey) {
    try { return new Set(JSON.parse(localStorage.getItem(storeKey) || '[]')); }
    catch (e) { return new Set(); }
  }
  function columnMenu(storeKey, cols, hidden, redraw) {
    const wrap = h('div', { className: 'col-menu' });
    const btn = h('button', { className: 'col-menu-btn', type: 'button' }, 'Columns');
    const panel = h('div', { className: 'col-menu-panel' });
    cols.forEach(c => {
      if (c.fixed) return; // fixed columns (e.g. pipeline name) can't be hidden
      const cb = h('input', { type: 'checkbox' });
      cb.checked = !hidden.has(c.key);
      cb.addEventListener('change', () => {
        if (cb.checked) hidden.delete(c.key); else hidden.add(c.key);
        try { localStorage.setItem(storeKey, JSON.stringify([...hidden])); } catch (e) {}
        redraw();
      });
      panel.appendChild(h('label', null, cb, document.createTextNode(' ' + c.label)));
    });
    btn.addEventListener('click', e => { e.stopPropagation(); wrap.classList.toggle('open'); });
    panel.addEventListener('click', e => e.stopPropagation());
    wrap.appendChild(btn);
    wrap.appendChild(panel);
    return wrap;
  }

  // Build a table from column defs honoring hidden columns. Each col has
  // { key, label, render(row), nosort?, fixed? }; non-nosort cols are sortable.
  function buildTable(cols, hidden, rows, sortCol, sortAsc, emptyHtml) {
    const vis = cols.filter(c => !hidden.has(c.key));
    let html = '<table><tr>';
    vis.forEach(c => {
      if (c.nosort) html += '<th>' + c.label + '</th>';
      else html += '<th class="' + sortIndicator(c.key, sortCol, sortAsc) + c.label + '</th>';
    });
    html += '</tr>';
    if (!rows.length) {
      html += '<tr><td colspan="' + vis.length + '">' + emptyHtml + '</td></tr>';
    }
    rows.forEach(r => {
      html += '<tr>' + vis.map(c => '<td>' + c.render(r) + '</td>').join('') + '</tr>';
    });
    html += '</table>';
    return html;
  }

  function genericSort(arr, sortCol, sortAsc, getVal) {
    arr.sort((a, b) => {
      let va = getVal(a, sortCol), vb = getVal(b, sortCol);
      if (va == null) va = '';
      if (vb == null) vb = '';
      // Numeric columns (rows, durations) carry nulls for failed/skipped runs;
      // compare as numbers only when both sides are numbers, and never call a
      // string method on a number (a mixed number/'' pair used to throw and
      // abort the whole sort — e.g. Duration).
      if (typeof va === 'number' && typeof vb === 'number') {
        return sortAsc ? va - vb : vb - va;
      }
      va = String(va).toLowerCase();
      vb = String(vb).toLowerCase();
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

  // ---- Tree filter pane (folder → pipeline navigator, shared across table tabs) ----
  // Replaces the per-tab group + free-text search inputs. Collapsible; collapse
  // state persists. onChange(selection) fires whenever the selection changes.
  //
  // The pipeline name encodes the config path with '__' separators
  // (e.g. api__genesyscloud__conversation_aggregate), so splitting it
  // reconstructs the real config folder hierarchy at any depth.
  function buildPipelineTree(pipelines) {
    const root = { name: '', path: '', children: {}, leaves: [] };
    pipelines.forEach(p => {
      const segs = p.pipeline_name.split('__');
      let node = root;
      for (let i = 0; i < segs.length - 1; i++) {
        const key = segs[i];
        if (!node.children[key]) {
          node.children[key] = {
            name: key, path: node.path ? node.path + '__' + key : key,
            children: {}, leaves: [],
          };
        }
        node = node.children[key];
      }
      node.leaves.push({ label: segs[segs.length - 1], full: p.pipeline_name });
    });
    return root;
  }
  function countLeaves(node) {
    let n = node.leaves.length;
    Object.values(node.children).forEach(c => { n += countLeaves(c); });
    return n;
  }
  function nodeMatchesQuery(node, q) {
    if (!q) return true;
    if (node.name && node.name.toLowerCase().includes(q)) return true;
    if (node.leaves.some(l => l.full.toLowerCase().includes(q))) return true;
    return Object.values(node.children).some(c => nodeMatchesQuery(c, q));
  }
  const pipelineTree = buildPipelineTree(D.pipelines);

  function createTreeFilter(onChange) {
    const collapsedKey = 'saga-tree-collapsed';
    const widthKey = 'saga-tree-width';
    let selection = { type: 'all', value: null };
    const expanded = new Set();

    const pane = h('div', { className: 'tree-pane' });
    const storedW = parseInt(localStorage.getItem(widthKey) || '', 10);
    const startCollapsed = localStorage.getItem(collapsedKey) === '1';
    if (startCollapsed) pane.classList.add('collapsed');
    else if (storedW) pane.style.flexBasis = storedW + 'px';

    const head = h('div', { className: 'tree-head' });
    head.appendChild(h('h3', null, 'Pipelines'));
    const collapseBtn = h('button', { className: 'tree-collapse-btn', type: 'button' });
    const syncBtn = () => { collapseBtn.textContent = pane.classList.contains('collapsed') ? '»' : '«'; };
    collapseBtn.addEventListener('click', () => {
      const willCollapse = !pane.classList.contains('collapsed');
      pane.classList.toggle('collapsed');
      if (willCollapse) {
        pane.style.flexBasis = ''; // let .collapsed control the width
      } else {
        const w = parseInt(localStorage.getItem(widthKey) || '', 10);
        if (w) pane.style.flexBasis = w + 'px';
      }
      try { localStorage.setItem(collapsedKey, willCollapse ? '1' : '0'); } catch (e) {}
      syncBtn();
    });
    head.appendChild(collapseBtn);
    pane.appendChild(head);

    const searchWrap = h('div', { className: 'tree-search' });
    const search = h('input', { className: 'filter-input', type: 'text', placeholder: 'Search pipelines…' });
    searchWrap.appendChild(search);
    pane.appendChild(searchWrap);

    const body = h('div', { className: 'tree-body' });
    pane.appendChild(body);

    // Drag-to-resize handle on the right edge.
    const resizer = h('div', { className: 'tree-resize', title: 'Drag to resize' });
    resizer.addEventListener('mousedown', e => {
      e.preventDefault();
      const startX = e.clientX;
      const startW = pane.getBoundingClientRect().width;
      document.body.style.userSelect = 'none';
      function move(ev) {
        const w = Math.max(160, Math.min(560, startW + (ev.clientX - startX)));
        pane.style.flexBasis = w + 'px';
      }
      function up() {
        document.removeEventListener('mousemove', move);
        document.removeEventListener('mouseup', up);
        document.body.style.userSelect = '';
        try { localStorage.setItem(widthKey, String(Math.round(pane.getBoundingClientRect().width))); } catch (e) {}
      }
      document.addEventListener('mousemove', move);
      document.addEventListener('mouseup', up);
    });
    pane.appendChild(resizer);

    const indentPad = depth => (8 + depth * 15) + 'px';

    function renderLeaf(leaf, depth) {
      const item = h('div', { className: 'tree-item tree-leaf' +
        (selection.type === 'pipeline' && selection.value === leaf.full ? ' selected' : '') },
        h('span', { className: 'tree-caret' }), document.createTextNode(leaf.label));
      item.style.paddingLeft = indentPad(depth);
      item.title = leaf.full;
      item.addEventListener('click', () => { selection = { type: 'pipeline', value: leaf.full }; render(); onChange(selection); });
      body.appendChild(item);
    }

    function renderFolder(node, depth, q) {
      if (q && !nodeMatchesQuery(node, q)) return;
      const isExp = expanded.has(node.path) || !!q;
      const selected = selection.type === 'folder' && selection.value === node.path;
      const fItem = h('div', { className: 'tree-item tree-group' + (isExp ? '' : ' collapsed') + (selected ? ' selected' : '') });
      fItem.style.paddingLeft = indentPad(depth);
      const caret = h('span', { className: 'tree-caret' }, '▾');
      caret.addEventListener('click', e => {
        e.stopPropagation();
        if (expanded.has(node.path)) expanded.delete(node.path); else expanded.add(node.path);
        render();
      });
      fItem.appendChild(caret);
      fItem.appendChild(document.createTextNode(node.name));
      fItem.appendChild(h('span', { className: 'tree-count' }, String(countLeaves(node))));
      fItem.addEventListener('click', () => {
        if (selection.type === 'folder' && selection.value === node.path) {
          // Already selected → clicking the row toggles expand (large hit target).
          if (expanded.has(node.path)) expanded.delete(node.path); else expanded.add(node.path);
          render();
        } else {
          expanded.add(node.path);
          selection = { type: 'folder', value: node.path };
          render(); onChange(selection);
        }
      });
      body.appendChild(fItem);
      if (isExp) renderChildren(node, depth + 1, q);
    }

    function renderChildren(node, depth, q) {
      Object.keys(node.children).sort().forEach(key => renderFolder(node.children[key], depth, q));
      node.leaves.slice()
        .sort((a, b) => a.label < b.label ? -1 : a.label > b.label ? 1 : 0)
        .forEach(leaf => {
          if (q && !leaf.full.toLowerCase().includes(q)) return;
          renderLeaf(leaf, depth);
        });
    }

    function render() {
      const q = search.value.toLowerCase();
      body.innerHTML = '';

      const allItem = h('div', { className: 'tree-item' + (selection.type === 'all' ? ' selected' : '') },
        h('span', { className: 'tree-caret' }), document.createTextNode('All pipelines'));
      allItem.style.paddingLeft = indentPad(0);
      allItem.addEventListener('click', () => { selection = { type: 'all', value: null }; render(); onChange(selection); });
      body.appendChild(allItem);

      renderChildren(pipelineTree, 0, q);
    }

    search.addEventListener('input', render);
    syncBtn();
    render();

    return {
      pane,
      reset() { selection = { type: 'all', value: null }; search.value = ''; render(); },
      matches(pipelineName) {
        if (selection.type === 'all') return true;
        if (selection.type === 'pipeline') return pipelineName === selection.value;
        if (selection.type === 'folder') {
          return pipelineName === selection.value || pipelineName.startsWith(selection.value + '__');
        }
        return true;
      },
    };
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
    const totalHist = allHistorizeRuns.length;
    const histFailed = allHistorizeRuns.filter(r => r.status === 'failed').length;
    const histSuccess = totalHist - histFailed;

    // Orchestration stats (execution-level)
    const totalExecs = execList.length;
    const failedExecs = execList.filter(e => e.failed > 0).length;
    const cleanExecs = totalExecs - failedExecs;

    // Pipelines without any runs
    const pipelinesWithRuns = new Set(allIngestRuns.map(r => r.pipeline_name)).size;
    const pipelinesNoRuns = totalPipelines - pipelinesWithRuns;

    // Currently failed pipelines (last run was a failure)
    const currentIngestFails = getCurrentlyFailed(allIngestRuns);
    const currentHistFails = getCurrentlyFailed(allHistorizeRuns);
    const failingNames = [...new Set([
      ...currentIngestFails.map(r => r.pipeline_name),
      ...currentHistFails.map(r => r.pipeline_name),
    ])];
    const currentlyFailedCount = failingNames.length;
    const healthyCount = totalPipelines - currentlyFailedCount;

    panel.innerHTML = '';
    panel.appendChild(h('h2', { className: 'page-title' }, 'Dashboard'));

    // Persistent health strip — same slot whether healthy or failing, so the
    // layout never shifts; the headline status is always visible without scrolling.
    if (currentlyFailedCount === 0) {
      const strip = h('div', { className: 'health-strip healthy' },
        h('span', { className: 'health-dot' }),
        h('span', null, h('strong', null, 'All ' + totalPipelines + ' pipelines healthy')));
      panel.appendChild(strip);
    } else {
      const shown = failingNames.slice(0, 4).join(', ') + (failingNames.length > 4 ? ', …' : '');
      const strip = h('div', { className: 'health-strip failing',
        onClick: () => { const t = $('#failures-anchor'); if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' }); } });
      strip.appendChild(h('span', { className: 'health-dot' }));
      strip.appendChild(h('span', null,
        h('strong', null, currentlyFailedCount + (currentlyFailedCount === 1 ? ' pipeline failing' : ' pipelines failing')),
        document.createTextNode('  '),
        h('span', { className: 'health-names' }, shown)));
      strip.appendChild(h('span', { className: 'health-cta' }, 'View failures ▾'));
      panel.appendChild(strip);
    }

    // Report context banner
    const banner = h('div', { className: 'report-banner' });
    banner.innerHTML =
      '<span>Environment: <strong>' + escHtml(D.environment || 'unknown') + '</strong></span>' +
      '<span>Database: <strong>' + escHtml(D.project || 'unknown') + '</strong></span>' +
      '<span>Period: <strong>Last ' + D.days + ' days</strong></span>' +
      '<span>Generated: <strong>' + fmtDate(D.generated_at) + '</strong></span>';
    panel.appendChild(banner);

    // Which activity types actually have data — used to hide empty cards/charts.
    const hasIngest = allIngestRuns.length > 0;
    const hasHist = totalHist > 0;

    // Consolidated KPI row — only include cards that have data behind them.
    const kpiGrid = h('div', { className: 'kpi-grid' });
    const kpis = [
      { label: 'Pipelines', value: totalPipelines,
        sub: groups.length + ' group(s), ' + pipelinesNoRuns + ' without runs' },
    ];
    if (totalExecs > 0) {
      kpis.push(
        { label: 'Clean executions', value: cleanExecs + ' / ' + totalExecs,
          sub: failedExecs > 0 ? failedExecs + ' with failures' : 'all clean',
          subColor: failedExecs > 0 ? 'var(--danger)' : null }
      );
    }
    if (hasIngest) {
      kpis.push(
        { label: 'Successful ingest runs', value: ingestSuccess + ' / ' + totalIngest,
          sub: (ingestFailed > 0 ? ingestFailed + ' failed' : 'all succeeded') + (ingestSkipped > 0 ? ', ' + ingestSkipped + ' skipped' : ''),
          subColor: ingestFailed > 0 ? 'var(--danger)' : null },
        { label: 'Rows loaded', value: fmtNum(totalRows), sub: 'across all ingest runs' }
      );
    }
    if (hasHist) {
      kpis.push(
        { label: 'Successful historize runs', value: histSuccess + ' / ' + totalHist,
          sub: histFailed > 0 ? histFailed + ' failed' : 'all succeeded',
          subColor: histFailed > 0 ? 'var(--danger)' : null }
      );
    }
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

    // Stacked activity charts with pipeline state donut
    const numDays = D.days || 14;
    const dayBuckets = buildDayBuckets(numDays);
    const labelEvery = numDays > 20 ? 5 : numDays > 10 ? 2 : 1;
    const chartSection = h('div', { className: 'section' });
    chartSection.appendChild(h('div', { className: 'section-header' }, 'Activity — last ' + numDays + ' days'));
    const chartContainer = h('div', { className: 'chart-container' });

    // Pipeline state donut chart
    chartContainer.appendChild(buildDonutChart('Pipeline state', [
      { value: healthyCount, color: 'var(--success)', label: 'passing' },
      { value: currentlyFailedCount, color: 'var(--danger)', label: 'failing' },
    ], currentlyFailedCount > 0 ? currentlyFailedCount : totalPipelines,
       currentlyFailedCount > 0 ? 'failing' : 'all passing'));

    // Orchestration: clean vs failed executions per day
    if (totalExecs > 0) {
      const orchCleanByDay = new Array(numDays).fill(0);
      const orchFailByDay = new Array(numDays).fill(0);
      execList.forEach(e => {
        if (!e.timestamp) return;
        const rd = new Date(e.timestamp);
        rd.setHours(0, 0, 0, 0);
        const idx = dayBuckets.findIndex(d => d.getTime() === rd.getTime());
        if (idx >= 0) {
          if (e.failed > 0) orchFailByDay[idx]++;
          else orchCleanByDay[idx]++;
        }
      });
      chartContainer.appendChild(buildStackedBarChart('Executions', dayBuckets,
        orchCleanByDay, orchFailByDay, 'var(--success)', 'var(--danger)', date => {
          pendingOrchDateFilter = date;
          showTab('orchestration');
          renderOrchestration();
        }));
    }

    // Ingest: success vs failed (+ rows per day) — only when there are ingest runs
    if (hasIngest) {
      const ingestSuccessRuns = allIngestRuns.filter(r => r.status === 'success');
      const ingestFailedRuns = allIngestRuns.filter(r => r.status === 'failed');
      chartContainer.appendChild(buildStackedBarChart('Ingest runs', dayBuckets,
        bucketByDay(ingestSuccessRuns, dayBuckets), bucketByDay(ingestFailedRuns, dayBuckets),
        'var(--success)', 'var(--danger)', date => {
          pendingIngestDateFilter = date;
          showTab('ingest-runs');
          renderIngestRuns();
        }));
      chartContainer.appendChild(buildBarChart('Rows loaded per day', dayBuckets,
        sumByDay(ingestSuccessRuns, dayBuckets, 'row_count'), 'var(--accent)', fmtNum, labelEvery));
    }

    // Historize: completed vs failed — only when there are historize runs
    if (hasHist) {
      chartContainer.appendChild(buildStackedBarChart('Historize runs', dayBuckets,
        bucketByDay(allHistorizeRuns.filter(r => r.status === 'completed'), dayBuckets),
        bucketByDay(allHistorizeRuns.filter(r => r.status === 'failed'), dayBuckets),
        'var(--success)', 'var(--danger)', date => {
          pendingHistorizeDateFilter = date;
          showTab('historize-runs');
          renderHistorizeRuns();
        }));
    }

    chartSection.appendChild(chartContainer);
    panel.appendChild(chartSection);

    // Pipeline groups breakdown
    const groupSection = h('div', { className: 'section' });
    groupSection.appendChild(h('div', { className: 'section-header' }, 'Pipeline groups'));
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

    // Currently failing pipelines (last run was a failure).
    // The first failing section carries the scroll anchor for the health strip.
    if (currentIngestFails.length) {
      const failSection = h('div', { className: 'section', id: 'failures-anchor' });
      failSection.appendChild(h('div', { className: 'section-header' }, 'Currently failing — ingest'));
      const failTable = h('div', { className: 'table-wrap' });
      let ftHTML = '<table><tr><th>Pipeline</th><th>Table</th><th>Last run</th><th>Error</th><th>Status</th></tr>';
      currentIngestFails.forEach(r => {
        ftHTML += '<tr><td><span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' +
          escHtml(r.pipeline_name) + '</span></td><td>' + escHtml(r.table_name) +
          '</td><td>' + fmtDate(r.started_at) + '</td><td>' + errorCell(r.error_message) +
          '</td><td>' + statusBadge(r.status) + '</td></tr>';
      });
      ftHTML += '</table>';
      failTable.innerHTML = ftHTML;
      failSection.appendChild(failTable);
      panel.appendChild(failSection);
    }

    if (currentHistFails.length) {
      const failSection = h('div', { className: 'section', id: currentIngestFails.length ? null : 'failures-anchor' });
      failSection.appendChild(h('div', { className: 'section-header' }, 'Currently failing — historize'));
      const failTable = h('div', { className: 'table-wrap' });
      let ftHTML = '<table><tr><th>Pipeline</th><th>Target</th><th>Last run</th><th>Error</th><th>Status</th></tr>';
      currentHistFails.forEach(r => {
        ftHTML += '<tr><td><span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' +
          escHtml(r.pipeline_name) + '</span></td><td>' + escHtml(r.target_table) +
          '</td><td>' + fmtDate(r.started_at) + '</td><td>' + errorCell(r.error_message) +
          '</td><td>' + statusBadge(r.status) + '</td></tr>';
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
        { label: 'Write disposition', value: p.write_disposition },
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
    const pHistRuns = allHistorizeRuns.filter(r => r.pipeline_name === pipelineName);

    // Stats
    const statGrid = h('div', { className: 'kpi-grid' });
    const pSuccess = pRuns.filter(r => r.status === 'success');
    const totalIngestRows = pSuccess.reduce((s,r) => s + (r.row_count||0), 0);
    const avgDuration = pSuccess.length ? (pSuccess.reduce((s,r) => s + (r.duration_seconds||0), 0) / pSuccess.length) : 0;
    const pFailed = pRuns.filter(r => r.status === 'failed').length;
    const histFailCount = pHistRuns.filter(r => r.status === 'failed').length;
    [
      { label: 'Successful ingest runs', value: pSuccess.length + ' / ' + (pSuccess.length + pFailed) },
      { label: 'Total rows loaded', value: fmtNum(totalIngestRows) },
      { label: 'Avg duration', value: fmtDuration(avgDuration) },
      { label: 'Historize runs', value: pHistRuns.length + (histFailCount > 0 ? ' (' + histFailCount + ' failed)' : '') },
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
      chartSection.appendChild(h('div', { className: 'section-header' }, 'Rows per run (ingest)'));
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
      runSection.appendChild(h('div', { className: 'section-header' }, 'Ingest run history'));
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
      histSection.appendChild(h('div', { className: 'section-header' }, 'Historize run history'));
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
    // Close any open column-chooser menus when clicking elsewhere
    $$('.col-menu.open').forEach(m => { if (!m.contains(e.target)) m.classList.remove('open'); });
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

    const tree = createTreeFilter(() => { pipelinesPage = 0; draw(); });
    const treeWrap = h('div', { className: 'tab-with-tree' });
    const treeMain = h('div', { className: 'tree-main' });
    treeWrap.appendChild(tree.pane);
    treeWrap.appendChild(treeMain);
    panel.appendChild(treeWrap);

    const section = h('div', { className: 'section' });
    const filters = h('div', { className: 'filters' });
    const tagSelect = addTagFilter(filters);
    const dispSelect = addDispositionFilter(filters);
    section.appendChild(filters);

    const filterInputs = [tagSelect, dispSelect];
    const clearBtn = addClearFiltersButton(filters, filterInputs, () => { pipelinesPage = 0; tree.reset(); draw(); });

    const tableWrap = h('div', { className: 'table-wrap' });
    const paginationWrap = h('div');
    section.appendChild(tableWrap);
    section.appendChild(paginationWrap);
    treeMain.appendChild(section);

    const lastRun = {};
    allIngestRuns.forEach(r => {
      if (!lastRun[r.pipeline_name] || r.started_at > lastRun[r.pipeline_name].started_at)
        lastRun[r.pipeline_name] = r;
    });

    const cols = [
      { key: 'pipeline_name', label: 'Pipeline', fixed: true,
        render: p => '<span class="pipeline-link" data-pipeline="' + escHtml(p.pipeline_name) + '">' + escHtml(p.pipeline_name) + '</span>' },
      { key: 'pipeline_group', label: 'Group', render: p => escHtml(p.pipeline_group) },
      { key: 'write_disposition', label: 'Disposition', render: p => {
        const caps = [];
        if (p.ingest_enabled) caps.push('<span class="badge badge-info">ingest</span>');
        if (p.historize_enabled) caps.push('<span class="badge badge-success">historize</span>');
        return escHtml(p.write_disposition) + ' ' + caps.join(' ');
      } },
      { key: 'tags', label: 'Tags', nosort: true, render: p => tagBadges(p.tags) },
      { key: 'last_run', label: 'Last run', render: p => {
        const lr = lastRun[p.pipeline_name];
        return lr ? fmtDate(lr.started_at) : '<span class="badge badge-neutral">no runs</span>';
      } },
      { key: 'last_rows', label: 'Last rows', render: p => {
        const lr = lastRun[p.pipeline_name];
        return lr ? fmtNum(lr.row_count) : '-';
      } },
    ];
    const hidden = loadHiddenCols('saga-cols-pipelines');
    filters.appendChild(columnMenu('saga-cols-pipelines', cols, hidden, () => draw()));

    let sortCol = 'pipeline_name', sortAsc = true;
    let pipelinesPage = 0;

    function draw(clickedCol) {
      if (clickedCol) {
        if (sortCol === clickedCol) sortAsc = !sortAsc;
        else { sortCol = clickedCol; sortAsc = true; }
        pipelinesPage = 0;
      }
      const tagFilter = tagSelect.value;
      const dispFilter = dispSelect.value;

      let filtered = D.pipelines.filter(p => {
        if (!tree.matches(p.pipeline_name)) return false;
        if (tagFilter && !p.tags.includes(tagFilter)) return false;
        if (dispFilter && p.write_disposition !== dispFilter) return false;
        return true;
      });

      genericSort(filtered, sortCol, sortAsc, (item, col) => {
        if (col === 'last_run') return lastRun[item.pipeline_name] ? lastRun[item.pipeline_name].started_at || '' : '';
        if (col === 'last_rows') return lastRun[item.pipeline_name] ? lastRun[item.pipeline_name].row_count || 0 : 0;
        return item[col] || '';
      });

      tableWrap.innerHTML = buildTable(cols, hidden, paginateRows(filtered, pipelinesPage),
        sortCol, sortAsc, '<div class="empty-state"><h3>No pipelines match</h3></div>');
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

    const tree = createTreeFilter(() => { ingestPage = 0; draw(); });
    const treeWrap = h('div', { className: 'tab-with-tree' });
    const treeMain = h('div', { className: 'tree-main' });
    treeWrap.appendChild(tree.pane);
    treeWrap.appendChild(treeMain);
    panel.appendChild(treeWrap);

    const section = h('div', { className: 'section' });
    const filters = h('div', { className: 'filters' });
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
    // Order: status, tag, schema, disposition, execution
    filters.insertBefore(datasetSelect, dispSelect);
    filters.insertBefore(tagSelect, datasetSelect);
    filters.insertBefore(statusSelect, tagSelect);
    filters.appendChild(execSelect);
    const ingestDateInput = addDateFilter(filters);
    filters.insertBefore(ingestDateInput, filters.firstChild);
    const ingestFilterInputs = [datasetSelect, statusSelect, execSelect, tagSelect, dispSelect, ingestDateInput];
    const ingestClearBtn = addClearFiltersButton(filters, ingestFilterInputs, () => { ingestPage = 0; tree.reset(); draw(); });
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
    treeMain.appendChild(section);

    const cols = [
      { key: 'pipeline_name', label: 'Pipeline', fixed: true,
        render: r => '<span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' + escHtml(r.pipeline_name) + '</span>' },
      { key: 'dataset_name', label: 'Schema', render: r => escHtml(r.dataset_name || '') },
      { key: 'table_name', label: 'Table', render: r => escHtml(r.table_name) },
      { key: 'row_count', label: 'Rows', render: r => fmtNum(r.row_count) },
      { key: 'started_at', label: 'Started', render: r => fmtDate(r.started_at) },
      { key: 'duration_seconds', label: 'Duration', render: r => fmtDuration(r.duration_seconds) },
      { key: 'status', label: 'Status', render: r => statusBadge(r.status) },
      { key: 'error_message', label: 'Error', render: r => errorCell(r.error_message) },
    ];
    const hidden = loadHiddenCols('saga-cols-ingest');
    filters.appendChild(columnMenu('saga-cols-ingest', cols, hidden, () => draw()));

    let sortCol = 'started_at', sortAsc = false;
    let ingestPage = 0;

    function draw(clickedCol) {
      if (clickedCol) {
        if (sortCol === clickedCol) sortAsc = !sortAsc;
        else { sortCol = clickedCol; sortAsc = true; }
        ingestPage = 0;
      }
      const dsFilter = datasetSelect.value;
      const statusFilter = statusSelect.value;
      const execFilter = execSelect.value;
      const tagFilter = tagSelect.value;
      const dispFilter = dispSelect.value;
      const dateFilter = ingestDateInput.value;

      let filtered = allIngestRuns.filter(r => {
        if (!tree.matches(r.pipeline_name)) return false;
        if (dsFilter && r.dataset_name !== dsFilter) return false;
        if (statusFilter && r.status !== statusFilter) return false;
        if (execFilter && r.execution_id !== execFilter) return false;
        if (!matchesTag(r.pipeline_name, tagFilter)) return false;
        if (!matchesDisposition(r.pipeline_name, dispFilter)) return false;
        if (!matchesDate(r.started_at, dateFilter)) return false;
        return true;
      });

      genericSort(filtered, sortCol, sortAsc, (item, col) => item[col]);

      tableWrap.innerHTML = buildTable(cols, hidden, paginateRows(filtered, ingestPage),
        sortCol, sortAsc,
        '<div class="empty-state"><h3>No ingest runs found</h3><p>Run pipelines with <code>saga ingest</code> to generate data.</p></div>');
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

    const tree = createTreeFilter(() => { histPage = 0; draw(); });
    const treeWrap = h('div', { className: 'tab-with-tree' });
    const treeMain = h('div', { className: 'tree-main' });
    treeWrap.appendChild(tree.pane);
    treeWrap.appendChild(treeMain);
    panel.appendChild(treeWrap);

    const section = h('div', { className: 'section' });
    const filters = h('div', { className: 'filters' });
    const statusSelect = h('select', { className: 'filter-input' });
    statusSelect.appendChild(h('option', { value: '' }, 'All statuses'));
    ['completed', 'failed'].forEach(s => statusSelect.appendChild(h('option', { value: s }, s)));
    const tagSelect = addTagFilter(filters);
    filters.insertBefore(statusSelect, tagSelect);
    const histDateInput = addDateFilter(filters);
    filters.insertBefore(histDateInput, filters.firstChild);
    const histFilterInputs = [statusSelect, tagSelect, histDateInput];
    const histClearBtn = addClearFiltersButton(filters, histFilterInputs, () => { histPage = 0; tree.reset(); draw(); });
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
    treeMain.appendChild(section);

    const cols = [
      { key: 'pipeline_name', label: 'Pipeline', fixed: true,
        render: r => '<span class="pipeline-link" data-pipeline="' + escHtml(r.pipeline_name) + '">' + escHtml(r.pipeline_name) + '</span>' },
      { key: 'source_table', label: 'Source', render: r => escHtml(r.source_table) },
      { key: 'target_table', label: 'Target', render: r => escHtml(r.target_table) },
      { key: 'snapshot_value', label: 'Snapshot', render: r => escHtml(r.snapshot_value) },
      { key: 'new_or_changed_rows', label: 'Changed', render: r => fmtNum(r.new_or_changed_rows) },
      { key: 'deleted_rows', label: 'Deleted', render: r => fmtNum(r.deleted_rows) },
      { key: 'started_at', label: 'Started', render: r => fmtDate(r.started_at) },
      { key: 'duration_seconds', label: 'Duration', render: r => fmtDuration(r.duration_seconds) },
      { key: 'status', label: 'Status', render: r => statusBadge(r.status) },
      { key: 'error_message', label: 'Error', render: r => errorCell(r.error_message) },
    ];
    const hidden = loadHiddenCols('saga-cols-historize');
    filters.appendChild(columnMenu('saga-cols-historize', cols, hidden, () => draw()));

    let sortCol = 'started_at', sortAsc = false;
    let histPage = 0;

    function draw(clickedCol) {
      if (clickedCol) {
        if (sortCol === clickedCol) sortAsc = !sortAsc;
        else { sortCol = clickedCol; sortAsc = true; }
        histPage = 0;
      }
      const statusFilter = statusSelect.value;
      const tagFilter = tagSelect.value;
      const dateFilter = histDateInput.value;

      let filtered = allHistorizeRuns.filter(r => {
        if (!tree.matches(r.pipeline_name)) return false;
        if (statusFilter && r.status !== statusFilter) return false;
        if (!matchesTag(r.pipeline_name, tagFilter)) return false;
        if (!matchesDate(r.started_at, dateFilter)) return false;
        return true;
      });

      genericSort(filtered, sortCol, sortAsc, (item, col) => item[col]);

      tableWrap.innerHTML = buildTable(cols, hidden, paginateRows(filtered, histPage),
        sortCol, sortAsc,
        '<div class="empty-state"><h3>No historize runs found</h3><p>Run pipelines with <code>saga historize</code> to generate data.</p></div>');
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
    panel.appendChild(h('h2', { className: 'page-title' }, 'Executions'));

    if (!orchRuns.length) {
      panel.appendChild(h('div', { className: 'section' },
        h('div', { className: 'empty-state' },
          h('h3', null, 'No executions recorded'),
          h('p', null, 'No execution runs found yet. Runs are recorded automatically — both local runs and orchestrated runs (saga ingest --orchestrate).')
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
    summarySection.appendChild(h('div', { className: 'section-header' }, 'Execution summary'));
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

    const typeSelect = h('select', { className: 'filter-input' });
    typeSelect.appendChild(h('option', { value: '' }, 'All types'));
    typeSelect.appendChild(h('option', { value: 'orchestrated' }, 'Orchestrated'));
    typeSelect.appendChild(h('option', { value: 'local' }, 'Local'));
    filters.appendChild(typeSelect);

    const orchDateInput = addDateFilter(filters);
    filters.insertBefore(orchDateInput, filters.firstChild);
    const orchFilterInputs = [statusSelect, commandSelect, criteriaSelect, typeSelect, orchDateInput];
    const orchClearBtn = addClearFiltersButton(filters, orchFilterInputs, () => { orchPage = 0; drawSummary(); });

    // An execution is "local" when its metadata says so, or (no metadata) any
    // of its tasks is flagged local.
    function execIsLocal(e) {
      return (e.meta && e.meta.is_orchestrated === false) ||
        (!e.meta && e.tasks.some(t => t.is_orchestrated === false));
    }

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

    function execStatus(e) {
      return e.failed > 0 ? 'failed' : e.running > 0 ? 'running' : e.pending > 0 ? 'pending' : 'completed';
    }

    const cols = [
      { key: 'execution_id', label: 'Execution ID', fixed: true,
        render: e => '<span class="exec-link" data-exec-id="' + escHtml(e.execution_id) +
          '" style="color:var(--accent-light);cursor:pointer;font-weight:600"><code>' +
          escHtml(e.execution_id.substring(0, 8)) + '</code></span>' },
      { key: 'type', label: 'Type', render: e => {
        const isLocal = execIsLocal(e);
        return '<span class="badge ' + (isLocal ? 'badge-neutral' : 'badge-info') + '">' +
          (isLocal ? 'local' : 'orchestrated') + '</span>';
      } },
      { key: 'time', label: 'Time', render: e => fmtDate(e.meta ? e.meta.created_at : e.timestamp) },
      { key: 'command', label: 'Command', render: e => e.meta ? escHtml(e.meta.command) : '-' },
      { key: 'select', label: 'Select', render: e => e.meta && e.meta.select_criteria
        ? '<code>' + escHtml(e.meta.select_criteria) + '</code>'
        : '<span style="color:var(--text-muted)">all</span>' },
      { key: 'total', label: 'Tasks', render: e => String(e.total) },
      { key: 'completed', label: 'Completed', render: e => String(e.completed) },
      { key: 'failed', label: 'Failed', render: e => e.failed > 0
        ? '<strong style="color:var(--danger)">' + e.failed + '</strong>' : '0' },
      { key: 'duration', label: 'Duration', render: e => fmtDuration(e.duration) },
      { key: 'status', label: 'Status', render: e => statusBadge(execStatus(e)) },
    ];
    // Backfills are the exception, not the rule — only surface the window column
    // when at least one execution in this report actually carried an override,
    // so the common (non-backfill) case stays uncluttered.
    const hasBackfill = execList.some(e =>
      e.meta && (e.meta.start_value_override || e.meta.end_value_override));
    if (hasBackfill) {
      const selectIdx = cols.findIndex(c => c.key === 'select');
      cols.splice(selectIdx + 1, 0, { key: 'backfill', label: 'Backfill window', render: e => {
        const start = e.meta && e.meta.start_value_override;
        const end = e.meta && e.meta.end_value_override;
        if (!start && !end) return '<span style="color:var(--text-muted)">—</span>';
        return '<code>' + escHtml(start || '…') + ' → ' + escHtml(end || '…') + '</code>';
      } });
    }
    const hidden = loadHiddenCols('saga-cols-orchestration');
    filters.appendChild(columnMenu('saga-cols-orchestration', cols, hidden, () => drawSummary()));

    let sortCol = 'time', sortAsc = false;
    let orchPage = 0;

    function drawSummary(clickedCol) {
      if (clickedCol) {
        if (sortCol === clickedCol) sortAsc = !sortAsc;
        else { sortCol = clickedCol; sortAsc = true; }
        orchPage = 0;
      }
      const filter = statusSelect.value;
      const commandFilter = commandSelect.value;
      const criteriaFilter = criteriaSelect.value;
      const typeFilter = typeSelect.value;
      const dateFilter = orchDateInput.value;
      let filtered = execList.slice();
      if (filter === 'failed') filtered = filtered.filter(e => e.failed > 0);
      if (filter === 'clean') filtered = filtered.filter(e => e.failed === 0);
      if (typeFilter === 'local') filtered = filtered.filter(e => execIsLocal(e));
      if (typeFilter === 'orchestrated') filtered = filtered.filter(e => !execIsLocal(e));
      if (commandFilter) filtered = filtered.filter(e => e.meta && e.meta.command === commandFilter);
      if (criteriaFilter) filtered = filtered.filter(e => e.meta && e.meta.select_criteria === criteriaFilter);
      if (dateFilter) {
        filtered = filtered.filter(e => matchesDate(e.meta ? e.meta.created_at : e.timestamp, dateFilter));
      }

      genericSort(filtered, sortCol, sortAsc, (e, col) => {
        switch (col) {
          case 'execution_id': return e.execution_id;
          case 'type': return execIsLocal(e) ? 'local' : 'orchestrated';
          case 'time': return (e.meta ? e.meta.created_at : e.timestamp) || '';
          case 'command': return e.meta ? e.meta.command : '';
          case 'select': return e.meta ? e.meta.select_criteria : '';
          case 'backfill': return e.meta ? (e.meta.start_value_override || '') : '';
          case 'total': return e.total;
          case 'completed': return e.completed;
          case 'failed': return e.failed;
          case 'duration': return e.duration;
          case 'status': return execStatus(e);
          default: return '';
        }
      });

      summaryTable.innerHTML = buildTable(cols, hidden, paginateRows(filtered, orchPage),
        sortCol, sortAsc, '<div class="empty-state"><h3>No executions match</h3></div>');
      attachSortHandlers(summaryTable, drawSummary);
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
