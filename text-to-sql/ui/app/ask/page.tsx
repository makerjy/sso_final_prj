'use client';

import { useEffect, useState } from 'react';

const DEFAULT_QUESTIONS = [
  'Average length of stay by diagnosis.',
  'Which procedures are most frequent?',
  'ICU admissions by unit.',
];

export default function AskPage() {
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [questions, setQuestions] = useState<string[]>(DEFAULT_QUESTIONS);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
  const [budget, setBudget] = useState<any>(null);
  const [budgetDraft, setBudgetDraft] = useState({ limit: '', alert: '' });
  const [budgetSaving, setBudgetSaving] = useState(false);

  const previewData = (() => {
    const payload = result?.payload;
    if (!payload) return null;
    const res = payload.result;
    if (res?.preview?.columns?.length && Array.isArray(res.preview.rows)) {
      return {
        mode: payload.mode,
        sql: res.sql,
        summary: res.summary,
        source: res.source,
        preview: res.preview,
        rowCount: res.preview.row_count,
        rowCap: res.preview.row_cap,
      };
    }
    return null;
  })();

  const payload = result?.payload;
  const qid = result?.qid;
  const policyMessage = (detail: string) => {
    if (detail.includes('WHERE clause required')) return 'PolicyGate: WHERE clause is required to prevent full scans.';
    if (detail.includes('Join limit exceeded')) return 'PolicyGate: Join count exceeds the allowed limit.';
    if (detail.includes('Only SELECT')) return 'PolicyGate: Only SELECT queries are permitted.';
    if (detail.includes('Write operations are not allowed')) return 'PolicyGate: Write operations are blocked.';
    return null;
  };

  const errorHints = (detail: string) => {
    const hints: string[] = [];
    if (detail.includes('WHERE clause required')) {
      hints.push('Add a WHERE clause to limit scope (required by policy).');
    }
    if (detail.includes('Join limit exceeded')) {
      hints.push('Reduce the number of JOINs to the configured limit.');
    }
    if (detail.includes('Only SELECT')) {
      hints.push('Use SELECT only. DML/DDL is blocked by policy.');
    }
    if (detail.includes('Budget limit exceeded')) {
      hints.push('Budget exceeded. Reduce usage or increase the limit.');
    }
    if (detail.includes('ORA-00942')) {
      hints.push('Table or view not found. Check schema or CURRENT_SCHEMA.');
    }
    return hints;
  };
  const warnings = (() => {
    const list: string[] = [];
    const risk = payload?.risk;
    if (risk?.risk >= 3) {
      list.push(`Risk score ${risk.risk} (intent: ${risk.intent}).`);
    }
    const w = payload?.final?.warnings;
    if (Array.isArray(w)) {
      w.forEach((item) => list.push(String(item)));
    } else if (typeof w === 'string' && w.trim()) {
      list.push(w.trim());
    }
    return list;
  })();

  useEffect(() => {
    setSortCol(null);
    setSortDir('asc');
    setPage(1);
    setPageSize(10);
    setHiddenCols(new Set());
  }, [previewData?.sql]);

  useEffect(() => {
    fetch('/query/demo/questions')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (data?.questions?.length) {
          setQuestions(data.questions);
        }
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    fetch('/admin/budget/status')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => setBudget(data))
      .catch(() => setBudget(null));
  }, []);

  useEffect(() => {
    if (budget) {
      setBudgetDraft({
        limit: String(budget.budget_limit_krw ?? ''),
        alert: String(budget.cost_alert_threshold_krw ?? ''),
      });
    }
  }, [budget?.budget_limit_krw, budget?.cost_alert_threshold_krw]);

  const columns = previewData?.preview?.columns || [];
  const rows = previewData?.preview?.rows || [];

  const visibleColumns = columns.filter((col: string) => !hiddenCols.has(col));
  const visibleIndexes = visibleColumns.map((col: string) => columns.indexOf(col));

  const sortedRows = (() => {
    if (!sortCol) return rows;
    const idx = columns.indexOf(sortCol);
    if (idx < 0) return rows;
    const dir = sortDir === 'asc' ? 1 : -1;
    const clone = [...rows];
    clone.sort((a: any[], b: any[]) => {
      const av = a[idx];
      const bv = b[idx];
      if (av == null && bv == null) return 0;
      if (av == null) return 1 * dir;
      if (bv == null) return -1 * dir;
      if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
      const ad = Date.parse(String(av));
      const bd = Date.parse(String(bv));
      if (!Number.isNaN(ad) && !Number.isNaN(bd)) return (ad - bd) * dir;
      return String(av).localeCompare(String(bv)) * dir;
    });
    return clone;
  })();

  const totalRows = sortedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
  const currentPage = Math.min(page, totalPages);
  const pageRows = sortedRows.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  const toggleColumn = (col: string) => {
    setHiddenCols((prev) => {
      const next = new Set(prev);
      if (next.has(col)) {
        next.delete(col);
      } else {
        next.add(col);
      }
      return next;
    });
  };

  const downloadCsv = () => {
    if (!previewData) return;
    const header = visibleColumns.join(',');
    const body = sortedRows
      .map((row: any[]) =>
        visibleIndexes
          .map((idx) => {
            const cell = row[idx];
            const text = cell == null ? '' : String(cell);
            if (/[\",\\n]/.test(text)) {
              return `"${text.replace(/\"/g, '""')}"`;
            }
            return text;
          })
          .join(','),
      )
      .join('\\n');
    const blob = new Blob([`${header}\\n${body}`], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'preview.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  const submit = async (q: string) => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await fetch('/query/oneshot', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: q }),
      });
      if (!res.ok) {
        const text = await res.text();
        let detail = text;
        try {
          const json = JSON.parse(text);
          if (json?.detail) detail = json.detail;
        } catch {}
        throw new Error(detail || `Request failed: ${res.status}`);
      }
      const data = await res.json();
      setResult(data);
      fetch('/admin/budget/status')
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => setBudget(d))
        .catch(() => {});
    } catch (err: any) {
      setError(err?.message || 'Request failed');
    } finally {
      setLoading(false);
    }
  };

  const saveBudget = async () => {
    setBudgetSaving(true);
    try {
      const limit = budgetDraft.limit ? Number(budgetDraft.limit) : null;
      const alert = budgetDraft.alert ? Number(budgetDraft.alert) : null;
      const res = await fetch('/admin/budget/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ budget_limit_krw: limit, cost_alert_threshold_krw: alert }),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `Request failed: ${res.status}`);
      }
      const data = await res.json();
      setBudget(data);
    } catch (err: any) {
      setError(err?.message || 'Failed to save budget config');
    } finally {
      setBudgetSaving(false);
    }
  };

  return (
    <main className="page">
      <header className="page-header">
        <div>
          <h1>Ask</h1>
          <p className="subtitle">Demo mode uses cached answers. Advanced mode runs a new plan.</p>
        </div>
        <div className="status-pill">{loading ? 'Running' : 'Demo Ready'}</div>
      </header>

      {budget && (
        <div className={`banner ${budget.over_limit ? 'banner-danger' : budget.over_alert ? 'banner-warn' : ''}`}>
          <strong>Budget</strong> {budget.total_krw} / {budget.budget_limit_krw} KRW
          <span> · Alert at {budget.cost_alert_threshold_krw} KRW</span>
          {budget.over_alert && !budget.over_limit && <span> · Approaching limit</span>}
          {budget.over_limit && <span> · Budget exceeded</span>}
        </div>
      )}

      <section className="layout">
        <div className="panel">
          <div className="card">
            <div className="card-title">Quick Questions</div>
            <div className="chip-group">
              {questions.map((q) => (
                <button key={q} className="chip" onClick={() => submit(q)} disabled={loading}>
                  {q}
                </button>
              ))}
            </div>
            <p className="hint">Click a question to use cached answers instantly.</p>
          </div>

          <div className="card">
            <div className="card-title">Ask your own</div>
            <textarea
              className="textarea"
              rows={4}
              placeholder="Describe what you want to see. Example: Average length of stay by diagnosis."
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
            />
            <div className="actions">
              <button className="primary-btn" onClick={() => submit(question)} disabled={loading || !question.trim()}>
                {loading ? 'Running…' : 'Ask'}
              </button>
              <span className="helper">Advanced mode may call LLM once.</span>
            </div>
          </div>

          <div className="card">
            <div className="card-title">Budget Settings</div>
            <div className="budget-grid">
              <label>
                Limit (KRW)
                <input
                  className="input"
                  type="number"
                  value={budgetDraft.limit}
                  onChange={(e) => setBudgetDraft({ ...budgetDraft, limit: e.target.value })}
                />
              </label>
              <label>
                Alert (KRW)
                <input
                  className="input"
                  type="number"
                  value={budgetDraft.alert}
                  onChange={(e) => setBudgetDraft({ ...budgetDraft, alert: e.target.value })}
                />
              </label>
            </div>
            <div className="actions">
              <button className="ghost-btn" onClick={saveBudget} disabled={budgetSaving}>
                {budgetSaving ? 'Saving…' : 'Save Budget'}
              </button>
              <span className="helper">Saved to server and applied immediately.</span>
            </div>
          </div>

          {error && (
            <div className="card error-card">
              <div className="card-title">Request Error</div>
              {policyMessage(error) && <div className="helper">{policyMessage(error)}</div>}
              <pre className="code-block">{error}</pre>
              {errorHints(error).length > 0 && (
                <ul className="policy-list">
                  {errorHints(error).map((hint, idx) => (
                    <li key={idx}>{hint}</li>
                  ))}
                </ul>
              )}
            </div>
          )}
        </div>

        <div className="panel">
          {result ? (
            <div className="card">
              <div className="card-title">Result Preview</div>
              {payload?.mode === 'advanced' && !previewData && (
                <div className="warning-box">
                  <strong>Advanced Mode</strong>
                  <p>Review SQL and acknowledge policy before execution.</p>
                  {qid && (
                    <a className="link-btn" href={`/review/${qid}`}>
                      Review & Run
                    </a>
                  )}
                  {warnings.length > 0 && (
                    <ul>
                      {warnings.map((w, idx) => (
                        <li key={idx}>{w}</li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
              {previewData ? (
                <>
                  <div className="badge-row">
                    <span className={`badge ${previewData.mode === 'demo' ? 'badge-demo' : 'badge-adv'}`}>
                      {previewData.mode}
                    </span>
                    <span className="badge badge-neutral">{previewData.source || 'unknown'}</span>
                    {payload?.mode === 'advanced' && qid && (
                      <a className="link-btn" href={`/review/${qid}`}>
                        Review & Run
                      </a>
                    )}
                  </div>

                  {warnings.length > 0 && (
                    <div className="warning-box">
                      <strong>Warnings</strong>
                      <ul>
                        {warnings.map((w, idx) => (
                          <li key={idx}>{w}</li>
                        ))}
                      </ul>
                    </div>
                  )}

                  <div className="stats">
                    <div>
                      <span className="stat-label">Rows</span>
                      <span className="stat-value">{previewData.rowCount ?? rows.length}</span>
                    </div>
                    <div>
                      <span className="stat-label">Row cap</span>
                      <span className="stat-value">{previewData.rowCap ?? '-'}</span>
                    </div>
                    <div>
                      <span className="stat-label">Columns</span>
                      <span className="stat-value">{columns.length}</span>
                    </div>
                  </div>

                  {previewData.sql && <pre className="code-block">{previewData.sql}</pre>}

                  <div className="toolbar">
                    <label className="select-label">
                      Page size
                      <select value={pageSize} onChange={(e) => setPageSize(Number(e.target.value))}>
                        <option value={5}>5</option>
                        <option value={10}>10</option>
                        <option value={25}>25</option>
                        <option value={50}>50</option>
                      </select>
                    </label>
                    <button className="ghost-btn" onClick={downloadCsv}>
                      Download CSV
                    </button>
                  </div>

                  <div className="column-toggles">
                    {columns.map((col: string) => (
                      <label key={col} className="toggle">
                        <input
                          type="checkbox"
                          checked={!hiddenCols.has(col)}
                          onChange={() => toggleColumn(col)}
                        />
                        <span>{col}</span>
                      </label>
                    ))}
                  </div>

                  <div className="table-wrap">
                    <table className="table">
                      <thead>
                        <tr>
                          {visibleColumns.map((col: string) => (
                            <th
                              key={col}
                              className={sortCol === col ? 'sorted' : ''}
                              onClick={() => {
                                if (sortCol === col) {
                                  setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
                                } else {
                                  setSortCol(col);
                                  setSortDir('asc');
                                }
                              }}
                            >
                              {col}
                              {sortCol === col ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ''}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {pageRows.map((row: any[], idx: number) => (
                          <tr key={idx}>
                            {visibleIndexes.map((cidx: number) => (
                              <td key={cidx}>{row[cidx] === null || row[cidx] === undefined ? '' : String(row[cidx])}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <div className="pagination">
                    <button className="ghost-btn" onClick={() => setPage(Math.max(1, currentPage - 1))} disabled={currentPage <= 1}>
                      Prev
                    </button>
                    <span>
                      Page {currentPage} / {totalPages}
                    </span>
                    <button className="ghost-btn" onClick={() => setPage(Math.min(totalPages, currentPage + 1))} disabled={currentPage >= totalPages}>
                      Next
                    </button>
                  </div>
                </>
              ) : (
                <div className="empty-state">
                  <h3>No preview yet</h3>
                  <p>Run a question to see a table preview and SQL.</p>
                </div>
              )}

              <details className="details">
                <summary>Raw JSON</summary>
                <pre className="code-block">{JSON.stringify(result, null, 2)}</pre>
              </details>
            </div>
          ) : (
            <div className="card empty-state">
              <h3>Results appear here</h3>
              <p>Pick a quick question or ask your own to generate a preview.</p>
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
