'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';

type HistoryItem = { ts: number; sql: string };

export default function ReviewPage() {
  const params = useParams();
  const qid = Array.isArray(params?.qid) ? params.qid[0] : (params?.qid as string);
  const [sql, setSql] = useState('');
  const [originalSql, setOriginalSql] = useState('');
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [payload, setPayload] = useState<any>(null);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [ack, setAck] = useState(false);
  const [budget, setBudget] = useState<any>(null);
  const [diffMode, setDiffMode] = useState<'inline' | 'side'>('side');

  useEffect(() => {
    fetch(`/query/get?qid=${qid}`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (data?.payload) {
          setPayload(data.payload);
          const finalSql = data.payload?.final?.final_sql || data.payload?.draft?.final_sql || '';
          setSql(finalSql);
          setOriginalSql(finalSql);
          setHistory([{ ts: Date.now(), sql: finalSql }]);
        }
      })
      .catch(() => {});
  }, [qid]);

  useEffect(() => {
    fetch('/admin/budget/status')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => setBudget(data))
      .catch(() => setBudget(null));
  }, []);

  const warnings = (() => {
    const list: string[] = [];
    const risk = payload?.risk;
    if (risk?.risk >= 3) {
      list.push(`Risk score ${risk.risk} (intent: ${risk.intent}).`);
    }
    const w = payload?.final?.warnings;
    if (Array.isArray(w)) {
      w.forEach((item: any) => list.push(String(item)));
    } else if (typeof w === 'string' && w.trim()) {
      list.push(w.trim());
    }
    return list;
  })();

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

  const recordChange = (nextSql: string) => {
    setHistory((prev) => {
      const last = prev[prev.length - 1];
      if (!last || last.sql !== nextSql) {
        return [...prev, { ts: Date.now(), sql: nextSql }];
      }
      return prev;
    });
  };

  const diffLines = (fromSql: string, toSql: string) => {
    const aLines = fromSql.split(/\r?\n/);
    const bLines = toSql.split(/\r?\n/);
    const max = Math.max(aLines.length, bLines.length);
    const out: { type: 'same' | 'add' | 'del'; text: string }[] = [];
    for (let i = 0; i < max; i += 1) {
      const a = aLines[i];
      const b = bLines[i];
      if (a === b) {
        if (a !== undefined) out.push({ type: 'same', text: a });
        continue;
      }
      if (a !== undefined) out.push({ type: 'del', text: a });
      if (b !== undefined) out.push({ type: 'add', text: b });
    }
    return out;
  };

  const sideBySide = () => {
    const left = originalSql.split(/\r?\n/);
    const right = sql.split(/\r?\n/);
    const max = Math.max(left.length, right.length);
    const rows = [];
    for (let i = 0; i < max; i += 1) {
      rows.push({
        left: left[i] ?? '',
        right: right[i] ?? '',
        changed: (left[i] ?? '') !== (right[i] ?? ''),
        line: i + 1,
      });
    }
    return rows;
  };

  const run = async () => {
    setError(null);
    setResult(null);
    try {
      const res = await fetch('/query/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ qid, sql, user_ack: true }),
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
      recordChange(sql);
      fetch('/admin/budget/status')
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => setBudget(d))
        .catch(() => {});
    } catch (err: any) {
      setError(err?.message || 'Request failed');
    }
  };

  const preview = result?.result;
  const columns = preview?.columns || [];
  const rows = preview?.rows || [];

  return (
    <main className="page">
      <header className="page-header">
        <div>
          <h1>Review & Run</h1>
          <p className="subtitle">Confirm SQL, acknowledge policy, then run against Oracle.</p>
        </div>
        <div className="status-pill">Query {qid}</div>
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
            <div className="card-title">Policy Checklist</div>
            <ul className="policy-list">
              <li>Only SELECT queries are allowed.</li>
              <li>WHERE clause is required.</li>
              <li>Join count is limited.</li>
              <li>Row cap and timeout are enforced.</li>
            </ul>
            <label className="toggle">
              <input type="checkbox" checked={ack} onChange={(e) => setAck(e.target.checked)} />
              <span>I acknowledge the policy checks above.</span>
            </label>
          </div>

          {warnings.length > 0 && (
            <div className="card warning-box">
              <strong>Warnings</strong>
              <ul>
                {warnings.map((w, idx) => (
                  <li key={idx}>{w}</li>
                ))}
              </ul>
            </div>
          )}

          {error && (
            <div className="card error-card">
              <div className="card-title">Run Error</div>
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
          <div className="card">
            <div className="card-title">SQL to Execute</div>
            <textarea
              className="textarea"
              rows={8}
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              onBlur={(e) => recordChange(e.target.value)}
            />
            <div className="actions">
              <button className="primary-btn" disabled={!ack || !sql.trim()} onClick={run}>
                Run Query
              </button>
              <span className="helper">Execution requires acknowledgment.</span>
            </div>
          </div>

          <div className="card">
            <div className="card-title">SQL Diff</div>
            <div className="toolbar">
              <label className="select-label">
                View
                <select value={diffMode} onChange={(e) => setDiffMode(e.target.value as 'inline' | 'side')}>
                  <option value="side">Side-by-side</option>
                  <option value="inline">Inline</option>
                </select>
              </label>
            </div>
            {diffMode === 'side' ? (
              <div className="diff-grid">
                <div className="diff-col">
                  <div className="diff-col-title">Original</div>
                  <pre className="diff">
                    {sideBySide().map((row, idx) => (
                      <span key={idx} className={`diff-line ${row.changed ? 'diff-del' : ''}`}>
                        {String(row.line).padStart(2, '0')} {row.left || ' '}
                        {'\n'}
                      </span>
                    ))}
                  </pre>
                </div>
                <div className="diff-col">
                  <div className="diff-col-title">Current</div>
                  <pre className="diff">
                    {sideBySide().map((row, idx) => (
                      <span key={idx} className={`diff-line ${row.changed ? 'diff-add' : ''}`}>
                        {String(row.line).padStart(2, '0')} {row.right || ' '}
                        {'\n'}
                      </span>
                    ))}
                  </pre>
                </div>
              </div>
            ) : (
              <pre className="diff">
                {diffLines(originalSql, sql).map((line, idx) => (
                  <span key={idx} className={`diff-line diff-${line.type}`}>
                    {line.type === 'add' ? '+ ' : line.type === 'del' ? '- ' : '  '}
                    {line.text}
                    {'\n'}
                  </span>
                ))}
              </pre>
            )}
          </div>

          <div className="card">
            <div className="card-title">Change History</div>
            {history.length <= 1 ? (
              <p className="helper">No edits yet.</p>
            ) : (
              <ul className="history-list">
                {history
                  .slice()
                  .reverse()
                  .map((item, idx) => (
                    <li key={idx}>
                      <span>{new Date(item.ts).toLocaleTimeString()}</span>
                      <button className="ghost-btn" onClick={() => setSql(item.sql)}>
                        Restore
                      </button>
                      <code>{item.sql.split(/\s+/).slice(0, 6).join(' ')}...</code>
                    </li>
                  ))}
              </ul>
            )}
          </div>

          {result && (
            <div className="card">
              <div className="card-title">Execution Result</div>
              {columns.length > 0 ? (
                <div className="table-wrap">
                  <table className="table">
                    <thead>
                      <tr>
                        {columns.map((col: string) => (
                          <th key={col}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row: any[], idx: number) => (
                        <tr key={idx}>
                          {row.map((cell: any, cidx: number) => (
                            <td key={cidx}>{cell === null || cell === undefined ? '' : String(cell)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <pre className="code-block">{JSON.stringify(result, null, 2)}</pre>
              )}
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
