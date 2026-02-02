'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';

export default function ReviewPage() {
  const params = useParams();
  const qid = Array.isArray(params?.qid) ? params.qid[0] : (params?.qid as string);
  const [sql, setSql] = useState('');
  const [payload, setPayload] = useState<any>(null);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [ack, setAck] = useState(false);
  const [budget, setBudget] = useState<any>(null);

  useEffect(() => {
    fetch(`/query/get?qid=${qid}`)
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => {
        if (data?.payload) {
          setPayload(data.payload);
          const finalSql = data.payload?.final?.final_sql || data.payload?.draft?.final_sql || '';
          setSql(finalSql);
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
            />
            <div className="actions">
              <button className="primary-btn" disabled={!ack || !sql.trim()} onClick={run}>
                Run Query
              </button>
              <span className="helper">Execution requires acknowledgment.</span>
            </div>
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
