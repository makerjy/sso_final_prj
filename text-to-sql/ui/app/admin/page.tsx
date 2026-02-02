'use client';

import { useEffect, useState } from 'react';

export default function AdminPage() {
  const [budget, setBudget] = useState<any>(null);
  const [budgetDraft, setBudgetDraft] = useState({ limit: '', alert: '' });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rag, setRag] = useState<any>(null);
  const [pool, setPool] = useState<any>(null);

  useEffect(() => {
    fetch('/admin/budget/status')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => setBudget(data))
      .catch(() => setBudget(null));
  }, []);

  useEffect(() => {
    fetch('/admin/rag/status')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => setRag(data))
      .catch(() => setRag(null));
    fetch('/admin/oracle/pool/status')
      .then((res) => (res.ok ? res.json() : null))
      .then((data) => setPool(data))
      .catch(() => setPool(null));
  }, []);

  useEffect(() => {
    if (budget) {
      setBudgetDraft({
        limit: String(budget.budget_limit_krw ?? ''),
        alert: String(budget.cost_alert_threshold_krw ?? ''),
      });
    }
  }, [budget?.budget_limit_krw, budget?.cost_alert_threshold_krw]);

  const saveBudget = async () => {
    setSaving(true);
    setError(null);
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
      setSaving(false);
    }
  };

  return (
    <main className="page">
      <header className="page-header">
        <div>
          <h1>Admin</h1>
          <p className="subtitle">Budget control and system status.</p>
        </div>
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
              <button className="ghost-btn" onClick={saveBudget} disabled={saving}>
                {saving ? 'Saving…' : 'Save Budget'}
              </button>
              <span className="helper">Saved to server and applied immediately.</span>
            </div>
            {error && <pre className="code-block">{error}</pre>}
          </div>

          <div className="card">
            <div className="card-title">RAG Status</div>
            <pre className="code-block">{JSON.stringify(rag, null, 2)}</pre>
          </div>

          <div className="card">
            <div className="card-title">Oracle Pool Status</div>
            <pre className="code-block">{JSON.stringify(pool, null, 2)}</pre>
          </div>
        </div>

        <div className="panel">
          <div className="card">
            <div className="card-title">Budget Status (Raw)</div>
            <pre className="code-block">{JSON.stringify(budget, null, 2)}</pre>
          </div>
        </div>
      </section>
    </main>
  );
}
