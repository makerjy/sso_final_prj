'use client';

import { useEffect, useState } from 'react';

export default function AdminPage() {
  const [budget, setBudget] = useState<any>(null);

  useEffect(() => {
    fetch('/admin/budget/status')
      .then((res) => res.json())
      .then(setBudget)
      .catch(() => setBudget(null));
  }, []);

  return (
    <main>
      <h1>Admin</h1>
      <p>Budget status:</p>
      <pre style={{ background: '#fff', padding: 16, borderRadius: 8 }}>
        {JSON.stringify(budget, null, 2)}
      </pre>
    </main>
  );
}
