'use client';

import { useParams } from 'next/navigation';

export default function ResultsPage() {
  const params = useParams();
  const qid = Array.isArray(params?.qid) ? params.qid[0] : (params?.qid as string);

  return (
    <main>
      <h1>Results</h1>
      <p>Query ID: {qid}</p>
      <p>Use the Review step to run SQL and view results.</p>
    </main>
  );
}
