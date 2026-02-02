import Link from 'next/link';

export default function Home() {
  return (
    <main>
      <h1>RAG SQL Demo</h1>
      <p>Choose a mode:</p>
      <ul>
        <li><Link href="/ask">Ask</Link></li>
        <li><Link href="/admin">Admin</Link></li>
      </ul>
    </main>
  );
}
