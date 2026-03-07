import Link from "next/link";

export default function FighterNotFound() {
  return (
    <div className="flex flex-col items-center justify-center py-24 text-zinc-400">
      <h2 className="text-xl font-semibold text-zinc-200 mb-2">Fighter not found</h2>
      <p className="text-sm mb-6">
        The fighter you&apos;re looking for doesn&apos;t exist or has been removed.
      </p>
      <Link
        href="/fighters"
        className="px-4 py-2 text-sm rounded bg-zinc-800 border border-zinc-700 hover:bg-zinc-700 text-zinc-200 transition-colors"
      >
        Browse fighters
      </Link>
    </div>
  );
}
