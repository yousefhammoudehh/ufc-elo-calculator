"use client";

export default function ErrorPage({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="flex flex-col items-center justify-center py-24 text-zinc-400">
      <h2 className="text-xl font-semibold text-zinc-200 mb-2">Something went wrong</h2>
      <p className="text-sm mb-6">{error.message || "An unexpected error occurred."}</p>
      <button
        onClick={reset}
        className="px-4 py-2 text-sm rounded bg-zinc-800 border border-zinc-700 hover:bg-zinc-700 text-zinc-200 cursor-pointer transition-colors"
      >
        Try again
      </button>
    </div>
  );
}
