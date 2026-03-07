const SERVER_API_URL = process.env.API_URL ?? "http://ufc-elo-calculator:80";

function getBaseUrl(): string {
  if (typeof window === "undefined") return SERVER_API_URL;
  // Browser-side: use relative URL (proxied via Next.js rewrites)
  return "";
}

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly detail: string,
  ) {
    super(`API ${status}: ${detail}`);
  }
}

export async function apiFetch<T>(
  path: string,
  params?: Record<string, string | number | undefined>,
  init?: RequestInit,
): Promise<T> {
  const base = getBaseUrl();
  const url =
    typeof window === "undefined"
      ? new URL(`${base}${path}`)
      : new URL(path, window.location.origin);

  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined) {
        url.searchParams.set(key, String(value));
      }
    }
  }

  const res = await fetch(url.toString(), {
    headers: { "Content-Type": "application/json" },
    ...init,
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new ApiError(res.status, body.detail ?? res.statusText);
  }

  return res.json() as Promise<T>;
}
