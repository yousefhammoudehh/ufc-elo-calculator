"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/rankings", label: "Rankings" },
  { href: "/fighters", label: "Fighters" },
  { href: "/events", label: "Events" },
];

export function NavBar() {
  const pathname = usePathname();

  return (
    <header className="border-b border-zinc-800 bg-zinc-950/80 backdrop-blur-sm sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 flex items-center gap-8">
        <Link
          href="/rankings"
          className="text-amber-500 font-bold text-lg tracking-tight whitespace-nowrap"
        >
          UFC ELO
        </Link>
        <nav className="flex gap-1">
          {links.map((link) => {
            const isActive = pathname.startsWith(link.href);
            return (
              <Link
                key={link.href}
                href={link.href}
                className={`px-3 py-1.5 rounded text-sm transition-colors ${
                  isActive
                    ? "bg-zinc-800 text-zinc-100"
                    : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/50"
                }`}
              >
                {link.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
