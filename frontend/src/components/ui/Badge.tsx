interface BadgeProps {
  variant: "win" | "loss" | "draw" | "nc" | "title";
  children: React.ReactNode;
}

const styles: Record<BadgeProps["variant"], string> = {
  win: "bg-green-500/15 text-green-400 border-green-500/30",
  loss: "bg-red-500/15 text-red-400 border-red-500/30",
  draw: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
  nc: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
  title: "bg-amber-500/15 text-amber-400 border-amber-500/30",
};

export function Badge({ variant, children }: BadgeProps) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${styles[variant]}`}
    >
      {children}
    </span>
  );
}

export function outcomeBadge(outcomeKey: string) {
  switch (outcomeKey) {
    case "W":
      return <Badge variant="win">W</Badge>;
    case "L":
      return <Badge variant="loss">L</Badge>;
    case "D":
      return <Badge variant="draw">D</Badge>;
    case "NC":
      return <Badge variant="nc">NC</Badge>;
    default:
      return <Badge variant="draw">{outcomeKey}</Badge>;
  }
}
