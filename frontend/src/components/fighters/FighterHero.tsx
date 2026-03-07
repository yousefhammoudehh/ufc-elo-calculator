import Image from "next/image";
import type { FighterProfileResponse } from "@/lib/types/fighter";

interface Props {
  profile: FighterProfileResponse;
}

export function FighterHero({ profile }: Props) {
  const details = [
    profile.nickname && { label: "Nickname", value: `"${profile.nickname}"` },
    profile.birth_date && { label: "Born", value: profile.birth_date },
    profile.birth_place && { label: "From", value: profile.birth_place },
    profile.fighting_out_of && { label: "Fights out of", value: profile.fighting_out_of },
    profile.affiliation_gym && { label: "Gym", value: profile.affiliation_gym },
    profile.foundation_style && { label: "Style", value: profile.foundation_style },
    profile.stance && { label: "Stance", value: profile.stance },
    profile.height_cm != null && {
      label: "Height",
      value: `${profile.height_cm} cm`,
    },
    profile.reach_cm != null && {
      label: "Reach",
      value: `${profile.reach_cm} cm`,
    },
  ].filter(Boolean) as { label: string; value: string }[];

  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-6">
      <div className="flex items-start gap-6">
        {profile.profile_image_url ? (
          <Image
            src={profile.profile_image_url}
            alt={profile.display_name}
            width={96}
            height={96}
            className="w-24 h-24 rounded-lg object-cover bg-zinc-800"
            unoptimized
          />
        ) : (
          <div className="w-24 h-24 rounded-lg bg-zinc-800 flex items-center justify-center text-zinc-600 text-3xl font-bold">
            {profile.display_name.charAt(0)}
          </div>
        )}
        <div className="flex-1 min-w-0">
          <h1 className="text-2xl font-bold text-zinc-100">{profile.display_name}</h1>
          {profile.nickname && (
            <p className="text-amber-400 text-sm mt-0.5">&quot;{profile.nickname}&quot;</p>
          )}
          <div className="mt-4 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-2">
            {details.map((d) => (
              <div key={d.label}>
                <span className="text-xs text-zinc-500 uppercase tracking-wider">
                  {d.label}
                </span>
                <p className="text-sm text-zinc-200">{d.value}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
