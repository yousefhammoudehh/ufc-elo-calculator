import type { NextConfig } from "next";

const config: NextConfig = {
  output: "standalone",
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "**.ufc.com" },
      { protocol: "https", hostname: "dmxg5wxfqgb4u.cloudfront.net" },
    ],
  },
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${process.env.API_URL ?? "http://ufc-elo-calculator:80"}/api/:path*`,
      },
    ];
  },
};

export default config;
