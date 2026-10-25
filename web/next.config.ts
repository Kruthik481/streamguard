import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // The console renders entirely from the live event stream, so nothing is prerendered or cached.
  agentRules: false,
  turbopack: {
    rules: {
      "*.css": {
        loaders: ["@tailwindcss/turbopack"],
        as: "*.css",
      },
    },
  },
};

export default nextConfig;
