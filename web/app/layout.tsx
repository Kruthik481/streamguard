import type { Metadata } from "next";
import { Barlow_Condensed, IBM_Plex_Mono, Shippori_Mincho_B1 } from "next/font/google";
import "./globals.css";

const display = Shippori_Mincho_B1({
  variable: "--font-display",
  subsets: ["latin"],
  weight: ["800"],
});

const ui = Barlow_Condensed({
  variable: "--font-ui",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
});

const mono = IBM_Plex_Mono({
  variable: "--font-plex-mono",
  subsets: ["latin"],
  weight: ["400", "600"],
});

export const metadata: Metadata = {
  title: "StreamGuard — real-time fraud detection",
  description: "Partitioned stream, idempotent consumer-group workers, live kill-a-worker demo.",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en" className={`${display.variable} ${ui.variable} ${mono.variable} h-full antialiased`}>
      <body className="min-h-full">{children}</body>
    </html>
  );
}
