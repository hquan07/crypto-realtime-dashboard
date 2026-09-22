"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

export default function Navbar() {
  const pathname = usePathname();
  const router = useRouter();
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  useEffect(() => {
    const token = localStorage.getItem("token");
    setIsAuthenticated(!!token);
  }, [pathname]);

  const handleLogout = () => {
    localStorage.removeItem("token");
    setIsAuthenticated(false);
    router.push("/login");
  };

  return (
    <nav className="border-b border-zinc-800 bg-zinc-950/50 backdrop-blur-xl sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          <div className="flex items-center gap-8">
            <Link href="/" className="font-bold text-xl tracking-tighter text-white">
              CRYPTO<span className="text-cyan-500">DASH</span>
            </Link>
            <div className="hidden md:flex space-x-4">
              <Link href="/" className={`px-3 py-2 rounded-md text-sm font-medium transition-colors ${pathname === "/" ? "bg-zinc-800 text-white" : "text-zinc-400 hover:text-white hover:bg-zinc-800/50"}`}>Dashboard</Link>
              <Link href="/watchlist" className={`px-3 py-2 rounded-md text-sm font-medium transition-colors ${pathname === "/watchlist" ? "bg-zinc-800 text-white" : "text-zinc-400 hover:text-white hover:bg-zinc-800/50"}`}>Watchlist</Link>
            </div>
          </div>
          <div>
            {isAuthenticated ? (
              <button onClick={handleLogout} className="text-sm font-medium text-zinc-400 hover:text-white transition-colors">
                Logout
              </button>
            ) : (
              <Link href="/login" className="text-sm font-medium bg-cyan-600 hover:bg-cyan-500 text-white px-4 py-2 rounded-md transition-colors">
                Login
              </Link>
            )}
          </div>
        </div>
      </div>
    </nav>
  );
}
