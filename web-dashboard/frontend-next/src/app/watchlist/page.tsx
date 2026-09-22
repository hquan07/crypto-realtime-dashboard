"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { gql, useQuery, useMutation } from "@apollo/client";

const GET_ME = gql`
  query GetMe {
    me {
      id
      username
      watchlists {
        id
        symbol
      }
      alerts {
        id
        symbol
        condition
        threshold
        isActive
      }
    }
  }
`;

const ADD_WATCHLIST = gql`
  mutation AddWatchlist($symbol: String!) {
    addWatchlist(symbol: $symbol) {
      id
      symbol
    }
  }
`;

export default function Watchlist() {
  const router = useRouter();
  const [newSymbol, setNewSymbol] = useState("");
  
  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) {
      router.push("/login");
    }
  }, [router]);

  const { data, loading, error, refetch } = useQuery(GET_ME, {
    errorPolicy: "all"
  });
  
  const [addWatchlist, { loading: adding }] = useMutation(ADD_WATCHLIST, {
    onCompleted: () => {
      setNewSymbol("");
      refetch();
    }
  });

  if (loading) return <div className="text-center mt-20 text-zinc-500">Loading...</div>;
  if (error && !data) return <div className="text-center mt-20 text-red-500">Failed to load user data. Are you logged in?</div>;

  const handleAdd = (e: React.FormEvent) => {
    e.preventDefault();
    if (newSymbol.trim()) {
      addWatchlist({ variables: { symbol: newSymbol.toUpperCase() } });
    }
  };

  return (
    <div className="space-y-8">
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h2 className="text-3xl font-bold tracking-tight text-white">Your Watchlist</h2>
          <p className="text-zinc-400 mt-1">Hello, <span className="text-cyan-500 font-medium">{data?.me?.username}</span>! Manage your favorite pairs here.</p>
        </div>
        <form onSubmit={handleAdd} className="flex gap-2">
          <input 
            type="text" 
            placeholder="e.g. BTCUSDT" 
            value={newSymbol}
            onChange={(e) => setNewSymbol(e.target.value)}
            className="px-4 py-2 bg-zinc-900 border border-zinc-800 rounded-lg focus:outline-none focus:border-cyan-500 text-white"
          />
          <button 
            type="submit" 
            disabled={adding}
            className="px-6 py-2 bg-cyan-600 hover:bg-cyan-500 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
          >
            {adding ? "Adding..." : "Add"}
          </button>
        </form>
      </div>
      
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
        {data?.me?.watchlists?.length === 0 ? (
          <p className="col-span-full text-center py-12 text-zinc-500 border border-dashed border-zinc-800 rounded-2xl">
            Your watchlist is empty. Add a symbol to get started!
          </p>
        ) : (
          data?.me?.watchlists?.map((item: any) => (
            <div key={item.id} className="p-6 rounded-2xl bg-zinc-900/50 border border-zinc-800 shadow-xl flex items-center justify-between group hover:border-cyan-500/50 transition-colors">
              <span className="font-bold text-xl text-white">{item.symbol}</span>
              <button className="text-zinc-600 hover:text-red-500 transition-colors opacity-0 group-hover:opacity-100">
                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/></svg>
              </button>
            </div>
          ))
        )}
      </div>
      
      <div className="mt-12">
        <h3 className="text-2xl font-bold tracking-tight text-white mb-4">Active Alerts</h3>
        <div className="bg-zinc-900/50 border border-zinc-800 rounded-2xl overflow-hidden shadow-xl">
          <table className="w-full text-left text-sm text-zinc-400">
            <thead className="bg-zinc-900/80 text-xs uppercase border-b border-zinc-800">
              <tr>
                <th className="px-6 py-4">Symbol</th>
                <th className="px-6 py-4">Condition</th>
                <th className="px-6 py-4">Threshold</th>
                <th className="px-6 py-4 text-right">Status</th>
              </tr>
            </thead>
            <tbody>
              {data?.me?.alerts?.length === 0 ? (
                <tr>
                  <td colSpan={4} className="px-6 py-8 text-center text-zinc-500">No alerts configured.</td>
                </tr>
              ) : (
                data?.me?.alerts?.map((alert: any) => (
                  <tr key={alert.id} className="border-b border-zinc-800/50 hover:bg-zinc-800/20">
                    <td className="px-6 py-4 font-medium text-white">{alert.symbol}</td>
                    <td className="px-6 py-4 text-cyan-500">{alert.condition}</td>
                    <td className="px-6 py-4">${alert.threshold}</td>
                    <td className="px-6 py-4 text-right">
                      <span className={`px-2 py-1 text-[10px] uppercase font-bold rounded-full ${alert.isActive ? "bg-green-500/20 text-green-400" : "bg-zinc-700/50 text-zinc-400"}`}>
                        {alert.isActive ? "Active" : "Inactive"}
                      </span>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
