"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

export default function Login() {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [isRegistering, setIsRegistering] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    
    try {
      if (isRegistering) {
        const res = await fetch("http://localhost:8000/api/auth/register", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ username, password })
        });
        if (!res.ok) {
          const data = await res.json();
          throw new Error(data.detail || "Registration failed");
        }
        setIsRegistering(false);
        setError("Registration successful! Please login.");
      } else {
        const formData = new URLSearchParams();
        formData.append("username", username);
        formData.append("password", password);
        
        const res = await fetch("http://localhost:8000/api/auth/login", {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: formData
        });
        if (!res.ok) {
          const data = await res.json();
          throw new Error(data.detail || "Login failed");
        }
        const data = await res.json();
        localStorage.setItem("token", data.access_token);
        // Dispatch custom event to trigger navbar update (or refresh)
        window.location.href = "/watchlist";
      }
    } catch (err: any) {
      setError(err.message);
    }
  };

  return (
    <div className="flex items-center justify-center min-h-[70vh]">
      <div className="w-full max-w-md p-8 rounded-2xl bg-zinc-900/80 border border-zinc-800 shadow-2xl backdrop-blur-md">
        <h2 className="text-3xl font-bold text-white mb-6 text-center">
          {isRegistering ? "Create Account" : "Welcome Back"}
        </h2>
        {error && <p className={`mb-4 text-sm text-center ${error.includes("successful") ? "text-green-500" : "text-red-500"}`}>{error}</p>}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-zinc-400 mb-1">Username</label>
            <input 
              type="text" 
              required
              className="w-full px-4 py-2 bg-zinc-950 border border-zinc-800 rounded-lg focus:outline-none focus:border-cyan-500 text-white transition-colors"
              value={username}
              onChange={e => setUsername(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-zinc-400 mb-1">Password</label>
            <input 
              type="password" 
              required
              className="w-full px-4 py-2 bg-zinc-950 border border-zinc-800 rounded-lg focus:outline-none focus:border-cyan-500 text-white transition-colors"
              value={password}
              onChange={e => setPassword(e.target.value)}
            />
          </div>
          <button 
            type="submit" 
            className="w-full py-2.5 bg-cyan-600 hover:bg-cyan-500 text-white font-medium rounded-lg transition-colors mt-2"
          >
            {isRegistering ? "Sign Up" : "Sign In"}
          </button>
        </form>
        <p className="mt-6 text-center text-sm text-zinc-500">
          {isRegistering ? "Already have an account?" : "Don't have an account?"}
          <button 
            onClick={() => { setIsRegistering(!isRegistering); setError(""); }} 
            className="ml-2 text-cyan-500 hover:text-cyan-400 font-medium"
          >
            {isRegistering ? "Login" : "Register"}
          </button>
        </p>
      </div>
    </div>
  );
}
