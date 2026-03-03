/**
 * Firebase Authentication hook.
 * Handles Google login and auth state observation.
 */
import { GoogleAuthProvider, onAuthStateChanged, signInWithPopup, signOut, type User } from "firebase/auth";
import { useEffect, useState } from "react";
import { auth } from "../firebase";

interface AuthState {
  user: User | null;
  loading: boolean;
  error: string | null;
}

export function useAuth() {
  const [state, setState] = useState<AuthState>({
    user: null,
    loading: true,
    error: null,
  });

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, (user) => {
      setState({ user, loading: false, error: null });
    });
    return unsubscribe;
  }, []);

  const login = async () => {
    try {
      setState((prev) => ({ ...prev, error: null }));
      const provider = new GoogleAuthProvider();
      await signInWithPopup(auth, provider);
    } catch (err) {
      setState((prev) => ({
        ...prev,
        error: err instanceof Error ? err.message : "Login failed",
      }));
    }
  };

  const logout = async () => {
    await signOut(auth);
  };

  return { ...state, login, logout };
}
