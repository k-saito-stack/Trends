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
  isAdmin: boolean;
}

export function useAuth() {
  const [state, setState] = useState<AuthState>({
    user: null,
    loading: true,
    error: null,
    isAdmin: false,
  });

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (user) => {
      if (user) {
        const tokenResult = await user.getIdTokenResult();
        const isAdmin = tokenResult.claims.admin === true;
        setState({ user, loading: false, error: null, isAdmin });
      } else {
        setState({ user: null, loading: false, error: null, isAdmin: false });
      }
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
