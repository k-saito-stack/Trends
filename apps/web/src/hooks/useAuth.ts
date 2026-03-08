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
        try {
          const tokenResult = await user.getIdTokenResult();
          const isAdmin = tokenResult.claims.admin === true;
          setState({ user, loading: false, error: null, isAdmin });
        } catch (err) {
          console.error("Failed to resolve auth claims", err);
          setState({
            user,
            loading: false,
            error: "Signed in, but account details could not be verified.",
            isAdmin: false,
          });
        }
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
      console.error("Login failed", err);
      setState((prev) => ({
        ...prev,
        error: "Login failed. Please try again.",
      }));
    }
  };

  const logout = async () => {
    try {
      await signOut(auth);
    } catch (err) {
      console.error("Logout failed", err);
      setState((prev) => ({
        ...prev,
        error: "Logout failed. Please try again.",
      }));
    }
  };

  return { ...state, login, logout };
}
