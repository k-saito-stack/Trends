/**
 * Root — OCI design system with GSAP + Lenis.
 * Integrates Loader, CustomScrollbar, GridLines, smooth scroll.
 */
import { useState, useCallback } from "react";
import Dashboard from "./components/Dashboard";
import Header from "./components/Header";
import LoginPage from "./components/LoginPage";
import SettingsDrawer from "./components/SettingsDrawer";
import Loader from "./components/Loader";
import CustomCursor from "./components/CustomCursor";
import CustomScrollbar from "./components/CustomScrollbar";
import GridLines from "./components/GridLines";
import { useAuth } from "./hooks/useAuth";
import { useLenis } from "./hooks/useLenis";

function getToday(): string {
  return new Date().toISOString().split("T")[0];
}

export default function App() {
  const { user, loading, error, login, logout } = useAuth();
  const [date, setDate] = useState(getToday());
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [loaderDone, setLoaderDone] = useState(false);

  // Smooth scroll
  useLenis();

  const handleLoaderComplete = useCallback(() => {
    setLoaderDone(true);
  }, []);

  // Show loader first
  if (!loaderDone) {
    return <Loader onComplete={handleLoaderComplete} />;
  }

  // Auth loading
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-oci-mercury">
        <span className="oci-label text-oci-blue/40">Loading...</span>
      </div>
    );
  }

  // Not authenticated
  if (!user) {
    return <LoginPage onLogin={login} error={error} />;
  }

  // Authenticated
  return (
    <div className="min-h-screen bg-oci-mercury">
      <CustomCursor />
      <CustomScrollbar />
      <GridLines />

      <Header
        date={date}
        onDateChange={setDate}
        onSettingsClick={() => setSettingsOpen(true)}
        onLogout={logout}
        userName={user.displayName}
      />

      <main className="relative z-10 max-w-5xl mx-auto px-6 lg:px-10 pt-8 pb-20">
        <Dashboard date={date} />
      </main>

      <SettingsDrawer
        isOpen={settingsOpen}
        onClose={() => setSettingsOpen(false)}
      />
    </div>
  );
}
