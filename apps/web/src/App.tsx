/**
 * Main application — OCI design system.
 * Mercury (#e8e6e0) base, Blue (#1925aa) accent.
 */
import { useState } from "react";
import Dashboard from "./components/Dashboard";
import Header from "./components/Header";
import LoginPage from "./components/LoginPage";
import SettingsDrawer from "./components/SettingsDrawer";
import { useAuth } from "./hooks/useAuth";

function getToday(): string {
  return new Date().toISOString().split("T")[0];
}

export default function App() {
  const { user, loading, error, login, logout } = useAuth();
  const [date, setDate] = useState(getToday());
  const [settingsOpen, setSettingsOpen] = useState(false);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-oci-mercury">
        <span className="oci-label text-oci-blue opacity-50">LOADING...</span>
      </div>
    );
  }

  if (!user) {
    return <LoginPage onLogin={login} error={error} />;
  }

  return (
    <div className="min-h-screen bg-oci-mercury">
      <Header
        date={date}
        onDateChange={setDate}
        onSettingsClick={() => setSettingsOpen(true)}
        onLogout={logout}
        userName={user.displayName}
      />

      <main className="max-w-3xl mx-auto px-4 lg:px-10 pt-6 pb-20">
        <Dashboard date={date} />
      </main>

      <SettingsDrawer
        isOpen={settingsOpen}
        onClose={() => setSettingsOpen(false)}
      />
    </div>
  );
}
