/**
 * Main application component.
 * Handles auth guard and renders dashboard or login page.
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
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-gray-400">読み込み中...</div>
      </div>
    );
  }

  if (!user) {
    return <LoginPage onLogin={login} error={error} />;
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Header
        date={date}
        onDateChange={setDate}
        onSettingsClick={() => setSettingsOpen(true)}
        onLogout={logout}
        userName={user.displayName}
      />

      <main className="max-w-4xl mx-auto px-4 py-4">
        <Dashboard date={date} />
      </main>

      <SettingsDrawer
        isOpen={settingsOpen}
        onClose={() => setSettingsOpen(false)}
      />
    </div>
  );
}
