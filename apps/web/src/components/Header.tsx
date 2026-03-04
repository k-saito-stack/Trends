/**
 * Dashboard header — blue background with white text and SVG icons.
 */
interface HeaderProps {
  date: string;
  onDateChange: (date: string) => void;
  onSettingsClick: () => void;
  onLogout: () => void;
  userName: string | null;
}

export default function Header({
  date,
  onDateChange,
  onSettingsClick,
  onLogout,
  userName,
}: HeaderProps) {
  const handlePrevDay = () => {
    const d = new Date(date);
    d.setDate(d.getDate() - 1);
    onDateChange(d.toISOString().split("T")[0]);
  };

  const handleNextDay = () => {
    const d = new Date(date);
    d.setDate(d.getDate() + 1);
    const today = new Date().toISOString().split("T")[0];
    const next = d.toISOString().split("T")[0];
    if (next <= today) {
      onDateChange(next);
    }
  };

  return (
    <header className="bg-[#0039d6]">
      <div className="max-w-2xl mx-auto px-4 py-3 flex items-center justify-between">
        {/* Left: title + date nav */}
        <div className="flex items-center gap-3">
          <h1 className="text-white text-lg font-medium">Trends</h1>
          <div className="flex items-center gap-1">
            <button
              onClick={handlePrevDay}
              className="text-white/70 hover:text-white p-1"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </button>
            <input
              type="date"
              value={date}
              onChange={(e) => onDateChange(e.target.value)}
              className="text-sm bg-transparent border border-white/30 text-white px-2 py-1 outline-none"
            />
            <button
              onClick={handleNextDay}
              className="text-white/70 hover:text-white p-1"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
            </button>
          </div>
        </div>

        {/* Right: user + settings + logout */}
        <div className="flex items-center gap-2">
          {userName && (
            <span className="text-xs text-white/50 hidden sm:inline">
              {userName}
            </span>
          )}
          <button
            onClick={onSettingsClick}
            className="text-white/70 hover:text-white p-1"
            title="設定"
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
            </svg>
          </button>
          <button
            onClick={onLogout}
            className="text-white/70 hover:text-white text-xs border border-white/30 px-3 py-1"
          >
            ログアウト
          </button>
        </div>
      </div>
    </header>
  );
}
