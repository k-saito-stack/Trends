/**
 * Dashboard header with date navigation and controls.
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
    <header className="bg-white shadow-sm border-b border-gray-200">
      <div className="max-w-4xl mx-auto px-4 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-bold text-gray-800">Trends</h1>
          <div className="flex items-center gap-1">
            <button
              onClick={handlePrevDay}
              className="px-2 py-1 text-gray-500 hover:text-gray-800 text-sm"
            >
              &lt;
            </button>
            <input
              type="date"
              value={date}
              onChange={(e) => onDateChange(e.target.value)}
              className="text-sm border border-gray-300 rounded px-2 py-1"
            />
            <button
              onClick={handleNextDay}
              className="px-2 py-1 text-gray-500 hover:text-gray-800 text-sm"
            >
              &gt;
            </button>
          </div>
        </div>

        <div className="flex items-center gap-3">
          {userName && (
            <span className="text-xs text-gray-500 hidden sm:inline">
              {userName}
            </span>
          )}
          <button
            onClick={onSettingsClick}
            className="text-gray-500 hover:text-gray-800 text-sm px-2 py-1"
          >
            設定
          </button>
          <button
            onClick={onLogout}
            className="text-gray-500 hover:text-gray-800 text-sm px-2 py-1"
          >
            ログアウト
          </button>
        </div>
      </div>
    </header>
  );
}
