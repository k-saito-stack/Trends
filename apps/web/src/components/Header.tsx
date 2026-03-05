/**
 * Header — OCI style: blue bg, mercury text, mono uppercase labels.
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
    <header className="oci-section-blue">
      <div className="max-w-3xl mx-auto px-4 lg:px-10 py-5 flex items-center justify-between">
        {/* Left: Title + date nav */}
        <div className="flex items-center gap-5">
          <h1 className="oci-heading text-oci-mercury text-2xl">Trends</h1>

          <div className="flex items-center gap-2">
            <button
              onClick={handlePrevDay}
              className="oci-hover text-oci-mercury/70 p-1"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 19l-7-7 7-7" />
              </svg>
            </button>
            <input
              type="date"
              value={date}
              onChange={(e) => onDateChange(e.target.value)}
              className="oci-label bg-transparent border border-oci-mercury/30 text-oci-mercury px-3 py-1.5 outline-none"
            />
            <button
              onClick={handleNextDay}
              className="oci-hover text-oci-mercury/70 p-1"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5l7 7-7 7" />
              </svg>
            </button>
          </div>
        </div>

        {/* Right: user + controls */}
        <div className="flex items-center gap-4">
          {userName && (
            <span className="oci-label text-oci-mercury/50 hidden sm:inline text-[0.625rem]">
              {userName}
            </span>
          )}
          <button
            onClick={onSettingsClick}
            className="oci-hover text-oci-mercury/70 p-1"
            title="Settings"
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
            </svg>
          </button>
          <button
            onClick={onLogout}
            className="oci-hover oci-label border border-oci-mercury/30 text-oci-mercury px-4 py-1.5 text-[0.625rem]"
          >
            Logout
          </button>
        </div>
      </div>
    </header>
  );
}
