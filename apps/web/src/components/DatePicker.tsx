/**
 * OCI-styled custom date picker.
 * Replaces native <input type="date"> with a styled calendar dropdown.
 */
import { useState, useRef, useEffect, useCallback } from "react";

interface DatePickerProps {
  value: string; // "YYYY-MM-DD"
  onChange: (date: string) => void;
}

const WEEKDAYS = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

function daysInMonth(year: number, month: number): number {
  return new Date(year, month + 1, 0).getDate();
}

function pad2(n: number): string {
  return n.toString().padStart(2, "0");
}

function formatISO(y: number, m: number, d: number): string {
  return `${y}-${pad2(m + 1)}-${pad2(d)}`;
}

export default function DatePicker({ value, onChange }: DatePickerProps) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Parse the current value
  const [selYear, selMonth, selDay] = value.split("-").map(Number);

  // Calendar view state (which month is displayed)
  const [viewYear, setViewYear] = useState(selYear);
  const [viewMonth, setViewMonth] = useState(selMonth - 1); // 0-indexed

  // Sync view to value when value changes externally
  useEffect(() => {
    const [y, m] = value.split("-").map(Number);
    setViewYear(y);
    setViewMonth(m - 1);
  }, [value]);

  // Close on click outside
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const goToPrevMonth = useCallback(() => {
    setViewMonth((prev) => {
      if (prev === 0) {
        setViewYear((y) => y - 1);
        return 11;
      }
      return prev - 1;
    });
  }, []);

  const goToNextMonth = useCallback(() => {
    setViewMonth((prev) => {
      if (prev === 11) {
        setViewYear((y) => y + 1);
        return 0;
      }
      return prev + 1;
    });
  }, []);

  const selectDate = useCallback(
    (day: number) => {
      const iso = formatISO(viewYear, viewMonth, day);
      const today = new Date().toISOString().split("T")[0];
      if (iso <= today) {
        onChange(iso);
        setOpen(false);
      }
    },
    [viewYear, viewMonth, onChange],
  );

  const goToToday = useCallback(() => {
    const today = new Date();
    const iso = formatISO(today.getFullYear(), today.getMonth(), today.getDate());
    onChange(iso);
    setOpen(false);
  }, [onChange]);

  // Build calendar grid
  const totalDays = daysInMonth(viewYear, viewMonth);
  const firstDayOfWeek = new Date(viewYear, viewMonth, 1).getDay();
  const todayISO = new Date().toISOString().split("T")[0];

  const cells: (number | null)[] = [];
  for (let i = 0; i < firstDayOfWeek; i++) cells.push(null);
  for (let d = 1; d <= totalDays; d++) cells.push(d);

  // Display text
  const displayText = `${selYear}/${pad2(selMonth)}/${pad2(selDay)}`;

  return (
    <div ref={containerRef} className="relative">
      {/* Trigger button */}
      <button
        onClick={() => setOpen(!open)}
        className="oci-label bg-white/5 border border-oci-mercury/30 text-oci-mercury
                   px-3 py-1.5 outline-none hover:bg-white/10 hover:border-oci-mercury/50
                   transition-colors duration-300 cursor-pointer"
      >
        {displayText}
      </button>

      {/* Calendar dropdown */}
      {open && (
        <div
          className="absolute top-full left-0 mt-1 z-50 border border-white/20"
          style={{
            backgroundColor: "#111b80",
            minWidth: "280px",
            boxShadow: "0 8px 30px rgba(0,0,0,0.3)",
          }}
        >
          {/* Month/Year header */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-white/10">
            <button
              onClick={goToPrevMonth}
              className="text-oci-mercury/60 hover:text-oci-mercury transition-colors p-1"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 19l-7-7 7-7" />
              </svg>
            </button>
            <span className="oci-label text-oci-mercury tracking-wider">
              {viewYear} / {pad2(viewMonth + 1)}
            </span>
            <button
              onClick={goToNextMonth}
              className="text-oci-mercury/60 hover:text-oci-mercury transition-colors p-1"
            >
              <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5l7 7-7 7" />
              </svg>
            </button>
          </div>

          {/* Weekday labels */}
          <div className="grid grid-cols-7 px-3 pt-3">
            {WEEKDAYS.map((wd) => (
              <div
                key={wd}
                className="text-center oci-label-sm text-oci-mercury/40 py-1"
              >
                {wd}
              </div>
            ))}
          </div>

          {/* Day grid */}
          <div className="grid grid-cols-7 px-3 pb-3 gap-0.5">
            {cells.map((day, i) => {
              if (day === null) {
                return <div key={`empty-${i}`} />;
              }

              const iso = formatISO(viewYear, viewMonth, day);
              const isSelected = iso === value;
              const isToday = iso === todayISO;
              const isFuture = iso > todayISO;

              return (
                <button
                  key={day}
                  onClick={() => !isFuture && selectDate(day)}
                  disabled={isFuture}
                  className={[
                    "py-2 text-center text-xs font-mono transition-colors duration-150",
                    isSelected
                      ? "bg-oci-mercury text-oci-blue font-medium"
                      : isToday
                        ? "text-oci-mercury border border-oci-mercury/50"
                        : isFuture
                          ? "text-white/15 cursor-not-allowed"
                          : "text-white/70 hover:bg-white/15 hover:text-white cursor-pointer",
                  ].join(" ")}
                >
                  {day}
                </button>
              );
            })}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end px-4 py-2.5 border-t border-white/10">
            <button
              onClick={goToToday}
              className="oci-label-sm text-oci-mercury/60 hover:text-oci-mercury transition-colors"
            >
              Today
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
