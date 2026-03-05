/**
 * Custom scrollbar — OCI style.
 * Fixed right edge, syncs with scroll position.
 * Mercury handle + blue border, inverts on hover.
 * Hidden on touch devices via CSS.
 */
import { useEffect, useRef, useCallback } from "react";

export default function CustomScrollbar() {
  const handleRef = useRef<HTMLDivElement>(null);
  const hideTimerRef = useRef<number>(0);

  const updatePosition = useCallback(() => {
    const handle = handleRef.current;
    if (!handle) return;

    const scrollY = window.scrollY;
    const docHeight = document.documentElement.scrollHeight;
    const viewHeight = window.innerHeight;

    if (docHeight <= viewHeight) {
      handle.classList.remove("visible");
      return;
    }

    const scrollRatio = scrollY / (docHeight - viewHeight);
    const handleHeight = Math.max(40, (viewHeight / docHeight) * viewHeight);
    const maxTop = viewHeight - handleHeight;
    const top = scrollRatio * maxTop;

    handle.style.height = `${handleHeight}px`;
    handle.style.top = `${top}px`;
    handle.classList.add("visible");

    clearTimeout(hideTimerRef.current);
    hideTimerRef.current = window.setTimeout(() => {
      handle.classList.remove("visible");
    }, 1500);
  }, []);

  useEffect(() => {
    window.addEventListener("scroll", updatePosition, { passive: true });
    window.addEventListener("resize", updatePosition, { passive: true });
    updatePosition();

    return () => {
      window.removeEventListener("scroll", updatePosition);
      window.removeEventListener("resize", updatePosition);
      clearTimeout(hideTimerRef.current);
    };
  }, [updatePosition]);

  return (
    <div className="oci-scrollbar">
      <div className="oci-scrollbar__track">
        <div ref={handleRef} className="oci-scrollbar__handle" />
      </div>
    </div>
  );
}
