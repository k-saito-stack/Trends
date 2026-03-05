/**
 * useMagnetic — makes an element subtly move toward the mouse cursor.
 * Used on buttons/links for a modern interactive feel.
 * Strength controls how far the element moves (default 0.3 = 30% of offset).
 */
import { useRef, useCallback } from "react";
import { gsap } from "./useGSAPSetup";

export function useMagnetic(strength = 0.3) {
  const ref = useRef<HTMLElement>(null);

  const onMouseMove = useCallback(
    (e: React.MouseEvent) => {
      const el = ref.current;
      if (!el) return;
      const rect = el.getBoundingClientRect();
      const cx = rect.left + rect.width / 2;
      const cy = rect.top + rect.height / 2;
      const dx = (e.clientX - cx) * strength;
      const dy = (e.clientY - cy) * strength;
      gsap.to(el, {
        x: dx,
        y: dy,
        duration: 0.3,
        ease: "power4.out",
        overwrite: true,
      });
    },
    [strength],
  );

  const onMouseLeave = useCallback(() => {
    const el = ref.current;
    if (!el) return;
    gsap.to(el, {
      x: 0,
      y: 0,
      duration: 0.5,
      ease: "elastic.out(1, 0.5)",
      overwrite: true,
    });
  }, []);

  return { ref, onMouseMove, onMouseLeave };
}
