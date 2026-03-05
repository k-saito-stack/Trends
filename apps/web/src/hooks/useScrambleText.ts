/**
 * Text scramble effect — free alternative to GSAP ScrambleTextPlugin.
 * Characters resolve left-to-right from random noise to target text.
 */
import { useCallback, useRef } from "react";

const CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZx&oci0123456789#@%";
const DURATION = 400;

export function useScrambleText() {
  const rafRef = useRef<number>(0);

  const scramble = useCallback(
    (element: HTMLElement, targetText: string, duration = DURATION) => {
      cancelAnimationFrame(rafRef.current);

      const startTime = performance.now();
      const len = targetText.length;

      const animate = (now: number) => {
        const elapsed = now - startTime;
        const progress = Math.min(elapsed / duration, 1);
        const resolved = Math.floor(progress * len);

        let display = "";
        for (let i = 0; i < len; i++) {
          if (i < resolved) {
            display += targetText[i];
          } else {
            display += CHARS[Math.floor(Math.random() * CHARS.length)];
          }
        }
        element.textContent = display;

        if (progress < 1) {
          rafRef.current = requestAnimationFrame(animate);
        } else {
          element.textContent = targetText;
        }
      };

      rafRef.current = requestAnimationFrame(animate);
    },
    [],
  );

  const cleanup = useCallback(() => {
    cancelAnimationFrame(rafRef.current);
  }, []);

  return { scramble, cleanup };
}
