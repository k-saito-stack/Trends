/**
 * Text scramble effect — free alternative to GSAP ScrambleTextPlugin.
 * Characters resolve left-to-right from random noise to target text.
 * Supports multiple concurrent scrambles (one per element).
 */
import { useCallback, useRef } from "react";

const CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZx&oci0123456789#@%";
const DURATION = 400;

export function useScrambleText() {
  const rafsRef = useRef<Map<HTMLElement, number>>(new Map());

  const scramble = useCallback(
    (element: HTMLElement, targetText: string, duration = DURATION): Promise<void> => {
      return new Promise((resolve) => {
        // Cancel any existing scramble on this element
        const existing = rafsRef.current.get(element);
        if (existing) cancelAnimationFrame(existing);

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
            rafsRef.current.set(element, requestAnimationFrame(animate));
          } else {
            element.textContent = targetText;
            rafsRef.current.delete(element);
            resolve();
          }
        };

        rafsRef.current.set(element, requestAnimationFrame(animate));
      });
    },
    [],
  );

  const cleanup = useCallback(() => {
    rafsRef.current.forEach((raf) => cancelAnimationFrame(raf));
    rafsRef.current.clear();
  }, []);

  return { scramble, cleanup };
}
