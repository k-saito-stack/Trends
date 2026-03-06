/**
 * Scramble background canvas — grid of characters
 * that constantly scramble and react to cursor proximity.
 *
 * mode="page"   → fixed full-screen (default, for main bg)
 * mode="inline"  → absolute, fills parent container (for header etc.)
 */
import { useRef, useEffect } from "react";

const CHARS = "01234567890!@#$%^&*()+-=[]{}|;:,.<>?/~ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const CHAR_SIZE = 14;
const CURSOR_RADIUS = 120;
const SCRAMBLE_SPEED = 0.08;

interface Props {
  color?: [number, number, number];
  mode?: "page" | "inline";
}

export default function ScrambleBackground({
  color = [255, 255, 255],
  mode = "page",
}: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const mouseRef = useRef({ x: -9999, y: -9999 });
  const gridRef = useRef<{ char: string; nextSwap: number }[]>([]);
  const colorRef = useRef(color);
  colorRef.current = color;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const cvs = canvas;
    const context = ctx;
    let animId: number;
    let cols = 0;
    let rows = 0;

    function randomChar() {
      return CHARS[Math.floor(Math.random() * CHARS.length)];
    }

    function initGrid() {
      const total = cols * rows;
      const grid = gridRef.current;
      while (grid.length < total) {
        grid.push({ char: randomChar(), nextSwap: Math.random() * 60 });
      }
      grid.length = total;
    }

    function getSize() {
      if (mode === "inline") {
        const parent = cvs.parentElement;
        return {
          w: parent ? parent.clientWidth : cvs.clientWidth,
          h: parent ? parent.clientHeight : cvs.clientHeight,
        };
      }
      return { w: window.innerWidth, h: window.innerHeight };
    }

    function resize() {
      const dpr = window.devicePixelRatio || 1;
      const { w, h } = getSize();
      cvs.width = w * dpr;
      cvs.height = h * dpr;
      cvs.style.width = w + "px";
      cvs.style.height = h + "px";
      context.setTransform(dpr, 0, 0, dpr, 0, 0);
      cols = Math.ceil(w / CHAR_SIZE);
      rows = Math.ceil(h / CHAR_SIZE);
      initGrid();
    }

    function handleMouseMove(e: MouseEvent) {
      if (mode === "inline") {
        const rect = cvs.getBoundingClientRect();
        mouseRef.current.x = e.clientX - rect.left;
        mouseRef.current.y = e.clientY - rect.top;
      } else {
        mouseRef.current.x = e.clientX;
        mouseRef.current.y = e.clientY;
      }
    }

    function handleMouseLeave() {
      mouseRef.current.x = -9999;
      mouseRef.current.y = -9999;
    }

    resize();
    window.addEventListener("resize", resize);
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseleave", handleMouseLeave);

    function render() {
      const { w, h } = getSize();
      // Re-check size for inline mode (parent may resize without window resize)
      if (mode === "inline") {
        const dpr = window.devicePixelRatio || 1;
        if (Math.abs(cvs.width / dpr - w) > 1 || Math.abs(cvs.height / dpr - h) > 1) {
          resize();
        }
      }

      context.clearRect(0, 0, w, h);
      context.font = `${CHAR_SIZE}px "JetBrains Mono", ui-monospace, monospace`;
      context.textAlign = "center";
      context.textBaseline = "middle";

      const mx = mouseRef.current.x;
      const my = mouseRef.current.y;
      const grid = gridRef.current;
      const c = colorRef.current;

      for (let row = 0; row < rows; row++) {
        for (let col = 0; col < cols; col++) {
          const idx = row * cols + col;
          const cell = grid[idx];
          if (!cell) continue;

          const cx = col * CHAR_SIZE + CHAR_SIZE / 2;
          const cy = row * CHAR_SIZE + CHAR_SIZE / 2;

          const dx = cx - mx;
          const dy = cy - my;
          const dist = Math.sqrt(dx * dx + dy * dy);
          const inRange = dist < CURSOR_RADIUS;

          cell.nextSwap -= SCRAMBLE_SPEED;
          if (inRange) cell.nextSwap -= 0.3;
          if (cell.nextSwap <= 0) {
            cell.char = randomChar();
            cell.nextSwap = inRange
              ? 2 + Math.random() * 4
              : 20 + Math.random() * 80;
          }

          // Invisible at rest, fully visible near cursor
          if (!inRange) continue;
          const proximity = 1 - dist / CURSOR_RADIUS;
          const alpha = proximity * 0.9;

          let drawX = cx;
          let drawY = cy;
          if (dist > 0) {
            const pushStrength = proximity * 6;
            drawX += (dx / dist) * pushStrength;
            drawY += (dy / dist) * pushStrength;
          }

          context.fillStyle = `rgba(${c[0]}, ${c[1]}, ${c[2]}, ${alpha})`;
          context.fillText(cell.char, drawX, drawY);
        }
      }

      animId = requestAnimationFrame(render);
    }

    render();

    return () => {
      cancelAnimationFrame(animId);
      window.removeEventListener("resize", resize);
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseleave", handleMouseLeave);
    };
  }, [mode]);

  const className =
    mode === "inline"
      ? "pointer-events-none absolute inset-0 z-0"
      : "pointer-events-none fixed inset-0 z-0";

  return <canvas ref={canvasRef} className={className} style={{ display: "block" }} />;
}
