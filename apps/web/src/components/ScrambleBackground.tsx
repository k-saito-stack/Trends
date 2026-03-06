/**
 * Scramble background canvas — full-screen grid of characters
 * that constantly scramble and react to cursor proximity.
 */
import { useRef, useEffect } from "react";

const CHARS = "01234567890!@#$%^&*()+-=[]{}|;:,.<>?/~ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const CHAR_SIZE = 14;
const COLOR = [255, 255, 255]; // white
const CURSOR_RADIUS = 120;
const SCRAMBLE_SPEED = 0.08; // lower = slower character change rate

export default function ScrambleBackground() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const mouseRef = useRef({ x: -9999, y: -9999 });
  const gridRef = useRef<{ char: string; nextSwap: number }[]>([]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    // Local aliases so TS knows they are non-null inside nested fns
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
      // Reuse existing entries, extend or shrink
      while (grid.length < total) {
        grid.push({ char: randomChar(), nextSwap: Math.random() * 60 });
      }
      grid.length = total;
    }

    function resize() {
      const dpr = window.devicePixelRatio || 1;
      const w = window.innerWidth;
      const h = window.innerHeight;
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
      mouseRef.current.x = e.clientX;
      mouseRef.current.y = e.clientY;
    }

    function handleMouseLeave() {
      mouseRef.current.x = -9999;
      mouseRef.current.y = -9999;
    }

    resize();
    window.addEventListener("resize", resize);
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseleave", handleMouseLeave);

    let frameCount = 0;

    function render() {
      frameCount++;
      const w = cvs.width / (window.devicePixelRatio || 1);
      const h = cvs.height / (window.devicePixelRatio || 1);
      context.clearRect(0, 0, w, h);
      context.font = `${CHAR_SIZE}px "JetBrains Mono", ui-monospace, monospace`;
      context.textAlign = "center";
      context.textBaseline = "middle";

      const mx = mouseRef.current.x;
      const my = mouseRef.current.y;
      const grid = gridRef.current;

      for (let row = 0; row < rows; row++) {
        for (let col = 0; col < cols; col++) {
          const idx = row * cols + col;
          const cell = grid[idx];
          if (!cell) continue;

          const cx = col * CHAR_SIZE + CHAR_SIZE / 2;
          const cy = row * CHAR_SIZE + CHAR_SIZE / 2;

          // Distance to cursor
          const dx = cx - mx;
          const dy = cy - my;
          const dist = Math.sqrt(dx * dx + dy * dy);
          const inRange = dist < CURSOR_RADIUS;

          // Base scramble: each cell has its own timer
          cell.nextSwap -= SCRAMBLE_SPEED;
          if (inRange) {
            // Near cursor: scramble much faster
            cell.nextSwap -= 0.3;
          }
          if (cell.nextSwap <= 0) {
            cell.char = randomChar();
            cell.nextSwap = inRange
              ? 2 + Math.random() * 4     // fast swap near cursor
              : 20 + Math.random() * 80;  // slow ambient swap
          }

          // Only render near cursor — invisible otherwise
          if (!inRange) continue;
          const proximity = 1 - dist / CURSOR_RADIUS;
          const alpha = proximity * 0.45;

          // Draw position: slight displacement near cursor
          let drawX = cx;
          let drawY = cy;
          if (inRange && dist > 0) {
            const pushStrength = (1 - dist / CURSOR_RADIUS) * 6;
            drawX += (dx / dist) * pushStrength;
            drawY += (dy / dist) * pushStrength;
          }

          context.fillStyle = `rgba(${COLOR[0]}, ${COLOR[1]}, ${COLOR[2]}, ${alpha})`;
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
  }, []);

  return (
    <canvas
      ref={canvasRef}
      className="pointer-events-none fixed inset-0 z-0"
      style={{ display: "block" }}
    />
  );
}
