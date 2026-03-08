/**
 * Login page — OCI style.
 * Reveal-from-bottom button, scramble title, animated line.
 */
import { useRef, useEffect } from "react";
import { gsap, useGSAP } from "../hooks/useGSAPSetup";
import { useScrambleText } from "../hooks/useScrambleText";
import ScrambleBackground from "./ScrambleBackground";

interface LoginPageProps {
  onLogin: () => void;
  error: string | null;
}

export default function LoginPage({ onLogin, error }: LoginPageProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cardRef = useRef<HTMLDivElement>(null);
  const titleRef = useRef<HTMLHeadingElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);
  const btnBgRef = useRef<HTMLDivElement>(null);
  const btnTextRef = useRef<HTMLSpanElement>(null);
  const { scramble, cleanup } = useScrambleText();

  // Entry animation
  useGSAP(
    () => {
      const tl = gsap.timeline({ delay: 0.2 });

      tl.fromTo(
        cardRef.current,
        { opacity: 0, y: 30 },
        { opacity: 1, y: 0, duration: 0.5, ease: "power4.out" },
      );
    },
    { scope: containerRef },
  );

  // Title scramble: on mount + periodic (same as Header)
  useEffect(() => {
    const run = () => {
      if (titleRef.current) scramble(titleRef.current, "TRENDS", 500);
    };
    const initTimer = setTimeout(run, 400);
    const interval = setInterval(run, 8000);
    return () => {
      clearTimeout(initTimer);
      clearInterval(interval);
      cleanup();
    };
  }, [scramble, cleanup]);

  // Direction-aware button hover (same style as TrendCard)
  const getDirection = (e: React.MouseEvent, el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    const nx = ((e.clientX - rect.left) / rect.width - 0.5) * 2;
    const ny = ((e.clientY - rect.top) / rect.height - 0.5) * 2;
    if (Math.abs(nx) > Math.abs(ny)) {
      return nx > 0
        ? { from: { scaleX: 0, scaleY: 1 }, origin: "right center" }
        : { from: { scaleX: 0, scaleY: 1 }, origin: "left center" };
    }
    return ny > 0
      ? { from: { scaleY: 0, scaleX: 1 }, origin: "center bottom" }
      : { from: { scaleY: 0, scaleX: 1 }, origin: "center top" };
  };

  const handleBtnEnter = (e: React.MouseEvent) => {
    if (btnBgRef.current && btnRef.current) {
      const { from, origin } = getDirection(e, btnRef.current);
      btnBgRef.current.style.transformOrigin = origin;
      gsap.fromTo(btnBgRef.current, { ...from }, {
        scaleX: 1, scaleY: 1, duration: 0.4, ease: "power4.out", overwrite: true,
      });
    }
    gsap.to(btnTextRef.current, {
      color: "#ffffff",
      duration: 0.3,
      overwrite: true,
    });
  };

  const handleBtnLeave = (e: React.MouseEvent) => {
    if (btnBgRef.current && btnRef.current) {
      const { from, origin } = getDirection(e, btnRef.current);
      btnBgRef.current.style.transformOrigin = origin;
      gsap.to(btnBgRef.current, {
        ...from, duration: 0.4, ease: "power4.out", overwrite: true,
      });
    }
    gsap.to(btnTextRef.current, {
      color: "#1925aa",
      duration: 0.3,
      overwrite: true,
    });
  };

  return (
    <div
      ref={containerRef}
      className="relative min-h-screen flex items-center justify-center overflow-hidden bg-oci-mercury p-4"
    >
      <ScrambleBackground />
      <div
        ref={cardRef}
        className="relative z-10 oci-card max-w-sm w-full px-10 py-8 text-center"
        style={{ opacity: 0 }}
      >
        <h1 ref={titleRef} className="oci-heading text-oci-blue text-6xl">
          TRENDS
        </h1>
        <button
          ref={btnRef}
          onClick={onLogin}
          onMouseEnter={handleBtnEnter}
          onMouseLeave={handleBtnLeave}
          className="oci-btn w-full py-3 px-4 mt-8"
        >
          <div
            ref={btnBgRef}
            style={{
              position: "absolute",
              inset: 0,
              backgroundColor: "#1925aa",
              transform: "scaleY(0)",
              transformOrigin: "center bottom",
              zIndex: 0,
              pointerEvents: "none",
            }}
          />
          <span ref={btnTextRef} className="oci-btn__text oci-label text-xs text-oci-blue">
            Sign in with Google
          </span>
        </button>

        {error && <p className="mt-4 text-oci-error text-sm">{error}</p>}

        <p className="mt-5 oci-label-sm text-oci-blue/40">
          @kodansha.co.jp accounts only
        </p>
      </div>
    </div>
  );
}
