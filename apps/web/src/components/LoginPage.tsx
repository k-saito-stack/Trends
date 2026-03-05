/**
 * Login page — OCI style.
 * Reveal-from-bottom button, scramble title, animated line.
 */
import { useRef, useEffect } from "react";
import { gsap, useGSAP } from "../hooks/useGSAPSetup";
import { useScrambleText } from "../hooks/useScrambleText";

interface LoginPageProps {
  onLogin: () => void;
  error: string | null;
}

export default function LoginPage({ onLogin, error }: LoginPageProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cardRef = useRef<HTMLDivElement>(null);
  const titleRef = useRef<HTMLHeadingElement>(null);
  const lineRef = useRef<HTMLDivElement>(null);
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
      ).fromTo(
        lineRef.current,
        { scaleX: 0 },
        {
          scaleX: 1,
          duration: 0.5,
          ease: "power4.out",
          transformOrigin: "left center",
        },
        "-=0.2",
      );
    },
    { scope: containerRef },
  );

  // Title scramble on mount
  useEffect(() => {
    if (titleRef.current) {
      const timer = setTimeout(() => {
        scramble(titleRef.current!, "Trends");
      }, 400);
      return () => {
        clearTimeout(timer);
        cleanup();
      };
    }
  }, [scramble, cleanup]);

  // Button hover handlers
  const handleBtnEnter = () => {
    gsap.to(btnBgRef.current, {
      yPercent: 0,
      scaleX: 1,
      duration: 0.5,
      ease: "power4.out",
      overwrite: true,
    });
    gsap.to(btnTextRef.current, {
      color: "#e8e6e0",
      duration: 0.3,
      overwrite: true,
    });
  };

  const handleBtnLeave = () => {
    gsap.to(btnBgRef.current, {
      yPercent: 101,
      scaleX: 0.5,
      duration: 0.5,
      ease: "power4.out",
      overwrite: true,
    });
    gsap.to(btnTextRef.current, {
      color: "#1925aa",
      duration: 0.3,
      overwrite: true,
    });
  };

  return (
    <div
      ref={containerRef}
      className="min-h-screen flex items-center justify-center bg-oci-mercury p-4"
    >
      <div ref={cardRef} className="oci-card max-w-sm w-full p-10" style={{ opacity: 0 }}>
        <h1 ref={titleRef} className="oci-heading text-oci-blue text-5xl mb-2">
          Trends
        </h1>
        <p className="oci-label-sm text-oci-blue/50 mb-10">
          Trend Detection Platform
        </p>

        <div
          ref={lineRef}
          className="oci-line mb-8"
          style={{ transform: "scaleX(0)", transformOrigin: "left center" }}
        />

        <button
          onClick={onLogin}
          onMouseEnter={handleBtnEnter}
          onMouseLeave={handleBtnLeave}
          className="oci-btn w-full py-3 px-4"
        >
          <div ref={btnBgRef} className="oci-btn__bg" />
          <span ref={btnTextRef} className="oci-btn__text oci-label text-xs text-oci-blue">
            Sign in with Google
          </span>
        </button>

        {error && <p className="mt-4 text-oci-error text-sm">{error}</p>}

        <p className="mt-8 oci-label-sm text-oci-blue/40">
          @kodansha.co.jp accounts only
        </p>
      </div>
    </div>
  );
}
