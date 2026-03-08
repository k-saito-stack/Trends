/**
 * Settings drawer — OCI style.
 * GSAP timeline open/close, stagger log entries, scramble title.
 */
import { useEffect, useRef, useCallback } from "react";
import { gsap } from "../hooks/useGSAPSetup";
import { useScrambleText } from "../hooks/useScrambleText";

interface SettingsDrawerProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function SettingsDrawer({ isOpen, onClose }: SettingsDrawerProps) {
  const backdropRef = useRef<HTMLDivElement>(null);
  const drawerRef = useRef<HTMLDivElement>(null);
  const titleRef = useRef<HTMLHeadingElement>(null);
  const { scramble } = useScrambleText();

  // Animate open
  useEffect(() => {
    if (isOpen) {
      gsap.set(backdropRef.current, { display: "block" });
      gsap.set(drawerRef.current, { display: "block" });

      const tl = gsap.timeline();
      tl.fromTo(
        backdropRef.current,
        { opacity: 0 },
        { opacity: 1, duration: 0.3, ease: "power4.out" },
      ).fromTo(
        drawerRef.current,
        { xPercent: 100 },
        { xPercent: 0, duration: 0.5, ease: "power4.out" },
        0,
      );

      if (titleRef.current) {
        setTimeout(() => {
          scramble(titleRef.current!, "SETTINGS");
        }, 300);
      }
    }
  }, [isOpen, scramble]);

  const handleClose = useCallback(() => {
    const tl = gsap.timeline({
      onComplete: () => {
        gsap.set(backdropRef.current, { display: "none" });
        gsap.set(drawerRef.current, { display: "none" });
        onClose();
      },
    });
    tl.to(drawerRef.current, {
      xPercent: 100,
      duration: 0.4,
      ease: "power4.inOut",
    }).to(backdropRef.current, { opacity: 0, duration: 0.3 }, 0);
  }, [onClose]);

  return (
    <>
      {/* Backdrop */}
      <div
        ref={backdropRef}
        className="fixed inset-0 z-40 bg-oci-navy/50"
        style={{ display: "none" }}
        onClick={handleClose}
      />

      {/* Drawer */}
      <div
        ref={drawerRef}
        className="fixed top-0 right-0 h-full w-80 bg-white border-l border-oci-blue z-50"
        style={{ display: "none" }}
      >
        <div className="p-5 border-b border-oci-blue/20 flex items-center justify-between">
          <h2 ref={titleRef} className="oci-heading text-oci-blue text-xl">
            SETTINGS
          </h2>
          <button
            onClick={handleClose}
            className="text-oci-blue/30 hover:text-oci-blue transition-colors duration-300 p-1"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1.5}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>

        <div className="p-5 overflow-y-auto h-full pb-20 hide-native-scrollbar">
          <div className="bg-oci-mercury p-4 border border-oci-blue/10">
            <h3 className="oci-label-sm text-oci-blue/50 mb-3">
              Settings Coming Soon
            </h3>
            <p className="text-oci-blue text-sm leading-relaxed font-sans">
              User-facing controls are currently in development.
            </p>
          </div>
        </div>
      </div>
    </>
  );
}
