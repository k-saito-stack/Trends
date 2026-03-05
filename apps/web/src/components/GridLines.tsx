/**
 * Background grid lines — OCI style.
 * Fixed 1px vertical lines that animate in with scaleY.
 */
import { useRef } from "react";
import { gsap, useGSAP } from "../hooks/useGSAPSetup";

export default function GridLines() {
  const containerRef = useRef<HTMLDivElement>(null);

  useGSAP(
    () => {
      gsap.fromTo(
        ".grid-line",
        { scaleY: 0 },
        {
          scaleY: 1,
          duration: 0.8,
          ease: "power4.out",
          stagger: 0.15,
          delay: 0.3,
        },
      );
    },
    { scope: containerRef },
  );

  return (
    <div
      ref={containerRef}
      className="pointer-events-none fixed inset-0 z-0 hidden lg:block"
    >
      <div className="max-w-5xl mx-auto h-full px-10 flex justify-between">
        <div className="grid-line oci-line-v h-full opacity-10" />
        <div className="grid-line oci-line-v h-full opacity-[0.07]" />
        <div className="grid-line oci-line-v h-full opacity-10" />
      </div>
    </div>
  );
}
