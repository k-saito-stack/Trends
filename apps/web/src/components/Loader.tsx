/**
 * Full-screen loader — OCI style.
 * Mercury→blue→mercury bg transition + circle scale + logo rotation.
 * Skipped on subsequent visits via sessionStorage.
 */
import { useRef, useState, useEffect } from "react";
import { gsap, useGSAP } from "../hooks/useGSAPSetup";

interface LoaderProps {
  onComplete: () => void;
}

export default function Loader({ onComplete }: LoaderProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const circleRef = useRef<HTMLDivElement>(null);
  const textRef = useRef<HTMLSpanElement>(null);
  const [skip] = useState(() => sessionStorage.getItem("loader_done") === "1");

  useEffect(() => {
    if (skip) {
      onComplete();
    }
  }, [skip, onComplete]);

  useGSAP(
    () => {
      if (skip) return;

      const tl = gsap.timeline({
        onComplete: () => {
          sessionStorage.setItem("loader_done", "1");
          onComplete();
        },
      });

      tl.to(circleRef.current, {
        scale: 1,
        duration: 0.6,
        ease: "power4.out",
      })
        .to(
          circleRef.current,
          {
            rotation: 360,
            duration: 1.0,
            ease: "power4.inOut",
          },
          0,
        )
        .to(
          containerRef.current,
          {
            backgroundColor: "#1925aa",
            duration: 0.5,
            ease: "power4.out",
          },
          0.4,
        )
        .to(
          textRef.current,
          {
            color: "#e8e6e0",
            duration: 0.3,
          },
          0.4,
        )
        .to(
          circleRef.current,
          {
            borderColor: "#e8e6e0",
            duration: 0.3,
          },
          0.4,
        )
        .to(
          containerRef.current,
          {
            backgroundColor: "#e8e6e0",
            duration: 0.4,
            ease: "power4.inOut",
          },
          1.0,
        )
        .to(
          textRef.current,
          {
            color: "#1925aa",
            duration: 0.3,
          },
          1.0,
        )
        .to(
          circleRef.current,
          {
            borderColor: "#1925aa",
            duration: 0.3,
          },
          1.0,
        )
        .to(
          containerRef.current,
          {
            opacity: 0,
            duration: 0.3,
            ease: "power4.out",
          },
          1.5,
        )
        .set(containerRef.current, { display: "none" });
    },
    { scope: containerRef },
  );

  if (skip) return null;

  return (
    <div ref={containerRef} className="oci-loader">
      <div ref={circleRef} className="oci-loader__circle">
        <span ref={textRef} className="oci-loader__text">
          T
        </span>
      </div>
    </div>
  );
}
