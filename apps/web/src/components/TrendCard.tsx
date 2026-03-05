/**
 * TrendCard — OCI style. The most important component.
 *
 * Effects:
 *   - Hover bg reveals from cursor entry direction
 *   - Card lift + shadow on hover
 *   - ALL text inverts to white on hover (high contrast on blue bg)
 *   - Name, summary, section labels all scramble on hover
 */
import { useRef, useState, useCallback } from "react";
import type { RankingItem } from "../hooks/useDailyRanking";
import { gsap } from "../hooks/useGSAPSetup";
import { useScrambleText } from "../hooks/useScrambleText";
import BreakdownBar from "./BreakdownBar";
import Sparkline from "./Sparkline";

interface TrendCardProps {
  item: RankingItem;
}

type Direction = "top" | "bottom" | "left" | "right";

/** Detect which edge the mouse crossed to enter/leave the element */
function getDirection(e: React.MouseEvent, el: HTMLElement): Direction {
  const rect = el.getBoundingClientRect();
  const x = e.clientX - rect.left;
  const y = e.clientY - rect.top;
  const w = rect.width;
  const h = rect.height;

  // Normalize to [-1, 1] range
  const nx = (x / w - 0.5) * 2;
  const ny = (y / h - 0.5) * 2;

  // Compare horizontal vs vertical distance to edge
  if (Math.abs(nx) > Math.abs(ny)) {
    return nx > 0 ? "right" : "left";
  }
  return ny > 0 ? "bottom" : "top";
}

/** Get GSAP `from` values and transformOrigin for a given direction */
function getDirectionProps(dir: Direction) {
  switch (dir) {
    case "top":
      return { from: { scaleY: 0, scaleX: 1 }, origin: "center top" };
    case "bottom":
      return { from: { scaleY: 0, scaleX: 1 }, origin: "center bottom" };
    case "left":
      return { from: { scaleX: 0, scaleY: 1 }, origin: "left center" };
    case "right":
      return { from: { scaleX: 0, scaleY: 1 }, origin: "right center" };
  }
}

export default function TrendCard({ item }: TrendCardProps) {
  const [hovered, setHovered] = useState(false);

  const cardRef = useRef<HTMLDivElement>(null);
  const hoverBgRef = useRef<HTMLDivElement>(null);
  const detailRef = useRef<HTMLDivElement>(null);
  const nameRef = useRef<HTMLSpanElement>(null);

  const rankRef = useRef<HTMLSpanElement>(null);
  const sepRef = useRef<HTMLDivElement>(null);
  const tagRef = useRef<HTMLSpanElement>(null);
  const scoreRef = useRef<HTMLSpanElement>(null);

  // Scramble targets
  const summaryRef = useRef<HTMLParagraphElement>(null);
  const labelBreakdownRef = useRef<HTMLHeadingElement>(null);
  const labelEvidenceRef = useRef<HTMLHeadingElement>(null);
  const labelPowerRef = useRef<HTMLSpanElement>(null);

  const { scramble } = useScrambleText();

  // --- Hover enter ---
  const handleMouseEnter = useCallback(
    (e: React.MouseEvent) => {
      setHovered(true);

      // Direction-aware bg reveal
      if (hoverBgRef.current && cardRef.current) {
        const dir = getDirection(e, cardRef.current);
        const { from, origin } = getDirectionProps(dir);
        hoverBgRef.current.style.transformOrigin = origin;
        gsap.fromTo(
          hoverBgRef.current,
          { ...from },
          { scaleX: 1, scaleY: 1, duration: 0.5, ease: "power4.out", overwrite: true },
        );
      }

      // Ensure card is fully visible (in case scroll animation was interrupted)
      gsap.set(cardRef.current, { opacity: 1, x: 0 });

      // Card lift + shadow
      gsap.to(cardRef.current, {
        y: -4,
        boxShadow: "0 8px 30px rgba(25,37,170,0.15)",
        duration: 0.4,
        ease: "power4.out",
        overwrite: true,
      });

      // Header text → white
      gsap.to([rankRef.current, nameRef.current, scoreRef.current], {
        color: "#ffffff",
        duration: 0.3,
        overwrite: true,
      });

      // Tag → white text + white border
      gsap.to(tagRef.current, {
        color: "rgba(255,255,255,0.6)",
        borderColor: "rgba(255,255,255,0.3)",
        duration: 0.3,
        overwrite: true,
      });

      gsap.to(sepRef.current, {
        backgroundColor: "rgba(255,255,255,0.3)",
        duration: 0.3,
        overwrite: true,
      });

      // Detail section — all text to white
      if (detailRef.current) {
        gsap.to(detailRef.current, {
          color: "#ffffff",
          duration: 0.3,
          overwrite: true,
        });
        const borders = detailRef.current.querySelectorAll("[class*='border-oci-blue']");
        gsap.to(borders, { borderColor: "rgba(255,255,255,0.2)", duration: 0.3, overwrite: true });
      }

      // Scramble: name + summary + labels (all concurrent)
      if (nameRef.current) {
        scramble(nameRef.current, item.displayName);
      }
      if (summaryRef.current && item.summary) {
        summaryRef.current.style.minHeight = `${summaryRef.current.offsetHeight}px`;
        scramble(summaryRef.current, item.summary, 600);
      }
      if (labelBreakdownRef.current) {
        scramble(labelBreakdownRef.current, "Score Breakdown", 300);
      }
      if (labelEvidenceRef.current) {
        scramble(labelEvidenceRef.current, "Evidence", 300);
      }
      if (labelPowerRef.current) {
        scramble(labelPowerRef.current, `Power: ${item.power?.toFixed(1)}`, 300);
      }
    },
    [scramble, item.displayName, item.summary, item.power],
  );

  // --- Hover leave ---
  const handleMouseLeave = useCallback(
    (e: React.MouseEvent) => {
      setHovered(false);

      // Direction-aware bg hide
      if (hoverBgRef.current && cardRef.current) {
        const dir = getDirection(e, cardRef.current);
        const { from, origin } = getDirectionProps(dir);
        hoverBgRef.current.style.transformOrigin = origin;
        gsap.to(hoverBgRef.current, {
          ...from,
          duration: 0.5,
          ease: "power4.out",
          overwrite: true,
        });
      }

      // Card drop back
      gsap.to(cardRef.current, {
        y: 0,
        boxShadow: "0 0px 0px rgba(25,37,170,0)",
        duration: 0.4,
        ease: "power4.out",
        overwrite: true,
      });

      // Header text → blue
      gsap.to([rankRef.current, nameRef.current, scoreRef.current], {
        color: "#1925aa",
        duration: 0.3,
        overwrite: true,
      });

      // Tag → original blue/40 + border blue/20
      gsap.to(tagRef.current, {
        color: "rgba(25,37,170,0.4)",
        borderColor: "rgba(25,37,170,0.2)",
        duration: 0.3,
        overwrite: true,
      });

      gsap.to(sepRef.current, {
        backgroundColor: "rgba(25,37,170,0.2)",
        duration: 0.3,
        overwrite: true,
      });

      // Detail section — revert to blue
      if (detailRef.current) {
        gsap.to(detailRef.current, {
          color: "#1925aa",
          duration: 0.3,
          overwrite: true,
        });
        const borders = detailRef.current.querySelectorAll("[class*='border-oci-blue']");
        gsap.to(borders, { borderColor: "", duration: 0.3, overwrite: true });
      }
    },
    [],
  );

  return (
    <div
      ref={cardRef}
      className="oci-card oci-card--interactive"
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      {/* Hover background — direction-aware reveal */}
      <div
        ref={hoverBgRef}
        style={{
          position: "absolute",
          inset: 0,
          backgroundColor: "#1925aa",
          transform: "scaleY(0)",
          transformOrigin: "center bottom",
          zIndex: 1,
          pointerEvents: "none",
        }}
      />

      {/* Card header */}
      <div className="relative z-10 w-full px-6 py-5 flex items-center gap-5">
        <span
          ref={rankRef}
          className="oci-heading text-oci-blue text-2xl shrink-0 w-8 text-center"
        >
          {item.rank.toString().padStart(2, "0")}
        </span>

        <div
          ref={sepRef}
          className="w-px h-10 shrink-0"
          style={{ backgroundColor: "rgba(25,37,170,0.2)" }}
        />

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3">
            <span
              ref={nameRef}
              className="font-sans font-bold text-lg text-oci-blue truncate"
            >
              {item.displayName}
            </span>
            <span
              ref={tagRef}
              className="oci-label-sm border px-2 py-0.5 shrink-0"
              style={{ color: "rgba(25,37,170,0.4)", borderColor: "rgba(25,37,170,0.2)" }}
            >
              {item.candidateType}
            </span>
          </div>
        </div>

        <div className="flex items-center gap-4 shrink-0">
          <Sparkline data={item.sparkline7d} inverted={hovered} />
          <span
            ref={scoreRef}
            className="font-mono text-sm text-oci-blue w-14 text-right"
          >
            {item.trendScore.toFixed(1)}
          </span>
        </div>
      </div>

      {/* Detail — always visible */}
      <div ref={detailRef} className="relative z-10 text-oci-blue">
        <div className="px-6 pb-6 border-t border-oci-blue/10">
          {item.summary && (
            <p ref={summaryRef} className="text-inherit/80 text-sm leading-relaxed mt-4 mb-5 font-sans">
              {item.summary}
            </p>
          )}

          {item.breakdownBuckets.length > 0 && (
            <div className="mb-5">
              <h4 ref={labelBreakdownRef} className="oci-label-sm opacity-50 mb-2">
                Score Breakdown
              </h4>
              <BreakdownBar buckets={item.breakdownBuckets} totalScore={item.trendScore} inverted={hovered} />
            </div>
          )}

          {item.evidenceTop3.length > 0 && (
            <div className="mb-3">
              <h4 ref={labelEvidenceRef} className="oci-label-sm opacity-50 mb-2">
                Evidence
              </h4>
              <div className="space-y-2">
                {item.evidenceTop3.map((ev, i) => (
                  <div key={i} className="text-sm font-sans">
                    {ev.url ? (
                      <a
                        href={ev.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="underline underline-offset-2 inline"
                      >
                        {ev.title || ev.url}
                      </a>
                    ) : (
                      <span>{ev.title}</span>
                    )}
                    <span className="oci-label-sm opacity-30 mx-1">
                      {ev.sourceId}
                    </span>
                    {ev.metric && (
                      <span className="opacity-30">({ev.metric})</span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {item.power != null && (
            <div className="flex items-center gap-2 mt-3 pt-3 border-t border-oci-blue/10">
              <span ref={labelPowerRef} className="oci-label-sm opacity-30">
                Power: {item.power.toFixed(1)}
              </span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
