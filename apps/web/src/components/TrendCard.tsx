/**
 * TrendCard — OCI style. The most important component.
 *
 * 3-layer structure:
 *   Layer 1: oci-btn__bg (blue) — reveals from bottom on hover
 *   Layer 2: Header (z-10) — rank, name, sparkline, score
 *   Layer 3: Detail (z-10, height:0) — expandable breakdown/evidence
 *
 * Hover: bg slides up from bottom, text color inverts blue→mercury.
 * Name gets scramble effect on hover.
 * Expand/collapse animated with GSAP.
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

export default function TrendCard({ item }: TrendCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [hovered, setHovered] = useState(false);

  const hoverBgRef = useRef<HTMLDivElement>(null);
  const nameRef = useRef<HTMLSpanElement>(null);
  const detailRef = useRef<HTMLDivElement>(null);
  const detailInnerRef = useRef<HTMLDivElement>(null);
  const chevronRef = useRef<SVGSVGElement>(null);

  const rankRef = useRef<HTMLSpanElement>(null);
  const sepRef = useRef<HTMLDivElement>(null);
  const tagRef = useRef<HTMLSpanElement>(null);
  const scoreRef = useRef<HTMLSpanElement>(null);

  const { scramble } = useScrambleText();

  // --- Hover: bg reveal from bottom ---
  const handleMouseEnter = useCallback(() => {
    setHovered(true);

    gsap.to(hoverBgRef.current, {
      yPercent: 0,
      scaleX: 1,
      duration: 0.5,
      ease: "power4.out",
      overwrite: true,
    });

    const targets = [
      rankRef.current,
      nameRef.current,
      tagRef.current,
      scoreRef.current,
      chevronRef.current,
    ];
    gsap.to(targets, {
      color: "#e8e6e0",
      duration: 0.3,
      overwrite: true,
    });

    gsap.to(sepRef.current, {
      backgroundColor: "rgba(232,230,224,0.3)",
      duration: 0.3,
      overwrite: true,
    });

    gsap.to(tagRef.current, {
      borderColor: "rgba(232,230,224,0.3)",
      duration: 0.3,
      overwrite: true,
    });

    if (nameRef.current) {
      scramble(nameRef.current, item.displayName);
    }
  }, [scramble, item.displayName]);

  const handleMouseLeave = useCallback(() => {
    setHovered(false);

    gsap.to(hoverBgRef.current, {
      yPercent: 101,
      scaleX: 0.5,
      duration: 0.5,
      ease: "power4.out",
      overwrite: true,
    });

    const targets = [
      rankRef.current,
      nameRef.current,
      tagRef.current,
      scoreRef.current,
      chevronRef.current,
    ];
    gsap.to(targets, {
      color: "#1925aa",
      duration: 0.3,
      overwrite: true,
    });

    gsap.to(sepRef.current, {
      backgroundColor: "rgba(25,37,170,0.2)",
      duration: 0.3,
      overwrite: true,
    });

    gsap.to(tagRef.current, {
      borderColor: "rgba(25,37,170,0.2)",
      duration: 0.3,
      overwrite: true,
    });
  }, []);

  // --- Expand/collapse ---
  const toggleExpand = useCallback(() => {
    const next = !expanded;
    setExpanded(next);

    if (next) {
      gsap.set(detailRef.current, { display: "block" });
      gsap.fromTo(
        detailRef.current,
        { height: 0, opacity: 0 },
        { height: "auto", opacity: 1, duration: 0.4, ease: "power4.out" },
      );
      gsap.to(chevronRef.current, {
        rotation: 180,
        duration: 0.3,
        ease: "power4.out",
      });

      if (detailInnerRef.current) {
        const children = detailInnerRef.current.children;
        gsap.fromTo(
          children,
          { opacity: 0, y: 10 },
          {
            opacity: 1,
            y: 0,
            duration: 0.3,
            ease: "power4.out",
            stagger: 0.05,
            delay: 0.1,
          },
        );
      }
    } else {
      gsap.to(detailRef.current, {
        height: 0,
        opacity: 0,
        duration: 0.3,
        ease: "power4.inOut",
        onComplete: () => {
          gsap.set(detailRef.current, { display: "none" });
        },
      });
      gsap.to(chevronRef.current, {
        rotation: 0,
        duration: 0.3,
        ease: "power4.inOut",
      });
    }
  }, [expanded]);

  return (
    <div className="oci-card">
      {/* Layer 1: Hover background */}
      <div ref={hoverBgRef} className="oci-btn__bg bg-oci-blue" />

      {/* Layer 2: Card header */}
      <button
        onClick={toggleExpand}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        className="relative z-10 w-full px-6 py-5 flex items-center gap-5 text-left cursor-pointer"
      >
        <span
          ref={rankRef}
          className="oci-heading text-oci-blue text-xl shrink-0 w-8 text-center"
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
              className="font-sans font-medium text-base text-oci-blue truncate"
            >
              {item.displayName}
            </span>
            <span
              ref={tagRef}
              className="oci-label-sm text-oci-blue/40 border border-oci-blue/20 px-2 py-0.5 shrink-0"
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
          <svg
            ref={chevronRef}
            xmlns="http://www.w3.org/2000/svg"
            className="h-4 w-4 text-oci-blue/30"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={1.5}
              d="M19 9l-7 7-7-7"
            />
          </svg>
        </div>
      </button>

      {/* Layer 3: Expandable detail */}
      <div
        ref={detailRef}
        className="relative z-10 overflow-hidden"
        style={{ height: 0, opacity: 0, display: "none" }}
      >
        <div ref={detailInnerRef} className="px-6 pb-6 border-t border-oci-blue/10">
          {item.summary && (
            <p className="text-oci-blue/80 text-xs leading-relaxed mt-4 mb-5 font-sans">
              {item.summary}
            </p>
          )}

          {item.breakdownBuckets.length > 0 && (
            <div className="mb-5">
              <h4 className="oci-label-sm text-oci-blue/50 mb-2">Score Breakdown</h4>
              <BreakdownBar buckets={item.breakdownBuckets} totalScore={item.trendScore} />
            </div>
          )}

          {item.evidenceTop3.length > 0 && (
            <div className="mb-3">
              <h4 className="oci-label-sm text-oci-blue/50 mb-2">Evidence</h4>
              <div className="space-y-2">
                {item.evidenceTop3.map((ev, i) => (
                  <div key={i} className="flex items-start gap-3 pl-1">
                    <div
                      className="w-1 bg-oci-blue/20 shrink-0 mt-0.5"
                      style={{ minHeight: "1rem" }}
                    />
                    <div className="text-xs font-sans">
                      <span className="oci-label-sm text-oci-blue/30 mr-1">
                        {ev.sourceId}
                      </span>
                      {ev.url ? (
                        <a
                          href={ev.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="oci-link text-oci-blue underline underline-offset-2 inline"
                        >
                          <span className="oci-link__dot" />
                          {ev.title || ev.url}
                        </a>
                      ) : (
                        <span className="text-oci-blue">{ev.title}</span>
                      )}
                      {ev.metric && (
                        <span className="text-oci-blue/30 ml-1">({ev.metric})</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {item.power != null && (
            <div className="flex items-center gap-2 mt-3 pt-3 border-t border-oci-blue/10">
              <span className="oci-label-sm text-oci-blue/30">
                Power: {item.power.toFixed(1)}
              </span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
