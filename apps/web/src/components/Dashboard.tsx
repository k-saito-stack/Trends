/**
 * Dashboard — OCI style.
 * Cards appear from bottom-right with ScrollTrigger as you scroll.
 */
import { useRef, useEffect } from "react";
import { useDailyRanking } from "../hooks/useDailyRanking";
import { gsap, ScrollTrigger } from "../hooks/useGSAPSetup";
import TrendCard from "./TrendCard";

interface DashboardProps {
  date: string;
}

export default function Dashboard({ date }: DashboardProps) {
  const { items, meta, loading, error } = useDailyRanking(date);
  const cardListRef = useRef<HTMLDivElement>(null);
  const loadingRef = useRef<HTMLSpanElement>(null);

  // Loading pulse
  useEffect(() => {
    if (loading && loadingRef.current) {
      const tween = gsap.to(loadingRef.current, {
        opacity: 0.2,
        repeat: -1,
        yoyo: true,
        duration: 0.8,
        ease: "power4.inOut",
      });
      return () => {
        tween.kill();
      };
    }
  }, [loading]);

  // ScrollTrigger: cards reveal from bottom-right as you scroll
  useEffect(() => {
    if (!loading && items.length > 0 && cardListRef.current) {
      const cards = cardListRef.current.querySelectorAll(".oci-card");

      cards.forEach((card) => {
        gsap.set(card, { opacity: 0, y: 60, x: 30 });

        ScrollTrigger.create({
          trigger: card,
          start: "top 90%",
          once: true,
          onEnter: () => {
            gsap.to(card, {
              opacity: 1,
              y: 0,
              x: 0,
              duration: 0.7,
              ease: "power4.out",
            });
          },
        });
      });

      // First few cards visible without scroll get a stagger
      const visibleCards = Array.from(cards).filter((card) => {
        const rect = card.getBoundingClientRect();
        return rect.top < window.innerHeight;
      });
      if (visibleCards.length > 0) {
        gsap.to(visibleCards, {
          opacity: 1,
          y: 0,
          x: 0,
          duration: 0.6,
          ease: "power4.out",
          stagger: 0.08,
        });
      }

      return () => {
        ScrollTrigger.getAll().forEach((st) => st.kill());
      };
    }
  }, [loading, items]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <span ref={loadingRef} className="oci-label text-oci-blue/40">
          Loading...
        </span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="oci-card p-6">
          <span className="oci-label text-oci-error">{error}</span>
        </div>
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="oci-card p-10 text-center">
          <p className="oci-heading text-oci-blue text-xl mb-3">{date}</p>
          <p className="oci-label-sm text-oci-blue/40">No data available</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      {meta && (
        <div className="flex items-center justify-between mb-6 pb-3 border-b border-oci-blue/10">
          <span className="oci-label-sm text-oci-blue/30">
            Generated: {new Date(meta.generatedAt).toLocaleString("ja-JP")}
          </span>
          <span className="oci-label-sm text-oci-blue/20">
            Run: {meta.runId.slice(0, 8)}
          </span>
        </div>
      )}

      <div
        ref={cardListRef}
        className="flex flex-col gap-5"
      >
        {items.map((item) => (
          <TrendCard key={item.candidateId} item={item} />
        ))}
      </div>
    </div>
  );
}
