/**
 * GSAP initialization — register plugins once at app startup.
 */
import { useGSAP } from "@gsap/react";
import gsap from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";

gsap.registerPlugin(useGSAP, ScrollTrigger);

gsap.config({
  nullTargetWarn: false,
});

export { gsap, useGSAP, ScrollTrigger };
