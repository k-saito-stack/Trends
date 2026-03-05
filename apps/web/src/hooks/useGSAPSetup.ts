/**
 * GSAP initialization — register plugins once at app startup.
 */
import { useGSAP } from "@gsap/react";
import gsap from "gsap";

gsap.registerPlugin(useGSAP);

gsap.config({
  nullTargetWarn: false,
});

export { gsap, useGSAP };
