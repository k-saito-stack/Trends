/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    borderRadius: {
      none: "0",
      full: "9999px",
    },
    extend: {
      colors: {
        oci: {
          blue: "#1925aa",
          mercury: "#e8e6e0",
          navy: "#0d1355",
          white: "#ffffff",
          error: "#c41230",
        },
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        mono: ['"JetBrains Mono"', "ui-monospace", "monospace"],
      },
      gridTemplateColumns: {
        "oci-12": "repeat(12, minmax(0, 1fr))",
        "oci-6": "repeat(6, minmax(0, 1fr))",
      },
      transitionTimingFunction: {
        "power4-out": "cubic-bezier(0.25, 1, 0.5, 1)",
        "power4-inout": "cubic-bezier(0.76, 0, 0.24, 1)",
      },
    },
  },
  plugins: [],
};
