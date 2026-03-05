/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        oci: {
          blue: "#1925aa",
          mercury: "#e8e6e0",
          navy: "#0d1355",
          error: "#c41230",
        },
      },
      fontFamily: {
        sans: ['"PP Neue Montreal"', "system-ui", "sans-serif"],
        mono: ['"GT America Mono"', "ui-monospace", "monospace"],
      },
    },
  },
  plugins: [],
};
