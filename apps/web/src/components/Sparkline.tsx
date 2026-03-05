/**
 * Mini sparkline — OCI blue strokes.
 * Supports `inverted` prop for when card hover bg is blue.
 */
interface SparklineProps {
  data: (number | null)[];
  width?: number;
  height?: number;
  inverted?: boolean;
}

export default function Sparkline({
  data,
  width = 80,
  height = 24,
  inverted = false,
}: SparklineProps) {
  const values = data.filter((v): v is number => v !== null && v !== undefined);
  if (values.length < 2) {
    return (
      <span
        className="oci-label-sm opacity-30"
        style={{ color: inverted ? "#ffffff" : "#1925aa" }}
      >
        —
      </span>
    );
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const points = values
    .map((v, i) => {
      const x = (i / (values.length - 1)) * width;
      const y = height - ((v - min) / range) * (height - 4) - 2;
      return `${x},${y}`;
    })
    .join(" ");

  const trend = values[values.length - 1] >= values[0];
  const baseColor = inverted ? "#ffffff" : "#1925aa";
  const color = trend ? baseColor : `${baseColor}80`;

  const lastX = width;
  const lastY =
    height - ((values[values.length - 1] - min) / range) * (height - 4) - 2;

  return (
    <svg
      width={width}
      height={height}
      className="inline-block"
      style={{ transition: "filter 0.3s" }}
    >
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        style={{ transition: "stroke 0.3s" }}
      />
      <circle
        cx={lastX}
        cy={lastY}
        r="2"
        fill={color}
        style={{ transition: "fill 0.3s" }}
      />
    </svg>
  );
}
