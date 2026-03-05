/**
 * Mini sparkline — OCI blue (#1925aa) strokes, no fill.
 */
interface SparklineProps {
  data: (number | null)[];
  width?: number;
  height?: number;
}

export default function Sparkline({
  data,
  width = 80,
  height = 24,
}: SparklineProps) {
  const values = data.filter((v): v is number => v !== null && v !== undefined);
  if (values.length < 2) return <span className="oci-label text-oci-blue/30 text-[0.625rem]">-</span>;

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
  const color = trend ? "#1925aa" : "#1925aa80";

  return (
    <svg width={width} height={height} className="inline-block">
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {values.length > 0 && (
        <circle
          cx={(values.length - 1) / (values.length - 1) * width}
          cy={height - ((values[values.length - 1] - min) / range) * (height - 4) - 2}
          r="2"
          fill={color}
        />
      )}
    </svg>
  );
}
