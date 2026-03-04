/**
 * Mini sparkline chart — blue strokes on white card.
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
  if (values.length < 2) return <span className="text-xs text-blue-600/30">-</span>;

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

  // Blue tones: lighter for decline, darker for rise
  const trend = values[values.length - 1] >= values[0];
  const color = trend ? "#2563eb" : "#93c5fd";

  return (
    <svg width={width} height={height} className="inline-block">
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {/* End dot */}
      {values.length > 0 && (
        <circle
          cx={(values.length - 1) / (values.length - 1) * width}
          cy={height - ((values[values.length - 1] - min) / range) * (height - 4) - 2}
          r="2.5"
          fill={color}
        />
      )}
    </svg>
  );
}
