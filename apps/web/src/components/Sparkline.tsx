/**
 * Mini sparkline chart showing 7-day trend.
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
  if (values.length < 2) return <span className="text-xs text-gray-400">-</span>;

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

  // Color: green if last > first, red if declining
  const trend = values[values.length - 1] >= values[0];
  const color = trend ? "#22c55e" : "#ef4444";

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
    </svg>
  );
}
