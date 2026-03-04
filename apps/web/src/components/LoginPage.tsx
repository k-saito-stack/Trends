/**
 * Google login page — blue full-screen background with white card.
 */
interface LoginPageProps {
  onLogin: () => void;
  error: string | null;
}

export default function LoginPage({ onLogin, error }: LoginPageProps) {
  return (
    <div className="min-h-screen flex items-center justify-center bg-[#0039d6] p-4">
      <div className="white-card max-w-sm w-full p-8 text-center">
        {/* Section header bar */}
        <div className="flex items-start justify-center mb-6">
          <div className="w-1 h-6 bg-blue-600 mr-3" />
          <div>
            <h1 className="text-blue-600 font-medium text-lg">Trends</h1>
            <p className="text-blue-600 text-sm">トレンド兆し検知</p>
          </div>
        </div>

        <button
          onClick={onLogin}
          className="w-full bg-blue-600 text-white py-3 px-4 text-sm
                     uppercase tracking-wider hover:bg-blue-700 transition-colors"
        >
          Google でログイン
        </button>

        {error && (
          <p className="mt-4 text-red-500 text-sm">{error}</p>
        )}

        <p className="mt-6 text-xs text-blue-600/60">
          @kodansha.co.jp アカウントでログインしてください
        </p>
      </div>
    </div>
  );
}
