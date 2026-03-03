/**
 * Google login page.
 */
interface LoginPageProps {
  onLogin: () => void;
  error: string | null;
}

export default function LoginPage({ onLogin, error }: LoginPageProps) {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="max-w-sm w-full bg-white rounded-lg shadow-md p-8 text-center">
        <h1 className="text-2xl font-bold text-gray-800 mb-2">Trends</h1>
        <p className="text-gray-500 mb-6">トレンド兆し検知ダッシュボード</p>

        <button
          onClick={onLogin}
          className="w-full bg-blue-600 text-white py-3 px-4 rounded-lg
                     hover:bg-blue-700 transition-colors font-medium"
        >
          Google でログイン
        </button>

        {error && (
          <p className="mt-4 text-red-500 text-sm">{error}</p>
        )}

        <p className="mt-6 text-xs text-gray-400">
          @kodansha.co.jp アカウントでログインしてください
        </p>
      </div>
    </div>
  );
}
