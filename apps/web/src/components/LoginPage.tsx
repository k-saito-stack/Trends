/**
 * Login page — OCI style: mercury bg, centered white card, blue accent.
 */
interface LoginPageProps {
  onLogin: () => void;
  error: string | null;
}

export default function LoginPage({ onLogin, error }: LoginPageProps) {
  return (
    <div className="min-h-screen flex items-center justify-center bg-oci-mercury p-4">
      <div className="oci-card max-w-sm w-full p-10">
        {/* Title */}
        <h1 className="oci-heading text-oci-blue text-4xl mb-2">Trends</h1>
        <p className="oci-label text-oci-blue/50 mb-10">
          Trend Detection Platform
        </p>

        {/* Divider */}
        <div className="oci-line mb-8" />

        <button
          onClick={onLogin}
          className="oci-hover w-full bg-oci-blue text-oci-mercury py-3 px-4
                     oci-label text-xs tracking-wider"
        >
          Sign in with Google
        </button>

        {error && (
          <p className="mt-4 text-oci-error text-xs">{error}</p>
        )}

        <p className="mt-8 oci-label text-oci-blue/40 text-[0.625rem]">
          @kodansha.co.jp accounts only
        </p>
      </div>
    </div>
  );
}
