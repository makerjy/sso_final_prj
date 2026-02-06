const API_BASE_URL =
  process.env.API_BASE_URL ||
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  "http://localhost:8001"

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  async rewrites() {
    return [
      { source: "/query/:path*", destination: `${API_BASE_URL}/query/:path*` },
      { source: "/admin/:path*", destination: `${API_BASE_URL}/admin/:path*` },
      { source: "/report/:path*", destination: `${API_BASE_URL}/report/:path*` },
    ]
  },
}

export default nextConfig
