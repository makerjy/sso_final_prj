"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { Shield, LogIn } from "lucide-react"
import { useAuth } from "@/components/auth-provider"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"

export default function LoginPage() {
  const router = useRouter()
  const { user, isHydrated, login } = useAuth()
  const [username, setUsername] = useState("researcher_01")
  const [password, setPassword] = useState("team9KDT__2026")
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!isHydrated) return
    if (user) {
      router.replace("/")
    }
  }, [isHydrated, user, router])

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      const result = await login({ username, password })
      if (!result.ok) {
        setError(result.error || "로그인에 실패했습니다.")
        return
      }
      router.replace("/")
    } finally {
      setSubmitting(false)
    }
  }

  if (!isHydrated) {
    return (
      <div className="min-h-screen flex items-center justify-center text-sm text-muted-foreground">
        로그인 상태를 확인 중입니다...
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-4">
      <Card className="w-full max-w-md">
        <CardHeader>
          <div className="w-10 h-10 rounded-full bg-primary/20 flex items-center justify-center mb-2">
            <Shield className="w-5 h-5 text-primary" />
          </div>
          <CardTitle>Query LENs 로그인</CardTitle>
          <CardDescription>상단 사용자 프로필과 연동되는 계정으로 로그인하세요.</CardDescription>
        </CardHeader>
        <CardContent>
          <form className="space-y-4" onSubmit={handleSubmit}>
            <div className="space-y-1.5">
              <Label htmlFor="username">아이디</Label>
              <Input
                id="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="researcher_01"
                autoComplete="username"
                required
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="password">비밀번호</Label>
              <Input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="********"
                autoComplete="current-password"
                required
              />
            </div>
            {error ? <div className="text-sm text-destructive">{error}</div> : null}
            <Button type="submit" className="w-full gap-2" disabled={submitting}>
              <LogIn className="w-4 h-4" />
              {submitting ? "로그인 중..." : "로그인"}
            </Button>
          </form>
          <div className="mt-4 text-xs text-muted-foreground">
            기본 계정: <code>researcher_01 / team9KDT__2026</code>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
