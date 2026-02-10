"use client"

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react"

export type AuthUser = {
  id: string
  username: string
  name: string
  role: string
}

type LoginInput = {
  username: string
  password: string
}

type AuthContextValue = {
  user: AuthUser | null
  isHydrated: boolean
  login: (input: LoginInput) => Promise<{ ok: boolean; error?: string }>
  logout: () => void
}

const AUTH_STORAGE_KEY = "querylens.auth.user"

const DEMO_USERS: Array<AuthUser & { password: string }> = [
  { id: "user-researcher-01", username: "researcher_01", password: "team9KDT__2026", name: "김연구원", role: "연구원" },
  { id: "user-admin-01", username: "admin_01", password: "admin1234", name: "박교수", role: "관리자" },
]

const AuthContext = createContext<AuthContextValue | undefined>(undefined)

const sanitizeAuthUser = (value: unknown): AuthUser | null => {
  if (!value || typeof value !== "object") return null
  const user = value as Partial<AuthUser>
  if (!user.id || !user.username || !user.name || !user.role) return null
  return {
    id: String(user.id),
    username: String(user.username),
    name: String(user.name),
    role: String(user.role),
  }
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null)
  const [isHydrated, setIsHydrated] = useState(false)

  useEffect(() => {
    try {
      const raw = localStorage.getItem(AUTH_STORAGE_KEY)
      if (!raw) return
      const parsed = JSON.parse(raw)
      const nextUser = sanitizeAuthUser(parsed)
      if (nextUser) {
        setUser(nextUser)
      }
    } catch {
      localStorage.removeItem(AUTH_STORAGE_KEY)
    } finally {
      setIsHydrated(true)
    }
  }, [])

  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key !== AUTH_STORAGE_KEY) return
      if (!event.newValue) {
        setUser(null)
        return
      }
      try {
        const parsed = JSON.parse(event.newValue)
        setUser(sanitizeAuthUser(parsed))
      } catch {
        setUser(null)
      }
    }
    window.addEventListener("storage", onStorage)
    return () => window.removeEventListener("storage", onStorage)
  }, [])

  const login = useCallback(async ({ username, password }: LoginInput) => {
    const normalized = username.trim().toLowerCase()
    const matched = DEMO_USERS.find(
      (item) => item.username.toLowerCase() === normalized && item.password === password
    )
    if (!matched) {
      return { ok: false, error: "아이디 또는 비밀번호가 올바르지 않습니다." }
    }
    const nextUser: AuthUser = {
      id: matched.id,
      username: matched.username,
      name: matched.name,
      role: matched.role,
    }
    setUser(nextUser)
    localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(nextUser))
    return { ok: true }
  }, [])

  const logout = useCallback(() => {
    setUser(null)
    localStorage.removeItem(AUTH_STORAGE_KEY)
  }, [])

  const value = useMemo(
    () => ({
      user,
      isHydrated,
      login,
      logout,
    }),
    [user, isHydrated, login, logout]
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider")
  }
  return context
}
