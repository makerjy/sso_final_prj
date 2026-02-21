"use client"

import { useEffect, useMemo, useState } from "react"
import { useRouter } from "next/navigation"
import { AppSidebar, type ViewType } from "@/components/app-sidebar"
import { ConnectionView } from "@/components/views/connection-view"
// import { ContextView } from "@/components/views/context-view"
import { QueryView } from "@/components/views/query-view"
import { DashboardView } from "@/components/views/dashboard-view"
import { AuditView } from "@/components/views/audit-view"
import { CohortView } from "@/components/views/cohort-view"
import { PdfCohortView } from "@/components/views/pdf-cohort-view"
import { ThemeToggle } from "@/components/theme-toggle"
import { useAuth } from "@/components/auth-provider"
import { Badge } from "@/components/ui/badge"
import { Database, Shield, Bell, Menu, LogOut } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Sheet, SheetContent } from "@/components/ui/sheet"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"

const VIEW_STORAGE_PREFIX = "querylens.ui.lastView:"
const VIEW_VALUES: ViewType[] = ["connection", "query", "dashboard", "audit", "cohort", "pdf-cohort"]

const isViewType = (value: string): value is ViewType =>
  VIEW_VALUES.includes(value as ViewType)

const getViewStorageKey = (userId: string) => `${VIEW_STORAGE_PREFIX}${userId}`

export default function Home() {
  const router = useRouter()
  const { user, isHydrated, logout } = useAuth()
  const [currentView, setCurrentView] = useState<ViewType>("connection")
  const [isPdfViewPinned, setIsPdfViewPinned] = useState(false)
  const [isViewRestored, setIsViewRestored] = useState(false)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const [hasOpenedQueryView, setHasOpenedQueryView] = useState(false)
  const [hasOpenedCohortView, setHasOpenedCohortView] = useState(false)
  const [hasOpenedPdfCohortView, setHasOpenedPdfCohortView] = useState(false)
  const effectiveView: ViewType = currentView

  useEffect(() => {
    if (!isHydrated) return
    if (!user) {
      router.replace("/login")
    }
  }, [isHydrated, user, router])

  useEffect(() => {
    const openQueryView = () => {
      setCurrentView("query")
    }
    window.addEventListener("ql-open-query-view", openQueryView)
    return () => {
      window.removeEventListener("ql-open-query-view", openQueryView)
    }
  }, [])

  useEffect(() => {
    if (!isHydrated || !user) {
      setIsViewRestored(false)
      return
    }
    try {
      const savedView = localStorage.getItem(getViewStorageKey(user.id))
      if (savedView && isViewType(savedView)) {
        setCurrentView(savedView)
      } else {
        setCurrentView("connection")
      }
    } catch {}
    setIsViewRestored(true)
  }, [isHydrated, user])

  useEffect(() => {
    if (!isHydrated || !user || !isViewRestored) return
    try {
      localStorage.setItem(getViewStorageKey(user.id), currentView)
    } catch {}
  }, [isHydrated, user, isViewRestored, currentView])

  useEffect(() => {
    if (effectiveView === "query") {
      setHasOpenedQueryView(true)
    }
  }, [effectiveView])

  const shouldRenderQueryView = hasOpenedQueryView || effectiveView === "query"

  useEffect(() => {
    if (effectiveView === "cohort") {
      setHasOpenedCohortView(true)
    }
  }, [effectiveView])

  const shouldRenderCohortView = hasOpenedCohortView || effectiveView === "cohort"

  useEffect(() => {
    if (effectiveView === "pdf-cohort") {
      setHasOpenedPdfCohortView(true)
    }
  }, [effectiveView])

  const shouldRenderPdfCohortView = hasOpenedPdfCohortView || isPdfViewPinned

  const userInitial = useMemo(() => {
    const base = (user?.name || "").trim()
    return base ? base.charAt(0) : "?"
  }, [user?.name])

  const handleLogout = () => {
    logout()
    router.replace("/login")
  }

  const renderView = () => {
    switch (effectiveView) {
      case "connection":
        return <ConnectionView />
      // case "context":
      //   return <ContextView />
      case "dashboard":
        return <DashboardView />
      case "audit":
        return <AuditView />
      case "cohort":
        return <CohortView />
      case "pdf-cohort":
        return <PdfCohortView onPinnedChange={setIsPdfViewPinned} />
      default:
        return <ConnectionView />
    }
  }

  const getViewTitle = () => {
    switch (effectiveView) {
      case "connection": return "DB 연결/권한 설정"
      // case "context": return "컨텍스트 편집"
      case "query": return "쿼리 & 분석"
      case "dashboard": return "결과 보드"
      case "audit": return "감사 로그"
      case "cohort": return "코호트 생성"
      case "pdf-cohort": return "PDF 코호트 분석"
      default: return ""
    }
  }

  const handleViewChange = (view: ViewType) => {
    setCurrentView(view)
    setMobileMenuOpen(false)
  }

  if (!isHydrated || !user) {
    return (
      <div className="h-screen flex items-center justify-center text-sm text-muted-foreground">
        로그인 상태를 확인 중입니다...
      </div>
    )
  }

  if (!isViewRestored) {
    return (
      <div className="h-screen flex items-center justify-center text-sm text-muted-foreground">
        마지막 화면을 불러오는 중입니다...
      </div>
    )
  }

  return (
    <div className="flex h-screen bg-background">
      {/* Desktop Sidebar */}
      <div className="hidden lg:block">
        <AppSidebar 
          currentView={effectiveView}
          onViewChange={handleViewChange}
          collapsed={sidebarCollapsed}
          onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
        />
      </div>

      {/* Mobile Sidebar */}
      <Sheet open={mobileMenuOpen} onOpenChange={setMobileMenuOpen}>
        <SheetContent side="left" className="p-0 w-64">
          <AppSidebar 
            currentView={effectiveView}
            onViewChange={handleViewChange}
            collapsed={false}
            onToggleCollapse={() => {}}
          />
        </SheetContent>
      </Sheet>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top Header */}
        <header className="h-14 sm:h-16 border-b border-border bg-card/50 backdrop-blur-sm flex items-center justify-between px-3 sm:px-6 shrink-0">
          <div className="flex items-center gap-2 sm:gap-4">
            {/* Mobile Menu Button */}
            <Button 
              variant="ghost" 
              size="icon" 
              className="lg:hidden h-8 w-8"
              onClick={() => setMobileMenuOpen(true)}
            >
              <Menu className="w-5 h-5" />
            </Button>
            
            <h1 className="text-sm sm:text-lg font-semibold text-foreground truncate">{getViewTitle()}</h1>
            {effectiveView === "cohort" && (
              <Badge variant="secondary" className="text-[10px] sm:text-xs hidden sm:inline-flex">부가 기능</Badge>
            )}
          </div>

          <div className="flex items-center gap-2 sm:gap-4">
            {/* DB Status - Hidden on mobile */}
            <div className="hidden md:flex items-center gap-2 text-xs text-muted-foreground">
              <Database className="w-3.5 h-3.5" />
              <span className="hidden lg:inline">MIMIC-IV 연동</span>
              <Badge variant="outline" className="text-[10px]">Read-Only</Badge>
            </div>

            {/* HIPAA Badge - Hidden on mobile */}
            <div className="hidden md:flex items-center gap-2 text-xs text-primary">
              <Shield className="w-3.5 h-3.5" />
              <span className="hidden lg:inline">HIPAA 준수</span>
            </div>

            {/* Theme Toggle */}
            <ThemeToggle />

            {/* Notifications */}
            <Button variant="ghost" size="icon" className="h-8 w-8 relative">
              <Bell className="w-4 h-4" />
              <span className="absolute top-1 right-1 w-2 h-2 rounded-full bg-primary" />
            </Button>

            {/* User Profile */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button
                  type="button"
                  className="flex items-center gap-2 pl-2 sm:pl-4 border-l border-border rounded-md py-1 pr-1 hover:bg-accent/50 transition-colors"
                >
                  <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center">
                    <span className="text-xs font-medium text-primary">{userInitial}</span>
                  </div>
                  <div className="hidden sm:block text-left">
                    <div className="text-sm font-medium text-foreground">{user.name}</div>
                    <div className="text-[10px] text-muted-foreground">{user.role}</div>
                  </div>
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-56 p-2">
                <div className="flex items-center gap-2 px-2 py-1">
                  <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center">
                    <span className="text-xs font-medium text-primary">{userInitial}</span>
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm font-medium text-foreground truncate">{user.name}</div>
                    <div className="text-[10px] text-muted-foreground">{user.role}</div>
                  </div>
                </div>
                <DropdownMenuSeparator />
                <DropdownMenuItem onSelect={handleLogout} className="gap-2">
                  <LogOut className="w-4 h-4" />
                  로그아웃
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </header>

        {/* Content Area */}
        <main className="flex-1 overflow-auto">
          {shouldRenderQueryView && (
            <section className={effectiveView === "query" ? "block" : "hidden"} aria-hidden={effectiveView !== "query"}>
              <QueryView />
            </section>
          )}
          {shouldRenderCohortView && (
            <section className={effectiveView === "cohort" ? "block" : "hidden"} aria-hidden={effectiveView !== "cohort"}>
              <CohortView />
            </section>
          )}
          {shouldRenderPdfCohortView && (
            <section className={effectiveView === "pdf-cohort" ? "block" : "hidden"} aria-hidden={effectiveView !== "pdf-cohort"}>
              <PdfCohortView onPinnedChange={setIsPdfViewPinned} />
            </section>
          )}
          {effectiveView !== "query" && effectiveView !== "cohort" && effectiveView !== "pdf-cohort" && renderView()}
        </main>
      </div>
    </div>
  )
}
