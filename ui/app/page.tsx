"use client"

import { useState } from "react"
import { AppSidebar, type ViewType } from "@/components/app-sidebar"
import { ConnectionView } from "@/components/views/connection-view"
import { ContextView } from "@/components/views/context-view"
import { QueryView } from "@/components/views/query-view"
import { DashboardView } from "@/components/views/dashboard-view"
import { AuditView } from "@/components/views/audit-view"
import { CohortView } from "@/components/views/cohort-view"
import { ThemeToggle } from "@/components/theme-toggle"
import { Badge } from "@/components/ui/badge"
import { Database, Shield, Bell, Menu, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet"

export default function Home() {
  const [currentView, setCurrentView] = useState<ViewType>("connection")
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)

  const renderView = () => {
    switch (currentView) {
      case "connection":
        return <ConnectionView />
      case "context":
        return <ContextView />
      case "query":
        return <QueryView />
      case "dashboard":
        return <DashboardView />
      case "audit":
        return <AuditView />
      case "cohort":
        return <CohortView />
      default:
        return <ConnectionView />
    }
  }

  const getViewTitle = () => {
    switch (currentView) {
      case "connection": return "DB 연결/권한 설정"
      case "context": return "컨텍스트 편집"
      case "query": return "쿼리 & 분석"
      case "dashboard": return "결과 보드"
      case "audit": return "감사 로그"
      case "cohort": return "코호트 생성"
      default: return ""
    }
  }

  const handleViewChange = (view: ViewType) => {
    setCurrentView(view)
    setMobileMenuOpen(false)
  }

  return (
    <div className="flex h-screen bg-background">
      {/* Desktop Sidebar */}
      <div className="hidden lg:block">
        <AppSidebar 
          currentView={currentView}
          onViewChange={setCurrentView}
          collapsed={sidebarCollapsed}
          onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
        />
      </div>

      {/* Mobile Sidebar */}
      <Sheet open={mobileMenuOpen} onOpenChange={setMobileMenuOpen}>
        <SheetContent side="left" className="p-0 w-64">
          <AppSidebar 
            currentView={currentView}
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
            {currentView === "cohort" && (
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
            <div className="flex items-center gap-2 pl-2 sm:pl-4 border-l border-border">
              <div className="w-8 h-8 rounded-full bg-primary/20 flex items-center justify-center">
                <span className="text-xs font-medium text-primary">김</span>
              </div>
              <div className="hidden sm:block">
                <div className="text-sm font-medium text-foreground">김연구원</div>
                <div className="text-[10px] text-muted-foreground">연구원</div>
              </div>
            </div>
          </div>
        </header>

        {/* Content Area */}
        <main className="flex-1 overflow-auto">
          {renderView()}
        </main>
      </div>
    </div>
  )
}
