"use client"

import { cn } from "@/lib/utils"
import Image from "next/image"
import {
  Database,
  // Settings2,
  MessageSquare,
  LayoutDashboard,
  FileText,
  ChevronLeft,
  ChevronRight,
  Users
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"

export type ViewType = "connection" | "query" | "dashboard" | "audit" | "cohort" | "pdf-cohort"

interface AppSidebarProps {
  currentView: ViewType
  onViewChange: (view: ViewType) => void
  collapsed: boolean
  onToggleCollapse: () => void
}

const navItems = [
  { id: "connection" as const, label: "DB 연결", icon: Database, description: "연결/권한 설정" },
  // { id: "context" as const, label: "컨텍스트", icon: Settings2, description: "관리자 설정" },
  { id: "query" as const, label: "쿼리", icon: MessageSquare, description: "질문 & SQL" },
  { id: "dashboard" as const, label: "대시보드", icon: LayoutDashboard, description: "결과 보드" },
  { id: "audit" as const, label: "감사 로그", icon: FileText, description: "의사결정 증적" },
]

const secondaryItems = [
  { id: "cohort" as const, label: "단면 연구 집단", icon: Users, description: "코호트 및 시뮬레이션" },
  { id: "pdf-cohort" as const, label: "PDF 코호트 분석", icon: FileText, description: "논문 기반 분석" },
]

export function AppSidebar({ currentView, onViewChange, collapsed, onToggleCollapse }: AppSidebarProps) {
  return (
    <TooltipProvider delayDuration={0}>
      <aside className={cn(
        "flex flex-col h-screen bg-card border-r border-border transition-all duration-300",
        collapsed ? "w-16" : "w-56"
      )}>
        {/* Logo */}
        <div className={cn(
          "flex items-center h-16 border-b border-border px-4",
          collapsed ? "justify-center" : "gap-1"
        )}>
          <div className={cn("flex items-center justify-center shrink-0", collapsed ? "w-8 h-8" : "w-9 h-9")}>
            <Image
              src="/query-lens-logo.svg"
              alt="Query LENs"
              width={collapsed ? 32 : 36}
              height={collapsed ? 32 : 36}
              priority
            />
          </div>
          {!collapsed && (
            <div className="overflow-hidden">
              <h1 className="text-sm font-semibold text-foreground truncate">Query LENs</h1>
              <p className="text-[10px] text-muted-foreground truncate">NL2SQL 플랫폼</p>
            </div>
          )}
        </div>

        {/* Main Navigation */}
        <nav className="flex-1 p-2 space-y-1">
          <div className={cn("text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-2", collapsed ? "text-center" : "px-3")}>
            {collapsed ? "Main" : "메인 메뉴"}
          </div>
          {navItems.map((item) => {
            const isActive = currentView === item.id
            const button = (
              <button
                key={item.id}
                onClick={() => onViewChange(item.id)}
                className={cn(
                  "flex items-center w-full rounded-lg transition-colors",
                  collapsed ? "justify-center p-3" : "gap-3 px-3 py-2.5",
                  isActive
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:bg-secondary hover:text-foreground"
                )}
              >
                <item.icon className={cn("shrink-0", collapsed ? "w-5 h-5" : "w-4 h-4")} />
                {!collapsed && (
                  <div className="flex-1 text-left overflow-hidden">
                    <div className="text-sm font-medium truncate">{item.label}</div>
                    <div className={cn("text-[10px] truncate", isActive ? "text-primary-foreground/70" : "text-muted-foreground")}>
                      {item.description}
                    </div>
                  </div>
                )}
              </button>
            )

            if (collapsed) {
              return (
                <Tooltip key={item.id}>
                  <TooltipTrigger asChild>{button}</TooltipTrigger>
                  <TooltipContent side="right" className="flex flex-col">
                    <span className="font-medium">{item.label}</span>
                    <span className="text-xs text-muted-foreground">{item.description}</span>
                  </TooltipContent>
                </Tooltip>
              )
            }
            return button
          })}

          {/* Secondary Navigation */}
          <div className={cn("text-[10px] font-medium text-muted-foreground uppercase tracking-wider mt-6 mb-2", collapsed ? "text-center" : "px-3")}>
            {collapsed ? "Sub" : "부가 기능"}
          </div>
          {secondaryItems.map((item) => {
            const isActive = currentView === item.id
            const button = (
              <button
                key={item.id}
                onClick={() => onViewChange(item.id)}
                className={cn(
                  "flex items-center w-full rounded-lg transition-colors",
                  collapsed ? "justify-center p-3" : "gap-3 px-3 py-2.5",
                  isActive
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:bg-secondary hover:text-foreground"
                )}
              >
                <item.icon className={cn("shrink-0", collapsed ? "w-5 h-5" : "w-4 h-4")} />
                {!collapsed && (
                  <div className="flex-1 text-left overflow-hidden">
                    <div className="text-sm font-medium truncate">{item.label}</div>
                    <div className={cn("text-[10px] truncate", isActive ? "text-primary-foreground/70" : "text-muted-foreground")}>
                      {item.description}
                    </div>
                  </div>
                )}
              </button>
            )

            if (collapsed) {
              return (
                <Tooltip key={item.id}>
                  <TooltipTrigger asChild>{button}</TooltipTrigger>
                  <TooltipContent side="right" className="flex flex-col">
                    <span className="font-medium">{item.label}</span>
                    <span className="text-xs text-muted-foreground">{item.description}</span>
                  </TooltipContent>
                </Tooltip>
              )
            }
            return button
          })}
        </nav>

        {/* Collapse Toggle */}
        <div className="p-2 border-t border-border">
          <Button
            variant="ghost"
            size="sm"
            onClick={onToggleCollapse}
            className={cn("w-full", collapsed ? "justify-center" : "justify-start gap-2")}
          >
            {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
            {!collapsed && <span className="text-xs">접기</span>}
          </Button>
        </div>
      </aside>
    </TooltipProvider>
  )
}
