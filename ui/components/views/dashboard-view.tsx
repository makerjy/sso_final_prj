"use client"

import React from "react"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { 
  Pin, 
  Clock, 
  Share2, 
  MoreHorizontal,
  Play,
  Calendar,
  Users,
  TrendingUp,
  TrendingDown,
  Search,
  Plus,
  Star,
  StarOff,
  Copy,
  Trash2,
  BarChart3,
  PieChart,
  Activity
} from "lucide-react"
import { cn } from "@/lib/utils"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

interface SavedQuery {
  id: string
  title: string
  description: string
  query: string
  lastRun: string
  schedule?: string
  isPinned: boolean
  category: string
  metrics: { label: string; value: string; trend?: "up" | "down" }[]
  chartType: "line" | "bar" | "pie"
}

const savedQueries: SavedQuery[] = [
  {
    id: "1",
    title: "65세 이상 심부전 생존 분석",
    description: "65세 이상 심부전 환자의 Kaplan-Meier 생존 곡선",
    query: "SELECT ... FROM patients WHERE age >= 65",
    lastRun: "2시간 전",
    schedule: "매일 09:00",
    isPinned: true,
    category: "생존분석",
    metrics: [
      { label: "환자 수", value: "1,247" },
      { label: "30일 생존율", value: "75.8%", trend: "down" },
      { label: "중앙 생존", value: "82일" },
    ],
    chartType: "line"
  },
  {
    id: "2",
    title: "월별 재입원율 추이",
    description: "30일 내 재입원율 월별 트렌드 분석",
    query: "SELECT ... FROM admissions",
    lastRun: "1일 전",
    schedule: "매주 월요일",
    isPinned: true,
    category: "재입원",
    metrics: [
      { label: "이번 달", value: "12.4%", trend: "up" },
      { label: "전월 대비", value: "+1.2%", trend: "up" },
      { label: "목표", value: "10%" },
    ],
    chartType: "bar"
  },
  {
    id: "3",
    title: "진단별 ICU 입실률",
    description: "주요 진단 코드별 ICU 입실 비율",
    query: "SELECT ... FROM diagnoses_icd",
    lastRun: "3일 전",
    isPinned: false,
    category: "ICU",
    metrics: [
      { label: "심부전", value: "34.2%" },
      { label: "패혈증", value: "52.1%" },
      { label: "뇌졸중", value: "28.7%" },
    ],
    chartType: "pie"
  },
  {
    id: "4",
    title: "응급실 평균 대기시간",
    description: "시간대별 응급실 대기시간 분석",
    query: "SELECT ... FROM edstays",
    lastRun: "12시간 전",
    schedule: "매일 18:00",
    isPinned: false,
    category: "응급실",
    metrics: [
      { label: "평균", value: "4.2시간" },
      { label: "피크시간", value: "6.8시간", trend: "up" },
      { label: "최소", value: "1.5시간" },
    ],
    chartType: "bar"
  },
]

const categories = ["전체", "생존분석", "재입원", "ICU", "응급실", "사망률"]

export function DashboardView() {
  const [queries, setQueries] = useState(savedQueries)
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedCategory, setSelectedCategory] = useState("전체")

  const togglePin = (id: string) => {
    setQueries(prev => prev.map(q => 
      q.id === id ? { ...q, isPinned: !q.isPinned } : q
    ))
  }

  const filteredQueries = queries.filter(q => {
    const matchesSearch = q.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         q.description.toLowerCase().includes(searchTerm.toLowerCase())
    const matchesCategory = selectedCategory === "전체" || q.category === selectedCategory
    return matchesSearch && matchesCategory
  })

  const pinnedQueries = filteredQueries.filter(q => q.isPinned)
  const otherQueries = filteredQueries.filter(q => !q.isPinned)

  const getChartIcon = (type: string) => {
    switch (type) {
      case "line": return <Activity className="w-4 h-4" />
      case "bar": return <BarChart3 className="w-4 h-4" />
      case "pie": return <PieChart className="w-4 h-4" />
      default: return <BarChart3 className="w-4 h-4" />
    }
  }

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-foreground">결과 보드</h2>
          <p className="text-sm text-muted-foreground mt-1">자주 사용하는 쿼리와 분석 결과를 관리합니다</p>
        </div>
        <Button className="gap-2 w-full sm:w-auto">
          <Plus className="w-4 h-4" />
          새 쿼리 추가
        </Button>
      </div>

      {/* Search and Filters */}
      <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3 sm:gap-4">
        <div className="relative flex-1 sm:max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input 
            placeholder="쿼리 검색..." 
            className="pl-9"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
        <div className="flex items-center gap-1 sm:gap-2 overflow-x-auto pb-1 sm:pb-0">
          {categories.map((cat) => (
            <Button
              key={cat}
              variant={selectedCategory === cat ? "default" : "outline"}
              size="sm"
              onClick={() => setSelectedCategory(cat)}
              className="whitespace-nowrap text-xs sm:text-sm"
            >
              {cat}
            </Button>
          ))}
        </div>
      </div>

      {/* Pinned Queries */}
      {pinnedQueries.length > 0 && (
        <div>
          <h3 className="text-sm font-medium text-muted-foreground mb-3 flex items-center gap-2">
            <Pin className="w-4 h-4" />
            고정된 쿼리
          </h3>
          <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-3 sm:gap-4">
            {pinnedQueries.map((query) => (
              <QueryCard key={query.id} query={query} onTogglePin={togglePin} getChartIcon={getChartIcon} />
            ))}
          </div>
        </div>
      )}

      {/* Other Queries */}
      {otherQueries.length > 0 && (
        <div>
          <h3 className="text-sm font-medium text-muted-foreground mb-3">기타 쿼리</h3>
          <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-3 sm:gap-4">
            {otherQueries.map((query) => (
              <QueryCard key={query.id} query={query} onTogglePin={togglePin} getChartIcon={getChartIcon} />
            ))}
          </div>
        </div>
      )}

      {filteredQueries.length === 0 && (
        <div className="text-center py-12">
          <div className="w-12 h-12 rounded-full bg-secondary flex items-center justify-center mx-auto mb-4">
            <Search className="w-6 h-6 text-muted-foreground" />
          </div>
          <p className="text-muted-foreground">검색 결과가 없습니다</p>
        </div>
      )}
    </div>
  )
}

interface QueryCardProps {
  query: SavedQuery
  onTogglePin: (id: string) => void
  getChartIcon: (type: string) => React.ReactNode
}

function QueryCard({ query, onTogglePin, getChartIcon }: QueryCardProps) {
  return (
    <Card className={cn(
      "hover:border-primary/30 transition-colors",
      query.isPinned && "border-primary/50 bg-primary/5"
    )}>
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-2">
            <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-secondary">
              {getChartIcon(query.chartType)}
            </div>
            <div>
              <CardTitle className="text-sm">{query.title}</CardTitle>
              <Badge variant="outline" className="text-[10px] mt-1">{query.category}</Badge>
            </div>
          </div>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon" className="h-8 w-8">
                <MoreHorizontal className="w-4 h-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={() => onTogglePin(query.id)}>
                {query.isPinned ? <StarOff className="w-4 h-4 mr-2" /> : <Star className="w-4 h-4 mr-2" />}
                {query.isPinned ? "고정 해제" : "고정"}
              </DropdownMenuItem>
              <DropdownMenuItem>
                <Share2 className="w-4 h-4 mr-2" />
                공유
              </DropdownMenuItem>
              <DropdownMenuItem>
                <Copy className="w-4 h-4 mr-2" />
                복제
              </DropdownMenuItem>
              <DropdownMenuItem>
                <Calendar className="w-4 h-4 mr-2" />
                스케줄 설정
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem className="text-destructive">
                <Trash2 className="w-4 h-4 mr-2" />
                삭제
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
        <CardDescription className="text-xs mt-2">{query.description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Metrics */}
        <div className="grid grid-cols-3 gap-2">
          {query.metrics.map((metric, idx) => (
            <div key={idx} className="text-center p-2 rounded-lg bg-secondary/50">
              <div className="text-sm font-semibold text-foreground flex items-center justify-center gap-1">
                {metric.value}
                {metric.trend === "up" && <TrendingUp className="w-3 h-3 text-destructive" />}
                {metric.trend === "down" && <TrendingDown className="w-3 h-3 text-primary" />}
              </div>
              <div className="text-[10px] text-muted-foreground">{metric.label}</div>
            </div>
          ))}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between pt-2 border-t border-border">
          <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
            <div className="flex items-center gap-1">
              <Clock className="w-3 h-3" />
              {query.lastRun}
            </div>
            {query.schedule && (
              <div className="flex items-center gap-1">
                <Calendar className="w-3 h-3" />
                {query.schedule}
              </div>
            )}
          </div>
          <Button size="sm" variant="ghost" className="h-7 gap-1 text-xs">
            <Play className="w-3 h-3" />
            실행
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
