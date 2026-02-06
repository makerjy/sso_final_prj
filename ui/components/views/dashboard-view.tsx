"use client"

import { type ReactNode, useEffect, useRef, useState } from "react"
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
  const [queries, setQueries] = useState<SavedQuery[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedCategory, setSelectedCategory] = useState("전체")
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const saveTimer = useRef<number | null>(null)

  const persistQueries = async (next: SavedQuery[], silent = false) => {
    if (!silent) {
      setSaving(true)
    }
    try {
      const res = await fetch("/dashboard/queries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ queries: next }),
      })
      if (!res.ok && !silent) {
        setError("결과 보드 저장에 실패했습니다.")
      }
    } catch (err) {
      if (!silent) {
        setError("결과 보드 저장에 실패했습니다.")
      }
    } finally {
      if (!silent) {
        setSaving(false)
      }
    }
  }

  const schedulePersist = (next: SavedQuery[]) => {
    if (saveTimer.current) {
      window.clearTimeout(saveTimer.current)
    }
    saveTimer.current = window.setTimeout(() => {
      persistQueries(next, true)
    }, 400)
  }

  const loadQueries = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch("/dashboard/queries")
      if (!res.ok) {
        throw new Error("Failed to fetch dashboard queries.")
      }
      const payload = await res.json()
      const remote = Array.isArray(payload?.queries) ? payload.queries : []
      if (remote.length > 0) {
        setQueries(remote)
      } else {
        setQueries(savedQueries)
        if (!payload?.detail) {
          persistQueries(savedQueries, true)
        }
      }
    } catch (err) {
      setQueries(savedQueries)
      setError("결과 보드를 불러오지 못했습니다.")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadQueries()
    return () => {
      if (saveTimer.current) {
        window.clearTimeout(saveTimer.current)
      }
    }
  }, [])

  const togglePin = (id: string) => {
    setQueries(prev => {
      const next = prev.map(q => q.id === id ? { ...q, isPinned: !q.isPinned } : q)
      schedulePersist(next)
      return next
    })
  }

  const handleDelete = (id: string) => {
    setQueries(prev => {
      const next = prev.filter(q => q.id !== id)
      schedulePersist(next)
      return next
    })
  }

  const handleDuplicate = (id: string) => {
    setQueries(prev => {
      const target = prev.find(q => q.id === id)
      if (!target) {
        return prev
      }
      const copy = {
        ...target,
        id: `copy-${Date.now()}`,
        title: `${target.title} (복제)`,
        isPinned: false,
        lastRun: "방금 생성",
      }
      const next = [copy, ...prev]
      schedulePersist(next)
      return next
    })
  }

  const handleAddQuery = () => {
    setQueries(prev => {
      const next = [
        {
          id: `new-${Date.now()}`,
          title: "새 쿼리",
          description: "설명을 입력하세요",
          query: "",
          lastRun: "방금 생성",
          isPinned: true,
          category: "전체",
          metrics: [
            { label: "지표 1", value: "-" },
            { label: "지표 2", value: "-" },
            { label: "지표 3", value: "-" },
          ],
          chartType: "bar" as const,
        },
        ...prev,
      ]
      schedulePersist(next)
      return next
    })
  }

  const handleShare = async (query: SavedQuery) => {
    try {
      await navigator.clipboard.writeText(query.query || query.title)
    } catch (err) {
      setError("클립보드 복사에 실패했습니다.")
    }
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
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 w-full max-w-none">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-foreground">결과 보드</h2>
          <p className="text-sm text-muted-foreground mt-1">자주 사용하는 쿼리와 분석 결과를 관리합니다</p>
        </div>
        <Button className="gap-2 w-full sm:w-auto" onClick={handleAddQuery} disabled={loading || saving}>
          <Plus className="w-4 h-4" />
          새 쿼리 추가
        </Button>
      </div>
      {error && (
        <div className="text-sm text-destructive">{error}</div>
      )}
      {saving && (
        <div className="text-xs text-muted-foreground">저장 중...</div>
      )}

      {/* Search and Filters */}
      <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3 sm:gap-4">
        <div className="relative flex-1 w-full">
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
              <QueryCard
                key={query.id}
                query={query}
                onTogglePin={togglePin}
                onDelete={handleDelete}
                onDuplicate={handleDuplicate}
                onShare={handleShare}
                getChartIcon={getChartIcon}
              />
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
              <QueryCard
                key={query.id}
                query={query}
                onTogglePin={togglePin}
                onDelete={handleDelete}
                onDuplicate={handleDuplicate}
                onShare={handleShare}
                getChartIcon={getChartIcon}
              />
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
  onDelete: (id: string) => void
  onDuplicate: (id: string) => void
  onShare: (query: SavedQuery) => void
  getChartIcon: (type: string) => ReactNode
}

function QueryCard({ query, onTogglePin, onDelete, onDuplicate, onShare, getChartIcon }: QueryCardProps) {
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
              <DropdownMenuItem onClick={() => onShare(query)}>
                <Share2 className="w-4 h-4 mr-2" />
                공유
              </DropdownMenuItem>
              <DropdownMenuItem onClick={() => onDuplicate(query.id)}>
                <Copy className="w-4 h-4 mr-2" />
                복제
              </DropdownMenuItem>
              <DropdownMenuItem>
                <Calendar className="w-4 h-4 mr-2" />
                스케줄 설정
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem className="text-destructive" onClick={() => onDelete(query.id)}>
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
