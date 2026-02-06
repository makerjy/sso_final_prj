"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { 
  FileText, 
  Clock, 
  User,
  Search,
  Filter,
  Download,
  ChevronDown,
  ChevronRight,
  Eye,
  Code,
  BookOpen,
  CheckCircle2,
  Shield,
  Database,
  Calendar
} from "lucide-react"
import { cn } from "@/lib/utils"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

interface AuditLog {
  id: string
  timestamp: string
  user: {
    name: string
    role: string
  }
  query: {
    original: string
    sql: string
  }
  appliedTerms: { term: string; version: string }[]
  appliedMetrics: { name: string; version: string }[]
  execution: {
    duration: string
    rowsReturned: number
    status: "success" | "error" | "warning"
  }
  resultSnapshot?: {
    summary: string
    downloadUrl: string
  }
}

const mockAuditLogs: AuditLog[] = [
  {
    id: "1",
    timestamp: "2024-12-15 14:32:15",
    user: { name: "김연구원", role: "연구원" },
    query: {
      original: "65세 이상 심부전 환자 코호트 만들어줘, 생존 곡선 그려줘",
      sql: `SELECT DISTINCT p.subject_id, p.gender,
  EXTRACT(YEAR FROM a.admittime) - p.anchor_year + p.anchor_age AS age
FROM mimiciv_hosp.patients p
INNER JOIN mimiciv_hosp.admissions a ON p.subject_id = a.subject_id
WHERE d.icd_code IN ('I50', 'I500', 'I501', 'I509')
  AND age >= 65
LIMIT 100;`
    },
    appliedTerms: [
      { term: "심부전", version: "v2.1" },
      { term: "노인", version: "v1.3" }
    ],
    appliedMetrics: [
      { name: "생존율", version: "v1.0" }
    ],
    execution: {
      duration: "2.34초",
      rowsReturned: 1247,
      status: "success"
    },
    resultSnapshot: {
      summary: "65세 이상 심부전 환자 1,247명 추출, 중앙 생존시간 82일",
      downloadUrl: "#"
    }
  },
  {
    id: "2",
    timestamp: "2024-12-15 11:45:22",
    user: { name: "박교수", role: "관리자" },
    query: {
      original: "지난 달 재입원율 15일 기준으로 계산해줘",
      sql: `SELECT COUNT(CASE WHEN readmit_days <= 15 THEN 1 END) * 100.0 / COUNT(*)
FROM admissions
WHERE dischtime >= '2024-11-01';`
    },
    appliedTerms: [
      { term: "재입원", version: "v1.5" }
    ],
    appliedMetrics: [
      { name: "재입원율", version: "v2.0" }
    ],
    execution: {
      duration: "1.12초",
      rowsReturned: 1,
      status: "success"
    },
    resultSnapshot: {
      summary: "15일 기준 재입원율: 8.7%",
      downloadUrl: "#"
    }
  },
  {
    id: "3",
    timestamp: "2024-12-14 16:20:08",
    user: { name: "이의사", role: "연구원" },
    query: {
      original: "당뇨 환자 중 ICU 입실한 환자 목록",
      sql: `SELECT p.*, i.intime, i.outtime
FROM patients p
INNER JOIN icustays i ON p.subject_id = i.subject_id
WHERE diagnosis LIKE '%diabetes%';`
    },
    appliedTerms: [
      { term: "당뇨", version: "v1.2" }
    ],
    appliedMetrics: [],
    execution: {
      duration: "3.56초",
      rowsReturned: 4521,
      status: "warning"
    },
    resultSnapshot: {
      summary: "당뇨 환자 ICU 입실 4,521건 (결과 제한 적용)",
      downloadUrl: "#"
    }
  },
  {
    id: "4",
    timestamp: "2024-12-14 09:15:33",
    user: { name: "최분석가", role: "연구원" },
    query: {
      original: "패혈증 환자 사망률 분석",
      sql: `SELECT 
  COUNT(CASE WHEN dod IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as mortality_rate
FROM patients p
WHERE diagnosis_code LIKE 'A41%';`
    },
    appliedTerms: [
      { term: "패혈증", version: "v1.0" }
    ],
    appliedMetrics: [
      { name: "사망률", version: "v1.1" }
    ],
    execution: {
      duration: "1.89초",
      rowsReturned: 1,
      status: "success"
    },
    resultSnapshot: {
      summary: "패혈증 환자 사망률: 31.2%",
      downloadUrl: "#"
    }
  },
]

export function AuditView() {
  const [logs, setLogs] = useState(mockAuditLogs)
  const [searchTerm, setSearchTerm] = useState("")
  const [expandedLogs, setExpandedLogs] = useState<string[]>([])
  const [dateFilter, setDateFilter] = useState("all")
  const [userFilter, setUserFilter] = useState("all")

  const toggleExpand = (id: string) => {
    setExpandedLogs(prev => 
      prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]
    )
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "success":
        return <Badge variant="default" className="text-[10px]">성공</Badge>
      case "error":
        return <Badge variant="destructive" className="text-[10px]">실패</Badge>
      case "warning":
        return <Badge variant="outline" className="text-[10px] border-yellow-500 text-yellow-500">경고</Badge>
      default:
        return null
    }
  }

  const filteredLogs = logs.filter(log => {
    const matchesSearch = log.query.original.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         log.user.name.toLowerCase().includes(searchTerm.toLowerCase())
    return matchesSearch
  })

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-foreground">감사 로그 (Audit Trail)</h2>
          <p className="text-sm text-muted-foreground mt-1">모든 쿼리 실행 기록과 의사결정 증적을 관리합니다</p>
        </div>
        <Button variant="outline" className="gap-2 bg-transparent w-full sm:w-auto">
          <Download className="w-4 h-4" />
          로그 내보내기
        </Button>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="py-3 sm:py-4">
          <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3 sm:gap-4">
            <div className="relative flex-1 sm:max-w-md">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input 
                placeholder="질문 또는 사용자 검색..." 
                className="pl-9"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
            </div>
            <div className="flex items-center gap-2">
              <Select value={dateFilter} onValueChange={setDateFilter}>
                <SelectTrigger className="w-full sm:w-[140px]">
                  <Calendar className="w-4 h-4 mr-2" />
                  <SelectValue placeholder="기간" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">전체 기간</SelectItem>
                  <SelectItem value="today">오늘</SelectItem>
                  <SelectItem value="week">최근 7일</SelectItem>
                  <SelectItem value="month">최근 30일</SelectItem>
                </SelectContent>
              </Select>
              <Select value={userFilter} onValueChange={setUserFilter}>
                <SelectTrigger className="w-full sm:w-[140px]">
                  <User className="w-4 h-4 mr-2" />
                  <SelectValue placeholder="사용자" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">전체 사용자</SelectItem>
                  <SelectItem value="researcher">연구원</SelectItem>
                  <SelectItem value="admin">관리자</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Summary Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
        {[
          { label: "총 쿼리 수", value: "1,247", icon: Database },
          { label: "오늘 실행", value: "23", icon: Clock },
          { label: "활성 사용자", value: "12", icon: User },
          { label: "성공률", value: "98.2%", icon: CheckCircle2 },
        ].map((stat) => (
          <Card key={stat.label}>
            <CardContent className="py-4">
              <div className="flex items-center gap-3">
                <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-primary/10">
                  <stat.icon className="w-5 h-5 text-primary" />
                </div>
                <div>
                  <div className="text-xl font-bold text-foreground">{stat.value}</div>
                  <div className="text-xs text-muted-foreground">{stat.label}</div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Audit Logs */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <FileText className="w-5 h-5" />
            실행 기록
          </CardTitle>
          <CardDescription>각 로그를 클릭하여 상세 정보를 확인하세요</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {filteredLogs.map((log) => (
            <Collapsible 
              key={log.id} 
              open={expandedLogs.includes(log.id)}
              onOpenChange={() => toggleExpand(log.id)}
            >
              <CollapsibleTrigger asChild>
                <div className={cn(
                  "flex items-center gap-4 p-4 rounded-lg border border-border cursor-pointer transition-colors hover:border-primary/30",
                  expandedLogs.includes(log.id) && "bg-secondary/30 border-primary/30"
                )}>
                  {expandedLogs.includes(log.id) ? (
                    <ChevronDown className="w-4 h-4 text-muted-foreground shrink-0" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-muted-foreground shrink-0" />
                  )}
                  
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="text-sm font-medium text-foreground truncate">
                        {log.query.original}
                      </span>
                      {getStatusBadge(log.execution.status)}
                    </div>
                    <div className="flex items-center gap-4 text-xs text-muted-foreground">
                      <span className="flex items-center gap-1">
                        <Clock className="w-3 h-3" />
                        {log.timestamp}
                      </span>
                      <span className="flex items-center gap-1">
                        <User className="w-3 h-3" />
                        {log.user.name} ({log.user.role})
                      </span>
                      <span>{log.execution.rowsReturned.toLocaleString()} rows</span>
                      <span>{log.execution.duration}</span>
                    </div>
                  </div>
                </div>
              </CollapsibleTrigger>

              <CollapsibleContent>
                <div className="ml-8 mt-2 p-4 rounded-lg bg-secondary/20 border border-border space-y-4">
                  {/* Applied Terms & Metrics */}
                  <div className="grid md:grid-cols-2 gap-4">
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                        <BookOpen className="w-3 h-3" />
                        적용된 용어
                      </div>
                      <div className="flex flex-wrap gap-1">
                        {log.appliedTerms.map((term, idx) => (
                          <Badge key={idx} variant="outline" className="text-[10px]">
                            {term.term} <span className="text-muted-foreground ml-1">{term.version}</span>
                          </Badge>
                        ))}
                        {log.appliedTerms.length === 0 && (
                          <span className="text-xs text-muted-foreground">없음</span>
                        )}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                        <Shield className="w-3 h-3" />
                        적용된 지표
                      </div>
                      <div className="flex flex-wrap gap-1">
                        {log.appliedMetrics.map((metric, idx) => (
                          <Badge key={idx} variant="outline" className="text-[10px]">
                            {metric.name} <span className="text-muted-foreground ml-1">{metric.version}</span>
                          </Badge>
                        ))}
                        {log.appliedMetrics.length === 0 && (
                          <span className="text-xs text-muted-foreground">없음</span>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* SQL Query */}
                  <div>
                    <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                      <Code className="w-3 h-3" />
                      실행된 SQL
                    </div>
                    <pre className="p-3 rounded-lg bg-background text-[11px] font-mono text-foreground overflow-x-auto whitespace-pre-wrap border border-border">
                      {log.query.sql}
                    </pre>
                  </div>

                  {/* Result Snapshot */}
                  {log.resultSnapshot && (
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                        <Eye className="w-3 h-3" />
                        결과 스냅샷
                      </div>
                      <div className="flex items-center justify-between p-3 rounded-lg bg-background border border-border">
                        <span className="text-sm text-foreground">{log.resultSnapshot.summary}</span>
                        <Button variant="ghost" size="sm" className="h-7 gap-1 text-xs">
                          <Download className="w-3 h-3" />
                          다운로드
                        </Button>
                      </div>
                    </div>
                  )}
                </div>
              </CollapsibleContent>
            </Collapsible>
          ))}
        </CardContent>
      </Card>
    </div>
  )
}
