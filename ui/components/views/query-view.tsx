"use client"

import { useState, useCallback } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Textarea } from "@/components/ui/textarea"
import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"
import { 
  Send, 
  Code, 
  BarChart3,
  CheckCircle2,
  AlertTriangle,
  XCircle,
  Play,
  Loader2,
  Eye,
  Pencil,
  Shield,
  Clock,
  Table2,
  FileText,
  RefreshCw,
  Copy,
  Download
} from "lucide-react"
import { cn } from "@/lib/utils"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  ReferenceLine,
  Tooltip,
} from "recharts"

interface ChatMessage {
  id: string
  role: "user" | "assistant"
  content: string
  timestamp: Date
  sql?: string
  validation?: {
    status: "safe" | "warning" | "danger"
    checks: { name: string; passed: boolean; message: string }[]
  }
}

const mockValidation = {
  status: "safe" as const,
  checks: [
    { name: "Read-Only 검사", passed: true, message: "SELECT 쿼리만 포함됨" },
    { name: "테이블 권한", passed: true, message: "허용된 테이블만 접근" },
    { name: "결과 제한", passed: true, message: "LIMIT 절 포함 (100행)" },
    { name: "실행 시간", passed: true, message: "예상 실행 시간 2.3초" },
  ]
}

const mockSurvivalData = [
  { time: 0, survival: 100 },
  { time: 7, survival: 94.2 },
  { time: 14, survival: 88.5 },
  { time: 21, survival: 82.1 },
  { time: 30, survival: 75.8 },
  { time: 45, survival: 68.3 },
  { time: 60, survival: 61.2 },
  { time: 75, survival: 54.8 },
  { time: 90, survival: 48.5 },
  { time: 120, survival: 39.2 },
  { time: 150, survival: 31.5 },
  { time: 180, survival: 25.1 },
]

const mockResultData = [
  { subject_id: 10023456, age: 72, gender: "M", admission_date: "2024-11-15", los_days: 8, status: "생존" },
  { subject_id: 10034567, age: 68, gender: "F", admission_date: "2024-11-12", los_days: 12, status: "사망" },
  { subject_id: 10045678, age: 81, gender: "M", admission_date: "2024-11-08", los_days: 15, status: "생존" },
  { subject_id: 10056789, age: 75, gender: "F", admission_date: "2024-11-05", los_days: 6, status: "생존" },
  { subject_id: 10067890, age: 69, gender: "M", admission_date: "2024-10-28", los_days: 22, status: "사망" },
]

export function QueryView() {
  const [isTechnicalMode, setIsTechnicalMode] = useState(false)
  const [query, setQuery] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [showResults, setShowResults] = useState(false)
  const [editedSql, setEditedSql] = useState("")
  const [isEditing, setIsEditing] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([])

  const generatedSql = `WITH heart_failure_patients AS (
  SELECT DISTINCT
    p.subject_id,
    p.gender,
    EXTRACT(YEAR FROM a.admittime) - p.anchor_year + p.anchor_age AS age,
    a.admittime AS admission_date,
    a.dischtime,
    p.dod AS death_date
  FROM mimiciv_hosp.patients p
  INNER JOIN mimiciv_hosp.admissions a 
    ON p.subject_id = a.subject_id
  INNER JOIN mimiciv_hosp.diagnoses_icd d 
    ON a.hadm_id = d.hadm_id
  WHERE d.icd_code IN ('I50', 'I500', 'I501', 'I509', '4280', '4281', '4289')
    AND (EXTRACT(YEAR FROM a.admittime) - p.anchor_year + p.anchor_age) >= 65
)
SELECT 
  subject_id, age, gender, admission_date,
  COALESCE(death_date, dischtime) - admission_date AS los_days,
  CASE WHEN death_date IS NOT NULL THEN '사망' ELSE '생존' END AS status
FROM heart_failure_patients
ORDER BY admission_date DESC
LIMIT 100;`

  const handleSubmit = useCallback(async () => {
    if (!query.trim()) return
    
    setIsLoading(true)
    const newMessage: ChatMessage = {
      id: Date.now().toString(),
      role: "user",
      content: query,
      timestamp: new Date()
    }
    setMessages(prev => [...prev, newMessage])
    setQuery("")
    
    await new Promise(resolve => setTimeout(resolve, 1500))
    
    const responseMessage: ChatMessage = {
      id: (Date.now() + 1).toString(),
      role: "assistant",
      content: "65세 이상 심부전 환자 코호트를 생성했습니다. 총 1,247명의 환자가 조건에 부합합니다.",
      timestamp: new Date(),
      sql: generatedSql,
      validation: mockValidation
    }
    setMessages(prev => [...prev, responseMessage])
    setEditedSql(generatedSql)
    setShowResults(true)
    setIsLoading(false)
  }, [query, generatedSql])

  const handleExecuteEdited = async () => {
    setIsLoading(true)
    await new Promise(resolve => setTimeout(resolve, 1000))
    setIsLoading(false)
    setIsEditing(false)
  }

  const getValidationIcon = (status: string) => {
    switch (status) {
      case "safe": return <CheckCircle2 className="w-4 h-4 text-primary" />
      case "warning": return <AlertTriangle className="w-4 h-4 text-yellow-500" />
      case "danger": return <XCircle className="w-4 h-4 text-destructive" />
      default: return null
    }
  }

  return (
    <div className="flex flex-col h-[calc(100vh-56px)] sm:h-[calc(100vh-64px)]">
      {/* Header */}
      <div className="p-3 sm:p-4 border-b border-border bg-card/50">
        <div className="flex items-center justify-between gap-2">
          <div className="min-w-0">
            <h2 className="text-base sm:text-lg font-semibold text-foreground truncate">쿼리 & 분석</h2>
            <p className="text-[10px] sm:text-xs text-muted-foreground hidden sm:block">자연어로 질문하고 SQL 결과를 확인하세요</p>
          </div>
          <div className="flex items-center gap-2 sm:gap-4 shrink-0">
            <div className="flex items-center gap-1 sm:gap-2">
              <Label htmlFor="mode-switch" className="text-[10px] sm:text-xs text-muted-foreground whitespace-nowrap">
                {isTechnicalMode ? "기술" : "비기술"}
              </Label>
              <Switch 
                id="mode-switch"
                checked={isTechnicalMode}
                onCheckedChange={setIsTechnicalMode}
              />
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col lg:flex-row overflow-hidden">
        {/* Chat Panel */}
        <div className="flex-1 flex flex-col lg:border-r border-border min-h-0">
          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            {messages.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-full text-center">
                <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center mb-4">
                  <Send className="w-6 h-6 text-primary" />
                </div>
                <h3 className="font-medium text-foreground mb-2">질문을 입력하세요</h3>
                <p className="text-sm text-muted-foreground max-w-sm">
                  예: "65세 이상 심부전 환자 코호트 만들어줘, 생존 곡선 그려줘"
                </p>
              </div>
            ) : (
              messages.map((message) => (
                <div key={message.id} className={cn(
                  "flex",
                  message.role === "user" ? "justify-end" : "justify-start"
                )}>
                  <div className={cn(
                    "max-w-[80%] rounded-lg p-3",
                    message.role === "user" 
                      ? "bg-primary text-primary-foreground" 
                      : "bg-secondary"
                  )}>
                    <p className="text-sm">{message.content}</p>
                    <span className="text-[10px] opacity-70 mt-1 block">
                      {message.timestamp.toLocaleTimeString()}
                    </span>
                  </div>
                </div>
              ))
            )}
            {isLoading && (
              <div className="flex justify-start">
                <div className="bg-secondary rounded-lg p-3 flex items-center gap-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  <span className="text-sm">분석 중...</span>
                </div>
              </div>
            )}
          </div>

          {/* Input */}
          <div className="p-4 border-t border-border">
            <div className="flex gap-2">
              <Textarea
                placeholder="자연어로 질문하세요..."
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="min-h-[60px] resize-none"
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault()
                    handleSubmit()
                  }
                }}
              />
              <Button 
                onClick={handleSubmit} 
                disabled={isLoading || !query.trim()}
                className="px-4"
              >
                <Send className="w-4 h-4" />
              </Button>
            </div>
          </div>
        </div>

        {/* Results Panel */}
        {showResults && (
          <div className="lg:w-[55%] flex flex-col overflow-hidden border-t lg:border-t-0 border-border max-h-[50vh] lg:max-h-none">
            <Tabs defaultValue={isTechnicalMode ? "sql" : "results"} className="flex-1 flex flex-col">
              <div className="px-4 pt-2 border-b border-border">
                <TabsList className="h-9">
                  {isTechnicalMode && (
                    <TabsTrigger value="sql" className="gap-1.5 text-xs">
                      <Code className="w-3.5 h-3.5" />
                      SQL
                    </TabsTrigger>
                  )}
                  <TabsTrigger value="results" className="gap-1.5 text-xs">
                    <Table2 className="w-3.5 h-3.5" />
                    결과
                  </TabsTrigger>
                  <TabsTrigger value="chart" className="gap-1.5 text-xs">
                    <BarChart3 className="w-3.5 h-3.5" />
                    차트
                  </TabsTrigger>
                  <TabsTrigger value="interpretation" className="gap-1.5 text-xs">
                    <FileText className="w-3.5 h-3.5" />
                    해석
                  </TabsTrigger>
                </TabsList>
              </div>

              {/* SQL Tab */}
              {isTechnicalMode && (
                <TabsContent value="sql" className="flex-1 overflow-y-auto p-4 space-y-4 mt-0">
                  {/* Validation Panel */}
                  <Card className={cn(
                    "border-l-4",
                    mockValidation.status === "safe" && "border-l-primary",
                    mockValidation.status === "warning" && "border-l-yellow-500",
                    mockValidation.status === "danger" && "border-l-destructive"
                  )}>
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm flex items-center gap-2">
                        <Shield className="w-4 h-4" />
                        안전 검증 결과
                        {getValidationIcon(mockValidation.status)}
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-2 gap-2">
                        {mockValidation.checks.map((check, idx) => (
                          <div key={idx} className="flex items-center gap-2 text-xs">
                            {check.passed ? (
                              <CheckCircle2 className="w-3 h-3 text-primary" />
                            ) : (
                              <XCircle className="w-3 h-3 text-destructive" />
                            )}
                            <span className="text-muted-foreground">{check.name}:</span>
                            <span className="text-foreground">{check.message}</span>
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>

                  {/* SQL Editor */}
                  <Card>
                    <CardHeader className="pb-2">
                      <div className="flex items-center justify-between">
                        <CardTitle className="text-sm">생성된 SQL</CardTitle>
                        <div className="flex items-center gap-2">
                          <Button variant="ghost" size="sm" className="h-7 gap-1">
                            <Copy className="w-3 h-3" />
                            복사
                          </Button>
                          <Button 
                            variant="ghost" 
                            size="sm" 
                            className="h-7 gap-1"
                            onClick={() => setIsEditing(!isEditing)}
                          >
                            {isEditing ? <Eye className="w-3 h-3" /> : <Pencil className="w-3 h-3" />}
                            {isEditing ? "미리보기" : "편집"}
                          </Button>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent>
                      {isEditing ? (
                        <div className="space-y-3">
                          <Textarea
                            value={editedSql}
                            onChange={(e) => setEditedSql(e.target.value)}
                            className="font-mono text-xs min-h-[200px] bg-secondary/50"
                          />
                          <div className="flex items-center gap-2">
                            <Button 
                              size="sm" 
                              onClick={handleExecuteEdited}
                              disabled={isLoading}
                              className="gap-1"
                            >
                              {isLoading ? (
                                <Loader2 className="w-3 h-3 animate-spin" />
                              ) : (
                                <Play className="w-3 h-3" />
                              )}
                              재검증 후 실행
                            </Button>
                            <Button variant="ghost" size="sm" onClick={() => setEditedSql(generatedSql)}>
                              <RefreshCw className="w-3 h-3 mr-1" />
                              초기화
                            </Button>
                          </div>
                          <p className="text-[10px] text-muted-foreground flex items-center gap-1">
                            <AlertTriangle className="w-3 h-3" />
                            수정된 SQL은 재검증을 통과해야 실행됩니다
                          </p>
                        </div>
                      ) : (
                        <pre className="p-3 rounded-lg bg-secondary/50 text-xs font-mono text-foreground overflow-x-auto whitespace-pre-wrap">
                          {editedSql || generatedSql}
                        </pre>
                      )}
                    </CardContent>
                  </Card>
                </TabsContent>
              )}

              {/* Results Tab */}
              <TabsContent value="results" className="flex-1 overflow-y-auto p-4 mt-0">
                <Card>
                  <CardHeader className="pb-2">
                    <div className="flex items-center justify-between">
                      <CardTitle className="text-sm">쿼리 결과</CardTitle>
                      <div className="flex items-center gap-2">
                        <Badge variant="secondary" className="text-xs">1,247 rows</Badge>
                        <Button variant="ghost" size="sm" className="h-7 gap-1">
                          <Download className="w-3 h-3" />
                          CSV
                        </Button>
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="rounded-lg border border-border overflow-hidden">
                      <table className="w-full text-xs">
                        <thead className="bg-secondary/50">
                          <tr>
                            <th className="text-left p-2 font-medium">subject_id</th>
                            <th className="text-left p-2 font-medium">age</th>
                            <th className="text-left p-2 font-medium">gender</th>
                            <th className="text-left p-2 font-medium">admission_date</th>
                            <th className="text-left p-2 font-medium">los_days</th>
                            <th className="text-left p-2 font-medium">status</th>
                          </tr>
                        </thead>
                        <tbody>
                          {mockResultData.map((row, idx) => (
                            <tr key={idx} className="border-t border-border hover:bg-secondary/30">
                              <td className="p-2 font-mono">{row.subject_id}</td>
                              <td className="p-2">{row.age}</td>
                              <td className="p-2">{row.gender}</td>
                              <td className="p-2">{row.admission_date}</td>
                              <td className="p-2">{row.los_days}</td>
                              <td className="p-2">
                                <Badge variant={row.status === "생존" ? "default" : "destructive"} className="text-[10px]">
                                  {row.status}
                                </Badge>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Chart Tab */}
              <TabsContent value="chart" className="flex-1 overflow-y-auto p-4 mt-0">
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm">Kaplan-Meier 생존 곡선</CardTitle>
                    <CardDescription className="text-xs">65세 이상 심부전 환자 코호트 (n=1,247)</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-[300px]">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={mockSurvivalData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                          <XAxis 
                            dataKey="time" 
                            stroke="#64748b"
                            tick={{ fontSize: 10 }}
                            label={{ value: '시간 (일)', position: 'bottom', offset: -5, fontSize: 10, fill: '#64748b' }}
                          />
                          <YAxis 
                            stroke="#64748b"
                            tick={{ fontSize: 10 }}
                            domain={[0, 100]}
                            label={{ value: '생존율 (%)', angle: -90, position: 'insideLeft', fontSize: 10, fill: '#64748b' }}
                          />
                          <Tooltip 
                            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: '8px' }}
                            labelStyle={{ color: '#94a3b8' }}
                          />
                          <ReferenceLine y={50} stroke="#475569" strokeDasharray="5 5" />
                          <Line 
                            type="stepAfter" 
                            dataKey="survival" 
                            stroke="#22c55e" 
                            strokeWidth={2}
                            dot={false}
                          />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                    <div className="grid grid-cols-3 gap-4 mt-4 pt-4 border-t border-border">
                      <div className="text-center">
                        <div className="text-lg font-bold text-foreground">82일</div>
                        <div className="text-[10px] text-muted-foreground">중앙 생존 시간</div>
                      </div>
                      <div className="text-center">
                        <div className="text-lg font-bold text-foreground">1,247</div>
                        <div className="text-[10px] text-muted-foreground">총 환자 수</div>
                      </div>
                      <div className="text-center">
                        <div className="text-lg font-bold text-foreground">934</div>
                        <div className="text-[10px] text-muted-foreground">총 사망 수</div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Interpretation Tab */}
              <TabsContent value="interpretation" className="flex-1 overflow-y-auto p-4 mt-0">
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm">분석 해석</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="p-4 rounded-lg bg-primary/10 border border-primary/30">
                      <h4 className="font-medium text-foreground mb-2">주요 발견</h4>
                      <ul className="text-sm text-muted-foreground space-y-2">
                        <li className="flex items-start gap-2">
                          <CheckCircle2 className="w-4 h-4 text-primary mt-0.5 shrink-0" />
                          65세 이상 심부전 환자 1,247명 중 934명(74.9%)이 180일 내 사망
                        </li>
                        <li className="flex items-start gap-2">
                          <CheckCircle2 className="w-4 h-4 text-primary mt-0.5 shrink-0" />
                          중앙 생존 시간은 82일로, 환자의 절반이 82일 이내 사망
                        </li>
                        <li className="flex items-start gap-2">
                          <CheckCircle2 className="w-4 h-4 text-primary mt-0.5 shrink-0" />
                          초기 7일 내 급격한 생존율 감소 관찰 (100% → 94.2%)
                        </li>
                      </ul>
                    </div>

                    <div className="p-4 rounded-lg bg-secondary/50 border border-border">
                      <h4 className="font-medium text-foreground mb-2">통계적 요약</h4>
                      <div className="grid grid-cols-2 gap-3 text-sm">
                        <div>
                          <span className="text-muted-foreground">평균 연령:</span>
                          <span className="ml-2 text-foreground">73.4세</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">성별 비율:</span>
                          <span className="ml-2 text-foreground">남 58% / 여 42%</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">평균 재원일수:</span>
                          <span className="ml-2 text-foreground">12.3일</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">30일 생존율:</span>
                          <span className="ml-2 text-foreground">75.8%</span>
                        </div>
                      </div>
                    </div>

                    <div className="p-4 rounded-lg border border-yellow-500/30 bg-yellow-500/5">
                      <h4 className="font-medium text-foreground mb-2 flex items-center gap-2">
                        <AlertTriangle className="w-4 h-4 text-yellow-500" />
                        주의사항
                      </h4>
                      <p className="text-sm text-muted-foreground">
                        본 분석은 후향적 데이터에 기반하며, 다양한 교란 변수가 통제되지 않았습니다. 
                        임상적 의사결정에 직접 사용하기 전에 추가적인 분석이 필요합니다.
                      </p>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          </div>
        )}
      </div>
    </div>
  )
}
