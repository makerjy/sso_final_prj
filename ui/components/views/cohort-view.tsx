"use client"

import { useState, useCallback } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Slider } from "@/components/ui/slider"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  Users, 
  Plus,
  Filter,
  Download,
  Play,
  Loader2,
  TrendingUp,
  TrendingDown,
  ArrowRight,
  RefreshCw,
  AlertTriangle
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
  Legend,
  Tooltip,
} from "recharts"

// Mock survival data for What-if analysis
const generateSurvivalData = (multiplier: number) => [
  { time: 0, current: 100, simulated: 100 },
  { time: 7, current: 94.2, simulated: 94.2 * multiplier },
  { time: 14, current: 88.5, simulated: 88.5 * multiplier },
  { time: 21, current: 82.1, simulated: 82.1 * multiplier },
  { time: 30, current: 75.8, simulated: 75.8 * multiplier },
  { time: 45, current: 68.3, simulated: 68.3 * multiplier },
  { time: 60, current: 61.2, simulated: 61.2 * multiplier },
  { time: 75, current: 54.8, simulated: 54.8 * multiplier },
  { time: 90, current: 48.5, simulated: 48.5 * multiplier },
  { time: 120, current: 39.2, simulated: 39.2 * multiplier },
  { time: 150, current: 31.5, simulated: 31.5 * multiplier },
  { time: 180, current: 25.1, simulated: Math.min(25.1 * multiplier, 100) },
]

export function CohortView() {
  const [isLoading, setIsLoading] = useState(false)
  
  // What-if parameters
  const [readmitDays, setReadmitDays] = useState([30])
  const [ageThreshold, setAgeThreshold] = useState([65])
  const [losThreshold, setLosThreshold] = useState([7])
  
  // Calculated metrics based on parameters
  const basePatients = 1247
  const baseReadmitRate = 12.4
  const baseMortality = 74.9
  const baseLos = 12.3

  const patientMultiplier = (65 - ageThreshold[0] + 65) / 65
  const simulatedPatients = Math.round(basePatients * patientMultiplier * (losThreshold[0] / 7))
  const simulatedReadmitRate = baseReadmitRate * (30 / readmitDays[0])
  const simulatedMortality = baseMortality * (ageThreshold[0] / 65) * 0.95
  const simulatedLos = baseLos * (losThreshold[0] / 7) * 0.8

  const survivalMultiplier = 1 + (65 - ageThreshold[0]) * 0.01 + (30 - readmitDays[0]) * 0.005
  const survivalData = generateSurvivalData(Math.min(survivalMultiplier, 1.2))

  const handleRunSimulation = async () => {
    setIsLoading(true)
    await new Promise(resolve => setTimeout(resolve, 1500))
    setIsLoading(false)
  }

  const getChangeIndicator = (current: number, simulated: number, inverse = false) => {
    const diff = simulated - current
    const isPositive = inverse ? diff < 0 : diff > 0
    const icon = isPositive ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />
    const color = isPositive ? "text-primary" : "text-destructive"
    return (
      <span className={cn("flex items-center gap-1 text-xs", color)}>
        {icon}
        {diff > 0 ? "+" : ""}{diff.toFixed(1)}
      </span>
    )
  }

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-foreground">코호트 생성 & What-if 분석</h2>
          <p className="text-sm text-muted-foreground mt-1">가상 코호트를 생성하고 조건 변경에 따른 결과를 시뮬레이션합니다</p>
          <Badge variant="secondary" className="mt-2">부가 기능</Badge>
        </div>
        <Button className="gap-2 w-full sm:w-auto">
          <Plus className="w-4 h-4" />
          새 코호트 생성
        </Button>
      </div>

      <Tabs defaultValue="whatif" className="space-y-4">
        <TabsList className="w-full sm:w-auto">
          <TabsTrigger value="whatif" className="text-xs sm:text-sm">What-if 시뮬레이션</TabsTrigger>
          <TabsTrigger value="cohorts" className="text-xs sm:text-sm">저장된 코호트</TabsTrigger>
        </TabsList>

        {/* What-if Analysis Tab */}
        <TabsContent value="whatif" className="space-y-4 sm:space-y-6">
          <div className="grid lg:grid-cols-3 gap-4 sm:gap-6">
            {/* Parameters */}
            <Card className="lg:col-span-1">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <Filter className="w-5 h-5" />
                  시뮬레이션 조건
                </CardTitle>
                <CardDescription>조건을 변경하여 지표 변화를 확인하세요</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Readmission Days */}
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-sm">재입원 기준일</Label>
                    <Badge variant="outline">{readmitDays[0]}일</Badge>
                  </div>
                  <Slider
                    value={readmitDays}
                    onValueChange={setReadmitDays}
                    min={7}
                    max={90}
                    step={1}
                    className="w-full"
                  />
                  <div className="flex justify-between text-[10px] text-muted-foreground">
                    <span>7일</span>
                    <span>현재: 30일</span>
                    <span>90일</span>
                  </div>
                </div>

                {/* Age Threshold */}
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-sm">연령 기준</Label>
                    <Badge variant="outline">{ageThreshold[0]}세 이상</Badge>
                  </div>
                  <Slider
                    value={ageThreshold}
                    onValueChange={setAgeThreshold}
                    min={18}
                    max={85}
                    step={1}
                    className="w-full"
                  />
                  <div className="flex justify-between text-[10px] text-muted-foreground">
                    <span>18세</span>
                    <span>현재: 65세</span>
                    <span>85세</span>
                  </div>
                </div>

                {/* LOS Threshold */}
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-sm">재원일수 기준</Label>
                    <Badge variant="outline">{losThreshold[0]}일 이상</Badge>
                  </div>
                  <Slider
                    value={losThreshold}
                    onValueChange={setLosThreshold}
                    min={1}
                    max={30}
                    step={1}
                    className="w-full"
                  />
                  <div className="flex justify-between text-[10px] text-muted-foreground">
                    <span>1일</span>
                    <span>현재: 7일</span>
                    <span>30일</span>
                  </div>
                </div>

                <Button 
                  onClick={handleRunSimulation} 
                  disabled={isLoading}
                  className="w-full gap-2"
                >
                  {isLoading ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Play className="w-4 h-4" />
                  )}
                  시뮬레이션 실행
                </Button>

                <Button variant="outline" className="w-full gap-2 bg-transparent" onClick={() => {
                  setReadmitDays([30])
                  setAgeThreshold([65])
                  setLosThreshold([7])
                }}>
                  <RefreshCw className="w-4 h-4" />
                  조건 초기화
                </Button>
              </CardContent>
            </Card>

            {/* Results */}
            <Card className="lg:col-span-2">
              <CardHeader>
                <CardTitle className="text-lg">시뮬레이션 결과</CardTitle>
                <CardDescription>현재 기준 대비 예상 변화량</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* Metrics Comparison */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  {[
                    { 
                      label: "대상 환자 수", 
                      current: basePatients, 
                      simulated: simulatedPatients,
                      unit: "명",
                      inverse: false
                    },
                    { 
                      label: "재입원율", 
                      current: baseReadmitRate, 
                      simulated: simulatedReadmitRate,
                      unit: "%",
                      inverse: true
                    },
                    { 
                      label: "사망률", 
                      current: baseMortality, 
                      simulated: simulatedMortality,
                      unit: "%",
                      inverse: true
                    },
                    { 
                      label: "평균 재원일수", 
                      current: baseLos, 
                      simulated: simulatedLos,
                      unit: "일",
                      inverse: true
                    },
                  ].map((metric) => (
                    <div key={metric.label} className="p-4 rounded-lg bg-secondary/30 border border-border">
                      <div className="text-xs text-muted-foreground mb-2">{metric.label}</div>
                      <div className="flex items-end gap-2">
                        <span className="text-lg font-bold text-foreground">
                          {metric.simulated.toLocaleString(undefined, { maximumFractionDigits: 1 })}
                        </span>
                        <span className="text-xs text-muted-foreground">{metric.unit}</span>
                      </div>
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-xs text-muted-foreground">
                          현재: {metric.current.toLocaleString(undefined, { maximumFractionDigits: 1 })}{metric.unit}
                        </span>
                        {getChangeIndicator(metric.current, metric.simulated, metric.inverse)}
                      </div>
                    </div>
                  ))}
                </div>

                {/* Survival Curve Comparison */}
                <div>
                  <h4 className="text-sm font-medium text-foreground mb-3">생존 곡선 비교</h4>
                  <div className="h-[250px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={survivalData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
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
                        <Legend verticalAlign="top" height={36} />
                        <ReferenceLine y={50} stroke="#475569" strokeDasharray="5 5" />
                        <Line 
                          type="stepAfter" 
                          dataKey="current" 
                          stroke="#64748b" 
                          strokeWidth={2}
                          strokeDasharray="5 5"
                          dot={false}
                          name="현재 기준"
                        />
                        <Line 
                          type="stepAfter" 
                          dataKey="simulated" 
                          stroke="#22c55e" 
                          strokeWidth={2}
                          dot={false}
                          name="시뮬레이션"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                {/* Insight */}
                <div className="p-4 rounded-lg bg-primary/10 border border-primary/30">
                  <h4 className="text-sm font-medium text-foreground mb-2 flex items-center gap-2">
                    <AlertTriangle className="w-4 h-4 text-primary" />
                    분석 인사이트
                  </h4>
                  <p className="text-sm text-muted-foreground">
                    {readmitDays[0] < 30 && (
                      <>재입원 기준을 {readmitDays[0]}일로 단축하면 재입원율이 약 {(simulatedReadmitRate - baseReadmitRate).toFixed(1)}%p 증가할 것으로 예상됩니다. </>
                    )}
                    {ageThreshold[0] < 65 && (
                      <>연령 기준을 {ageThreshold[0]}세로 낮추면 대상 환자가 약 {Math.round((simulatedPatients - basePatients) / basePatients * 100)}% 증가합니다. </>
                    )}
                    {ageThreshold[0] > 65 && (
                      <>연령 기준을 {ageThreshold[0]}세로 높이면 고위험군에 집중하여 사망률이 더 높게 관찰될 수 있습니다. </>
                    )}
                    정책 변경 전 추가적인 하위그룹 분석을 권장합니다.
                  </p>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Saved Cohorts Tab */}
        <TabsContent value="cohorts" className="space-y-4">
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
            {[
              { id: 1, name: "65세 이상 심부전", count: 1247, created: "2024-12-15", status: "active" },
              { id: 2, name: "30일 재입원 고위험군", count: 423, created: "2024-12-14", status: "active" },
              { id: 3, name: "ICU 장기 재원 환자", count: 156, created: "2024-12-10", status: "archived" },
              { id: 4, name: "당뇨 + 고혈압 복합", count: 892, created: "2024-12-08", status: "active" },
            ].map((cohort) => (
              <Card key={cohort.id} className="hover:border-primary/30 transition-colors">
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-sm">{cohort.name}</CardTitle>
                    <Badge variant={cohort.status === "active" ? "default" : "secondary"} className="text-[10px]">
                      {cohort.status === "active" ? "활성" : "보관"}
                    </Badge>
                  </div>
                  <CardDescription className="text-xs">생성일: {cohort.created}</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Users className="w-4 h-4 text-muted-foreground" />
                      <span className="text-lg font-bold text-foreground">{cohort.count.toLocaleString()}</span>
                      <span className="text-xs text-muted-foreground">명</span>
                    </div>
                    <div className="flex items-center gap-1">
                      <Button variant="ghost" size="sm" className="h-7 gap-1 text-xs">
                        <Download className="w-3 h-3" />
                        내보내기
                      </Button>
                      <Button variant="ghost" size="sm" className="h-7 gap-1 text-xs">
                        <ArrowRight className="w-3 h-3" />
                        분석
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>
      </Tabs>
    </div>
  )
}
