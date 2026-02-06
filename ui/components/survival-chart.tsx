"use client"

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  ReferenceLine,
  Area,
  ComposedChart,
} from "recharts"
import { Activity, Info } from "lucide-react"
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart"

interface SurvivalDataPoint {
  time: number
  survival: number
  lowerCI: number
  upperCI: number
  atRisk: number
  events: number
}

interface SurvivalChartProps {
  data: SurvivalDataPoint[]
  medianSurvival: number
  totalPatients: number
  totalEvents: number
}

export function SurvivalChart({ data, medianSurvival, totalPatients, totalEvents }: SurvivalChartProps) {
  const chartConfig = {
    survival: {
      label: "생존율",
      color: "#3ecf8e",
    },
    ci: {
      label: "95% CI",
      color: "#3ecf8e",
    },
  }

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary/10">
            <Activity className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h3 className="text-sm font-medium text-foreground">Kaplan-Meier 생존 곡선</h3>
            <p className="text-xs text-muted-foreground">65세 이상 심부전 환자 코호트</p>
          </div>
        </div>
        <div className="flex items-center gap-4 text-sm">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 rounded-full bg-primary" />
            <span className="text-muted-foreground">생존율</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-3 h-0.5 bg-primary/30" />
            <span className="text-muted-foreground">95% CI</span>
          </div>
        </div>
      </div>

      <div className="p-4">
        <div className="grid grid-cols-3 gap-4 mb-6">
          <div className="rounded-lg bg-secondary/50 p-3">
            <p className="text-xs text-muted-foreground">대상 환자</p>
            <p className="text-xl font-semibold text-foreground">{totalPatients.toLocaleString()}</p>
          </div>
          <div className="rounded-lg bg-secondary/50 p-3">
            <p className="text-xs text-muted-foreground">이벤트 (사망)</p>
            <p className="text-xl font-semibold text-foreground">{totalEvents.toLocaleString()}</p>
          </div>
          <div className="rounded-lg bg-secondary/50 p-3">
            <p className="text-xs text-muted-foreground">중앙 생존 시간</p>
            <p className="text-xl font-semibold text-primary">{medianSurvival}일</p>
          </div>
        </div>

        <ChartContainer config={chartConfig} className="h-[350px]">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis
                dataKey="time"
                stroke="var(--muted-foreground)"
                fontSize={12}
                tickLine={false}
                axisLine={false}
                tickFormatter={(value) => `${value}일`}
              />
              <YAxis
                stroke="var(--muted-foreground)"
                fontSize={12}
                tickLine={false}
                axisLine={false}
                domain={[0, 100]}
                tickFormatter={(value) => `${value}%`}
              />
              <ChartTooltip
                content={
                  <ChartTooltipContent
                    formatter={(value, name) => {
                      if (name === "survival") return [`${Number(value).toFixed(1)}%`, "생존율"]
                      return [value, name]
                    }}
                  />
                }
              />
              <defs>
                <linearGradient id="survivalGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#3ecf8e" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#3ecf8e" stopOpacity={0.05} />
                </linearGradient>
              </defs>
              <Area
                type="stepAfter"
                dataKey="upperCI"
                stroke="none"
                fill="url(#survivalGradient)"
                fillOpacity={1}
              />
              <Area
                type="stepAfter"
                dataKey="lowerCI"
                stroke="none"
                fill="var(--background)"
                fillOpacity={1}
              />
              <ReferenceLine
                y={50}
                stroke="var(--muted-foreground)"
                strokeDasharray="5 5"
                strokeOpacity={0.5}
              />
              <ReferenceLine
                x={medianSurvival}
                stroke="var(--primary)"
                strokeDasharray="5 5"
                strokeOpacity={0.7}
              />
              <Line
                type="stepAfter"
                dataKey="survival"
                stroke="#3ecf8e"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: "#3ecf8e", stroke: "var(--background)", strokeWidth: 2 }}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartContainer>

        <div className="mt-4 rounded-lg bg-secondary/30 p-3 flex items-start gap-2">
          <Info className="w-4 h-4 text-muted-foreground mt-0.5 shrink-0" />
          <div className="text-xs text-muted-foreground">
            <p><strong className="text-foreground">해석:</strong> 65세 이상 심부전 환자의 중앙 생존 시간은 {medianSurvival}일입니다.</p>
            <p className="mt-1">30일 생존율: {data.find(d => d.time === 30)?.survival.toFixed(1) || "-"}% | 90일 생존율: {data.find(d => d.time === 90)?.survival.toFixed(1) || "-"}%</p>
          </div>
        </div>
      </div>
    </div>
  )
}
