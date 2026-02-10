"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Label } from "@/components/ui/label"
import { Slider } from "@/components/ui/slider"
import { Switch } from "@/components/ui/switch"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Users,
  Plus,
  Filter,
  Code,
  Copy,
  Download,
  Play,
  Loader2,
  TrendingUp,
  TrendingDown,
  ArrowRight,
  RefreshCw,
  AlertTriangle,
  Trash2,
  ChevronDown,
  ChevronRight,
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

type TabType = "whatif" | "cohorts"

type CohortParams = {
  readmitDays: number
  ageThreshold: number
  losThreshold: number
  gender: "all" | "M" | "F"
  icuOnly: boolean
  entryFilter: "all" | "er" | "non_er"
  outcomeFilter: "all" | "survived" | "expired"
}

type CohortMetrics = {
  patientCount: number
  readmitRate: number
  mortalityRate: number
  avgLosDays: number
  medianLosDays: number
  readmit7dRate: number
  longStayRate: number
  icuAdmissionRate: number
  erAdmissionRate: number
}

type ConfidenceMetric = {
  metric: string
  label: string
  unit: string
  current: number
  simulated: number
  difference: number
  ci: [number, number]
  pValue: number
  effectSize: number
  effectSizeType: string
  bootstrapCi: [number, number]
  significant: boolean
}

type ConfidencePayload = {
  method: string
  alpha: number
  bootstrapIterations: number
  nCurrent: number
  nSimulated: number
  metrics: ConfidenceMetric[]
}

type SubgroupMetrics = {
  admissionCount: number
  patientCount: number
  readmissionRate: number
  mortalityRate: number
  avgLosDays: number
}

type SubgroupRow = {
  key: string
  label: string
  current: SubgroupMetrics
  simulated: SubgroupMetrics
  delta: SubgroupMetrics
}

type SubgroupPayload = {
  age: SubgroupRow[]
  gender: SubgroupRow[]
  comorbidity: SubgroupRow[]
}

type SurvivalPoint = {
  time: number
  current: number
  simulated: number
}

type SimulationPayload = {
  params: CohortParams
  baselineParams: CohortParams
  current: CohortMetrics
  simulated: CohortMetrics
  survival: SurvivalPoint[]
  confidence: ConfidencePayload | null
  subgroups: SubgroupPayload
}

type SavedCohort = {
  id: string
  name: string
  createdAt: string
  status: "active" | "archived"
  params: CohortParams
  metrics: CohortMetrics
}

const DEFAULT_PARAMS: CohortParams = {
  readmitDays: 30,
  ageThreshold: 65,
  losThreshold: 7,
  gender: "all",
  icuOnly: false,
  entryFilter: "all",
  outcomeFilter: "all",
}

const EMPTY_METRICS: CohortMetrics = {
  patientCount: 0,
  readmitRate: 0,
  mortalityRate: 0,
  avgLosDays: 0,
  medianLosDays: 0,
  readmit7dRate: 0,
  longStayRate: 0,
  icuAdmissionRate: 0,
  erAdmissionRate: 0,
}

function toApiParams(params: CohortParams) {
  return {
    readmit_days: params.readmitDays,
    age_threshold: params.ageThreshold,
    los_threshold: params.losThreshold,
    gender: params.gender,
    icu_only: params.icuOnly,
    entry_filter: params.entryFilter,
    outcome_filter: params.outcomeFilter,
  }
}

function fromApiParams(value: any): CohortParams {
  const rawGender = String(value?.gender ?? DEFAULT_PARAMS.gender)
  const gender = rawGender === "M" || rawGender === "F" ? rawGender : "all"
  const rawEntryFilter = String(value?.entry_filter ?? DEFAULT_PARAMS.entryFilter)
  const entryFilter = rawEntryFilter === "er" || rawEntryFilter === "non_er" ? rawEntryFilter : "all"
  const rawOutcomeFilter = String(value?.outcome_filter ?? DEFAULT_PARAMS.outcomeFilter)
  const outcomeFilter = rawOutcomeFilter === "survived" || rawOutcomeFilter === "expired" ? rawOutcomeFilter : "all"
  return {
    readmitDays: Number(value?.readmit_days ?? DEFAULT_PARAMS.readmitDays),
    ageThreshold: Number(value?.age_threshold ?? DEFAULT_PARAMS.ageThreshold),
    losThreshold: Number(value?.los_threshold ?? DEFAULT_PARAMS.losThreshold),
    gender,
    icuOnly: Boolean(value?.icu_only ?? DEFAULT_PARAMS.icuOnly),
    entryFilter,
    outcomeFilter,
  }
}

function fromApiMetrics(value: any): CohortMetrics {
  return {
    patientCount: Number(value?.patient_count ?? 0),
    readmitRate: Number(value?.readmission_rate ?? 0),
    mortalityRate: Number(value?.mortality_rate ?? 0),
    avgLosDays: Number(value?.avg_los_days ?? 0),
    medianLosDays: Number(value?.median_los_days ?? 0),
    readmit7dRate: Number(value?.readmission_7d_rate ?? 0),
    longStayRate: Number(value?.long_stay_rate ?? 0),
    icuAdmissionRate: Number(value?.icu_admission_rate ?? 0),
    erAdmissionRate: Number(value?.er_admission_rate ?? 0),
  }
}

function fromApiConfidence(value: any): ConfidencePayload | null {
  if (!value || typeof value !== "object") return null
  const metrics = Array.isArray(value?.metrics)
    ? value.metrics.map((item: any) => ({
        metric: String(item?.metric ?? ""),
        label: String(item?.label ?? ""),
        unit: String(item?.unit ?? ""),
        current: Number(item?.current ?? 0),
        simulated: Number(item?.simulated ?? 0),
        difference: Number(item?.difference ?? 0),
        ci: [Number(item?.ci?.[0] ?? 0), Number(item?.ci?.[1] ?? 0)] as [number, number],
        pValue: Number(item?.p_value ?? 1),
        effectSize: Number(item?.effect_size ?? 0),
        effectSizeType: String(item?.effect_size_type ?? ""),
        bootstrapCi: [Number(item?.bootstrap_ci?.[0] ?? 0), Number(item?.bootstrap_ci?.[1] ?? 0)] as [number, number],
        significant: Boolean(item?.significant),
      }))
    : []
  return {
    method: String(value?.method ?? ""),
    alpha: Number(value?.alpha ?? 0.05),
    bootstrapIterations: Number(value?.bootstrap_iterations ?? 0),
    nCurrent: Number(value?.n_current ?? 0),
    nSimulated: Number(value?.n_simulated ?? 0),
    metrics,
  }
}

function fromApiSubgroupMetrics(value: any): SubgroupMetrics {
  return {
    admissionCount: Number(value?.admission_count ?? 0),
    patientCount: Number(value?.patient_count ?? 0),
    readmissionRate: Number(value?.readmission_rate ?? 0),
    mortalityRate: Number(value?.mortality_rate ?? 0),
    avgLosDays: Number(value?.avg_los_days ?? 0),
  }
}

function fromApiSubgroupRows(value: any): SubgroupRow[] {
  if (!Array.isArray(value)) return []
  return value.map((item: any) => ({
    key: String(item?.key ?? ""),
    label: String(item?.label ?? ""),
    current: fromApiSubgroupMetrics(item?.current),
    simulated: fromApiSubgroupMetrics(item?.simulated),
    delta: fromApiSubgroupMetrics(item?.delta),
  }))
}

function fromApiSubgroups(value: any): SubgroupPayload {
  return {
    age: fromApiSubgroupRows(value?.age),
    gender: fromApiSubgroupRows(value?.gender),
    comorbidity: fromApiSubgroupRows(value?.comorbidity),
  }
}

function toViewSavedCohort(item: any): SavedCohort {
  return {
    id: String(item?.id || ""),
    name: String(item?.name || "이름 없는 코호트"),
    createdAt: String(item?.created_at || ""),
    status: item?.status === "archived" ? "archived" : "active",
    params: fromApiParams(item?.params),
    metrics: fromApiMetrics(item?.metrics),
  }
}

function formatMetricValue(value: number, unit: string) {
  if (unit === "명") {
    return Math.round(value).toLocaleString()
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 1 })
}

function formatDiff(current: number, simulated: number, unit: string) {
  const diff = simulated - current
  if (unit === "명") {
    return `${diff > 0 ? "+" : ""}${Math.round(diff).toLocaleString()}`
  }
  return `${diff > 0 ? "+" : ""}${diff.toFixed(1)}`
}

function buildCohortName(params: CohortParams) {
  const genderLabel = params.gender === "all" ? "전체" : params.gender
  const icuLabel = params.icuOnly ? "ICU 포함" : "전체 입실"
  const entryLabel =
    params.entryFilter === "all" ? "입원경로 전체" : params.entryFilter === "er" ? "응급실 유입" : "비응급 유입"
  const outcomeLabel =
    params.outcomeFilter === "all"
      ? "퇴원결과 전체"
      : params.outcomeFilter === "survived"
        ? "생존 퇴원"
        : "원내사망"
  return `${params.ageThreshold}세 이상 / 재입원 ${params.readmitDays}일 / LOS ${params.losThreshold}일 / ${genderLabel} / ${icuLabel} / ${entryLabel} / ${outcomeLabel}`
}

export function CohortView() {
  const [activeTab, setActiveTab] = useState<TabType>("whatif")
  const [isLoading, setIsLoading] = useState(false)
  const [isSaving, setIsSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [message, setMessage] = useState<string | null>(null)

  const [readmitDays, setReadmitDays] = useState([DEFAULT_PARAMS.readmitDays])
  const [ageThreshold, setAgeThreshold] = useState([DEFAULT_PARAMS.ageThreshold])
  const [losThreshold, setLosThreshold] = useState([DEFAULT_PARAMS.losThreshold])
  const [genderFilter, setGenderFilter] = useState<CohortParams["gender"]>(DEFAULT_PARAMS.gender)
  const [icuOnly, setIcuOnly] = useState(DEFAULT_PARAMS.icuOnly)
  const [entryFilter, setEntryFilter] = useState<CohortParams["entryFilter"]>(DEFAULT_PARAMS.entryFilter)
  const [outcomeFilter, setOutcomeFilter] = useState<CohortParams["outcomeFilter"]>(DEFAULT_PARAMS.outcomeFilter)

  const [simulation, setSimulation] = useState<SimulationPayload | null>(null)
  const [savedCohorts, setSavedCohorts] = useState<SavedCohort[]>([])
  const [isSaveDialogOpen, setIsSaveDialogOpen] = useState(false)
  const [cohortNameInput, setCohortNameInput] = useState("")
  const [isSqlDialogOpen, setIsSqlDialogOpen] = useState(false)
  const [isSqlLoading, setIsSqlLoading] = useState(false)
  const [cohortSqlText, setCohortSqlText] = useState("")
  const [isConditionSummaryOpen, setIsConditionSummaryOpen] = useState(true)

  const currentParams = useMemo<CohortParams>(
    () => ({
      readmitDays: readmitDays[0],
      ageThreshold: ageThreshold[0],
      losThreshold: losThreshold[0],
      gender: genderFilter,
      icuOnly,
      entryFilter,
      outcomeFilter,
    }),
    [readmitDays, ageThreshold, losThreshold, genderFilter, icuOnly, entryFilter, outcomeFilter]
  )

  const runSimulation = useCallback(async (params: CohortParams) => {
    setIsLoading(true)
    setError(null)
    setMessage(null)
    try {
      const res = await fetch("/cohort/simulate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          params: toApiParams(params),
          include_baseline: true,
        }),
      })
      if (!res.ok) {
        const detail = await res.text()
        throw new Error(detail || "시뮬레이션 실행에 실패했습니다.")
      }
      const payload = await res.json()
      setSimulation({
        params: fromApiParams(payload?.params),
        baselineParams: fromApiParams(payload?.baseline_params),
        current: fromApiMetrics(payload?.current),
        simulated: fromApiMetrics(payload?.simulated),
        survival: Array.isArray(payload?.survival)
          ? payload.survival.map((point: any) => ({
              time: Number(point?.time ?? 0),
              current: Number(point?.current ?? 0),
              simulated: Number(point?.simulated ?? 0),
            }))
          : [],
        confidence: fromApiConfidence(payload?.confidence),
        subgroups: fromApiSubgroups(payload?.subgroups),
      })
    } catch (err) {
      setError("시뮬레이션 실행 중 오류가 발생했습니다.")
    } finally {
      setIsLoading(false)
    }
  }, [])

  const loadSavedCohorts = useCallback(async () => {
    try {
      const res = await fetch("/cohort/saved")
      if (!res.ok) {
        return
      }
      const payload = await res.json()
      const items = Array.isArray(payload?.cohorts) ? payload.cohorts : []
      setSavedCohorts(items.map(toViewSavedCohort))
    } catch {
      // Keep UI usable even when saved cohort loading fails.
    }
  }, [])

  useEffect(() => {
    void runSimulation(DEFAULT_PARAMS)
    void loadSavedCohorts()
  }, [runSimulation, loadSavedCohorts])

  const handleRunSimulation = async () => {
    await runSimulation(currentParams)
  }

  const handleReset = async () => {
    setReadmitDays([DEFAULT_PARAMS.readmitDays])
    setAgeThreshold([DEFAULT_PARAMS.ageThreshold])
    setLosThreshold([DEFAULT_PARAMS.losThreshold])
    setGenderFilter(DEFAULT_PARAMS.gender)
    setIcuOnly(DEFAULT_PARAMS.icuOnly)
    setEntryFilter(DEFAULT_PARAMS.entryFilter)
    setOutcomeFilter(DEFAULT_PARAMS.outcomeFilter)
    await runSimulation(DEFAULT_PARAMS)
  }

  const handleOpenSaveDialog = () => {
    setCohortNameInput(buildCohortName(currentParams))
    setIsSaveDialogOpen(true)
  }

  const handleShowCohortSql = async () => {
    setIsSqlLoading(true)
    setError(null)
    try {
      const res = await fetch("/cohort/sql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          params: toApiParams(currentParams),
        }),
      })
      if (!res.ok) {
        const detail = await res.text()
        throw new Error(detail || "코호트 SQL 조회에 실패했습니다.")
      }
      const payload = await res.json()
      const sql = payload?.sql || {}
      const sections = [
        ["코호트 CTE", String(sql?.cohort_cte || "")],
        ["통합 지표 집계", String(sql?.metrics_sql || "")],
        ["대상 환자 수", String(sql?.patient_count_sql || "")],
        ["재입원율", String(sql?.readmission_rate_sql || "")],
        ["사망률", String(sql?.mortality_rate_sql || "")],
        ["평균 재원일수", String(sql?.avg_los_sql || "")],
        ["중앙 재원일수", String(sql?.median_los_sql || "")],
        ["7일 재입원율", String(sql?.readmission_7d_rate_sql || "")],
        ["장기재원 비율", String(sql?.long_stay_rate_sql || "")],
        ["ICU 입실 비율", String(sql?.icu_admission_rate_sql || "")],
        ["응급실 입원 비율", String(sql?.er_admission_rate_sql || "")],
        ["생존분석 라이프테이블", String(sql?.life_table_sql || "")],
      ].filter(([, query]) => query.trim().length > 0)
      const combined = sections.map(([title, query]) => `-- ${title}\n${query.trim()}`).join("\n\n")
      setCohortSqlText(combined || "표시할 SQL이 없습니다.")
      setIsSqlDialogOpen(true)
    } catch {
      setError("코호트 SQL 조회 중 오류가 발생했습니다.")
    } finally {
      setIsSqlLoading(false)
    }
  }

  const handleCopyCohortSql = async () => {
    const text = cohortSqlText.trim()
    if (!text) return
    try {
      await navigator.clipboard.writeText(text)
      setMessage("코호트 SQL을 복사했습니다.")
      setError(null)
    } catch {
      setError("클립보드 복사에 실패했습니다.")
    }
  }

  const handleSaveCohort = async () => {
    const name = cohortNameInput.trim()
    if (!name) return
    setIsSaving(true)
    setError(null)
    setMessage(null)
    try {
      const res = await fetch("/cohort/saved", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          params: toApiParams(currentParams),
          status: "active",
        }),
      })
      if (!res.ok) {
        const detail = await res.text()
        throw new Error(detail || "코호트 저장에 실패했습니다.")
      }
      await loadSavedCohorts()
      setMessage("코호트를 저장했습니다.")
      setIsSaveDialogOpen(false)
      setActiveTab("cohorts")
    } catch {
      setError("코호트 저장 중 오류가 발생했습니다.")
    } finally {
      setIsSaving(false)
    }
  }

  const handleDeleteCohort = async (cohortId: string) => {
    setError(null)
    setMessage(null)
    try {
      const res = await fetch(`/cohort/saved/${cohortId}`, {
        method: "DELETE",
      })
      if (!res.ok) {
        const detail = await res.text()
        throw new Error(detail || "코호트 삭제에 실패했습니다.")
      }
      await loadSavedCohorts()
      setMessage("코호트를 삭제했습니다.")
    } catch {
      setError("코호트 삭제 중 오류가 발생했습니다.")
    }
  }

  const handleAnalyzeSavedCohort = async (cohort: SavedCohort) => {
    setReadmitDays([cohort.params.readmitDays])
    setAgeThreshold([cohort.params.ageThreshold])
    setLosThreshold([cohort.params.losThreshold])
    setGenderFilter(cohort.params.gender)
    setIcuOnly(cohort.params.icuOnly)
    setEntryFilter(cohort.params.entryFilter)
    setOutcomeFilter(cohort.params.outcomeFilter)
    setActiveTab("whatif")
    await runSimulation(cohort.params)
  }

  const handleExportSavedCohort = (cohort: SavedCohort) => {
    const payload = JSON.stringify(cohort, null, 2)
    const blob = new Blob([payload], { type: "application/json;charset=utf-8" })
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement("a")
    anchor.href = url
    anchor.download = `${cohort.name.replace(/\s+/g, "_")}.json`
    document.body.appendChild(anchor)
    anchor.click()
    anchor.remove()
    URL.revokeObjectURL(url)
  }

  const currentMetrics = simulation?.current ?? EMPTY_METRICS
  const simulatedMetrics = simulation?.simulated ?? EMPTY_METRICS
  const survivalData = simulation?.survival ?? []
  const confidence = simulation?.confidence
  const subgroupData: SubgroupPayload = simulation?.subgroups ?? { age: [], gender: [], comorbidity: [] }

  const metricCards = [
    {
      label: "대상 환자 수",
      current: currentMetrics.patientCount,
      simulated: simulatedMetrics.patientCount,
      unit: "명",
      inverse: false,
    },
    {
      label: "재입원율",
      current: currentMetrics.readmitRate,
      simulated: simulatedMetrics.readmitRate,
      unit: "%",
      inverse: true,
    },
    {
      label: "사망률",
      current: currentMetrics.mortalityRate,
      simulated: simulatedMetrics.mortalityRate,
      unit: "%",
      inverse: true,
    },
    {
      label: "평균 재원일수",
      current: currentMetrics.avgLosDays,
      simulated: simulatedMetrics.avgLosDays,
      unit: "일",
      inverse: true,
    },
    {
      label: "ICU 입실 비율",
      current: currentMetrics.icuAdmissionRate,
      simulated: simulatedMetrics.icuAdmissionRate,
      unit: "%",
      inverse: false,
    },
    {
      label: "응급실 입원 비율",
      current: currentMetrics.erAdmissionRate,
      simulated: simulatedMetrics.erAdmissionRate,
      unit: "%",
      inverse: false,
    },
  ]

  const readmitDiff = simulatedMetrics.readmitRate - currentMetrics.readmitRate
  const mortalityDiff = simulatedMetrics.mortalityRate - currentMetrics.mortalityRate
  const losDiff = simulatedMetrics.avgLosDays - currentMetrics.avgLosDays
  const icuAdmissionDiff = simulatedMetrics.icuAdmissionRate - currentMetrics.icuAdmissionRate
  const erAdmissionDiff = simulatedMetrics.erAdmissionRate - currentMetrics.erAdmissionRate

  const getChangeIndicator = (current: number, simulated: number, unit: string, inverse = false) => {
    const diff = simulated - current
    const isPositive = inverse ? diff < 0 : diff > 0
    const icon = isPositive ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />
    const color = isPositive ? "text-primary" : "text-destructive"
    return (
      <span className={cn("flex items-center gap-1 text-xs", color)}>
        {icon}
        {formatDiff(current, simulated, unit)}
      </span>
    )
  }

  const conditionSummary = [
    { label: "재입원", value: `${readmitDays[0]}일` },
    { label: "연령", value: `${ageThreshold[0]}세 이상` },
    { label: "LOS", value: `${losThreshold[0]}일 이상` },
    { label: "성별", value: genderFilter === "all" ? "전체" : genderFilter },
    { label: "ICU 제한", value: icuOnly ? "ON" : "OFF" },
    { label: "입원 경로", value: entryFilter === "all" ? "전체" : entryFilter === "er" ? "응급실" : "비응급" },
    { label: "퇴원 결과", value: outcomeFilter === "all" ? "전체" : outcomeFilter === "survived" ? "생존" : "사망" },
  ]

  const conditionSqlPreview = [
    `ANCHOR_AGE >= ${ageThreshold[0]}`,
    `LOS >= ${losThreshold[0]}`,
    genderFilter === "all" ? null : `GENDER = '${genderFilter}'`,
    icuOnly ? "EXISTS(ICUSTAYS)" : null,
    entryFilter === "all" ? null : entryFilter === "er" ? "ADMISSION_LOCATION in ER/ED" : "ADMISSION_LOCATION not ER/ED",
    outcomeFilter === "all" ? null : outcomeFilter === "survived" ? "HOSPITAL_EXPIRE_FLAG = 0" : "HOSPITAL_EXPIRE_FLAG = 1",
  ]
    .filter(Boolean)
    .join(" AND ")

  const formatPValue = (value: number) => {
    if (!Number.isFinite(value)) return "-"
    if (value < 0.001) return "< 0.001"
    return value.toFixed(3)
  }

  const formatSigned = (value: number, unit: "count" | "pct" | "days") => {
    const prefix = value > 0 ? "+" : ""
    if (unit === "count") return `${prefix}${Math.round(value).toLocaleString()}`
    if (unit === "days") return `${prefix}${value.toFixed(2)}`
    return `${prefix}${value.toFixed(2)}`
  }

  const subgroupSections: Array<{ key: keyof SubgroupPayload; title: string; rows: SubgroupRow[] }> = [
    { key: "age", title: "나이대", rows: subgroupData.age },
    { key: "gender", title: "성별", rows: subgroupData.gender },
    { key: "comorbidity", title: "기저질환", rows: subgroupData.comorbidity },
  ]

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-foreground">코호트 생성 & What-if 분석</h2>
          <p className="text-sm text-muted-foreground mt-1">가상 코호트를 생성하고 조건 변경에 따른 결과를 시뮬레이션합니다</p>
          <Badge variant="secondary" className="mt-2">부가 기능</Badge>
        </div>
        <Button className="gap-2 w-full sm:w-auto" onClick={handleOpenSaveDialog} disabled={isSaving || isLoading}>
          {isSaving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
          코호트 저장
        </Button>
      </div>

      {error && <div className="text-sm text-destructive">{error}</div>}
      {message && <div className="text-sm text-emerald-600">{message}</div>}

      <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as TabType)} className="space-y-4">
        <TabsList className="w-full sm:w-auto">
          <TabsTrigger value="whatif" className="text-xs sm:text-sm">What-if 시뮬레이션</TabsTrigger>
          <TabsTrigger value="cohorts" className="text-xs sm:text-sm">저장된 코호트</TabsTrigger>
        </TabsList>

        <TabsContent value="whatif" className="space-y-4 sm:space-y-6">
          <div className="grid lg:grid-cols-3 gap-4 sm:gap-6">
            <Card className="lg:col-span-1">
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <Filter className="w-5 h-5" />
                  시뮬레이션 조건
                </CardTitle>
                <CardDescription>조건을 변경하여 지표 변화를 확인하세요</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
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
                    <span>현재: {readmitDays[0]}일</span>
                    <span>90일</span>
                  </div>
                </div>

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
                    <span>현재: {ageThreshold[0]}세</span>
                    <span>85세</span>
                  </div>
                </div>

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
                    <span>현재: {losThreshold[0]}일</span>
                    <span>30일</span>
                  </div>
                </div>

                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-sm">성별 필터</Label>
                    <Badge variant="outline">{genderFilter === "all" ? "전체" : genderFilter}</Badge>
                  </div>
                  <div className="grid grid-cols-3 gap-2">
                    {(["all", "M", "F"] as const).map((value) => (
                      <Button
                        key={value}
                        type="button"
                        variant={genderFilter === value ? "default" : "outline"}
                        className="h-8 text-xs"
                        onClick={() => setGenderFilter(value)}
                      >
                        {value === "all" ? "전체" : value}
                      </Button>
                    ))}
                  </div>
                </div>

                <div className="flex items-center justify-between rounded-lg border border-border p-3">
                  <div className="space-y-1">
                    <Label className="text-sm">ICU 입실 환자만</Label>
                    <p className="text-[11px] text-muted-foreground">켜면 ICU 입실 환자 코호트만 대상으로 계산합니다.</p>
                  </div>
                  <Switch checked={icuOnly} onCheckedChange={setIcuOnly} />
                </div>

                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-sm">입원 경로</Label>
                    <Badge variant="outline">
                      {entryFilter === "all" ? "전체" : entryFilter === "er" ? "응급실 유입" : "비응급 유입"}
                    </Badge>
                  </div>
                  <div className="grid grid-cols-3 gap-2">
                    <Button
                      type="button"
                      variant={entryFilter === "all" ? "default" : "outline"}
                      className="h-8 text-xs"
                      onClick={() => setEntryFilter("all")}
                    >
                      전체
                    </Button>
                    <Button
                      type="button"
                      variant={entryFilter === "er" ? "default" : "outline"}
                      className="h-8 text-xs"
                      onClick={() => setEntryFilter("er")}
                    >
                      응급실 유입
                    </Button>
                    <Button
                      type="button"
                      variant={entryFilter === "non_er" ? "default" : "outline"}
                      className="h-8 text-xs"
                      onClick={() => setEntryFilter("non_er")}
                    >
                      비응급 유입
                    </Button>
                  </div>
                </div>

                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <Label className="text-sm">퇴원 결과</Label>
                    <Badge variant="outline">
                      {outcomeFilter === "all" ? "전체" : outcomeFilter === "survived" ? "생존 퇴원" : "원내사망"}
                    </Badge>
                  </div>
                  <div className="grid grid-cols-3 gap-2">
                    <Button
                      type="button"
                      variant={outcomeFilter === "all" ? "default" : "outline"}
                      className="h-8 text-xs"
                      onClick={() => setOutcomeFilter("all")}
                    >
                      전체
                    </Button>
                    <Button
                      type="button"
                      variant={outcomeFilter === "survived" ? "default" : "outline"}
                      className="h-8 text-xs"
                      onClick={() => setOutcomeFilter("survived")}
                    >
                      생존 퇴원
                    </Button>
                    <Button
                      type="button"
                      variant={outcomeFilter === "expired" ? "default" : "outline"}
                      className="h-8 text-xs"
                      onClick={() => setOutcomeFilter("expired")}
                    >
                      원내사망
                    </Button>
                  </div>
                </div>

                <div className="rounded-lg border border-border bg-secondary/20 p-3 space-y-3">
                  <button
                    type="button"
                    className="w-full flex items-center justify-between"
                    onClick={() => setIsConditionSummaryOpen(prev => !prev)}
                  >
                    <div className="flex items-center gap-2">
                      <Label className="text-sm font-medium cursor-pointer">조건 요약</Label>
                      <Badge variant="secondary" className="text-[10px]">현재 적용</Badge>
                    </div>
                    {isConditionSummaryOpen ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                  </button>
                  {isConditionSummaryOpen ? (
                    <div className="space-y-3">
                      <div className="grid grid-cols-2 gap-2">
                        {conditionSummary.map((item) => (
                          <div key={item.label} className="rounded-md border border-border/70 bg-background/80 px-2 py-1">
                            <div className="text-[10px] text-muted-foreground">{item.label}</div>
                            <div className="text-xs font-medium text-foreground">{item.value}</div>
                          </div>
                        ))}
                      </div>
                      <div className="rounded-md border border-border/70 bg-background/80 px-2 py-1.5">
                        <div className="text-[10px] text-muted-foreground mb-1">SQL 필터 요약</div>
                        <div className="text-[11px] text-foreground break-all leading-relaxed">{conditionSqlPreview}</div>
                      </div>
                    </div>
                  ) : null}
                </div>

                <Button onClick={handleRunSimulation} disabled={isLoading} className="w-full gap-2">
                  {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                  시뮬레이션 실행
                </Button>

                <Button
                  variant="outline"
                  className="w-full gap-2 bg-transparent"
                  onClick={() => void handleShowCohortSql()}
                  disabled={isSqlLoading || isLoading}
                >
                  {isSqlLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Code className="w-4 h-4" />}
                  코호트 SQL 보기
                </Button>

                <Button variant="outline" className="w-full gap-2 bg-transparent" onClick={handleReset} disabled={isLoading}>
                  <RefreshCw className="w-4 h-4" />
                  조건 초기화
                </Button>
              </CardContent>
            </Card>

            <Card className="lg:col-span-2">
              <CardHeader>
                <CardTitle className="text-lg">시뮬레이션 결과</CardTitle>
                <CardDescription>현재 기준 대비 예상 변화량</CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                  {metricCards.map((metric) => (
                    <div key={metric.label} className="p-4 rounded-lg bg-secondary/30 border border-border">
                      <div className="text-xs text-muted-foreground mb-2">{metric.label}</div>
                      <div className="flex items-end gap-2">
                        <span className="text-lg font-bold text-foreground">
                          {formatMetricValue(metric.simulated, metric.unit)}
                        </span>
                        <span className="text-xs text-muted-foreground">{metric.unit}</span>
                      </div>
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-xs text-muted-foreground">
                          현재: {formatMetricValue(metric.current, metric.unit)}
                          {metric.unit}
                        </span>
                        {getChangeIndicator(metric.current, metric.simulated, metric.unit, metric.inverse)}
                      </div>
                    </div>
                  ))}
                </div>

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
                          label={{ value: "시간 (일)", position: "bottom", offset: -5, fontSize: 10, fill: "#64748b" }}
                        />
                        <YAxis
                          stroke="#64748b"
                          tick={{ fontSize: 10 }}
                          domain={[0, 100]}
                          label={{ value: "생존율 (%)", angle: -90, position: "insideLeft", fontSize: 10, fill: "#64748b" }}
                        />
                        <Tooltip
                          contentStyle={{ backgroundColor: "#1e293b", border: "1px solid #334155", borderRadius: "8px" }}
                          labelStyle={{ color: "#94a3b8" }}
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

                <div className="rounded-lg border border-border bg-secondary/20 p-4 space-y-3">
                  <div className="flex items-center justify-between gap-2">
                    <h4 className="text-sm font-medium text-foreground">통계 신뢰도</h4>
                    <Badge variant="outline" className="text-[10px]">
                      {confidence ? `n=${confidence.nSimulated.toLocaleString()}` : "데이터 없음"}
                    </Badge>
                  </div>
                  {confidence ? (
                    <div className="space-y-2">
                      <div className="text-[11px] text-muted-foreground">
                        {confidence.method} / alpha={confidence.alpha} / bootstrap={confidence.bootstrapIterations}
                      </div>
                      <div className="overflow-x-auto rounded-md border border-border/70 bg-background/80">
                        <table className="w-full text-xs">
                          <thead className="border-b border-border/70 bg-secondary/40">
                            <tr>
                              <th className="text-left px-2 py-2">지표</th>
                              <th className="text-right px-2 py-2">차이(시뮬-현재)</th>
                              <th className="text-right px-2 py-2">95% CI</th>
                              <th className="text-right px-2 py-2">p-value</th>
                              <th className="text-right px-2 py-2">효과크기</th>
                              <th className="text-right px-2 py-2">Bootstrap CI</th>
                            </tr>
                          </thead>
                          <tbody>
                            {confidence.metrics.map((item) => (
                              <tr key={item.metric} className="border-b border-border/40">
                                <td className="px-2 py-1.5">
                                  <div className="flex items-center gap-2">
                                    <span>{item.label}</span>
                                    <Badge variant={item.significant ? "default" : "secondary"} className="text-[10px]">
                                      {item.significant ? "유의" : "비유의"}
                                    </Badge>
                                  </div>
                                </td>
                                <td className="text-right px-2 py-1.5">
                                  {item.difference.toFixed(2)} {item.unit === "days" ? "일" : "%p"}
                                </td>
                                <td className="text-right px-2 py-1.5">
                                  [{item.ci[0].toFixed(2)}, {item.ci[1].toFixed(2)}]
                                </td>
                                <td className="text-right px-2 py-1.5">{formatPValue(item.pValue)}</td>
                                <td className="text-right px-2 py-1.5">
                                  {item.effectSize.toFixed(3)} ({item.effectSizeType})
                                </td>
                                <td className="text-right px-2 py-1.5">
                                  [{item.bootstrapCi[0].toFixed(2)}, {item.bootstrapCi[1].toFixed(2)}]
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ) : (
                    <div className="text-xs text-muted-foreground">신뢰도 계산 결과가 없습니다.</div>
                  )}
                </div>

                <div className="rounded-lg border border-border bg-secondary/20 p-4 space-y-4">
                  <div className="flex items-center justify-between gap-2">
                    <h4 className="text-sm font-medium text-foreground">서브그룹/층화 분석</h4>
                    <Badge variant="outline" className="text-[10px]">나이대 · 성별 · 기저질환</Badge>
                  </div>
                  <p className="text-[11px] text-muted-foreground">
                    동일 조건에서 현재 기준 대비 시뮬레이션의 하위집단별 변화를 한 번에 비교합니다.
                  </p>
                  <div className="space-y-3">
                    {subgroupSections.map((section) => (
                      <div key={section.key} className="rounded-md border border-border/70 bg-background/80 p-2 space-y-2">
                        <div className="text-xs font-medium text-foreground">{section.title}</div>
                        {section.rows.length === 0 ? (
                          <div className="text-[11px] text-muted-foreground px-1 py-2">표시할 데이터가 없습니다.</div>
                        ) : (
                          <div className="overflow-x-auto">
                            <table className="w-full min-w-[900px] text-xs">
                              <thead className="border-b border-border/60">
                                <tr className="text-muted-foreground">
                                  <th className="text-left px-2 py-1.5">그룹</th>
                                  <th className="text-right px-2 py-1.5">현재 환자수</th>
                                  <th className="text-right px-2 py-1.5">시뮬 환자수</th>
                                  <th className="text-right px-2 py-1.5">Δ 환자수</th>
                                  <th className="text-right px-2 py-1.5">현재 재입원율</th>
                                  <th className="text-right px-2 py-1.5">시뮬 재입원율</th>
                                  <th className="text-right px-2 py-1.5">Δ 재입원율</th>
                                  <th className="text-right px-2 py-1.5">현재 사망률</th>
                                  <th className="text-right px-2 py-1.5">시뮬 사망률</th>
                                  <th className="text-right px-2 py-1.5">Δ 사망률</th>
                                  <th className="text-right px-2 py-1.5">Δ 평균LOS</th>
                                </tr>
                              </thead>
                              <tbody>
                                {section.rows.map((row) => (
                                  <tr key={`${section.key}-${row.key}`} className="border-b border-border/30">
                                    <td className="px-2 py-1.5 font-medium text-foreground">{row.label}</td>
                                    <td className="text-right px-2 py-1.5">{Math.round(row.current.patientCount).toLocaleString()}</td>
                                    <td className="text-right px-2 py-1.5">{Math.round(row.simulated.patientCount).toLocaleString()}</td>
                                    <td className="text-right px-2 py-1.5">{formatSigned(row.delta.patientCount, "count")}</td>
                                    <td className="text-right px-2 py-1.5">{row.current.readmissionRate.toFixed(2)}%</td>
                                    <td className="text-right px-2 py-1.5">{row.simulated.readmissionRate.toFixed(2)}%</td>
                                    <td className="text-right px-2 py-1.5">{formatSigned(row.delta.readmissionRate, "pct")}%p</td>
                                    <td className="text-right px-2 py-1.5">{row.current.mortalityRate.toFixed(2)}%</td>
                                    <td className="text-right px-2 py-1.5">{row.simulated.mortalityRate.toFixed(2)}%</td>
                                    <td className="text-right px-2 py-1.5">{formatSigned(row.delta.mortalityRate, "pct")}%p</td>
                                    <td className="text-right px-2 py-1.5">{formatSigned(row.delta.avgLosDays, "days")}일</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>

                <div className="p-4 rounded-lg bg-primary/10 border border-primary/30">
                  <h4 className="text-sm font-medium text-foreground mb-2 flex items-center gap-2">
                    <AlertTriangle className="w-4 h-4 text-primary" />
                    분석 인사이트
                  </h4>
                  <p className="text-sm text-muted-foreground">
                    재입원율 {readmitDiff >= 0 ? "증가" : "감소"} {Math.abs(readmitDiff).toFixed(1)}%p, 사망률{" "}
                    {mortalityDiff >= 0 ? "증가" : "감소"} {Math.abs(mortalityDiff).toFixed(1)}%p, 평균 재원일수{" "}
                    {losDiff >= 0 ? "증가" : "감소"} {Math.abs(losDiff).toFixed(1)}일, ICU 입실 비율{" "}
                    {icuAdmissionDiff >= 0 ? "증가" : "감소"} {Math.abs(icuAdmissionDiff).toFixed(1)}%p, 응급실 입원 비율{" "}
                    {erAdmissionDiff >= 0 ? "증가" : "감소"} {Math.abs(erAdmissionDiff).toFixed(1)}%p로 추정됩니다.
                    임계값 변경 시 분모가 달라질 수 있으므로 하위 그룹 분석을 함께 확인하세요.
                  </p>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="cohorts" className="space-y-4">
          {savedCohorts.length === 0 ? (
            <Card>
              <CardContent className="py-8 text-sm text-muted-foreground">
                저장된 코호트가 없습니다. `코호트 저장`으로 현재 조건을 저장하세요.
              </CardContent>
            </Card>
          ) : (
            <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
              {savedCohorts.map((cohort) => (
                <Card key={cohort.id} className="hover:border-primary/30 transition-colors">
                  <CardHeader className="pb-2">
                    <div className="flex items-center justify-between gap-2">
                      <CardTitle className="text-sm">{cohort.name}</CardTitle>
                      <Badge variant={cohort.status === "active" ? "default" : "secondary"} className="text-[10px]">
                        {cohort.status === "active" ? "활성" : "보관"}
                      </Badge>
                    </div>
                    <CardDescription className="text-xs">생성일: {cohort.createdAt || "-"}</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Users className="w-4 h-4 text-muted-foreground" />
                        <span className="text-lg font-bold text-foreground">
                          {Math.round(cohort.metrics.patientCount).toLocaleString()}
                        </span>
                        <span className="text-xs text-muted-foreground">명</span>
                      </div>
                    </div>
                    <div className="text-xs text-muted-foreground space-y-1">
                      <div>재입원율: {cohort.metrics.readmitRate.toFixed(1)}%</div>
                      <div>사망률: {cohort.metrics.mortalityRate.toFixed(1)}%</div>
                      <div>평균 재원일수: {cohort.metrics.avgLosDays.toFixed(1)}일</div>
                      <div>ICU 입실 비율: {cohort.metrics.icuAdmissionRate.toFixed(1)}%</div>
                      <div>응급실 입원 비율: {cohort.metrics.erAdmissionRate.toFixed(1)}%</div>
                    </div>
                    <div className="flex items-center gap-1 pt-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 gap-1 text-xs"
                        onClick={() => handleExportSavedCohort(cohort)}
                      >
                        <Download className="w-3 h-3" />
                        내보내기
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 gap-1 text-xs"
                        onClick={() => void handleAnalyzeSavedCohort(cohort)}
                      >
                        <ArrowRight className="w-3 h-3" />
                        분석
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 gap-1 text-xs text-destructive"
                        onClick={() => void handleDeleteCohort(cohort.id)}
                      >
                        <Trash2 className="w-3 h-3" />
                        삭제
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
        </TabsContent>
      </Tabs>

      <Dialog open={isSaveDialogOpen} onOpenChange={setIsSaveDialogOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>코호트 이름</DialogTitle>
            <DialogDescription>저장할 코호트 이름을 입력하세요.</DialogDescription>
          </DialogHeader>
          <Input
            value={cohortNameInput}
            placeholder="코호트 이름"
            onChange={(e) => setCohortNameInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && cohortNameInput.trim() && !isSaving) {
                void handleSaveCohort()
              }
            }}
            autoFocus
          />
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsSaveDialogOpen(false)} disabled={isSaving}>
              취소
            </Button>
            <Button onClick={() => void handleSaveCohort()} disabled={isSaving || !cohortNameInput.trim()}>
              {isSaving ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : null}
              확인
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isSqlDialogOpen} onOpenChange={setIsSqlDialogOpen}>
        <DialogContent className="sm:max-w-4xl">
          <DialogHeader>
            <DialogTitle>코호트 생성 SQL</DialogTitle>
            <DialogDescription>현재 시뮬레이션 조건으로 생성되는 SQL입니다.</DialogDescription>
          </DialogHeader>
          <div className="max-h-[65vh] overflow-auto rounded-md border border-border bg-muted/40 p-3">
            <pre className="text-xs leading-relaxed whitespace-pre-wrap break-all font-mono text-foreground">
              {cohortSqlText}
            </pre>
          </div>
          <DialogFooter>
            <Button variant="outline" className="gap-2" onClick={() => void handleCopyCohortSql()}>
              <Copy className="w-4 h-4" />
              복사
            </Button>
            <Button variant="outline" onClick={() => setIsSqlDialogOpen(false)}>
              닫기
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
