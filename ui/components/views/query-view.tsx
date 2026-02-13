"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import dynamic from "next/dynamic"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
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
  Sparkles,
  Shield,
  Table2,
  FileText,
  RefreshCw,
  Copy,
  Download,
  Trash2,
  BookmarkPlus
} from "lucide-react"
import { cn } from "@/lib/utils"
import { SurvivalChart } from "@/components/survival-chart"
import { useAuth } from "@/components/auth-provider"
import {
  ResponsiveContainer,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ComposedChart,
  BarChart,
  Bar,
  LineChart,
  Line,
  ScatterChart,
  Scatter,
} from "recharts"

interface ChatMessage {
  id: string
  role: "user" | "assistant"
  content: string
  timestamp: Date
}

interface PersistedChatMessage {
  id: string
  role: "user" | "assistant"
  content: string
  timestamp: string
}

interface PreviewData {
  columns: string[]
  rows: any[][]
  row_count: number
  row_cap: number
}

interface DemoResult {
  sql?: string
  preview?: PreviewData
  summary?: string
  source?: string
}

interface PolicyCheck {
  name: string
  passed: boolean
  message: string
}

interface PolicyResult {
  passed?: boolean
  checks?: PolicyCheck[]
}

interface OneShotPayload {
  mode: "demo" | "advanced" | "clarify"
  question: string
  result?: DemoResult
  risk?: { risk?: number; intent?: string }
  policy?: PolicyResult | null
  draft?: { final_sql?: string }
  final?: { final_sql?: string; risk_score?: number; used_tables?: string[] }
  clarification?: {
    reason?: string
    question?: string
    options?: string[]
    example_inputs?: string[]
  }
}

interface OneShotResponse {
  qid: string
  payload: OneShotPayload
}

interface RunResponse {
  sql: string
  result: PreviewData
  policy?: PolicyResult | null
}

interface VisualizationChartSpec {
  chart_type?: string
  x?: string
  y?: string
  group?: string
  agg?: string
}

interface VisualizationAnalysisCard {
  chart_spec?: VisualizationChartSpec
  reason?: string
  summary?: string
  figure_json?: Record<string, unknown>
  code?: string
}

interface VisualizationResponsePayload {
  sql?: string
  table_preview?: Array<Record<string, unknown>>
  analyses?: VisualizationAnalysisCard[]
  insight?: string
}

interface PersistedQueryState {
  query: string
  lastQuestion: string
  messages: PersistedChatMessage[]
  response: OneShotResponse | null
  runResult: RunResponse | null
  suggestedQuestions: string[]
  showResults: boolean
  showSqlPanel: boolean
  showQueryResultPanel: boolean
  editedSql: string
  isEditing: boolean
}

interface ResultTabState {
  id: string
  question: string
  sql: string
  resultData: PreviewData | null
  visualization: VisualizationResponsePayload | null
  statistics: SimpleStatsRow[]
  insight: string
  status: "pending" | "error" | "success"
  error?: string | null
  response: OneShotResponse | null
  runResult: RunResponse | null
  suggestedQuestions: string[]
  showSqlPanel: boolean
  showQueryResultPanel: boolean
  editedSql: string
  isEditing: boolean
}

interface DashboardFolderOption {
  id: string
  name: string
}

const MAX_PERSIST_ROWS = 200
const VIZ_CACHE_PREFIX = "viz_cache_v3:"
const VIZ_CACHE_TTL_MS = 1000 * 60 * 60 * 24
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any

const hashText = (value: string) => {
  let hash = 2166136261
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i)
    hash +=
      (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24)
  }
  return (hash >>> 0).toString(16)
}

const buildVizCacheKey = (sqlText: string, questionText: string, previewData: PreviewData | null) => {
  if (!previewData) return null
  const columns = previewData.columns || []
  const rows = previewData.rows || []
  const rowCount = previewData.row_count ?? rows.length
  const head = rows.slice(0, 10)
  const tail = rows.slice(Math.max(0, rows.length - 10))
  const basis = JSON.stringify({
    q: (questionText || "").trim(),
    sql: (sqlText || "").trim(),
    columns,
    rowCount,
    head,
    tail,
  })
  return `${VIZ_CACHE_PREFIX}${hashText(basis)}`
}

const trimPreview = (preview?: PreviewData): PreviewData | undefined => {
  if (!preview) return preview
  const rows = Array.isArray(preview.rows) ? preview.rows : []
  const trimmedRows = rows.slice(0, MAX_PERSIST_ROWS)
  return {
    ...preview,
    rows: trimmedRows,
    row_count: trimmedRows.length,
  }
}

const sanitizeRunResult = (runResult: RunResponse | null): RunResponse | null => {
  if (!runResult) return null
  return {
    ...runResult,
    result: trimPreview(runResult.result) || runResult.result,
  }
}

const sanitizeResponse = (response: OneShotResponse | null): OneShotResponse | null => {
  if (!response) return null
  const payload = response.payload || ({} as OneShotPayload)
  const result = payload.result
    ? {
        ...payload.result,
        preview: trimPreview(payload.result.preview),
      }
    : undefined
  const draft = payload.draft ? { final_sql: payload.draft.final_sql } : undefined
  const final = payload.final
    ? {
        final_sql: payload.final.final_sql,
        risk_score: payload.final.risk_score,
        used_tables: payload.final.used_tables,
      }
    : undefined
  const policy = payload.policy
    ? {
        passed: payload.policy.passed,
        checks: Array.isArray(payload.policy.checks)
          ? payload.policy.checks.map((item) => ({
              name: String(item.name ?? ""),
              passed: Boolean(item.passed),
              message: String(item.message ?? ""),
            }))
          : [],
      }
    : undefined
  const clarification = payload.clarification
    ? {
        reason: payload.clarification.reason,
        question: payload.clarification.question,
        options: Array.isArray(payload.clarification.options)
          ? payload.clarification.options.map((item) => String(item))
          : [],
        example_inputs: Array.isArray(payload.clarification.example_inputs)
          ? payload.clarification.example_inputs.map((item) => String(item))
          : [],
      }
    : undefined
  return {
    qid: response.qid,
    payload: {
      mode: payload.mode,
      question: payload.question,
      result,
      risk: payload.risk,
      policy,
      draft,
      final,
      clarification,
    },
  }
}

const serializeMessages = (messages: ChatMessage[]): PersistedChatMessage[] =>
  messages.map((message) => ({
    ...message,
    timestamp: message.timestamp.toISOString(),
  }))

const deserializeMessages = (messages: PersistedChatMessage[]): ChatMessage[] =>
  messages.map((message) => {
    const parsed = new Date(message.timestamp)
    return {
      ...message,
      timestamp: Number.isNaN(parsed.getTime()) ? new Date() : parsed,
    }
  })

export function QueryView() {
  const { user, isHydrated: isAuthHydrated } = useAuth()
  const apiBaseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "").replace(/\/$/, "")
  const apiUrl = (path: string) => (apiBaseUrl ? `${apiBaseUrl}${path}` : path)
  const chatUser = (user?.name || "김연구원").trim() || "김연구원"
  const chatUserRole = (user?.role || "연구원").trim() || "연구원"
  const chatHistoryUser = (user?.id || chatUser).trim() || chatUser
  const fetchWithTimeout = async (input: RequestInfo, init: RequestInit = {}, timeoutMs = 45000) => {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)
    try {
      return await fetch(input, { ...init, signal: controller.signal })
    } catch (error: any) {
      const message = String(error?.message || "")
      const isAbort =
        error?.name === "AbortError" ||
        error?.name === "TimeoutError" ||
        /aborted/i.test(message)
      if (isAbort) {
        throw new Error("요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.")
      }
      throw error
    } finally {
      clearTimeout(timeout)
    }
  }
  const [query, setQuery] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [showResults, setShowResults] = useState(false)
  const [showSqlPanel, setShowSqlPanel] = useState(false)
  const [showQueryResultPanel, setShowQueryResultPanel] = useState(false)
  const [editedSql, setEditedSql] = useState("")
  const [isEditing, setIsEditing] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [response, setResponse] = useState<OneShotResponse | null>(null)
  const [runResult, setRunResult] = useState<RunResponse | null>(null)
  const [visualizationResult, setVisualizationResult] = useState<VisualizationResponsePayload | null>(null)
  const [visualizationLoading, setVisualizationLoading] = useState(false)
  const [visualizationError, setVisualizationError] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [resultTabs, setResultTabs] = useState<ResultTabState[]>([])
  const [activeTabId, setActiveTabId] = useState<string>("")
  const [boardSaving, setBoardSaving] = useState(false)
  const [boardMessage, setBoardMessage] = useState<string | null>(null)
  const [isSaveDialogOpen, setIsSaveDialogOpen] = useState(false)
  const [saveTitle, setSaveTitle] = useState("")
  const [saveFolderMode, setSaveFolderMode] = useState<"existing" | "new">("existing")
  const [saveFolderId, setSaveFolderId] = useState<string>("")
  const [saveNewFolderName, setSaveNewFolderName] = useState("")
  const [saveFolderOptions, setSaveFolderOptions] = useState<DashboardFolderOption[]>([])
  const [saveFoldersLoading, setSaveFoldersLoading] = useState(false)
  const [lastQuestion, setLastQuestion] = useState<string>("")
  const [suggestedQuestions, setSuggestedQuestions] = useState<string[]>([])
  const [quickQuestions, setQuickQuestions] = useState<string[]>([
    "입원 환자 수를 월별 추이로 보여줘",
    "가장 흔한 진단 코드는 무엇인가요?",
    "ICU 재원일수가 긴 환자군을 알려줘",
  ])
  const [isHydrated, setIsHydrated] = useState(false)
  const [isSqlDragging, setIsSqlDragging] = useState(false)
  const [isDesktopLayout, setIsDesktopLayout] = useState(false)
  const [resultsPanelWidth, setResultsPanelWidth] = useState(55)
  const [isPanelResizing, setIsPanelResizing] = useState(false)
  const saveTimerRef = useRef<number | null>(null)
  const mainContentRef = useRef<HTMLDivElement | null>(null)
  const sqlScrollRef = useRef<HTMLDivElement | null>(null)
  const sqlDragRef = useRef({
    active: false,
    startX: 0,
    startY: 0,
    scrollLeft: 0,
    scrollTop: 0,
  })
  const panelResizeRef = useRef({
    active: false,
    startX: 0,
    startRightWidth: 55,
    containerWidth: 0,
  })
  const requestTokenRef = useRef(0)

  const payload = response?.payload
  const mode = payload?.mode
  const demoResult = mode === "demo" ? payload?.result : null
  const currentSql =
    (mode === "demo"
      ? demoResult?.sql
      : payload?.final?.final_sql || payload?.draft?.final_sql) || ""
  const riskScore = payload?.final?.risk_score ?? payload?.risk?.risk
  const riskIntent = payload?.risk?.intent
  const preview = runResult?.result ?? demoResult?.preview ?? null
  const previewColumns = preview?.columns ?? []
  const previewRows = preview?.rows ?? []
  const previewRowCount = preview?.row_count ?? previewRows.length
  const previewRowCap = preview?.row_cap
  const survivalChartData = buildSurvivalFromPreview(previewColumns, previewRows)
  const totalPatients = survivalChartData?.length
    ? Math.max(...survivalChartData.map((item) => item.atRisk)) || previewRowCount
    : previewRowCount
  const totalEvents = survivalChartData?.length
    ? Math.max(...survivalChartData.map((item) => item.events))
    : 0
  const medianSurvival = (() => {
    if (!survivalChartData?.length) return 0
    const sorted = [...survivalChartData].sort((a, b) => a.time - b.time)
    const hit = sorted.find((item) => item.survival <= 50)
    return hit?.time ?? sorted[sorted.length - 1]?.time ?? 0
  })()
  const summary = demoResult?.summary
  const source = demoResult?.source
  const previewRecords = useMemo(
    () =>
      previewRows.map((row) =>
        Object.fromEntries(previewColumns.map((col, idx) => [col, row?.[idx]]))
      ),
    [previewColumns, previewRows]
  )
  const statsRows = useMemo(() => buildSimpleStats(previewColumns, previewRows), [previewColumns, previewRows])
  const boxPlotRows = useMemo(
    () =>
      statsRows
        .filter((row) => row.count > 0 && row.q1 != null && row.q3 != null && row.min != null && row.max != null)
        .map((row) => {
          const min = row.min as number
          const q1 = row.q1 as number
          const median = row.median as number
          const q3 = row.q3 as number
          const max = row.max as number
          const iqr = Math.max(0, q3 - q1)
          const whiskerLow = Math.max(min, q1 - 1.5 * iqr)
          const whiskerHigh = Math.min(max, q3 + 1.5 * iqr)
          return {
            column: row.column,
            min,
            q1,
            median,
            q3,
            max,
            whiskerLow,
            whiskerHigh,
            outlierLow: min < whiskerLow ? min : null,
            outlierHigh: max > whiskerHigh ? max : null,
            iqrBase: q1,
            iqr,
          }
        }),
    [statsRows]
  )
  const boxPlotYDomain = useMemo<[number, number] | undefined>(() => {
    if (!boxPlotRows.length) return undefined
    const minValue = Math.min(...boxPlotRows.map((row) => row.whiskerLow))
    const maxValue = Math.max(...boxPlotRows.map((row) => row.whiskerHigh))
    const spread = Math.max(1, maxValue - minValue)
    const pad = Math.max(20, spread * 0.1)
    const paddedMin = minValue - pad
    const paddedMax = maxValue + pad
    if (paddedMin === paddedMax) return [paddedMin - 1, paddedMax + 1]
    return [paddedMin, paddedMax]
  }, [boxPlotRows])
  const displaySql = (isEditing ? editedSql : runResult?.sql || currentSql) || ""
  const recommendedAnalysis = useMemo(
    () => (Array.isArray(visualizationResult?.analyses) ? visualizationResult!.analyses![0] : null),
    [visualizationResult]
  )
  const recommendedFigure = useMemo(() => {
    const fig = recommendedAnalysis?.figure_json
    if (fig && typeof fig === "object") return fig as { data?: unknown[]; layout?: Record<string, unknown> }
    return null
  }, [recommendedAnalysis])
  const recommendedChart = useMemo(() => {
    const spec = recommendedAnalysis?.chart_spec
    if (!spec || !previewRecords.length) return null
    const chartType = String(spec.chart_type || "bar").toLowerCase()
    const xKey = spec.x && previewColumns.includes(spec.x) ? spec.x : previewColumns[0]
    const candidateY = spec.y && previewColumns.includes(spec.y) ? spec.y : previewColumns.find((col) => {
      const v = previewRecords[0]?.[col]
      return Number.isFinite(Number(v))
    })

    if (!xKey) return null

    if (chartType === "scatter" && candidateY) {
      const points = previewRecords
        .map((row) => ({
          x: Number(row[xKey]),
          y: Number(row[candidateY]),
        }))
        .filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y))
      return { type: "scatter" as const, xKey, yKey: candidateY, data: points }
    }

    const grouped = new Map<string, { total: number; count: number }>()
    for (const row of previewRecords) {
      const key = String(row[xKey] ?? "")
      if (!grouped.has(key)) grouped.set(key, { total: 0, count: 0 })
      const bucket = grouped.get(key)!
      if (candidateY) {
        const num = Number(row[candidateY])
        if (Number.isFinite(num)) {
          bucket.total += num
          bucket.count += 1
        }
      } else {
        bucket.count += 1
      }
    }
    const agg = String(spec.agg || (candidateY ? "avg" : "count")).toLowerCase()
    const data = Array.from(grouped.entries()).map(([x, v]) => {
      const y =
        !candidateY || agg === "count"
          ? v.count
          : agg === "sum"
            ? v.total
            : v.count > 0
              ? v.total / v.count
              : 0
      return { x, y: Number(y.toFixed(4)) }
    })
    return {
      type: chartType === "line" ? ("line" as const) : ("bar" as const),
      xKey,
      yKey: candidateY || "count",
      data,
    }
  }, [recommendedAnalysis, previewColumns, previewRecords])
  const resultInterpretation = useMemo(() => {
    if (summary) return summary
    if (!previewColumns.length) return "쿼리 결과가 없어 해석을 생성할 수 없습니다."
    const numericCols = statsRows.filter((row) => row.count > 0)
    const topNumeric = numericCols
      .slice()
      .sort((a, b) => (b.avg ?? Number.NEGATIVE_INFINITY) - (a.avg ?? Number.NEGATIVE_INFINITY))[0]
    const base = `현재 결과는 ${previewColumns.length}개 컬럼, 미리보기 ${previewRowCount}행입니다.`
    if (!topNumeric || topNumeric.avg == null) return `${base} 수치형 요약 대상이 제한적입니다.`
    return `${base} 평균 기준으로 '${topNumeric.column}' 컬럼이 가장 큽니다(평균 ${formatStatNumber(topNumeric.avg)}).`
  }, [summary, previewColumns, previewRowCount, statsRows])
  const chartInterpretation = useMemo(() => {
    if (recommendedAnalysis?.summary) return recommendedAnalysis.summary
    if (recommendedAnalysis?.reason) return recommendedAnalysis.reason
    if (recommendedChart) {
      return `차트 유형은 ${recommendedChart.type.toUpperCase()}이며, X축은 ${recommendedChart.xKey}, Y축은 ${recommendedChart.yKey} 기준입니다.`
    }
    if (survivalChartData?.length) {
      return `생존 곡선을 표시했습니다. 추정 중앙 생존시간은 약 ${medianSurvival.toFixed(2)}입니다.`
    }
    return "현재 결과에서는 자동 차트 추천 근거가 충분하지 않습니다."
  }, [recommendedAnalysis, recommendedChart, survivalChartData, medianSurvival])
  const statsInterpretation = useMemo(() => {
    if (!statsRows.length) return "통계표를 생성할 결과가 없습니다."
    const numeric = statsRows.filter((row) => row.count > 0)
    const nullTotal = statsRows.reduce((sum, row) => sum + row.nullCount, 0)
    const missingTotal = statsRows.reduce((sum, row) => sum + row.missingCount, 0)
    if (!numeric.length) return `수치형 컬럼이 없어 결측/NULL 중심으로 확인됩니다(결측 ${missingTotal}, NULL ${nullTotal}).`
    const widest = numeric
      .slice()
      .sort((a, b) => ((b.max ?? 0) - (b.min ?? 0)) - ((a.max ?? 0) - (a.min ?? 0)))[0]
    const range = widest.max != null && widest.min != null ? widest.max - widest.min : null
    return `수치형 컬럼 ${numeric.length}개를 집계했습니다. 결측 ${missingTotal}, NULL ${nullTotal}이며, '${widest.column}'의 분산폭이 가장 큽니다${range != null ? ` (범위 ${formatStatNumber(range)})` : ""}.`
  }, [statsRows])
  const normalizeInsightText = (text: string) => {
    return text
      .replace(/Detected category \+ numeric for comparison\.?/gi, "범주형-수치형 조합 비교가 적합한 결과입니다.")
      .replace(/Result aliases indicate a time-series aggregate\.?/gi, "집계 결과가 시계열 추세 분석에 적합합니다.")
      .replace(/Result aliases indicate a grouped aggregate\.?/gi, "집계 결과가 그룹 비교 분석에 적합합니다.")
      .replace(/Detected time-like and numeric columns for a trend chart\.?/gi, "시간형-수치형 조합으로 추세 차트가 적합합니다.")
      .replace(/Detected multiple numeric columns for correlation\.?/gi, "수치형 컬럼 간 상관관계 분석이 적합합니다.")
      .replace(/Detected a single numeric column for distribution\.?/gi, "단일 수치형 컬럼 분포 분석이 적합합니다.")
      .replace(/^\s*Detected.*$/gim, "")
      .replace(/^Rows:\s*(\d+)$/gim, "결과 행 수: $1")
      .replace(/^source:\s*.+$/gim, "")
      .replace(/^\s*source:\s*.+$/gim, "")
      .replace(/^\s*recommended reason:\s*.+$/gim, "")
      .replace(/\bsource\s*:\s*llm\b/gi, "")
      .replace(/\n{3,}/g, "\n\n")
      .trim()
  }

  const integratedInsight = useMemo(() => {
    if (visualizationResult?.insight) return normalizeInsightText(visualizationResult.insight)
    const lines: string[] = []
    lines.push(resultInterpretation)
    lines.push(chartInterpretation)
    lines.push(statsInterpretation)
    if (riskScore != null) {
      lines.push(`위험 점수는 ${riskScore}${riskIntent ? ` (${riskIntent})` : ""}로 평가되었습니다.`)
    }
    return normalizeInsightText(lines.join("\n\n"))
  }, [visualizationResult, resultInterpretation, chartInterpretation, statsInterpretation, riskScore, riskIntent])
  const formattedDisplaySql = useMemo(() => formatSqlForDisplay(displaySql), [displaySql])
  const highlightedDisplaySql = useMemo(() => highlightSqlForDisplay(displaySql), [displaySql])
  const visibleQuickQuestions = quickQuestions.slice(0, 3)
  const hasConversation =
    messages.length > 0 || Boolean(response) || Boolean(runResult) || query.trim().length > 0
  const shouldShowResizablePanels = showResults && isDesktopLayout
  const chatPanelStyle = shouldShowResizablePanels ? { width: `${100 - resultsPanelWidth}%` } : undefined
  const resultsPanelStyle = shouldShowResizablePanels ? { width: `${resultsPanelWidth}%` } : undefined
  const appendSuggestions = (base: string, _suggestions?: string[]) => base
  const createResultTab = (questionText: string): ResultTabState => ({
    id: `tab-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    question: questionText,
    sql: "",
    resultData: null,
    visualization: null,
    statistics: [],
    insight: "",
    status: "pending",
    error: null,
    response: null,
    runResult: null,
    suggestedQuestions: [],
    showSqlPanel: false,
    showQueryResultPanel: false,
    editedSql: "",
    isEditing: false,
  })

  const updateTab = (tabId: string, patch: Partial<ResultTabState>) => {
    setResultTabs((prev) =>
      prev.map((tab) => (tab.id === tabId ? { ...tab, ...patch } : tab))
    )
  }

  const updateActiveTab = (patch: Partial<ResultTabState>) => {
    if (!activeTabId) return
    updateTab(activeTabId, patch)
  }

  const fetchVisualizationPlan = async (
    sqlText: string,
    questionText: string,
    previewData: PreviewData | null,
    targetTabId?: string
  ) => {
    if (!sqlText?.trim() || !previewData?.columns?.length || !previewData?.rows?.length) {
      if (!targetTabId || targetTabId === activeTabId) {
        setVisualizationResult(null)
        setVisualizationError(null)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: null })
      }
      return null
    }
    // Always fetch latest visualization/insight from server (disable local cache)
    setVisualizationLoading(true)
    setVisualizationError(null)
    try {
      const records = previewData.rows.map((row) =>
        Object.fromEntries(previewData.columns.map((col, idx) => [col, row?.[idx]]))
      )
      const res = await fetchWithTimeout(
        apiUrl("/visualize"),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_query: questionText || lastQuestion || "",
            sql: sqlText,
            rows: records,
          }),
        },
        90000
      )
      if (!res.ok) throw new Error(await readError(res))
      const data: VisualizationResponsePayload = await res.json()
      if (!targetTabId || targetTabId === activeTabId) {
        setVisualizationResult(data)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: data })
      }
      return data
    } catch (err: any) {
      const message = err?.message || "시각화 추천 플랜 조회에 실패했습니다."
      if (!targetTabId || targetTabId === activeTabId) {
        setVisualizationResult(null)
        setVisualizationError(message)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: null, error: message })
      }
      return null
    } finally {
      setVisualizationLoading(false)
    }
  }

  const handlePanelResizeMouseDown = (event: React.MouseEvent<HTMLDivElement>) => {
    if (event.button !== 0 || !shouldShowResizablePanels) return
    const container = mainContentRef.current
    if (!container) return
    const rect = container.getBoundingClientRect()
    if (rect.width <= 0) return

    panelResizeRef.current = {
      active: true,
      startX: event.clientX,
      startRightWidth: resultsPanelWidth,
      containerWidth: rect.width,
    }
    setIsPanelResizing(true)
    document.body.style.cursor = "col-resize"
    document.body.style.userSelect = "none"
    event.preventDefault()
  }

  const handleSqlMouseDown = (event: React.MouseEvent<HTMLDivElement>) => {
    if (event.button !== 0) return
    const el = sqlScrollRef.current
    if (!el) return

    sqlDragRef.current = {
      active: true,
      startX: event.clientX,
      startY: event.clientY,
      scrollLeft: el.scrollLeft,
      scrollTop: el.scrollTop,
    }
    setIsSqlDragging(true)
    event.preventDefault()
  }

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      if (!sqlDragRef.current.active) return
      const el = sqlScrollRef.current
      if (!el) return
      const dx = event.clientX - sqlDragRef.current.startX
      const dy = event.clientY - sqlDragRef.current.startY
      el.scrollLeft = sqlDragRef.current.scrollLeft - dx
      el.scrollTop = sqlDragRef.current.scrollTop - dy
    }

    const stopDragging = () => {
      if (!sqlDragRef.current.active) return
      sqlDragRef.current.active = false
      setIsSqlDragging(false)
    }

    window.addEventListener("mousemove", handleMouseMove)
    window.addEventListener("mouseup", stopDragging)
    return () => {
      window.removeEventListener("mousemove", handleMouseMove)
      window.removeEventListener("mouseup", stopDragging)
    }
  }, [])

  useEffect(() => {
    if (!activeTabId) return
    const tab = resultTabs.find((item) => item.id === activeTabId)
    if (!tab) return
    setResponse(tab.response)
    setRunResult(tab.runResult)
    setVisualizationResult(tab.visualization)
    setVisualizationError(tab.error || null)
    setError(tab.error || null)
    setEditedSql(tab.editedSql || tab.sql || "")
    setIsEditing(tab.isEditing)
    setSuggestedQuestions(tab.suggestedQuestions || [])
    setLastQuestion(tab.question || "")
    setShowSqlPanel(tab.showSqlPanel)
    setShowQueryResultPanel(tab.showQueryResultPanel)
    setShowResults(true)
  }, [activeTabId, resultTabs])

  useEffect(() => {
    const syncDesktopLayout = () => {
      setIsDesktopLayout(window.innerWidth >= 1024)
    }
    syncDesktopLayout()
    window.addEventListener("resize", syncDesktopLayout)
    return () => window.removeEventListener("resize", syncDesktopLayout)
  }, [])

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      if (!panelResizeRef.current.active) return
      const { startX, startRightWidth, containerWidth } = panelResizeRef.current
      if (containerWidth <= 0) return
      const deltaPercent = ((event.clientX - startX) / containerWidth) * 100
      const nextRightWidth = Math.min(70, Math.max(30, startRightWidth - deltaPercent))
      setResultsPanelWidth(nextRightWidth)
    }

    const stopResizing = () => {
      if (!panelResizeRef.current.active) return
      panelResizeRef.current.active = false
      setIsPanelResizing(false)
      document.body.style.cursor = ""
      document.body.style.userSelect = ""
    }

    window.addEventListener("mousemove", handleMouseMove)
    window.addEventListener("mouseup", stopResizing)
    return () => {
      window.removeEventListener("mousemove", handleMouseMove)
      window.removeEventListener("mouseup", stopResizing)
      document.body.style.cursor = ""
      document.body.style.userSelect = ""
    }
  }, [])

  function normalizeColumn(value: string) {
    return value.toLowerCase().replace(/[^a-z0-9]/g, "")
  }

  function findColumnIndex(columns: string[], candidates: string[]) {
    const normalized = columns.map((col) => normalizeColumn(col))
    for (const candidate of candidates) {
      const idx = normalized.indexOf(normalizeColumn(candidate))
      if (idx >= 0) return idx
    }
    return -1
  }

  function toNumber(value: unknown) {
    if (value == null) return null
    const num = Number(value)
    return Number.isFinite(num) ? num : null
  }

  function buildSurvivalFromPreview(columns: string[], rows: any[][]) {
    if (!columns.length || !rows.length) return null
    const timeIdx = findColumnIndex(columns, ["time", "days", "day", "week", "weeks", "month", "months"])
    const survivalIdx = findColumnIndex(columns, ["survival", "survivalrate", "rate", "prob", "probability"])
    if (timeIdx < 0 || survivalIdx < 0) return null
    const lowerIdx = findColumnIndex(columns, ["lowerci", "ci_lower", "lcl", "lower"])
    const upperIdx = findColumnIndex(columns, ["upperci", "ci_upper", "ucl", "upper"])
    const atRiskIdx = findColumnIndex(columns, ["atrisk", "at_risk", "risk"])
    const eventsIdx = findColumnIndex(columns, ["events", "event", "death", "deaths"])

    const data = rows
      .map((row) => {
        const time = toNumber(row[timeIdx])
        const survival = toNumber(row[survivalIdx])
        if (time == null || survival == null) return null
        const lowerCI = toNumber(row[lowerIdx]) ?? survival
        const upperCI = toNumber(row[upperIdx]) ?? survival
        const atRisk = toNumber(row[atRiskIdx]) ?? 0
        const events = toNumber(row[eventsIdx]) ?? 0
        return { time, survival, lowerCI, upperCI, atRisk, events }
      })
      .filter(Boolean) as {
        time: number
        survival: number
        lowerCI: number
        upperCI: number
        atRisk: number
        events: number
      }[]

    return data.length ? data : null
  }

  const buildSuggestions = (questionText: string, columns?: string[]) => {
    const suggestions: string[] = []
    const normalized = questionText.toLowerCase()
    const cols = (columns || []).map((col) => col.toLowerCase())

    const pushUnique = (text: string) => {
      if (!text || suggestions.includes(text)) return
      suggestions.push(text)
    }

    if (normalized.includes("diagnos") || normalized.includes("진단") || cols.some((c) => c.includes("icd"))) {
      pushUnique("상위 10개 진단 보기")
      pushUnique("성별/연령별 진단 분포")
      pushUnique("진단 추세 보기")
    } else if (normalized.includes("icu") || normalized.includes("재원") || cols.some((c) => c.includes("stay"))) {
      pushUnique("ICU 평균 재원일수")
      pushUnique("ICU 재원일수 분포")
      pushUnique("ICU 재원 상위 10명")
    } else if (normalized.includes("입원") || normalized.includes("admission")) {
      pushUnique("입원 추이 보기")
      pushUnique("진단별 입원 건수")
      pushUnique("평균 입원기간")
    }

    if (cols.some((c) => c.includes("date") || c.includes("time"))) {
      pushUnique("기간별 추이")
    }
    if (cols.some((c) => c.includes("gender"))) {
      pushUnique("성별 통계 보기")
    }
    if (cols.some((c) => c.includes("age"))) {
      pushUnique("연령대별 보기")
    }

    if (suggestions.length === 0) {
      pushUnique("상위 10개 보기")
      pushUnique("최근 6개월")
      pushUnique("성별 통계 보기")
    }

    return suggestions.slice(0, 3)
  }

  const buildClarificationSuggestions = (payload?: OneShotPayload) => {
    if (!payload?.clarification) return []
    const options = Array.isArray(payload.clarification.options)
      ? payload.clarification.options.map((item) => String(item).trim()).filter(Boolean)
      : []
    const examples = Array.isArray(payload.clarification.example_inputs)
      ? payload.clarification.example_inputs.map((item) => String(item).trim()).filter(Boolean)
      : []
    const merged = [...options, ...examples]
    return Array.from(new Set(merged)).slice(0, 5)
  }

  const readError = async (res: Response) => {
    const text = await res.text()
    try {
      const json = JSON.parse(text)
      if (json?.detail) return String(json.detail)
    } catch {}
    return text || `${res.status} ${res.statusText}`
  }

  const buildAssistantMessage = (data: OneShotResponse) => {
    if (data.payload.mode === "clarify") {
      const clarify = data.payload.clarification
      const prompt = clarify?.question?.trim() || "질문 범위를 조금 더 좁혀주세요."
      const options = Array.isArray(clarify?.options) ? clarify.options.filter(Boolean) : []
      const examples = Array.isArray(clarify?.example_inputs) ? clarify.example_inputs.filter(Boolean) : []
      const reason = clarify?.reason?.trim()
      const lines = [prompt]
      if (reason) {
        lines.push(`이유: ${reason}`)
      }
      if (options.length) {
        lines.push(`선택 예시: ${options.slice(0, 4).join(", ")}`)
      }
      if (examples.length) {
        lines.push(`입력 예: ${examples.slice(0, 2).join(" / ")}`)
      }
      return lines.join("\n")
    }
    if (data.payload.mode === "demo") {
      const parts: string[] = []
      const summaryText = data.payload.result?.summary
      if (summaryText) {
        parts.push(summaryText.endsWith(".") ? summaryText : `${summaryText}.`)
      } else {
        parts.push("데모 캐시 결과를 가져왔어요.")
      }
      const rowCount = data.payload.result?.preview?.row_count
      if (rowCount != null) parts.push(`미리보기로 ${rowCount}행을 보여드렸어요.`)
      if (data.payload.result?.source) parts.push(`데모 캐시(source: ${data.payload.result.source}) 기반입니다.`)
      return parts.join(" ")
    }
    const base = "요청하신 내용을 바탕으로 SQL을 준비했어요. 실행하면 결과를 가져올게요."
    const payload = data.payload
    const localRiskScore = payload?.final?.risk_score ?? payload?.risk?.risk
    const localRiskIntent = payload?.risk?.intent
    const riskLabel =
      localRiskScore != null ? `위험도 ${localRiskScore}${localRiskIntent ? ` (${localRiskIntent})` : ""}로 평가되었어요.` : ""
    return [base, riskLabel].filter(Boolean).join(" ")
  }

  useEffect(() => {
    if (!isAuthHydrated || !chatHistoryUser) return
    const loadChatState = async () => {
      setQuery("")
      setMessages([])
      setResponse(null)
      setRunResult(null)
      setShowResults(false)
      setShowSqlPanel(false)
      setShowQueryResultPanel(false)
      setEditedSql("")
      setIsEditing(false)
      setLastQuestion("")
      setSuggestedQuestions([])
      try {
        const res = await fetchWithTimeout(
          apiUrl(`/chat/history?user=${encodeURIComponent(chatHistoryUser)}`),
          {},
          12000
        )
        if (!res.ok) return
        const data = await res.json()
        const state = data?.state as Partial<PersistedQueryState> | null
        if (!state) return
        if (typeof state.query === "string") setQuery(state.query)
        if (Array.isArray(state.messages)) setMessages(deserializeMessages(state.messages))
        if (state.response) setResponse(state.response)
        if (state.runResult) setRunResult(state.runResult)
        if (typeof state.lastQuestion === "string") setLastQuestion(state.lastQuestion)
        if (Array.isArray(state.suggestedQuestions)) setSuggestedQuestions(state.suggestedQuestions)
        if (typeof state.showResults === "boolean") setShowResults(state.showResults)
        if (typeof state.showSqlPanel === "boolean") setShowSqlPanel(state.showSqlPanel)
        if (typeof state.showQueryResultPanel === "boolean") setShowQueryResultPanel(state.showQueryResultPanel)
        if (typeof state.editedSql === "string") setEditedSql(state.editedSql)
        if (typeof state.isEditing === "boolean") setIsEditing(state.isEditing)
      } catch {
        // ignore hydration errors
      } finally {
        setIsHydrated(true)
      }
    }
    loadChatState()
  }, [apiBaseUrl, isAuthHydrated, chatHistoryUser])

  useEffect(() => {
    if (!isHydrated || !chatHistoryUser) return
    if (saveTimerRef.current) {
      window.clearTimeout(saveTimerRef.current)
    }
    const state: PersistedQueryState = {
      query,
      lastQuestion,
      messages: serializeMessages(messages),
      response: sanitizeResponse(response),
      runResult: sanitizeRunResult(runResult),
      suggestedQuestions,
      showResults,
      showSqlPanel,
      showQueryResultPanel,
      editedSql,
      isEditing,
    }
    saveTimerRef.current = window.setTimeout(() => {
      fetch(apiUrl("/chat/history"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user: chatHistoryUser, state })
      }).catch(() => {})
    }, 600)
    return () => {
      if (saveTimerRef.current) {
        window.clearTimeout(saveTimerRef.current)
      }
    }
  }, [
    isHydrated,
    query,
    lastQuestion,
    messages,
    response,
    runResult,
    suggestedQuestions,
    showResults,
    showSqlPanel,
    showQueryResultPanel,
    editedSql,
    isEditing,
    apiBaseUrl,
    chatHistoryUser,
  ])

  useEffect(() => {
    const loadQuestions = async () => {
      try {
        const res = await fetchWithTimeout(apiUrl("/query/demo/questions"), {}, 15000)
        if (!res.ok) return
        const data = await res.json()
        if (Array.isArray(data?.questions) && data.questions.length) {
          setQuickQuestions(data.questions.slice(0, 3))
        }
      } catch {}
    }
    loadQuestions()
  }, [])

  const runQuery = async (questionText: string) => {
    const trimmed = questionText.trim()
    if (!trimmed) return

    const tab = createResultTab(trimmed)
    setResultTabs((prev) => [tab, ...prev])
    setActiveTabId(tab.id)
    setShowResults(true)
    const requestToken = ++requestTokenRef.current

    setIsLoading(true)
    setError(null)
    setBoardMessage(null)
    setResponse(null)
    setRunResult(null)
    setVisualizationResult(null)
    setVisualizationError(null)
    setEditedSql("")
    // Keep split layout while running next question to avoid chat panel jumping to full width.
    // Results content is refreshed below when response arrives.
    setShowSqlPanel(false)
    setShowQueryResultPanel(false)
    setLastQuestion(trimmed)
    setSuggestedQuestions([])
    const newMessage: ChatMessage = {
      id: Date.now().toString(),
      role: "user",
      content: trimmed,
      timestamp: new Date()
    }
    const shouldUseClarificationContext = response?.payload?.mode === "clarify"
    const conversationSeed = shouldUseClarificationContext ? [...messages, newMessage] : [newMessage]
    const conversation = conversationSeed
      .slice(-10)
      .map((item) => ({ role: item.role, content: item.content }))
    setMessages(prev => [...prev, newMessage])
    setQuery("")

    try {
      const res = await fetchWithTimeout(apiUrl("/query/oneshot"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: trimmed,
          conversation,
          user_name: chatUser,
          user_role: chatUserRole,
        })
      })
      if (!res.ok) {
        throw new Error(await readError(res))
      }
      if (requestToken !== requestTokenRef.current) return
      const data: OneShotResponse = await res.json()
      setResponse(data)
      updateTab(tab.id, {
        response: data,
        status: "success",
      })
      if (data.payload.mode === "clarify") {
        setShowSqlPanel(false)
        setShowQueryResultPanel(false)
        setEditedSql("")
        setIsEditing(false)
        const clarificationSuggestions = buildClarificationSuggestions(data.payload)
        setSuggestedQuestions(clarificationSuggestions)
        updateTab(tab.id, {
          suggestedQuestions: clarificationSuggestions,
          response: data,
          status: "success",
        })
        const responseMessage: ChatMessage = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(buildAssistantMessage(data), clarificationSuggestions),
          timestamp: new Date()
        }
        setMessages(prev => [...prev, responseMessage])
        return
      }

      setShowResults(true)
      setShowSqlPanel(false)
      setShowQueryResultPanel(false)
      const generatedSql =
        (data.payload.mode === "demo"
          ? data.payload.result?.sql
          : data.payload.final?.final_sql || data.payload.draft?.final_sql) || ""
      setEditedSql(
        generatedSql
      )
      setIsEditing(false)
      const suggestions =
        data.payload.mode === "demo"
          ? buildSuggestions(trimmed, data.payload.result?.preview?.columns)
          : []
      setSuggestedQuestions(suggestions)
      updateTab(tab.id, {
        question: trimmed,
        sql: generatedSql,
        response: data,
        suggestedQuestions: suggestions,
        editedSql: generatedSql,
        status: "success",
      })
      const responseMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: appendSuggestions(buildAssistantMessage(data), suggestions),
        timestamp: new Date()
      }
      setMessages(prev => [...prev, responseMessage])

      // Advanced 모드에서는 SQL 생성 직후 자동 실행
      if (data.payload.mode === "advanced" && generatedSql.trim()) {
        await executeAdvancedSql({
          qid: data.qid,
          sql: generatedSql,
          questionForSuggestions: trimmed,
          addAssistantMessage: true,
          tabId: tab.id,
        })
      } else if (data.payload.mode === "demo" && generatedSql.trim()) {
        const viz = await fetchVisualizationPlan(
          generatedSql.trim(),
          trimmed,
          data.payload.result?.preview || null,
          tab.id
        )
        updateTab(tab.id, {
          resultData: data.payload.result?.preview || null,
          visualization: viz,
          insight: normalizeInsightText(viz?.insight || ""),
        })
      }
    } catch (err: any) {
      const message =
        err?.name === "AbortError"
          ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
          : err?.message || "요청이 실패했습니다."
      setError(message)
      updateTab(tab.id, {
        status: "error",
        error: message,
      })
      setMessages(prev => [
        ...prev,
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: `오류: ${message}`,
          timestamp: new Date()
        }
      ])
    } finally {
      setIsLoading(false)
    }
  }

  const handleSubmit = async () => {
    await runQuery(query)
  }

  const handleQuickQuestion = async (text: string) => {
    await runQuery(text)
  }

  const deriveDashboardCategory = (text: string) => {
    const normalized = text.toLowerCase()
    if (normalized.includes("생존") || normalized.includes("survival")) return "생존분석"
    if (normalized.includes("재입원") || normalized.includes("readmission")) return "재입원"
    if (normalized.includes("icu")) return "ICU"
    if (normalized.includes("응급") || normalized.includes("emergency")) return "응급실"
    if (normalized.includes("사망") || normalized.includes("mortality")) return "사망률"
    return "전체"
  }

  const loadDashboardFolders = async () => {
    setSaveFoldersLoading(true)
    try {
      const res = await fetchWithTimeout(apiUrl("/dashboard/queries"), {}, 12000)
      if (!res.ok) throw new Error("failed to load folders")
      const data = await res.json()
      const folders = Array.isArray(data?.folders) ? data.folders : []
      const queries = Array.isArray(data?.queries) ? data.queries : []

      const folderMap = new Map<string, DashboardFolderOption>()
      const nameKeyMap = new Map<string, DashboardFolderOption>()

      const pushFolder = (idRaw: unknown, nameRaw: unknown) => {
        const id = String(idRaw || "").trim()
        const name = String(nameRaw || "").trim()
        if (!id || !name) return
        if (!folderMap.has(id)) {
          const item = { id, name }
          folderMap.set(id, item)
          nameKeyMap.set(name.toLowerCase(), item)
        }
      }

      folders.forEach((item: any) => {
        pushFolder(item?.id, item?.name)
      })

      queries.forEach((item: any) => {
        const folderId = String(item?.folderId || "").trim()
        const category = String(item?.category || "").trim()
        if (!category) return
        if (folderId) {
          pushFolder(folderId, category)
          return
        }
        const key = category.toLowerCase()
        const existed = nameKeyMap.get(key)
        if (existed) return
        const syntheticId = `category:${encodeURIComponent(category)}`
        pushFolder(syntheticId, category)
      })

      const mapped = Array.from(folderMap.values()).sort((a, b) => a.name.localeCompare(b.name, "ko"))
      setSaveFolderOptions(mapped)
      if (!mapped.length) {
        setSaveFolderId("")
      } else if (!mapped.some((item) => item.id === saveFolderId)) {
        setSaveFolderId(mapped[0].id)
      }
    } catch {
      setSaveFolderOptions([])
      setSaveFolderId("")
    } finally {
      setSaveFoldersLoading(false)
    }
  }

  const openSaveDialog = async () => {
    if (!displaySql && !currentSql) {
      setBoardMessage("저장할 SQL이 없습니다.")
      return
    }
    const title = (lastQuestion || query || "저장된 쿼리").trim() || "저장된 쿼리"
    setSaveTitle(title)
    setSaveFolderMode("existing")
    setSaveNewFolderName("")
    setBoardMessage(null)
    setIsSaveDialogOpen(true)
    await loadDashboardFolders()
  }

  const handleSaveToDashboard = async () => {
    const finalTitle = (saveTitle || lastQuestion || query || "저장된 쿼리").trim()
    if (!finalTitle) {
      setBoardMessage("저장 이름을 입력해주세요.")
      return
    }

    const newFolderName = saveNewFolderName.trim()
    if (saveFolderMode === "new" && !newFolderName) {
      setBoardMessage("새 폴더 이름을 입력해주세요.")
      return
    }

    const selectedFolder = saveFolderOptions.find((item) => item.id === saveFolderId) || null
    const folderId =
      saveFolderMode === "new"
        ? `folder-${Date.now()}`
        : selectedFolder?.id || ""
    const folderName =
      saveFolderMode === "new"
        ? newFolderName
        : selectedFolder?.name || deriveDashboardCategory(finalTitle)

    const category = folderName || deriveDashboardCategory(finalTitle)
    const metrics = [
      { label: "행 수", value: String(previewRowCount ?? 0) },
      { label: "컬럼 수", value: String(previewColumns.length) },
      { label: "ROW CAP", value: previewRowCap != null ? String(previewRowCap) : "-" },
    ]
    const previewPayload =
      previewColumns.length && previewRows.length
        ? {
            columns: previewColumns,
            rows: previewRows.slice(0, 50),
            row_count: previewRowCount ?? previewRows.length,
            row_cap: previewRowCap ?? null,
          }
        : undefined
    const newEntry = {
      id: `dashboard-${Date.now()}`,
      title: finalTitle,
      description: summary || "쿼리 결과 요약",
      query: displaySql || currentSql,
      lastRun: "방금 전",
      isPinned: true,
      category,
      folderId: folderId || undefined,
      preview: previewPayload,
      metrics,
      chartType: "bar",
    }
    setBoardSaving(true)
    setBoardMessage(null)
    try {
      const saveRes = await fetchWithTimeout(apiUrl("/dashboard/saveQuery"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: finalTitle,
          sql: displaySql || currentSql,
          metadata: {
            row_count: previewRowCount ?? 0,
            column_count: previewColumns.length,
            row_cap: previewRowCap ?? null,
            summary: summary || "",
            mode: mode || "",
            entry: newEntry,
            new_folder:
              saveFolderMode === "new"
                ? {
                    id: folderId,
                    name: folderName,
                    createdAt: new Date().toISOString(),
                  }
                : null,
          },
        }),
      }, 15000)
      if (!saveRes.ok) {
        throw new Error("save failed")
      }
      updateActiveTab({ question: finalTitle })
      setLastQuestion(finalTitle)
      setBoardMessage("결과 보드에 저장했습니다.")
      setIsSaveDialogOpen(false)
    } catch (err) {
      setBoardMessage("결과 보드 저장에 실패했습니다.")
    } finally {
      setBoardSaving(false)
    }
  }

  const executeAdvancedSql = async ({
    qid,
    sql,
    questionForSuggestions,
    addAssistantMessage = true,
    tabId,
  }: {
    qid?: string
    sql?: string
    questionForSuggestions?: string
    addAssistantMessage?: boolean
    tabId?: string
  }) => {
    const body: Record<string, any> = {
      user_ack: true,
      user_name: chatUser,
      user_role: chatUserRole,
    }
    if (qid) {
      body.qid = qid
    }
    if (sql?.trim()) {
      body.sql = sql.trim()
    }

    const res = await fetchWithTimeout(apiUrl("/query/run"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    })
    if (!res.ok) {
      throw new Error(await readError(res))
    }

    const data: RunResponse = await res.json()
    setRunResult(data)
    setShowResults(true)
    setIsEditing(false)
    const targetTabId = tabId || activeTabId
    const viz = await fetchVisualizationPlan(
      (data.sql || sql || "").trim(),
      questionForSuggestions || lastQuestion || "",
      data.result || null,
      targetTabId
    )

    const suggestions = buildSuggestions(questionForSuggestions || lastQuestion, data.result?.columns)
    setSuggestedQuestions(suggestions)
    if (targetTabId) {
      updateTab(targetTabId, {
        sql: data.sql || sql || "",
        runResult: data,
        resultData: data.result || null,
        visualization: viz,
        suggestedQuestions: suggestions,
        insight: normalizeInsightText(viz?.insight || ""),
        status: "success",
        error: null,
      })
    }

    if (addAssistantMessage) {
      setMessages(prev => [
        ...prev,
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(
            `쿼리를 실행했어요. 미리보기로 ${data.result?.row_count ?? 0}행을 가져왔습니다.`,
            suggestions
          ),
          timestamp: new Date()
        }
      ])
    }
  }

  const handleExecuteEdited = async (overrideSql?: string) => {
    if (!response || mode !== "advanced") return
    setIsLoading(true)
    setError(null)
    setBoardMessage(null)
    updateActiveTab({ status: "pending", error: null })
    try {
      const sqlToRun = (overrideSql || editedSql || currentSql).trim()
      await executeAdvancedSql({
        qid: response.qid,
        sql: sqlToRun,
        questionForSuggestions: lastQuestion,
        addAssistantMessage: true,
        tabId: activeTabId,
      })
    } catch (err: any) {
      const message =
        err?.name === "AbortError"
          ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
          : err?.message || "실행이 실패했습니다."
      setError(message)
      updateActiveTab({ status: "error", error: message })
      setMessages(prev => [
        ...prev,
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: `오류: ${message}`,
          timestamp: new Date()
        }
      ])
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    const runPendingDashboardQuery = async () => {
      if (typeof window === "undefined") return
      const raw = localStorage.getItem("ql_pending_dashboard_query")
      if (!raw) return
      localStorage.removeItem("ql_pending_dashboard_query")

      let parsed: { question?: string; sql?: string } | null = null
      try {
        parsed = JSON.parse(raw)
      } catch {
        return
      }
      const sqlText = String(parsed?.sql || "").trim()
      if (!sqlText) return

      const questionText = String(parsed?.question || "").trim() || "대시보드 저장 쿼리"
      const tab = createResultTab(questionText)
      tab.sql = sqlText
      tab.editedSql = sqlText
      tab.showSqlPanel = true
      setResultTabs((prev) => [tab, ...prev])
      setActiveTabId(tab.id)
      setShowResults(true)
      setIsLoading(true)
      setError(null)
      setBoardMessage(null)
      setLastQuestion(questionText)
      setSuggestedQuestions([])

      try {
        await executeAdvancedSql({
          sql: sqlText,
          questionForSuggestions: questionText,
          addAssistantMessage: true,
          tabId: tab.id,
        })
      } catch (err: any) {
        const message =
          err?.name === "AbortError"
            ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
            : err?.message || "실행이 실패했습니다."
        setError(message)
        updateTab(tab.id, { status: "error", error: message })
      } finally {
        setIsLoading(false)
      }
    }

    runPendingDashboardQuery()
    const onOpenQueryView = () => {
      void runPendingDashboardQuery()
    }
    window.addEventListener("ql-open-query-view", onOpenQueryView)
    return () => {
      window.removeEventListener("ql-open-query-view", onOpenQueryView)
    }
  }, [])

  const handleCopySql = async () => {
    if (!displaySql) return
    try {
      await navigator.clipboard.writeText(displaySql)
    } catch {}
  }

  const handleDownloadCsv = () => {
    if (!previewColumns.length || !previewRows.length) return
    const header = previewColumns.join(",")
    const body = previewRows
      .map((row) =>
        previewColumns
          .map((_, idx) => {
            const cell = row[idx]
            const text = cell == null ? "" : String(cell)
            if (/[\",\\n]/.test(text)) {
              return `"${text.replace(/\"/g, '""')}"`
            }
            return text
          })
          .join(",")
      )
      .join("\\n")
    const blob = new Blob([`${header}\\n${body}`], { type: "text/csv;charset=utf-8;" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "results.csv"
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleResetConversation = () => {
    setMessages([])
    setResponse(null)
    setRunResult(null)
    setVisualizationResult(null)
    setVisualizationError(null)
    setShowResults(false)
    setShowSqlPanel(false)
    setShowQueryResultPanel(false)
    setQuery("")
    setEditedSql("")
    setIsEditing(false)
    setLastQuestion("")
    setSuggestedQuestions([])
    setError(null)
    setResultTabs([])
    setActiveTabId("")
    fetch(apiUrl("/chat/history"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user: chatHistoryUser, state: null })
    }).catch(() => {})
  }

  const handleCloseTab = (tabId: string) => {
    setResultTabs((prev) => {
      const next = prev.filter((tab) => tab.id !== tabId)
      if (activeTabId === tabId) {
        const fallback = next[0]
        setActiveTabId(fallback?.id || "")
        if (!fallback) {
          setShowResults(false)
        }
      }
      return next
    })
  }

  const validation = (() => {
    if (!payload) return null
    if (mode === "demo") {
      return {
        status: "safe" as const,
        checks: [{ name: "Demo cache", passed: true, message: "캐시 결과" }]
      }
    }
    if (mode === "clarify") {
      return {
        status: "warning" as const,
        checks: [{ name: "Clarification", passed: false, message: "추가 질문 응답 대기 중" }]
      }
    }
    const checks: { name: string; passed: boolean; message: string }[] = []
    const policyChecks = Array.isArray(payload?.policy?.checks)
      ? payload.policy!.checks!
      : Array.isArray(runResult?.policy?.checks)
        ? runResult!.policy!.checks!
        : []
    if (policyChecks.length > 0) {
      const passedAll = policyChecks.every((item) => item.passed)
      checks.push({ name: "PolicyGate", passed: passedAll, message: passedAll ? "통과" : "실패" })
      for (const item of policyChecks) {
        checks.push({
          name: item.name || "Policy",
          passed: Boolean(item.passed),
          message: item.message || "",
        })
      }
    } else {
      checks.push({ name: "PolicyGate", passed: true, message: "통과" })
    }
    if (riskScore != null) {
      checks.push({
        name: "Risk score",
        passed: riskScore < 3,
        message: `${riskScore}${riskIntent ? ` (${riskIntent})` : ""}`
      })
    }
    let status: "safe" | "warning" | "danger" = "safe"
    if (checks.some((item) => !item.passed && item.name !== "Risk score")) {
      status = "danger"
    } else if ((riskScore ?? 0) >= 4) status = "danger"
    else if ((riskScore ?? 0) >= 2) status = "warning"
    return { status, checks }
  })()
  const validationStatus = validation?.status ?? "safe"
  const validationChecks = validation?.checks ?? []

  const getValidationIcon = (status: string) => {
    switch (status) {
      case "safe": return <CheckCircle2 className="w-4 h-4 text-primary" />
      case "warning": return <AlertTriangle className="w-4 h-4 text-yellow-500" />
      case "danger": return <XCircle className="w-4 h-4 text-destructive" />
      default: return null
    }
  }

  const getValidationLabel = (name: string) => {
    const labels: Record<string, string> = {
      "PolicyGate": "정책 게이트",
      "Read-only": "읽기 전용",
      "Statement type": "쿼리 타입",
      "CTE": "CTE 문법",
      "Join limit": "조인 개수 제한",
      "WHERE rule": "WHERE 규칙",
      "Table scope": "테이블 범위",
      "Risk score": "위험 점수",
      "Demo cache": "데모 캐시",
      "Clarification": "질문 명확화",
    }
    return labels[name] || name
  }

  return (
    <div className="flex flex-col h-[calc(100vh-56px)] sm:h-[calc(100vh-64px)]">
      {/* Main Content */}
      <div ref={mainContentRef} className="flex-1 min-h-0 flex flex-col lg:flex-row overflow-hidden">
        {/* Chat Panel */}
        <div
          className={cn(
            "min-h-0 flex flex-col border-border",
            shouldShowResizablePanels ? "lg:flex-none" : "flex-1"
          )}
          style={chatPanelStyle}
        >
          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            {messages.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-full text-center">
                <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center mb-4">
                  <Send className="w-6 h-6 text-primary" />
                </div>
                <h3 className="font-medium text-foreground mb-2">질문을 입력하세요</h3>
                <p className="text-sm text-muted-foreground max-w-sm">
                  예: "65세 이상 환자 코호트를 만들고 생존 곡선을 보여줘"
                </p>
                {visibleQuickQuestions.length > 0 && (
                  <div className="mt-4 flex flex-col gap-2 w-full max-w-sm">
                    {visibleQuickQuestions.map((item) => (
                      <Button
                        key={item}
                        variant="secondary"
                        size="sm"
                        onClick={() => handleQuickQuestion(item)}
                        disabled={isLoading}
                        className="text-xs w-full justify-start"
                      >
                        {item}
                      </Button>
                    ))}
                  </div>
                )}
              </div>
            ) : (
              messages.map((message, idx) => {
                const isAssistant = message.role === "assistant"
                const isLastMessage = idx === messages.length - 1
                const showSuggestions = isAssistant && isLastMessage && suggestedQuestions.length > 0
                return (
                  <div key={message.id} className={cn(
                    "flex flex-col",
                    message.role === "user" ? "items-end" : "items-start"
                  )}>
                    <div className={cn(
                      "max-w-[80%] rounded-lg p-3",
                      message.role === "user" 
                        ? "bg-primary text-primary-foreground" 
                        : "bg-secondary"
                    )}>
                      <p className="text-sm whitespace-pre-line break-words">{message.content}</p>
                      <span className="text-[10px] opacity-70 mt-1 block">
                        {message.timestamp.toLocaleTimeString()}
                      </span>
                    </div>
                    {showSuggestions && (
                      <div className="mt-2 max-w-[80%] rounded-lg border border-border/60 bg-secondary/40 p-2">
                        <div className="mb-2 flex items-center gap-1 text-[10px] text-muted-foreground">
                          <Sparkles className="h-3 w-3 text-primary" />
                          추천 질문
                        </div>
                        <div className="flex flex-wrap gap-2">
                          {suggestedQuestions.map((item) => (
                            <Button
                              key={item}
                              variant="outline"
                              size="sm"
                              onClick={() => handleQuickQuestion(item)}
                              disabled={isLoading}
                              className="h-7 rounded-full px-2.5 text-[10px] shadow-xs bg-background/80"
                            >
                              {item}
                            </Button>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )
              })
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

        {shouldShowResizablePanels && (
          <div
            role="separator"
            aria-orientation="vertical"
            aria-label="패널 크기 조정"
            aria-valuemin={30}
            aria-valuemax={70}
            aria-valuenow={Math.round(resultsPanelWidth)}
            onMouseDown={handlePanelResizeMouseDown}
            className={cn(
              "hidden lg:flex w-3 shrink-0 items-center justify-center border-x border-border/50 bg-card/30 cursor-col-resize select-none transition-colors",
              isPanelResizing && "bg-secondary/60"
            )}
          >
            <div className="h-16 w-1 rounded-full bg-border/80" />
          </div>
        )}

        {/* Results Panel */}
        {showResults && (
          <div
            className={cn(
              "min-h-0 flex flex-col overflow-hidden border-t lg:border-t-0 border-border max-h-[50vh] lg:max-h-none",
              shouldShowResizablePanels && "lg:flex-none"
            )}
            style={resultsPanelStyle}
          >
            <div className="flex-1 overflow-y-auto p-4 pb-6 space-y-4">
              {resultTabs.length > 0 && (
                <div className="flex items-center gap-2 overflow-x-auto pb-1">
                  {resultTabs.map((tab) => (
                    <button
                      key={tab.id}
                      type="button"
                      onClick={() => setActiveTabId(tab.id)}
                      className={cn(
                        "inline-flex items-center gap-2 rounded-md border px-2 py-1 text-xs whitespace-nowrap",
                        activeTabId === tab.id ? "bg-secondary border-primary/30" : "bg-background"
                      )}
                    >
                      <span className="max-w-[180px] truncate">{tab.question || "새 질문"}</span>
                      <span
                        className={cn(
                          "inline-block h-2 w-2 rounded-full",
                          tab.status === "pending" && "bg-yellow-500",
                          tab.status === "success" && "bg-primary",
                          tab.status === "error" && "bg-destructive"
                        )}
                      />
                      <span
                        onClick={(e) => {
                          e.stopPropagation()
                          handleCloseTab(tab.id)
                        }}
                        className="text-muted-foreground hover:text-foreground"
                        role="button"
                        aria-label="탭 닫기"
                      >
                        ×
                      </span>
                    </button>
                  ))}
                </div>
              )}
              <div className="flex flex-wrap items-center gap-2">
                <Button
                  variant={showSqlPanel ? "secondary" : "outline"}
                  size="sm"
                  className="h-7"
                  onClick={() => {
                    const next = !showSqlPanel
                    setShowSqlPanel(next)
                    updateActiveTab({ showSqlPanel: next })
                  }}
                >
                  {showSqlPanel ? "SQL 숨기기" : "SQL 보기"}
                </Button>
                <Button
                  variant={showQueryResultPanel ? "secondary" : "outline"}
                  size="sm"
                  className="h-7"
                  onClick={() => {
                    const next = !showQueryResultPanel
                    setShowQueryResultPanel(next)
                    updateActiveTab({ showQueryResultPanel: next })
                  }}
                >
                  {showQueryResultPanel ? "쿼리 결과 숨기기" : "쿼리 결과 보기"}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 gap-1"
                  onClick={openSaveDialog}
                  disabled={boardSaving || (!displaySql && !currentSql)}
                >
                  <BookmarkPlus className="w-3 h-3" />
                  저장하기
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 gap-1 ml-auto"
                  onClick={handleResetConversation}
                  disabled={isLoading || !hasConversation}
                >
                  <Trash2 className="w-3 h-3" />
                  대화 초기화
                </Button>
              </div>

              <Card
                className={cn(
                  "border-l-4",
                  validationStatus === "safe" && "border-l-primary",
                  validationStatus === "warning" && "border-l-yellow-500",
                  validationStatus === "danger" && "border-l-destructive"
                )}
              >
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <Shield className="w-4 h-4" />
                    안전 검증 결과
                    {getValidationIcon(validationStatus)}
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {validationChecks.length ? (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                      {validationChecks.map((check, idx) => (
                        <div key={idx} className="flex items-start gap-2 text-xs">
                          {check.passed ? (
                            <CheckCircle2 className="w-3 h-3 text-primary mt-0.5 shrink-0" />
                          ) : (
                            <XCircle className="w-3 h-3 text-destructive mt-0.5 shrink-0" />
                          )}
                          <span className="text-muted-foreground shrink-0 whitespace-nowrap">{getValidationLabel(check.name)}:</span>
                          <span className="text-foreground break-words">{check.message}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-xs text-muted-foreground">검증 정보가 아직 없습니다.</div>
                  )}
                </CardContent>
              </Card>

              {showSqlPanel && (
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-sm">생성된 SQL</CardTitle>
                    <div className="flex items-center gap-2">
                      {mode === "advanced" && (
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 gap-1"
                          onClick={() => handleExecuteEdited()}
                          disabled={isLoading || !displaySql}
                        >
                          {isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
                          실행
                        </Button>
                      )}
                      <Button variant="ghost" size="sm" className="h-7 gap-1" onClick={handleCopySql}>
                        <Copy className="w-3 h-3" />
                        복사
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 gap-1"
                        onClick={() => {
                          const next = !isEditing
                          setIsEditing(next)
                          updateActiveTab({ isEditing: next })
                        }}
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
                        onChange={(e) => {
                          setEditedSql(e.target.value)
                          updateActiveTab({ editedSql: e.target.value })
                        }}
                        className="font-mono text-xs min-h-[200px] bg-secondary/50"
                      />
                      <div className="flex items-center gap-2">
                        <Button size="sm" onClick={() => handleExecuteEdited(editedSql)} disabled={isLoading || !editedSql.trim()} className="gap-1">
                          {isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
                          검증 후 실행
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            setEditedSql(currentSql)
                            updateActiveTab({ editedSql: currentSql })
                          }}
                        >
                          <RefreshCw className="w-3 h-3 mr-1" />
                          초기화
                        </Button>
                      </div>
                      <p className="text-[10px] text-muted-foreground flex items-center gap-1">
                        <AlertTriangle className="w-3 h-3" />
                        수정한 SQL은 검증을 통과해야 실행됩니다.
                      </p>
                    </div>
                  ) : (
                    <div
                      ref={sqlScrollRef}
                      onMouseDown={handleSqlMouseDown}
                      className={cn(
                        "p-4 pb-6 pr-6 rounded-xl bg-secondary/50 border border-border/60 text-[13px] font-mono leading-7 text-foreground overflow-x-auto overflow-y-visible [scrollbar-gutter:stable]",
                        "cursor-grab select-none",
                        isSqlDragging && "cursor-grabbing"
                      )}
                    >
                      {formattedDisplaySql ? (
                        <pre className="w-max min-w-full whitespace-pre pb-1">
                          <code dangerouslySetInnerHTML={{ __html: highlightedDisplaySql }} />
                        </pre>
                      ) : (
                        "SQL이 아직 생성되지 않았습니다."
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
              )}

              {showQueryResultPanel && (
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <CardTitle className="text-sm">쿼리 결과</CardTitle>
                      {mode && (
                        <Badge variant="outline" className="text-[10px] uppercase">
                          {mode}
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary" className="text-xs">
                        {previewColumns.length ? `${previewRowCount} rows` : "no results"}
                      </Badge>
                      {previewRowCap != null && (
                        <Badge variant="outline" className="text-[10px]">
                          cap {previewRowCap}
                        </Badge>
                      )}
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-7 gap-1"
                        onClick={openSaveDialog}
                        disabled={boardSaving || (!displaySql && !currentSql)}
                      >
                        <BookmarkPlus className="w-3 h-3" />
                        결과 보드 저장
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 gap-1"
                        onClick={handleDownloadCsv}
                        disabled={!previewColumns.length}
                      >
                        <Download className="w-3 h-3" />
                        CSV
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {boardMessage && (
                    <div className="mb-3 rounded-lg border border-border bg-secondary/40 px-3 py-2 text-xs text-muted-foreground">
                      {boardMessage}
                    </div>
                  )}
                  {error && (
                    <div className="mb-3 rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                      {error}
                    </div>
                  )}
                  {previewColumns.length ? (
                    <div className="rounded-lg border border-border overflow-hidden">
                      <table className="w-full text-xs">
                        <thead className="bg-secondary/50">
                          <tr>
                            {previewColumns.map((col) => (
                              <th key={col} className="text-left p-2 font-medium">
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {previewRows.slice(0, 10).map((row, idx) => (
                            <tr key={idx} className="border-t border-border hover:bg-secondary/30">
                              {previewColumns.map((_, colIdx) => {
                                const cell = row[colIdx]
                                const text = cell == null ? "" : String(cell)
                                return (
                                  <td key={`${idx}-${colIdx}`} className="p-2 font-mono">
                                    {text}
                                  </td>
                                )
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="flex flex-col items-center justify-center gap-3 rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      <div>결과가 없습니다.</div>
                      {mode === "advanced" && displaySql && (
                        <Button size="sm" onClick={() => handleExecuteEdited()} disabled={isLoading}>
                          {isLoading ? <Loader2 className="w-3 h-3 mr-1 animate-spin" /> : <Play className="w-3 h-3 mr-1" />}
                          실행
                        </Button>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
              )}

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">시각화 차트</CardTitle>
                  <CardDescription className="text-xs">결과 테이블 기반 시각화</CardDescription>
                </CardHeader>
                <CardContent>
                  {visualizationLoading ? (
                    <div className="rounded-lg border border-border p-6 text-xs text-muted-foreground">
                      시각화 추천 플랜을 생성 중입니다...
                    </div>
                  ) : visualizationError ? (
                    <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-6 text-xs text-destructive">
                      {visualizationError}
                    </div>
                  ) : previewColumns.length === 0 ? (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      결과 테이블이 없어 차트를 표시할 수 없습니다.
                    </div>
                  ) : recommendedFigure ? (
                    <div className="space-y-3">
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">
                          {String(recommendedAnalysis?.chart_spec?.chart_type || "plotly").toUpperCase()}
                        </Badge>
                        {recommendedAnalysis?.chart_spec?.x && (
                          <Badge variant="secondary">X: {recommendedAnalysis.chart_spec.x}</Badge>
                        )}
                        {recommendedAnalysis?.chart_spec?.y && (
                          <Badge variant="secondary">Y: {recommendedAnalysis.chart_spec.y}</Badge>
                        )}
                      </div>
                      <div className="h-[380px] w-full rounded-lg border border-border p-2">
                        <Plot
                          data={Array.isArray(recommendedFigure.data) ? recommendedFigure.data : []}
                          layout={{
                            autosize: true,
                            margin: { l: 48, r: 16, t: 16, b: 48 },
                            paper_bgcolor: "transparent",
                            plot_bgcolor: "transparent",
                            ...(recommendedFigure.layout || {}),
                          }}
                          config={{ responsive: true, displaylogo: false }}
                          style={{ width: "100%", height: "100%" }}
                        />
                      </div>
                      {(recommendedAnalysis?.reason || recommendedAnalysis?.summary) && (
                        <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs text-muted-foreground space-y-1">
                          {recommendedAnalysis?.reason && <p>추천 이유: {normalizeInsightText(recommendedAnalysis.reason)}</p>}
                          {recommendedAnalysis?.summary && <p>요약: {normalizeInsightText(recommendedAnalysis.summary)}</p>}
                        </div>
                      )}
                    </div>
                  ) : recommendedChart?.type === "scatter" ? (
                    <div className="space-y-3">
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">SCATTER</Badge>
                        <Badge variant="secondary">X: {recommendedChart.xKey}</Badge>
                        <Badge variant="secondary">Y: {recommendedChart.yKey}</Badge>
                      </div>
                      <div className="h-[340px] w-full rounded-lg border border-border p-3">
                        <ResponsiveContainer width="100%" height="100%">
                          <ScatterChart>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis type="number" dataKey="x" tick={{ fontSize: 12 }} />
                            <YAxis type="number" dataKey="y" tick={{ fontSize: 12 }} />
                            <Tooltip />
                            <Legend />
                            <Scatter data={recommendedChart.data} fill="#3b82f6" name={recommendedChart.yKey} />
                          </ScatterChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  ) : recommendedChart ? (
                    <div className="space-y-3">
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">{recommendedChart.type.toUpperCase()}</Badge>
                        <Badge variant="secondary">X: {recommendedChart.xKey}</Badge>
                        <Badge variant="secondary">Y: {recommendedChart.yKey}</Badge>
                      </div>
                      <div className="h-[340px] w-full rounded-lg border border-border p-3">
                        <ResponsiveContainer width="100%" height="100%">
                          {recommendedChart.type === "line" ? (
                            <LineChart data={recommendedChart.data}>
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis dataKey="x" tick={{ fontSize: 12 }} />
                              <YAxis tick={{ fontSize: 12 }} />
                              <Tooltip />
                              <Legend />
                              <Line type="monotone" dataKey="y" stroke="#10b981" strokeWidth={2} dot={false} />
                            </LineChart>
                          ) : (
                            <BarChart data={recommendedChart.data}>
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis dataKey="x" tick={{ fontSize: 12 }} />
                              <YAxis tick={{ fontSize: 12 }} />
                              <Tooltip />
                              <Legend />
                              <Bar dataKey="y" fill="#3b82f6" />
                            </BarChart>
                          )}
                        </ResponsiveContainer>
                      </div>
                      {(recommendedAnalysis?.reason || recommendedAnalysis?.summary) && (
                        <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs text-muted-foreground space-y-1">
                          {recommendedAnalysis?.reason && <p>추천 이유: {recommendedAnalysis.reason}</p>}
                          {recommendedAnalysis?.summary && <p>요약: {recommendedAnalysis.summary}</p>}
                        </div>
                      )}
                    </div>
                  ) : survivalChartData?.length ? (
                    <SurvivalChart
                      data={survivalChartData}
                      medianSurvival={medianSurvival}
                      totalPatients={totalPatients}
                      totalEvents={totalEvents}
                    />
                  ) : (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      현재 결과로 생성 가능한 차트가 없습니다. 시간/이벤트 컬럼이 포함되면 생존 차트를 표시합니다.
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">통계 자료</CardTitle>
                  <CardDescription className="text-xs">컬럼별 MIN, Q1, 중앙값, Q3, MAX, 평균, 결측치, NULL 개수</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  {previewColumns.length ? (
                    <div className="space-y-4">
                      <div className="rounded-lg border border-border overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead className="bg-secondary/50">
                            <tr>
                              <th className="text-left p-2 font-medium">컬럼</th>
                              <th className="text-right p-2 font-medium">N</th>
                              <th className="text-right p-2 font-medium">결측치</th>
                              <th className="text-right p-2 font-medium">NULL</th>
                              <th className="text-right p-2 font-medium">MIN</th>
                              <th className="text-right p-2 font-medium">Q1</th>
                              <th className="text-right p-2 font-medium">중앙값</th>
                              <th className="text-right p-2 font-medium">Q3</th>
                              <th className="text-right p-2 font-medium">MAX</th>
                              <th className="text-right p-2 font-medium">평균</th>
                            </tr>
                          </thead>
                          <tbody>
                            {statsRows.map((row) => (
                              <tr key={row.column} className="border-t border-border">
                                <td className="p-2 font-medium">{row.column}</td>
                                <td className="p-2 text-right">{row.count}</td>
                                <td className="p-2 text-right">{row.missingCount}</td>
                                <td className="p-2 text-right">{row.nullCount}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.min)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.q1)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.median)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.q3)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.max)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.avg)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>

                      {boxPlotRows.length ? (
                        <div className="rounded-lg border border-border p-3">
                          <div className="mb-2 text-xs text-muted-foreground">박스플롯 (컬럼별 분포)</div>
                          <div className="h-[320px] w-full">
                            <ResponsiveContainer width="100%" height="100%">
                              <ComposedChart data={boxPlotRows}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="column" tick={{ fontSize: 12 }} />
                                <YAxis tick={{ fontSize: 12 }} domain={boxPlotYDomain} allowDataOverflow />
                                <Tooltip />
                                <Legend />
                                <Bar dataKey="iqrBase" stackId="box" fill="transparent" legendType="none" />
                                <Bar dataKey="iqr" stackId="box" name="IQR (Q1~Q3)" fill="#60a5fa" stroke="#3b82f6" />
                                <Line type="linear" dataKey="whiskerLow" name="MIN (whisker)" stroke="#64748b" dot={false} />
                                <Line type="linear" dataKey="whiskerHigh" name="MAX (whisker)" stroke="#64748b" dot={false} />
                                <Scatter dataKey="median" name="중앙값" fill="#ef4444" />
                                <Scatter dataKey="outlierLow" name="하위 이상치" fill="#f59e0b" />
                                <Scatter dataKey="outlierHigh" name="상위 이상치" fill="#f59e0b" />
                              </ComposedChart>
                            </ResponsiveContainer>
                          </div>
                        </div>
                      ) : (
                        <div className="rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
                          수치형 컬럼이 없어 박스플롯을 생성할 수 없습니다.
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      결과가 없어 통계 자료를 표시할 수 없습니다.
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">해석</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="p-4 rounded-lg bg-primary/10 border border-primary/30">
                    <h4 className="font-medium text-foreground mb-2">데이터 분석 인사이트</h4>
                    <p className="text-sm text-muted-foreground whitespace-pre-line">{integratedInsight}</p>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        )}
      </div>

      <Dialog open={isSaveDialogOpen} onOpenChange={setIsSaveDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>결과 보드에 저장</DialogTitle>
            <DialogDescription>저장 이름과 폴더를 선택하세요.</DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">저장 이름</label>
              <Input
                value={saveTitle}
                onChange={(e) => setSaveTitle(e.target.value)}
                placeholder="예: 성별 입원 건수 비교"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">저장 폴더</label>
              <Select
                value={saveFolderMode}
                onValueChange={(value) => setSaveFolderMode(value as "existing" | "new")}
              >
                <SelectTrigger>
                  <SelectValue placeholder="폴더 방식 선택" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="existing">기존 폴더 선택</SelectItem>
                  <SelectItem value="new">새 폴더 만들기</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {saveFolderMode === "existing" ? (
              <div className="space-y-2">
                <label className="text-sm font-medium">기존 폴더</label>
                <Select value={saveFolderId || undefined} onValueChange={setSaveFolderId} disabled={saveFoldersLoading}>
                  <SelectTrigger>
                    <SelectValue placeholder={saveFoldersLoading ? "폴더 불러오는 중..." : "폴더 선택"} />
                  </SelectTrigger>
                  <SelectContent>
                    {saveFolderOptions.length ? (
                      saveFolderOptions.map((folder) => (
                        <SelectItem key={folder.id} value={folder.id}>
                          {folder.name}
                        </SelectItem>
                      ))
                    ) : (
                      <SelectItem value="__none__" disabled>
                        폴더가 없습니다
                      </SelectItem>
                    )}
                  </SelectContent>
                </Select>
              </div>
            ) : (
              <div className="space-y-2">
                <label className="text-sm font-medium">새 폴더 이름</label>
                <Input
                  value={saveNewFolderName}
                  onChange={(e) => setSaveNewFolderName(e.target.value)}
                  placeholder="예: 응급실 분석"
                />
              </div>
            )}

            {boardMessage && (
              <div className="text-xs text-muted-foreground">{boardMessage}</div>
            )}
          </div>

          <DialogFooter>
            <Button variant="ghost" onClick={() => setIsSaveDialogOpen(false)} disabled={boardSaving}>
              취소
            </Button>
            <Button onClick={handleSaveToDashboard} disabled={boardSaving}>
              {boardSaving ? <Loader2 className="w-3 h-3 mr-1 animate-spin" /> : null}
              저장
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

interface SimpleStatsRow {
  column: string
  count: number
  nullCount: number
  missingCount: number
  min: number | null
  q1: number | null
  median: number | null
  q3: number | null
  max: number | null
  avg: number | null
}

function buildSimpleStats(columns: string[], rows: any[][]): SimpleStatsRow[] {
  return columns.map((column, colIdx) => {
    const numbers: number[] = []
    let nullCount = 0
    let missingCount = 0

    for (const row of rows) {
      const value = row?.[colIdx]
      const isNull = value == null
      const isBlank = typeof value === "string" && value.trim() === ""
      if (isNull) {
        nullCount += 1
        missingCount += 1
        continue
      }
      if (isBlank) {
        missingCount += 1
        continue
      }
      const num = Number(value)
      if (Number.isFinite(num)) {
        numbers.push(num)
      }
    }

    if (!numbers.length) {
      return {
        column,
        count: 0,
        nullCount,
        missingCount,
        min: null,
        q1: null,
        median: null,
        q3: null,
        max: null,
        avg: null,
      }
    }

    const sorted = [...numbers].sort((a, b) => a - b)
    const q1 = quantile(sorted, 0.25)
    const median = quantile(sorted, 0.5)
    const q3 = quantile(sorted, 0.75)
    const min = Math.min(...numbers)
    const max = Math.max(...numbers)
    const avg = numbers.reduce((sum, value) => sum + value, 0) / numbers.length

    return {
      column,
      count: numbers.length,
      nullCount,
      missingCount,
      min: Number(min.toFixed(4)),
      q1: Number(q1.toFixed(4)),
      median: Number(median.toFixed(4)),
      q3: Number(q3.toFixed(4)),
      max: Number(max.toFixed(4)),
      avg: Number(avg.toFixed(4)),
    }
  })
}

function quantile(sorted: number[], q: number) {
  if (!sorted.length) return 0
  const pos = (sorted.length - 1) * q
  const base = Math.floor(pos)
  const rest = pos - base
  const next = sorted[base + 1]
  if (next === undefined) return sorted[base]
  return sorted[base] + rest * (next - sorted[base])
}

function formatStatNumber(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "-"
  return Number(value.toFixed(4)).toLocaleString()
}

function formatSqlForDisplay(sql: string) {
  if (!sql?.trim()) return sql

  let formatted = sql.replace(/\s+/g, " ").trim()

  const clausePatterns: RegExp[] = [
    /\bWITH\b/gi,
    /\bSELECT\b/gi,
    /\bFROM\b/gi,
    /\bLEFT\s+OUTER\s+JOIN\b/gi,
    /\bRIGHT\s+OUTER\s+JOIN\b/gi,
    /\bFULL\s+OUTER\s+JOIN\b/gi,
    /\bLEFT\s+JOIN\b/gi,
    /\bRIGHT\s+JOIN\b/gi,
    /\bINNER\s+JOIN\b/gi,
    /\bFULL\s+JOIN\b/gi,
    /\bJOIN\b/gi,
    /\bON\b/gi,
    /\bWHERE\b/gi,
    /\bGROUP\s+BY\b/gi,
    /\bHAVING\b/gi,
    /\bORDER\s+BY\b/gi,
    /\bUNION\s+ALL\b/gi,
    /\bUNION\b/gi,
  ]

  for (const pattern of clausePatterns) {
    formatted = formatted.replace(pattern, (match, offset) => {
      const token = match.toUpperCase().replace(/\s+/g, " ")
      return offset === 0 ? token : `\n${token}`
    })
  }

  formatted = formatted.replace(/,\s*/g, ",\n  ")
  formatted = formatted.replace(/\bCASE\b/gi, "\nCASE")
  formatted = formatted.replace(/\bWHEN\b/gi, "\n  WHEN")
  formatted = formatted.replace(/\bTHEN\b/gi, "\n    THEN")
  formatted = formatted.replace(/\bELSE\b/gi, "\n  ELSE")
  formatted = formatted.replace(/\bEND\b/gi, "\nEND")
  formatted = formatted.replace(/\s+(AND|OR)\s+/gi, (_, op) => `\n  ${String(op).toUpperCase()} `)
  return formatted.replace(/\n{3,}/g, "\n\n").trim()
}

function highlightSqlForDisplay(sql: string) {
  const formatted = formatSqlForDisplay(sql)
  if (!formatted?.trim()) return ""

  const keywordPattern =
    /\b(WITH|SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|FULL|OUTER|ON|GROUP|BY|HAVING|ORDER|UNION|ALL|DISTINCT|AS|CASE|WHEN|THEN|ELSE|END|AND|OR|IN|IS|NOT|NULL|LIKE)\b/gi
  const functionPattern =
    /\b(COUNT|SUM|AVG|MIN|MAX|CAST|COALESCE|NVL|EXTRACT|ROUND|TRUNC|TO_DATE|TO_CHAR)\b(?=\s*\()/gi

  let highlighted = escapeHtml(formatted)
  const placeholders: string[] = []

  const stash = (pattern: RegExp, className: string) => {
    highlighted = highlighted.replace(pattern, (match) => {
      const token = `__SQL_TOKEN_${placeholders.length}__`
      placeholders.push(`<span class="${className}">${match}</span>`)
      return token
    })
  }

  stash(/--[^\n]*/g, "text-muted-foreground")
  stash(/'(?:''|[^'])*'/g, "text-lime-700 dark:text-lime-400")
  stash(/"(?:[^"]|"")*"/g, "text-lime-700 dark:text-lime-400")

  highlighted = highlighted.replace(
    functionPattern,
    (match) => `<span class="text-pink-600 dark:text-pink-400 font-semibold">${match.toUpperCase()}</span>`
  )
  highlighted = highlighted.replace(
    keywordPattern,
    (match) => `<span class="text-sky-600 dark:text-sky-400 font-semibold">${match.toUpperCase()}</span>`
  )

  highlighted = highlighted.replace(/__SQL_TOKEN_(\d+)__/g, (_, idx) => {
    const token = placeholders[Number(idx)]
    return token || ""
  })

  return highlighted
}

function escapeHtml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
}
