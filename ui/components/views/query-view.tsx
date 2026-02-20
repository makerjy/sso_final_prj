"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import dynamic from "next/dynamic"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Input } from "@/components/ui/input"
import { Checkbox } from "@/components/ui/checkbox"
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
  AlertTriangle,
  Play,
  Loader2,
  Eye,
  Pencil,
  Sparkles,
  Table2,
  FileText,
  RefreshCw,
  Copy,
  Download,
  Trash2,
  BookmarkPlus
} from "lucide-react"
import { cn } from "@/lib/utils"
import { useAuth } from "@/components/auth-provider"

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
  total_count?: number | null
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
  assistant_message?: string
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

interface QueryAnswerResponse {
  answer?: string
  source?: string
  suggested_questions?: string[]
  suggestions_source?: string
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
  preferredChartType?: "line" | "bar" | "pie" | null
}

interface DashboardFolderOption {
  id: string
  name: string
}

interface CategoryTypeSummary {
  value: string
  occurrences: number
}

const MAX_PERSIST_ROWS = 200
const VIZ_CACHE_PREFIX = "viz_cache_v3:"
const VIZ_CACHE_TTL_MS = 1000 * 60 * 60 * 24
const CHART_CATEGORY_THRESHOLD = 10
const CHART_CATEGORY_DEFAULT_COUNT = 10
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
  const preservedRowCount =
    typeof preview.row_count === "number" && Number.isFinite(preview.row_count)
      ? preview.row_count
      : trimmedRows.length
  return {
    ...preview,
    rows: trimmedRows,
    row_count: preservedRowCount,
  }
}

const sanitizeRunResult = (runResult: RunResponse | null): RunResponse | null => {
  if (!runResult) return null
  return {
    ...runResult,
    result: trimPreview(runResult.result) || runResult.result,
  }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value)

const readFiniteNumber = (value: unknown): number | null => {
  const num = typeof value === "number" ? value : Number(value)
  return Number.isFinite(num) ? num : null
}

const getAxisKey = (trace: Record<string, unknown>): "x" | "y" =>
  String(trace.orientation || "").toLowerCase() === "h" ? "y" : "x"

const collectCategoryTypeSummariesFromFigure = (
  figure: { data?: unknown[]; layout?: Record<string, unknown> } | null
): CategoryTypeSummary[] => {
  if (!figure || !Array.isArray(figure.data)) return []
  const counts = new Map<string, number>()
  const order: string[] = []
  for (const rawTrace of figure.data) {
    if (!isRecord(rawTrace)) continue
    const axisKey = getAxisKey(rawTrace)
    const axisValues = rawTrace[axisKey]
    if (!Array.isArray(axisValues) || !axisValues.length) continue
    for (const rawValue of axisValues) {
      const value = String(rawValue ?? "").trim()
      if (!value) continue
      if (!counts.has(value)) order.push(value)
      counts.set(value, (counts.get(value) || 0) + 1)
    }
  }
  return order.map((value) => ({
    value,
    occurrences: counts.get(value) || 0,
  }))
}

const filterFigureByCategories = (
  figure: { data?: unknown[]; layout?: Record<string, unknown> } | null,
  selectedCategories: string[] | null
): { data?: unknown[]; layout?: Record<string, unknown> } | null => {
  if (!figure || !selectedCategories?.length || !Array.isArray(figure.data)) return figure
  const allowed = new Set(selectedCategories)
  const filteredData = figure.data.map((rawTrace) => {
    if (!isRecord(rawTrace)) return rawTrace
    const trace = { ...rawTrace } as Record<string, unknown>
    const axisKey = getAxisKey(trace)
    const axisValues = trace[axisKey]
    if (!Array.isArray(axisValues) || !axisValues.length) return trace
    const normalizedAxis = axisValues.map((value) => String(value ?? "").trim())
    const filteredIndexes: number[] = []
    for (let i = 0; i < normalizedAxis.length; i += 1) {
      if (allowed.has(normalizedAxis[i])) filteredIndexes.push(i)
    }
    if (!filteredIndexes.length) {
      trace[axisKey] = []
      const otherAxisKey = axisKey === "x" ? "y" : "x"
      if (Array.isArray(trace[otherAxisKey]) && trace[otherAxisKey].length === axisValues.length) {
        trace[otherAxisKey] = []
      }
      return trace
    }

    trace[axisKey] = filteredIndexes.map((idx) => axisValues[idx])

    const otherAxisKey = axisKey === "x" ? "y" : "x"
    const otherAxis = trace[otherAxisKey]
    if (Array.isArray(otherAxis) && otherAxis.length === axisValues.length) {
      trace[otherAxisKey] = filteredIndexes.map((idx) => otherAxis[idx])
    }

    if (Array.isArray(trace.text) && trace.text.length === axisValues.length) {
      trace.text = filteredIndexes.map((idx) => trace.text[idx])
    }

    if (Array.isArray(trace.customdata) && trace.customdata.length === axisValues.length) {
      trace.customdata = filteredIndexes.map((idx) => trace.customdata[idx])
    }

    if (isRecord(trace.marker)) {
      const marker = { ...trace.marker }
      if (Array.isArray(marker.color) && marker.color.length === axisValues.length) {
        marker.color = filteredIndexes.map((idx) => marker.color[idx])
      }
      trace.marker = marker
    }

    return trace
  })

  return {
    ...figure,
    data: filteredData,
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
      assistant_message: payload.assistant_message ? String(payload.assistant_message) : undefined,
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
  const runtimeLocalApiFallback =
    typeof window !== "undefined" &&
    (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1")
      ? "http://localhost:8001"
      : ""
  const apiBaseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || runtimeLocalApiFallback).replace(/\/$/, "")
  const apiUrl = (path: string) => (apiBaseUrl ? `${apiBaseUrl}${path}` : path)
  // Keep visualization on same-origin rewrite path to avoid bypassing /visualize proxy.
  const vizUrl = (path: string) => path
  const chatUser = (user?.name || "김연구원").trim() || "김연구원"
  const chatUserRole = (user?.role || "연구원").trim() || "연구원"
  const chatHistoryUser = (user?.id || chatUser).trim() || chatUser
  const apiUrlWithUser = (path: string) => {
    const base = apiUrl(path)
    if (!chatHistoryUser) return base
    const separator = base.includes("?") ? "&" : "?"
    return `${base}${separator}user=${encodeURIComponent(chatHistoryUser)}`
  }
  const pendingDashboardQueryKey = chatHistoryUser
    ? `ql_pending_dashboard_query:${chatHistoryUser}`
    : "ql_pending_dashboard_query"
  const ONESHOT_TIMEOUT_MS = 90_000
  const RUN_TIMEOUT_MS = 130_000
  const VISUALIZE_TIMEOUT_MS = 120_000
  const VISUALIZE_MAX_ROWS = 1200
  const ANSWER_TIMEOUT_MS = 35_000
  const ANSWER_MAX_ROWS = 120
  const ANSWER_MAX_COLS = 20
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
  const sampleRowsForVisualization = (rows: any[][], maxRows: number) => {
    if (!Array.isArray(rows) || rows.length <= maxRows) return rows
    const sampled: any[][] = []
    const step = rows.length / maxRows
    for (let i = 0; i < maxRows; i += 1) {
      const idx = Math.min(rows.length - 1, Math.floor(i * step))
      sampled.push(rows[idx])
    }
    return sampled
  }
  const buildResultSummaryMessage = (totalRows: number | null, fetchedRows: number) => {
    return totalRows != null
      ? `쿼리를 실행했어요. 전체 결과는 ${totalRows}행입니다.`
      : `쿼리를 실행했어요. 미리보기로 ${fetchedRows}행을 가져왔습니다.`
  }
  const requestQueryAnswerMessage = async ({
    questionText,
    sqlText,
    previewData,
    totalRows,
    fetchedRows,
  }: {
    questionText: string
    sqlText: string
    previewData: PreviewData | null
    totalRows: number | null
    fetchedRows: number
  }): Promise<{ answerText: string; suggestedQuestions: string[] }> => {
    const fallback = buildResultSummaryMessage(totalRows, fetchedRows)
    const fallbackSuggestions = buildSuggestions(questionText, previewData?.columns)
    const question = String(questionText || "").trim()
    if (!question || !previewData?.columns?.length) {
      return { answerText: fallback, suggestedQuestions: fallbackSuggestions }
    }
    const columns = previewData.columns.slice(0, ANSWER_MAX_COLS)
    const sampledRows = sampleRowsForVisualization(previewData.rows || [], ANSWER_MAX_ROWS)
    const rows = sampledRows.map((row) => columns.map((_, idx) => row?.[idx] ?? null))
    try {
      const res = await fetchWithTimeout(apiUrl("/query/answer"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          sql: sqlText,
          columns,
          rows,
          total_rows: totalRows,
          fetched_rows: fetchedRows,
        }),
      }, ANSWER_TIMEOUT_MS)
      if (!res.ok) return { answerText: fallback, suggestedQuestions: fallbackSuggestions }
      const data: QueryAnswerResponse = await res.json()
      const answer = String(data?.answer || "").trim()
      const suggestedQuestions = Array.isArray(data?.suggested_questions)
        ? Array.from(
            new Set(
              data.suggested_questions
                .map((item) => String(item || "").trim())
                .filter(Boolean)
            )
          ).slice(0, 3)
        : []
      return {
        answerText: answer || fallback,
        suggestedQuestions:
          suggestedQuestions.length > 0 ? suggestedQuestions : fallbackSuggestions,
      }
    } catch {
      return { answerText: fallback, suggestedQuestions: fallbackSuggestions }
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
  const [isPastQueriesDialogOpen, setIsPastQueriesDialogOpen] = useState(false)
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
  const [visibleTabLimit, setVisibleTabLimit] = useState(3)
  const [selectedStatsBoxColumn, setSelectedStatsBoxColumn] = useState<string>("")
  const [showStatsBoxPlot, setShowStatsBoxPlot] = useState(false)
  const [isChartCategoryPickerOpen, setIsChartCategoryPickerOpen] = useState(false)
  const [selectedChartCategoryValues, setSelectedChartCategoryValues] = useState<string[]>([])
  const [selectedChartCategoryCount, setSelectedChartCategoryCount] = useState<string>(String(CHART_CATEGORY_DEFAULT_COUNT))
  const saveTimerRef = useRef<number | null>(null)
  const mainContentRef = useRef<HTMLDivElement | null>(null)
  const chatScrollRef = useRef<HTMLDivElement | null>(null)
  const lastAutoScrolledMessageIdRef = useRef<string>("")
  const tabHeaderRef = useRef<HTMLDivElement | null>(null)
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
  const previewTotalCount =
    typeof preview?.total_count === "number" && Number.isFinite(preview.total_count)
      ? preview.total_count
      : null
  const effectiveTotalRows = previewTotalCount ?? previewRowCount
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
  const boxPlotEligibleRows = useMemo(
    () =>
      statsRows.filter((row) => {
        const required = [row.min, row.q1, row.median, row.q3, row.max, row.avg]
        return row.numericCount > 0 && required.every((value) => typeof value === "number" && Number.isFinite(value))
      }),
    [statsRows]
  )
  const displaySql = (isEditing ? editedSql : runResult?.sql || currentSql) || ""
  const activeTab = useMemo(
    () => resultTabs.find((item) => item.id === activeTabId) || null,
    [resultTabs, activeTabId]
  )
  const preferredDashboardChartType = activeTab?.preferredChartType || null
  const recommendedAnalysis = useMemo(() => {
    const analyses = Array.isArray(visualizationResult?.analyses) ? visualizationResult.analyses : []
    if (!analyses.length) return null
    if (preferredDashboardChartType === "pie") {
      return (
        analyses.find((item) => {
          const chartType = String(item?.chart_spec?.chart_type || "").toLowerCase()
          return chartType === "pie" || chartType === "nested_pie" || chartType === "sunburst"
        }) || analyses[0]
      )
    }
    if (preferredDashboardChartType === "line") {
      return (
        analyses.find(
          (item) => String(item?.chart_spec?.chart_type || "").toLowerCase() === "line"
        ) || analyses[0]
      )
    }
    if (preferredDashboardChartType === "bar") {
      return (
        analyses.find(
          (item) => String(item?.chart_spec?.chart_type || "").toLowerCase().startsWith("bar")
        ) || analyses[0]
      )
    }
    return analyses[0]
  }, [visualizationResult, preferredDashboardChartType])
  const recommendedFigure = useMemo(() => {
    const fig = recommendedAnalysis?.figure_json
    if (fig && typeof fig === "object") return fig as { data?: unknown[]; layout?: Record<string, unknown> }
    return null
  }, [recommendedAnalysis])
  const normalizedRecommendedLayout = useMemo<Record<string, unknown>>(() => {
    const baseLayout = isRecord(recommendedFigure?.layout) ? { ...recommendedFigure.layout } : {}
    delete baseLayout.height
    delete baseLayout.width

    const sourceMargin = isRecord(baseLayout.margin) ? baseLayout.margin : {}
    const marginLeft = readFiniteNumber(sourceMargin.l)
    const marginRight = readFiniteNumber(sourceMargin.r)
    const marginTop = readFiniteNumber(sourceMargin.t)
    const marginBottom = readFiniteNumber(sourceMargin.b)

    const xaxis = isRecord(baseLayout.xaxis)
      ? { ...baseLayout.xaxis, automargin: true }
      : { automargin: true }
    const yaxis = isRecord(baseLayout.yaxis)
      ? { ...baseLayout.yaxis, automargin: true }
      : { automargin: true }

    return {
      ...baseLayout,
      autosize: true,
      margin: {
        ...sourceMargin,
        l: Math.max(64, marginLeft ?? 0),
        r: Math.max(24, marginRight ?? 0),
        t: Math.max(20, marginTop ?? 0),
        b: Math.max(56, marginBottom ?? 0),
      },
      xaxis,
      yaxis,
      paper_bgcolor: "transparent",
      plot_bgcolor: "transparent",
    }
  }, [recommendedFigure])
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
  const fallbackChart = useMemo(() => {
    if (!previewColumns.length || !previewRecords.length) return null

    const numericColumns = previewColumns.filter((col) =>
      previewRecords.some((row) => Number.isFinite(Number(row?.[col])))
    )
    if (!numericColumns.length) return null

    const categoryColumns = previewColumns.filter((col) => !numericColumns.includes(col))
    const xKey = categoryColumns[0] || previewColumns[0]
    const yKey = numericColumns.find((col) => col !== xKey) || numericColumns[0]
    if (!xKey || !yKey) return null

    const grouped = new Map<string, { total: number; count: number }>()
    for (const row of previewRecords) {
      const key = String(row?.[xKey] ?? "")
      const yValue = Number(row?.[yKey])
      if (!Number.isFinite(yValue)) continue
      if (!grouped.has(key)) grouped.set(key, { total: 0, count: 0 })
      const item = grouped.get(key)!
      item.total += yValue
      item.count += 1
    }

    const data = Array.from(grouped.entries())
      .map(([x, value]) => ({
        x,
        y: Number((value.count ? value.total / value.count : 0).toFixed(4)),
      }))
      .filter((item) => item.x.length > 0)

    if (!data.length) return null
    return {
      type: "bar" as const,
      xKey,
      yKey,
      data: data.slice(0, 50),
    }
  }, [previewColumns, previewRecords])
  const chartForRender = recommendedChart || fallbackChart
  const localFallbackFigure = useMemo(() => {
    if (!chartForRender?.data?.length) return null

    if (chartForRender.type === "scatter") {
      return {
        data: [
          {
            type: "scatter",
            mode: "markers",
            x: chartForRender.data.map((p) => p.x),
            y: chartForRender.data.map((p) => p.y),
            name: chartForRender.yKey,
            marker: { color: "#3b82f6", size: 8, opacity: 0.85 },
          },
        ],
        layout: {
          autosize: true,
          margin: { l: 52, r: 24, t: 20, b: 56 },
          xaxis: { title: chartForRender.xKey },
          yaxis: { title: chartForRender.yKey },
          paper_bgcolor: "transparent",
          plot_bgcolor: "transparent",
        },
      }
    }

    return {
      data: [
        {
          type: chartForRender.type === "line" ? "scatter" : "bar",
          mode: chartForRender.type === "line" ? "lines+markers" : undefined,
          x: chartForRender.data.map((p) => p.x),
          y: chartForRender.data.map((p) => p.y),
          name: chartForRender.yKey,
          marker: { color: chartForRender.type === "line" ? "#10b981" : "#3b82f6" },
          line: chartForRender.type === "line" ? { color: "#10b981", width: 2 } : undefined,
          text: chartForRender.type === "bar" ? chartForRender.data.map((p) => p.y) : undefined,
          textposition: chartForRender.type === "bar" ? "outside" : undefined,
        },
      ],
      layout: {
        autosize: true,
        margin: { l: 52, r: 24, t: 20, b: 56 },
        xaxis: { title: chartForRender.xKey },
        yaxis: { title: chartForRender.yKey },
        paper_bgcolor: "transparent",
        plot_bgcolor: "transparent",
      },
    }
  }, [chartForRender])
  const activeVisualizationFigure = recommendedFigure || localFallbackFigure
  const chartCategorySummaries = useMemo(
    () => collectCategoryTypeSummariesFromFigure(activeVisualizationFigure),
    [activeVisualizationFigure]
  )
  const chartCategories = useMemo(
    () => chartCategorySummaries.map((item) => item.value),
    [chartCategorySummaries]
  )
  const isChartCategoryPickerEnabled = chartCategories.length > CHART_CATEGORY_THRESHOLD
  const chartCategoryCountOptions = useMemo(() => {
    if (!isChartCategoryPickerEnabled) return []
    const total = chartCategories.length
    const presetCounts = Array.from(new Set([10, 20, 30, 50].filter((count) => count < total)))
    return [
      ...presetCounts.map((count) => ({ value: String(count), label: `${count}개` })),
      { value: "all", label: `전체 (${total})` },
    ]
  }, [isChartCategoryPickerEnabled, chartCategories])
  const applyChartCategoryCountSelection = (nextValue: string) => {
    setSelectedChartCategoryCount(nextValue)
    if (!isChartCategoryPickerEnabled) return
    if (nextValue === "all") {
      setSelectedChartCategoryValues(chartCategories)
      return
    }
    const parsed = Number(nextValue)
    const count = Number.isFinite(parsed)
      ? Math.max(1, Math.min(chartCategories.length, parsed))
      : Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length)
    setSelectedChartCategoryValues(chartCategories.slice(0, count))
  }
  const toggleChartCategoryValue = (category: string, checked: boolean) => {
    if (!isChartCategoryPickerEnabled) return
    setSelectedChartCategoryValues((previous) => {
      const fallback = chartCategories.slice(0, Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length))
      const base = previous.length ? previous : fallback
      const nextSet = new Set(base)
      if (checked) {
        nextSet.add(category)
      } else {
        if (nextSet.size === 1 && nextSet.has(category)) return base
        nextSet.delete(category)
      }
      return chartCategories.filter((value) => nextSet.has(value))
    })
  }
  useEffect(() => {
    if (!isChartCategoryPickerEnabled) {
      if (selectedChartCategoryValues.length) setSelectedChartCategoryValues([])
      if (selectedChartCategoryCount !== String(CHART_CATEGORY_DEFAULT_COUNT)) {
        setSelectedChartCategoryCount(String(CHART_CATEGORY_DEFAULT_COUNT))
      }
      if (isChartCategoryPickerOpen) setIsChartCategoryPickerOpen(false)
      return
    }
    const validSet = new Set(chartCategories)
    const normalizedSelected = selectedChartCategoryValues.filter((value) => validSet.has(value))
    if (normalizedSelected.length !== selectedChartCategoryValues.length) {
      setSelectedChartCategoryValues(normalizedSelected)
      return
    }
    if (!normalizedSelected.length) {
      const initialCount = Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length)
      setSelectedChartCategoryValues(chartCategories.slice(0, initialCount))
      if (selectedChartCategoryCount !== String(initialCount)) {
        setSelectedChartCategoryCount(String(initialCount))
      }
      return
    }
    if (
      selectedChartCategoryCount !== "all" &&
      !chartCategoryCountOptions.some((option) => option.value === selectedChartCategoryCount)
    ) {
      setSelectedChartCategoryCount(String(Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length)))
    }
  }, [
    isChartCategoryPickerEnabled,
    chartCategories,
    selectedChartCategoryValues,
    selectedChartCategoryCount,
    chartCategoryCountOptions,
    isChartCategoryPickerOpen,
  ])
  const effectiveSelectedChartCategories = useMemo(() => {
    if (!isChartCategoryPickerEnabled) return null
    if (selectedChartCategoryValues.length) return selectedChartCategoryValues
    return chartCategories.slice(0, Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length))
  }, [isChartCategoryPickerEnabled, selectedChartCategoryValues, chartCategories])
  const selectedChartCategorySet = useMemo(
    () => new Set(effectiveSelectedChartCategories || []),
    [effectiveSelectedChartCategories]
  )
  const filteredRecommendedFigure = useMemo(
    () => filterFigureByCategories(recommendedFigure, effectiveSelectedChartCategories),
    [recommendedFigure, effectiveSelectedChartCategories]
  )
  const filteredLocalFallbackFigure = useMemo(
    () => filterFigureByCategories(localFallbackFigure, effectiveSelectedChartCategories),
    [localFallbackFigure, effectiveSelectedChartCategories]
  )

  const survivalFigure = useMemo(() => {
    if (!survivalChartData?.length) return null
    const sorted = [...survivalChartData].sort((a, b) => a.time - b.time)
    return {
      data: [
        {
          type: "scatter",
          mode: "lines",
          x: sorted.map((d) => d.time),
          y: sorted.map((d) => d.upperCI),
          line: { width: 0 },
          hoverinfo: "skip",
          showlegend: false,
          name: "Upper CI",
        },
        {
          type: "scatter",
          mode: "lines",
          x: sorted.map((d) => d.time),
          y: sorted.map((d) => d.lowerCI),
          fill: "tonexty",
          fillcolor: "rgba(62,207,142,0.18)",
          line: { width: 0 },
          hoverinfo: "skip",
          showlegend: false,
          name: "Lower CI",
        },
        {
          type: "scatter",
          mode: "lines+markers",
          x: sorted.map((d) => d.time),
          y: sorted.map((d) => d.survival),
          name: "Survival",
          line: { color: "#3ecf8e", width: 2, shape: "hv" },
          marker: { size: 5 },
          hovertemplate: "time=%{x}<br>survival=%{y:.2f}%<extra></extra>",
        },
      ],
      layout: {
        autosize: true,
        margin: { l: 56, r: 24, t: 22, b: 56 },
        xaxis: { title: "Time" },
        yaxis: { title: "Survival (%)", range: [0, 100] },
        shapes: [
          {
            type: "line",
            x0: Math.min(...sorted.map((d) => d.time)),
            x1: Math.max(...sorted.map((d) => d.time)),
            y0: 50,
            y1: 50,
            line: { color: "#94a3b8", width: 1, dash: "dash" },
          },
          {
            type: "line",
            x0: medianSurvival,
            x1: medianSurvival,
            y0: 0,
            y1: 100,
            line: { color: "#10b981", width: 1, dash: "dash" },
          },
        ],
        annotations: [
          {
            x: 0.99,
            y: 0.02,
            xref: "paper",
            yref: "paper",
            showarrow: false,
            text: `N=${totalPatients}, Events=${totalEvents}, Median=${medianSurvival.toFixed(2)}`,
            font: { size: 11, color: "#64748b" },
            xanchor: "right",
            yanchor: "bottom",
          },
        ],
        paper_bgcolor: "transparent",
        plot_bgcolor: "transparent",
      },
    }
  }, [survivalChartData, medianSurvival, totalPatients, totalEvents])

  const statsBoxPlotFigures = useMemo<Array<{ column: string; figure: { data: unknown[]; layout: Record<string, unknown> } }>>(() => {
    if (!previewRecords.length || !boxPlotEligibleRows.length) return []
    const figures: Array<{ column: string; figure: { data: unknown[]; layout: Record<string, unknown> } }> = []
    for (const row of boxPlotEligibleRows) {
      const values = previewRecords
        .map((record) => Number(record?.[row.column]))
        .filter((value) => Number.isFinite(value))
      if (!values.length) continue
      figures.push({
        column: row.column,
        figure: {
          data: [
            {
              type: "box",
              name: row.column,
              y: values,
              boxpoints: "outliers",
              marker: { color: "#60a5fa", opacity: 0.72 },
              line: { color: "#3b82f6" },
            },
          ],
          layout: {
            autosize: true,
            margin: { l: 52, r: 20, t: 10, b: 28 },
            yaxis: { title: row.column, automargin: true },
            xaxis: { title: "", showticklabels: false, automargin: true },
            paper_bgcolor: "transparent",
            plot_bgcolor: "transparent",
            showlegend: false,
          },
        },
      })
    }
    return figures
  }, [previewRecords, boxPlotEligibleRows])
  const selectedStatsBoxPlot = useMemo(() => {
    if (!statsBoxPlotFigures.length) return null
    return (
      statsBoxPlotFigures.find((item) => item.column === selectedStatsBoxColumn) ||
      statsBoxPlotFigures[0]
    )
  }, [statsBoxPlotFigures, selectedStatsBoxColumn])
  useEffect(() => {
    if (!statsBoxPlotFigures.length) {
      if (selectedStatsBoxColumn) setSelectedStatsBoxColumn("")
      if (showStatsBoxPlot) setShowStatsBoxPlot(false)
      return
    }
    const exists = statsBoxPlotFigures.some((item) => item.column === selectedStatsBoxColumn)
    if (!exists) setSelectedStatsBoxColumn(statsBoxPlotFigures[0].column)
  }, [statsBoxPlotFigures, selectedStatsBoxColumn, showStatsBoxPlot])
  const resultInterpretation = useMemo(() => {
    if (summary) return summary
    if (!previewColumns.length) return "쿼리 결과가 없어 해석을 생성할 수 없습니다."
    const numericCols = statsRows.filter((row) => row.numericCount > 0)
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
    const numeric = statsRows.filter((row) => row.numericCount > 0)
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

  const splitInsightIntoPoints = (text: string): string[] => {
    const normalized = String(text || "")
      .replace(/\r/g, "")
      .replace(/[ \t]+/g, " ")
      .replace(/\n{2,}/g, "\n")
      .trim()
    if (!normalized) return []

    const rawLines = normalized
      .split("\n")
      .map((line) => line.replace(/^[\-\*\u2022\u25CF\u25E6]\s*/, "").trim())
      .filter(Boolean)

    const points: string[] = []
    const seen = new Set<string>()

    const pushPoint = (value: string) => {
      const cleaned = value.replace(/\s+/g, " ").trim()
      if (!cleaned) return
      if (/^[\)\]\}\.,;:!?]+$/.test(cleaned)) return
      const key = cleaned.toLowerCase()
      if (seen.has(key)) return
      seen.add(key)
      points.push(cleaned)
    }

    for (const line of rawLines) {
      const sentenceParts =
        line
          .match(/[^.!?]+[.!?]?/g)
          ?.map((part) => part.trim())
          .filter(Boolean) || []

      if (sentenceParts.length > 1) {
        sentenceParts.forEach(pushPoint)
        continue
      }

      if (line.length > 110 && line.includes(",")) {
        line
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean)
          .forEach(pushPoint)
        continue
      }

      pushPoint(line)
    }

    return points.slice(0, 6)
  }

  const integratedInsight = useMemo(() => {
    const serverInsight = normalizeInsightText(
      String(visualizationResult?.insight || activeTab?.insight || "").trim()
    )
    if (serverInsight) return serverInsight
    if (visualizationLoading) {
      return "시각화 LLM이 SQL과 쿼리 결과를 기반으로 인사이트를 생성 중입니다."
    }
    if (visualizationError) {
      return `시각화 LLM 인사이트 생성에 실패했습니다. (${visualizationError})`
    }
    return "시각화 LLM 인사이트가 아직 없습니다. 쿼리를 다시 실행하거나 시각화를 새로고침해 주세요."
  }, [visualizationResult, activeTab?.insight, visualizationLoading, visualizationError])
  const insightPoints = useMemo(() => splitInsightIntoPoints(integratedInsight), [integratedInsight])
  const insightHeadline = insightPoints[0] || integratedInsight
  const formattedDisplaySql = useMemo(() => formatSqlForDisplay(displaySql), [displaySql])
  const highlightedDisplaySql = useMemo(() => highlightSqlForDisplay(displaySql), [displaySql])
  const visibleQuickQuestions = quickQuestions.slice(0, 3)
  const latestVisibleTabs = useMemo(
    () => resultTabs.slice(0, visibleTabLimit),
    [resultTabs, visibleTabLimit]
  )
  const pastQueryTabs = useMemo(
    () => resultTabs.slice(visibleTabLimit),
    [resultTabs, visibleTabLimit]
  )
  const compactTabLabel = (text: string, maxChars = 10) => {
    const normalized = String(text || "").trim() || "새 질문"
    const chars = Array.from(normalized)
    if (chars.length <= maxChars) return normalized
    return `${chars.slice(0, maxChars).join("")}...`
  }
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
    preferredChartType: null,
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
      const sampledRows = sampleRowsForVisualization(previewData.rows, VISUALIZE_MAX_ROWS)
      const sampledRecords = sampledRows.map((row) =>
        Object.fromEntries(previewData.columns.map((col, idx) => [col, row?.[idx]]))
      )
      const res = await fetchWithTimeout(
        vizUrl("/visualize"),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_query: questionText || lastQuestion || "",
            sql: sqlText,
            rows: sampledRecords.length ? sampledRecords : records,
          }),
        },
        VISUALIZE_TIMEOUT_MS
      )
      if (!res.ok) throw new Error(await readError(res))
      const data: VisualizationResponsePayload = await res.json()
      if (!targetTabId || targetTabId === activeTabId) {
        setVisualizationResult(data)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: data, insight: normalizeInsightText(String(data?.insight || "")) })
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
    if (!messages.length) return
    const container = chatScrollRef.current
    if (!container) return

    const last = messages[messages.length - 1]
    const isNewMessage = last.id !== lastAutoScrolledMessageIdRef.current
    const shouldScroll = isNewMessage && (last.role === "assistant" || isLoading)
    if (!shouldScroll) return

    lastAutoScrolledMessageIdRef.current = last.id
    const rafId = window.requestAnimationFrame(() => {
      container.scrollTo({ top: container.scrollHeight, behavior: "smooth" })
    })
    return () => window.cancelAnimationFrame(rafId)
  }, [messages, isLoading])

  useEffect(() => {
    if (typeof window === "undefined") return
    const target = tabHeaderRef.current
    if (!target) return

    const recalcVisibleTabLimit = () => {
      const width = target.clientWidth || 0
      if (width <= 0) return
      const tabWidthEstimate = 118
      const historyButtonReserve = 112
      let next = Math.floor(width / tabWidthEstimate)
      next = Math.max(1, Math.min(8, next))
      if (resultTabs.length > next) {
        next = Math.floor((width - historyButtonReserve) / tabWidthEstimate)
        next = Math.max(1, Math.min(8, next))
      }
      setVisibleTabLimit(next)
    }

    recalcVisibleTabLimit()
    window.addEventListener("resize", recalcVisibleTabLimit)

    let observer: ResizeObserver | null = null
    if (typeof ResizeObserver !== "undefined") {
      observer = new ResizeObserver(() => recalcVisibleTabLimit())
      observer.observe(target)
    }
    return () => {
      window.removeEventListener("resize", recalcVisibleTabLimit)
      observer?.disconnect()
    }
  }, [resultTabs.length])

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
    return Array.from(new Set(merged)).slice(0, 3)
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
    const llmAssistantMessage = String(data.payload.assistant_message || "").trim()
    if (llmAssistantMessage) {
      return llmAssistantMessage
    }
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
        const res = await fetchWithTimeout(apiUrlWithUser("/query/demo/questions"), {}, 15000)
        if (!res.ok) return
        const data = await res.json()
        if (Array.isArray(data?.questions) && data.questions.length) {
          setQuickQuestions(data.questions.slice(0, 3))
        }
      } catch {}
    }
    loadQuestions()
  }, [chatHistoryUser])

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
          user_id: chatHistoryUser,
          user_name: chatUser,
          user_role: chatUserRole,
        })
      }, ONESHOT_TIMEOUT_MS)
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
      const suggestions: string[] = []
      setSuggestedQuestions(suggestions)
      updateTab(tab.id, {
        question: trimmed,
        sql: generatedSql,
        response: data,
        suggestedQuestions: suggestions,
        editedSql: generatedSql,
        status: "success",
      })

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
        const preview = data.payload.result?.preview || null
        const fetchedRows = Number(preview?.row_count ?? 0)
        const totalRows =
          typeof preview?.total_count === "number" && Number.isFinite(preview.total_count)
            ? preview.total_count
            : null
        const answerPayload = await requestQueryAnswerMessage({
          questionText: trimmed,
          sqlText: generatedSql.trim(),
          previewData: preview,
          totalRows,
          fetchedRows,
        })
        const llmSuggestions = answerPayload.suggestedQuestions
        setSuggestedQuestions(llmSuggestions)
        updateTab(tab.id, {
          resultData: preview,
          suggestedQuestions: llmSuggestions,
          visualization: null,
          insight: "",
        })
        void fetchVisualizationPlan(
          generatedSql.trim(),
          trimmed,
          preview,
          tab.id
        )
          const responseMessage: ChatMessage = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(
            answerPayload.answerText,
            llmSuggestions
          ),
          timestamp: new Date()
        }
        setMessages(prev => [...prev, responseMessage])
      } else {
        const responseMessage: ChatMessage = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(buildAssistantMessage(data), suggestions),
          timestamp: new Date()
        }
        setMessages(prev => [...prev, responseMessage])
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

  const handleCopyMessage = async (text: string) => {
    const trimmed = text.trim()
    if (!trimmed) return
    try {
      await navigator.clipboard.writeText(text)
    } catch {}
  }

  const handleRerunMessage = async (text: string) => {
    const trimmed = text.trim()
    if (!trimmed || isLoading) return
    await runQuery(text)
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
      const res = await fetchWithTimeout(apiUrlWithUser("/dashboard/queries"), {}, 12000)
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
      { label: "행 수", value: String(effectiveTotalRows ?? 0) },
      { label: "전체 행 수", value: previewTotalCount != null ? String(previewTotalCount) : "-" },
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
            total_count: previewTotalCount,
          }
        : undefined
    const resolvedChartType: "line" | "bar" | "pie" = (() => {
      const specType = String(recommendedAnalysis?.chart_spec?.chart_type || "").toLowerCase()
      if (specType === "line") return "line"
      if (specType === "pie" || specType === "nested_pie" || specType === "sunburst") return "pie"
      if (chartForRender?.type === "line") return "line"
      return "bar"
    })()
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
      chartType: resolvedChartType,
    }
    setBoardSaving(true)
    setBoardMessage(null)
    try {
      const saveRes = await fetchWithTimeout(apiUrl("/dashboard/saveQuery"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user: chatHistoryUser || null,
          question: finalTitle,
          sql: displaySql || currentSql,
          metadata: {
            row_count: effectiveTotalRows ?? 0,
            column_count: previewColumns.length,
            row_cap: previewRowCap ?? null,
            total_count: previewTotalCount,
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
      user_id: chatHistoryUser,
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
    }, RUN_TIMEOUT_MS)
    if (!res.ok) {
      throw new Error(await readError(res))
    }

    const data: RunResponse = await res.json()
    setRunResult(data)
    setShowResults(true)
    setIsEditing(false)
    const targetTabId = tabId || activeTabId
    const fetchedRows = Number(data.result?.row_count ?? 0)
    const totalRows =
      typeof data.result?.total_count === "number" && Number.isFinite(data.result.total_count)
        ? data.result.total_count
        : null
    const answerPromise = requestQueryAnswerMessage({
      questionText: questionForSuggestions || lastQuestion || "",
      sqlText: (data.sql || sql || "").trim(),
      previewData: data.result || null,
      totalRows,
      fetchedRows,
    })
    void fetchVisualizationPlan(
      (data.sql || sql || "").trim(),
      questionForSuggestions || lastQuestion || "",
      data.result || null,
      targetTabId
    )

    setSuggestedQuestions([])
    if (targetTabId) {
      updateTab(targetTabId, {
        sql: data.sql || sql || "",
        runResult: data,
        resultData: data.result || null,
        visualization: null,
        suggestedQuestions: [],
        insight: "",
        status: "success",
        error: null,
      })
    }

    const answerPayload = await answerPromise
    const suggestions = answerPayload.suggestedQuestions
    setSuggestedQuestions(suggestions)
    if (targetTabId) {
      updateTab(targetTabId, {
        suggestedQuestions: suggestions,
      })
    }

    if (addAssistantMessage) {
      setMessages(prev => [
        ...prev,
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(
            answerPayload.answerText,
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
      const raw =
        localStorage.getItem(pendingDashboardQueryKey) ||
        localStorage.getItem("ql_pending_dashboard_query")
      if (!raw) return
      localStorage.removeItem(pendingDashboardQueryKey)
      localStorage.removeItem("ql_pending_dashboard_query")

      let parsed: { question?: string; sql?: string; chartType?: "line" | "bar" | "pie" } | null = null
      try {
        parsed = JSON.parse(raw)
      } catch {
        return
      }
      const sqlText = String(parsed?.sql || "").trim()
      if (!sqlText) return

      const questionText = String(parsed?.question || "").trim() || "대시보드 저장 쿼리"
      const preferredChartType =
        parsed?.chartType === "line" || parsed?.chartType === "bar" || parsed?.chartType === "pie"
          ? parsed.chartType
          : null
      const tab = createResultTab(questionText)
      tab.sql = sqlText
      tab.editedSql = sqlText
      tab.showSqlPanel = true
      tab.preferredChartType = preferredChartType
      setResultTabs((prev) => [tab, ...prev])
      setActiveTabId(tab.id)
      setShowResults(true)
      setIsLoading(true)
      setError(null)
      setBoardMessage(null)
      setLastQuestion(questionText)
      setSuggestedQuestions([])

      try {
        const questionForViz =
          preferredChartType === "pie" ? `${questionText} 파이 차트로 비율 중심 시각화` : questionText
        await executeAdvancedSql({
          sql: sqlText,
          questionForSuggestions: questionForViz,
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
  }, [pendingDashboardQueryKey])

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
      .join("\r\n")
    // Use UTF-8 BOM + CRLF so Excel opens Korean text and row breaks correctly.
    const blob = new Blob([`\uFEFF${header}\r\n${body}`], { type: "text/csv;charset=utf-8;" })
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

  const handleActivateTab = (tabId: string, promote = false) => {
    setActiveTabId(tabId)
    if (!promote) return
    setResultTabs((prev) => {
      const idx = prev.findIndex((item) => item.id === tabId)
      if (idx <= 0) return prev
      const target = prev[idx]
      return [target, ...prev.slice(0, idx), ...prev.slice(idx + 1)]
    })
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
          <div ref={chatScrollRef} className="flex-1 overflow-y-auto p-4 space-y-4">
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
                const isUser = message.role === "user"
                const isAssistant = message.role === "assistant"
                const isLastMessage = idx === messages.length - 1
                const showSuggestions = isAssistant && isLastMessage && suggestedQuestions.length > 0
                const canActOnMessage = Boolean(message.content.trim())
                return (
                  <div key={message.id} className={cn(
                    "flex flex-col",
                    isUser ? "items-end" : "items-start"
                  )}>
                    {isUser ? (
                      <div className="group max-w-[85%]">
                        <div className="rounded-lg bg-primary p-3 text-primary-foreground">
                          <p className="text-sm whitespace-pre-line break-words">{message.content}</p>
                          <span className="mt-1 block text-[10px] opacity-70">
                            {message.timestamp.toLocaleTimeString()}
                          </span>
                        </div>
                        <div className="mt-1 flex items-center justify-end gap-0.5 pr-0.5 opacity-100 transition-opacity md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            className="h-5 w-5 text-muted-foreground hover:text-foreground"
                            title="복사"
                            aria-label="질문 복사"
                            onClick={() => {
                              void handleCopyMessage(message.content)
                            }}
                            disabled={!canActOnMessage}
                          >
                            <Copy className="h-2.5 w-2.5" />
                          </Button>
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            className="h-5 w-5 text-muted-foreground hover:text-foreground"
                            title="재실행"
                            aria-label="질문 재실행"
                            onClick={() => {
                              void handleRerunMessage(message.content)
                            }}
                            disabled={isLoading || !canActOnMessage}
                          >
                            <RefreshCw className="h-2.5 w-2.5" />
                          </Button>
                        </div>
                      </div>
                    ) : (
                      <div className="max-w-[80%] rounded-lg bg-secondary p-3">
                        <p className="text-sm whitespace-pre-line break-words">{message.content}</p>
                        <span className="mt-1 block text-[10px] opacity-70">
                          {message.timestamp.toLocaleTimeString()}
                        </span>
                      </div>
                    )}
                    {showSuggestions && (
                      <div className="mt-2 max-w-[80%] rounded-lg border border-border/60 bg-secondary/40 p-2">
                        <div className="mb-2 flex items-center gap-1 text-[10px] text-muted-foreground">
                          <Sparkles className="h-3 w-3 text-primary" />
                          추천 질문
                        </div>
                        <div className="flex flex-wrap gap-2">
                          {suggestedQuestions.slice(0, 3).map((item) => (
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
                <div ref={tabHeaderRef} className="flex items-center gap-2 overflow-hidden pb-1">
                  {latestVisibleTabs.map((tab) => (
                    <button
                      key={tab.id}
                      type="button"
                      onClick={() => handleActivateTab(tab.id)}
                      title={tab.question || "새 질문"}
                      className={cn(
                        "inline-flex items-center gap-1.5 rounded-md border px-2 py-1 text-[11px]",
                        activeTabId === tab.id ? "bg-secondary border-primary/30" : "bg-background"
                      )}
                    >
                      <span className="max-w-[110px] truncate">{compactTabLabel(tab.question, 10)}</span>
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
                  {pastQueryTabs.length > 0 && (
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 shrink-0"
                      onClick={() => setIsPastQueriesDialogOpen(true)}
                    >
                      이전 쿼리 {pastQueryTabs.length}개
                    </Button>
                  )}
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
                        {previewColumns.length ? `${effectiveTotalRows} total` : "no results"}
                      </Badge>
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

                      {statsBoxPlotFigures.length ? (
                        <div className="space-y-3">
                          <div
                            className={cn(
                              "gap-2",
                              isDesktopLayout
                                ? "grid min-h-8 grid-cols-[auto_1fr_auto] items-center"
                                : "flex flex-col"
                            )}
                          >
                            <div className="text-xs text-muted-foreground">
                              박스플롯 (컬럼별 개별 분포)
                            </div>
                            <div
                              className={cn(
                                isDesktopLayout
                                  ? "w-full max-w-[260px] justify-self-center"
                                  : "mx-auto w-full max-w-[260px]"
                              )}
                            >
                              {showStatsBoxPlot && statsBoxPlotFigures.length > 1 ? (
                                <div className="w-full">
                                  <Select
                                    value={selectedStatsBoxPlot?.column || undefined}
                                    onValueChange={setSelectedStatsBoxColumn}
                                  >
                                    <SelectTrigger className="relative h-8 w-full justify-end pr-8 [&_svg]:absolute [&_svg]:right-3">
                                      <SelectValue
                                        className="pointer-events-none absolute left-1/2 max-w-[calc(100%-2.5rem)] -translate-x-1/2 truncate text-center"
                                        placeholder="박스플롯 컬럼 선택"
                                      />
                                    </SelectTrigger>
                                    <SelectContent>
                                      {statsBoxPlotFigures.map((item) => (
                                        <SelectItem key={item.column} value={item.column}>
                                          {item.column}
                                        </SelectItem>
                                      ))}
                                    </SelectContent>
                                  </Select>
                                </div>
                              ) : (
                                <div className={cn(isDesktopLayout ? "h-8 w-full" : "hidden")} />
                              )}
                            </div>
                            <div className={cn(isDesktopLayout ? "justify-self-end" : "self-end")}>
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-8"
                                onClick={() => setShowStatsBoxPlot((prev) => !prev)}
                              >
                                {showStatsBoxPlot ? "박스플롯 숨기기" : "박스플롯 보기"}
                              </Button>
                            </div>
                          </div>
                          {showStatsBoxPlot && selectedStatsBoxPlot ? (
                            <div className="rounded-lg border border-border p-3">
                              <div className="mb-2 text-xs text-muted-foreground">{selectedStatsBoxPlot.column}</div>
                              <div className="h-[320px] w-full">
                                <Plot
                                  data={Array.isArray(selectedStatsBoxPlot.figure.data) ? selectedStatsBoxPlot.figure.data : []}
                                  layout={selectedStatsBoxPlot.figure.layout || {}}
                                  config={{ responsive: true, displaylogo: false, editable: false }}
                                  style={{ width: "100%", height: "100%" }}
                                />
                              </div>
                            </div>
                          ) : (
                            <div className="rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
                              박스플롯 보기를 눌러 컬럼 분포를 확인하세요.
                            </div>
                          )}
                        </div>
                      ) : (
                        <div className="rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
                          MIN, Q1, 중앙값, Q3, MAX, 평균이 모두 있는 수치형 컬럼이 없어 박스플롯을 생성할 수 없습니다.
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
                  <CardTitle className="text-sm">시각화 차트</CardTitle>
                  <CardDescription className="text-xs">결과 테이블 기반 시각화</CardDescription>
                </CardHeader>
                <CardContent>
                  {visualizationLoading ? (
                    <div className="rounded-lg border border-border p-6 text-xs text-muted-foreground">
                      시각화 추천 플랜을 생성 중입니다...
                    </div>
                  ) : previewColumns.length < 2 ? (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      결과 컬럼이 1개라 시각화를 표시할 수 없습니다. 최소 2개 컬럼이 필요합니다.
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
                      {isChartCategoryPickerEnabled && (
                        <div className="flex flex-col gap-2 rounded-lg border border-border/60 bg-secondary/20 p-2 md:flex-row md:items-center md:justify-between">
                          <div className="text-xs text-muted-foreground">
                            X축 카테고리 {chartCategories.length}개 / 선택 {(effectiveSelectedChartCategories?.length ?? chartCategories.length)}개
                          </div>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            className="h-8 w-fit"
                            onClick={() => setIsChartCategoryPickerOpen(true)}
                          >
                            종류 선택
                          </Button>
                        </div>
                      )}
                      <div className="h-[380px] w-full rounded-lg border border-border p-2 overflow-hidden">
                        <Plot
                          data={Array.isArray(filteredRecommendedFigure?.data) ? filteredRecommendedFigure.data : []}
                          layout={normalizedRecommendedLayout}
                          config={{ responsive: true, displaylogo: false, editable: true }}
                          style={{ width: "100%", height: "100%" }}
                          useResizeHandler
                        />
                      </div>
                      {(recommendedAnalysis?.reason || recommendedAnalysis?.summary) && (
                        <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs text-muted-foreground space-y-1">
                          {recommendedAnalysis?.reason && <p>추천 이유: {normalizeInsightText(recommendedAnalysis.reason)}</p>}
                          {recommendedAnalysis?.summary && <p>요약: {normalizeInsightText(recommendedAnalysis.summary)}</p>}
                        </div>
                      )}
                    </div>
                  ) : localFallbackFigure && chartForRender ? (
                    <div className="space-y-3">
                      {visualizationError && (
                        <div className="rounded-lg border border-amber-300/40 bg-amber-500/10 p-3 text-xs text-amber-700 dark:text-amber-300">
                          시각화 API 응답이 없어 로컬 폴백 차트를 표시합니다: {visualizationError}
                        </div>
                      )}
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">{chartForRender.type.toUpperCase()}</Badge>
                        <Badge variant="secondary">X: {chartForRender.xKey}</Badge>
                        <Badge variant="secondary">Y: {chartForRender.yKey}</Badge>
                        {!recommendedChart && <Badge variant="secondary">AUTO</Badge>}
                      </div>
                      {isChartCategoryPickerEnabled && (
                        <div className="flex flex-col gap-2 rounded-lg border border-border/60 bg-secondary/20 p-2 md:flex-row md:items-center md:justify-between">
                          <div className="text-xs text-muted-foreground">
                            X축 카테고리 {chartCategories.length}개 / 선택 {(effectiveSelectedChartCategories?.length ?? chartCategories.length)}개
                          </div>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            className="h-8 w-fit"
                            onClick={() => setIsChartCategoryPickerOpen(true)}
                          >
                            종류 선택
                          </Button>
                        </div>
                      )}
                      <div className="h-[340px] w-full rounded-lg border border-border p-3 overflow-hidden">
                        <Plot
                          data={Array.isArray(filteredLocalFallbackFigure?.data) ? filteredLocalFallbackFigure.data : []}
                          layout={filteredLocalFallbackFigure?.layout || {}}
                          config={{ responsive: true, displaylogo: false, editable: true }}
                          style={{ width: "100%", height: "100%" }}
                          useResizeHandler
                        />
                      </div>
                      {(recommendedAnalysis?.reason || recommendedAnalysis?.summary) && (
                        <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs text-muted-foreground space-y-1">
                          {recommendedAnalysis?.reason && <p>추천 이유: {recommendedAnalysis.reason}</p>}
                          {recommendedAnalysis?.summary && <p>요약: {recommendedAnalysis.summary}</p>}
                        </div>
                      )}
                    </div>
                  ) : survivalFigure ? (
                    <div className="space-y-3">
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">SURVIVAL (PLOTLY)</Badge>
                        <Badge variant="secondary">Median: {medianSurvival.toFixed(2)}</Badge>
                      </div>
                      <div className="h-[360px] w-full rounded-lg border border-border p-2 overflow-hidden">
                        <Plot
                          data={Array.isArray(survivalFigure.data) ? survivalFigure.data : []}
                          layout={survivalFigure.layout || {}}
                          config={{ responsive: true, displaylogo: false, editable: true }}
                          style={{ width: "100%", height: "100%" }}
                          useResizeHandler
                        />
                      </div>
                    </div>
                  ) : (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      현재 결과로 생성 가능한 차트가 없습니다. 시간/이벤트 컬럼이 포함되면 생존 차트를 표시합니다.
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">해석</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="rounded-lg border border-primary/30 bg-primary/10 p-4">
                    <div className="space-y-3">
                      <div className="rounded-md border border-primary/20 bg-background/70 px-3 py-2 text-sm font-medium leading-relaxed text-foreground">
                        {insightHeadline}
                      </div>
                      {insightPoints.length > 1 ? (
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          {insightPoints.slice(1).map((point, idx) => (
                            <li key={`insight-${idx}-${point.slice(0, 24)}`} className="flex items-start gap-2">
                              <span className="mt-0.5 inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-primary/20 text-[11px] font-semibold text-primary">
                                {idx + 1}
                              </span>
                              <span className="leading-relaxed">{point}</span>
                            </li>
                          ))}
                        </ul>
                      ) : null}
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        )}
      </div>

      <Dialog open={isChartCategoryPickerOpen} onOpenChange={setIsChartCategoryPickerOpen}>
        <DialogContent className="w-[min(96vw,980px)] max-h-[90dvh] overflow-hidden p-0 sm:max-w-[980px]">
          <div className="flex h-full max-h-[90dvh] flex-col">
            <DialogHeader className="border-b border-border/70 px-6 py-4 pr-12">
              <DialogTitle>시각화 종류 선택</DialogTitle>
              <DialogDescription>
                X축 카테고리가 많을 때 표시할 개수와 종류를 선택합니다.
              </DialogDescription>
            </DialogHeader>

            {isChartCategoryPickerEnabled ? (
              <div className="min-h-0 flex-1 overflow-y-auto px-6 py-4">
                <div className="space-y-3">
                  <div className="flex flex-col gap-2 rounded-lg border border-border/60 bg-secondary/20 p-3">
                    <div className="text-xs text-muted-foreground">
                      총 {chartCategories.length}개 / 선택 {(effectiveSelectedChartCategories?.length ?? chartCategories.length)}개
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-xs text-muted-foreground">표시 개수</span>
                      <div className="w-[160px]">
                        <Select value={selectedChartCategoryCount} onValueChange={applyChartCategoryCountSelection}>
                          <SelectTrigger className="h-8">
                            <SelectValue placeholder="표시 개수 선택" />
                          </SelectTrigger>
                          <SelectContent>
                            {chartCategoryCountOptions.map((option) => (
                              <SelectItem key={option.value} value={option.value}>
                                {option.label}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="ml-auto flex items-center gap-2">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-8"
                          onClick={() =>
                            applyChartCategoryCountSelection(
                              String(Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length))
                            )
                          }
                        >
                          기본 {Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length)}개
                        </Button>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-8"
                          onClick={() => applyChartCategoryCountSelection("all")}
                        >
                          전체 선택
                        </Button>
                      </div>
                    </div>
                  </div>

                  <div className="overflow-hidden rounded-lg border border-border">
                    {chartCategorySummaries.map((item, idx) => (
                      <label
                        key={`${item.value}-${idx}`}
                        className={cn(
                          "flex cursor-pointer items-center gap-3 px-3 py-2 text-sm hover:bg-secondary/40",
                          idx > 0 && "border-t border-border/60"
                        )}
                      >
                        <Checkbox
                          checked={selectedChartCategorySet.has(item.value)}
                          onCheckedChange={(checked) => toggleChartCategoryValue(item.value, checked === true)}
                        />
                        <span className="min-w-0 flex-1 truncate">{item.value}</span>
                        <Badge variant="outline" className="text-[10px]">
                          {item.occurrences}
                        </Badge>
                      </label>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="px-6 py-4">
                <div className="rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
                  X축 카테고리가 10개 이하라서 종류 선택 팝업이 필요하지 않습니다.
                </div>
              </div>
            )}

            <DialogFooter className="border-t border-border/70 px-6 py-4">
              <Button variant="outline" onClick={() => setIsChartCategoryPickerOpen(false)}>
                닫기
              </Button>
            </DialogFooter>
          </div>
        </DialogContent>
      </Dialog>

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

      <Dialog open={isPastQueriesDialogOpen} onOpenChange={setIsPastQueriesDialogOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>이전 쿼리 목록</DialogTitle>
            <DialogDescription> 최근 쿼리를 제외한 이전 쿼리입니다. 선택하면 상단 탭으로 이동합니다.</DialogDescription>
          </DialogHeader>

          <div className="max-h-[55vh] space-y-2 overflow-y-auto">
            {pastQueryTabs.length > 0 ? (
              pastQueryTabs.map((tab) => (
                <button
                  key={`past-${tab.id}`}
                  type="button"
                  onClick={() => {
                    handleActivateTab(tab.id, true)
                    setIsPastQueriesDialogOpen(false)
                  }}
                  className="flex w-full items-center gap-2 rounded-md border px-3 py-2 text-left text-sm hover:bg-secondary/40"
                >
                  <span
                    className={cn(
                      "inline-block h-2 w-2 rounded-full shrink-0",
                      tab.status === "pending" && "bg-yellow-500",
                      tab.status === "success" && "bg-primary",
                      tab.status === "error" && "bg-destructive"
                    )}
                  />
                  <span className="truncate">{tab.question || "새 질문"}</span>
                </button>
              ))
            ) : (
              <div className="rounded-md border border-dashed p-4 text-center text-sm text-muted-foreground">
                표시할 이전 쿼리가 없습니다.
              </div>
            )}
          </div>

          <DialogFooter>
            <Button variant="ghost" onClick={() => setIsPastQueriesDialogOpen(false)}>
              닫기
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
  numericCount: number
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
  const totalCount = rows.length
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
        count: totalCount,
        numericCount: 0,
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
      count: totalCount,
      numericCount: numbers.length,
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
