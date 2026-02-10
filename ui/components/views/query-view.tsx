"use client"

import { useState, useEffect, useMemo, useRef } from "react"
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

interface PersistedQueryState {
  query: string
  lastQuestion: string
  messages: PersistedChatMessage[]
  response: OneShotResponse | null
  runResult: RunResponse | null
  suggestedQuestions: string[]
  showResults: boolean
  editedSql: string
  isEditing: boolean
  isTechnicalMode: boolean
}

const MAX_PERSIST_ROWS = 200

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
    } finally {
      clearTimeout(timeout)
    }
  }
  const [isTechnicalMode, setIsTechnicalMode] = useState(false)
  const [query, setQuery] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [showResults, setShowResults] = useState(false)
  const [editedSql, setEditedSql] = useState("")
  const [isEditing, setIsEditing] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [response, setResponse] = useState<OneShotResponse | null>(null)
  const [runResult, setRunResult] = useState<RunResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [boardSaving, setBoardSaving] = useState(false)
  const [boardMessage, setBoardMessage] = useState<string | null>(null)
  const [lastQuestion, setLastQuestion] = useState<string>("")
  const [suggestedQuestions, setSuggestedQuestions] = useState<string[]>([])
  const [quickQuestions, setQuickQuestions] = useState<string[]>([
    "입원 환자 수를 월별로 보여줘.",
    "가장 흔한 진단 코드는 무엇인가요?",
    "ICU 재원일수가 가장 긴 환자는 누구인가요?",
  ])
  const [isHydrated, setIsHydrated] = useState(false)
  const [isSqlDragging, setIsSqlDragging] = useState(false)
  const saveTimerRef = useRef<number | null>(null)
  const sqlScrollRef = useRef<HTMLDivElement | null>(null)
  const sqlDragRef = useRef({
    active: false,
    startX: 0,
    startY: 0,
    scrollLeft: 0,
    scrollTop: 0,
  })

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
  const displaySql = (isEditing ? editedSql : runResult?.sql || currentSql) || ""
  const formattedDisplaySql = useMemo(() => formatSqlForDisplay(displaySql), [displaySql])
  const highlightedDisplaySql = useMemo(() => highlightSqlForDisplay(displaySql), [displaySql])
  const visibleQuickQuestions = quickQuestions.slice(0, 3)
  const hasConversation =
    messages.length > 0 || Boolean(response) || Boolean(runResult) || query.trim().length > 0
  const appendSuggestions = (base: string, _suggestions?: string[]) => base

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
      pushUnique("진단 최근 추이")
    } else if (normalized.includes("icu") || normalized.includes("재원") || cols.some((c) => c.includes("stay"))) {
      pushUnique("ICU 평균 재원일수")
      pushUnique("ICU 재원일수 분포")
      pushUnique("ICU 재원 상위 10명")
    } else if (normalized.includes("입원") || normalized.includes("admission")) {
      pushUnique("입원 월별 추이")
      pushUnique("진단별 입원 건수")
      pushUnique("평균 입원기간")
    }

    if (cols.some((c) => c.includes("date") || c.includes("time"))) {
      pushUnique("기간별 추이")
    }
    if (cols.some((c) => c.includes("gender"))) {
      pushUnique("성별로 나눠 보기")
    }
    if (cols.some((c) => c.includes("age"))) {
      pushUnique("연령대별로 보기")
    }

    if (suggestions.length === 0) {
      pushUnique("상위 10개 보기")
      pushUnique("최근 6개월")
      pushUnique("성별로 나눠 보기")
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
      const parts = [prompt]
      if (reason) {
        parts.push(`이유: ${reason}`)
      }
      if (options.length) {
        parts.push(`선택 예시: ${options.slice(0, 4).join(", ")}`)
      }
      if (examples.length) {
        parts.push(`답변 예: ${examples.slice(0, 2).join(" / ")}`)
      }
      return parts.join(" ")
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
      localRiskScore != null ? `위험도 ${localRiskScore}${localRiskIntent ? ` (${localRiskIntent})` : ""}로 평가됐어요.` : ""
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
        if (typeof state.editedSql === "string") setEditedSql(state.editedSql)
        if (typeof state.isEditing === "boolean") setIsEditing(state.isEditing)
        if (typeof state.isTechnicalMode === "boolean") setIsTechnicalMode(state.isTechnicalMode)
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
      editedSql,
      isEditing,
      isTechnicalMode,
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
    editedSql,
    isEditing,
    isTechnicalMode,
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

    setIsLoading(true)
    setError(null)
    setBoardMessage(null)
    setResponse(null)
    setRunResult(null)
    setEditedSql("")
    setShowResults(false)
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
      const data: OneShotResponse = await res.json()
      setResponse(data)
      if (data.payload.mode === "clarify") {
        setShowResults(false)
        setEditedSql("")
        setIsEditing(false)
        const clarificationSuggestions = buildClarificationSuggestions(data.payload)
        setSuggestedQuestions(clarificationSuggestions)
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
        })
      }
    } catch (err: any) {
      const message =
        err?.name === "AbortError"
          ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
          : err?.message || "요청에 실패했습니다."
      setError(message)
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

  const handleSaveToDashboard = async () => {
    if (!displaySql && !currentSql) {
      setBoardMessage("저장할 SQL이 없습니다.")
      return
    }
    const title = (lastQuestion || query || "저장된 쿼리").trim() || "저장된 쿼리"
    const category = deriveDashboardCategory(title)
    const metrics = [
      { label: "행 수", value: String(previewRowCount ?? 0) },
      { label: "컬럼 수", value: String(previewColumns.length) },
      { label: "ROW CAP", value: previewRowCap != null ? String(previewRowCap) : "-" },
    ]
    const newEntry = {
      id: `dashboard-${Date.now()}`,
      title,
      description: summary || "쿼리 결과 저장",
      query: displaySql || currentSql,
      lastRun: "방금 저장",
      isPinned: true,
      category,
      metrics,
      chartType: "bar",
    }
    setBoardSaving(true)
    setBoardMessage(null)
    try {
      const res = await fetchWithTimeout(apiUrl("/dashboard/queries"), {}, 15000)
      const payload = res.ok ? await res.json() : null
      const existing = Array.isArray(payload?.queries) ? payload.queries : []
      const next = [newEntry, ...existing]
      const saveRes = await fetchWithTimeout(apiUrl("/dashboard/queries"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ queries: next }),
      }, 15000)
      if (!saveRes.ok) {
        throw new Error("save failed")
      }
      setBoardMessage("결과 보드에 저장했습니다.")
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
  }: {
    qid?: string
    sql?: string
    questionForSuggestions?: string
    addAssistantMessage?: boolean
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

    const suggestions = buildSuggestions(questionForSuggestions || lastQuestion, data.result?.columns)
    setSuggestedQuestions(suggestions)

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
    try {
      const sqlToRun = (overrideSql || editedSql || currentSql).trim()
      await executeAdvancedSql({
        qid: response.qid,
        sql: sqlToRun,
        questionForSuggestions: lastQuestion,
        addAssistantMessage: true,
      })
    } catch (err: any) {
      const message =
        err?.name === "AbortError"
          ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
          : err?.message || "실행에 실패했습니다."
      setError(message)
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
    setShowResults(false)
    setQuery("")
    setEditedSql("")
    setIsEditing(false)
    setLastQuestion("")
    setSuggestedQuestions([])
    setError(null)
    fetch(apiUrl("/chat/history"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user: chatHistoryUser, state: null })
    }).catch(() => {})
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
            <Button
              variant="ghost"
              size="sm"
              className="h-7 gap-1"
              onClick={handleResetConversation}
              disabled={isLoading || !hasConversation}
            >
              <Trash2 className="w-3 h-3" />
              대화 초기화
            </Button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 min-h-0 flex flex-col lg:flex-row overflow-hidden">
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
                      <p className="text-sm">{message.content}</p>
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

        {/* Results Panel */}
        {showResults && (
          <div className="lg:w-[55%] min-h-0 flex flex-col overflow-hidden border-t lg:border-t-0 border-border max-h-[50vh] lg:max-h-none">
            <Tabs defaultValue={isTechnicalMode ? "sql" : "results"} className="flex-1 min-h-0 flex flex-col">
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
                <TabsContent value="sql" className="flex-1 min-h-0 overflow-y-auto p-4 pb-6 space-y-4 mt-0">
                  {/* Validation Panel */}
                  <Card className={cn(
                    "border-l-4",
                    validationStatus === "safe" && "border-l-primary",
                    validationStatus === "warning" && "border-l-yellow-500",
                    validationStatus === "danger" && "border-l-destructive"
                  )}>
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

                  {/* SQL Editor */}
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
                              {isLoading ? (
                                <Loader2 className="w-3 h-3 animate-spin" />
                              ) : (
                                <Play className="w-3 h-3" />
                              )}
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
                              onClick={() => handleExecuteEdited(editedSql)}
                              disabled={isLoading || !editedSql.trim()}
                              className="gap-1"
                            >
                              {isLoading ? (
                                <Loader2 className="w-3 h-3 animate-spin" />
                              ) : (
                                <Play className="w-3 h-3" />
                              )}
                              재검증 후 실행
                            </Button>
                            <Button variant="ghost" size="sm" onClick={() => setEditedSql(currentSql)}>
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
                </TabsContent>
              )}

              {/* Results Tab */}
              <TabsContent value="results" className="flex-1 overflow-y-auto p-4 mt-0">
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
                          onClick={handleSaveToDashboard}
                          disabled={boardSaving || (!displaySql && !currentSql)}
                        >
                          <BookmarkPlus className="w-3 h-3" />
                          결과 보드에 저장
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
                            {previewRows.map((row, idx) => (
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
                            {isLoading ? (
                              <Loader2 className="w-3 h-3 mr-1 animate-spin" />
                            ) : (
                              <Play className="w-3 h-3 mr-1" />
                            )}
                            실행
                          </Button>
                        )}
                      </div>
                    )}
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Chart Tab */}
              <TabsContent value="chart" className="flex-1 overflow-y-auto p-4 mt-0">
                <Card>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-sm">결과 시각화</CardTitle>
                    <CardDescription className="text-xs">실행 결과를 기반으로 시각화를 제공합니다.</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      {previewColumns.length
                        ? "현재는 자동 시각화가 연결되어 있지 않습니다. 결과 데이터를 기반으로 차트를 추가할 수 있습니다."
                        : "쿼리를 실행하면 여기에 시각화가 표시됩니다."}
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
                      <h4 className="font-medium text-foreground mb-2">요약</h4>
                      <p className="text-sm text-muted-foreground">
                        {summary || "요약 정보를 아직 받지 못했습니다. 결과 실행 후 요약을 표시할 수 있습니다."}
                      </p>
                      {source && (
                        <p className="text-xs text-muted-foreground mt-2">source: {source}</p>
                      )}
                    </div>

                    {riskScore != null && (
                      <div className="p-4 rounded-lg bg-secondary/50 border border-border">
                        <h4 className="font-medium text-foreground mb-2">위험도</h4>
                        <div className="text-sm text-muted-foreground">
                          위험 점수: <span className="text-foreground">{riskScore}</span>
                          {riskIntent ? ` (${riskIntent})` : ""}
                        </div>
                      </div>
                    )}
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
