"use client"

import { type ReactNode, useEffect, useMemo, useRef, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Pin,
  Clock,
  Share2,
  MoreHorizontal,
  Play,
  Calendar,
  Search,
  Plus,
  Star,
  StarOff,
  Copy,
  Trash2,
  BarChart3,
  PieChart,
  Activity,
  FolderOpen,
  Folder,
  FolderPlus,
  Pencil,
  Check,
  ChevronDown,
} from "lucide-react"
import { cn } from "@/lib/utils"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
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
  folderId?: string
  preview?: {
    columns: string[]
    rows: any[][]
    row_count: number
    row_cap?: number | null
  }
  metrics: { label: string; value: string; trend?: "up" | "down" }[]
  chartType: "line" | "bar" | "pie"
}

interface SavedFolder {
  id: string
  name: string
  tone?: string
  createdAt?: string
}

interface FolderCardInfo {
  id: string
  name: string
  count: number
  pinnedCount: number
  tone?: string
  editable: boolean
}

const ALL_FOLDER_ID = "__all__"
const DEFAULT_FOLDER_ID = "folder-general"

const FOLDER_TONES = ["emerald", "sky", "amber", "rose", "violet"] as const

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
    chartType: "line",
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
    chartType: "bar",
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
    chartType: "pie",
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
    chartType: "bar",
  },
]

const seedFolders: SavedFolder[] = [
  { id: "folder-survival", name: "생존분석", tone: "emerald" },
  { id: "folder-readmission", name: "재입원", tone: "sky" },
  { id: "folder-icu", name: "ICU", tone: "amber" },
  { id: "folder-er", name: "응급실", tone: "rose" },
]

const makeFolderId = () => `folder-${Date.now()}`

const normalizeName = (value: string) => value.trim().replace(/\s+/g, " ")

const nextTone = (index: number) => FOLDER_TONES[index % FOLDER_TONES.length]

function normalizeDashboardData(rawQueries: SavedQuery[], rawFolders: SavedFolder[]) {
  const folderById = new Map<string, SavedFolder>()
  const folderByName = new Map<string, SavedFolder>()
  const folders: SavedFolder[] = []
  let changed = false

  for (const raw of rawFolders) {
    const id = String(raw?.id || "").trim()
    const name = normalizeName(String(raw?.name || ""))
    if (!id || !name || folderById.has(id)) {
      changed = true
      continue
    }
    const folder: SavedFolder = {
      id,
      name,
      tone: raw?.tone ? String(raw.tone) : undefined,
      createdAt: raw?.createdAt ? String(raw.createdAt) : undefined,
    }
    folderById.set(id, folder)
    folderByName.set(name.toLowerCase(), folder)
    folders.push(folder)
  }

  const getOrCreateFolderByName = (nameInput: string) => {
    const normalized = normalizeName(nameInput || "") || "기타"
    const key = normalized.toLowerCase()
    const existing = folderByName.get(key)
    if (existing) return existing
    const created: SavedFolder = {
      id: makeFolderId(),
      name: normalized,
      tone: nextTone(folders.length),
    }
    folders.push(created)
    folderById.set(created.id, created)
    folderByName.set(key, created)
    changed = true
    return created
  }

  if (folders.length === 0) {
    for (const item of seedFolders) {
      folders.push(item)
      folderById.set(item.id, item)
      folderByName.set(item.name.toLowerCase(), item)
    }
    changed = true
  }

  const queries = rawQueries.map((raw) => {
    const currentFolderId = typeof raw.folderId === "string" ? raw.folderId.trim() : ""
    const currentFolder = currentFolderId ? folderById.get(currentFolderId) : undefined
    let targetFolder = currentFolder

    if (!targetFolder) {
      const category = normalizeName(raw.category || "")
      targetFolder = getOrCreateFolderByName(category && category !== "전체" ? category : "기타")
    }

    const nextCategory = targetFolder.name
    const nextQuery: SavedQuery = {
      ...raw,
      folderId: targetFolder.id,
      category: nextCategory,
    }

    if (raw.folderId !== nextQuery.folderId || raw.category !== nextQuery.category) {
      changed = true
    }

    return nextQuery
  })

  if (folders.length === 0) {
    folders.push({ id: DEFAULT_FOLDER_ID, name: "기타", tone: nextTone(0) })
    changed = true
  }

  return { queries, folders, changed }
}

export function DashboardView() {
  const [queries, setQueries] = useState<SavedQuery[]>([])
  const [folders, setFolders] = useState<SavedFolder[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [activeFolderId, setActiveFolderId] = useState(ALL_FOLDER_ID)
  const [openedFolderId, setOpenedFolderId] = useState<string | null>(null)
  const [isFolderDialogOpen, setIsFolderDialogOpen] = useState(false)
  const [dialogQueryId, setDialogQueryId] = useState<string | null>(null)
  const [isCreateFolderOpen, setIsCreateFolderOpen] = useState(false)
  const [createFolderName, setCreateFolderName] = useState("")
  const [isDeleteFolderOpen, setIsDeleteFolderOpen] = useState(false)
  const [deleteFolderTargetId, setDeleteFolderTargetId] = useState<string | null>(null)
  const [isRenameFolderOpen, setIsRenameFolderOpen] = useState(false)
  const [renameFolderTargetId, setRenameFolderTargetId] = useState<string | null>(null)
  const [renameFolderName, setRenameFolderName] = useState("")
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const saveTimer = useRef<number | null>(null)
  const listSectionRef = useRef<HTMLDivElement | null>(null)

  const folderMap = useMemo(() => new Map(folders.map((folder) => [folder.id, folder])), [folders])

  const persistDashboard = async (nextQueries: SavedQuery[], nextFolders: SavedFolder[], silent = false) => {
    if (!silent) {
      setSaving(true)
    }
    try {
      const res = await fetch("/dashboard/queries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ queries: nextQueries, folders: nextFolders }),
      })
      if (!res.ok && !silent) {
        setError("결과 보드 저장에 실패했습니다.")
      }
    } catch {
      if (!silent) {
        setError("결과 보드 저장에 실패했습니다.")
      }
    } finally {
      if (!silent) {
        setSaving(false)
      }
    }
  }

  const schedulePersist = (nextQueries: SavedQuery[], nextFolders: SavedFolder[]) => {
    if (saveTimer.current) {
      window.clearTimeout(saveTimer.current)
    }
    saveTimer.current = window.setTimeout(() => {
      persistDashboard(nextQueries, nextFolders, true)
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
      const remoteQueries = Array.isArray(payload?.queries) ? payload.queries : []
      const remoteFolders = Array.isArray(payload?.folders) ? payload.folders : []

      const usingFallback = remoteQueries.length === 0
      const baseQueries = usingFallback ? savedQueries : remoteQueries
      const baseFolders = usingFallback ? seedFolders : remoteFolders

      const normalized = normalizeDashboardData(baseQueries, baseFolders)
      setQueries(normalized.queries)
      setFolders(normalized.folders)

      if ((usingFallback && !payload?.detail) || normalized.changed) {
        persistDashboard(normalized.queries, normalized.folders, true)
      }
    } catch {
      const normalized = normalizeDashboardData(savedQueries, seedFolders)
      setQueries(normalized.queries)
      setFolders(normalized.folders)
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

  useEffect(() => {
    const existsActive =
      activeFolderId === ALL_FOLDER_ID || folders.some((folder) => folder.id === activeFolderId)
    if (!existsActive) {
      setActiveFolderId(ALL_FOLDER_ID)
    }
    if (openedFolderId) {
      const existsOpened = folders.some((folder) => folder.id === openedFolderId)
      if (!existsOpened) {
        setOpenedFolderId(null)
        setIsFolderDialogOpen(false)
      }
    }
  }, [folders, activeFolderId, openedFolderId])

  const updateDashboardState = (nextQueries: SavedQuery[], nextFolders: SavedFolder[]) => {
    setQueries(nextQueries)
    setFolders(nextFolders)
    schedulePersist(nextQueries, nextFolders)
  }

  const togglePin = (id: string) => {
    const nextQueries = queries.map((q) => (q.id === id ? { ...q, isPinned: !q.isPinned } : q))
    updateDashboardState(nextQueries, folders)
  }

  const handleDelete = (id: string) => {
    const nextQueries = queries.filter((q) => q.id !== id)
    updateDashboardState(nextQueries, folders)
  }

  const handleDuplicate = (id: string) => {
    const target = queries.find((q) => q.id === id)
    if (!target) return
    const nextQueries = [
      {
        ...target,
        id: `copy-${Date.now()}`,
        title: `${target.title} (복제)`,
        isPinned: false,
        lastRun: "방금 생성",
      },
      ...queries,
    ]
    updateDashboardState(nextQueries, folders)
  }

  const handleAddQuery = () => {
    const targetFolderId = activeFolderId || folders[0]?.id
    const targetFolder = folders.find((folder) => folder.id === targetFolderId) || folders[0]
    if (!targetFolder) {
      setError("폴더를 먼저 생성해주세요.")
      return
    }
    const nextQueries = [
      {
        id: `new-${Date.now()}`,
        title: "새 쿼리",
        description: "설명을 입력하세요",
        query: "",
        lastRun: "방금 생성",
        isPinned: true,
        category: targetFolder.name,
        folderId: targetFolder.id,
        metrics: [
          { label: "지표 1", value: "-" },
          { label: "지표 2", value: "-" },
          { label: "지표 3", value: "-" },
        ],
        chartType: "bar" as const,
      },
      ...queries,
    ]
    updateDashboardState(nextQueries, folders)
  }

  const handleShare = async (query: SavedQuery) => {
    try {
      await navigator.clipboard.writeText(query.query || query.title)
    } catch {
      setError("클립보드 복사에 실패했습니다.")
    }
  }

  const handleCreateFolder = (rawName: string) => {
    const name = normalizeName(rawName)
    if (!name) {
      setError("폴더 이름을 입력해주세요.")
      return
    }
    const duplicated = folders.some((folder) => folder.name.toLowerCase() === name.toLowerCase())
    if (duplicated) {
      setError("같은 이름의 폴더가 이미 있습니다.")
      return
    }

    const nextFolder: SavedFolder = {
      id: makeFolderId(),
      name,
      tone: nextTone(folders.length),
      createdAt: new Date().toISOString(),
    }
    const nextFolders = [...folders, nextFolder]
    updateDashboardState(queries, nextFolders)
    setActiveFolderId(nextFolder.id)
  }

  const handleRenameFolder = (folderId: string) => {
    const target = folders.find((folder) => folder.id === folderId)
    if (!target) return
    setRenameFolderTargetId(folderId)
    setRenameFolderName(target.name)
    setIsRenameFolderOpen(true)
  }

  const confirmRenameFolder = () => {
    const targetId = renameFolderTargetId
    if (!targetId) return
    const name = normalizeName(renameFolderName)
    if (!name) {
      setError("폴더 이름을 입력해주세요.")
      return
    }
    const duplicated = folders.some((folder) => folder.id !== targetId && folder.name.toLowerCase() === name.toLowerCase())
    if (duplicated) {
      setError("같은 이름의 폴더가 이미 있습니다.")
      return
    }

    const nextFolders = folders.map((folder) =>
      folder.id === targetId ? { ...folder, name } : folder
    )
    const nextQueries = queries.map((query) =>
      query.folderId === targetId ? { ...query, category: name } : query
    )
    updateDashboardState(nextQueries, nextFolders)
    setIsRenameFolderOpen(false)
    setRenameFolderTargetId(null)
    setRenameFolderName("")
  }

  const handleDeleteFolder = (folderId: string) => {
    const target = folders.find((folder) => folder.id === folderId)
    if (!target) return
    if (folders.length <= 1) {
      setError("마지막 폴더는 삭제할 수 없습니다.")
      return
    }
    setDeleteFolderTargetId(folderId)
    setIsDeleteFolderOpen(true)
  }

  const confirmDeleteFolder = () => {
    const targetId = deleteFolderTargetId
    if (!targetId) return
    const target = folders.find((folder) => folder.id === targetId)
    if (!target) return
    const fallbackFolder = folders.find((folder) => folder.id !== targetId) || folders[0]
    if (!fallbackFolder) {
      setError("이동할 폴더가 없습니다.")
      return
    }
    const nextFolders = folders.filter((folder) => folder.id !== targetId)
    const nextQueries = queries.map((query) =>
      query.folderId === targetId
        ? { ...query, folderId: fallbackFolder.id, category: fallbackFolder.name }
        : query
    )
    updateDashboardState(nextQueries, nextFolders)
    if (activeFolderId === targetId) {
      setActiveFolderId(fallbackFolder.id)
    }
    setIsDeleteFolderOpen(false)
    setDeleteFolderTargetId(null)
  }

  const moveQueryToFolder = (queryId: string, folderId: string) => {
    const folder = folderMap.get(folderId)
    if (!folder) return
    const nextQueries = queries.map((query) =>
      query.id === queryId
        ? {
            ...query,
            folderId,
            category: folder.name,
          }
        : query
    )
    updateDashboardState(nextQueries, folders)
  }

  const getChartIcon = (type: string) => {
    switch (type) {
      case "line":
        return <Activity className="w-4 h-4" />
      case "bar":
        return <BarChart3 className="w-4 h-4" />
      case "pie":
        return <PieChart className="w-4 h-4" />
      default:
        return <BarChart3 className="w-4 h-4" />
    }
  }

  const folderCards = useMemo<FolderCardInfo[]>(() => {
    const counts = new Map<string, { count: number; pinnedCount: number }>()
    for (const query of queries) {
      const folderId = query.folderId || DEFAULT_FOLDER_ID
      const current = counts.get(folderId) || { count: 0, pinnedCount: 0 }
      current.count += 1
      if (query.isPinned) {
        current.pinnedCount += 1
      }
      counts.set(folderId, current)
    }

    const allPinned = queries.filter((query) => query.isPinned).length
    const cards: FolderCardInfo[] = [
      {
        id: ALL_FOLDER_ID,
        name: "전체",
        count: queries.length,
        pinnedCount: allPinned,
        tone: undefined,
        editable: false,
      },
    ]
    const orderedFolders = [...folders].sort((a, b) => a.name.localeCompare(b.name, "ko"))
    for (const folder of orderedFolders) {
      const stats = counts.get(folder.id) || { count: 0, pinnedCount: 0 }
      cards.push({
        id: folder.id,
        name: folder.name,
        count: stats.count,
        pinnedCount: stats.pinnedCount,
        tone: folder.tone,
        editable: true,
      })
    }

    return cards
  }, [folders, queries])

  const normalizedSearch = searchTerm.trim().toLowerCase()
  const filteredQueries = useMemo(() => {
    return queries
      .filter((query) => {
        const matchesSearch =
          query.title.toLowerCase().includes(normalizedSearch) ||
          query.description.toLowerCase().includes(normalizedSearch)
        const matchesFolder =
          activeFolderId === ALL_FOLDER_ID || query.folderId === activeFolderId
        return matchesSearch && matchesFolder
      })
      .sort((a, b) => {
        if (a.isPinned !== b.isPinned) {
          return a.isPinned ? -1 : 1
        }
        return a.title.localeCompare(b.title, "ko")
      })
  }, [normalizedSearch, queries, activeFolderId])

  const selectedFolderName =
    activeFolderId === ALL_FOLDER_ID
      ? "전체 쿼리"
      : `${folderMap.get(activeFolderId)?.name || "알 수 없는 폴더"} 쿼리`

  const openedFolderName = openedFolderId
    ? folderMap.get(openedFolderId)?.name || "알 수 없는 폴더"
    : ""

  const dialogQueries = useMemo(() => {
    if (!openedFolderId) return []
    return queries.filter((query) => query.folderId === openedFolderId)
  }, [openedFolderId, queries])

  useEffect(() => {
    if (!isFolderDialogOpen) return
    if (!dialogQueries.length) {
      setDialogQueryId(null)
      return
    }
    if (!dialogQueryId || !dialogQueries.some((item) => item.id === dialogQueryId)) {
      setDialogQueryId(dialogQueries[0].id)
    }
  }, [dialogQueries, dialogQueryId, isFolderDialogOpen])

  const selectedDialogQuery = dialogQueries.find((item) => item.id === dialogQueryId) || null

  const handleOpenFolder = (folderId: string) => {
    if (folderId === ALL_FOLDER_ID) return
    setActiveFolderId(folderId)
    setOpenedFolderId(folderId)
    setIsFolderDialogOpen(true)
  }

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 w-full max-w-none">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-foreground">결과 보드</h2>
          <p className="text-sm text-muted-foreground mt-1">폴더를 만들고 쿼리를 이동해 체계적으로 관리합니다</p>
        </div>
      </div>

      {error && <div className="text-sm text-destructive">{error}</div>}
      {saving && <div className="text-xs text-muted-foreground">저장 중...</div>}

      <div className="relative w-full">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
        <Input
          placeholder="쿼리 검색..."
          className="pl-9"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
      </div>

      <div>
        <div className="mb-3 flex items-center justify-between gap-2">
          <h3 className="text-sm font-medium text-muted-foreground">폴더</h3>
          <Button variant="outline" size="sm" className="h-8 gap-1" onClick={() => setIsCreateFolderOpen(true)}>
            <FolderPlus className="w-3.5 h-3.5" />
            폴더 생성
          </Button>
        </div>
        <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-3 sm:gap-4">
          {folderCards.map((folder) => (
            <FolderCard
              key={folder.id}
              folder={folder}
              active={activeFolderId === folder.id}
              onSelect={() => setActiveFolderId(folder.id)}
              onOpen={() => handleOpenFolder(folder.id)}
              onRename={folder.editable ? () => handleRenameFolder(folder.id) : undefined}
              onDelete={folder.editable ? () => handleDeleteFolder(folder.id) : undefined}
            />
          ))}
        </div>
      </div>

      <Card ref={listSectionRef}>
        <CardHeader className="pb-3">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
            <div>
              <CardTitle className="text-base">{selectedFolderName}</CardTitle>
              <CardDescription>
                {activeFolderId === ALL_FOLDER_ID
                  ? "전체 쿼리를 리스트 형식으로 확인하고 관리합니다."
                  : "선택한 폴더의 쿼리를 리스트 형식으로 확인하고 관리합니다."}
              </CardDescription>
            </div>
            <Badge variant="secondary" className="w-fit">
              {filteredQueries.length}개 쿼리
            </Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {filteredQueries.length > 0 ? (
            <div className="rounded-xl border border-border overflow-hidden">
              <div className="hidden lg:grid grid-cols-[minmax(0,2fr)_minmax(0,1.2fr)_minmax(0,1fr)_130px_110px] gap-3 px-4 py-2 bg-secondary/40 text-[11px] font-medium text-muted-foreground">
                <span>쿼리</span>
                <span>주요 지표</span>
                <span>실행 정보</span>
                <span>폴더</span>
                <span className="text-right">작업</span>
              </div>
              <div className="divide-y divide-border">
                {filteredQueries.map((query) => (
                  <DashboardQueryRow
                    key={query.id}
                    query={query}
                    folderName={folderMap.get(query.folderId || "")?.name || query.category || "기타"}
                    folders={folders}
                    onMoveToFolder={moveQueryToFolder}
                    onTogglePin={togglePin}
                    onDelete={handleDelete}
                    onDuplicate={handleDuplicate}
                    onShare={handleShare}
                    getChartIcon={getChartIcon}
                  />
                ))}
              </div>
            </div>
          ) : (
            <div className="text-center py-12">
              <div className="w-12 h-12 rounded-full bg-secondary flex items-center justify-center mx-auto mb-4">
                <Search className="w-6 h-6 text-muted-foreground" />
              </div>
              <p className="text-muted-foreground">폴더 또는 검색 조건에 맞는 쿼리가 없습니다</p>
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={isFolderDialogOpen} onOpenChange={(open) => {
        setIsFolderDialogOpen(open)
        if (!open) setOpenedFolderId(null)
      }}>
        <DialogContent className="!left-0 !top-0 !translate-x-0 !translate-y-0 !w-screen !h-screen !max-w-none rounded-none">
          <DialogHeader>
            <DialogTitle>{openedFolderName || "폴더"}</DialogTitle>
            <DialogDescription>폴더 안 쿼리를 한눈에 확인합니다.</DialogDescription>
          </DialogHeader>
          <div className="mt-4 h-[calc(100%-4rem)] flex flex-col">
            <div className="flex items-center justify-between mb-3">
              <Badge variant="secondary">
                {dialogQueries.length}개 쿼리
              </Badge>
            </div>
            <div className="flex-1 min-h-0 rounded-xl border border-border bg-card/60 p-4 overflow-y-auto">
              {selectedDialogQuery ? (
                <div className="space-y-4">
                  <div>
                    <div className="text-sm font-semibold text-foreground">{selectedDialogQuery.title}</div>
                    <div className="text-xs text-muted-foreground mt-1">{selectedDialogQuery.description}</div>
                  </div>

                  <Card className="border border-border/60">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">시각화</CardTitle>
                      <CardDescription className="text-xs">저장된 시각화가 있으면 여기에 표시됩니다.</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                        저장된 시각화가 없습니다.
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="border border-border/60">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">결과 미리보기</CardTitle>
                      <CardDescription className="text-xs">쿼리탭에서 저장한 결과 일부를 표시합니다.</CardDescription>
                    </CardHeader>
                    <CardContent>
                      {selectedDialogQuery.preview?.columns?.length ? (
                        <div className="rounded-lg border border-border overflow-hidden">
                          <table className="w-full text-xs">
                            <thead className="bg-secondary/50">
                              <tr>
                                {selectedDialogQuery.preview.columns.map((col) => (
                                  <th key={col} className="text-left p-2 font-medium">
                                    {col}
                                  </th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {selectedDialogQuery.preview.rows.map((row, idx) => (
                                <tr key={idx} className="border-t border-border hover:bg-secondary/30">
                                  {selectedDialogQuery.preview!.columns.map((_, colIdx) => {
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
                        <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                          저장된 결과가 없습니다.
                        </div>
                      )}
                    </CardContent>
                  </Card>
                </div>
              ) : (
                <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
                  쿼리를 선택하면 상세가 표시됩니다.
                </div>
              )}
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isCreateFolderOpen} onOpenChange={(open) => {
        setIsCreateFolderOpen(open)
        if (!open) {
          setCreateFolderName("")
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>새 폴더 생성</DialogTitle>
            <DialogDescription>새 폴더 이름을 입력하세요.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <Input
              placeholder="폴더 이름"
              value={createFolderName}
              onChange={(e) => setCreateFolderName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault()
                  handleCreateFolder(createFolderName)
                  setIsCreateFolderOpen(false)
                  setCreateFolderName("")
                }
              }}
            />
            <div className="flex items-center justify-end gap-2">
              <Button variant="ghost" onClick={() => setIsCreateFolderOpen(false)}>
                취소
              </Button>
              <Button
                onClick={() => {
                  handleCreateFolder(createFolderName)
                  setIsCreateFolderOpen(false)
                  setCreateFolderName("")
                }}
              >
                확인
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isRenameFolderOpen} onOpenChange={(open) => {
        setIsRenameFolderOpen(open)
        if (!open) {
          setRenameFolderTargetId(null)
          setRenameFolderName("")
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>폴더 이름 변경</DialogTitle>
            <DialogDescription>새 폴더 이름을 입력하세요.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <Input
              placeholder="폴더 이름"
              value={renameFolderName}
              onChange={(e) => setRenameFolderName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault()
                  confirmRenameFolder()
                }
              }}
            />
            <div className="flex items-center justify-end gap-2">
              <Button variant="ghost" onClick={() => setIsRenameFolderOpen(false)}>
                취소
              </Button>
              <Button onClick={confirmRenameFolder}>
                확인
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isDeleteFolderOpen} onOpenChange={(open) => {
        setIsDeleteFolderOpen(open)
        if (!open) {
          setDeleteFolderTargetId(null)
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>폴더 삭제</DialogTitle>
            <DialogDescription>
              {deleteFolderTargetId
                ? `폴더 "${folders.find((folder) => folder.id === deleteFolderTargetId)?.name || ""}"를 삭제할까요?`
                : "선택한 폴더를 삭제할까요?"}
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center justify-end gap-2">
            <Button variant="ghost" onClick={() => setIsDeleteFolderOpen(false)}>
              취소
            </Button>
            <Button variant="destructive" onClick={confirmDeleteFolder}>
              삭제
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}

interface FolderCardProps {
  folder: FolderCardInfo
  active: boolean
  onSelect: () => void
  onOpen: () => void
  onRename?: () => void
  onDelete?: () => void
}

function FolderCard({ folder, active, onSelect, onOpen, onRename, onDelete }: FolderCardProps) {
  return (
    <Card
      className={cn(
        "rounded-2xl border bg-card transition-colors aspect-[3.5/1] w-full",
        active ? "border-primary/50 bg-primary/5" : "hover:border-primary/30"
      )}
    >
      <CardContent className="p-4 h-full flex items-center">
        <div className="flex items-start gap-2 w-full">
          <button
            type="button"
            className="flex-1 text-left min-w-0"
            onClick={onSelect}
            onDoubleClick={onOpen}
          >
            <div className="flex items-center gap-2 min-w-0">
                <div className="w-9 h-9 rounded-lg bg-secondary flex items-center justify-center shrink-0">
                  {folder.id === ALL_FOLDER_ID ? (
                    <FolderOpen className="w-4 h-4 text-primary" />
                  ) : (
                    <Folder className="w-4 h-4 text-primary" />
                  )}
                </div>
              <div className="min-w-0">
                <div className="text-sm font-semibold text-foreground truncate">{folder.name}</div>
                <div className="text-xs text-muted-foreground">{folder.count}개 쿼리</div>
              </div>
            </div>
          </button>
          <div className="ml-auto flex items-center gap-2 shrink-0">
            {folder.pinnedCount > 0 && (
              <Badge variant="outline" className="text-[10px] shrink-0">
                <Pin className="w-3 h-3 mr-1" />
                {folder.pinnedCount}
              </Badge>
            )}
            {onRename && (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0">
                    <MoreHorizontal className="w-4 h-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onClick={onRename}>
                    <Pencil className="w-4 h-4 mr-2" />
                    이름 변경
                  </DropdownMenuItem>
                  {onDelete && (
                    <>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem className="text-destructive" onClick={onDelete}>
                        <Trash2 className="w-4 h-4 mr-2" />
                        폴더 삭제
                      </DropdownMenuItem>
                    </>
                  )}
                </DropdownMenuContent>
              </DropdownMenu>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

interface DashboardQueryRowProps {
  query: SavedQuery
  folderName: string
  folders: SavedFolder[]
  onMoveToFolder: (queryId: string, folderId: string) => void
  onTogglePin: (id: string) => void
  onDelete: (id: string) => void
  onDuplicate: (id: string) => void
  onShare: (query: SavedQuery) => void
  getChartIcon: (type: string) => ReactNode
}

function DashboardQueryRow({
  query,
  folderName,
  folders,
  onMoveToFolder,
  onTogglePin,
  onDelete,
  onDuplicate,
  onShare,
  getChartIcon,
}: DashboardQueryRowProps) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-[minmax(0,2fr)_minmax(0,1.2fr)_minmax(0,1fr)_130px_110px] gap-3 px-4 py-3 hover:bg-secondary/20">
      <div className="min-w-0">
        <div className="flex items-center gap-2 min-w-0">
          <div className="w-8 h-8 rounded-lg bg-secondary flex items-center justify-center shrink-0">
            {getChartIcon(query.chartType)}
          </div>
          <div className="min-w-0">
            <div className="text-sm font-medium text-foreground truncate">{query.title}</div>
            <div className="text-xs text-muted-foreground line-clamp-1">{query.description}</div>
          </div>
          {query.isPinned && <Pin className="w-3.5 h-3.5 text-primary shrink-0" />}
        </div>
      </div>

      <div className="flex flex-wrap gap-1">
        {query.metrics.slice(0, 3).map((metric, idx) => (
          <Badge key={idx} variant="secondary" className="text-[10px] font-normal">
            {metric.label}: {metric.value}
          </Badge>
        ))}
      </div>

      <div className="text-xs text-muted-foreground space-y-1">
        <div className="flex items-center gap-1">
          <Clock className="w-3 h-3" />
          <span>{query.lastRun}</span>
        </div>
        {query.schedule && (
          <div className="flex items-center gap-1">
            <Calendar className="w-3 h-3" />
            <span>{query.schedule}</span>
          </div>
        )}
      </div>

      <div className="flex items-center">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" size="sm" className="h-7 px-2 text-[10px] gap-1">
              {folderName}
              <ChevronDown className="w-3 h-3" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            {folders.map((folder) => (
              <DropdownMenuItem key={folder.id} onClick={() => onMoveToFolder(query.id, folder.id)}>
                {query.folderId === folder.id ? (
                  <Check className="w-4 h-4 mr-2 text-primary" />
                ) : (
                  <span className="w-4 h-4 mr-2" />
                )}
                {folder.name}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="flex items-center justify-end gap-1">
        <Button size="sm" variant="ghost" className="h-8 px-2 text-xs">
          <Play className="w-3 h-3 mr-1" />
          실행
        </Button>
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
            <DropdownMenuSub>
              <DropdownMenuSubTrigger>폴더 이동</DropdownMenuSubTrigger>
              <DropdownMenuSubContent>
                {folders.map((folder) => (
                  <DropdownMenuItem key={folder.id} onClick={() => onMoveToFolder(query.id, folder.id)}>
                    {query.folderId === folder.id ? (
                      <Check className="w-4 h-4 mr-2 text-primary" />
                    ) : (
                      <span className="w-4 h-4 mr-2" />
                    )}
                    {folder.name}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuSubContent>
            </DropdownMenuSub>
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
    </div>
  )
}
