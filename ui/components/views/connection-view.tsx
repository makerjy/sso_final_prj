"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import { 
  Database, 
  CheckCircle2, 
  XCircle, 
  Shield, 
  Lock,
  RefreshCw,
  AlertTriangle,
  Server,
  Eye
} from "lucide-react"
import { cn } from "@/lib/utils"

interface TableScope {
  id: string
  name: string
  schema: string
  description: string
  rowCount: string
  selected: boolean
}

interface PoolStatus {
  open?: boolean
  busy?: number | null
  open_connections?: number | null
  max?: number | null
}

export function ConnectionView() {
  const apiBaseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "").replace(/\/$/, "")
  const apiUrl = (path: string) => (apiBaseUrl ? `${apiBaseUrl}${path}` : path)
  const [isConnected, setIsConnected] = useState(false)
  const [isReadOnly, setIsReadOnly] = useState(true)
  const [isTesting, setIsTesting] = useState(false)
  const [poolStatus, setPoolStatus] = useState<PoolStatus | null>(null)
  const [statusError, setStatusError] = useState<string | null>(null)
  const [lastChecked, setLastChecked] = useState<Date | null>(null)
  const [connectionConfig, setConnectionConfig] = useState({
    host: "mimic-iv.hospital.edu",
    port: "5432",
    database: "mimiciv",
    username: "researcher_01",
    sslMode: "require"
  })

  const [tableScopes, setTableScopes] = useState<TableScope[]>([
    { id: "patients", name: "patients", schema: "mimiciv_hosp", description: "환자 기본 정보", rowCount: "382,278", selected: true },
    { id: "admissions", name: "admissions", schema: "mimiciv_hosp", description: "입원 기록", rowCount: "524,520", selected: true },
    { id: "diagnoses_icd", name: "diagnoses_icd", schema: "mimiciv_hosp", description: "ICD 진단 코드", rowCount: "5,280,857", selected: true },
    { id: "procedures_icd", name: "procedures_icd", schema: "mimiciv_hosp", description: "ICD 시술 코드", rowCount: "704,124", selected: true },
    { id: "labevents", name: "labevents", schema: "mimiciv_hosp", description: "검사 결과", rowCount: "122,103,667", selected: false },
    { id: "prescriptions", name: "prescriptions", schema: "mimiciv_hosp", description: "처방 정보", rowCount: "17,021,399", selected: false },
    { id: "icustays", name: "icustays", schema: "mimiciv_icu", description: "ICU 재원 기록", rowCount: "76,943", selected: true },
    { id: "chartevents", name: "chartevents", schema: "mimiciv_icu", description: "차트 이벤트", rowCount: "329,499,788", selected: false },
  ])

  const readError = async (res: Response) => {
    const text = await res.text()
    try {
      const json = JSON.parse(text)
      if (json?.detail) return String(json.detail)
    } catch {}
    return text || `${res.status} ${res.statusText}`
  }

  const fetchPoolStatus = async () => {
    setIsTesting(true)
    setStatusError(null)
    try {
      const res = await fetch(apiUrl("/admin/oracle/pool/status"))
      if (!res.ok) {
        throw new Error(await readError(res))
      }
      const data: PoolStatus = await res.json()
      setPoolStatus(data)
      setIsConnected(Boolean(data?.open))
    } catch (err: any) {
      setIsConnected(false)
      setPoolStatus(null)
      setStatusError(err?.message || "연결 상태를 확인할 수 없습니다.")
    } finally {
      setIsTesting(false)
      setLastChecked(new Date())
    }
  }

  const handleTestConnection = async () => {
    await fetchPoolStatus()
  }

  const toggleTable = (id: string) => {
    setTableScopes(prev => prev.map(t => 
      t.id === id ? { ...t, selected: !t.selected } : t
    ))
  }

  const selectedCount = tableScopes.filter(t => t.selected).length

  useEffect(() => {
    fetchPoolStatus()
  }, [])

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 max-w-5xl">
      <div>
        <h2 className="text-xl sm:text-2xl font-bold text-foreground">DB 연결 및 권한 설정</h2>
        <p className="text-muted-foreground mt-1">데이터베이스 연결을 구성하고 접근 권한을 관리합니다.</p>
      </div>

      {/* Connection Status */}
      <Card className={cn(
        "border-2 transition-colors",
        isConnected ? "border-primary/50 bg-primary/5" : "border-destructive/50 bg-destructive/5"
      )}>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={cn(
                "flex items-center justify-center w-10 h-10 rounded-full",
                isConnected ? "bg-primary/20" : "bg-destructive/20"
              )}>
                {isConnected ? (
                  <CheckCircle2 className="w-5 h-5 text-primary" />
                ) : (
                  <XCircle className="w-5 h-5 text-destructive" />
                )}
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <span className="font-medium text-foreground">
                    {isConnected ? "연결됨" : "연결 안됨"}
                  </span>
                  {isConnected && (
                    <Badge variant="outline" className="text-xs">
                      <Lock className="w-3 h-3 mr-1" />
                      SSL
                    </Badge>
                  )}
                </div>
                <p className="text-sm text-muted-foreground">
                  {statusError
                    ? statusError
                    : isTesting && !lastChecked
                      ? "연결 확인 중..."
                      : isConnected
                        ? `Oracle pool ${poolStatus?.open_connections ?? "-"} / ${poolStatus?.max ?? "-"} (busy ${poolStatus?.busy ?? "-"})`
                        : "데이터베이스에 연결되지 않았습니다"}
                </p>
                {lastChecked && (
                  <p className="text-xs text-muted-foreground mt-1">
                    마지막 확인: {lastChecked.toLocaleTimeString()}
                  </p>
                )}
              </div>
            </div>
            <Button 
              variant="outline" 
              onClick={handleTestConnection}
              disabled={isTesting}
            >
              <RefreshCw className={cn("w-4 h-4 mr-2", isTesting && "animate-spin")} />
              {isTesting ? "테스트 중..." : "연결 테스트"}
            </Button>
          </div>
        </CardContent>
      </Card>

      <div className="grid lg:grid-cols-2 gap-4 sm:gap-6">
        {/* Connection Configuration */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-lg">
              <Server className="w-5 h-5" />
              연결 설정
            </CardTitle>
            <CardDescription>데이터베이스 연결 정보를 입력하세요</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="host">호스트</Label>
                <Input 
                  id="host" 
                  value={connectionConfig.host}
                  onChange={(e) => setConnectionConfig(prev => ({ ...prev, host: e.target.value }))}
                  placeholder="localhost"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="port">포트</Label>
                <Input 
                  id="port" 
                  value={connectionConfig.port}
                  onChange={(e) => setConnectionConfig(prev => ({ ...prev, port: e.target.value }))}
                  placeholder="5432"
                />
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="database">데이터베이스</Label>
              <Input 
                id="database" 
                value={connectionConfig.database}
                onChange={(e) => setConnectionConfig(prev => ({ ...prev, database: e.target.value }))}
                placeholder="mimiciv"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="username">사용자명</Label>
              <Input 
                id="username" 
                value={connectionConfig.username}
                onChange={(e) => setConnectionConfig(prev => ({ ...prev, username: e.target.value }))}
                placeholder="username"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">비밀번호</Label>
              <Input 
                id="password" 
                type="password"
                placeholder="••••••••"
              />
            </div>
          </CardContent>
        </Card>

        {/* Security Settings */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-lg">
              <Shield className="w-5 h-5" />
              보안 설정
            </CardTitle>
            <CardDescription>쿼리 실행 권한 및 보안 정책을 설정합니다</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Read-Only Mode */}
            <div className="flex items-center justify-between p-4 rounded-lg bg-secondary/50 border border-border">
              <div className="flex items-center gap-3">
                <div className="flex items-center justify-center w-8 h-8 rounded-full bg-primary/20">
                  <Eye className="w-4 h-4 text-primary" />
                </div>
                <div>
                  <div className="font-medium text-foreground">Read-Only 모드</div>
                  <div className="text-xs text-muted-foreground">SELECT 쿼리만 허용 (필수)</div>
                </div>
              </div>
              <Switch 
                checked={isReadOnly} 
                onCheckedChange={setIsReadOnly}
                disabled
              />
            </div>

            {/* Security Policies */}
            <div className="space-y-3">
              <div className="flex items-start gap-3 p-3 rounded-lg border border-border">
                <CheckCircle2 className="w-4 h-4 text-primary mt-0.5" />
                <div>
                  <div className="text-sm font-medium text-foreground">SQL Injection 방지</div>
                  <div className="text-xs text-muted-foreground">파라미터화된 쿼리 강제 적용</div>
                </div>
              </div>
              <div className="flex items-start gap-3 p-3 rounded-lg border border-border">
                <CheckCircle2 className="w-4 h-4 text-primary mt-0.5" />
                <div>
                  <div className="text-sm font-medium text-foreground">쿼리 결과 제한</div>
                  <div className="text-xs text-muted-foreground">최대 10,000개 행 반환</div>
                </div>
              </div>
              <div className="flex items-start gap-3 p-3 rounded-lg border border-border">
                <CheckCircle2 className="w-4 h-4 text-primary mt-0.5" />
                <div>
                  <div className="text-sm font-medium text-foreground">타임아웃 설정</div>
                  <div className="text-xs text-muted-foreground">쿼리 실행 30초 제한</div>
                </div>
              </div>
            </div>

            <div className="flex items-center gap-2 p-3 rounded-lg bg-yellow-500/10 border border-yellow-500/30">
              <AlertTriangle className="w-4 h-4 text-yellow-500" />
              <span className="text-xs text-yellow-500">모든 쿼리는 감사 로그에 기록됩니다</span>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Table Scope Selection */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-lg">
            <Database className="w-5 h-5" />
            테이블 스코프 선택
          </CardTitle>
          <CardDescription>
            쿼리 대상 테이블을 선택하세요. 선택된 테이블만 NL2SQL 변환에 사용됩니다.
            <Badge variant="secondary" className="ml-2">{selectedCount}개 선택됨</Badge>
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid md:grid-cols-2 gap-3">
            {tableScopes.map((table) => (
              <div 
                key={table.id}
                className={cn(
                  "flex items-start gap-3 p-3 rounded-lg border transition-colors cursor-pointer",
                  table.selected 
                    ? "border-primary/50 bg-primary/5" 
                    : "border-border hover:border-primary/30"
                )}
                onClick={() => toggleTable(table.id)}
              >
                <Checkbox 
                  checked={table.selected}
                  onCheckedChange={() => toggleTable(table.id)}
                  className="mt-0.5"
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-sm font-medium text-foreground">{table.name}</span>
                    <Badge variant="outline" className="text-[10px]">{table.schema}</Badge>
                  </div>
                  <p className="text-xs text-muted-foreground mt-1">{table.description}</p>
                  <p className="text-[10px] text-muted-foreground mt-1">{table.rowCount} rows</p>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Save Button */}
      <div className="flex justify-end gap-3">
        <Button variant="outline">취소</Button>
        <Button>설정 저장</Button>
      </div>
    </div>
  )
}
