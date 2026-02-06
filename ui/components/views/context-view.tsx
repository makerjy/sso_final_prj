"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Textarea } from "@/components/ui/textarea"
import { 
  GitBranch, 
  Calculator, 
  BookOpen,
  Plus,
  Pencil,
  Trash2,
  ArrowRight,
  Search,
  Table2,
  Hash
} from "lucide-react"
import { cn } from "@/lib/utils"

interface JoinRelation {
  id: string
  leftTable: string
  leftColumn: string
  rightTable: string
  rightColumn: string
  joinType: string
}

interface Metric {
  id: string
  name: string
  nameKo: string
  formula: string
  description: string
  category: string
}

interface Term {
  id: string
  term: string
  aliases: string[]
  definition: string
  sqlMapping?: string
}

export function ContextView() {
  const [joins, setJoins] = useState<JoinRelation[]>([
    { id: "1", leftTable: "patients", leftColumn: "subject_id", rightTable: "admissions", rightColumn: "subject_id", joinType: "INNER" },
    { id: "2", leftTable: "admissions", leftColumn: "hadm_id", rightTable: "diagnoses_icd", rightColumn: "hadm_id", joinType: "INNER" },
    { id: "3", leftTable: "admissions", leftColumn: "hadm_id", rightTable: "procedures_icd", rightColumn: "hadm_id", joinType: "LEFT" },
    { id: "4", leftTable: "patients", leftColumn: "subject_id", rightTable: "icustays", rightColumn: "subject_id", joinType: "LEFT" },
  ])

  const [metrics, setMetrics] = useState<Metric[]>([
    { id: "1", name: "LOS", nameKo: "재원일수", formula: "EXTRACT(DAY FROM dischtime - admittime)", description: "입원부터 퇴원까지의 일수", category: "재원" },
    { id: "2", name: "Readmission30", nameKo: "30일 재입원", formula: "CASE WHEN next_admit - dischtime <= 30 THEN 1 ELSE 0 END", description: "퇴원 후 30일 이내 재입원 여부", category: "재입원" },
    { id: "3", name: "MortalityRate", nameKo: "사망률", formula: "COUNT(CASE WHEN dod IS NOT NULL THEN 1 END) / COUNT(*)", description: "전체 환자 대비 사망 환자 비율", category: "결과" },
    { id: "4", name: "ICUStay", nameKo: "ICU 재원일수", formula: "EXTRACT(DAY FROM outtime - intime)", description: "ICU 입실부터 퇴실까지의 일수", category: "재원" },
  ])

  const [terms, setTerms] = useState<Term[]>([
    { id: "1", term: "심부전", aliases: ["HF", "Heart Failure", "울혈성 심부전", "CHF"], definition: "심장이 충분한 혈액을 펌프질하지 못하는 상태", sqlMapping: "icd_code IN ('I50', 'I500', 'I501', 'I509', '4280', '4281', '4289')" },
    { id: "2", term: "고혈압", aliases: ["HTN", "Hypertension", "혈압 높음"], definition: "지속적으로 높은 혈압 상태", sqlMapping: "icd_code LIKE 'I10%' OR icd_code LIKE '401%'" },
    { id: "3", term: "당뇨", aliases: ["DM", "Diabetes", "당뇨병"], definition: "혈당 조절 장애 질환", sqlMapping: "icd_code LIKE 'E11%' OR icd_code LIKE '250%'" },
    { id: "4", term: "노인", aliases: ["고령자", "65세 이상", "elderly"], definition: "만 65세 이상의 환자", sqlMapping: "age >= 65" },
  ])

  const [searchTerm, setSearchTerm] = useState("")

  const filteredTerms = terms.filter(t => 
    t.term.toLowerCase().includes(searchTerm.toLowerCase()) ||
    t.aliases.some(a => a.toLowerCase().includes(searchTerm.toLowerCase()))
  )

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 max-w-6xl">
      <div>
        <h2 className="text-xl sm:text-2xl font-bold text-foreground">컨텍스트 편집</h2>
        <p className="text-sm text-muted-foreground mt-1">NL2SQL 변환에 사용되는 조인, 지표, 용어를 관리합니다.</p>
        <Badge variant="outline" className="mt-2">관리자 전용</Badge>
      </div>

      <Tabs defaultValue="joins" className="space-y-4">
        <TabsList className="grid w-full grid-cols-3 max-w-md h-auto">
          <TabsTrigger value="joins" className="gap-1 sm:gap-2 text-xs sm:text-sm py-2">
            <GitBranch className="w-3 h-3 sm:w-4 sm:h-4" />
            <span className="hidden sm:inline">조인 관계</span>
            <span className="sm:hidden">조인</span>
          </TabsTrigger>
          <TabsTrigger value="metrics" className="gap-1 sm:gap-2 text-xs sm:text-sm py-2">
            <Calculator className="w-3 h-3 sm:w-4 sm:h-4" />
            <span className="hidden sm:inline">지표 템플릿</span>
            <span className="sm:hidden">지표</span>
          </TabsTrigger>
          <TabsTrigger value="terms" className="gap-1 sm:gap-2 text-xs sm:text-sm py-2">
            <BookOpen className="w-3 h-3 sm:w-4 sm:h-4" />
            <span className="hidden sm:inline">용어 사전</span>
            <span className="sm:hidden">용어</span>
          </TabsTrigger>
        </TabsList>

        {/* Joins Tab */}
        <TabsContent value="joins" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">테이블 조인 관계</CardTitle>
                  <CardDescription>테이블 간의 조인 관계를 정의합니다. NL2SQL이 자동으로 적절한 조인을 선택합니다.</CardDescription>
                </div>
                <Button size="sm" className="gap-2">
                  <Plus className="w-4 h-4" />
                  조인 추가
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {/* Visual Join Graph */}
              <div className="mb-6 p-4 rounded-lg bg-secondary/30 border border-border">
                <div className="text-xs text-muted-foreground mb-3">테이블 관계도</div>
                <div className="flex items-center justify-center gap-4 flex-wrap">
                  {["patients", "admissions", "diagnoses_icd", "icustays"].map((table, idx) => (
                    <div key={table} className="flex items-center gap-2">
                      <div className="px-3 py-2 rounded-lg bg-primary/20 border border-primary/30">
                        <span className="font-mono text-sm text-foreground">{table}</span>
                      </div>
                      {idx < 3 && <ArrowRight className="w-4 h-4 text-muted-foreground" />}
                    </div>
                  ))}
                </div>
              </div>

              {/* Join List */}
              <div className="space-y-3">
                {joins.map((join) => (
                  <div key={join.id} className="flex items-center gap-3 p-3 rounded-lg border border-border hover:border-primary/30 transition-colors">
                    <div className="flex items-center gap-2 flex-1">
                      <div className="flex items-center gap-1 px-2 py-1 rounded bg-secondary">
                        <Table2 className="w-3 h-3 text-muted-foreground" />
                        <span className="font-mono text-xs">{join.leftTable}</span>
                      </div>
                      <span className="text-xs text-muted-foreground">.{join.leftColumn}</span>
                      <Badge variant="outline" className="text-[10px]">{join.joinType}</Badge>
                      <div className="flex items-center gap-1 px-2 py-1 rounded bg-secondary">
                        <Table2 className="w-3 h-3 text-muted-foreground" />
                        <span className="font-mono text-xs">{join.rightTable}</span>
                      </div>
                      <span className="text-xs text-muted-foreground">.{join.rightColumn}</span>
                    </div>
                    <div className="flex items-center gap-1">
                      <Button variant="ghost" size="icon" className="h-8 w-8">
                        <Pencil className="w-3 h-3" />
                      </Button>
                      <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive">
                        <Trash2 className="w-3 h-3" />
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Metrics Tab */}
        <TabsContent value="metrics" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">지표 템플릿</CardTitle>
                  <CardDescription>자주 사용하는 지표의 SQL 공식을 미리 정의합니다.</CardDescription>
                </div>
                <Button size="sm" className="gap-2">
                  <Plus className="w-4 h-4" />
                  지표 추가
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="grid md:grid-cols-2 gap-4">
                {metrics.map((metric) => (
                  <div key={metric.id} className="p-4 rounded-lg border border-border hover:border-primary/30 transition-colors">
                    <div className="flex items-start justify-between mb-2">
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="font-medium text-foreground">{metric.nameKo}</span>
                          <Badge variant="secondary" className="text-[10px]">{metric.category}</Badge>
                        </div>
                        <span className="text-xs text-muted-foreground font-mono">{metric.name}</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <Button variant="ghost" size="icon" className="h-7 w-7">
                          <Pencil className="w-3 h-3" />
                        </Button>
                        <Button variant="ghost" size="icon" className="h-7 w-7 text-destructive">
                          <Trash2 className="w-3 h-3" />
                        </Button>
                      </div>
                    </div>
                    <p className="text-xs text-muted-foreground mb-2">{metric.description}</p>
                    <div className="p-2 rounded bg-secondary/50 font-mono text-[11px] text-foreground overflow-x-auto">
                      {metric.formula}
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Terms Tab */}
        <TabsContent value="terms" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">용어 사전 / 약어 등록</CardTitle>
                  <CardDescription>의료 용어와 약어를 SQL 조건으로 매핑합니다.</CardDescription>
                </div>
                <Button size="sm" className="gap-2">
                  <Plus className="w-4 h-4" />
                  용어 추가
                </Button>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* Search */}
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                <Input 
                  placeholder="용어 또는 약어 검색..." 
                  className="pl-9"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>

              {/* Terms List */}
              <div className="space-y-3">
                {filteredTerms.map((term) => (
                  <div key={term.id} className="p-4 rounded-lg border border-border hover:border-primary/30 transition-colors">
                    <div className="flex items-start justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <Hash className="w-4 h-4 text-primary" />
                        <span className="font-medium text-foreground">{term.term}</span>
                      </div>
                      <div className="flex items-center gap-1">
                        <Button variant="ghost" size="icon" className="h-7 w-7">
                          <Pencil className="w-3 h-3" />
                        </Button>
                        <Button variant="ghost" size="icon" className="h-7 w-7 text-destructive">
                          <Trash2 className="w-3 h-3" />
                        </Button>
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-1 mb-2">
                      {term.aliases.map((alias) => (
                        <Badge key={alias} variant="outline" className="text-[10px]">{alias}</Badge>
                      ))}
                    </div>
                    <p className="text-xs text-muted-foreground mb-2">{term.definition}</p>
                    {term.sqlMapping && (
                      <div className="p-2 rounded bg-secondary/50 font-mono text-[11px] text-foreground overflow-x-auto">
                        {term.sqlMapping}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Save Button */}
      <div className="flex justify-end gap-3">
        <Button variant="outline">변경 취소</Button>
        <Button>모든 변경 저장</Button>
      </div>
    </div>
  )
}
