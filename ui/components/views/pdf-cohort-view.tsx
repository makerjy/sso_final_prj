"use client"

import { useState, useRef } from "react"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Upload, FileText, Sparkles, Loader2, ArrowLeft } from "lucide-react"
import PdfResultPanel from "./pdf-result-panel"

export function PdfCohortView() {
    const [isLoading, setIsLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [message, setMessage] = useState<string | null>(null)

    // PDF 분석 결과 상태
    const [pdfResult, setPdfResult] = useState<any>(null)
    const fileInputRef = useRef<HTMLInputElement>(null)

    const handlePdfUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0]
        if (!file) return

        setIsLoading(true)
        setError(null)
        setMessage(null)
        setPdfResult(null)

        const formData = new FormData()
        formData.append("file", file)

        try {
            const res = await fetch("/pdf/upload", {
                method: "POST",
                body: formData,
            })

            if (!res.ok) {
                const detail = await res.text()
                throw new Error(detail || "PDF 업로드 실패")
            }

            const { task_id } = await res.json()
            setMessage("PDF 분석이 시작되었습니다. 잠시만 기다려주세요...")

            const poll = async () => {
                try {
                    const statusRes = await fetch(`/pdf/status/${task_id}`)
                    if (!statusRes.ok) throw new Error("분석 상태 확인 실패")

                    const statusData = await statusRes.json()
                    console.log("Task Status:", statusData)

                    if (statusData.status === "completed") {
                        setPdfResult(statusData.result)
                        setMessage("분석이 완료되었습니다.")
                        setIsLoading(false)
                    } else if (statusData.status === "failed") {
                        setError(statusData.error || "분석 실패")
                        setIsLoading(false)
                    } else {
                        setTimeout(poll, 2000)
                        setMessage(`분석 중... (${statusData.message || "처리 중"})`)
                    }
                } catch (pollErr) {
                    setError(pollErr instanceof Error ? pollErr.message : "분석 중 오류 발생")
                    setIsLoading(false)
                }
            }
            poll()

        } catch (err) {
            setError(err instanceof Error ? err.message : "업로드 중 오류가 발생했습니다.")
            setIsLoading(false)
        } finally {
            if (fileInputRef.current) {
                fileInputRef.current.value = ""
            }
        }
    }

    const handleReset = () => {
        setPdfResult(null)
        setError(null)
        setMessage(null)
    }

    const handleConfirmPdf = async () => {
        try {
            const hash = pdfResult?.pdf_hash
            if (!hash) {
                setError("저장할 수 있는 PDF 해시 정보(pdf_hash)가 존재하지 않습니다.")
                return
            }

            const res = await fetch("/cohort/pdf/confirm", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    pdf_hash: hash,
                    data: pdfResult,
                    status: "confirmed"
                })
            })

            if (!res.ok) {
                const errData = await res.json()
                throw new Error(errData.detail || "서버 저장 실패")
            }

            setMessage("코호트 분석 결과가 최종 확정되어 시스템에 영구 저장되었습니다.")
            // 저장 후 초기화 할지, 유지할지 선택. 일단 유지.
        } catch (err: any) {
            setError(`확정 저장 중 오류 발생: ${err.message}`)
        }
    }

    const handleCopySQL = async (sql: string) => {
        try {
            await navigator.clipboard.writeText(sql)
            setMessage("SQL이 클립보드에 복사되었습니다.")
        } catch {
            setError("클립보드 복사에 실패했습니다.")
        }
    }

    const handleDownloadCSV = () => {
        // CSV 다운로드 로직 구현 (필요시)
        setMessage("CSV 다운로드 기능은 준비 중입니다.")
    }

    // 분석 결과가 있으면 PdfResultPanel 표시
    if (pdfResult) {
        return (
            <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 h-full flex flex-col">
                <div className="flex items-center justify-between">
                    <div>
                        <h2 className="text-xl sm:text-2xl font-bold text-foreground">PDF 코호트 분석 결과</h2>
                        <p className="text-sm text-muted-foreground mt-1">논문에서 추출된 코호트 기준을 검토하고 SQL을 생성합니다.</p>
                    </div>
                    <Button variant="outline" onClick={handleReset} className="gap-2">
                        <ArrowLeft className="w-4 h-4" />
                        다른 PDF 분석하기
                    </Button>
                </div>

                <div className="flex-1 overflow-y-auto border rounded-lg bg-background">
                    <PdfResultPanel
                        pdfResult={pdfResult}
                        charts={[]} // 차트 데이터가 있다면 pdfResult에서 가공해서 전달
                        onSave={handleConfirmPdf}
                        onClose={() => { }}
                        onCopySQL={handleCopySQL}
                        onDownloadCSV={handleDownloadCSV}
                        setMessage={setMessage}
                        setError={setError}
                        setPdfResult={setPdfResult}
                    />
                </div>
            </div>
        )
    }

    // 초기 화면: 업로드
    return (
        <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 h-full flex flex-col items-center justify-center">
            <div className="max-w-2xl w-full space-y-8">
                <div className="text-center space-y-2">
                    <h2 className="text-2xl sm:text-3xl font-bold text-foreground">PDF 코호트 분석</h2>
                    <p className="text-muted-foreground">
                        의학 논문(PDF)을 업로드하면 AI가 코호트 선정 기준을 자동 추출하고<br className="hidden sm:inline" />
                        MIMIC-IV 데이터베이스에 맞는 SQL 쿼리를 생성합니다.
                    </p>
                </div>

                <Card className="border-2 border-dashed border-muted-foreground/25 hover:border-primary/50 transition-colors bg-muted/50">
                    <CardContent className="flex flex-col items-center justify-center py-12 space-y-4 text-center">
                        <div className="p-4 rounded-full bg-background border shadow-sm">
                            {isLoading ? (
                                <Loader2 className="w-10 h-10 text-primary animate-spin" />
                            ) : (
                                <FileText className="w-10 h-10 text-muted-foreground" />
                            )}
                        </div>

                        <div className="space-y-1">
                            <h3 className="font-semibold text-lg">논문 PDF 업로드</h3>
                            <p className="text-sm text-muted-foreground">
                                클릭하거나 파일을 드래그하여 업로드하세요 (최대 10MB)
                            </p>
                        </div>

                        {error && (
                            <div className="text-sm text-destructive font-medium bg-destructive/10 px-3 py-1 rounded-md">
                                {error}
                            </div>
                        )}

                        {message && !error && (
                            <div className="text-sm text-primary font-medium bg-primary/10 px-3 py-1 rounded-md">
                                {message}
                            </div>
                        )}

                        <div className="relative mt-4">
                            <input
                                type="file"
                                accept=".pdf"
                                onChange={handlePdfUpload}
                                disabled={isLoading}
                                ref={fileInputRef}
                                className="absolute inset-0 w-full h-full opacity-0 cursor-pointer disabled:cursor-not-allowed"
                            />
                            <Button disabled={isLoading} className="gap-2 pointer-events-none">
                                {isLoading ? "분석 중..." : (
                                    <>
                                        <Upload className="w-4 h-4" />
                                        PDF 파일 선택
                                    </>
                                )}
                            </Button>
                        </div>

                        <div className="flex items-center gap-2 text-xs text-muted-foreground mt-4 bg-background/50 px-3 py-1.5 rounded-full border">
                            <Sparkles className="w-3 h-3 text-amber-500" />
                            <span>AI가 Inclusion/Exclusion Criteria를 자동 분석합니다</span>
                        </div>
                    </CardContent>
                </Card>

                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                    <div className="p-4 rounded-lg border bg-card text-center space-y-2">
                        <Badge variant="outline" className="mb-1">Step 1</Badge>
                        <h4 className="font-medium">PDF 텍스트 추출</h4>
                        <p className="text-xs text-muted-foreground">논문의 Methods 섹션을 분석하여 코호트 정의를 파악합니다.</p>
                    </div>
                    <div className="p-4 rounded-lg border bg-card text-center space-y-2">
                        <Badge variant="outline" className="mb-1">Step 2</Badge>
                        <h4 className="font-medium">임상 변수 매핑</h4>
                        <p className="text-xs text-muted-foreground">추출된 변수를 MIMIC-IV 스키마 코드와 자동으로 매핑합니다.</p>
                    </div>
                    <div className="p-4 rounded-lg border bg-card text-center space-y-2">
                        <Badge variant="outline" className="mb-1">Step 3</Badge>
                        <h4 className="font-medium">SQL 쿼리 생성</h4>
                        <p className="text-xs text-muted-foreground">실행 가능한 Oracle SQL을 생성하여 코호트를 추출합니다.</p>
                    </div>
                </div>
            </div>
        </div>
    )
}
