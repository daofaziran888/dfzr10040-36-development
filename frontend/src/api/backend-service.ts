import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export type EvidenceRow = {
  row_id: string
  partition: string
  status: string
  written_at: string
  write_source: string
}

export type EvidenceAttempt = {
  attempt_no: number
  status: string
  scope: string[]
  failure_reason: string
  failed_at: string
  written_rows: string[]
  affected_rows: string[]
  resolved_at: string
}

export type EvidenceReceipt = {
  receipt: string
  attempt_no: number | null
  rows: string[]
  status: string
  adopted_rows: string[]
  duplicate_rows: string[]
  received_at: string
}

export type EvidenceChecklist = {
  task_code: string
  partition: string
  total: number
  written_count: number
  pending_count: number
  written_rows: string[]
  pending_rows: string[]
  rows: EvidenceRow[]
  source_failure: {
    attempt_no?: number
    failure_reason?: string
    failed_at?: string
    scope?: string[]
  }
  open_attempt: number | null
  attempts: EvidenceAttempt[]
  receipts: EvidenceReceipt[]
  conflicts: Record<string, unknown>[]
  next_claim: string[]
}

export async function loadEvidence(taskCode: string): Promise<EvidenceChecklist> {
  const response = await request(`/api/recovery/${encodeURIComponent(taskCode)}/evidence`)
  if (!response.ok) throw new Error('续办证据清单读取失败')
  return await response.json()
}

export async function issueClaim(
  taskCode: string,
  entryId: number,
): Promise<Record<string, unknown>> {
  const response = await request(`/api/recovery/${encodeURIComponent(taskCode)}/claims`, {
    method: 'POST',
    body: JSON.stringify({ entry_id: entryId }),
  })
  return await response.json()
}

export async function writeClaim(
  taskCode: string,
  claim: string,
  writtenRows: string[],
): Promise<Record<string, unknown>> {
  const response = await request(
    `/api/recovery/${encodeURIComponent(taskCode)}/claims/write`,
    { method: 'POST', body: JSON.stringify({ claim, written_rows: writtenRows }) },
  )
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
