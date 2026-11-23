<template>
  <section class="page" data-module="backfill_evidence">
    <header class="page-head">
      <div>
        <h2>续办证据</h2>
        <p class="page-desc">
          按行身份回查回填的真实写入结果；恢复队列从这里领取待补范围，断点只作提示。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="reload-evidence" type="button" @click="reload">
          回查证据清单
        </button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>任务编号</span>
        <input v-model="taskCode" data-testid="evidence-task" placeholder="如 BACK-0002" />
      </label>
      <button class="btn primary" data-testid="query-evidence" type="submit">查询</button>
    </form>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <template v-if="checklist">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">分区</span>
          <strong class="stat-value">{{ checklist.partition }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">总行数</span>
          <strong class="stat-value">{{ checklist.total }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已核定写入</span>
          <strong class="stat-value">{{ checklist.written_count }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">待补写</span>
          <strong class="stat-value">{{ checklist.pending_count }}</strong>
        </article>
      </div>

      <h3>原失败证据</h3>
      <table class="data-table" data-testid="source-failure">
        <thead>
          <tr><th>尝试编号</th><th>失败原因</th><th>受影响范围</th><th>失败时间</th><th>当前暂扣</th></tr>
        </thead>
        <tbody>
          <tr>
            <td>{{ checklist.source_failure.attempt_no ?? '—' }}</td>
            <td>{{ checklist.source_failure.failure_reason ?? '—' }}</td>
            <td>{{ (checklist.source_failure.scope ?? []).join('、') || '—' }}</td>
            <td>{{ checklist.source_failure.failed_at ?? '—' }}</td>
            <td>{{ checklist.open_attempt ?? '—' }}</td>
          </tr>
        </tbody>
      </table>

      <h3>尝试证据</h3>
      <table class="data-table">
        <thead>
          <tr><th>尝试编号</th><th>状态</th><th>失败原因</th><th>提交范围</th><th>核实写入</th><th>核对时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="attempt in checklist.attempts" :key="attempt.attempt_no">
            <td>{{ attempt.attempt_no }}</td>
            <td>{{ attempt.status }}</td>
            <td>{{ attempt.failure_reason }}</td>
            <td>{{ attempt.scope.join('、') }}</td>
            <td>{{ attempt.written_rows.join('、') || '—' }}</td>
            <td>{{ attempt.resolved_at || '—' }}</td>
          </tr>
          <tr v-if="!checklist.attempts.length">
            <td colspan="6" class="empty-state">暂无失败尝试</td>
          </tr>
        </tbody>
      </table>

      <h3>回执核对</h3>
      <table class="data-table">
        <thead>
          <tr><th>回执</th><th>尝试编号</th><th>回执行</th><th>判定</th><th>采用</th><th>忽略重复</th></tr>
        </thead>
        <tbody>
          <tr v-for="receipt in checklist.receipts" :key="receipt.receipt">
            <td>{{ receipt.receipt }}</td>
            <td>{{ receipt.attempt_no ?? '—' }}</td>
            <td>{{ receipt.rows.join('、') }}</td>
            <td>{{ receipt.status }}</td>
            <td>{{ receipt.adopted_rows.join('、') || '—' }}</td>
            <td>{{ receipt.duplicate_rows.join('、') || '—' }}</td>
          </tr>
          <tr v-if="!checklist.receipts.length">
            <td colspan="6" class="empty-state">暂无回执</td>
          </tr>
        </tbody>
      </table>

      <h3>逐行清单</h3>
      <table class="data-table" data-testid="evidence-rows">
        <thead>
          <tr><th>行身份</th><th>状态</th><th>写入来源</th><th>写入时间</th><th>下次续办</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in checklist.rows" :key="row.row_id">
            <td>{{ row.row_id }}</td>
            <td>{{ row.status }}</td>
            <td>{{ row.write_source || '—' }}</td>
            <td>{{ row.written_at || '—' }}</td>
            <td>{{ checklist.next_claim.includes(row.row_id) ? '是' : '否' }}</td>
          </tr>
        </tbody>
      </table>

      <h3>续办领取</h3>
      <form class="filter-bar" @submit.prevent="claimRows">
        <label class="filter-item">
          <span>重放条目编号</span>
          <input v-model.number="entryId" data-testid="claim-entry" type="number" min="1" />
        </label>
        <button class="btn primary" data-testid="claim" type="submit">领取待补范围</button>
      </form>
      <p v-if="claimMessage" class="page-desc">{{ claimMessage }}</p>
      <p v-if="claimNote" class="page-desc">{{ claimNote }}</p>
      <form v-if="claimCode" class="filter-bar" @submit.prevent="writeClaimed">
        <label class="filter-item">
          <span>领取到的行（逗号分隔）</span>
          <input v-model="claimedRows" data-testid="claimed-rows" />
        </label>
        <button class="btn primary" data-testid="write-claim" type="submit">写入领到的行</button>
      </form>
      <p v-if="writeMessage" class="page-desc">{{ writeMessage }}</p>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  type EvidenceChecklist,
  issueClaim,
  loadEvidence,
  writeClaim,
} from '@/api/backend-service'

const taskCode = ref('BACK-0002')
const entryId = ref(1)
const checklist = ref<EvidenceChecklist | null>(null)
const errorMessage = ref('')
const claimMessage = ref('')
const claimNote = ref('')
const claimCode = ref('')
const claimedRows = ref('')
const writeMessage = ref('')

async function reload() {
  errorMessage.value = ''
  claimMessage.value = ''
  claimNote.value = ''
  claimCode.value = ''
  writeMessage.value = ''
  try {
    checklist.value = await loadEvidence(taskCode.value.trim())
  } catch (error) {
    checklist.value = null
    errorMessage.value = error instanceof Error ? error.message : '续办证据清单读取失败'
  }
}

async function claimRows() {
  claimMessage.value = ''
  claimNote.value = ''
  claimCode.value = ''
  try {
    const result = await issueClaim(taskCode.value.trim(), Number(entryId.value))
    if (!result.ok) {
      claimMessage.value = String(result.reason ?? '暂无可领取范围')
      claimNote.value = String(result.queue_status ?? '')
      return
    }
    claimCode.value = String(result.claim)
    claimedRows.value = (result.rows as string[]).join(',')
    claimMessage.value = `领取单 ${claimCode.value}`
    claimNote.value = String(result.note ?? '')
  } catch (error) {
    claimMessage.value = error instanceof Error ? error.message : '领取失败'
  }
}

async function writeClaimed() {
  writeMessage.value = ''
  try {
    const rows = claimedRows.value
      .split(',')
      .map((row) => row.trim())
      .filter(Boolean)
    const result = await writeClaim(taskCode.value.trim(), claimCode.value, rows)
    if (!result.ok) {
      writeMessage.value = String(result.message ?? '写入未被接受')
      return
    }
    writeMessage.value =
      `已写入 ${(result.written_rows as string[]).join('、') || '无'}，` +
      `剩余待补 ${(result.pending_rows as string[]).join('、') || '无'}`
    await reload()
  } catch (error) {
    writeMessage.value = error instanceof Error ? error.message : '写入登记失败'
  }
}

onMounted(reload)
</script>
