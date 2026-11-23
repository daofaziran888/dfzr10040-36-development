<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">熔炉工坊 · 实时数据融合与宽表构建平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向多源异构数据的实时接入融合、宽表映射构建、维度关联编排、数据质量门禁与分区快照管理的实时数据融合平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "数据源定义", path: "/source_def" }, { label: "Schema映射", path: "/schema_map" }, { label: "维度表", path: "/dim_table" }, { label: "融合作业", path: "/fusion_job" }, { label: "冲突仲裁", path: "/merge_conflict" }, { label: "质量门禁", path: "/quality_gate" }, { label: "分区快照", path: "/snapshot_mgr" }, { label: "数据回填", path: "/backfill_task" }, { label: "续办证据", path: "/backfill_evidence" }, { label: "数据血缘", path: "/lineage_edge" }, { label: "重放队列", path: "/replay_queue" }, { label: "水位追踪", path: "/watermark_track" }, { label: "分区策略", path: "/partition_rule" }, { label: "宽表定义", path: "/wide_table_def" }, { label: "增量归并", path: "/delta_merge" }, { label: "冷热分层", path: "/cold_store" }, { label: "快照导出", path: "/export_snapshot" }, { label: "指标聚合", path: "/metric_aggr" }, { label: "交叉校验", path: "/cross_validate" }, { label: "数据漂移", path: "/data_drift" }, { label: "融合监控", path: "/fusion_monitor" }, { label: "变更审计", path: "/audit_trail" }, { label: "目录检索", path: "/catalog_search" }]
</script>
