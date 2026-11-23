import { createRouter, createWebHistory } from 'vue-router'

import BackfillEvidenceView from '@/views/BackfillEvidenceView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/backfill_evidence', name: 'backfill_evidence', component: BackfillEvidenceView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
