const express = require('express')
const fetch   = require('node-fetch')
const cors    = require('cors')

const app    = express()
const PORT   = process.env.PORT || 8080
const APIKEY = process.env.WINDSOR_API_KEY

app.use(cors())
app.options('*', cors())
app.use(express.json())

// ── Health check + keep-alive ────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ ok: true, service: 'blissclub-proxy', ts: Date.now() }))
app.get('/ping', (req, res) => res.json({ pong: true, ts: Date.now() }))

// ── Generic Windsor fetch helper ─────────────────────────────────────────────
async function windsorFetch(fields, accounts, datePreset = 'last_30d') {
  if (!APIKEY) throw new Error('WINDSOR_API_KEY not set')
  const params = new URLSearchParams({
    api_key:     APIKEY,
    date_preset: datePreset,
    fields:      fields.join(','),
  })
  if (accounts) params.set('select_accounts', accounts)
  const url = `https://connectors.windsor.ai/all?${params}`

  // node-fetch v2 uses AbortController for timeout
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 55000) // 55s timeout
  try {
    const res = await fetch(url, { signal: controller.signal })
    clearTimeout(timer)
    if (!res.ok) throw new Error(`Windsor ${res.status}: ${await res.text()}`)
    const json = await res.json()
    if (Array.isArray(json))             return json
    if (Array.isArray(json.data))        return json.data
    if (json.data && Array.isArray(json.data.data)) return json.data.data
    for (const val of Object.values(json)) {
      if (Array.isArray(val)) return val
    }
    return []
  } catch (e) {
    clearTimeout(timer)
    throw e
  }
}

// ── Meta daily ────────────────────────────────────────────────────────────────
app.get('/api/meta-daily', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'campaign', 'adset_name', 'ad_name',
      'spend', 'impressions', 'clicks', 'datasource',
      'sessions', 'source',
      'cost_per_action_type_landing_page_view', 'cpc',
      'purchase_roas_omni_purchase',
      'session_manual_ad_content', 'session_manual_term',
      'totalrevenue', 'transactions',
    ], `facebook__584820145452956,googleanalytics4__344633503`, req.query.preset || 'last_30d')
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Google campaigns ──────────────────────────────────────────────────────────
app.get('/api/google-campaigns', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'campaign_name', 'ad_name',
      'cost', 'spend', 'impressions', 'clicks',
      'average_cpm', 'cpc', 'ctr',
      'conversions', 'conversion_value', 'roas',
    ], `google_ads__858-197-3435`, req.query.preset || 'last_30d')
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Google search terms ───────────────────────────────────────────────────────
app.get('/api/google-search-terms', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'search_term', 'campaign_name', 'ad_group_name',
      'cost', 'impressions', 'clicks', 'conversions', 'conversion_value',
    ], `google_ads__858-197-3435`, req.query.preset || 'last_30d')
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Google keywords ───────────────────────────────────────────────────────────
app.get('/api/google-keywords', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'keyword_text', 'keyword_match_type',
      'campaign_name', 'ad_group_name',
      'cost', 'impressions', 'clicks', 'conversions', 'conversion_value',
    ], `google_ads__858-197-3435`, req.query.preset || 'last_30d')
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── GA4 standalone ────────────────────────────────────────────────────────────
app.get('/api/ga4', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'campaign', 'session_manual_term', 'session_manual_ad_content',
      'sessions', 'transactions', 'totalrevenue', 'source',
    ], `googleanalytics4__344633503`, req.query.preset || 'last_30d')
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Google awareness ──────────────────────────────────────────────────────────
app.get('/api/google-awareness', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'campaign_name', 'ad_name',
      'cost', 'impressions', 'clicks',
      'video_views', 'vtr', 'cpv', 'average_cpm',
    ], `google_ads__858-197-3435`, req.query.preset || 'last_30d')
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Sync all ──────────────────────────────────────────────────────────────────
app.get('/api/sync-all', async (req, res) => {
  const preset = req.query.preset || 'last_30d'
  const results = {}
  const errors  = {}

  const tasks = [
    { key: 'meta',        path: `/api/meta-daily?preset=${preset}` },
    { key: 'ga4',         path: `/api/ga4?preset=${preset}` },
    { key: 'google',      path: `/api/google-campaigns?preset=${preset}` },
    { key: 'searchTerms', path: `/api/google-search-terms?preset=${preset}` },
    { key: 'keywords',    path: `/api/google-keywords?preset=${preset}` },
    { key: 'awareness',   path: `/api/google-awareness?preset=${preset}` },
  ]

  await Promise.allSettled(tasks.map(async t => {
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}${t.path}`)
      const j = await r.json()
      results[t.key] = { count: (j.data || []).length, ok: j.ok }
    } catch (e) {
      errors[t.key] = e.message
    }
  }))

  res.json({ ok: true, results, errors, preset })
})

app.listen(PORT, () => console.log(`BlissClub proxy on port ${PORT}`))
