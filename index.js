const express = require('express')
const fetch   = require('node-fetch')
const cors    = require('cors')

const app    = express()
const PORT   = process.env.PORT || 8080
const APIKEY = process.env.WINDSOR_API_KEY

app.use(cors())
app.options('*', cors())
app.use(express.json())

app.get('/', (req, res) => res.json({ ok: true, service: 'blissclub-proxy', ts: Date.now() }))
app.get('/ping', (req, res) => res.json({ pong: true, ts: Date.now() }))

async function windsorFetch(fields, accounts, datePreset = 'last_30d') {
  if (!APIKEY) throw new Error('WINDSOR_API_KEY not set')
  const params = new URLSearchParams({
    api_key:     APIKEY,
    date_preset: datePreset,
    fields:      fields.join(','),
  })
  if (accounts) params.set('select_accounts', accounts)
  const url = `https://connectors.windsor.ai/all?${params}`
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), 55000)
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

// ── Meta daily — split into 2 calls to stay under Windsor field limit ─────────
app.get('/api/meta-daily', async (req, res) => {
  try {
    const preset = req.query.preset || 'last_30d'
    // Call 1: Meta spend/performance fields
    const metaData = await windsorFetch([
      'date', 'campaign', 'adset_name', 'ad_name',
      'spend', 'impressions', 'clicks', 'datasource',
    ], `facebook__584820145452956`, preset)

    // Call 2: GA4 + revenue fields
    const ga4Data = await windsorFetch([
      'date', 'campaign', 'session_manual_term', 'session_manual_ad_content',
      'sessions', 'totalrevenue', 'transactions', 'datasource', 'source',
    ], `googleanalytics4__344633503`, preset)

    const data = [...metaData, ...ga4Data]
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
      'cost', 'impressions', 'clicks',
      'conversions', 'conversion_value',
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

// ── Google products (Shopping + PMax) ────────────────────────────────────────
app.get('/api/google-products', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'campaign_name', 'product_title',
      'cost', 'impressions', 'clicks',
      'conversions', 'conversion_value',
    ], `google_ads__858-197-3435`, req.query.preset || 'last_30d')
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Google Demand Gen (video + static ad level) ──────────────────────────────
app.get('/api/google-demandgen', async (req, res) => {
  try {
    const data = await windsorFetch([
      'date', 'campaign_name', 'ad_name',
      'cost', 'impressions', 'clicks',
      'conversions', 'conversion_value',
      'average_cpm', 'ctr',
    ], `google_ads__858-197-3435`, req.query.preset || 'last_30d')
    // Filter only demand gen campaigns
    const filtered = data.filter(r => {
      const name = (r.campaign_name || '').toLowerCase()
      return name.includes('demand') || name.includes('demandgen') || name.includes('demand_gen')
    })
    res.json({ ok: true, data: filtered, count: filtered.length })
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
    { key: 'products',    path: `/api/google-products?preset=${preset}` },
    { key: 'demandgen',   path: `/api/google-demandgen?preset=${preset}` },
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
