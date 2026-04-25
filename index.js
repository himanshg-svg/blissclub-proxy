const express = require('express')
const fetch   = require('node-fetch')
const cors    = require('cors')

const app    = express()
const PORT   = process.env.PORT || 8080
const APIKEY = process.env.WINDSOR_API_KEY

app.use(cors())
app.options('*', cors())
app.use(express.json())

const PRESETS = ['last_7d', 'last_14d', 'last_28d', 'last_60d', 'last_90d']

// ── Base Windsor fetch ────────────────────────────────────────────────────────
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
  const timer = setTimeout(() => controller.abort(), 90000)
  try {
    const res = await fetch(url, { signal: controller.signal })
    clearTimeout(timer)
    if (!res.ok) throw new Error(`Windsor ${res.status}: ${await res.text()}`)
    const text = await res.text()
    const json = JSON.parse(text)
    if (Array.isArray(json))                        return json
    if (Array.isArray(json.data))                   return json.data
    if (json.data && Array.isArray(json.data.data)) return json.data.data
    for (const val of Object.values(json)) { if (Array.isArray(val)) return val }
    return []
  } catch (e) { clearTimeout(timer); throw e }
}

// ── Chunked 90-day fetch — fires all presets in parallel, dedupes by key ─────
async function windsorFetch90(fields, accounts, dedupeKey) {
  const chunks = await Promise.allSettled(
    PRESETS.map(p => windsorFetch(fields, accounts, p))
  )
  const allRows = []
  for (const chunk of chunks) {
    if (chunk.status === 'fulfilled' && Array.isArray(chunk.value)) {
      allRows.push(...chunk.value)
    }
  }
  if (!dedupeKey) return allRows
  // Deduplicate by composite key
  const seen = new Map()
  for (const row of allRows) {
    const key = dedupeKey(row)
    seen.set(key, row)
  }
  return Array.from(seen.values())
}

app.get('/',     (req, res) => res.json({ ok: true, service: 'blissclub-proxy', ts: Date.now() }))
app.get('/ping', (req, res) => res.json({ pong: true, ts: Date.now() }))

// ── Meta daily — 90d chunked ──────────────────────────────────────────────────
app.get('/api/meta-daily', async (req, res) => {
  try {
    const fields  = ['date','campaign','adset_name','ad_name','spend','impressions','clicks']
    const account = `facebook__584820145452956`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.ad_name || ''}__${r.adset_name || ''}`)
    console.log(`Meta daily: ${data.length} rows`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    console.error('meta-daily error:', e.message)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Meta GA4 — 90d chunked ────────────────────────────────────────────────────
app.get('/api/meta-ga4', async (req, res) => {
  try {
    const fields  = ['date','campaign','session_manual_term','sessions','totalrevenue','transactions']
    const account = `googleanalytics4__344633503`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.campaign || ''}__${r.session_manual_term || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) {
    console.error('meta-ga4 error:', e.message)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── Google campaigns — 90d chunked ───────────────────────────────────────────
app.get('/api/google-campaigns', async (req, res) => {
  try {
    const fields  = ['date','campaign_name','ad_name','cost','impressions','clicks','conversions','conversion_value']
    const account = `google_ads__858-197-3435`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.campaign_name || ''}__${r.ad_name || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Google search terms — 90d chunked ────────────────────────────────────────
app.get('/api/google-search-terms', async (req, res) => {
  try {
    const fields  = ['date','search_term','campaign','ad_group_name','cost','impressions','clicks','conversions','conversion_value']
    const account = `google_ads__858-197-3435`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.search_term || ''}__${r.campaign || ''}`)
    // Cap at 10k rows to avoid memory issues
    res.json({ ok: true, data: data.slice(0, 10000), count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Google keywords — 90d chunked ────────────────────────────────────────────
app.get('/api/google-keywords', async (req, res) => {
  try {
    const fields  = ['date','keyword_text','keyword_match_type','campaign_name','ad_group_name','cost','impressions','clicks','conversions','conversion_value']
    const account = `google_ads__858-197-3435`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.keyword_text || ''}__${r.campaign_name || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── GA4 — 90d chunked ────────────────────────────────────────────────────────
app.get('/api/ga4', async (req, res) => {
  try {
    const fields  = ['date','campaign','session_manual_term','session_manual_ad_content','sessions','transactions','totalrevenue','source']
    const account = `googleanalytics4__344633503`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.campaign || ''}__${r.session_manual_term || ''}__${r.source || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Google awareness — 90d chunked ───────────────────────────────────────────
app.get('/api/google-awareness', async (req, res) => {
  try {
    const fields  = ['date','campaign_name','ad_name','cost','impressions','clicks','video_views','vtr','cpv','average_cpm']
    const account = `google_ads__858-197-3435`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.campaign_name || ''}__${r.ad_name || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Google products — 90d chunked ────────────────────────────────────────────
app.get('/api/google-products', async (req, res) => {
  try {
    const fields  = ['date','campaign_name','product_title','cost','impressions','clicks','conversions','conversion_value']
    const account = `google_ads__858-197-3435`
    const data = await windsorFetch90(fields, account,
      r => `${r.date}__${r.campaign_name || ''}__${r.product_title || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Google demand gen — 90d chunked ──────────────────────────────────────────
app.get('/api/google-demandgen', async (req, res) => {
  try {
    const fields  = ['date','campaign_name','ad_name','cost','impressions','clicks','conversions','conversion_value','average_cpm','ctr']
    const account = `google_ads__858-197-3435`
    const allData = await windsorFetch90(fields, account,
      r => `${r.date}__${r.campaign_name || ''}__${r.ad_name || ''}`)
    const filtered = allData.filter(r => {
      const name = (r.campaign_name || '').toLowerCase()
      return name.includes('demand') || name.includes('demandgen') || name.includes('demand_gen')
    })
    res.json({ ok: true, data: filtered, count: filtered.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Meta catalog — 90d chunked (P2) ──────────────────────────────────────────
app.get('/api/meta-catalog', async (req, res) => {
  try {
    const fields  = ['date','campaign','adset_name','ad_name','spend','impressions','clicks','conversions','conversion_value']
    const account = `facebook__584820145452956`
    const allData = await windsorFetch90(fields, account,
      r => `${r.date}__${r.ad_name || ''}__${r.adset_name || ''}`)
    // Filter catalog campaigns
    const filtered = allData.filter(r => {
      const name = (r.campaign || r.adset_name || r.ad_name || '').toLowerCase()
      return name.includes('catalog') || name.includes('dpa') || name.includes('dco')
    })
    res.json({ ok: true, data: filtered, count: filtered.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Sync all ──────────────────────────────────────────────────────────────────
app.get('/api/sync-all', async (req, res) => {
  const results = {}, errors = {}
  const tasks = [
    { key: 'meta',        path: '/api/meta-daily' },
    { key: 'metaGa4',    path: '/api/meta-ga4' },
    { key: 'ga4',         path: '/api/ga4' },
    { key: 'google',      path: '/api/google-campaigns' },
    { key: 'searchTerms', path: '/api/google-search-terms' },
    { key: 'keywords',    path: '/api/google-keywords' },
    { key: 'awareness',   path: '/api/google-awareness' },
    { key: 'products',    path: '/api/google-products' },
    { key: 'demandgen',   path: '/api/google-demandgen' },
    { key: 'catalog',     path: '/api/meta-catalog' },
  ]
  // Sequential to avoid memory spike
  for (const t of tasks) {
    try {
      const r = await fetch(`http://127.0.0.1:${PORT}${t.path}`)
      const j = await r.json()
      results[t.key] = { count: (j.data || []).length, ok: j.ok }
    } catch (e) { errors[t.key] = e.message }
  }
  res.json({ ok: true, results, errors })
})

app.listen(PORT, () => console.log(`BlissClub proxy on port ${PORT}`))
