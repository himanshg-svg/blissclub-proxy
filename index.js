const express = require('express')
const fetch   = require('node-fetch')
const cors    = require('cors')

const app    = express()
const PORT   = process.env.PORT || 3001
const APIKEY = process.env.WINDSOR_API_KEY

const META_ACCOUNT = 'facebook__584820145452956'
const GA4_ACCOUNT  = 'googleanalytics4__344633503'
const GADS_ACCOUNT = 'googleads__7746914820'

app.use(cors())
app.options('*', cors())
app.use(express.json())

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.json({ ok: true, service: 'blissclub-proxy' }))

// ── Date helpers ──────────────────────────────────────────────────────────────
function fmt(d) {
  return d.toISOString().split('T')[0]
}

// 2 sequential 30-day chunks = 60 days total
function chunks60() {
  const now = new Date()
  const day = 24 * 60 * 60 * 1000
  return [
    { from: fmt(new Date(now - 29 * day)), to: fmt(now) },
    { from: fmt(new Date(now - 59 * day)), to: fmt(new Date(now - 30 * day)) },
  ]
}

// ── Core Windsor fetch — single chunk ─────────────────────────────────────────
async function windsorFetch(fields, account, from, to) {
  if (!APIKEY) throw new Error('WINDSOR_API_KEY not set')
  const params = new URLSearchParams({
    api_key:   APIKEY,
    date_from: from,
    date_to:   to,
    fields:    fields.join(','),
    connector: account,
    limit:     '50000',
  })
  const url = 'https://connectors.windsor.ai/all?' + params.toString()
  console.log('[Windsor]', account, from, '->', to)
  const res  = await fetch(url)
  const json = await res.json()
  const rows = Array.isArray(json) ? json : (json.data || [])
  console.log('[Windsor] got', rows.length, 'rows')
  return rows
}

// Fetch 60 days SEQUENTIALLY — frees each chunk from memory before next
async function windsorFetch60(fields, account, dedupeKey) {
  const parts = chunks60()
  const seen  = new Map()
  for (const { from, to } of parts) {
    const rows = await windsorFetch(fields, account, from, to)
    for (const row of rows) {
      const k = dedupeKey ? dedupeKey(row) : (row.date + JSON.stringify(row))
      if (!seen.has(k)) seen.set(k, row)
    }
    // rows freed here before next chunk starts
  }
  return Array.from(seen.values())
}

// ── Endpoints ─────────────────────────────────────────────────────────────────

app.get('/api/meta-daily', async (req, res) => {
  try {
    const fields = ['date','campaign','adset_name','ad_name','spend','impressions','clicks','datasource']
    const data   = await windsorFetch60(fields, META_ACCOUNT,
      r => r.date + '__' + (r.ad_name || '') + '__' + (r.adset_name || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/meta-ga4', async (req, res) => {
  try {
    const metaFields = ['date','campaign','adset_name','ad_name','spend','impressions','clicks','datasource']
    const ga4Fields  = ['date','campaign','session_manual_term','session_manual_ad_content',
                        'sessions','totalrevenue','transactions','datasource','source']
    // Strictly sequential — Meta first, then GA4
    const metaData = await windsorFetch60(metaFields, META_ACCOUNT,
      r => r.date + '__' + (r.ad_name || '') + '__meta')
    const ga4Data  = await windsorFetch60(ga4Fields, GA4_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__ga4')
    const data = [...metaData, ...ga4Data]
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/ga4', async (req, res) => {
  try {
    const fields = ['date','campaign','source','medium','sessions','totalrevenue','transactions','datasource']
    const data   = await windsorFetch60(fields, GA4_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.source || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-campaigns', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const data   = await windsorFetch60(fields, GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-search-terms', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','search_term','impressions','clicks','spend','conversions']
    const data   = await windsorFetch60(fields, GADS_ACCOUNT,
      r => r.date + '__' + (r.search_term || '') + '__' + (r.campaign || ''))
    const capped = data.slice(0, 5000) // cap to avoid memory spike
    res.json({ ok: true, data: capped, count: capped.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-keywords', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','keyword','match_type','impressions','clicks','spend','conversions']
    const data   = await windsorFetch60(fields, GADS_ACCOUNT,
      r => r.date + '__' + (r.keyword || '') + '__' + (r.campaign || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-awareness', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch60(fields, GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || ''))
    const data   = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('brand') || c.includes('awareness') || c.includes('reach')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-products', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch60(fields, GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.ad_group_name || ''))
    const data   = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('shopping') || c.includes('pmax') || c.includes('product')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-demandgen', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch60(fields, GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.ad_group_name || ''))
    const data   = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('demand') || c.includes('demandgen') || c.includes('discovery')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/meta-catalog', async (req, res) => {
  try {
    const fields = ['date','campaign','adset_name','ad_name','spend','impressions','clicks','conversions','conversion_value']
    const all    = await windsorFetch60(fields, META_ACCOUNT,
      r => r.date + '__' + (r.ad_name || '') + '__' + (r.adset_name || ''))
    const data   = all.filter(r => {
      const n = (r.campaign || r.adset_name || r.ad_name || '').toLowerCase()
      return n.includes('catalog') || n.includes('dpa') || n.includes('dco')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Sync-all: all endpoints sequentially ─────────────────────────────────────
app.get('/api/sync-all', async (req, res) => {
  const results = {}
  const errors  = {}
  const tasks   = [
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
  for (const t of tasks) {
    try {
      const r = await fetch('http://127.0.0.1:' + PORT + t.path)
      const j = await r.json()
      results[t.key] = { count: (j.data || []).length, ok: j.ok }
    } catch (e) {
      errors[t.key] = e.message
    }
  }
  res.json({ ok: true, results, errors })
})

app.listen(PORT, () => console.log('BlissClub proxy on port ' + PORT))
