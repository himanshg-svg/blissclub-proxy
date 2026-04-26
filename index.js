import express from 'express'
import fetch from 'node-fetch'

const app  = express()
const PORT = process.env.PORT || 3001
app.use(express.json({ limit: '2mb' }))

const API_KEY       = process.env.WINDSOR_API_KEY
const META_ACCOUNT  = 'facebook__584820145452956'
const GA4_ACCOUNT   = 'googleanalytics4__344633503'
const GADS_ACCOUNT  = 'googleads__7746914820'

// ── Date helpers ──────────────────────────────────────────────────────────────
function fmt(d) {
  return d.toISOString().split('T')[0]
}

// Returns [{from, to}] for N sequential 30-day chunks covering last 60 days
// Chunk 0 = most recent 30 days, Chunk 1 = previous 30 days
function chunks60() {
  const now   = new Date()
  const day   = 24 * 60 * 60 * 1000
  return [
    { from: fmt(new Date(now - 29 * day)), to: fmt(now) },
    { from: fmt(new Date(now - 59 * day)), to: fmt(new Date(now - 30 * day)) },
  ]
}

// ── Core Windsor fetch — one chunk at a time, returns plain array ─────────────
async function windsorFetch(fields, account, from, to) {
  const url =
    `https://connectors.windsor.ai/v2?api_key=${API_KEY}` +
    `&date_from=${from}&date_to=${to}` +
    `&fields=${fields.join(',')}` +
    `&connector=${account}` +
    `&limit=50000`

  console.log(`[Windsor] ${account} ${from} → ${to}`)
  const res  = await fetch(url)
  const json = await res.json()
  // Windsor returns { data: [...] } or an array directly
  const rows = Array.isArray(json) ? json : (json.data || [])
  console.log(`[Windsor] got ${rows.length} rows`)
  return rows
}

// Fetch all 60 days SEQUENTIALLY, dedupe by key function
async function windsorFetch60(fields, account, dedupeKey) {
  const parts = chunks60()
  const seen  = new Map()

  for (const { from, to } of parts) {
    const rows = await windsorFetch(fields, account, from, to)
    for (const row of rows) {
      const k = dedupeKey ? dedupeKey(row) : JSON.stringify(row)
      if (!seen.has(k)) seen.set(k, row)
    }
    // rows goes out of scope here — GC can free it
  }

  return Array.from(seen.values())
}

// ── Endpoints ─────────────────────────────────────────────────────────────────

// Meta daily (spend + impressions + clicks by ad/adset/campaign per day)
app.get('/api/meta-daily', async (req, res) => {
  try {
    const fields  = ['date','campaign','adset_name','ad_name','spend','impressions','clicks','datasource']
    const data    = await windsorFetch60(fields, META_ACCOUNT,
      r => `${r.date}__${r.ad_name || ''}__${r.adset_name || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Meta + GA4 blended daily (revenue attribution)
app.get('/api/meta-ga4', async (req, res) => {
  try {
    const metaFields = ['date','campaign','adset_name','ad_name','spend','impressions','clicks','datasource']
    const ga4Fields  = ['date','campaign','session_manual_term','session_manual_ad_content',
                        'sessions','totalrevenue','transactions','datasource','source']

    // Strictly sequential — fetch Meta first, then GA4
    const metaData = await windsorFetch60(metaFields, META_ACCOUNT,
      r => `${r.date}__${r.ad_name || ''}__${r.adset_name || ''}__meta`)
    const ga4Data  = await windsorFetch60(ga4Fields,  GA4_ACCOUNT,
      r => `${r.date}__${r.campaign || ''}__${r.session_manual_ad_content || ''}__ga4`)

    const data = [...metaData, ...ga4Data]
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// GA4 only
app.get('/api/ga4', async (req, res) => {
  try {
    const fields = ['date','campaign','source','medium','sessions','totalrevenue','transactions','datasource']
    const data   = await windsorFetch60(fields, GA4_ACCOUNT,
      r => `${r.date}__${r.campaign || ''}__${r.source || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Google Ads campaigns
app.get('/api/google-campaigns', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const data   = await windsorFetch60(fields, GADS_ACCOUNT,
      r => `${r.date}__${r.campaign || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Google Ads search terms (capped at 5000 most recent to avoid memory spike)
app.get('/api/google-search-terms', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','search_term','impressions','clicks','spend','conversions']
    const data   = await windsorFetch60(fields, GADS_ACCOUNT,
      r => `${r.date}__${r.search_term || ''}__${r.campaign || ''}`)
    const capped = data.slice(0, 5000)
    res.json({ ok: true, data: capped, count: capped.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Google Ads keywords
app.get('/api/google-keywords', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','keyword','match_type','impressions','clicks','spend','conversions']
    const data   = await windsorFetch60(fields, GADS_ACCOUNT,
      r => `${r.date}__${r.keyword || ''}__${r.campaign || ''}`)
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Google Ads awareness (top-of-funnel campaigns)
app.get('/api/google-awareness', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch60(fields, GADS_ACCOUNT,
      r => `${r.date}__${r.campaign || ''}`)
    const data   = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('brand') || c.includes('awareness') || c.includes('reach')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Google Shopping / Products
app.get('/api/google-products', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch60(fields, GADS_ACCOUNT,
      r => `${r.date}__${r.campaign || ''}__${r.ad_group_name || ''}`)
    const data   = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('shopping') || c.includes('pmax') || c.includes('product')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Google Demand Gen
app.get('/api/google-demandgen', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch60(fields, GADS_ACCOUNT,
      r => `${r.date}__${r.campaign || ''}__${r.ad_group_name || ''}`)
    const data   = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('demand') || c.includes('demandgen') || c.includes('discovery')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// Meta Catalog / DPA
app.get('/api/meta-catalog', async (req, res) => {
  try {
    const fields = ['date','campaign','adset_name','ad_name','spend','impressions','clicks','conversions','conversion_value']
    const all    = await windsorFetch60(fields, META_ACCOUNT,
      r => `${r.date}__${r.ad_name || ''}__${r.adset_name || ''}`)
    const data   = all.filter(r => {
      const n = (r.campaign || r.adset_name || r.ad_name || '').toLowerCase()
      return n.includes('catalog') || n.includes('dpa') || n.includes('dco')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Sync-all: calls every endpoint sequentially ───────────────────────────────
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
      const r = await fetch(`http://127.0.0.1:${PORT}${t.path}`)
      const j = await r.json()
      results[t.key] = { count: (j.data || []).length, ok: j.ok }
    } catch (e) {
      errors[t.key] = e.message
    }
  }

  res.json({ ok: true, results, errors })
})

app.listen(PORT, () => console.log(`BlissClub proxy on port ${PORT}`))
