const express = require('express')
const fetch   = require('node-fetch')
const cors    = require('cors')

const app    = express()
const PORT   = process.env.PORT || 3001
const APIKEY = process.env.WINDSOR_API_KEY

const META_ACCOUNT = '584820145452956'
const GA4_ACCOUNT  = '344633503'
const GADS_ACCOUNT = '8581973435'

app.use(cors())
app.options('*', cors())
app.use(express.json())

app.get('/', (req, res) => res.json({ ok: true, service: 'blissclub-proxy' }))

// ── Date helpers ──────────────────────────────────────────────────────────────
function fmt(d) {
  return d.toISOString().split('T')[0]
}

function chunks60() {
  const now = new Date()
  const day = 24 * 60 * 60 * 1000
  return [
    { from: fmt(new Date(now - 29 * day)), to: fmt(now) },
    { from: fmt(new Date(now - 59 * day)), to: fmt(new Date(now - 30 * day)) },
  ]
}

// ── Core Windsor fetch ────────────────────────────────────────────────────────
async function windsorFetch(fields, connector, account, from, to) {
  if (!APIKEY) throw new Error('WINDSOR_API_KEY not set')
  const params = new URLSearchParams({
    api_key:   APIKEY,
    date_from: from,
    date_to:   to,
    fields:    fields.join(','),
    accounts:  account,
  })
  const url = 'https://connectors.windsor.ai/' + connector + '?' + params.toString()
  console.log('[Windsor]', connector, account, from, '->', to)
  const res  = await fetch(url)
  const json = await res.json()
  const rows = Array.isArray(json) ? json : (json.data || [])
  console.log('[Windsor] got', rows.length, 'rows')
  return rows
}

// Sequential 60-day fetch — frees memory between chunks
async function windsorFetch60(fields, connector, account, dedupeKey) {
  const parts = chunks60()
  const seen  = new Map()
  for (const { from, to } of parts) {
    const rows = await windsorFetch(fields, connector, account, from, to)
    for (const row of rows) {
      const k = dedupeKey ? dedupeKey(row) : (row.date + JSON.stringify(row))
      if (!seen.has(k)) seen.set(k, row)
    }
  }
  return Array.from(seen.values())
}

// ── Meta endpoints ────────────────────────────────────────────────────────────

app.get('/api/meta-daily', async (req, res) => {
  try {
    const fields = ['date','campaign','adset_name','ad_name','spend','impressions','clicks']
    const data   = await windsorFetch60(fields, 'facebook', META_ACCOUNT,
      r => r.date + '__' + (r.ad_name || '') + '__' + (r.adset_name || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/meta-catalog', async (req, res) => {
  try {
    const fields = [
      'date','campaign','adset_name','product_id',
      'spend','impressions','clicks',
      'actions_purchase','action_values_purchase',
      'actions_add_to_cart','actions_view_content',
    ]
    const all = await windsorFetch60(fields, 'facebook', META_ACCOUNT,
      r => r.date + '__' + (r.product_id || '') + '__' + (r.campaign || ''))
    const data = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('catalog') || c.includes('dpa')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── GA4 endpoints ─────────────────────────────────────────────────────────────

app.get('/api/ga4', async (req, res) => {
  try {
    const fields = ['date','campaign','source','medium','sessions','totalrevenue','transactions']
    const data   = await windsorFetch60(fields, 'googleanalytics4', GA4_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.source || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/meta-ga4', async (req, res) => {
  try {
    const metaFields = ['date','campaign','adset_name','ad_name','spend','impressions','clicks']
    const ga4Fields  = ['date','campaign','source','medium','sessions','totalrevenue','transactions']
    // Strictly sequential — Meta fully done before GA4 starts
    const metaData = await windsorFetch60(metaFields, 'facebook', META_ACCOUNT,
      r => r.date + '__' + (r.ad_name || '') + '__meta')
    const ga4Data  = await windsorFetch60(ga4Fields, 'googleanalytics4', GA4_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__ga4')
    const data = [...metaData, ...ga4Data]
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Google Ads endpoints ──────────────────────────────────────────────────────

app.get('/api/google-campaigns', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const data   = await windsorFetch60(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-search-terms', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','search_term','impressions','clicks','spend','conversions']
    const data   = await windsorFetch60(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.search_term || '') + '__' + (r.campaign || ''))
    const capped = data.slice(0, 5000)
    res.json({ ok: true, data: capped, count: capped.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-keywords', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','keyword','match_type','impressions','clicks','spend','conversions']
    const data   = await windsorFetch60(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.keyword || '') + '__' + (r.campaign || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-awareness', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch60(fields, 'google_ads', GADS_ACCOUNT,
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
    const all    = await windsorFetch60(fields, 'google_ads', GADS_ACCOUNT,
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
    const all    = await windsorFetch60(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.ad_group_name || ''))
    const data   = all.filter(r => {
      const c = (r.campaign || '').toLowerCase()
      return c.includes('demand') || c.includes('demandgen') || c.includes('discovery')
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── GA4 item-level endpoint ───────────────────────────────────────────────────
// item_id format: shopify_IN_{product_id}_{variant_id}
// variant_id (last segment after final _) matches Meta product_id number
app.get('/api/ga4-items', async (req, res) => {
  try {
    const fields = ['date','item_id','item_name','item_revenue','item_views','item_quantity']
    const data   = await windsorFetch60(fields, 'googleanalytics4', GA4_ACCOUNT,
      r => (r.item_id || '') + '__' + (r.date || ''))
    const enriched = data.map(r => ({
      ...r,
      variant_id: r.item_id ? r.item_id.split('_').pop() : null,
    }))
    res.json({ ok: true, data: enriched, count: enriched.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Sync-all: all endpoints sequentially ─────────────────────────────────────
app.get('/api/sync-all', async (req, res) => {
  const results = {}
  const errors  = {}
  const tasks   = [
    { key: 'meta',        path: '/api/meta-daily' },
    { key: 'catalog',     path: '/api/meta-catalog' },
    { key: 'ga4Items',   path: '/api/ga4-items' },
    { key: 'ga4',         path: '/api/ga4' },
    { key: 'metaGa4',    path: '/api/meta-ga4' },
    { key: 'google',      path: '/api/google-campaigns' },
    { key: 'searchTerms', path: '/api/google-search-terms' },
    { key: 'keywords',    path: '/api/google-keywords' },
    { key: 'awareness',   path: '/api/google-awareness' },
    { key: 'products',    path: '/api/google-products' },
    { key: 'demandgen',   path: '/api/google-demandgen' },
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
