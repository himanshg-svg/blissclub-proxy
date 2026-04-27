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

function chunks30() {
  const now = new Date()
  const day = 24 * 60 * 60 * 1000
  return [
    { from: fmt(new Date(now - 29 * day)), to: fmt(now) },
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

// GA4-specific fetch using /all endpoint (connector URL doesn't work for GA4)
async function windsorFetchGA4(fields, account, from, to) {
  if (!APIKEY) throw new Error('WINDSOR_API_KEY not set')
  const params = new URLSearchParams({
    api_key:   APIKEY,
    date_from: from,
    date_to:   to,
    fields:    fields.join(','),
    connector: 'googleanalytics4',
    accounts:  account,
  })
  const url = 'https://connectors.windsor.ai/all?' + params.toString()
  console.log('[GA4] fetching', from, '->', to)
  const res  = await fetch(url)
  const json = await res.json()
  const rows = Array.isArray(json) ? json : (json.data || [])
  console.log('[GA4] got', rows.length, 'rows')
  return rows
}

// Sequential 30-day fetch — frees memory between chunks
async function windsorFetch30(fields, connector, account, dedupeKey) {
  const parts = chunks30()
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
    const metaFields = [
      'date','campaign','adset_name','ad_name',
      'spend','impressions','clicks','datasource',
      'purchase_roas_omni_purchase','actions_omni_purchase','action_values_omni_purchase',
    ]
    const ga4Fields = [
      'date','campaign','session_manual_ad_content','session_manual_term',
      'sessions','totalrevenue','transactions','datasource','source',
    ]
    const metaData = await windsorFetch30(metaFields, 'facebook', META_ACCOUNT,
      r => r.date + '__' + (r.ad_name || '') + '__' + (r.adset_name || ''))
    const ga4Raw   = await windsorFetch30(ga4Fields, 'googleanalytics4', GA4_ACCOUNT,
      r => r.date + '__' + (r.session_manual_ad_content || '') + '__' + (r.campaign || ''))
    // Keep only paid Meta sessions with real revenue
    const PAID_SOURCES = new Set(['ig','fb','facebook','paid','cpc','social'])
    const ga4Data  = ga4Raw.filter(r =>
      r.totalrevenue > 0 &&
      r.session_manual_ad_content &&
      r.session_manual_ad_content !== '(not set)' &&
      r.session_manual_ad_content !== 'sag_organic' &&
      PAID_SOURCES.has((r.source || '').toLowerCase())
    )
    console.log('[meta-daily] meta:', metaData.length, 'ga4 raw:', ga4Raw.length, 'ga4 filtered:', ga4Data.length)
    const data = [...metaData, ...ga4Data]
    res.json({ ok: true, data, count: data.length, meta: metaData.length, ga4: ga4Data.length })
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
    const all = await windsorFetch30(fields, 'facebook', META_ACCOUNT,
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
    const fields = ['date','campaign','session_manual_ad_content','source','medium','sessions','totalrevenue','transactions']
    const data   = await windsorFetch30(fields, 'googleanalytics4', GA4_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.session_manual_ad_content || '') + '__' + (r.source || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/meta-ga4', async (req, res) => {
  try {
    const metaFields = [
      'date','campaign','adset_name','ad_name',
      'spend','impressions','clicks','datasource',
      'purchase_roas_omni_purchase','actions_omni_purchase',
    ]
    const ga4Fields = [
      'date','campaign','session_manual_term','session_manual_ad_content',
      'sessions','totalrevenue','transactions','datasource','source',
    ]
    const metaData = await windsorFetch30(metaFields, 'facebook', META_ACCOUNT,
      r => r.date + '__' + (r.ad_name || '') + '__meta')
    const ga4Data  = await windsorFetch30(ga4Fields, 'googleanalytics4', GA4_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.session_manual_ad_content || '') + '__ga4')
    const data = [...metaData, ...ga4Data]
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Google Ads endpoints ──────────────────────────────────────────────────────

app.get('/api/google-campaigns', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const data   = await windsorFetch30(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-search-terms', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','search_term','impressions','clicks','spend','conversions']
    const data   = await windsorFetch30(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.search_term || '') + '__' + (r.campaign || ''))
    const capped = data.slice(0, 5000)
    res.json({ ok: true, data: capped, count: capped.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-keywords', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','keyword_text','keyword','match_type','impressions','clicks','spend','conversions']
    const data   = await windsorFetch30(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.keyword_text || r.keyword || '') + '__' + (r.campaign || ''))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-awareness', async (req, res) => {
  try {
    const fields = ['date','campaign','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch30(fields, 'google_ads', GADS_ACCOUNT,
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
    const fields = ['date','campaign','product_title','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch30(fields, 'google_ads', GADS_ACCOUNT,
      r => r.date + '__' + (r.campaign || '') + '__' + (r.product_title || ''))
    // Only rows with real product titles — filter out pure-pipe or near-empty titles
    const data   = all.filter(r => {
      if (!r.product_title) return false
      const cleaned = (r.product_title || '').replace(/[|\s]/g, '')
      return cleaned.length > 3
    })
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/google-demandgen', async (req, res) => {
  try {
    const fields = ['date','campaign','ad_group_name','ad_name','impressions','clicks','spend','conversions','conversion_value']
    const all    = await windsorFetch30(fields, 'google_ads', GADS_ACCOUNT,
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
    const data   = await windsorFetch30(fields, 'googleanalytics4', GA4_ACCOUNT,
      r => (r.item_id || '') + '__' + (r.date || ''))
    const enriched = data.map(r => ({
      ...r,
      variant_id: r.item_id ? r.item_id.split('_').pop() : null,
    }))
    res.json({ ok: true, data: enriched, count: enriched.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Sync-all: all endpoints sequentially ─────────────────────────────────────
app.get('/api/ga4-items', async (req, res) => {
  try {
    if (!APIKEY) throw new Error('WINDSOR_API_KEY not set')
    const now = new Date(), day = 24*60*60*1000
    const from = fmt(new Date(now - 29*day)), to = fmt(now)
    // Use /all endpoint with connector param — item fields need this endpoint
    const params = new URLSearchParams({
      api_key: APIKEY, date_from: from, date_to: to,
      fields: 'date,item_name,item_category,item_revenue,items_purchased,gross_item_revenue',
      connector: 'googleanalytics4', accounts: GA4_ACCOUNT, limit: '50000'
    })
    const res2 = await fetch('https://connectors.windsor.ai/all?' + params)
    const json = await res2.json()
    const raw  = Array.isArray(json) ? json : (json.data || [])
    const data = raw.filter(r => r.item_name && (Number(r.item_revenue||0) > 0 || Number(r.items_purchased||0) > 0))
    res.json({ ok: true, data, count: data.length })
  } catch (e) { res.status(500).json({ ok: false, error: e.message }) }
})

app.get('/api/sync-all', async (req, res) => {
  const results = {}
  const errors  = {}
  const tasks   = [
    { key: 'meta',        path: '/api/meta-daily' },
    { key: 'catalog',     path: '/api/meta-catalog' },
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



// ══════════════════════════════════════════════════════════════════════════════
// BACKEND — Auth, Users, Data Cache, Activity, Filters, Annotations
// All proxy routes above are UNCHANGED
// ══════════════════════════════════════════════════════════════════════════════

const bcrypt = require('bcryptjs')
const jwt    = require('jsonwebtoken')
const { Pool } = require('pg')
const cron   = require('node-cron')

const JWT_SECRET = process.env.JWT_SECRET || 'blissclub-secret-change-in-prod'

// ── Database ──────────────────────────────────────────────────────────────────
const pool = process.env.DATABASE_URL ? new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
}) : null

// ── Auth middleware ────────────────────────────────────────────────────────────
function authMiddleware(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1]
  if (!token) return res.status(401).json({ error: 'No token' })
  try { req.user = jwt.verify(token, JWT_SECRET); next() }
  catch (e) { res.status(401).json({ error: 'Invalid token' }) }
}

function adminOnly(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' })
  next()
}

// ── DB Setup ──────────────────────────────────────────────────────────────────
async function setupDB() {
  if (!pool) { console.log('[DB] No DATABASE_URL — skipping DB setup'); return }
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id         SERIAL PRIMARY KEY,
      name       TEXT NOT NULL,
      email      TEXT UNIQUE NOT NULL,
      password   TEXT NOT NULL,
      role       TEXT NOT NULL DEFAULT 'media_buyer',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS sync_cache (
      id         SERIAL PRIMARY KEY,
      endpoint   TEXT UNIQUE NOT NULL,
      data       JSONB NOT NULL,
      row_count  INTEGER DEFAULT 0,
      synced_at  TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS user_activity (
      id         SERIAL PRIMARY KEY,
      user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
      page       TEXT,
      action     TEXT,
      metadata   JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS saved_filters (
      id         SERIAL PRIMARY KEY,
      user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
      name       TEXT NOT NULL,
      page       TEXT NOT NULL,
      filters    JSONB NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS annotations (
      id          SERIAL PRIMARY KEY,
      user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
      entity_type TEXT NOT NULL,
      entity_id   TEXT NOT NULL,
      note        TEXT NOT NULL,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );
  `)
  console.log('[DB] Tables ready')
  const { rows } = await pool.query('SELECT COUNT(*) FROM users')
  if (rows[0].count === '0') {
    const hash = await bcrypt.hash('blissclub2024', 10)
    await pool.query(
      'INSERT INTO users (name, email, password, role) VALUES ($1,$2,$3,$4)',
      ['Admin', 'admin@blissclub.com', hash, 'admin']
    )
    console.log('[DB] Default admin: admin@blissclub.com / blissclub2024')
  }
}

// ── Windsor sync to DB ────────────────────────────────────────────────────────
const SYNC_ENDPOINTS = [
  { key: 'meta_daily',          path: '/api/meta-daily' },
  { key: 'meta_catalog',        path: '/api/meta-catalog' },
  { key: 'ga4',                 path: '/api/ga4' },
  { key: 'google_campaigns',    path: '/api/google-campaigns' },
  { key: 'google_search_terms', path: '/api/google-search-terms' },
  { key: 'google_keywords',     path: '/api/google-keywords' },
  { key: 'google_awareness',    path: '/api/google-awareness' },
  { key: 'google_products',     path: '/api/google-products' },
  { key: 'google_demandgen',    path: '/api/google-demandgen' },
  { key: 'ga4_items',            path: '/api/ga4-items' },
]

async function syncAndCache(endpoint) {
  if (!pool) return
  try {
    const res  = await fetch('http://127.0.0.1:' + PORT + endpoint.path)
    const json = await res.json()
    const data = json.data || []
    await pool.query(`
      INSERT INTO sync_cache (endpoint, data, row_count, synced_at)
      VALUES ($1,$2,$3,NOW())
      ON CONFLICT (endpoint) DO UPDATE SET data=$2, row_count=$3, synced_at=NOW()
    `, [endpoint.key, JSON.stringify(data), data.length])
    console.log('[Cache]', endpoint.key, data.length, 'rows')
  } catch (e) { console.error('[Cache]', endpoint.key, e.message) }
}

async function syncAllToCache() {
  console.log('[Sync] Starting scheduled sync...')
  for (const ep of SYNC_ENDPOINTS) await syncAndCache(ep)
  console.log('[Sync] Done')
}

// Schedule every 6 hours
cron.schedule('0 */6 * * *', syncAllToCache)

// ── Auth routes ───────────────────────────────────────────────────────────────
app.post('/auth/login', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not configured' })
  const { email, password } = req.body
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' })
  try {
    const { rows } = await pool.query('SELECT * FROM users WHERE email=$1', [email.toLowerCase()])
    if (!rows.length) return res.status(401).json({ error: 'Invalid credentials' })
    const user = rows[0]
    if (!await bcrypt.compare(password, user.password)) return res.status(401).json({ error: 'Invalid credentials' })
    const token = jwt.sign({ id:user.id, email:user.email, name:user.name, role:user.role }, JWT_SECRET, { expiresIn:'7d' })
    await pool.query('INSERT INTO user_activity (user_id,page,action) VALUES ($1,$2,$3)', [user.id,'auth','login'])
    res.json({ token, user:{ id:user.id, name:user.name, email:user.email, role:user.role } })
  } catch (e) { res.status(500).json({ error: e.message }) }
})

app.get('/auth/me', authMiddleware, async (req, res) => {
  const { rows } = await pool.query('SELECT id,name,email,role,created_at FROM users WHERE id=$1', [req.user.id])
  res.json(rows[0])
})

// ── User management ───────────────────────────────────────────────────────────
app.get('/users', authMiddleware, adminOnly, async (req, res) => {
  const { rows } = await pool.query('SELECT id,name,email,role,created_at FROM users ORDER BY created_at')
  res.json(rows)
})

app.post('/users', authMiddleware, adminOnly, async (req, res) => {
  const { name, email, password, role = 'media_buyer' } = req.body
  if (!name||!email||!password) return res.status(400).json({ error: 'Name, email and password required' })
  try {
    const hash = await bcrypt.hash(password, 10)
    const { rows } = await pool.query(
      'INSERT INTO users (name,email,password,role) VALUES ($1,$2,$3,$4) RETURNING id,name,email,role',
      [name, email.toLowerCase(), hash, role]
    )
    res.json(rows[0])
  } catch (e) {
    if (e.code==='23505') return res.status(400).json({ error: 'Email already exists' })
    res.status(500).json({ error: e.message })
  }
})

app.delete('/users/:id', authMiddleware, adminOnly, async (req, res) => {
  if (parseInt(req.params.id)===req.user.id) return res.status(400).json({ error: 'Cannot delete yourself' })
  await pool.query('DELETE FROM users WHERE id=$1', [req.params.id])
  res.json({ ok: true })
})

app.patch('/users/:id/password', authMiddleware, async (req, res) => {
  if (req.user.role!=='admin' && parseInt(req.params.id)!==req.user.id) return res.status(403).json({ error: 'Forbidden' })
  const { password } = req.body
  if (!password||password.length<6) return res.status(400).json({ error: 'Password must be 6+ chars' })
  const hash = await bcrypt.hash(password, 10)
  await pool.query('UPDATE users SET password=$1 WHERE id=$2', [hash, req.params.id])
  res.json({ ok: true })
})

// ── Cached data routes (used by dashboard instead of direct Windsor calls) ────
app.get('/data/:endpoint', authMiddleware, async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'DB not configured' })
  const { rows } = await pool.query(
    'SELECT data,row_count,synced_at FROM sync_cache WHERE endpoint=$1',
    [req.params.endpoint]
  )
  if (!rows.length) return res.json({ data:[], synced_at:null, row_count:0 })
  res.json({ data:rows[0].data, synced_at:rows[0].synced_at, row_count:rows[0].row_count })
})

app.get('/data', authMiddleware, async (req, res) => {
  const { rows } = await pool.query('SELECT endpoint,row_count,synced_at FROM sync_cache ORDER BY endpoint')
  res.json(rows)
})

// ── Manual sync trigger ───────────────────────────────────────────────────────
app.post('/sync', authMiddleware, adminOnly, async (req, res) => {
  res.json({ ok:true, message:'Sync started in background' })
  syncAllToCache()
})

// ── Activity logging ──────────────────────────────────────────────────────────
app.post('/activity', authMiddleware, async (req, res) => {
  if (!pool) return res.json({ ok: true })
  const { page, action, metadata } = req.body
  await pool.query(
    'INSERT INTO user_activity (user_id,page,action,metadata) VALUES ($1,$2,$3,$4)',
    [req.user.id, page, action, JSON.stringify(metadata||{})]
  )
  res.json({ ok: true })
})

app.get('/activity', authMiddleware, adminOnly, async (req, res) => {
  const { rows } = await pool.query(`
    SELECT a.id,u.name,u.email,a.page,a.action,a.metadata,a.created_at
    FROM user_activity a JOIN users u ON u.id=a.user_id
    ORDER BY a.created_at DESC LIMIT 200
  `)
  res.json(rows)
})

// ── Saved filters ─────────────────────────────────────────────────────────────
app.get('/filters', authMiddleware, async (req, res) => {
  const { rows } = await pool.query(
    'SELECT * FROM saved_filters WHERE user_id=$1 ORDER BY created_at DESC', [req.user.id])
  res.json(rows)
})

app.post('/filters', authMiddleware, async (req, res) => {
  const { name, page, filters } = req.body
  const { rows } = await pool.query(
    'INSERT INTO saved_filters (user_id,name,page,filters) VALUES ($1,$2,$3,$4) RETURNING *',
    [req.user.id, name, page, JSON.stringify(filters)]
  )
  res.json(rows[0])
})

app.delete('/filters/:id', authMiddleware, async (req, res) => {
  await pool.query('DELETE FROM saved_filters WHERE id=$1 AND user_id=$2', [req.params.id, req.user.id])
  res.json({ ok: true })
})

// ── Annotations ───────────────────────────────────────────────────────────────
app.get('/annotations/:type/:id', authMiddleware, async (req, res) => {
  const { rows } = await pool.query(`
    SELECT a.*,u.name as author FROM annotations a
    JOIN users u ON u.id=a.user_id
    WHERE a.entity_type=$1 AND a.entity_id=$2
    ORDER BY a.created_at DESC
  `, [req.params.type, req.params.id])
  res.json(rows)
})

app.post('/annotations', authMiddleware, async (req, res) => {
  const { entity_type, entity_id, note } = req.body
  const { rows } = await pool.query(
    'INSERT INTO annotations (user_id,entity_type,entity_id,note) VALUES ($1,$2,$3,$4) RETURNING *',
    [req.user.id, entity_type, entity_id, note]
  )
  res.json(rows[0])
})

app.delete('/annotations/:id', authMiddleware, async (req, res) => {
  await pool.query('DELETE FROM annotations WHERE id=$1 AND user_id=$2', [req.params.id, req.user.id])
  res.json({ ok: true })
})

// ── Start ─────────────────────────────────────────────────────────────────────
setupDB().then(() => {
  app.listen(PORT, () => {
    console.log('BlissClub proxy+backend on port ' + PORT)
    // Initial cache fill if empty
    if (pool) {
      pool.query('SELECT COUNT(*) FROM sync_cache').then(({ rows }) => {
        if (rows[0].count === '0') {
          console.log('[Startup] Cache empty — running initial sync in 30s...')
          setTimeout(syncAllToCache, 30000)
        }
      }).catch(() => {})
    }
  })
})
