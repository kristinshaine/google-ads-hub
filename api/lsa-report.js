export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const BASE = 'https://google-ads-onboarding-production.up.railway.app';
  const ORIGIN = 'https://pwmarketingpros-ads.vercel.app';

  try {
    const { start_date, end_date } = req.body;
    if (!start_date || !end_date) return res.status(400).json({ error: 'start_date and end_date required' });

    const email    = process.env.LSA_EMAIL;
    const password = process.env.LSA_PASSWORD;
    if (!email || !password) return res.status(500).json({ error: 'LSA_EMAIL and LSA_PASSWORD environment variables not set in Vercel' });

    // ── Step 1: Login to get a fresh token ──
    const loginResp = await fetch(`${BASE}/api/auth/login`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Origin':   ORIGIN,
        'Referer':  ORIGIN + '/',
      },
      body: JSON.stringify({ email, password }),
    });

    // Try alternate endpoints if first fails
    let token = null;
    if (loginResp.ok) {
      const loginData = await loginResp.json();
      token = loginData.token || loginData.access_token || loginData.accessToken || loginData.data?.token || loginData.data?.access_token;
    }

    // If /api/auth/login didn't work, try /api/login
    if (!token) {
      const login2Resp = await fetch(`${BASE}/api/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Origin': ORIGIN, 'Referer': ORIGIN + '/' },
        body: JSON.stringify({ email, password }),
      });
      if (login2Resp.ok) {
        const d = await login2Resp.json();
        token = d.token || d.access_token || d.accessToken || d.data?.token;
      }
    }

    // If still no token, try /auth/login
    if (!token) {
      const login3Resp = await fetch(`${BASE}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Origin': ORIGIN, 'Referer': ORIGIN + '/' },
        body: JSON.stringify({ email, password }),
      });
      if (login3Resp.ok) {
        const d = await login3Resp.json();
        token = d.token || d.access_token || d.accessToken || d.data?.token;
      }
    }

    if (!token) {
      return res.status(401).json({ error: 'Login failed — check LSA_EMAIL and LSA_PASSWORD in Vercel environment variables' });
    }

    // ── Step 2: Call the report API with fresh token ──
    const reportResp = await fetch(`${BASE}/api/lsa/report`, {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${token}`,
        'Origin':        ORIGIN,
        'Referer':       ORIGIN + '/',
      },
      body: JSON.stringify({ start_date, end_date, assigned_to: null }),
    });

    const data = await reportResp.json();
    return res.status(reportResp.status).json(data);

  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
