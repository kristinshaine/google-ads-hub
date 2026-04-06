export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const {
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_MCC_ID,
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_REFRESH_TOKEN,
  } = process.env;

  const missing = ['GOOGLE_ADS_DEVELOPER_TOKEN','GOOGLE_ADS_MCC_ID','GOOGLE_OAUTH_CLIENT_ID','GOOGLE_OAUTH_CLIENT_SECRET','GOOGLE_OAUTH_REFRESH_TOKEN'].filter(k => !process.env[k]);
  if (missing.length) return res.status(500).json({ error: 'Missing env vars: ' + missing.join(', ') });

  try {
    // 1. Get access token via refresh token
    const tokenResp = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id:     GOOGLE_OAUTH_CLIENT_ID,
        client_secret: GOOGLE_OAUTH_CLIENT_SECRET,
        refresh_token: GOOGLE_OAUTH_REFRESH_TOKEN,
        grant_type:    'refresh_token',
      }),
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) return res.status(401).json({ error: 'OAuth failed', detail: tokenData });

    const accessToken = tokenData.access_token;
    const mccId = GOOGLE_ADS_MCC_ID.replace(/-/g, '');

    // 2. Query Google Ads API for all customer accounts under MCC
    const adsResp = await fetch(
      `https://googleads.googleapis.com/v17/customers/${mccId}/googleAds:searchStream`,
      {
        method: 'POST',
        headers: {
          'Authorization':          `Bearer ${accessToken}`,
          'developer-token':        GOOGLE_ADS_DEVELOPER_TOKEN,
          'login-customer-id':      mccId,
          'Content-Type':           'application/json',
        },
        body: JSON.stringify({
          query: `
            SELECT
              customer_client.client_customer,
              customer_client.descriptive_name,
              customer_client.id,
              customer_client.status,
              customer_client.manager,
              customer_client.level
            FROM customer_client
            WHERE customer_client.level <= 1
              AND customer_client.status = 'ENABLED'
              AND customer_client.manager = false
            ORDER BY customer_client.descriptive_name ASC
          `
        }),
      }
    );

    const adsText = await adsResp.text();
    if (!adsResp.ok) return res.status(adsResp.status).json({ error: 'Google Ads API error', detail: adsText });

    const adsData = JSON.parse(adsText);

    // 3. Parse accounts from response (searchStream returns array of batches)
    const accounts = [];
    for (const batch of adsData) {
      for (const row of (batch.results || [])) {
        const c = row.customerClient;
        if (!c || c.manager) continue;
        accounts.push({
          id:     c.id || '',
          name:   c.descriptiveName || c.id || 'Unknown',
          status: c.status || 'ENABLED',
        });
      }
    }

    // Dedupe by id
    const seen = new Set();
    const unique = accounts.filter(a => { if (seen.has(a.id)) return false; seen.add(a.id); return true; });
    unique.sort((a,b) => a.name.localeCompare(b.name));

    return res.status(200).json({ accounts: unique, total: unique.length });

  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
