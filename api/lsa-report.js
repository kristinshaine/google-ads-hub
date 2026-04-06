export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { start_date, end_date, assigned_to } = req.body;

    // Get auth token — from env var or from request header
    const token = process.env.LSA_BEARER_TOKEN || (req.headers.authorization || '').replace('Bearer ', '');

    const upstream = await fetch(
      'https://google-ads-onboarding-production.up.railway.app/api/lsa/report',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Origin': 'https://pwmarketingpros-ads.vercel.app',
          'Referer': 'https://pwmarketingpros-ads.vercel.app/',
          ...(token ? { 'Authorization': 'Bearer ' + token } : {})
        },
        body: JSON.stringify({ start_date, end_date, assigned_to: assigned_to || null })
      }
    );

    const data = await upstream.json();
    return res.status(upstream.status).json(data);

  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
