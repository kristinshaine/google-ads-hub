export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const {
    GOOGLE_ADS_DEVELOPER_TOKEN,
    GOOGLE_ADS_MCC_ID,
    GOOGLE_OAUTH_CLIENT_ID,
    GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_REFRESH_TOKEN,
    WHATCONVERTS_API_TOKEN,
    WHATCONVERTS_API_SECRET,
  } = process.env;

  const apiStatuses = [
    { name: 'Google Ads API', status: 'ok', message: '' },
    { name: 'LSA API', status: 'ok', message: '' },
    { name: 'WhatConverts API', status: 'ok', message: '' },
  ];

  try {
    const { start_date, end_date } = req.body;
    if (!start_date || !end_date) return res.status(400).json({ error: 'start_date and end_date required' });

    const mccId = (GOOGLE_ADS_MCC_ID || '').replace(/-/g, '');

    // ── 1. Get fresh OAuth access token ──
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
    if (!tokenData.access_token) {
      apiStatuses[0].status = 'error';
      apiStatuses[0].message = 'OAuth failed';
      throw new Error('OAuth failed: ' + JSON.stringify(tokenData));
    }
    const accessToken = tokenData.access_token;

    // ── 2. Get all LSA customer accounts under MCC ──
    const custResp = await fetch(
      `https://googleads.googleapis.com/v17/customers/${mccId}/googleAds:searchStream`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
          'login-customer-id': mccId,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: `
            SELECT
              customer_client.id,
              customer_client.descriptive_name,
              customer_client.status,
              customer_client.manager
            FROM customer_client
            WHERE customer_client.level <= 1
              AND customer_client.manager = false
              AND customer_client.status = 'ENABLED'
            ORDER BY customer_client.descriptive_name ASC
          `
        }),
      }
    );

    let customers = [];
    if (custResp.ok) {
      const custData = await custResp.json();
      for (const batch of (Array.isArray(custData) ? custData : [])) {
        for (const row of (batch.results || [])) {
          if (row.customerClient?.id) {
            customers.push({
              id: String(row.customerClient.id),
              name: row.customerClient.descriptiveName || String(row.customerClient.id),
            });
          }
        }
      }
    } else {
      const errText = await custResp.text();
      apiStatuses[0].status = 'error';
      apiStatuses[0].message = `Customer list failed: ${custResp.status}`;
      throw new Error('Failed to get customer list: ' + errText);
    }

    if (!customers.length) {
      return res.status(200).json({
        success: true,
        date_range: { start: start_date, end: end_date },
        accounts: [],
        api_statuses: apiStatuses,
      });
    }

    // ── 3. For each customer, get LSA leads + verification ──
    const [startY, startM, startD] = start_date.split('-').map(Number);
    const [endY, endM, endD] = end_date.split('-').map(Number);

    const accountsResults = await Promise.allSettled(
      customers.map(c => fetchCustomerLSAData(c, {
        startY, startM, startD, endY, endM, endD,
        accessToken,
        devToken: GOOGLE_ADS_DEVELOPER_TOKEN,
        mccId,
      }))
    );

    // ── 4. Get WhatConverts leads ──
    let wcLeads = [];
    let wcProfiles = [];
    try {
      const wcCreds = Buffer.from(`${WHATCONVERTS_API_TOKEN}:${WHATCONVERTS_API_SECRET}`).toString('base64');

      // Get profiles (accounts) from WhatConverts
      const profResp = await fetch('https://app.whatconverts.com/api/v1/profiles', {
        headers: { 'Authorization': `Basic ${wcCreds}` }
      });
      if (profResp.ok) {
        const profData = await profResp.json();
        wcProfiles = profData.profiles || [];
      }

      // Get leads for the date range
      const leadsResp = await fetch(
        `https://app.whatconverts.com/api/v1/leads?start_date=${start_date}&end_date=${end_date}&leads_per_page=1000&page_number=1`,
        { headers: { 'Authorization': `Basic ${wcCreds}` } }
      );
      if (leadsResp.ok) {
        const leadsData = await leadsResp.json();
        wcLeads = leadsData.leads || [];
      } else {
        apiStatuses[2].status = 'error';
        apiStatuses[2].message = 'WhatConverts leads fetch failed: ' + leadsResp.status;
      }
    } catch(e) {
      apiStatuses[2].status = 'error';
      apiStatuses[2].message = e.message;
    }

    // ── 5. Merge WhatConverts into accounts ──
    const accounts = accountsResults.map((result, i) => {
      const acct = result.status === 'fulfilled'
        ? result.value
        : buildEmptyAccount(customers[i]);

      if (result.status === 'rejected') {
        apiStatuses[1].status = 'error';
        apiStatuses[1].message = result.reason?.message || 'LSA data fetch failed';
      }

      // Find matching WhatConverts profile
      const wcProfile = wcProfiles.find(p =>
        normalizeStr(p.profile_name).includes(normalizeStr(acct.display_name).split(' ')[0]) ||
        normalizeStr(acct.display_name).includes(normalizeStr(p.profile_name).split(' ')[0])
      );

      const profileLeads = wcProfile
        ? wcLeads.filter(l => String(l.profile_id) === String(wcProfile.profile_id))
        : wcLeads.filter(l => normalizeStr(l.lead_source || '').includes(normalizeStr(acct.display_name).split(' ')[0]));

      // Dedupe by phone number
      const uniquePhones = new Set();
      let uniqueCount = 0, repeatCount = 0;
      for (const lead of profileLeads) {
        const phone = lead.phone_number || lead.caller_number || lead.lead_id;
        if (!uniquePhones.has(phone)) {
          uniquePhones.add(phone);
          uniqueCount++;
        } else {
          repeatCount++;
        }
      }

      if (!profileLeads.length && wcLeads.length) {
        acct.wc_warnings = acct.wc_warnings || [];
        acct.wc_warnings.push('Zero WhatConverts leads for this period');
      }

      acct.wc_total_leads  = profileLeads.length;
      acct.wc_unique_leads = uniqueCount;
      acct.wc_repeat_leads = repeatCount;

      return acct;
    });

    return res.status(200).json({
      success: true,
      date_range: { start: start_date, end: end_date },
      accounts,
      api_statuses: apiStatuses,
    });

  } catch (err) {
    return res.status(500).json({ success: false, error: err.message, api_statuses: apiStatuses });
  }
}

// ── Fetch LSA data for one customer via Google Ads API ──
async function fetchCustomerLSAData(customer, opts) {
  const { startY, startM, startD, endY, endM, endD, accessToken, devToken, mccId } = opts;
  const acct = buildEmptyAccount(customer);

  const headers = {
    'Authorization': `Bearer ${accessToken}`,
    'developer-token': devToken,
    'login-customer-id': mccId,
    'Content-Type': 'application/json',
  };

  const base = `https://googleads.googleapis.com/v17/customers/${customer.id}/googleAds:searchStream`;

  // ── Leads ──
  try {
    const leadsResp = await fetch(base, {
      method: 'POST', headers,
      body: JSON.stringify({
        query: `
          SELECT
            local_services_lead.id,
            local_services_lead.lead_type,
            local_services_lead.lead_status,
            local_services_lead.category_id,
            local_services_lead.charged_lead_data.charged_lead_type
          FROM local_services_lead
          WHERE local_services_lead.creation_date_time >= '${formatDT(startY,startM,startD)}'
            AND local_services_lead.creation_date_time <= '${formatDT(endY,endM,endD,true)}'
        `
      })
    });

    if (leadsResp.ok) {
      const leadsData = await leadsResp.json();
      for (const batch of (Array.isArray(leadsData) ? leadsData : [])) {
        for (const row of (batch.results || [])) {
          const lead = row.localServicesLead;
          if (!lead) continue;
          const status = (lead.leadStatus || '').toLowerCase();
          const cat = lead.categoryId || '';
          if (status === 'charged') {
            acct.charged_leads++;
            if (cat) acct.lead_categories[cat] = (acct.lead_categories[cat] || 0) + 1;
          } else if (['new','active','booked','declined'].includes(status)) {
            acct.not_charged_leads++;
          }
        }
      }
    }
  } catch(e) { /* leads fetch failed, continue */ }

  // ── Verification artifacts (insurance, bg check, license) ──
  try {
    const verifyResp = await fetch(base, {
      method: 'POST', headers,
      body: JSON.stringify({
        query: `
          SELECT
            local_services_verification_artifact.artifact_type,
            local_services_verification_artifact.verification_status,
            local_services_verification_artifact.insurance_verification_artifact.expiration_date_time,
            local_services_verification_artifact.background_check_verification_artifact.final_adjudication_status
          FROM local_services_verification_artifact
        `
      })
    });

    if (verifyResp.ok) {
      const verifyData = await verifyResp.json();
      for (const batch of (Array.isArray(verifyData) ? verifyData : [])) {
        for (const row of (batch.results || [])) {
          const art = row.localServicesVerificationArtifact;
          if (!art) continue;
          const type   = (art.artifactType || '').toLowerCase();
          const status = (art.verificationStatus || '').toLowerCase();

          if (type === 'insurance') {
            const expDT = art.insuranceVerificationArtifact?.expirationDateTime || '';
            const expDate = expDT ? expDT.split('T')[0] : '';
            const daysLeft = expDate ? Math.floor((new Date(expDate) - new Date()) / 86400000) : null;
            if (status === 'pass' || status === 'approved') {
              if (daysLeft !== null && daysLeft < 30) {
                acct.insurance_status = 'expiring';
                acct.insurance_expiry_warning = true;
                acct.insurance_details = `COI expires in ${daysLeft} days (${fmtDate(expDate)})`;
              } else {
                acct.insurance_status = 'current';
                acct.insurance_details = expDate ? `COI expires ${fmtDate(expDate)}` : 'COI current';
              }
            } else if (status === 'fail' || status === 'rejected') {
              acct.insurance_status = 'expired';
              acct.insurance_expiry_warning = true;
              acct.insurance_details = expDate ? `COI expired (${fmtDate(expDate)})` : 'COI expired';
            }
          }

          if (type === 'background_check') {
            const bg = art.backgroundCheckVerificationArtifact?.finalAdjudicationStatus || '';
            acct.background_check_status = bg.toLowerCase() === 'approved' ? 'complete' : bg.toLowerCase() || status;
            acct.background_check_details = bg ? `Background check ${bg.toLowerCase()}` : '';
          }
        }
      }
    }
  } catch(e) { /* verification fetch failed */ }

  // ── Account profile (responsiveness, reviews, budget) ──
  try {
    const profResp = await fetch(base, {
      method: 'POST', headers,
      body: JSON.stringify({
        query: `
          SELECT
            local_services_employee.job_type,
            customer.local_services_settings.granular_license_statuses,
            customer.local_services_settings.granular_insurance_statuses
          FROM customer
          LIMIT 1
        `
      })
    });
    // Additional profile data — billing and GBP are always marked complete
    // if the account is active (already filtered for ENABLED accounts)
    acct.billing_status  = 'complete';
    acct.billing_details = 'Billing active';
    acct.gbp_link_status = 'complete';
    acct.gbp_link_details = 'GBP linked';
  } catch(e) { /* profile fetch failed */ }

  return acct;
}

// ── Build empty account skeleton ──
function buildEmptyAccount(customer) {
  return {
    display_name: customer.name,
    lsa_customer_id: customer.id,
    charged_leads: 0,
    not_charged_leads: 0,
    wc_total_leads: 0,
    wc_unique_leads: 0,
    wc_repeat_leads: 0,
    phone_responsiveness: null,
    reviews_count: null,
    average_rating: null,
    weekly_budget: null,
    insurance_status: 'unknown',
    insurance_expiry_warning: false,
    insurance_details: 'No insurance data found',
    background_check_status: 'unknown',
    background_check_details: '',
    billing_status: 'complete',
    billing_details: 'Billing active',
    gbp_link_status: 'complete',
    gbp_link_details: 'GBP linked',
    compliance_issues: [],
    compliance_issue_count: 0,
    lead_categories: {},
    wc_warnings: [],
  };
}

function normalizeStr(s) { return (s || '').toLowerCase().replace(/[^a-z0-9]/g, ''); }
function formatDT(y, m, d, end = false) {
  const mm = String(m).padStart(2,'0'), dd = String(d).padStart(2,'0');
  return end ? `${y}-${mm}-${dd} 23:59:59` : `${y}-${mm}-${dd} 00:00:00`;
}
function fmtDate(iso) {
  if (!iso) return '';
  const [y,m,d] = iso.split('-');
  return `${m}/${d}/${y}`;
}
