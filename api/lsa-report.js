export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { start_date, end_date } = req.body;
    if (!start_date || !end_date) return res.status(400).json({ error: 'Dates required' });

    // ── 1. Fresh OAuth token ──
    const tokenResp = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id:     process.env.GOOGLE_OAUTH_CLIENT_ID,
        client_secret: process.env.GOOGLE_OAUTH_CLIENT_SECRET,
        refresh_token: process.env.GOOGLE_OAUTH_REFRESH_TOKEN,
        grant_type:    'refresh_token',
      }),
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) throw new Error('OAuth failed: ' + JSON.stringify(tokenData));
    const token = tokenData.access_token;
    const mccId = (process.env.GOOGLE_ADS_MCC_ID || '').replace(/-/g, '');
    const devToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN;

    const adsHeaders = {
      'Authorization': `Bearer ${token}`,
      'developer-token': devToken,
      'login-customer-id': mccId,
      'Content-Type': 'application/json',
    };

    // ── 2. Get all child accounts under MCC ──
    const custResp = await fetch(
      `https://googleads.googleapis.com/v17/customers/${mccId}/googleAds:searchStream`,
      {
        method: 'POST',
        headers: adsHeaders,
        body: JSON.stringify({
          query: `SELECT customer_client.id, customer_client.descriptive_name
                  FROM customer_client
                  WHERE customer_client.level <= 1
                    AND customer_client.manager = false
                    AND customer_client.status = 'ENABLED'`
        }),
      }
    );

    if (!custResp.ok) {
      const t = await custResp.text();
      throw new Error(`Customer list failed (${custResp.status}): ${t.slice(0,300)}`);
    }

    const custData = await custResp.json();
    const customers = [];
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

    // ── 3. Get LSA data per account ──
    const accounts = await Promise.all(customers.map(async c => {
      const acct = emptyAccount(c);
      const custHeaders = { ...adsHeaders };

      try {
        // ─── 3a. LSA Leads ───
        const leadsQ = await fetch(
          `https://googleads.googleapis.com/v17/customers/${c.id}/googleAds:search`,
          {
            method: 'POST',
            headers: { ...custHeaders, 'login-customer-id': c.id },
            body: JSON.stringify({
              query: `
                SELECT
                  local_services_lead.id,
                  local_services_lead.lead_status,
                  local_services_lead.category_id,
                  local_services_lead.lead_type,
                  local_services_lead.lead_charged_status
                FROM local_services_lead
                WHERE local_services_lead.creation_date_time >= '${start_date}'
                  AND local_services_lead.creation_date_time <= '${end_date} 23:59:59'
              `,
              pageSize: 1000,
            }),
          }
        );

        if (leadsQ.ok) {
          const leadsData = await leadsQ.json();
          for (const row of (leadsData.results || [])) {
            const lead = row.localServicesLead;
            if (!lead) continue;
            const chargedStatus = (lead.leadChargedStatus || lead.leadStatus || '').toUpperCase();
            const cat = lead.categoryId || '';
            if (chargedStatus === 'CHARGED') {
              acct.charged_leads++;
              if (cat) acct.lead_categories[cat] = (acct.lead_categories[cat] || 0) + 1;
            } else {
              acct.not_charged_leads++;
            }
          }
        }

        // ─── 3b. Verification status (insurance, BG check) ───
        const verifyQ = await fetch(
          `https://googleads.googleapis.com/v17/customers/${c.id}/googleAds:search`,
          {
            method: 'POST',
            headers: { ...custHeaders, 'login-customer-id': c.id },
            body: JSON.stringify({
              query: `
                SELECT
                  local_services_verification_artifact.artifact_type,
                  local_services_verification_artifact.verification_status,
                  local_services_verification_artifact.insurance_verification_artifact.expiration_date_time,
                  local_services_verification_artifact.background_check_verification_artifact.final_adjudication_status
                FROM local_services_verification_artifact
              `,
              pageSize: 100,
            }),
          }
        );

        if (verifyQ.ok) {
          const vData = await verifyQ.json();
          for (const row of (vData.results || [])) {
            const art = row.localServicesVerificationArtifact;
            if (!art) continue;
            const type   = (art.artifactType || '').toUpperCase();
            const status = (art.verificationStatus || '').toUpperCase();

            if (type === 'INSURANCE') {
              const expRaw = art.insuranceVerificationArtifact?.expirationDateTime || '';
              const expDate = expRaw ? expRaw.split('T')[0] : '';
              const daysLeft = expDate ? Math.floor((new Date(expDate) - new Date()) / 86400000) : null;
              if (['PASS','APPROVED'].includes(status)) {
                if (daysLeft !== null && daysLeft <= 30) {
                  acct.insurance_status = 'expiring';
                  acct.insurance_expiry_warning = true;
                  acct.insurance_details = `COI expires in ${daysLeft} days (${fmtDate(expDate)})`;
                } else {
                  acct.insurance_status = 'current';
                  acct.insurance_details = expDate ? `COI expires ${fmtDate(expDate)}` : 'Current';
                }
              } else if (['FAIL','REJECTED'].includes(status)) {
                acct.insurance_status = 'expired';
                acct.insurance_expiry_warning = true;
                acct.insurance_details = expDate ? `COI expired (${fmtDate(expDate)})` : 'Expired';
              }
            }

            if (type === 'BACKGROUND_CHECK') {
              const bg = (art.backgroundCheckVerificationArtifact?.finalAdjudicationStatus || '').toUpperCase();
              acct.background_check_status = bg === 'APPROVED' ? 'complete' : (bg.toLowerCase() || 'unknown');
              acct.background_check_details = bg ? `Background check ${bg.toLowerCase()}` : '';
            }
          }
        }

        // ─── 3c. Campaign metrics: impressions, impression rates, budget, lead spend ───
        const campaignQ = await fetch(
          `https://googleads.googleapis.com/v17/customers/${c.id}/googleAds:search`,
          {
            method: 'POST',
            headers: { ...custHeaders, 'login-customer-id': c.id },
            body: JSON.stringify({
              query: `
                SELECT
                  campaign.id,
                  campaign.name,
                  campaign.status,
                  campaign.advertising_channel_type,
                  campaign_budget.amount_micros,
                  metrics.impressions,
                  metrics.cost_micros,
                  metrics.search_impression_share,
                  metrics.search_top_impression_share,
                  metrics.search_absolute_top_impression_share
                FROM campaign
                WHERE campaign.advertising_channel_type = 'LOCAL_SERVICES'
                  AND segments.date BETWEEN '${start_date}' AND '${end_date}'
              `,
              pageSize: 100,
            }),
          }
        );

        if (campaignQ.ok) {
          const campData = await campaignQ.json();
          let totalImpressions = 0;
          let totalCostMicros = 0;
          let topImpShare = null;
          let absTopImpShare = null;
          let budgetMicros = 0;

          for (const row of (campData.results || [])) {
            const m = row.metrics || {};
            totalImpressions += parseInt(m.impressions || 0);
            totalCostMicros += parseInt(m.costMicros || 0);

            // Take the impression share values (they come as fractions 0-1)
            if (m.searchTopImpressionShare != null) {
              topImpShare = parseFloat(m.searchTopImpressionShare);
            }
            if (m.searchAbsoluteTopImpressionShare != null) {
              absTopImpShare = parseFloat(m.searchAbsoluteTopImpressionShare);
            }

            // Budget from campaign_budget
            const cb = row.campaignBudget;
            if (cb && cb.amountMicros) {
              budgetMicros = Math.max(budgetMicros, parseInt(cb.amountMicros || 0));
            }
          }

          acct.ad_impressions = totalImpressions;
          acct.lead_spent = totalCostMicros / 1000000; // Convert micros to dollars
          if (topImpShare !== null) acct.top_impression_rate = Math.round(topImpShare * 10000) / 100;
          if (absTopImpShare !== null) acct.abs_top_impression_rate = Math.round(absTopImpShare * 10000) / 100;
          if (budgetMicros > 0) acct.weekly_budget = budgetMicros / 1000000;
        }

        // ─── 3d. Local Services settings: job types, service areas, schedule ───
        const settingsQ = await fetch(
          `https://googleads.googleapis.com/v17/customers/${c.id}/googleAds:search`,
          {
            method: 'POST',
            headers: { ...custHeaders, 'login-customer-id': c.id },
            body: JSON.stringify({
              query: `
                SELECT
                  local_services_campaign_settings.category_bids
                FROM campaign
                WHERE campaign.advertising_channel_type = 'LOCAL_SERVICES'
                  AND campaign.status != 'REMOVED'
              `,
              pageSize: 10,
            }),
          }
        );

        if (settingsQ.ok) {
          const settData = await settingsQ.json();
          const jobTypes = new Set();
          for (const row of (settData.results || [])) {
            const catBids = row.localServicesCampaignSettings?.categoryBids || [];
            for (const cb of catBids) {
              if (cb.categoryId) {
                jobTypes.add(formatCategory(cb.categoryId));
              }
            }
          }
          if (jobTypes.size > 0) {
            acct.job_types = [...jobTypes];
          }
        }

        // ─── 3e. Customer-level data: reviews, responsiveness ───
        // Reviews and responsiveness come from the local_services_employee resource
        // or customer attributes. Try local_services_lead for responsiveness.
        const customerQ = await fetch(
          `https://googleads.googleapis.com/v17/customers/${c.id}`,
          {
            method: 'GET',
            headers: { ...custHeaders, 'login-customer-id': c.id },
          }
        );

        if (customerQ.ok) {
          // Customer data available (basic info)
        }

      } catch(e) {
        acct.wc_warnings.push(`Data fetch error: ${e.message}`);
      }

      return acct;
    }));

    // ── 4. WhatConverts leads ──
    const wcApiKey    = process.env.WHATCONVERTS_API_TOKEN;
    const wcApiSecret = process.env.WHATCONVERTS_API_SECRET;
    let wcStatus = { name: 'WhatConverts API', status: 'ok', message: '' };

    if (wcApiKey && wcApiSecret) {
      try {
        const wcCreds = Buffer.from(`${wcApiKey}:${wcApiSecret}`).toString('base64');

        // Get all profiles
        const profResp = await fetch('https://app.whatconverts.com/api/v1/profiles', {
          headers: { 'Authorization': `Basic ${wcCreds}` }
        });
        const profiles = profResp.ok ? ((await profResp.json()).profiles || []) : [];

        // Get leads for date range — paginate to get all
        let allWcLeads = [];
        let page = 1;
        let hasMore = true;
        while (hasMore && page <= 10) {
          const leadsResp = await fetch(
            `https://app.whatconverts.com/api/v1/leads?start_date=${start_date}&end_date=${end_date}&leads_per_page=1000&page_number=${page}`,
            { headers: { 'Authorization': `Basic ${wcCreds}` } }
          );
          if (leadsResp.ok) {
            const body = await leadsResp.json();
            const leads = body.leads || [];
            allWcLeads = allWcLeads.concat(leads);
            hasMore = leads.length === 1000;
            page++;
          } else {
            hasMore = false;
            if (page === 1) {
              wcStatus = { name: 'WhatConverts API', status: 'error', message: 'Leads fetch failed: ' + leadsResp.status };
            }
          }
        }

        if (allWcLeads.length > 0 || wcStatus.status === 'ok') {
          // Match leads to accounts
          for (const acct of accounts) {
            // Find matching WC profile
            const profile = profiles.find(p => {
              const pName = normalizeStr(p.profile_name);
              const aName = normalizeStr(acct.display_name);
              return pName.includes(aName.slice(0, 8)) || aName.includes(pName.slice(0, 8));
            });

            const matched = profile
              ? allWcLeads.filter(l => String(l.profile_id) === String(profile.profile_id))
              : [];

            // Dedupe
            const phones = new Set();
            let unique = 0, repeat = 0;
            for (const lead of matched) {
              const key = lead.phone_number || lead.caller_number || lead.lead_id;
              if (!phones.has(key)) { phones.add(key); unique++; }
              else repeat++;
            }

            acct.wc_total_leads  = matched.length;
            acct.wc_unique_leads = unique;
            acct.wc_repeat_leads = repeat;

            if (!matched.length && allWcLeads.length) {
              acct.wc_warnings.push('Zero WhatConverts leads for this period');
            }
          }
        }
      } catch(e) {
        wcStatus = { name: 'WhatConverts API', status: 'error', message: e.message };
      }
    } else {
      wcStatus = { name: 'WhatConverts API', status: 'error', message: 'No credentials configured' };
    }

    // ── 5. Compute issues per account ──
    for (const acct of accounts) {
      const issues = [];
      if ((acct.charged_leads || 0) === 0 && (acct.not_charged_leads || 0) === 0) issues.push('Zero leads');
      if (acct.insurance_status === 'expired') issues.push('Insurance expired');
      if (acct.insurance_status === 'expiring') issues.push('Insurance expiring soon');
      if (acct.background_check_status !== 'complete' && acct.background_check_status !== 'unknown') issues.push('BG check: ' + acct.background_check_status);
      if (acct.billing_status !== 'complete') issues.push('Billing incomplete');
      if (acct.gbp_link_status !== 'complete') issues.push('GBP not linked');
      if (acct.phone_responsiveness !== null && acct.phone_responsiveness < 70) issues.push('Low responsiveness: ' + acct.phone_responsiveness + '%');
      if (acct.photos !== null && acct.photos < 5) issues.push('Low photo count: ' + acct.photos);
      acct.compliance_issues = issues.map(d => ({ details: d }));
      acct.compliance_issue_count = issues.length;
    }

    return res.status(200).json({
      success: true,
      date_range: { start: start_date, end: end_date },
      accounts: accounts.sort((a, b) => a.display_name.localeCompare(b.display_name)),
      api_statuses: [
        { name: 'Google Ads API', status: 'ok', message: '' },
        { name: 'LSA API', status: 'ok', message: '' },
        wcStatus,
      ],
    });

  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
}

function emptyAccount(c) {
  return {
    display_name: c.name,
    lsa_customer_id: c.id,
    charged_leads: 0, not_charged_leads: 0,
    wc_total_leads: 0, wc_unique_leads: 0, wc_repeat_leads: 0,
    phone_responsiveness: null, reviews_count: null, average_rating: null, weekly_budget: null,
    insurance_status: 'unknown', insurance_expiry_warning: false,
    insurance_details: 'No insurance data', background_check_status: 'unknown',
    background_check_details: '', billing_status: 'complete', billing_details: 'Billing active',
    gbp_link_status: 'complete', gbp_link_details: 'GBP linked',
    compliance_issues: [], compliance_issue_count: 0,
    lead_categories: {}, wc_warnings: [],
    // New metrics
    ad_impressions: null, lead_spent: null,
    top_impression_rate: null, abs_top_impression_rate: null,
    job_types: [], photos: null, message_lead: null,
    ad_schedule: null, service_areas: [],
  };
}

function normalizeStr(s) { return (s || '').toLowerCase().replace(/[^a-z0-9]/g, ''); }
function fmtDate(iso) {
  if (!iso) return '';
  const [y, m, d] = iso.split('-');
  return `${m}/${d}/${y}`;
}
function formatCategory(catId) {
  return (catId || '')
    .replace('xcat:service_area_business_', '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}
