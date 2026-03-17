exports.handler = async function(event, context) {
  const AIRTABLE_API_KEY = process.env.AIRTABLE_TOKEN;
  const BASE_ID = "appyfDILW0PkDwiHH";
  const TABLE_NAME = "VC Firms";

  const params = event.queryStringParameters || {};
  const limit = parseInt(params.limit) || 25;
  const offset = params.offset || null;
  const search = params.search || null;
  const state = params.state || null;
  const sort = params.sort || "aum-desc";

  let filters = [];
  if (search) filters.push(`SEARCH("${search.toUpperCase()}",UPPER({Firm}))`);
  if (state) filters.push(`{State}="${state}"`);

  let url = `https://api.airtable.com/v0/${BASE_ID}/${TABLE_NAME}?pageSize=${limit}`;

  if (filters.length === 1) url += `&filterByFormula=${encodeURIComponent(filters[0])}`;
  if (filters.length > 1) url += `&filterByFormula=${encodeURIComponent(`AND(${filters.join(",")})`)}`;
  if (offset) url += `&offset=${offset}`;

  if (sort === "aum-desc") url += `&sort[0][field]=AUM&sort[0][direction]=desc`;
  if (sort === "aum-asc") url += `&sort[0][field]=AUM&sort[0][direction]=asc`;
  if (sort === "name-asc") url += `&sort[0][field]=Firm&sort[0][direction]=asc`;
  if (sort === "name-desc") url += `&sort[0][field]=Firm&sort[0][direction]=desc`;

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
  });

  const data = await res.json();

  return {
    statusCode: 200,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      records: data.records.map(r => r.fields),
      offset: data.offset || null
    })
  };
};