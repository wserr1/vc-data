exports.handler = async function(event, context) {
  const AIRTABLE_API_KEY = process.env.AIRTABLE_TOKEN;
  const BASE_ID = "appyfDILW0PkDwiHH";
  const TABLE_NAME = "VC Firms";

  const params = event.queryStringParameters || {};
  const limit = parseInt(params.limit) || 100;
  const offset = params.offset || null;
  const search = params.search || null;

  let url = `https://api.airtable.com/v0/${BASE_ID}/${TABLE_NAME}?pageSize=${limit}`;

  if (offset) url += `&offset=${offset}`;
  if (search) url += `&filterByFormula=SEARCH("${search.toUpperCase()}",UPPER({Firm}))`;

  url += `&sort[0][field]=AUM&sort[0][direction]=desc`;

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