exports.handler = async function(event, context) {
  const AIRTABLE_API_KEY = process.env.AIRTABLE_TOKEN;
  const BASE_ID = "appyfDILW0PkDwiHH";
  const TABLE_NAME = "VC Firms";

  let allFirms = [];
  let offset = null;

  do {
    let url = `https://api.airtable.com/v0/${BASE_ID}/${TABLE_NAME}`;
    if (offset) url += `?offset=${offset}`;

    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
    });

    const data = await res.json();
    allFirms = allFirms.concat(data.records.map(r => r.fields));
    offset = data.offset;
  } while (offset);

  return {
    statusCode: 200,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(allFirms)
  };
};