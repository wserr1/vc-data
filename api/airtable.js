export default async function handler(req, res) {
  const AIRTABLE_API_KEY = process.env.AIRTABLE_TOKEN;
  const BASE_ID = "appyfDILW0PkDwiHH";
  const TABLE_NAME = "VC Firms";

  const { limit, offset, search, state, sort } = req.query;

  const pageSize = parseInt(limit) || 25;

  let filters = [];
  if (search) filters.push(`SEARCH("${search.toUpperCase()}",UPPER({Firm}))`);
  if (state) filters.push(`{State}="${state}"`);

  let url = `https://api.airtable.com/v0/${BASE_ID}/${TABLE_NAME}?pageSize=${pageSize}`;

  if (filters.length === 1) url += `&filterByFormula=${encodeURIComponent(filters[0])}`;
  if (filters.length > 1) url += `&filterByFormula=${encodeURIComponent(`AND(${filters.join(",")})`)}`;
  if (offset) url += `&offset=${offset}`;

  if (!sort || sort === "aum-desc") url += `&sort[0][field]=AUM&sort[0][direction]=desc`;
  if (sort === "aum-asc") url += `&sort[0][field]=AUM&sort[0][direction]=asc`;
  if (sort === "name-asc") url += `&sort[0][field]=Firm&sort[0][direction]=asc`;
  if (sort === "name-desc") url += `&sort[0][field]=Firm&sort[0][direction]=desc`;

  const airtableRes = await fetch(url, {
    headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
  });

  const data = await airtableRes.json();

  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "s-maxage=300, stale-while-revalidate");
  res.json({
    records: data.records.map(r => r.fields),
    offset: data.offset || null
  });
}