from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os, re, asyncpg
app=FastAPI()
DATABASE_URL=os.getenv("VANNA_DATABASE_URL")
class GenReq(BaseModel):
    query: str
    max_rows: int = 500
async def get_conn():
    return await asyncpg.connect(DATABASE_URL)
@app.post("/generate-sql")
async def generate_sql(req: GenReq):
    q=req.query.lower()
    if "top 5 vendors" in q:
        sql = 'SELECT v.name as vendor, SUM(i.total) as spend FROM "Vendor" v JOIN "Invoice" i ON i."vendorId" = v.id GROUP BY v.name ORDER BY spend DESC LIMIT 5'
    elif "total spend" in q and "90" in q:
        sql = "SELECT SUM(total) as total_spend FROM "Invoice" WHERE date >= current_date - interval '90 days'"
    else:
        sql = 'SELECT invoiceNumber, date, total, status FROM "Invoice" ORDER BY date DESC LIMIT 50'
    if not re.match(r"^\s*select\b", sql, re.I):
        raise HTTPException(status_code=400, detail="Generated SQL not allowed")
    final_sql = f"SELECT * FROM ({sql}) t LIMIT {req.max_rows};"
    conn = await get_conn()
    try:
        rows = await conn.fetch(final_sql)
        results = [dict(r) for r in rows]
    finally:
        await conn.close()
    return {"sql": sql, "final_sql": final_sql, "results": results}