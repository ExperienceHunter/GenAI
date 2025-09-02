from fastapi import FastAPI, Request
import psycopg2
import os

# ------------------------------
# Postgres Database Configuration
# ------------------------------
# This API connects to a Postgres database (tcg_db)
# All card data is stored in Postgres, not in memory or files
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "tcg_db")
DB_USER = os.getenv("DB_USER", "zakwanzahid")
DB_PASSWORD = os.getenv("DB_PASSWORD", "yourpassword")
DB_PORT = os.getenv("DB_PORT", "5432")

# Connect to Postgres
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
cur = conn.cursor()

app = FastAPI(title="TCG Cards Postgres API")

# ------------------------------
# POST endpoint: forward data from Dify
# ------------------------------
@app.post("/forward")
async def forward(request: Request):
    """
    Dynamic forwarder:
    Accepts any JSON payload and inserts into the 'cards' table in Postgres.
    Keys in JSON must match column names in DB.
    """
    data = await request.json()

    if not data:
        return {"status": "error", "message": "No data received"}

    # Build dynamic query
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    values = list(data.values())

    query = f"INSERT INTO cards ({columns}) VALUES ({placeholders})"
    cur.execute(query, values)
    conn.commit()

    return {"status": "success", "inserted_columns": list(data.keys())}

# ------------------------------
# GET endpoint: retrieve all cards
# ------------------------------
@app.get("/cards")
def get_cards():
    """
    Return all rows from the 'cards' table in Postgres
    """
    cur.execute("SELECT * FROM cards")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    result = [dict(zip(colnames, row)) for row in rows]
    return {"cards": result}
