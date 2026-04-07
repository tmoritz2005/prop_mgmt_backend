from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery

app = FastAPI()
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "mgmt-54500-sp2026"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties









from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional
from datetime import date

app = FastAPI()

PROJECT_ID = "mgmt-54500-sp2026"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------

class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float

class PropertyUpdate(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    property_type: Optional[str] = None
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None

class IncomeCreate(BaseModel):
    amount: float
    date: date
    description: Optional[str] = None

class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str
    vendor: Optional[str] = None
    description: Optional[str] = None


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id, name, address, city, state,
            postal_code, property_type, tenant_name, monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database query failed: {str(e)}")
    return [dict(row) for row in results]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id, name, address, city, state,
            postal_code, property_type, tenant_name, monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """
    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database query failed: {str(e)}")
    if not results:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Property {property_id} not found")
    return dict(results[0])


@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    id_query = f"SELECT COALESCE(MAX(property_id), 0) + 1 AS new_id FROM `{PROJECT_ID}.{DATASET}.properties`"
    try:
        new_id = list(bq.query(id_query).result())[0]["new_id"]
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"ID generation failed: {str(e)}")

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
            (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
        VALUES (
            {new_id},
            '{prop.name}',
            '{prop.address}',
            '{prop.city}',
            '{prop.state}',
            '{prop.postal_code}',
            '{prop.property_type}',
            {'NULL' if prop.tenant_name is None else f"'{prop.tenant_name}'"},
            {prop.monthly_rent}
        )
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Insert failed: {str(e)}")
    return {**prop.dict(), "property_id": new_id}


@app.put("/properties/{property_id}")
def update_property(property_id: int, prop: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    fields = prop.dict(exclude_none=True)
    if not fields:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="No fields provided to update")

    set_clause = ", ".join([
        f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
        for k, v in fields.items()
    ])

    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.properties`
        SET {set_clause}
        WHERE property_id = {property_id}
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Update failed: {str(e)}")
    return get_property(property_id, bq)


@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Delete failed: {str(e)}")
    return {"message": f"Property {property_id} deleted successfully"}


# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/income")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    query = f"""
        SELECT income_id, property_id, amount, date, description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
        ORDER BY date
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database query failed: {str(e)}")
    return [dict(row) for row in results]


@app.post("/properties/{property_id}/income", status_code=status.HTTP_201_CREATED)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    id_query = f"SELECT COALESCE(MAX(income_id), 0) + 1 AS new_id FROM `{PROJECT_ID}.{DATASET}.income`"
    try:
        new_id = list(bq.query(id_query).result())[0]["new_id"]
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"ID generation failed: {str(e)}")

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income`
            (income_id, property_id, amount, date, description)
        VALUES (
            {new_id},
            {property_id},
            {income.amount},
            '{income.date}',
            {'NULL' if income.description is None else f"'{income.description}'"}
        )
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Insert failed: {str(e)}")
    return {**income.dict(), "income_id": new_id, "property_id": property_id}


@app.delete("/properties/{property_id}/income/{income_id}")
def delete_income(property_id: int, income_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE income_id = {income_id} AND property_id = {property_id}
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Delete failed: {str(e)}")
    return {"message": f"Income record {income_id} deleted successfully"}


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/expenses")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    query = f"""
        SELECT expense_id, property_id, amount, date, category, vendor, description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
        ORDER BY date
    """
    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database query failed: {str(e)}")
    return [dict(row) for row in results]


@app.post("/properties/{property_id}/expenses", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    id_query = f"SELECT COALESCE(MAX(expense_id), 0) + 1 AS new_id FROM `{PROJECT_ID}.{DATASET}.expenses`"
    try:
        new_id = list(bq.query(id_query).result())[0]["new_id"]
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"ID generation failed: {str(e)}")

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
            (expense_id, property_id, amount, date, category, vendor, description)
        VALUES (
            {new_id},
            {property_id},
            {expense.amount},
            '{expense.date}',
            '{expense.category}',
            {'NULL' if expense.vendor is None else f"'{expense.vendor}'"},
            {'NULL' if expense.description is None else f"'{expense.description}'"}
        )
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Insert failed: {str(e)}")
    return {**expense.dict(), "expense_id": new_id, "property_id": property_id}


@app.delete("/properties/{property_id}/expenses/{expense_id}")
def delete_expense(property_id: int, expense_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE expense_id = {expense_id} AND property_id = {property_id}
    """
    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Delete failed: {str(e)}")
    return {"message": f"Expense record {expense_id} deleted successfully"}


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/summary")
def get_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    query = f"""
        SELECT
            (SELECT COALESCE(SUM(amount), 0) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}) AS total_income,
            (SELECT COALESCE(SUM(amount), 0) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}) AS total_expenses,
            (SELECT COALESCE(SUM(amount), 0) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}) -
            (SELECT COALESCE(SUM(amount), 0) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}) AS net
    """
    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database query failed: {str(e)}")
    return dict(results[0])


@app.get("/properties/{property_id}/profit_margin")
def get_profit_margin(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    get_property(property_id, bq)

    query = f"""
        WITH financials AS (
            SELECT
                (SELECT COALESCE(SUM(amount), 0) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}) AS total_income,
                (SELECT COALESCE(SUM(amount), 0) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}) AS total_expenses
        )
        SELECT
            CASE
                WHEN total_income = 0 THEN 0.0
                ELSE ROUND(((total_income - total_expenses) / total_income) * 100, 2)
            END AS profit_margin_percent
        FROM financials
    """
    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Database query failed: {str(e)}")
    return dict(results[0])