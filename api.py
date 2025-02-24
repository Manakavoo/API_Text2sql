
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import os
import pandas as pd
# import sqlite3
# import psycopg2
# import mysql.connector
from dotenv import load_dotenv
import google.generativeai as genai
from typing import Dict, Any, Optional
# from  prompt import prompt_query
from contextlib import contextmanager
from fastapi.middleware.cors import CORSMiddleware
from Data_Base_Manager import DatabaseManager
from Query_Generator import QueryGenerator
import datetime , time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace "*" with specific origins (e.g., ["http://localhost:3000"])
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

load_dotenv()

genai.configure(api_key=os.getenv("GOOGLE_PRO_API_KEY"))

class DBConfig(BaseModel):
    db_type: str
    db_host: Optional[str] = None
    db_port: Optional[str] = None
    db_name: str
    db_user: Optional[str] = None
    db_password: Optional[str] = None

class UserQuery(BaseModel):
    question: str
    sql_query:Optional[str] = None
    db_data: DBConfig

db_manager = DatabaseManager()
query_generator = QueryGenerator()


@app.get("/")
async def home():
    """Health check endpoint"""
    return {
        "status": "active",
        "message": "API is running",
        "version": "1.0"
    }

@app.post("/")
async def home_post():
    """Post method health check endpoint"""
    return {
        "status": "active",
        "message": "POST method is working",
        "version": "1.0"
    }

@app.post("/connect")
async def connect_db(db_data: DBConfig):
    """Test database connection"""
    try:
        
        with db_manager.get_connection(db_data.dict()) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1") 
            return {
                "success": True,
                "message": f"Successfully connected to {db_data.db_type} database"
            }
    except ConnectionError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database connection failed: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred: {str(e)}"
        )

@app.post("/get_schema")
async def get_schema(db_data: DBConfig):
    """Get database schema"""
    try:
        schema_result = db_manager.get_schema(db_data.dict())
        if not schema_result['success']:
            raise HTTPException(
                status_code=400,
                detail=schema_result['message']
            )
        return schema_result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve schema: {str(e)}"
        )

@app.post("/generate_query")
async def generate_sql(user_query: UserQuery):
    """Generate SQL query from natural language"""
    try:
        schema_result = db_manager.get_schema(user_query.db_data.dict())
        if not schema_result['success']:
            raise HTTPException(
                status_code=400,
                detail=schema_result['message']
            )
        
        query = query_generator.generate_query(user_query.question, schema_result)
        
        if not query or query.isspace():
            raise HTTPException(
                status_code=400,
                detail="Failed to generate valid SQL query"
            )
            
        return {
            "success": True,
            "sql_query": query,
            "db_type": user_query.db_data.db_type
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query generation failed: {str(e)}"
        )

@app.post("/execute_query")
async def execute_sql(user_query: UserQuery):
    """Execute generated SQL query"""
    try:
        schema_result = db_manager.get_schema(user_query.db_data.dict())
        if not schema_result['success']:
            raise HTTPException(
                status_code=400,
                detail=schema_result['message']
            )
        
        # query = query_generator.generate_query(user_query.question, schema_result)
        # if not query or query.isspace():
        #     raise HTTPException(
        #         status_code=400,
        #         detail="Failed to generate valid SQL query"
        #     )
        
        results = db_manager.execute_query(user_query.sql_query, user_query.db_data.dict())
        
        return {
            "success": True,
            "query": user_query.sql_query,
            "results": results.to_dict(orient="records") if results is not None else [],
            "row_count": len(results) if results is not None else 0
        }
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query execution failed: {str(e)}"
        )

@app.post("/execute_raw_query")
async def execute_raw_sql(db_data: DBConfig, query: str):
    """Execute raw SQL query"""
    try:
        results = db_manager.execute_query(query, db_data.dict())
        
        return {
            "success": True,
            "query": query,
            "results": results.to_dict(orient="records") if results is not None else [],
            "row_count": len(results) if results is not None else 0
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Raw query execution failed: {str(e)}"
        )

@app.get("/health/{db_type}")
async def check_db_health(db_type: str, db_data: DBConfig):
    """Check database health status"""
    try:
        with db_manager.get_connection(db_data.dict()) as conn:
            cursor = conn.cursor()
            start_time = time.time()
            cursor.execute("SELECT 1")
            response_time = time.time() - start_time
            
            return {
                "success": True,
                "database_type": db_type,
                "status": "healthy",
                "response_time_ms": round(response_time * 1000, 2),
                "timestamp": datetime.datetime.now().isoformat()
            }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database health check failed: {str(e)}"
        )

