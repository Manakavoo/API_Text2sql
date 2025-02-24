from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import os
import pandas as pd
import sqlite3
import psycopg2
import mysql.connector
from dotenv import load_dotenv
import google.generativeai as genai
from typing import Dict, Any, Optional
from  prompt import prompt_query
from contextlib import contextmanager
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow specific origins or all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace "*" with specific origins (e.g., ["http://localhost:3000"])
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

load_dotenv()

genai.configure(api_key=os.getenv("GOOGLE_PRO_API_KEY"))
class DatabaseManager:
    def __init__(self):
        self.connections = {}
    
    @contextmanager
    def get_connection(self, db_data: Dict[str, Any]):
        """Context manager for database connections"""
        db_type = db_data['db_type']
        conn_key = f"{db_type}_{db_data.get('db_name')}_{db_data.get('db_host')}"
        
        try:
            if conn_key not in self.connections:
                self.connections[conn_key] = self._create_connection(db_data)
                
            yield self.connections[conn_key]
        except Exception as e:
            raise e
        finally:
            pass  
    
    def _create_connection(self, db_data: Dict[str, Any]):
        """Create a new database connection"""
        try:
            if db_data['db_type'] == "SQLite":
                print("Connected to SQLite")
                return sqlite3.connect(db_data['db_name'])
                
            elif db_data['db_type'] == "PostgreSQL":
                print("Connected to PostgreSQL")
                return psycopg2.connect(
                    host=db_data['db_host'],
                    database=db_data['db_name'],
                    user=db_data['db_user'],
                    password=db_data['db_password'],
                    port=db_data['db_port']
                )
            
            elif db_data['db_type'] == "MySQL":
                print("Connected to MYSQL")
                return mysql.connector.connect(
                    host=db_data['db_host'],
                    database=db_data['db_name'],
                    user=db_data['db_user'],
                    password=db_data['db_password'],
                    port=db_data['db_port']
                )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def test_connection(self, db_data: Dict[str, Any]) -> Dict[str, Any]:
        """Test database connection"""
        try:
            with self.get_connection(db_data) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return {'success': True, 'message': "Connection successful!"}
        except Exception as e:
            return {'success': False, 'message': f"Connection failed: {str(e)}"}

    def get_schema(self, db_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get database schema"""
        try:
            with self.get_connection(db_data) as conn:
                cursor = conn.cursor()
                schema = self._get_schema_details(cursor, db_data['db_type'])
                return {'success': True, 'schema': schema}
        except Exception as e:
            return {'success': False, 'message': f"Failed to get schema: {str(e)}"}

    def _get_schema_details(self, cursor, db_type: str) -> str:
        """Get detailed schema information"""
        schema = []
        
        try:
            if db_type == "SQLite":
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"PRAGMA table_info({table_name});")
                    columns = cursor.fetchall()
                    schema.append(self._format_table_schema(table_name, columns, 'sqlite'))
                    
            elif db_type == "PostgreSQL":
                print("postgres schema execution")
                cursor.execute("""
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position;
                """)
                schema_data = cursor.fetchall()
                print(schema)
                schema.extend(self._format_postgres_schema(schema_data))
                
            elif db_type == "MySQL":
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SHOW COLUMNS FROM {table_name};")
                    columns = cursor.fetchall()
                    schema.append(self._format_table_schema(table_name, columns, 'mysql'))
            
            return "\n".join(schema)
        except Exception as e:
            raise Exception(f"Error fetching schema details: {str(e)}")

    def _format_table_schema(self, table_name: str, columns: list, db_type: str) -> str:
        """Format table schema based on database type"""
        if db_type == 'sqlite':
            columns_info = [f"    - {col[1]} ({col[2]})" for col in columns]
        elif db_type == 'mysql':
            columns_info = [f"    - {col[0]} ({col[1]})" for col in columns]
        else:
            columns_info = [f"    - {col[1]} ({col[2]})" for col in columns]
            
        return f"\n  Table: {table_name}\n" + "\n".join(columns_info)

    def _format_postgres_schema(self, schema_data: list) -> list:
        """Format PostgreSQL schema data"""
        table_columns = {}
        for table_name, column_name, data_type in schema_data:
            if table_name not in table_columns:
                table_columns[table_name] = []
            table_columns[table_name].append(f"    - {column_name} ({data_type})")
        
        return [f"\n  Table: {table_name}\n" + "\n".join(columns) 
                for table_name, columns in table_columns.items()]

    def execute_query(self, query: str, db_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Execute SQL query and return results as DataFrame"""
        try:
            with self.get_connection(db_data) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                if cursor:
                    columns = [desc[0] for desc in cursor.description]

                    rows = cursor.fetchall()
   
                    if rows:
                        return pd.DataFrame(rows, columns=columns)
                return None
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")


class QueryGenerator:
    def __init__(self):
        self.model = genai.GenerativeModel("gemini-pro")

    def generate_query(self, question: str, schema: str) -> str:
        """Generate SQL query from natural language question"""
        prompt = self._get_prompt_template(schema)
        # return "SELECT   name FROM   student;"
        response = self.model.generate_content(prompt + "\n\nUser Question: " + question)
        return response.text

    def _get_prompt_template(self, schema: str) -> str:
        """Get the prompt template for query generation"""
        return f"""
        You are an expert SQL assistant. You will help users write SQL queries that are efficient, secure and follow best practices.

        Database Schema:
        {schema}

        Rules to follow:
        1. Only write standardized SQL that works across major databases
        2. Convert English questions into SQL queries based on the above schema
        3. Add proper indexing suggestions when relevant
        4. Use clear aliases and formatting
        5. Consider performance implications
        6. Avoid SQL injection risks
        7. Include error handling where needed
        8. SQL code should not have any syntax errors
        9. Do not begin or end with any ``` or any other special characters
        10. Only use tables and columns that exist in the schema above

        Important note: Don't mention sql in your response, provide only the SQL query

        Please provide only the SQL query without any explanations unless specifically asked for details.
        """
  


# Pydantic Models
class DBConfig(BaseModel):
    db_type: str
    db_host: Optional[str] = None
    db_port: Optional[str] = None
    db_name: str
    db_user: Optional[str] = None
    db_password: Optional[str] = None

class UserQuery(BaseModel):
    question: str
    db_data: DBConfig

# Initialize Classes
db_manager = DatabaseManager()
query_generator = QueryGenerator()

# API Endpoints
@app.get("/")
def home():
    return {"Msg":"Home Page get method"}

@app.post("/")
def home():
    return {"Msg":"Home Page post method"}

@app.post("/connect")
def connect_db(db_data: DBConfig):
    try:
        db_manager.get_connection(db_data.dict())
        return {"success": True, "message": "Connection successful!"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/get_schema")
def get_schema(db_data: DBConfig):

    print("Get_schema running")    
    try:
        schema = db_manager.get_schema(db_data.dict())
        return {"success": True, "schema": schema}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/generate_query")
def generate_sql(user_query: UserQuery):
    try:
        schema = db_manager.get_schema(user_query.db_data.dict())
        query = query_generator.generate_query(user_query.question, schema)
        return {"success": True, "sql_query": query}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/execute_query")
def execute_sql(user_query: UserQuery):
    try:
        schema = db_manager.get_schema(user_query.db_data.dict())
        query = query_generator.generate_query(user_query.question, schema)
        results = db_manager.execute_query(query, user_query.db_data.dict())
        return {"success": True, "results": results.to_dict(orient="records") if results is not None else "No results"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
