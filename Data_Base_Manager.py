from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import os
import pandas as pd
import sqlite3
import psycopg2
import mysql.connector
from typing import Dict, Any, Optional
from contextlib import contextmanager

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
            raise ConnectionError(f"Database connection failed: {str(e)}")
        finally:
            if db_type == "SQLite":  # For SQLite, we need to commit after each operation
                self.connections[conn_key].commit()
    
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
    def get_schema(self, db_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get database schema"""
        try:
            with self.get_connection(db_data) as conn:
                cursor = conn.cursor()
                db_type = db_data['db_type']
                
                if db_type == "SQLite":
                    schema = self._get_sqlite_schema(cursor)
                elif db_type == "PostgreSQL":
                    schema = self._get_postgres_schema(cursor)
                elif db_type == "MySQL":
                    schema = self._get_mysql_schema(cursor)
                else:
                    raise ValueError(f"Unsupported database type: {db_type}")
                
                return {'success': True, 'schema': schema}
        except Exception as e:
            return {'success': False, 'message': f"Failed to get schema: {str(e)}"}

    def _get_sqlite_schema(self, cursor) -> str:
        """Get SQLite schema"""
        schema_parts = []
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info('{table_name}');")
            columns = cursor.fetchall()
            
            # Format table schema
            column_details = [f"    - {col[1]} ({col[2]})" for col in columns]
            table_schema = f"\n  Table: {table_name}\n" + "\n".join(column_details)
            schema_parts.append(table_schema)
        
        return "\n".join(schema_parts)

    def _get_postgres_schema(self, cursor) -> str:
        """Get PostgreSQL schema"""
        schema_parts = []
        
        # Get all tables and their columns
        cursor.execute("""
            SELECT 
                t.table_name,
                c.column_name,
                c.data_type,
                c.column_default,
                c.is_nullable
            FROM 
                information_schema.tables t
                JOIN information_schema.columns c ON t.table_name = c.table_name
            WHERE 
                t.table_schema = 'public'
                AND t.table_type = 'BASE TABLE'
            ORDER BY 
                t.table_name,
                c.ordinal_position;
        """)
        
        current_table = None
        column_details = []
        
        for row in cursor.fetchall():
            table_name, column_name, data_type, default, nullable = row
            
            if current_table != table_name:
                if current_table is not None:
                    schema_parts.append(f"\n  Table: {current_table}\n" + "\n".join(column_details))
                current_table = table_name
                column_details = []
            
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            default_str = f" DEFAULT {default}" if default else ""
            column_details.append(f"    - {column_name} ({data_type} {nullable_str}{default_str})")
        
        if current_table is not None:
            schema_parts.append(f"\n  Table: {current_table}\n" + "\n".join(column_details))
        
        return "\n".join(schema_parts)

    def _get_mysql_schema(self, cursor) -> str:
        """Get MySQL schema"""
        schema_parts = []
        
        # Get all tables
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"DESCRIBE `{table_name}`;")
            columns = cursor.fetchall()
            
            # Format table schema
            column_details = []
            for col in columns:
                field, type_, null, key, default, extra = col
                nullable = "NULL" if null == "YES" else "NOT NULL"
                default_str = f" DEFAULT {default}" if default else ""
                key_str = f" {key}" if key else ""
                extra_str = f" {extra}" if extra else ""
                column_details.append(f"    - {field} ({type_} {nullable}{default_str}{key_str}{extra_str})")
            
            table_schema = f"\n  Table: {table_name}\n" + "\n".join(column_details)
            schema_parts.append(table_schema)
        
        return "\n".join(schema_parts)
    
    def execute_query(self, sql_query, db_data):
        try:
            with self.get_connection(db_data) as conn:
                cursor = conn.cursor()
                cursor.execute(sql_query)
                if cursor:
                    columns = [desc[0] for desc in cursor.description]

                    rows = cursor.fetchall()
   
                    if rows:
                        return pd.DataFrame(rows, columns=columns)
                return None
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")
        
        # return {"query_":sql_query }