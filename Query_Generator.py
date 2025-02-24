# QueryGenerator class needs a minor update to handle the new schema format
from dotenv import load_dotenv
import google.generativeai as genai
from typing import Dict, Any, Optional

class QueryGenerator:
    def __init__(self):
        self.model = genai.GenerativeModel("gemini-pro")

    def generate_query(self, question: str, schema: Dict[str, Any]) -> str:
        """Generate SQL query from natural language question"""
        # Extract schema string from the new schema response format
        schema_str = schema.get('schema', '') if isinstance(schema, dict) else schema
        prompt = self._get_prompt_template(schema_str)
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
