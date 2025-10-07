from fastmcp import FastMCP
import os
import pandas as pd
import aiosqlite
import tempfile
import json
from dotenv import load_dotenv
load_dotenv()
from dataclasses import dataclass

mcp = FastMCP(
    name="Menu Maker Helper",
    instructions="""
        This server provides data analysis tools.
        Call get_average() to analyze numerical data.
        Also provides a tool run_sql() to run SQL queries and return processed DataFrame.
    """,
)
DB_PATH = "menu_recommendation.db"

@mcp.tool()
async def async_query_to_df(query: str) -> list[dict]:
    """
    Execute any SQL query asynchronously on the database.

    - SELECT queries return a list of dictionaries (one per row).
    - Non-SELECT queries return a single-item list with a message.
    """
   

    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute(query)
        
        if cur.description:  # SELECT or CTE with results
            rows = await cur.fetchall()
            cols = [d[0] for d in cur.description]
            result = [dict(zip(cols, r)) for r in rows]
        else:
            await conn.commit()
            result = [{"message": "Query executed successfully."}]
        
        await cur.close()
        return result
        

@mcp.tool()
async def generate_menu_metrics_summary(data: list[dict]) -> list[dict]:
    """
    Analyze key menu item performance metrics from JSON/dict input and return a structured summary.

    Input:
    - data: List of dictionaries representing menu items and their metrics, e.g.,
      [
        {"Price": 120, "Avg_Rating": 4.5, "Total_Orders": 300, "Last_Week_Sales": 50, "Last_Month_Sales": 200},
        ...
      ]

    Continuous columns analyzed:
    - Price: Selling price of the menu item
    - Avg_Rating: Average customer rating
    - Total_Orders: Total number of orders for the item
    - Last_Week_Sales: Number of orders in the last week
    - Last_Month_Sales: Number of orders in the last month

    Output:
    Returns a list of dictionaries where each dictionary contains the statistics
    for a metric, including:
    - metric: Column name
    - count: Number of non-null records
    - mean: Average value
    - std: Standard deviation
    - min: Minimum value
    - 25%, 50%, 75%: Quartiles
    - max: Maximum value
    - median: Median value
    - sum: Total sum
    """
    # Convert input JSON/dict to DataFrame
    df = pd.DataFrame(data)

    continuous_cols = ["Price", "Avg_Rating", "Total_Orders", "Last_Week_Sales", "Last_Month_Sales"]
    df_numeric = df[continuous_cols]
    
    # Compute summary statistics
    summary = df_numeric.describe().T
    summary["median"] = df_numeric.median()
    summary["sum"] = df_numeric.sum()
    
    # Convert summary to a list of dictionaries for FastMCP/LLM compatibility
    summary_list = summary.reset_index().rename(columns={"index": "metric"}).to_dict(orient="records")
    
    return summary_list



# ------------------ Run MCP Server ------------------

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8001)
