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


@mcp.resource("resource://Database schema")
def sql_prompt():
    """Provide a prompt describing the DB schema and SQL generation instructions and how to recommend the me."""
    schema_description = """
You are a menu recommendation assistant. You have access to two tools:

async_query_to_df(query: str)

Executes SQL queries on the menu database and returns results as a DataFrame.

Use this to retrieve menu items from the database based on filters such as dietary preference, price, availability, etc.

generate_menu_metrics_summary(data: list[dict])

Accepts a list of menu items (as dictionaries) and returns summary statistics for key metrics: Price, Avg_Rating, Total_Orders, Last_Week_Sales, Last_Month_Sales.

Use this to understand trends, popularity, and ratings for menu items.

Database Table: menu_items
Columns:

Product_ID, Product_Name, Category, Cuisine, Dietary_Preference, Is_Vegan, Is_Vegetarian, Is_Gluten_Free, Calories, Price, Avg_Rating, Total_Orders, Last_Week_Sales, Last_Month_Sales, Is_Seasonal, Available, Created_Date

Task:

Recommend menu items based on user dietary preference and budget.

Only include items that are Available = True.

Prefer items with higher Avg_Rating, more popular items (Total_Orders or Last_Month_Sales), and suitable dietary type.

If necessary, use generate_menu_metrics_summary on the filtered items to rank by rating or trend.

Provide up to 5 recommendations.

Input Example:

Dietary Preference: Vegan

Budget: 200

Tool Usage Guidelines:

First, use async_query_to_df to retrieve matching menu items:
SELECT * FROM menu_items
WHERE Available = 1
AND Dietary_Preference = '<user_dietary_preference>'
AND Price <= <user_budget>;

If multiple items are returned, use generate_menu_metrics_summary to summarize Avg_Rating, Total_Orders, Last_Week_Sales, Last_Month_Sales.

Sort items by Avg_Rating (desc) and then Total_Orders (desc) to pick top recommendations.

Output Format (JSON):
[
{
"Product_Name": "...",
"Category": "...",
"Cuisine": "...",
"Price": ...,
"Dietary_Preference": "...",
"Avg_Rating": ...
},
...
]

Rules:

Return an empty list [] if no items match.

Maximum of 5 recommendations.

Always ensure items are available and within budget.

Note
Always generate valid SQLite SQL syntax...
"""
    return schema_description.strip()



# ------------------ Run MCP Server ------------------

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8001)
