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


@mcp.resource("sql://Database schema")
def sql_prompt():
    """Provide a prompt describing the DB schema and SQL generation instructions."""
    schema_description = """
Database Name: menu_items.db
Table Name: menu_items

Columns and Descriptions:

Product_ID (INTEGER): Unique identifier for each menu item.

Product_Name (TEXT): Name of the menu item, usually including dietary type and category.

Category (TEXT): General category of the item, e.g., Salad, Bowl, Beverage, Dessert, Wrap, Soup, Snack, Juice, Smoothie.

Cuisine (TEXT): Cuisine style of the item, e.g., Continental, Indian, Asian, Fusion, Western, Mexican.

Dietary_Preference (TEXT): Dietary classification, e.g., High Protein, Vegetarian, Vegan, Low Fat, High Calorie, Balanced, Low Calorie.

Is_Vegan (BOOLEAN): Whether the item is vegan (True/False).

Is_Vegetarian (BOOLEAN): Whether the item is vegetarian (True/False).

Is_Gluten_Free (BOOLEAN): Whether the item is gluten-free (True/False).

Calories (INTEGER): Energy content in kcal for the menu item.

Price (INTEGER): Selling price of the menu item in the local currency.

Avg_Rating (FLOAT): Average customer rating on a scale of 1–5.

Total_Orders (INTEGER): Total number of orders received for the item.

Last_Week_Sales (INTEGER): Number of orders for the item in the last week.

Last_Month_Sales (INTEGER): Number of orders for the item in the last month.

Is_Seasonal (BOOLEAN): Whether the item is seasonal (True/False).

Available (BOOLEAN): Whether the item is currently available (True/False).

Created_Date (DATE/TIMESTAMP): The date when the menu item was added to the database.

Additional Notes:

Continuous columns suitable for aggregation or statistics: Price, Avg_Rating, Total_Orders, Last_Week_Sales, Last_Month_Sales, Calories.

Boolean columns (Is_Vegan, Is_Vegetarian, Is_Gluten_Free, Is_Seasonal, Available) can be used for filtering items.

Category, Cuisine, and Dietary_Preference are useful for segmentation or recommendation logic.

Time-based analysis can be done using Created_Date, Last_Week_Sales, and Last_Month_Sales.

Your task is to generate SQL queries based on user questions. 
Return only valid SQLite SQL syntax.
"""
    return schema_description.strip()



# ------------------ Run MCP Server ------------------

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8001)
