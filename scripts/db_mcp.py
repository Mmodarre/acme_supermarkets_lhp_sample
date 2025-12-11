# mcp_databricks_wrapper.py
from mcp.server import FastMCP
import httpx
import os
import json

DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
DATABRICKS_ENDPOINT = "https://adb-984752964297111.11.azuredatabricks.net/serving-endpoints/ka-38ce1980-endpoint/invocations"

server = FastMCP(name="lhp-chatbot")

@server.tool()
async def ask_lakehouse_plumber(query: str) -> str:
    """Ask the Lakehouse Plumber chatbot for help with flowgroups, templates, and YAML configurations."""
    
    try:
        # Set a longer timeout (60 seconds) to handle cold starts
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                DATABRICKS_ENDPOINT,
                headers={
                    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                    "Content-Type": "application/json"
                },
                json={
                    "input": [{"role": "user", "content": query}]
                }
            )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse the response
            result = response.json()
            
            # Extract the actual answer from Databricks response structure
            # Response format: {"output": [{"content": [{"text": "part1"}, {"text": "part2"}, ...]}]}
            if "output" in result and len(result["output"]) > 0:
                content = result["output"][0].get("content", [])
                if len(content) > 0:
                    # Concatenate all text parts from the content array
                    text_parts = [item.get("text", "") for item in content if item.get("type") == "output_text"]
                    answer = "".join(text_parts)
                    if not answer:
                        answer = "No response text found"
                else:
                    answer = "No content in response"
            else:
                # Fallback: return formatted JSON if structure is different
                answer = json.dumps(result, indent=2)
            
            return answer
            
    except httpx.TimeoutException:
        return "Error: Request timed out. The Databricks endpoint took too long to respond (>60s)."
    except httpx.HTTPStatusError as e:
        return f"Error: HTTP {e.response.status_code} - {e.response.text}"
    except Exception as e:
        return f"Error: {type(e).__name__}: {str(e)}"

if __name__ == "__main__":
    server.run()
