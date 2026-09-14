"""
Learning edition - local adk agent that connects to your local mcp server.
"""

import asyncio
import os

from dotenv import load_dotenv
from google.adk.agents import Agent
from google.adk.runners import InMemoryRunner
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPServerParams
from google.genai import types

load_dotenv()

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL","http://localhost:8000/mcp")
API_KEY_HEADER = os.getenv("MCP_API_KEY")

def build_headers()->dict:
    headers = {"content-Type": "application/json"}
    if API_KEY_HEADER:
        headers["x-api-key"] = API_KEY_HEADER
    return headers


#---------------
# wiring up my MCP toolset - this is where my agent "connect to" my mcp server
#---------------

db_mcp_toolset = McpToolset(
    connection_params = StreamableHTTPServerParams(url=MCP_SERVER_URL),
    header_provider = lambda ctx: build_headers(),
)

learning_agent = Agent(
    name = "local_db_learning_agent",
    model = "gemini-2.5-flash",
    description = "A learning agent that answers questions that customer orders using a local MCP server.",
    instruction = (
        "You are a helpful assistant with access to a database via tools."
        "when a user asks about orders, call the appropriate tool with the "
        "account ID they mention. If they don't give an account ID, ask for it. "
        "Summarize the tool's JSON result in plain, friendly language."
    ),
    tools = [db_mcp_toolset],
)

async def main():
    runner = InMemoryRunner(agent=learning_agent, app_name="local_learning_app")
    session = await runner.session_service.create_session(
        app_name = "local_learning_app", user_id = "learner"
    )
    print("Local ADK Agent ready. Type a quesion (or 'quit'):")
    print("Example: 'show me the last 5 orders for account ACC-12345'\n")

    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ("quit","exit"):
            break

        content = types.Content(role="user",parts=[types.Part(text=user_input)])
        async for event in runner.run_async(
            user_id = "learner", session_id = session.id, new_message = content
        ):
            if event.is_final_response() and event.content and event.content.parts:
                print(f"Agent: {event.content.parts[0].text}\n")

if __name__ == "__main__":
    asyncio.run(main())