
from dotenv import load_dotenv
from openai import OpenAI
import os
import json
import time
from pathlib import Path
from datetime import datetime
from collections import Counter
import markdown
from weasyprint import HTML, CSS

load_dotenv()

# Read Bigdata API key from environment
BIGDATA_API_KEY = os.getenv("BIGDATA_API_KEY")

client = OpenAI()

# Track response time
start_time = time.time()
TOPIC = "You are a senior equity analyst preparing for an upc..."
logger.info(f"Launching deep research with the following topic \n {TOPIC}")
resp = client.responses.create(
    model="o3-deep-research-2025-06-26",
    input="""You are a senior equity analyst preparing for an upcoming earnings call. Please provide a comprehensive earnings preview and analysis for Micron.

Cover:
- Recent developments and initiatives
- Industry trends and sector dynamics
- Bull/bear cases
- Key metrics to watch

Deliverable Format: Present findings as a concise, actionable brief suitable for investment professionals. Focus on business fundamentals, avoid speculation, and highlight areas of uncertainty or debate. Be decisive in your assessments while acknowledging alternative viewpoints. Add inline source attribution and use the Bigdata MCP.""",
    tools=[
        {
            "type": "mcp",
            "server_label": "bigdata",
            "server_url": "https://mcp.bigdata.com/deepresearch/",
            "headers": {
                "x-api-key": BIGDATA_API_KEY
            },
            "require_approval": "never",
        }
    ]
)

end_time = time.time()
elapsed_time = end_time - start_time

# Display MCP tool calls and responses
print("=" * 80)
print("MCP REQUESTS AND RESPONSES")
print("=" * 80)

mcp_call_count = 0
tool_calls_counter = Counter()

for item in resp.output:
    item_type = getattr(item, 'type', None)
    
    # MCP tool calls (requests to MCP server)
    if item_type == 'mcp_call':
        mcp_call_count += 1
        tool_name = getattr(item, 'name', 'N/A')
        tool_calls_counter[tool_name] += 1
        print(f"\n[MCP CALL #{mcp_call_count}]")
        print(f"  Server: {getattr(item, 'server_label', 'N/A')}")
        print(f"  Tool: {tool_name}")
        print(f"  Call ID: {getattr(item, 'id', 'N/A')}")
        if hasattr(item, 'arguments'):
            print(f"  Arguments: {item.arguments}")
    
    # MCP tool results (responses from MCP server)
    elif item_type == 'mcp_call_output':
        print(f"\n[MCP RESPONSE for call: {getattr(item, 'call_id', 'N/A')}]")
        output = getattr(item, 'output', None)
        if output:
            # Try to pretty-print if it's JSON
            try:
                parsed = json.loads(output) if isinstance(output, str) else output
                print(f"  Output: {json.dumps(parsed, indent=4)[:2000]}...")  # Truncate long outputs
            except (json.JSONDecodeError, TypeError):
                print(f"  Output: {str(output)[:2000]}...")  # Truncate long outputs
    
    # MCP list tools response
    elif item_type == 'mcp_list_tools':
        print(f"\n[MCP LIST TOOLS - Server: {getattr(item, 'server_label', 'N/A')}]")
        tools = getattr(item, 'tools', [])
        for tool in tools:
            tool_name = getattr(tool, 'name', 'N/A')
            tool_desc = getattr(tool, 'description', '')[:100]
            print(f"  - {tool_name}: {tool_desc}")

# Display summary with table
print(f"\n{'=' * 80}")
print("MCP CALLS SUMMARY")
print("=" * 80)

# Format elapsed time
if elapsed_time >= 60:
    minutes = int(elapsed_time // 60)
    seconds = elapsed_time % 60
    time_str = f"{minutes}m {seconds:.2f}s"
else:
    time_str = f"{elapsed_time:.2f}s"

print(f"\nTotal Response Time: {time_str}")
print(f"\nTotal MCP Calls: {mcp_call_count}")

if tool_calls_counter:
    print("\nCalls per Tool:")
    print("-" * 40)
    print(f"{'Tool Name':<30} {'Calls':>8}")
    print("-" * 40)
    for tool_name, count in tool_calls_counter.most_common():
        print(f"{tool_name:<30} {count:>8}")
    print("-" * 40)
    print(f"{'TOTAL':<30} {mcp_call_count:>8}")

print("=" * 80)

# Display final output
print("\n" + "=" * 80)
print("FINAL OUTPUT")
print("=" * 80)
print(resp.output_text)

# Convert markdown to PDF
print("\n" + "=" * 80)
print("GENERATING PDF")
print("=" * 80)

# Convert markdown to HTML
html_content = markdown.markdown(
    resp.output_text,
    extensions=['tables', 'fenced_code', 'toc']
)

# Add CSS styling for professional look
css_style = CSS(string='''
    @page {
        margin: 1in;
        size: letter;
    }
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        font-size: 11pt;
        line-height: 1.6;
        color: #333;
    }
    h1 {
        color: #1a1a1a;
        border-bottom: 2px solid #333;
        padding-bottom: 0.3em;
        font-size: 24pt;
    }
    h2 {
        color: #2c2c2c;
        border-bottom: 1px solid #ccc;
        padding-bottom: 0.2em;
        font-size: 18pt;
        margin-top: 1.5em;
    }
    h3 {
        color: #444;
        font-size: 14pt;
        margin-top: 1.2em;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        margin: 1em 0;
    }
    th, td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: left;
    }
    th {
        background-color: #f5f5f5;
        font-weight: bold;
    }
    tr:nth-child(even) {
        background-color: #fafafa;
    }
    code {
        background-color: #f4f4f4;
        padding: 2px 6px;
        border-radius: 3px;
        font-family: "Courier New", monospace;
        font-size: 10pt;
    }
    pre {
        background-color: #f4f4f4;
        padding: 1em;
        border-radius: 5px;
        overflow-x: auto;
    }
    blockquote {
        border-left: 4px solid #ccc;
        margin: 1em 0;
        padding-left: 1em;
        color: #666;
    }
    ul, ol {
        margin: 0.5em 0;
        padding-left: 2em;
    }
    li {
        margin: 0.3em 0;
    }
''')

# Wrap HTML content with proper document structure
full_html = f'''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Research Report</title>
</head>
<body>
{html_content}
</body>
</html>
'''

# Create output directory if it doesn't exist
script_dir = Path(__file__).parent
output_dir = script_dir / 'output'
output_dir.mkdir(exist_ok=True)

# Generate PDF filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
pdf_filename = output_dir / f"research_report_{timestamp}.pdf"

# Create PDF
HTML(string=full_html).write_pdf(pdf_filename, stylesheets=[css_style])

print(f"\nPDF generated: {pdf_filename}")