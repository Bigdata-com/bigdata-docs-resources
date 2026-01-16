# Deep Research MCP Script

A Python script that uses OpenAI's o3-deep-research model with Bigdata.com's MCP (Model Context Protocol) server to generate comprehensive equity research reports.

## Features

- Generates detailed equity research reports using AI
- Provides visibility into all MCP requests and responses
- Displays a summary table of tool calls by type
- Tracks total response time
- Exports the final report as a professionally styled PDF

## Prerequisites

- Python 3.9 or higher
- An OpenAI API key with access to the `o3-deep-research-2025-06-26` model
- A Bigdata.com API key

### System Dependencies

WeasyPrint requires some system libraries for PDF generation.

**Ubuntu/Debian:**

```bash
sudo apt-get install libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0
```

**macOS:**

```bash
brew install pango
```

**Windows:**

Follow the [WeasyPrint installation guide](https://doc.courtbouillon.org/weasyprint/stable/first_steps.html#windows) for Windows-specific instructions.

## Setup

### 1. Clone or download the repository

```bash
cd /path/to/your/directory
```

### 2. Create a virtual environment (recommended)

```bash
python -m venv venv
```

Activate the virtual environment:

**Linux/macOS:**

```bash
source venv/bin/activate
```

**Windows:**

```bash
venv\Scripts\activate
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. API Key configuration

Copy the `.env_template` file to `.env`:

```bash
cp .env_template .env
```

Open the `.env` file and replace the placeholder values with your actual API keys:

```
OPENAI_API_KEY=your_openai_api_key_here
BIGDATA_API_KEY=your_bigdata_api_key_here
```

You can obtain your API keys from:
- **OpenAI API Key:** [OpenAI Platform](https://platform.openai.com/api-keys)
- **Bigdata API Key:** [Bigdata Platform](https://platform.bigdata.com/api-keys)

**Important:** Never commit your `.env` file to version control. Add it to your `.gitignore` file.

## Usage

Run the script:

```bash
python deep-research-mcp.py
```

### Customizing the Research Query

To change the research topic, edit the `input` parameter in the `resp = client.responses.create()` call in `deep-research-mcp.py`. The current prompt requests a Micron earnings preview, but you can modify it to research any topic supported by the Bigdata.com MCP.

## Output

The script produces three types of output:

### 1. MCP Requests and Responses (Console)

Displays each MCP tool call made by the model, including:
- Server label
- Tool name
- Call ID
- Arguments passed
- Response data (truncated for readability)

### 2. MCP Calls Summary (Console)

A summary table showing:
- Total response time
- Number of calls per tool
- Total MCP calls made

### 3. PDF Report

A professionally formatted PDF file is generated with a timestamped filename:

```
research_report_YYYYMMDD_HHMMSS.pdf
```

The PDF includes styled headers, tables, code blocks, and proper typography suitable for professional use.

## Troubleshooting

### "OPENAI_API_KEY not set" error

Ensure your `.env` file exists and contains a valid `OPENAI_API_KEY`.

### "BIGDATA_API_KEY not set" error

Ensure your `.env` file contains a valid `BIGDATA_API_KEY`.

### WeasyPrint errors

If you encounter errors during PDF generation, ensure you have installed the required system dependencies (see Prerequisites section).

### Model access errors

The `o3-deep-research-2025-06-26` model requires specific API access. Contact OpenAI if you don't have access to this model.

## License

This project is provided as-is for demonstration purposes.
