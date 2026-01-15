# Theme Volume Evolution Script

A Python script that retrieves and visualizes theme volume data from the Bigdata API. It displays the evolution of documents, chunks, and sentiment over time for any given theme, with both daily values and weekly averages.

## Features

- Retrieves theme volume data from Bigdata API
- Visualizes three key metrics: number of documents, number of chunks, and sentiment
- Displays daily values as bars (documents and chunks) or lines (sentiment)
- Overlays weekly average trends for better pattern recognition
- Generates high-resolution PNG charts with theme-specific filenames
- Supports custom date ranges and themes via command-line arguments

## Prerequisites

- Python 3.9 or higher
- A Bigdata.com API key

### System Dependencies

Matplotlib requires some system libraries for chart generation.

**Ubuntu/Debian:**

```bash
sudo apt-get install python3-tk
```

**macOS:**

```bash
# Usually pre-installed, but if needed:
brew install python-tk
```

**Windows:**

Matplotlib should work out of the box with the pip installation.

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

Open the `.env` file and replace the placeholder value with your actual API key:

```
BIGDATA_API_KEY=your_bigdata_api_key_here
```

You can obtain your API key from:
- **Bigdata API Key:** [Bigdata Platform](https://platform.bigdata.com/api-keys)

**Important:** Never commit your `.env` file to version control. Add it to your `.gitignore` file.

## Usage

Run the script with required date range parameters:

```bash
python theme_volume_evolution.py --start-date 2025-01-01 --end-date 2025-12-15
```

### Command-Line Arguments

- `--start-date` or `-s`: Start date in format `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ` (required)
- `--end-date` or `-e`: End date in format `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ` (required)
- `--theme` or `-t`: Theme to search for (optional, default: "Tariffs impact")

### Usage Examples

Basic usage with default theme:

```bash
python theme_volume_evolution.py --start-date 2025-01-01 --end-date 2025-12-15
```

Using short flags:

```bash
python theme_volume_evolution.py -s 2025-01-01 -e 2025-12-15
```

With full ISO format dates:

```bash
python theme_volume_evolution.py --start-date 2025-01-01T14:15:22Z --end-date 2025-12-15T14:15:22Z
```

With a custom theme:

```bash
python theme_volume_evolution.py -s 2025-01-01 -e 2025-12-15 --theme "Trade war"
```

View help:

```bash
python theme_volume_evolution.py --help
```

## Output

The script produces two types of output:

### 1. Console Logs

Displays detailed information about the data retrieval and processing:
- API request status
- Total documents, chunks, and sentiment values
- Number of data points retrieved
- Weekly average calculations
- Chart generation status
- Output filename

### 2. PNG Chart

A high-resolution (300 DPI) chart is generated with a timestamped filename:

```
{theme}_volume_evolution_YYYYMMDD_HHMMSS.png
```

For example:
- `tariffs_impact_volume_evolution_20260115_190153.png` (default theme)
- `trade_war_volume_evolution_20260115_190153.png` (custom theme)

The chart includes three subplots:

1. **Unique Documents per Day**: Daily document counts as bars with weekly average line
2. **Chunks per Day**: Daily chunk counts as bars with weekly average line
3. **Sentiment per Day**: Daily sentiment values as a line with weekly average line

Each chart shows:
- Daily values in softer colors (40% opacity)
- Weekly averages in bright colors with thicker lines
- Grid lines for easier reading
- Rotated date labels for better visibility
- Legend distinguishing daily vs weekly average

## Troubleshooting

### "BIGDATA_API_KEY not found" error

Ensure your `.env` file exists in the same directory as the script and contains a valid `BIGDATA_API_KEY`.

### API request errors

If you encounter API errors:
- Verify your API key is correct and has the necessary permissions
- Check that the date range is valid (start date must be before end date)
- Ensure your internet connection is stable
- Review the error message in the console logs for specific details

### Chart display issues

If the chart doesn't display:
- Ensure you have a display available (for GUI environments)
- The PNG file is still saved even if the display fails
- Check that matplotlib backend is properly configured

### Date format errors

The script accepts dates in multiple formats:
- `YYYY-MM-DD` (e.g., `2025-01-01`)
- `YYYY-MM-DDTHH:MM:SSZ` (e.g., `2025-01-01T14:15:22Z`)
- `YYYY-MM-DD HH:MM:SS` (e.g., `2025-01-01 14:15:22`)

If you get a date parsing error, ensure your date format matches one of these patterns.

### No data available

If the script reports "No data available to visualize":
- The theme may not have any data for the specified date range
- Try expanding the date range
- Verify the theme name is spelled correctly
- Check the API response in the logs for more details

## License

This project is provided as-is for demonstration purposes.
