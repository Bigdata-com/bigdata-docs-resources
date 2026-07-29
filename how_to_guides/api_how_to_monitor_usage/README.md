# How to monitor usage and cost of a Bigdata Search API call

This guide shows how to measure what a **single** call to the Bigdata Search Service
consumes, and how to turn that consumption into a cost in US dollars.

## How it works

1. The request is sent to `POST https://api.bigdata.com/v1/search`.
2. Every response contains a `usage` object listing the tokens consumed, broken down by
   content type:

   ```json
   "usage": {
     "premium_news_tokens": 1732,
     "corporate_communications_tokens": 445,
     "web_tokens": 654
   }
   ```

3. Each token type has its own price. So far, we have hardcoded the pricing in this script and in the near future, we will create an endpoint to retrieve it.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file in this directory with your API key:

```
BIGDATA_API_KEY=your-api-key-here
```

## Usage

```bash
# Default query: "Analyse the impact of Chinese DUV technology into ASML, and other
# chipmakers", 10 chunks, smart search mode
python monitor_api_usage_tokens.py

# Custom query
python monitor_api_usage_tokens.py --query "tesla supply chain risks" --max-chunks 25
```

Options:

| Option | Default | Description |
| --- | --- | --- |
| `--query` | `Analyse the impact of Chinese DUV technology into ASML, and other chipmakers` | Free text query to search for |
| `--max-chunks` | `10` | Maximum number of chunks to retrieve |

## Example output

```
Usage object returned by the API
----------------------------------------------------------------------------------------------------
{
  "premium_news_tokens": 1732,
  "corporate_communications_tokens": 445,
  "web_tokens": 654
}

Cost of this API call
----------------------------------------------------------------------------------------------------
Token type                          Family                    Tokens   US$ / 1M tokens    Cost (US$)
----------------------------------------------------------------------------------------------------
premium_news_tokens                 Unstructured content       1,732             96.00      0.166272
corporate_communications_tokens     Unstructured content         445             36.00      0.016020
web_tokens                          Unstructured content         654              8.00      0.005232
----------------------------------------------------------------------------------------------------
TOTAL                                                          2,831                        0.187524

Total cost: 0.187524 US$ (18.7524 US$ cents)
```

## Price list

Prices live in the `PRICING_CENTS_PER_MILLION_TOKENS` dictionary at the top of
[monitor_api_usage_tokens.py](monitor_api_usage_tokens.py), grouped into three families:
unstructured content, structured content and search analytics. Update that dictionary
when prices change.

Note that the number of tokens consumed depends on which content the search actually
matches, so the same query can hit different token types (and therefore different prices)
on different days.

## If your subscription is not billed per token

Some subscriptions are metered per query rather than per content token. On those API keys
the `usage` object returns `api_query_units` instead of token counts:

```json
"usage": {
  "api_query_units": 1.0
}
```

**This script does not apply to those accounts.** There is no token consumption to price,
so the script stops and tells you so instead of producing a misleading cost:

```
This API key does not use token billing
----------------------------------------------------------------------------------------------------
The 'usage' object reports 'api_query_units', which means your subscription is
metered per query instead of per content token. The token price list used by this
script therefore does not apply to your account, and no token cost can be
calculated for this call.

To monitor the usage of a query-based subscription, see:
  https://docs.bigdata.com/how-to-guides/monitor_usage
```

If that is your case, follow
[docs.bigdata.com/how-to-guides/monitor_usage](https://docs.bigdata.com/how-to-guides/monitor_usage)
instead of this guide.

Separately, any token type that is not in the local price list is listed as unpriced and
left out of the total, so a newly added content type is reported rather than silently
priced at zero.
