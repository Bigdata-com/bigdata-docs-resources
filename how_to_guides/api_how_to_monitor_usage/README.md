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

3. Each token type has its own price. The script reads the prices currently applied to
   your API key from `GET https://api.bigdata.com/v1/subscription/quotas`.
4. Each billing unit of that endpoint reports a `units_price`, the price of a **single**
   token in US$ cents, so the cost of one token type is:

   ```
   cost_in_cents = tokens * units_price
   ```

   The total cost of the call is the sum across all token types.
5. The IDs in the response of the endpoint `GET https://api.bigdata.com/v1/subscription/quotas` does not match 1:1 with the token name of the `usage` object. The function `map_unit_id` helps us map them.

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
| `--search-mode` | `smart` | Search mode to use. Content tokens are priced per mode |
| `--show-prices` | off | Print the price list of your subscription before searching |

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
Prices: live prices read from https://api.bigdata.com/v1/subscription/quotas
----------------------------------------------------------------------------------------------------
Token type                          Price group               Tokens   US$ / 1M tokens    Cost (US$)
----------------------------------------------------------------------------------------------------
premium_news_tokens                 search.smart               1,732             96.00      0.166272
corporate_communications_tokens     search.smart                 445             36.00      0.016020
web_tokens                          search.smart                 654              8.00      0.005232
----------------------------------------------------------------------------------------------------
TOTAL                                                          2,831                        0.187524

Total cost: 0.187524 US$ (18.7524 US$ cents)
```

## Price list

Prices are read at run time from the subscription quotas endpoint, so there is nothing to
keep up to date in the script. Use `--show-prices` to print the full list your key is
billed at:

```bash
python monitor_api_usage_tokens.py --show-prices
```

The quotas endpoint identifies every billing unit with a colon separated id, which the
script maps to the short token names used in the `usage` object:

```
search:smart:content-premium-news:tokens    ->  search.smart          premium_news_tokens
structured-data:read:content-jobs:tokens    ->  structured-data.read  jobs_tokens
search:comentions::tokens                   ->  search                comentions_tokens
```

The group matters because the same token type can be priced differently depending on how
it was consumed. Content tokens are priced per search mode, so a search run in `fast` mode
and one in `smart` mode are billed from different groups. The report shows which group
priced each line.

Units that are not billed per token (stored pages, PDF pages, ...) are ignored.

Note that the number of tokens consumed depends on which content the search actually
matches, so the same query can hit different token types (and therefore different prices)
on different days.

If the quotas endpoint cannot be reached, the script fails instead of falling back to a
built-in price list: reporting a cost from prices that may no longer apply would be worse
than reporting no cost at all.

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
