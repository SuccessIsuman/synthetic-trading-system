from __future__ import annotations

import asyncio

from deriv_client import DerivPublicClient

KEYWORDS = ("volatility", "boom", "crash", "step", "jump", "range")


def symbol_fields(item: dict) -> tuple[str, str, str, str]:
    symbol = item.get("underlying_symbol") or item.get("symbol") or ""
    name = item.get("underlying_symbol_name") or item.get("display_name") or ""
    market = item.get("market") or ""
    submarket = item.get("submarket") or ""
    return str(symbol), str(name), str(market), str(submarket)


async def main() -> None:
    symbols = await DerivPublicClient().active_symbols()
    matches = []
    for item in symbols:
        symbol, name, market, submarket = symbol_fields(item)
        searchable = f"{symbol} {name} {market} {submarket}".lower()

        if any(keyword in searchable for keyword in KEYWORDS):
            matches.append((symbol, name, market, submarket))

    print(f"Received {len(symbols)} active symbols; synthetic-like matches: {len(matches)}\n")
    print(f"{'SYMBOL':<18} {'NAME':<42} {'MARKET':<18} SUBMARKET")
    print("-" * 100)
    for symbol, name, market, submarket in matches:
        print(f"{symbol:<18} {name[:41]:<42} {market[:17]:<18} {submarket}")


if __name__ == "__main__":
    asyncio.run(main())
