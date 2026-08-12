from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP


SIZE_SCALE = Decimal("100000000")


def validate_profitability(spread_bps: float, fee_bps: float, slippage_bps: float) -> float:
    floor = Decimal(str(spread_bps)) - Decimal(str(fee_bps)) - Decimal(str(slippage_bps))
    if not floor.is_finite() or floor <= 0:
        raise ValueError("价差必须高于 BitTrade Maker 费率、GMO 手续费与预期滑点之和")
    return float(floor)


def _hedgeable_size(levels, reference: Decimal, max_slip_bps: Decimal,
                    buy_hedge: bool) -> Decimal:
    slip = max(Decimal("0"), max_slip_bps) / Decimal("10000")
    limit = reference * (Decimal("1") + slip if buy_hedge else Decimal("1") - slip)
    total = Decimal("0")
    for raw_price, raw_size in levels:
        price = Decimal(str(raw_price))
        size = max(Decimal("0"), Decimal(str(raw_size)))
        if buy_hedge and price > limit or not buy_hedge and price < limit:
            break
        total += size
    return total


def make_quotes(bid, ask, bid_levels, ask_levels, spread_bps, max_quote_size,
                price_tick, max_slip_bps):
    bid = Decimal(str(bid))
    ask = Decimal(str(ask))
    spread = Decimal(str(spread_bps)) / Decimal("10000")
    maximum = Decimal(str(max_quote_size))
    tick = Decimal(str(price_tick))
    slippage = Decimal(str(max_slip_bps))
    if bid <= 0 or ask <= bid or maximum <= 0 or tick <= 0:
        raise ValueError("invalid market or quote size")
    buy_price = (
        bid * (Decimal("1") - spread) / tick
    ).to_integral_value(rounding=ROUND_FLOOR) * tick
    sell_price = (
        ask * (Decimal("1") + spread) / tick
    ).to_integral_value(rounding=ROUND_CEILING) * tick
    sell_depth = _hedgeable_size(bid_levels, bid, slippage, False)
    buy_depth = _hedgeable_size(ask_levels, ask, slippage, True)
    return [
        ("BUY", float(buy_price), float(min(sell_depth, maximum)), float(bid)),
        ("SELL", float(sell_price), float(min(buy_depth, maximum)), float(ask)),
    ]


def hedge_side(client_side: str) -> str:
    if client_side == "BUY":
        return "SELL"
    if client_side == "SELL":
        return "BUY"
    raise ValueError("side must be BUY or SELL")


def trade_pnl(client_side: str, client_price: float, hedge_price: float,
              size: float, client_fee: float, hedge_fee: float) -> tuple[float, float]:
    client = Decimal(str(client_price))
    hedge = Decimal(str(hedge_price))
    qty = Decimal(str(size))
    if client_side == "BUY":
        spread = (hedge - client) * qty
    elif client_side == "SELL":
        spread = (client - hedge) * qty
    else:
        raise ValueError("side must be BUY or SELL")
    net = spread + Decimal(str(client_fee)) - Decimal(str(hedge_fee))
    return float(spread), float(net)


def reconcile(client_fills, hedge_fills) -> tuple[float, float, float]:
    def units(rows) -> int:
        total = 0
        for side, raw_size in rows:
            sign = 1 if side == "BUY" else -1 if side == "SELL" else None
            if sign is None:
                raise ValueError("side must be BUY or SELL")
            total += sign * int(
                (Decimal(str(raw_size)) * SIZE_SCALE).to_integral_value(rounding=ROUND_HALF_UP),
            )
        return total

    client_units = units(client_fills)
    hedge_units = units(hedge_fills)
    return (
        float(Decimal(client_units) / SIZE_SCALE),
        float(Decimal(hedge_units) / SIZE_SCALE),
        float(Decimal(client_units + hedge_units) / SIZE_SCALE),
    )
