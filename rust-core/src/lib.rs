use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

const SIZE_SCALE: f64 = 100_000_000.0;

#[inline]
fn side_sign(side: &str) -> PyResult<i64> {
    match side {
        "BUY" => Ok(1),
        "SELL" => Ok(-1),
        _ => Err(PyValueError::new_err("side must be BUY or SELL")),
    }
}

#[pyfunction]
fn validate_profitability(spread_bps: f64, fee_bps: f64, slippage_bps: f64) -> PyResult<f64> {
    let floor = spread_bps - fee_bps - slippage_bps;
    if !floor.is_finite() || floor <= 0.0 {
        return Err(PyValueError::new_err("价差必须高于 BitTrade Maker 费率、GMO 手续费与预期滑点之和"));
    }
    Ok(floor)
}

#[pyfunction]
fn make_quotes(
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
    spread_bps: f64,
    max_quote_size: f64,
    price_tick: f64,
) -> PyResult<Vec<(String, f64, f64, f64)>> {
    if bid <= 0.0 || ask <= bid || max_quote_size <= 0.0 || price_tick <= 0.0 {
        return Err(PyValueError::new_err("invalid market or quote size"));
    }
    let spread = spread_bps / 10_000.0;
    let buy_price = ((bid * (1.0 - spread)) / price_tick).floor() * price_tick;
    let sell_price = ((ask * (1.0 + spread)) / price_tick).ceil() * price_tick;
    Ok(vec![
        ("BUY".into(), buy_price, bid_size.min(max_quote_size).max(0.0), bid),
        ("SELL".into(), sell_price, ask_size.min(max_quote_size).max(0.0), ask),
    ])
}

#[pyfunction]
fn hedge_side(client_side: &str) -> PyResult<&'static str> {
    match client_side {
        "BUY" => Ok("SELL"),
        "SELL" => Ok("BUY"),
        _ => Err(PyValueError::new_err("side must be BUY or SELL")),
    }
}

#[pyfunction]
fn trade_pnl(
    client_side: &str,
    client_price: f64,
    hedge_price: f64,
    size: f64,
    client_fee: f64,
    hedge_fee: f64,
) -> PyResult<(f64, f64)> {
    let spread_pnl = match client_side {
        "BUY" => (hedge_price - client_price) * size,
        "SELL" => (client_price - hedge_price) * size,
        _ => return Err(PyValueError::new_err("side must be BUY or SELL")),
    };
    Ok((spread_pnl, spread_pnl + client_fee - hedge_fee))
}

#[pyfunction]
fn reconcile(client_fills: Vec<(String, f64)>, hedge_fills: Vec<(String, f64)>) -> PyResult<(f64, f64, f64)> {
    let mut client_units: i64 = 0;
    let mut hedge_units: i64 = 0;
    for (side, size) in client_fills {
        client_units += side_sign(&side)? * (size * SIZE_SCALE).round() as i64;
    }
    for (side, size) in hedge_fills {
        hedge_units += side_sign(&side)? * (size * SIZE_SCALE).round() as i64;
    }
    let client = client_units as f64 / SIZE_SCALE;
    let hedge = hedge_units as f64 / SIZE_SCALE;
    Ok((client, hedge, (client_units + hedge_units) as f64 / SIZE_SCALE))
}

#[pymodule]
fn hedge_core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(validate_profitability, module)?)?;
    module.add_function(wrap_pyfunction!(make_quotes, module)?)?;
    module.add_function(wrap_pyfunction!(hedge_side, module)?)?;
    module.add_function(wrap_pyfunction!(trade_pnl, module)?)?;
    module.add_function(wrap_pyfunction!(reconcile, module)?)?;
    Ok(())
}
