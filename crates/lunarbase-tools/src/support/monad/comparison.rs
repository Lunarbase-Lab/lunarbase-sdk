use super::{
    helpers::parse_u256_hex,
    types::{MonadArguments, SolidityCall, ValidationVector},
    *,
};

pub(super) async fn compare_vector(
    http: &reqwest::Client,
    arguments: &MonadArguments,
    vector: &ValidationVector,
    solidity: &SolidityCall,
) -> bool {
    let quote = http
        .post(format!(
            "{}/v1/quote",
            arguments.indexer_url.trim_end_matches('/')
        ))
        .json(&vector.quote)
        .send()
        .await;
    let Ok(quote) = quote else {
        return false;
    };
    let Ok(quote) = quote.json::<Value>().await else {
        return false;
    };
    let Some(quote_amount) = quote
        .pointer(&format!("/result/{}", solidity.quote_field))
        .and_then(Value::as_str)
        .and_then(|value| U256::from_str(value).ok())
    else {
        return false;
    };
    let rpc = http
        .post(&arguments.rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{
                "to": solidity.to,
                "data": solidity.data,
            }, solidity.block_tag],
        }))
        .send()
        .await;
    let Ok(rpc) = rpc else {
        return false;
    };
    let Ok(rpc) = rpc.json::<Value>().await else {
        return false;
    };
    rpc.get("result")
        .and_then(Value::as_str)
        .and_then(parse_u256_hex)
        .is_some_and(|solidity_amount| solidity_amount == quote_amount)
}
