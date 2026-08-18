use super::{LoadArguments, LoadError};
use bytes::Bytes;
use serde_json::{Value, json};

pub(super) fn load_vectors(arguments: &LoadArguments) -> Result<Vec<Value>, LoadError> {
    if let Some(path) = &arguments.vectors {
        let vectors: Vec<Value> = serde_json::from_slice(&std::fs::read(path)?)?;
        if vectors.len() != arguments.pairs {
            return Err(LoadError::Invalid(format!(
                "vector file contains {} requests but --pairs is {}",
                vectors.len(),
                arguments.pairs
            )));
        }
        return Ok(vectors);
    }
    Ok(synthetic_vectors(arguments.lanes, arguments.pairs))
}

fn synthetic_vectors(lanes: usize, pairs: usize) -> Vec<Value> {
    directed_pairs(lanes)
        .into_iter()
        .take(pairs)
        .enumerate()
        .map(|(index, (asset_in, asset_out))| {
            json!({
                "assetIn": asset_in,
                "assetOut": asset_out,
                "amount": (1_000 + index).to_string(),
                "mode": if index % 2 == 0 { "exactIn" } else { "exactOut" }
            })
        })
        .collect()
}

fn directed_pairs(lanes: usize) -> Vec<(String, String)> {
    let cash = address(1);
    let assets = (0..lanes).map(lane_address).collect::<Vec<_>>();
    let mut pairs = Vec::with_capacity(lanes.saturating_mul(lanes));
    pairs.extend(assets.iter().enumerate().map(|(index, asset)| {
        if index % 2 == 0 {
            (cash.clone(), asset.clone())
        } else {
            (asset.clone(), cash.clone())
        }
    }));
    for offset in 1..lanes {
        pairs.extend(
            assets.iter().enumerate().map(|(input, asset_in)| {
                (asset_in.clone(), assets[(input + offset) % lanes].clone())
            }),
        );
    }
    pairs
}

pub(super) fn prepare_bodies(
    vectors: &[Value],
    batch_size: usize,
) -> Result<Vec<Bytes>, LoadError> {
    if vectors.is_empty() {
        return Err(LoadError::Invalid(
            "at least one quote vector is required".into(),
        ));
    }
    (0..vectors.len())
        .map(|offset| {
            if batch_size == 1 {
                serde_json::to_string(&vectors[offset])
                    .map(Bytes::from)
                    .map_err(LoadError::from)
            } else {
                let batch = (0..batch_size)
                    .map(|index| &vectors[(offset + index) % vectors.len()])
                    .collect::<Vec<_>>();
                serde_json::to_string(&batch)
                    .map(Bytes::from)
                    .map_err(LoadError::from)
            }
        })
        .collect()
}

/// Returns the deterministic lane address shared by synthetic load and process fixtures.
pub fn lane_address(index: usize) -> String {
    if index == 0 {
        address(2)
    } else {
        address(100_u64.saturating_add(index as u64))
    }
}

fn address(value: u64) -> String {
    format!("0x{value:040x}")
}

#[cfg(test)]
mod tests {
    use super::{address, directed_pairs, prepare_bodies, synthetic_vectors};
    use std::collections::HashSet;

    #[test]
    fn synthetic_vectors_cover_unique_direct_and_routed_pairs() {
        for lanes in [15, 64] {
            let pairs = directed_pairs(lanes)
                .into_iter()
                .take(100)
                .collect::<Vec<_>>();
            assert_eq!(pairs.iter().cloned().collect::<HashSet<_>>().len(), 100);
            assert_eq!(pairs[0], (address(1), super::lane_address(0)));
            assert_eq!(pairs[1], (super::lane_address(1), address(1)));
            assert!(
                pairs
                    .iter()
                    .any(|(input, output)| input != &address(1) && output != &address(1))
            );
            for lane in 0..lanes {
                let lane = super::lane_address(lane);
                assert!(
                    pairs
                        .iter()
                        .any(|(input, output)| input == &lane || output == &lane)
                );
            }
            let vectors = synthetic_vectors(lanes, 100);
            assert_eq!(vectors[0]["mode"], "exactIn");
            assert_eq!(vectors[1]["mode"], "exactOut");
        }
    }

    #[test]
    fn batch_bodies_have_the_requested_quote_count() {
        let bodies = prepare_bodies(&synthetic_vectors(15, 100), 256).unwrap();
        let body: Vec<serde_json::Value> = serde_json::from_slice(&bodies[0]).unwrap();
        assert_eq!(body.len(), 256);
    }
}
