
use std::mem::replace;

use serde::{Serialize, Deserialize};

use pgx::*;

use palloc::{Internal, in_memory_context};

use tdigest::TDigest;

use aggregate_utils::aggregate_mctx;

pg_module_magic!();

mod palloc;
mod aggregate_utils;

#[derive(Serialize, Deserialize)]
pub struct TDigestTransState {
    #[serde(skip_serializing)]
    buffer: Vec<f64>,
    digested: TDigest,
}

impl TDigestTransState {
    fn push(&mut self, value: f64) {
        self.buffer.push(value);
        if self.buffer.len() >= self.digested.max_size() {
            self.digest()
        }
    }

    fn digest(&mut self) {
        if self.buffer.is_empty() {
            return
        }
        let new = replace(&mut self.buffer, vec![]);
        self.digested = self.digested.merge_unsorted(new)
    }
}

#[allow(non_camel_case_types)]
type int = u32;

#[pg_extern]
fn tdigest_trans(
    state: Option<Internal<TDigestTransState>>,
    size: int,
    value: Option<f64>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<Internal<TDigestTransState>> {
    let mctx = aggregate_mctx(fcinfo);
    let mctx = match mctx {
        None => pgx::error!("cannot call as non-aggregate"),
        Some(mctx) => mctx,
    };
    unsafe {
        in_memory_context(mctx, || {
            let value = match value {
                None => return state,
                Some(value) => value,
            };
            let mut state = match state {
                None => TDigestTransState{
                    buffer: vec![],
                    digested: TDigest::new_with_size(size as _),
                }.into(),
                Some(state) => state,
            };
            state.push(value);
            Some(state)
        })
    }
}

#[pg_extern]
fn tdigest_final(
    state: Option<Internal<TDigestTransState>>,
    fcinfo: pg_sys::FunctionCallInfo,
) -> Option<f64> {
    let mctx = aggregate_mctx(fcinfo);
    let mctx = match mctx {
        None => pgx::error!("cannot call as non-aggregate"),
        Some(mctx) => mctx,
    };
    unsafe {
        in_memory_context(mctx, || {
            let mut state = match state {
                None => return None,
                Some(state) => state,
            };
            state.digest();
            Some(state.digested.estimate_quantile(0.50))
        })
    }
}

#[allow(non_camel_case_types)]
type bytea = pg_sys::Datum;

#[pg_extern]
fn tdigest_serialize(
    mut state: Internal<TDigestTransState>,
) -> bytea {
    state.digest();
    let size = bincode::serialized_size(&*state)
        .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
    let mut bytes = Vec::with_capacity(size as usize + 4);
    let mut varsize = [0; 4];
    unsafe {
        pgx::set_varsize(&mut varsize as *mut _ as *mut _, size as _);
    }
    bytes.extend_from_slice(&varsize);
    bincode::serialize_into(&mut bytes, &*state)
        .unwrap_or_else(|e| pgx::error!("serialization error {}", e));
    bytes.as_mut_ptr() as pg_sys::Datum
}

#[pg_extern]
fn hello_statscale() -> &'static str {
    "Hello, statscale"
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hello_statscale() {
        assert_eq!("Hello, statscale", crate::hello_statscale());
    }

}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
