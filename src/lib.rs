#![feature(async_fn_in_trait)]
#![feature(specialization)]
#![allow(unused)]
#![allow(incomplete_features)]
// #![feature(impl_trait_projections)]
pub mod client;
mod googlebigquery;
mod data;
mod utils;

pub use google_bigquery_derive;
pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests;
