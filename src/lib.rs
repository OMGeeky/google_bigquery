#![feature(async_fn_in_trait)]
#![feature(specialization)]
#![allow(unused)]
#![allow(incomplete_features)]

pub use google_bigquery_derive::HasBigQueryClient as HasBigQueryClientDerive;
pub use google_bigquery_derive::BigDataTable as BigDataTableDerive;
// pub use google_bigquery_derive::MyDerive;

pub use client::{BigqueryClient, HasBigQueryClient};
pub use data::{BigDataTable, BigDataTableBase, BigDataTableBaseConvenience, BigDataTableHasPk};

pub mod client;
mod googlebigquery;
mod data;
pub mod utils;

// pub fn add(left: usize, right: usize) -> usize {
//     left + right
// }

#[cfg(test)]
mod tests;
