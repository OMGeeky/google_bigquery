use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;

use crate::client::{BigqueryClient, HasBigQueryClient};
use crate::utils::BigDataValueType;

pub trait BigDataTableHasPk<TPK>
    where TPK: BigDataValueType<TPK> + FromStr + std::fmt::Debug + Clone {
    fn get_pk_name() -> String;
    fn get_pk_value(&self) -> TPK;
}

pub trait BigDataTableBase<'a, TABLE, TPK>: HasBigQueryClient<'a> + BigDataTableHasPk<TPK>
    where TPK: BigDataValueType<TPK> + FromStr + std::fmt::Debug + Clone
{
    // fn get_pk_name() -> String;
    // fn get_pk_value(&self) -> TPK;
    fn get_field_name(field_name: &str) -> Result<String, Box<dyn Error>>;
    fn get_query_fields() -> HashMap<String, String>;
    fn get_table_name() -> String;
    fn create_with_pk(client: &'a BigqueryClient, pk: TPK) -> TABLE;
    fn write_from_table_row(&mut self,
                            row: &google_bigquery2::api::TableRow,
                            index_to_name_mapping: &HashMap<String, usize>)
                            -> Result<(), Box<dyn Error>>;
    // fn get_query_fields_update_str(&self) -> String;
    fn get_all_query_parameters(&self) -> Vec<google_bigquery2::api::QueryParameter>;

    fn create_from_table_row(client: &'a BigqueryClient,
                             row: &google_bigquery2::api::TableRow,
                             index_to_name_mapping: &HashMap<String, usize>)
                             -> Result<Self, Box<dyn Error>>
        where
            Self: Sized;

    // fn parse_bigquery_value<T: BigDataValueType<T>>(value: &String) -> Result<T, Box<dyn Error>>;
}
