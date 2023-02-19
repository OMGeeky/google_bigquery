use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::str::FromStr;

use google_bigquery2::api::{QueryParameter, TableSchema};

pub use big_data_table_base::BigDataTableBase;
pub use big_data_table_base_convenience::BigDataTableBaseConvenience;

use crate::client::{BigqueryClient, HasBigQueryClient};
use crate::utils::BigDataValueType;

mod big_data_table_base_convenience;
mod big_data_table_base;

// pub trait BigDataTable<'a, TABLE, TPK: BigDataValueType + FromStr + Debug>: HasBigQueryClient<'a> + BigDataTableBaseConvenience<'a, TABLE, TPK> + BigDataTableBase<'a, TABLE, TPK> {
pub trait BigDataTable<'a, TABLE, TPK>
: HasBigQueryClient<'a>
+ BigDataTableBaseConvenience<'a, TABLE, TPK>
+ BigDataTableBase<'a, TABLE, TPK>
+ Default
    where TPK: BigDataValueType + FromStr + Debug {
    async fn from_pk(
        client: &'a BigqueryClient,
        pk: TPK,
    ) -> Result<Self, Box<dyn Error>>
        where
            Self: Sized;
    async fn save_to_bigquery(&self) -> Result<(), Box<dyn Error>>;
    async fn load_from_bigquery(&mut self) -> Result<(), Box<dyn Error>>;
    async fn load_by_field<T: BigDataValueType>(client: &'a BigqueryClient, field_name: &str, field_value: Option<T>, max_amount: usize)
                                                -> Result<Vec<TABLE>, Box<dyn Error>>;

    async fn load_by_custom_query(client: &'a BigqueryClient, query: &str, parameters: Vec<QueryParameter>, max_amount: usize)
                                  -> Result<Vec<TABLE>, Box<dyn Error>>;
}

impl<'a, TABLE, TPK> BigDataTable<'a, TABLE, TPK> for TABLE
where
    TABLE: HasBigQueryClient<'a> + BigDataTableBaseConvenience<'a, TABLE, TPK> + Default,
    TPK: BigDataValueType + FromStr + Debug,
    <TPK as FromStr>::Err: Debug
{
    async fn from_pk(client: &'a BigqueryClient, pk: TPK) -> Result<Self, Box<dyn Error>> where Self: Sized {
        let mut res = Self::create_with_pk(client, pk);
        res.load_from_bigquery().await?;
        Ok(res)
    }

    async fn save_to_bigquery(&self) -> Result<(), Box<dyn Error>> {
        let project_id = self.get_client().get_project_id();

        let table_identifier = self.get_identifier().await?;
        let where_clause = Self::get_base_where();
        // region check for existing data
        let exists_row: bool;
        let existing_count = format!("select count(*) from {} where {} limit 1", table_identifier, where_clause);

        let req = google_bigquery2::api::QueryRequest {
            query: Some(existing_count),
            query_parameters: Some(vec![self.get_pk_param()]),
            use_legacy_sql: Some(false),
            ..Default::default()
        };

        let (_, query_res) = self.run_query(req, project_id).await?;
        // let (res, query_res) = self.get_client().get_client().jobs().query(req, project_id)
        //     .doit().await?;
        //
        // if res.status() != 200 {
        //     return Err(format!("Wrong status code returned! ({})", res.status()).into());
        // }

        if let None = &query_res.rows {
            return Err("No rows returned!".into());
        }

        let rows = query_res.rows.unwrap();

        if rows.len() != 1 {
            return Err(format!("Wrong amount of data returned! ({})", rows.len()).into());
        }

        let row = &rows[0];
        let amount: i32 = row.f.as_ref().unwrap()[0].clone().v.unwrap().parse().unwrap();

        if amount == 0 {
            exists_row = false;
        } else if amount == 1 {
            exists_row = true;
        } else {
            panic!("Too many rows with key!");
        }

        // endregion


        // region update or insert

        let query = match exists_row {
            true => format!("update {} set {} where {}", table_identifier, self.get_query_fields_update_str(), where_clause),
            false => format!("insert into {} ({}, {}) values(@__{}, {})", table_identifier,
                             Self::get_pk_name(),
                             Self::get_query_fields_str(),
                             Self::get_pk_name(),
                             Self::get_query_fields_insert_str()),
        };

        let mut query_parameters = self.get_all_query_parameters();
        // query_parameters.push(self.get_pk_param()); // todo: check if this is needed
        let req = google_bigquery2::api::QueryRequest {
            query: Some(query),
            query_parameters: Some(query_parameters),
            use_legacy_sql: Some(false),
            ..Default::default()
        };


        let (_, _) = self.run_query(req, project_id).await?;
        // let (res, _) = self.get_client().get_client().jobs().query(req, project_id)
        //     .doit().await?;
        //
        // if res.status() != 200 {
        //     return Err(format!("Wrong status code returned! ({})", res.status()).into());
        // }

        //endregion

        Ok(())
    }

    async fn load_from_bigquery(&mut self) -> Result<(), Box<dyn Error>> {
        let project_id = self.get_client().get_project_id();
        let table_identifier  = self.get_identifier().await?;
        let where_clause = Self::get_base_where();

        let query = format!("select {} from {} where {} limit 1", Self::get_query_fields_str(), table_identifier, where_clause);
        let query_res = self.run_get_query(&query, project_id).await?;

        if let None = &query_res.rows {
            return Err("No rows returned!".into());
        }

        let rows = query_res.rows.unwrap();

        if rows.len() != 1 {
            return Err(format!("Wrong amount of data returned! ({})", rows.len()).into());
        }
        let mut index_to_name_mapping: HashMap<String, usize> = get_name_index_mapping(query_res.schema);
        println!("index_to_name_mapping: {:?}", index_to_name_mapping);

        let row = &rows[0];
        self.write_from_table_row(row, &index_to_name_mapping)
    }

    async fn load_by_field<T: BigDataValueType>(client: &'a BigqueryClient, field_name: &str, field_value: Option<T>, max_amount: usize)
                                                -> Result<Vec<TABLE>, Box<dyn Error>> {
        let field_name: String = field_name.into();
        let field_name = Self::get_field_name(&field_name).expect(format!("Field '{}' not found!", field_name).as_str());
        let where_clause = Self::get_where_part(&field_name, field_value.is_none());
        // let where_clause = format!(" {} = @__{}", field_name, field_name);
        let table_identifier = Self::get_identifier_from_client(client).await?;
        let query = format!("select {} from {} where {} limit {}",  Self::get_query_fields_str(), table_identifier, where_clause, max_amount);

        let mut params = vec![];
        if !(field_value.is_none()) {
            params.push(Self::get_query_param(&field_name, &field_value));
        }
        Self::load_by_custom_query(client, &query, params, max_amount).await
    }

    async fn load_by_custom_query(client: &'a BigqueryClient, query: &str, parameters: Vec<QueryParameter>, max_amount: usize)
                                  -> Result<Vec<TABLE>, Box<dyn Error>> {

        let project_id = client.get_project_id();
        let query_res: google_bigquery2::api::QueryResponse = Self::run_get_query_with_params_on_client(client, &query, parameters, project_id).await?;

        if let None = &query_res.rows {
            return Ok(vec![]);
        }

        let rows: Vec<google_bigquery2::api::TableRow> = query_res.rows.unwrap();
        let mut result: Vec<TABLE> = vec![];

        let mut index_to_name_mapping: HashMap<String, usize> = get_name_index_mapping(query_res.schema);

        for row in rows.iter() {
            for cell in row.f.iter() {
                //create a new object and write the values to each field
                let obj = Self::create_from_table_row(client, row, &index_to_name_mapping)?;
                result.push(obj);
            }
        }

        Ok(result)
    }
}

fn get_name_index_mapping(schema: Option<TableSchema>) -> HashMap<String, usize> {
    let mut index_to_name_mapping: HashMap<String, usize> = HashMap::new();
    for (i, x) in schema.unwrap().fields.unwrap().iter().enumerate() {
        index_to_name_mapping.insert(x.name.clone().unwrap(), i);
    }
    index_to_name_mapping
}
