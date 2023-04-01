use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::str::FromStr;
use async_trait::async_trait;

use google_bigquery2::api::{QueryParameter, QueryParameterType, QueryParameterValue, QueryRequest};
use google_bigquery2::hyper::{Body, Response};

use crate::client::BigqueryClient;
use crate::data::BigDataTableBase;
use crate::utils::BigDataValueType;

#[async_trait]
pub trait BigDataTableBaseConvenience<'a, TABLE, TPK>
: BigDataTableBase<'a, TABLE, TPK>
    where TPK: BigDataValueType<TPK> + FromStr + Debug + Clone,
          TABLE: Sync {
    fn get_pk_param(&self) -> google_bigquery2::api::QueryParameter;
    fn get_query_fields_str() -> String;
    fn get_query_fields_insert_str() -> String;
    fn get_query_fields_update_str(&self) -> String;
    fn get_where_part(field_name: &str, is_comparing_to_null: bool) -> String;
    //region run query
    async fn run_query(&self, req: QueryRequest, project_id: &str)
                       -> Result<(Response<Body>, google_bigquery2::api::QueryResponse), Box<dyn Error>>;

    async fn run_query_on_client(client: &'a BigqueryClient,
                                 req: QueryRequest,
                                 project_id: &str)
                                 -> Result<(Response<Body>, google_bigquery2::api::QueryResponse), Box<dyn Error>>;
    //endregion run query

    //region run get query
    async fn run_get_query(&self, query: &str, project_id: &str)
                           -> Result<google_bigquery2::api::QueryResponse, Box<dyn Error>>;

    async fn run_get_query_with_params(&self,
                                       query: &str,
                                       parameters: Vec<google_bigquery2::api::QueryParameter>,
                                       project_id: &str)
                                       -> Result<google_bigquery2::api::QueryResponse, Box<dyn Error>>;

    async fn run_get_query_with_params_on_client(client: &'a BigqueryClient,
                                                 query: &str,
                                                 parameters: Vec<google_bigquery2::api::QueryParameter>,
                                                 project_id: &str)
                                                 -> Result<google_bigquery2::api::QueryResponse, Box<dyn Error>>
        where TABLE: 'async_trait;
    //endregion


    // async fn get_identifier_and_base_where(&self) -> Result<(String, String), Box<dyn Error>>;
    async fn get_identifier(&self) -> Result<String, Box<dyn Error>>;
    async fn get_identifier_from_client(client: &'a BigqueryClient) -> Result<String, Box<dyn Error>>
        where TABLE: 'async_trait;
    fn get_base_where() -> String;

    // async fn get_identifier_and_base_where_from_client(client: &'a BigqueryClient, pk_name: &str, table_name: &str) -> Result<(String, String), Box<dyn Error>>;

    fn get_query_param<TField: BigDataValueType<TField>>(field_name: &str, field_value: &Option<TField>)
                                                         -> google_bigquery2::api::QueryParameter;

    fn parse_value_to_parameter<TValue>(value: &TValue) -> String
        where TValue: std::fmt::Display + BigDataValueType<TValue>;

    // fn create_from_table_row(client: &'a BigqueryClient,
    //                          row: &google_bigquery2::api::TableRow,
    //                          index_to_name_mapping: &HashMap<String, usize>)
    //                          -> Result<Self, Box<dyn Error>>
    //     where
    //         Self: Sized;
}

#[async_trait]
impl<'a, TABLE, TPK> BigDataTableBaseConvenience<'a, TABLE, TPK> for TABLE
    where
        TABLE:  BigDataTableBase<'a, TABLE, TPK> + Sync,
        TPK: BigDataValueType<TPK> + FromStr + Debug + Clone,
        <TPK as FromStr>::Err: Debug,
{
    fn get_pk_param(&self) -> QueryParameter {
        Self::get_query_param(&Self::get_pk_name(), &Some(self.get_pk_value()))
        // QueryParameter {
        //     name: Some(format!("__{}",Self::get_pk_name())),
        //     parameter_type: Some(QueryParameterType {
        //         array_type: None,
        //         struct_types: None,
        //         type_: Some(TPK::to_bigquery_type()),
        //     }),
        //     parameter_value: Some(QueryParameterValue {
        //         value: Some(self.get_pk_value().to_bigquery_param_value()),
        //         ..Default::default()
        //     }),
        // }
    }
    fn get_query_fields_str() -> String {
        let mut values = Self::get_query_fields().values()
            .into_iter()
            .map(|v| format!("{}", v))
            .collect::<Vec<String>>();
        values.sort();
        values.join(", ")
    }

    fn get_query_fields_insert_str() -> String {
        let mut values = Self::get_query_fields()
            .values()
            .into_iter()
            .map(|v| format!("@__{}", v))
            .collect::<Vec<String>>();
        values.sort();
        values.join(", ")
    }

    fn get_query_fields_update_str(&self) -> String {
        let x = Self::get_query_fields();
        let pk_name = Self::get_pk_name();
        let mut values = x.values()
            .filter(|k| *k != &pk_name)
            .map(|k| format!("{} = @__{}", k, k))
            .collect::<Vec<String>>();
        values.sort();
        let update_str = values.join(", ");
        update_str
    }

    fn get_where_part(field_name: &str, is_comparing_to_null: bool) -> String {
        if is_comparing_to_null {
            format!("{} IS NULL", field_name)
        } else {
            format!("{} = @__{}", field_name, field_name)
        }
    }

    //region run query

    async fn run_query(&self, req: QueryRequest, project_id: &str)
                       -> Result<(Response<Body>, google_bigquery2::api::QueryResponse), Box<dyn Error>> {
        Self::run_query_on_client(self.get_client(), req, project_id).await
    }

    async fn run_query_on_client(client: &'a BigqueryClient,
                                 req: QueryRequest,
                                 project_id: &str)
                                 -> Result<(Response<Body>, google_bigquery2::api::QueryResponse), Box<dyn Error>> {
        #[cfg(debug_assertions="true")]
        {
            println!("Query: {}", &req.query.as_ref().unwrap());//There has to be a query, this would not make any sense otherwise
            if let Some(parameters) = &req.query_parameters {
                println!("Parameters: {}", parameters.len());
                for (i, param) in parameters.iter().enumerate() {
                    println!("{:2}: {:?}", i, param);
                }
            } else {
                println!("Parameters: None");
            }
            println!();
        }

        let (res, query_res) = client.get_client().jobs().query(req, project_id)
            .doit().await?;

        if res.status() != 200 {
            return Err(format!("Wrong status code returned! ({})", res.status()).into());
        }

        Ok((res, query_res))
    }
    //endregion run query

    async fn run_get_query(&self, query: &str, project_id: &str)
                           -> Result<google_bigquery2::api::QueryResponse, Box<dyn Error>> {
        let parameters = vec![self.get_pk_param()];//default parameters (pk)
        self.run_get_query_with_params(query, parameters, project_id).await
    }


    async fn run_get_query_with_params(&self,
                                       query: &str,
                                       parameters: Vec<google_bigquery2::api::QueryParameter>,
                                       project_id: &str)
                                       -> Result<google_bigquery2::api::QueryResponse, Box<dyn Error>> {
        let client = self.get_client();
        Self::run_get_query_with_params_on_client(client, query, parameters, project_id).await
    }

    async fn run_get_query_with_params_on_client(client: &'a BigqueryClient,
                                                 query: &str,
                                                 parameters: Vec<google_bigquery2::api::QueryParameter>,
                                                 project_id: &str)
                                                 -> Result<google_bigquery2::api::QueryResponse, Box<dyn Error>> {
        let req = google_bigquery2::api::QueryRequest {
            query: Some(query.to_string()),
            query_parameters: Some(parameters),
            use_legacy_sql: Some(false),
            ..Default::default()
        };
        let (_, query_res) = Self::run_query_on_client(client, req, project_id).await?;
        // let (res, query_res) = client.get_client().jobs().query(req, project_id)
        //     .doit().await?;
        //
        // if res.status() != 200 {
        //     return Err(format!("Wrong status code returned! ({})", res.status()).into());
        // }
        Ok(query_res)
    }
    // async fn get_identifier_and_base_where(&self)
    //                                        -> Result<(String, String), Box<dyn Error>> {
    //     let pk_name = Self::get_pk_name();
    //     let table_name = Self::get_table_name();
    //     Ok(Self::get_identifier_and_base_where_from_client(&self.get_client(), &pk_name, &table_name).await?)
    // }
    async fn get_identifier(&self) -> Result<String, Box<dyn Error>> {
        let client = self.get_client();
        Self::get_identifier_from_client(&client).await
    }
    async fn get_identifier_from_client(client: &'a BigqueryClient) -> Result<String, Box<dyn Error>> {
        let dataset_id = client.get_dataset_id();
        let table_name = Self::get_table_name();
        let table_identifier = format!("{}.{}", dataset_id, table_name);
        Ok(table_identifier)
    }

    fn get_base_where() -> String {
        let pk_name = Self::get_pk_name();
        Self::get_where_part(&pk_name, false)
    }

    fn get_query_param<TField: BigDataValueType<TField>>(field_name: &str, field_value: &Option<TField>) -> google_bigquery2::api::QueryParameter
    {
        let type_to_string: String = TField::to_bigquery_type();
        let value: Option<google_bigquery2::api::QueryParameterValue> = Some(google_bigquery2::api::QueryParameterValue {
            value: match field_value {
                Some(value) => Some(value.to_bigquery_param_value()),//TODO: maybe add a way to use array types
                None => None,
            },
            ..Default::default()
        });

        google_bigquery2::api::QueryParameter {
            name: Some(format!("__{}", field_name.clone())),
            parameter_type: Some(google_bigquery2::api::QueryParameterType {
                type_: Some(type_to_string),
                ..Default::default()
            }),
            parameter_value: value,
            ..Default::default()
        }
    }
    fn parse_value_to_parameter<TValue>(value: &TValue) -> String
        where TValue: std::fmt::Display + BigDataValueType<TValue>
    {
        return value.to_bigquery_param_value();
    }

    //
    // fn create_from_table_row(client: &'a BigqueryClient,
    //                          row: &google_bigquery2::api::TableRow,
    //                          index_to_name_mapping: &HashMap<String, usize>)
    //                          -> Result<Self, Box<dyn Error>>
    //     where
    //         Self: Sized
    // {
    //     let pk_index = *index_to_name_mapping.get(&Self::get_pk_name()).unwrap();
    //     let pk = row
    //         .f.as_ref()
    //         .unwrap()[pk_index]
    //         .v.as_ref()
    //         .unwrap()
    //         .parse::<TPK>()
    //         .unwrap();
    //     let mut res = Self::create_with_pk(client, pk);
    //     res.write_from_table_row(row, index_to_name_mapping)?;
    //     Ok(res)
    // }
}
