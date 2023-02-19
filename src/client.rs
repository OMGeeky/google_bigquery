use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use google_bigquery2::Bigquery;
use google_bigquery2::hyper::client::HttpConnector;
use google_bigquery2::hyper_rustls::HttpsConnector;

use crate::googlebigquery;

pub struct BigqueryClient {
    client: Bigquery<HttpsConnector<HttpConnector>>,
    project_id: String,
    dataset_id: String,
}

impl BigqueryClient {
    pub async fn new<S: Into<String>>(
        project_id: S,
        dataset_id: S,
        service_account_path: Option<S>,
    ) -> Result<BigqueryClient, Box<dyn Error>> {
        let client = googlebigquery::get_client(service_account_path).await?;
        Ok(BigqueryClient {
            client,
            project_id: project_id.into(),
            dataset_id: dataset_id.into(),
        })
    }

    pub fn get_client(&self) -> &Bigquery<HttpsConnector<HttpConnector>> {
        &self.client
    }
    pub fn get_project_id(&self) -> &str {
        &self.project_id
    }
    pub fn get_dataset_id(&self) -> &str {
        &self.dataset_id
    }
}

impl Debug for BigqueryClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigqueryClient")
            .field("project_id", &self.project_id)
            .field("dataset_id", &self.dataset_id)
            .finish()
    }
}

pub trait HasBigQueryClient<'a> {
    fn get_client(&self) -> &'a BigqueryClient;
}