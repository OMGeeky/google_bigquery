use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use google_bigquery2::{Bigquery, hyper, hyper_rustls, oauth2};
use google_bigquery2::hyper::client::HttpConnector;
use google_bigquery2::hyper_rustls::HttpsConnector;

use crate::googlebigquery;

pub struct BigqueryClient {
    client: Bigquery<HttpsConnector<HttpConnector>>,
    project_id: String,
    dataset_id: String,
}

impl BigqueryClient {
    pub(crate) fn empty() -> &'static BigqueryClient {
        todo!("Implement BigqueryClient::empty() or throw an error if it's not possible or something.");
        // let hyper_client = hyper::Client::builder().build(
        //     hyper_rustls::HttpsConnectorBuilder::new()
        //         .with_native_roots()
        //         .https_or_http()
        //         .enable_http1()
        //         .enable_http2()
        //         .build(),
        // );
        //
        // let auth = oauth2::ServiceAccountAuthenticator::with_client();
        // let client =  Bigquery::new(hyper_client,auth);
        // Self {
        //     dataset_id: Default::default(),
        //     project_id: Default::default(),
        //     client,
        // }
    }
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