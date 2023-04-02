use std::error::Error;

use google_bigquery2::{hyper, hyper_rustls, oauth2, Bigquery};
// use google_bigquery2::api::QueryRequest;
use google_bigquery2::hyper::client::HttpConnector;
use google_bigquery2::hyper_rustls::HttpsConnector;

pub async fn get_client<S: Into<String>>(
    service_account_path: Option<S>,
) -> Result<Bigquery<HttpsConnector<HttpConnector>>, Box<dyn Error>> {
    let hyper_client = hyper::Client::builder().build(
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build(),
    );
    let service_account_path = match service_account_path {
        None => "auth/service_account2.json".to_string(),
        Some(s) => s.into(),
    };
    let secret = oauth2::read_service_account_key(service_account_path)
        .await
        .expect(format!("Failed to read service account key from file. {}", service_account_path).as_str());
    let auth = oauth2::ServiceAccountAuthenticator::builder(secret)
        .build()
        .await
        .expect("Failed to authenticate with service account key.");
    let client: Bigquery<HttpsConnector<HttpConnector>> = Bigquery::new(hyper_client, auth);

    Ok(client)
}
