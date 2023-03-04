use std::fmt::Display;

pub trait ConvertTypeToBigQueryType {
    fn to_bigquery_type() -> String where Self: Sized;
}

impl ConvertTypeToBigQueryType for bool {
    fn to_bigquery_type() -> String {
        "BOOL".to_string()
    }
}

impl ConvertTypeToBigQueryType for i32 {
    fn to_bigquery_type() -> String {
        "INT64".to_string()
    }
}

impl ConvertTypeToBigQueryType for i64 {
    fn to_bigquery_type() -> String {
        "INT64".to_string()
    }
}

impl ConvertTypeToBigQueryType for String {
    fn to_bigquery_type() -> String {
        "STRING".to_string()
    }
}

impl ConvertTypeToBigQueryType for &str {
    fn to_bigquery_type() -> String {
        "STRING".to_string()
    }
}

impl<T> ConvertTypeToBigQueryType for chrono::DateTime<T>
where T: chrono::TimeZone + Display + Send + Sync + 'static {
    fn to_bigquery_type() -> String {
        "DATETIME".to_string()
    }
}
