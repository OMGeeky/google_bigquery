use std::fmt::Display;

pub trait ConvertTypeToBigQueryType {
    fn to_bigquery_type() -> String;
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
