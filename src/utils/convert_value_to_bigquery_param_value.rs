use std::fmt::Display;

pub trait ConvertValueToBigqueryParamValue {
    fn to_bigquery_param_value(&self) -> String;
}

impl ConvertValueToBigqueryParamValue for bool {
    fn to_bigquery_param_value(&self) -> String {
        match self.to_string().as_str() {
            "true" => "TRUE".to_string(),
            "false" => "FALSE".to_string(),
            _ => panic!("Invalid value for bool"),
        }
    }
}

impl<R: Display> ConvertValueToBigqueryParamValue for R {
    default fn to_bigquery_param_value(&self) -> String {
        format!("{}", self)
    }
}