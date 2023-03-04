// pub use convert_bigquery_value_to_value::ConvertBigQueryValueToValue2 as ConvertBigQueryValueToValue;
// pub use convert_bigquery_value_to_value::ConvertBigQueryValueToOptionValue2 as ConvertBigQueryValueToOptionValue;
pub use convert_type_to_big_query_type::ConvertTypeToBigQueryType;
pub use convert_value_to_bigquery_param_value::ConvertValueToBigqueryParamValue;

mod convert_type_to_big_query_type;
mod convert_value_to_bigquery_param_value;
mod convert_bigquery_value_to_value;

pub trait BigDataValueType<T>: ConvertTypeToBigQueryType + ConvertValueToBigqueryParamValue {}

impl<T: ConvertTypeToBigQueryType + ConvertValueToBigqueryParamValue> BigDataValueType<T> for T
{}

