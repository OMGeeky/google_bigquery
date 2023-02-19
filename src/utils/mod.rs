pub use convert_type_to_big_query_type::ConvertTypeToBigQueryType;
pub use convert_value_to_bigquery_param_value::ConvertValueToBigqueryParamValue;

mod convert_type_to_big_query_type;
mod convert_value_to_bigquery_param_value;

pub trait BigDataValueType: ConvertTypeToBigQueryType + ConvertValueToBigqueryParamValue {}
impl<T: ConvertTypeToBigQueryType + ConvertValueToBigqueryParamValue> BigDataValueType for T {}