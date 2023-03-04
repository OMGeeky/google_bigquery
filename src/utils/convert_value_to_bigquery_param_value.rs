use std::error::Error;
use std::fmt::Display;
use std::str::FromStr;

use chrono::{NaiveDateTime, Utc};

pub trait ConvertValueToBigqueryParamValue {
    fn to_bigquery_param_value(&self) -> String;
    fn from_bigquery_value(value :&String) -> Result<Self, Box<dyn Error>> where Self: Sized;
}

impl ConvertValueToBigqueryParamValue for i64 {
    fn to_bigquery_param_value(&self) -> String {
        format!("{}", self)
    }
    fn from_bigquery_value(value :&String) -> Result<Self, Box<dyn Error>>  where Self: Sized{
        Ok(value.parse()?)
    }
}
impl ConvertValueToBigqueryParamValue for String {
    fn to_bigquery_param_value(&self) -> String {
        self.to_string()
    }
    fn from_bigquery_value(value :&String) -> Result<Self, Box<dyn Error>>  where Self: Sized{
        Ok(value.to_string())
    }
}

impl ConvertValueToBigqueryParamValue for bool {
    fn to_bigquery_param_value(&self) -> String {
        match self.to_string().as_str() {
            "true" => "TRUE".to_string(),
            "false" => "FALSE".to_string(),
            _ => panic!("Invalid value for bool"),
        }
    }
    fn from_bigquery_value(value :&String) -> Result<Self, Box<dyn Error>>  where Self: Sized{
        match value.as_str() {
            "TRUE" => Ok(true),
            "FALSE" => Ok(false),
            _ => Err("Invalid value for bool".into()),
        }
    }
}


impl ConvertValueToBigqueryParamValue for chrono::DateTime<Utc> {
    fn to_bigquery_param_value(&self) -> String {
        println!("ConvertValueToBigqueryParamValue::to_bigquery_param_value DateTime<Utc> -> in:  {:?}", self);
        let value = self.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let value = value.replace("Z", "").replace("T", " ");
        // let value = format!("\"{}\"", value);
        println!("ConvertValueToBigqueryParamValue::to_bigquery_param_value DateTime<Utc> -> out: {}", value);
        value

    }
    fn from_bigquery_value(value :&String) -> Result<Self, Box<dyn Error>>  where Self: Sized{
        // println!("ConvertValueToBigqueryParamValue::from_bigquery_value DateTime<Utc> -> in:  {:?}", value);
        let value = value.replace("T", " ").replace("Z", "");
        // let x = NaiveDateTime::from_str(&value)
        let x = NaiveDateTime::parse_from_str(&value,"%Y-%m-%d %H:%M:%S")
            .expect(&format!("Could not parse &String to NaiveDateTime: {}", value));
        let time = chrono::DateTime::<Utc>::from_utc(x, Utc);
        // let x = chrono::DateTime::parse_from_rfc3339(value)?;
        // let time = x.with_timezone(&Utc);
        // println!("ConvertValueToBigqueryParamValue::from_bigquery_value DateTime<Utc> -> out: {:?}", time);
        Ok(time)
    }
}

impl<R:ConvertValueToBigqueryParamValue> ConvertValueToBigqueryParamValue for Option<R>{
    fn to_bigquery_param_value(&self) -> String {
        match self {
            Some(x) => x.to_bigquery_param_value(),
            None => "NULL".to_string(),
        }
    }
    fn from_bigquery_value(value :&String) -> Result<Option<R>, Box<dyn Error>>  where Self: Sized {
        if value == "NULL" {
            Ok(None)
        } else {
            Ok(R::from_bigquery_value(value).ok())
        }
    }
}

// impl<R: Display + FromStr> ConvertValueToBigqueryParamValue for R where <R as FromStr>::Err: std::error::Error{
//     default fn to_bigquery_param_value(&self) -> String {
//         format!("{}", self)
//     }
//     default fn from_bigquery_value(value :&String) -> Result<Self, Box<dyn Error>>  where Self: Sized{
//         Ok(value.parse()?)
//     }
// }