use std::error::Error;
use std::fmt::Debug;
use std::str::FromStr;

use chrono::{DateTime, Utc};

use crate::utils::BigDataValueType;



//
// pub fn to_value<T>(x: &String) -> Result<T, Box<dyn Error>> {
//     let x = x.parse()?;
//     Ok(x)
// }
// //TODO: figure out how I can make this work so i can have specific
// // implementations for DateTime<Utc> and some other types but also have a
// // generic implementation for all other types
//
// impl ConvertBigQueryValueToValue2<DateTime<Utc>> {
//     pub fn to_value(x: &String) -> Result<DateTime<Utc>, Box<dyn Error>> {
//         let x = DateTime::parse_from_rfc3339(x)?;
//         let time = x.with_timezone(&Utc);
//         Ok(time)
//     }
// }
//
// impl<T> ConvertBigQueryValueToOptionValue2<T> {
//     pub fn to_value(x: &String) -> Option<T> {
//         ConvertBigQueryValueToValue2::<T>::to_value(x).ok()
//     }
// }
//
// impl ConvertBigQueryValueToOptionValue2<DateTime<Utc>> {
//     pub fn to_value(x: &String) -> Option<DateTime<Utc>> {
//         ConvertBigQueryValueToValue2::<DateTime<Utc>>::to_value(x).ok()
//     }
// }
//
// pub trait ConvertBigQueryValueToValue<T> {
//     fn to_value(&self) -> Result<T, Box<dyn Error>>;
// }
//
// pub trait ConvertBigQueryValueToOptionValue<T> {
//     fn to_opt_value(&self) -> Result<Option<T>, Box<dyn Error>>;
// }
//
// impl ConvertBigQueryValueToValue<chrono::DateTime<Utc>> for &String {
//     fn to_value(&self) -> Result<chrono::DateTime<Utc>, Box<dyn Error>> {
//         println!("ConvertBigQueryValueToValue DateTime<Utc> -> in: {:?}", self);
//         let x = chrono::DateTime::parse_from_rfc3339(self)?;
//         let time = x.with_timezone(&Utc);
//         println!("ConvertBigQueryValueToValue DateTime<Utc> -> out: {:?}", time);
//         Ok(time)
//     }
// }
//
// impl<R: FromStr> ConvertBigQueryValueToValue<R> for &String
//     where R::Err: Error + 'static
// {
//     default fn to_value(&self) -> Result<R, Box<dyn Error>> {
//         let x = self.parse()?;
//         Ok(x)
//     }
// }
//
// impl ConvertBigQueryValueToValue<String> for &String {
//     default fn to_value(&self) -> Result<String, Box<dyn Error>> {
//         let x = self.to_string();
//         Ok(x)
//     }
// }
//
// // impl<S: ConvertBigQueryValueToValue<R>, R: FromStr> ConvertBigQueryValueToOptionValue<R> for S
// //     where R::Err: Error + 'static,
// //           S: ConvertBigQueryValueToValue<R> {
// //     default fn to_opt_value(&self) -> Result<Option<R>, Box<dyn Error>> {
// //         Ok(match (self as &dyn ConvertBigQueryValueToValue<R>).to_value() {
// //             Ok(x) => Some(x),
// //             Err(_) => None,
// //         })
// //     }
// // }
//
// impl ConvertBigQueryValueToOptionValue<DateTime<Utc>> for &String
// {
//     default fn to_opt_value(&self) -> Result<Option<DateTime<Utc>>, Box<dyn Error>> {
//         Ok(match (self as &dyn ConvertBigQueryValueToValue<DateTime<Utc>>).to_value() {
//             Ok(x) => Some(x),
//             Err(_) => None,
//         })
//     }
// }
//
// #[cfg(test)]
// fn test123() {
//     let x = &"2021-01-01T00:00:00Z".to_string();
//     let y: chrono::DateTime<Utc> = x.to_value().unwrap();
//     let z: Option<chrono::DateTime<Utc>> = x.to_opt_value().unwrap();
//     println!("{:?}", y);
//     let x = "2021-01-01T00:00:00Z".to_string();
//     let y: i64 = x.to_value().unwrap();
//     let z: Option<i64> = x.to_opt_value().unwrap();
//     println!("{:?}", y);
// }
//
//
//
// impl<R: FromStr + Debug> ConvertBigQueryValueToValue<R> for String
//     where R::Err: Error + 'static
// {
//     default fn to_value(&self) -> Result<R, Box<dyn Error>> {
//         println!("ConvertBigQueryValueToValue<{}> -> in: {:?}", stringify!(R), self);
//         let x = self.parse()?;
//         println!("ConvertBigQueryValueToValue<{}> -> out: {:?}", stringify!(R), x);
//         Ok(x)
//     }
// }
//
// impl<R: ConvertBigQueryValueToValue<R> + FromStr> ConvertBigQueryValueToOptionValue<R> for String
//     where R::Err: Error + 'static {
//     default fn to_opt_value(&self) -> Result<Option<R>, Box<dyn Error>> {
//         Ok(match self.to_value() {
//             Ok(x) => Some(x),
//             Err(_) => None,
//         })
//     }
// }
