use google_bigquery_derive::BigDataTable;
use google_bigquery_derive::HasBigQueryClient;

use crate::data::{BigDataTable, BigDataTableBase, BigDataTableBaseConvenience};
use crate::client::HasBigQueryClient;
use crate::utils::BigDataValueType;


use crate::utils::ConvertValueToBigqueryParamValue;

use super::*;

// #[test]
// fn it_works() {
//     let result = add(2, 2);
//     assert_eq!(result, 4);
// }

#[tokio::test]
async fn save() {
    let client = get_test_client().await;

    let mut q = Infos::load_by_field(&client, stringify!(info1), Some("a".to_string()), 10).await.unwrap();
    assert_eq!(q.len(), 1);

    let mut i1 = &mut q[0];
    assert_eq!(i1.row_id, 3);
    assert_eq!(i1.info3, Some("c".to_string()));
    assert_eq!(i1.info2, None);

    i1.info2 = Some("b".to_string());
    i1.save_to_bigquery().await.unwrap();

    assert_eq!(i1.info2, Some("b".to_string()));
    i1.info2 = Some("c".to_string());
    assert_eq!(i1.info2, Some("c".to_string()));

    i1.load_from_bigquery().await.unwrap();
    assert_eq!(i1.info2, Some("b".to_string()));

    i1.info2 = None;
    i1.save_to_bigquery().await.unwrap();
    i1.load_from_bigquery().await.unwrap();

    assert_eq!(i1.row_id, 3);
    assert_eq!(i1.info3, Some("c".to_string()));
    assert_eq!(i1.info2, None);
}

#[tokio::test]
async fn load_by_field() {
    let client = get_test_client().await;

    let q = Infos::load_by_field(&client, stringify!(info1), Some("a".to_string()), 10).await.unwrap();
    assert_eq!(q.len(), 1);

    let i1 = &q[0];
    assert_eq!(i1.row_id, 3);
    assert_eq!(i1.info3, Some("c".to_string()));

    let mut q = Infos::load_by_field(&client, stringify!(yes), Some(true), 10).await.unwrap();
    q.sort_by(|a, b| a.row_id.cmp(&b.row_id));
    assert_eq!(q.len(), 3);

    let i2 = &q[0];
    assert_eq!(i2.row_id, 1);
    assert_eq!(i2.info3, Some("x3".to_string()));

    let i3 = &q[1];
    assert_eq!(i3.row_id, 19);
    assert_eq!(i3.info3, Some("cc".to_string()));

    let i4 = &q[2];
    assert_eq!(i4.row_id, 123123);
    assert_eq!(i4.info3, Some("cc".to_string()));


    let q = Infos::load_by_field(&client, stringify!(info1), Some("aosdinsofnpsngusn".to_string()), 10).await.unwrap();
    assert_eq!(q.len(), 0);
}

#[tokio::test]
async fn load_by_field_none_param() {
    let client = get_test_client().await;
    let q = Infos::load_by_field::<bool>(&client, stringify!(yes), None, 10).await.unwrap();
    assert_eq!(q.len(), 1);
}

#[tokio::test]
async fn from_pk() {
    let client = get_test_client().await;
    let i1 = Infos::load_from_pk(&client, "3".to_string()).await.unwrap().unwrap();
    assert_eq!(i1.row_id, 3);
    assert_eq!(i1.info1, Some("a".to_string()));
    assert_eq!(i1.info3, Some("c".to_string()));
    assert_eq!(i1.int_info4, None);
    assert_eq!(i1.info2, None);
    assert_eq!(i1.yes, None);
}

async fn get_test_client() -> BigqueryClient {
    let client = BigqueryClient::new("testrustproject-372221", "test1", None).await.unwrap();
    client
}

#[derive(Debug)]
#[cfg_attr(man_impl_has_client = "false", derive(HasBigQueryClient))]
#[cfg_attr(not(man_impl = "true"), derive(BigDataTable))]
pub struct Infos<'a> {
    #[cfg_attr(not(man_impl = "true"), required)]
    #[cfg_attr(not(man_impl = "true"), db_name("Id"))]
    row_id: i64,
    #[cfg_attr(not(man_impl = "true"), required)]
    #[cfg_attr(not(man_impl = "true"), primary_key)]
    row_ids: String,
    #[cfg_attr(any(not(man_impl = "true"), man_impl_has_client = "false"), client)]
    /// This client should never be left as None, doing so will cause a panic when trying to use it
    client: Option<&'a BigqueryClient>,
    info1: Option<String>,
    #[cfg_attr(not(man_impl="true"), db_name("info"))]
    info2: Option<String>,
    info3: Option<String>,
    #[cfg_attr(not(man_impl="true"), db_name("info4i"))]
    int_info4: Option<i64>,
    yes: Option<bool>,
}


#[cfg(not(man_impl_has_client="false"))]
impl<'a> HasBigQueryClient<'a> for Infos<'a> {
    fn get_client(&self) -> &'a BigqueryClient {
        self.client.unwrap()
    }
}
//
// impl<'a> BigDataTableHasPk<String> for Infos<'a> {
//     fn get_pk_name() -> String {
//         "row_ids".to_string()
//     }
//
//     fn get_pk_value(&self) -> String {
//         self.row_ids.clone()
//     }
// }

impl<'a> Default for Infos<'a> {
    fn default() -> Self {
        Self {
            // client: &BigqueryClient::new("none", "none", None).await.unwrap(),
            client: None,
            row_id: -9999,
            row_ids: "-9999".to_string(),
            info1: Default::default(),
            info2: Default::default(),
            info3: Default::default(),
            int_info4: Default::default(),
            yes: Default::default(),
        }
    }
}

#[cfg(man_impl = "true")]
impl<'a> BigDataTableBase<'a, Infos<'a>, i64> for Infos<'a> {
    fn get_pk_name() -> String {
        Self::get_field_name(stringify!(row_id)).unwrap()
    }

    fn get_field_name(field_name: &str) -> Result<String, Box<dyn Error>> {
        match field_name {
            "row_id" => Ok("Id".to_string()),
            "info1" => Ok("info1".to_string()),
            "info2" => Ok("info".to_string()),
            "info3" => Ok("info3".to_string()),
            "int_info4" => Ok("info4i".to_string()),
            "yes" => Ok("yes".to_string()),
            _ => Err("Field not found".into()),
        }
    }

    fn get_query_fields() -> HashMap<String, String> {
        let mut fields = HashMap::new();
        fields.insert(stringify!(info1).to_string(), Self::get_field_name(&stringify!(info1).to_string()).unwrap());
        fields.insert(stringify!(info2).to_string(), Self::get_field_name(&stringify!(info2).to_string()).unwrap());
        fields.insert(stringify!(info3).to_string(), Self::get_field_name(&stringify!(info3).to_string()).unwrap());
        fields.insert(stringify!(int_info4).to_string(), Self::get_field_name(&stringify!(int_info4).to_string()).unwrap());
        fields.insert(stringify!(yes).to_string(), Self::get_field_name(&stringify!(yes).to_string()).unwrap());

        //TODO: decide if the primary key should be included in the query fields
        fields.insert(stringify!(row_id).to_string(), Self::get_field_name(&stringify!(row_id).to_string()).unwrap());

        println!("get_query_fields: fields: {:?}", fields);
        fields
    }

    fn get_table_name() -> String {
        stringify!(Infos).to_string()
    }

    fn create_with_pk(client: &'a BigqueryClient, pk: i64) -> Self {
        Self {
            row_id: pk,
            client: Some(client),
            ..Default::default()
        }
    }

    fn write_from_table_row(&mut self,
                            row: &google_bigquery2::api::TableRow,
                            index_to_name_mapping: &HashMap<String, usize>)
                            -> Result<(), Box<dyn Error>> {
        let cell = row.f.as_ref().unwrap();

        let info1 = *index_to_name_mapping.get(Self::get_field_name(stringify!(info1))?.as_str()).unwrap();
        self.info1 = match cell[info1].v.as_ref() {
            Some(v) => Some(v.parse()?),
            None => None
        };

        let info2 = *index_to_name_mapping.get(Self::get_field_name(stringify!(info2))?.as_str()).unwrap();
        self.info2 = match cell[info2].v.as_ref() {
            Some(v) => Some(v.parse()?),
            None => None
        };

        let info3 = *index_to_name_mapping.get(Self::get_field_name(stringify!(info3))?.as_str()).unwrap();
        self.info3 = match cell[info3].v.as_ref() {
            Some(v) => Some(v.parse()?),
            None => None
        };

        let int_info4 = *index_to_name_mapping.get(Self::get_field_name(stringify!(int_info4))?.as_str()).unwrap();
        self.int_info4 = match cell[int_info4].v.as_ref() {
            Some(v) => Some(v.parse()?),
            None => None
        };

        let yes = *index_to_name_mapping.get(Self::get_field_name(stringify!(yes))?.as_str()).unwrap();
        self.yes = match cell[yes].v.as_ref() {
            Some(v) => Some(v.parse()?),
            None => None
        };

        Ok(())
    }

    fn get_pk_value(&self) -> i64 {
        self.row_id
    }

    /*
    fn get_query_fields_update_str(&self) -> String {
        let x = Self::get_query_fields();
        let pk_name = Self::get_pk_name();
        let mut vec = x.values()
            .filter(|k| *k != &pk_name)
            .map(|k| format!("{} = @__{}", k, k))
            .collect::<Vec<String>>();
        vec.sort();
        let x = vec
            .join(", ");

        // let mut fields = String::new();
        // let info1 = Self::get_field_name(stringify!(info1)).unwrap();
        // fields.push_str(&format!("{} = @__{}, ", info1, info1));
        // let info2 = Self::get_field_name(stringify!(info2)).unwrap();
        // fields.push_str(&format!("{} = @__{}, ", info2, info2));
        // let info3 = Self::get_field_name(stringify!(info3)).unwrap();
        // fields.push_str(&format!("{} = @__{}, ", info3, info3));
        // let int_info4 = Self::get_field_name(stringify!(int_info4)).unwrap();
        // fields.push_str(&format!("{} = @__{}, ", int_info4, int_info4));
        // let yes = Self::get_field_name(stringify!(yes)).unwrap();
        // fields.push_str(&format!("{} = @__{}", yes, yes));
        // println!("fields: {}", fields);
        println!("x     : {}", x);
        // fields
        x
    }*/

    fn get_all_query_parameters(&self) -> Vec<QueryParameter> {
        let mut parameters = Vec::new();

        parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(info1)).unwrap(), &self.info1));
        parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(info2)).unwrap(), &self.info2));
        parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(info3)).unwrap(), &self.info3));
        parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(int_info4)).unwrap(), &self.int_info4));
        parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(yes)).unwrap(), &self.yes));
        //TODO: decide if the primary key should be included in this list
        parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(row_id)).unwrap(), &Some(self.row_id)));
        parameters
    }


    fn create_from_table_row(client: &'a BigqueryClient,
                             row: &google_bigquery2::api::TableRow,
                             index_to_name_mapping: &HashMap<String, usize>)
                             -> Result<Self, Box<dyn Error>>
        where
            Self: Sized
    {
        let pk_index = *index_to_name_mapping.get(&Self::get_pk_name()).unwrap();
        let pk = row
            .f.as_ref()
            .unwrap()[pk_index]
            .v.as_ref()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let mut res = Self::create_with_pk(client, pk);
        res.write_from_table_row(row, index_to_name_mapping)?;
        Ok(res)
    }
}

