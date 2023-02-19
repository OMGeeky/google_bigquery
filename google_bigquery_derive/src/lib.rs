#![allow(unused)]
extern crate proc_macro;

use std::any::Any;

use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

struct Field {
    field_ident: quote::__private::Ident,
    db_name: std::string::String,
    local_name: std::string::String,
    ty: syn::Type,
    required: bool,
}

#[proc_macro_derive(BigDataTable,
attributes(primary_key, client, db_name, db_ignore, required))]
pub fn big_data_table(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let tokens = implement_derive(&ast);
    tokens.into()
}

#[proc_macro_derive(HasBigQueryClient, attributes(client))]
pub fn has_big_query_client(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let tokens = implement_derive_has_big_query_client(&ast);
    tokens.into()
}

fn implement_derive_has_big_query_client(ast: &DeriveInput) -> TokenStream {
    let table_ident = &ast.ident;
    let client = get_client_field(&ast);
    let implementation_has_bigquery_client = implement_has_bigquery_client_trait(table_ident, &client.field_ident);
    quote! {
        #implementation_has_bigquery_client;
    }
}

fn implement_derive(ast: &DeriveInput) -> TokenStream {
    let table_ident = &ast.ident;
    let pk = get_pk_field(&ast);
    let implementation_big_data_table_base = implement_big_data_table_base_trait(table_ident, &pk, ast);

    let tokens = quote! {
        #implementation_big_data_table_base;
    };
    tokens
}

fn implement_big_data_table_base_trait(table_ident: &Ident, primary_key: &Field, ast: &DeriveInput) -> TokenStream {
    let pk_ty = &primary_key.ty;
    let client_field = get_client_field(&ast);

    let mut db_fields = get_fields(&ast.data);


    let get_pk_name = get_get_pk_name(primary_key);
    let get_pk_value = get_get_pk_value(primary_key);
    let get_field_name = get_get_field_name(ast, &db_fields);

    db_fields.retain(|f|f.local_name != client_field.local_name);

    let write_from_table_row = get_write_from_table_row(&db_fields);
    let get_query_fields = get_get_query_fields(ast);
    let get_table_name = get_get_table_name(&table_ident);
    let create_with_pk = get_create_with_pk(ast);
    let create_from_table_row = get_create_from_table_row(ast);
    let get_query_fields_update_str = get_get_query_fields_update_str(ast);
    let get_all_query_parameters = get_get_all_query_parameters(ast);
    quote! {
        impl<'a> BigDataTableBase<'a, #table_ident<'a>, #pk_ty> for #table_ident<'a>  {
            #get_pk_name
            #get_field_name
            #get_query_fields
            #get_table_name
            #create_with_pk
            #create_from_table_row
            #write_from_table_row
            #get_pk_value
            #get_query_fields_update_str
            #get_all_query_parameters
        }
    }
}

//region BigDataTableBase functions

fn get_get_pk_name(primary_key_field: &Field) -> TokenStream {
    let pk_name = &primary_key_field.db_name;
    quote! {
        fn get_pk_name() -> String {
            Self::get_field_name(stringify!(#pk_name)).unwrap()
        }
    }
}

fn get_get_pk_value(pk_field: &Field) -> TokenStream {
    let pk_ident = &pk_field.field_ident;
    quote! {
        fn get_pk_value(&self) -> i64 {
            self.#pk_ident
        }
    }
}

fn get_get_query_fields_update_str(ast: &DeriveInput) -> TokenStream {
    quote! {
        fn get_query_fields_update_str(&self) -> String {
            todo!();//TODO get_query_fields_update_str
        }
    }
}

fn get_get_all_query_parameters(ast: &DeriveInput) -> TokenStream {
    quote! {
        fn get_all_query_parameters(&self) -> Vec<QueryParameter> {
            todo!();//TODO get_all_query_parameters
        }
    }
}

fn get_write_from_table_row(db_fields: &Vec<Field>) -> TokenStream {
    fn get_write_from_table_row_single_field(field: &Field) -> TokenStream {
        let field_ident = &field.field_ident;
        let field_name = &field.db_name;
        if field.required {
            /*
        let pk_index = *index_to_name_mapping.get(&Self::get_pk_name()).unwrap();
        let pk = row
            .f.as_ref()
            .unwrap()[pk_index]
            .v.as_ref()
            .unwrap()
            .parse::<TPK>()
            .unwrap();
             */
            quote! {
                let index = *index_to_name_mapping.get(Self::get_field_name(stringify!(#field_name))?.as_str()).unwrap();
                self.#field_ident = row.f.as_ref()
                    .unwrap()[index]
                    .v.as_ref()
                    .unwrap()
                    .parse()
                    .unwrap();
            }
        } else {
            /*
        let info1 = *index_to_name_mapping.get(Self::get_field_name(stringify!(info1))?.as_str()).unwrap();
        self.info1 = match cell[info1].v.as_ref() {
            Some(v) => Some(v.parse()?),
            None => None
        };
             */
            quote! {
                let index = *index_to_name_mapping.get(Self::get_field_name(stringify!(#field_name))?.as_str()).unwrap();
                self.#field_ident = match row.f.as_ref().unwrap()[index].v.as_ref() {
                    Some(v) => Some(v.parse()?),
                    None => None
                };
            }
        }
    }

    let tokens: Vec<TokenStream> = db_fields.iter().map(|field| get_write_from_table_row_single_field(field)).collect();
    quote! {
        fn write_from_table_row(&mut self, row: &TableRow, index_to_name_mapping: &HashMap<String, usize>) -> Result<(), Box<dyn Error>> {
            #(#tokens)*
            Ok(())
        }
    }
}

fn get_create_from_table_row(ast: &DeriveInput) -> TokenStream {
    quote! {

    fn create_from_table_row(client: &'a BigqueryClient,
                             row: &google_bigquery2::api::TableRow,
                             index_to_name_mapping: &HashMap<String, usize>)
                             -> Result<Self, Box<dyn Error>>
        where
            Self: Sized{
            todo!();//TODO create_from_table_row
        }
    }
}

fn get_create_with_pk(ast: &DeriveInput) -> TokenStream {
    quote! {
        fn create_with_pk(client: &'a BigqueryClient, pk: i64) -> Self {
            todo!();//TODO create_with_pk
        }
    }
}

fn get_get_table_name(table_ident: &Ident) -> TokenStream {
    quote! {
        fn get_table_name() -> String {
            stringify!(#table_ident).to_string()
        }
    }
}

fn get_get_query_fields(ast: &DeriveInput) -> TokenStream {
    quote! {
        fn get_query_fields() -> HashMap<String, String> {
            todo!();//TODO get_query_fields
        }
    }
}

fn get_get_field_name(ast: &DeriveInput, db_fields: &Vec<Field>) -> TokenStream {
    let mut mapping: Vec<(&Ident, String)> = Vec::new();
    for db_field in db_fields {
        let field_name_local = &db_field.field_ident;
        let mut field_name_remote = &db_field.db_name;
        mapping.push((field_name_local, field_name_remote.to_string()));
    }

    let mapping_tok: Vec<TokenStream> = mapping.iter().map(|(field_name_local, field_name_remote)| {
        quote! {
            #field_name_local => Ok(#field_name_remote.to_string()),
        }
    }).collect();


    quote! {
        fn get_field_name(field_name: &str) -> Result<String, Box<dyn Error>> {
            match field_name {
                //ex.: "row_id" => Ok("Id".to_string()),
                #(#mapping_tok)*
                _ => Err("Field not found".into()),
            }
        }
    }
}

//endregion

fn implement_has_bigquery_client_trait(table_ident: &Ident, client_ident: &Ident) -> TokenStream {
    let implementation_has_bigquery_client = quote! {
        impl<'a> HasBigQueryClient<'a> for #table_ident<'a> {
            fn get_client(&self) -> &'a BigqueryClient {
                self.#client_ident
            }
        }
    };
    implementation_has_bigquery_client
}

//region Helper functions

fn get_helper_fields(ast: &syn::DeriveInput) -> (Field, Field) {
    let pk = get_pk_field(&ast);
    let client = get_client_field(&ast);
    (pk, client)
}

fn get_pk_field(ast: &&DeriveInput) -> Field {
    let mut pk_fields = get_attributed_fields(&ast.data, "primary_key");
    if pk_fields.len() != 1 {
        panic!("Exactly one primary key field must be specified");
    }
    let pk = pk_fields.remove(0);
    pk
}

fn get_client_field(ast: &&DeriveInput) -> Field {
//region client
    let mut client_fields = get_attributed_fields(&ast.data, "client");
    if client_fields.len() != 1 {
        panic!("Exactly one client field must be specified");
    }
    let client = client_fields.remove(0);
    //endregion
    client
}


fn get_fields(data: &syn::Data) -> Vec<Field> {
    let mut res = vec![];

    match data {
        syn::Data::Struct(ref data_struct) => match data_struct.fields {
            syn::Fields::Named(ref fields_named) => {
                'field_loop: for field in fields_named.named.iter() {
                    if let Some(ident) = &field.ident {
                        let mut name = None;
                        let mut required = false;
                        let attrs = &field.attrs;
                        for attribute in attrs {
                            if attribute.path.is_ident("db_ignore") {
                                continue 'field_loop; //skip this field completely
                            }
                            if attribute.path.is_ident("db_name") {
                                let args: syn::LitStr =
                                    attribute.parse_args().expect("Failed to parse target name");
                                let args = args.value();
                                name = Some(args);
                            }
                            if attribute.path.is_ident("required") {
                                required = true;
                            }
                        }

                        let local_name = ident.to_string();
                        let name = match name {
                            None => local_name.clone(),
                            Some(n) => n,
                        };
                        // let name: String = "".to_string();
                        res.push(Field {
                            field_ident: ident.clone(),
                            local_name,
                            db_name: name,
                            ty: field.ty.clone(),
                            required,
                        });
                    }
                }
            }
            _ => (),
        },
        _ => panic!("Must be a struct!"),
    };

    return res;
}


fn get_attributed_fields(data: &syn::Data, attribute_name: &str) -> Vec<Field> {
    let mut res = vec![];
    match data {
        // Only process structs
        syn::Data::Struct(ref data_struct) => {
            // Check the kind of fields the struct contains
            match data_struct.fields {
                // Structs with named fields
                syn::Fields::Named(ref fields_named) => {
                    // Iterate over the fields
                    'field_loop: for field in fields_named.named.iter() {
                        if let Some(ident) = &field.ident {
                            // Get attributes #[..] on each field
                            for attr in field.attrs.iter() {
                                // Parse the attribute
                                if attr.path.is_ident(attribute_name) {
                                    let mut name = None;
                                    let mut required = false;
                                    let attrs = &field.attrs;
                                    for attribute in attrs {
                                        if attribute.path.is_ident("db_ignore") {
                                            continue 'field_loop; //skip this field completely
                                        } else if attribute.path.is_ident("db_name") {
                                            let args: syn::LitStr = attribute
                                                .parse_args()
                                                .expect("Failed to parse target name");
                                            let args = args.value();
                                            name = Some(args);
                                        } else if attribute.path.is_ident("required") {
                                            required = true;
                                        } else if attribute.path.is_ident("primary_key") {
                                            required = true;
                                        }
                                    }

                                    let local_name = ident.to_string();
                                    let name = match name {
                                        None => local_name.clone(),
                                        Some(n) => n,
                                    };

                                    let item = field.clone();
                                    res.push(Field {
                                        field_ident: item.ident.unwrap(),
                                        local_name,
                                        ty: item.ty,
                                        db_name: name,
                                        required,
                                    });
                                }
                            }
                        }
                    }
                }

                // Struct with unnamed fields
                _ => (),
            }
        }

        // Panic when we don't have a struct
        _ => panic!("Must be a struct"),
    }

    // let res = res.iter();//.map(|(ident, ty)| (ident)).collect();
    // .fold(quote!(), |es, (name, ty)| (name, ty));
    return res;
}

//endregion

/*
/// Example of [function-like procedural macro][1].
///
/// [1]: https://doc.rust-lang.org/reference/procedural-macros.html#function-like-procedural-macros
#[proc_macro]
pub fn my_macro(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let tokens = quote! {
        #input

        struct Hello;
    };

    tokens.into()
}
*/

/*
/// Example of user-defined [procedural macro attribute][1].
///
/// [1]: https://doc.rust-lang.org/reference/procedural-macros.html#attribute-macros
#[proc_macro_attribute]
pub fn my_attribute(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let tokens = quote! {
        #input

        struct Hello;
    };

    tokens.into()
}
*/