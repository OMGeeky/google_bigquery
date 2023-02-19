#![allow(unused)]
extern crate proc_macro;

use std::any::Any;

use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{DeriveInput, parse_macro_input, Type};

struct Field {
    field_ident: quote::__private::Ident,
    db_name: std::string::String,
    local_name: std::string::String,
    ty: syn::Type,
    required: bool,
}

//region HasBigQueryClient derive

#[proc_macro_derive(HasBigQueryClient, attributes(client))]
pub fn has_big_query_client(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let tokens = implement_derive_has_big_query_client(&ast);
    tokens.into()
}

fn implement_derive_has_big_query_client(ast: &DeriveInput) -> TokenStream {
    fn implement_has_bigquery_client_trait(table_ident: &Ident, client_ident: &Ident) -> TokenStream {
        let implementation_has_bigquery_client = quote! {
            impl<'a> HasBigQueryClient<'a> for #table_ident<'a> {
                fn get_client(&self) -> &'a BigqueryClient {
                    self.#client_ident.unwrap()
                }
            }
        };
        implementation_has_bigquery_client
    }

    let table_ident = &ast.ident;
    let client = get_client_field(&ast);
    let implementation_has_bigquery_client = implement_has_bigquery_client_trait(table_ident, &client.field_ident);
    quote! {
        #implementation_has_bigquery_client;
    }
}

//endregion HasBigQueryClient derive

//region BigDataTable derive
#[proc_macro_derive(BigDataTable,
attributes(primary_key, client, db_name, db_ignore, required))]
pub fn big_data_table(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let tokens = implement_derive(&ast);
    tokens.into()
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

    db_fields.retain(|f| f.local_name != client_field.local_name);

    let get_field_name = get_get_field_name(ast, &db_fields);
    let get_query_fields = get_get_query_fields(&db_fields);
    let write_from_table_row = get_write_from_table_row(&db_fields);
    let get_table_name = get_get_table_name(&table_ident);
    let create_with_pk = get_create_with_pk(&primary_key, &client_field);
    let create_from_table_row = get_create_from_table_row(&pk_ty);
    let get_all_query_parameters = get_get_all_query_parameters(&db_fields);
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
            #get_all_query_parameters
        }
    }
}

//region BigDataTableBase functions

fn get_get_pk_name(primary_key_field: &Field) -> TokenStream {
    let pk_name = &primary_key_field.local_name;
    quote! {
        fn get_pk_name() -> String {
            let name = #pk_name;
            Self::get_field_name(name).unwrap()
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

fn get_get_all_query_parameters(db_fields: &Vec<Field>) -> TokenStream {
    fn get_all_query_parameters(field: &Field) -> TokenStream {
        let field_ident = &field.field_ident;
        match field.required {
            true => quote! {
                parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(#field_ident)).unwrap(), &Some(self.#field_ident)));
            },
            false => quote! {
                parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(#field_ident)).unwrap(), &self.#field_ident));
            }
        }
    }
    let tokens: Vec<TokenStream> = db_fields.iter().map(|field| get_all_query_parameters(field)).collect();
    quote! {
        fn get_all_query_parameters(&self) -> Vec<QueryParameter> {
            let mut parameters = Vec::new();

            // parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(info1)).unwrap(), &self.info1));

            #(#tokens)*

            parameters
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
                println!("get_write_from_table_row_single_field: field_name: (1) {}", #field_name);
                let index = *index_to_name_mapping.get(#field_name)
                .expect(format!("could not find index for field in mapping!: (1) {}", #field_name).as_str());
                self.#field_ident = row.f.as_ref()
                    .expect("row.f is None (1)")
                    [index]
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
                let index = *index_to_name_mapping.get(#field_name)
                .expect(format!("could not find index for field in mapping!: (2) {}", #field_name).as_str());
                println!("get_write_from_table_row_single_field: field_name: (2) {} at index: {}", #field_name, index);
                self.#field_ident = match row.f.as_ref()
                    .expect("row.f is None (1)")
                    [index].v.as_ref() {
                    Some(v) => Some(v.parse().expect(format!("could not parse field: {} with value {}",stringify!(#field_name),v).as_str())),
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

fn get_create_from_table_row(pk_ty: &Type) -> TokenStream {
    quote! {
        fn create_from_table_row(client: &'a BigqueryClient,
                                 row: &google_bigquery2::api::TableRow,
                                 index_to_name_mapping: &HashMap<String, usize>)
                                 -> Result<Self, Box<dyn Error>>
            where
                Self: Sized {
            //TODO
            // create_from_table_row maybe push this to the convenience part.
            // NOTE: its a bit weird with the unwrap and the pk type if not implemented here, but I believe :)
            let pk_index = *index_to_name_mapping.get(&Self::get_pk_name()).unwrap();
            let pk = row
                .f.as_ref()
                .unwrap()[pk_index]
                .v.as_ref()
                .unwrap()
                .parse::<#pk_ty>()
                .unwrap();
            let mut res = Self::create_with_pk(client, pk);
            res.write_from_table_row(row, index_to_name_mapping)?;
            Ok(res)
        }
    }
}

fn get_create_with_pk(pk_field: &Field, client_field: &Field) -> TokenStream {
    let pk_ident = &pk_field.field_ident;
    let client_ident = &client_field.field_ident;
    quote! {
        fn create_with_pk(client: &'a BigqueryClient, pk: i64) -> Self {
            Self {
                #pk_ident: pk,
                #client_ident: Some(client),
                ..Default::default()
            }
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

fn get_get_query_fields(db_fields: &Vec<Field>) -> TokenStream {
    fn get_query_fields_single_field(field: &Field) -> TokenStream {
        let field_ident = &field.field_ident;
        let field_name = &field.db_name;
        quote! {
            fields.insert(stringify!(#field_ident).to_string(), Self::get_field_name(&stringify!(#field_ident).to_string()).unwrap());
        }
    }

    let tokens: Vec<TokenStream> = db_fields.iter().map(|field| get_query_fields_single_field(field)).collect();

    quote! {
        fn get_query_fields() -> HashMap<String, String> {
            let mut fields = HashMap::new();
            #(#tokens)*
            println!("get_query_fields: fields: {:?}", fields);
            fields
        }
    }
}

fn get_get_field_name(ast: &DeriveInput, db_fields: &Vec<Field>) -> TokenStream {
    // let mut mapping: Vec<(&Ident, String)> = Vec::new();
    // for db_field in db_fields {
    //     let field_name_local = &db_field.field_ident;
    //     let mut field_name_remote = &db_field.db_name;
    //     mapping.push((field_name_local, field_name_remote.to_string()));
    // }
    //
    // let mapping_tok: Vec<TokenStream> = mapping.iter().map(|(field_name_local, field_name_remote)| {
    //     quote! {
    //         stringify!(#field_name_local) => Ok(#field_name_remote.to_string()),
    //     }
    // }).collect();
    fn get_field_name_single_field(field: &Field) -> TokenStream {
        let field_name_local = &field.field_ident.to_string();
        let mut field_name_remote = &field.db_name;
        quote! {
            #field_name_local => Ok(#field_name_remote.to_string()),
        }
    }
    let mapping_tok: Vec<TokenStream> = db_fields.iter().map(get_field_name_single_field).collect();
    let possible_fields: String = db_fields.iter().map(|field| field.field_ident.to_string()).collect::<Vec<String>>().join(", ");
    quote! {
        fn get_field_name(field_name: &str) -> Result<String, Box<dyn Error>> {
            println!("get_field_name: field_name: {:?}", field_name);
            match field_name {
                //ex.: "row_id" => Ok("Id".to_string()),
                #(#mapping_tok)*
                _ => Err(format!("Field not found {}\nPlease choose one of the following: {}", field_name, #possible_fields).into()),
            }
        }
    }
}

//endregion

//endregion BigDataTable derive

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
