#![allow(unused)]
extern crate proc_macro;

use proc_macro2::Ident;
use syn::{DeriveInput, Type};

struct Field {
    // field_ident: quote::__private::Ident,
    field_ident: proc_macro2::Ident,
    db_name: std::string::String,
    local_name: std::string::String,
    ty: syn::Type,
    required: bool,
}

struct Attribute {
    name: std::string::String,
    value: std::string::String,
}

// // pub trait MyTrait<T> where T: Clone {
// //     fn my_method(&self) -> T;
// // }
//
// #[proc_macro_derive(MyDerive, attributes(pk))]
// pub fn my_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
//     let ast: DeriveInput = syn::parse(input).unwrap();
//     let pk_field = get_pk_field_my_derive(&ast);
//
//     let pk_ident: &Ident = &pk_field.field_ident;
//     let pk_type: Type = pk_field.ty;
//     let struct_ident: &Ident = &ast.ident;
//     let tokens = quote::quote!{
//         impl<#pk_type> MyTrait<#pk_type> for #struct_ident {
//             fn my_method(&self) -> #pk_type {
//                 self.#pk_ident.clone()
//             }
//         }
//     };
//     tokens.into()
// }

fn get_pk_field_my_derive(ast: &syn::DeriveInput) -> Field {
    let mut pk_fields = get_attributed_fields(&ast.data, "pk");
    if pk_fields.len() != 1 {
        panic!("Exactly one pk field must be specified");
    }
    let pk = pk_fields.remove(0);
    pk
}

//region HasBigQueryClient derive

#[proc_macro_derive(HasBigQueryClient, attributes(client))]
pub fn has_big_query_client(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let tokens = implement_derive_has_big_query_client(&ast);
    tokens.into()
}

fn implement_derive_has_big_query_client(ast: &syn::DeriveInput) -> proc_macro2::TokenStream {
    fn implement_has_bigquery_client_trait(table_ident: &proc_macro2::Ident, client_ident: &proc_macro2::Ident) -> proc_macro2::TokenStream {
        let implementation_has_bigquery_client = quote::quote! {

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
    quote::quote! {
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

fn implement_derive(ast: &syn::DeriveInput) -> proc_macro2::TokenStream {
    let table_ident = &ast.ident;
    let pk = get_pk_field(&ast);
    let implementation_big_data_table_base = implement_big_data_table_base_trait(table_ident, &pk, ast);

    let tokens = quote::quote! {
        #implementation_big_data_table_base;
    };
    tokens
}

fn implement_big_data_table_base_trait(table_ident: &proc_macro2::Ident,
                                       primary_key: &Field,
                                       ast: &syn::DeriveInput) -> proc_macro2::TokenStream {
    let table_name = get_table_name(&ast);
    let pk_ty = &primary_key.ty;
    let client_field = get_client_field(&ast);

    let mut db_fields = get_fields(&ast.data);
    db_fields.retain(|f| f.local_name != client_field.local_name);

    let get_pk_name = get_get_pk_name(primary_key);
    let get_pk_value = get_get_pk_value(primary_key);

    let get_field_name = get_get_field_name(ast, &db_fields);
    let get_query_fields = get_get_query_fields(&db_fields);
    let write_from_table_row = get_write_from_table_row(&db_fields);
    let get_table_name = get_get_table_name(&table_name);
    let create_with_pk = get_create_with_pk(&primary_key, &client_field);
    let create_from_table_row = get_create_from_table_row(&pk_ty);
    let get_all_query_parameters = get_get_all_query_parameters(&db_fields);
    quote::quote! {
        impl<'a> BigDataTableHasPk<#pk_ty> for #table_ident<'a> {
            #get_pk_name
            #get_pk_value
        }
        impl<'a> BigDataTableBase<'a, #table_ident<'a>, #pk_ty> for #table_ident<'a>  {
            // #get_pk_name
            // #get_pk_value
            #get_field_name
            #get_query_fields
            #get_table_name
            #create_with_pk
            #create_from_table_row
            #write_from_table_row
            #get_all_query_parameters
        }
    }
}

fn get_table_name(ast: &DeriveInput) -> String {
    for attr in get_struct_attributes(ast) {
        if attr.name.eq("db_name") {
            let tokens = &attr.value;
            return tokens.to_string();
        }
    }
    ast.ident.to_string()
}

//region BigDataTableBase functions

fn get_get_pk_name(primary_key_field: &Field) -> proc_macro2::TokenStream {
    let pk_name = &primary_key_field.local_name;
    quote::quote! {
        fn get_pk_name() -> String {
            let name = #pk_name;
            Self::get_field_name(name).unwrap()
        }
    }
}

fn get_get_pk_value(pk_field: &Field) -> proc_macro2::TokenStream {
    let pk_ident = &pk_field.field_ident;
    let pk_ty = &pk_field.ty;
    quote::quote! {
        fn get_pk_value(&self) -> #pk_ty {
            self.#pk_ident.clone()
        }
    }
}

fn get_get_all_query_parameters(db_fields: &Vec<Field>) -> proc_macro2::TokenStream {
    fn get_all_query_parameters(field: &Field) -> proc_macro2::TokenStream {
        let field_ident = &field.field_ident;
        match field.required {
            true => quote::quote! {
                parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(#field_ident)).unwrap(), &Some(self.#field_ident.clone())));
            },
            false => quote::quote! {
                parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(#field_ident)).unwrap(), &self.#field_ident));
            }
        }
    }
    let tokens: Vec<proc_macro2::TokenStream> = db_fields.iter().map(|field| get_all_query_parameters(field)).collect();
    quote::quote! {
        fn get_all_query_parameters(&self) -> Vec<google_bigquery2::api::QueryParameter> {
            let mut parameters = Vec::new();

            // parameters.push(Self::get_query_param(&Self::get_field_name(stringify!(info1)).unwrap(), &self.info1));

            #(#tokens)*

            parameters
        }
    }
}

fn get_write_from_table_row(db_fields: &Vec<Field>) -> proc_macro2::TokenStream {
    fn get_write_from_table_row_single_field(field: &Field) -> proc_macro2::TokenStream {
        let field_ident = &field.field_ident;
        let field_name = &field.db_name;
        let field_ty = &field.ty;
        // if(field.ty == chrono::D)
        // let parse_fn_tok = quote::quote!{
        //
        //     fn parse_value<T: BigDataValueType<#field_ty>>(v: &String) -> T {
        //         v.to_value()
        //         // v.parse()
        //             .expect(format!("could not parse field: {} with value {}", stringify!(#field_name), v)
        //                 .as_str())
        //     };
        //
        //     // parse()
        //     // .expect(format!("could not parse field: {} with value {}", stringify!(#field_name), v)
        //     //     .as_str())
        // };
        if field.required {
            quote::quote! {
                // println!("get_write_from_table_row_single_field: field_name: (1) {}", #field_name);
                let index = *index_to_name_mapping.get(#field_name)
                    .expect(format!("could not find index for field in mapping!: (1) {}", #field_name).as_str());

                {
                    self.#field_ident = match row.f.as_ref()
                        .expect("row.f is None (1)")
                        [index]
                        .v.as_ref() {
                        // Some(v)=> parse_value(v),
                        // Some(v)=> v.to_value()
                        // Some(v)=>todo!(),
                        Some(v)=> #field_ty::from_bigquery_value(v)
                            .expect(format!("could not parse required field: {} with value {}", stringify!(#field_name), v)
                                .as_str()),
                        // Some(v)=> v.to_value(),
                        None => panic!("field is required but is None: {}", #field_name)
                    };
                }
            }
        } else {
            let field_option_ty = extract_type_from_option(&field_ty)
                .expect(&format!("could not extract type from option: {}->{:?}", field_name, field_ty));

            quote::quote! {
                // println!("get_write_from_table_row_single_field: field_name: (2) {} at index: {}", #field_name, index);
                let index = *index_to_name_mapping.get(#field_name)
                    .expect(format!("could not find index for field in mapping!: (2) {}", #field_name).as_str());

                {
                    self.#field_ident = match row.f.as_ref()
                        .expect("row.f is None (1)")
                        [index].v.as_ref() {
                        // Some(v) => Some(v.to_value()),
                        // Some(v) => v.to_opt_value()
                        // Some(v)=> todo!()
                        Some(v) => Option::<#field_option_ty>::from_bigquery_value(v)
                            .expect(format!("could not parse field: {} with value {}", stringify!(#field_name), v).as_str())
                        ,
                        // Some(v) => Some(parse_value(v)),
                        None => None
                    };
                }
            }
        }
    }

    let tokens: Vec<proc_macro2::TokenStream> = db_fields.iter().map(|field| get_write_from_table_row_single_field(field)).collect();
    quote::quote! {
        fn write_from_table_row(&mut self, row: &google_bigquery2::api::TableRow, index_to_name_mapping: &std::collections::HashMap<String, usize>) -> Result<(), Box<dyn std::error::Error>> {
            #(#tokens)*
            Ok(())
        }
    }
}

fn get_create_from_table_row(pk_ty: &syn::Type) -> proc_macro2::TokenStream {
    quote::quote! {
        fn create_from_table_row(client: &'a BigqueryClient,
                                 row: &google_bigquery2::api::TableRow,
                                 index_to_name_mapping: &std::collections::HashMap<String, usize>)
                                 -> Result<Self, Box<dyn std::error::Error>>
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

fn get_create_with_pk(pk_field: &Field, client_field: &Field) -> proc_macro2::TokenStream {
    let pk_ident = &pk_field.field_ident;
    let pk_ty = &pk_field.ty;
    let client_ident = &client_field.field_ident;
    quote::quote! {
        fn create_with_pk(client: &'a BigqueryClient, pk: #pk_ty) -> Self {
            Self {
                #pk_ident: pk,
                #client_ident: Some(client),
                ..Default::default()
            }
        }
    }
}

fn get_get_table_name(table_name: &str) -> proc_macro2::TokenStream {
    quote::quote! {
        fn get_table_name() -> String {
            #table_name.to_string()
        }
    }
}

fn get_get_query_fields(db_fields: &Vec<Field>) -> proc_macro2::TokenStream {
    fn get_query_fields_single_field(field: &Field) -> proc_macro2::TokenStream {
        let field_ident = &field.field_ident;
        let field_name = &field.db_name;
        quote::quote! {
            fields.insert(stringify!(#field_ident).to_string(), Self::get_field_name(&stringify!(#field_ident).to_string()).unwrap());
        }
    }

    let tokens: Vec<proc_macro2::TokenStream> = db_fields.iter().map(|field| get_query_fields_single_field(field)).collect();

    quote::quote! {
        fn get_query_fields() -> std::collections::HashMap<String, String> {
            let mut fields = std::collections::HashMap::new();
            #(#tokens)*
            // println!("get_query_fields: fields: {:?}", fields);
            fields
        }
    }
}

fn get_get_field_name(ast: &syn::DeriveInput, db_fields: &Vec<Field>) -> proc_macro2::TokenStream {
    // let mut mapping: Vec<(&proc_macro2::Ident, String)> = Vec::new();
    // for db_field in db_fields {
    //     let field_name_local = &db_field.field_ident;
    //     let mut field_name_remote = &db_field.db_name;
    //     mapping.push((field_name_local, field_name_remote.to_string()));
    // }
    //
    // let mapping_tok: Vec<proc_macro2::TokenStream> = mapping.iter().map(|(field_name_local, field_name_remote)| {
    //     quote::quote! {
    //         stringify!(#field_name_local) => Ok(#field_name_remote.to_string()),
    //     }
    // }).collect();
    fn get_field_name_single_field(field: &Field) -> proc_macro2::TokenStream {
        let field_name_local = &field.field_ident.to_string();
        let mut field_name_remote = &field.db_name;
        quote::quote! {
            #field_name_local => Ok(#field_name_remote.to_string()),
        }
    }
    let mapping_tok: Vec<proc_macro2::TokenStream> = db_fields.iter().map(get_field_name_single_field).collect();
    let possible_fields: String = db_fields.iter().map(|field| field.field_ident.to_string()).collect::<Vec<String>>().join(", ");
    quote::quote! {
        fn get_field_name(field_name: &str) -> Result<String, Box<dyn std::error::Error>> {
            // println!("get_field_name: field_name: {:?}", field_name);
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

fn get_pk_field(ast: &syn::DeriveInput) -> Field {
    let mut pk_fields = get_attributed_fields(&ast.data, "primary_key");
    if pk_fields.len() != 1 {
        panic!("Exactly one primary key field must be specified");
    }
    let pk = pk_fields.remove(0);
    pk
}

fn get_client_field(ast: &syn::DeriveInput) -> Field {
//region client
    let mut client_fields = get_attributed_fields(&ast.data, "client");
    if client_fields.len() != 1 {
        panic!("Exactly one client field must be specified");
    }
    let client = client_fields.remove(0);
    //endregion
    client
}

fn get_struct_attributes(ast: &syn::DeriveInput) -> Vec<Attribute> {
    let attrs = &ast.attrs;
    let mut res = vec![];
    for attr in attrs {
        if attr.path.is_ident("db_name") {
            let args: syn::LitStr = attr
                .parse_args()
                .expect("Failed to parse target name");
            let args = args.value();
            res.push(Attribute {
                name: "db_name".to_string(),
                value: args,
            });
        }
    }
    res
}

fn extract_type_from_option(ty: &syn::Type) -> Option<&syn::Type> {
    use syn::{GenericArgument, Path, PathArguments, PathSegment};

    fn extract_type_path(ty: &syn::Type) -> Option<&Path> {
        match *ty {
            syn::Type::Path(ref typepath) if typepath.qself.is_none() => Some(&typepath.path),
            _ => None,
        }
    }

    // TODO store (with lazy static) the vec of string
    // TODO maybe optimization, reverse the order of segments
    fn extract_option_segment(path: &Path) -> Option<&PathSegment> {
        let idents_of_path = path
            .segments
            .iter()
            .into_iter()
            .fold(String::new(), |mut acc, v| {
                acc.push_str(&v.ident.to_string());
                acc.push('|');
                acc
            });
        vec!["Option|", "std|option|Option|", "core|option|Option|"]
            .into_iter()
            .find(|s| &idents_of_path == *s)
            .and_then(|_| path.segments.last())
    }

    extract_type_path(ty)
        .and_then(|path| extract_option_segment(path))
        .and_then(|path_seg| {
            let type_params = &path_seg.arguments;
            // It should have only on angle-bracketed param ("<String>"):
            match *type_params {
                PathArguments::AngleBracketed(ref params) => params.args.first(),
                _ => None,
            }
        })
        .and_then(|generic_arg| match *generic_arg {
            GenericArgument::Type(ref ty) => Some(ty),
            _ => None,
        })
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
    // .fold(quote::quote!(), |es, (name, ty)| (name, ty));
    return res;
}

//endregion
