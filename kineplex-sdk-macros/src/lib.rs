//! KinePlex SDK macros

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

/// Macro to derive Graph submission helper.
#[proc_macro_derive(GraphSubmit)]
pub fn graph_submit_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident;

    quote! {
        impl #name {
            pub fn new(tenant_id: impl Into<String>) -> Self {
                Self {
                    tenant_id: tenant_id.into(),
                    config: GraphConfigDto::default(),
                    idempotency_key: None,
                }
            }

            pub fn with_config(mut self, config: GraphConfigDto) -> Self {
                self.config = config;
                self
            }

            pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
                self.idempotency_key = Some(key.into());
                self
            }
        }
    }
    .into()
}

fn expand_submit_graph(tenant_id: &str) -> proc_macro2::TokenStream {
    let tenant_id = syn::LitStr::new(tenant_id, proc_macro2::Span::call_site());
    quote! {
        GraphSubmitRequest {
            tenant_id: #tenant_id.to_string(),
            config: GraphConfigDto::default(),
            idempotency_key: None,
        }
    }
}

/// Macro for creating a graph submission request builder.
///
/// Usage: `submit_graph!("tenant-1")`.
#[proc_macro]
pub fn submit_graph(input: TokenStream) -> TokenStream {
    let tenant_id = parse_macro_input!(input as syn::LitStr);
    expand_submit_graph(&tenant_id.value()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_submit_graph_expansion() {
        let result = expand_submit_graph("tenant-1");
        let output = result.to_string();
        assert!(output.contains("GraphSubmitRequest"));
        assert!(output.contains("tenant-1"));
    }
}