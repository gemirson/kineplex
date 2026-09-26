//! KinePlex SDK macros

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

/// Macro to derive Graph submission helper
#[proc_macro_derive(GraphSubmit)]
pub fn graph_submit_derive(input: TokenStream) -> TokenStream {
    let _ = parse_macro_input!(input as syn::DeriveInput);
    
    quote! {
        impl GraphSubmit {
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
    }.into()
}

/// Macro for creating a graph submission request builder
#[proc_macro]
pub fn submit_graph(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as syn::AttributeArgs);
    
    let tenant_id = args.first()
        .and_then(|a| {
            if let syn::NestedMeta::Meta(syn::Meta::Path(p)) = a {
                p.get_ident().map(|i| i.to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "default".to_string());
    
    quote! {
        GraphSubmitRequest {
            tenant_id: #tenant_id.to_string(),
            config: GraphConfigDto::default(),
            idempotency_key: None,
        }
    }.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_macro_output() {
        let result = submit_graph(quote! { "tenant-1" });
        assert!(!result.to_string().is_empty());
    }
}