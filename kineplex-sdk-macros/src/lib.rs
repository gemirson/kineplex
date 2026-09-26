//! User-facing WebAssembly entry-point macro.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, ReturnType, Type};

/// Wraps `fn(RecordBatch) -> RecordBatch` in the KinePlex Arrow IPC guest ABI.
#[proc_macro_attribute]
pub fn synapse(_attribute: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);
    if function.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(
            &function.sig,
            "#[synapse] expects one RecordBatch argument",
        )
        .to_compile_error()
        .into();
    }
    if !matches!(function.sig.inputs.first(), Some(FnArg::Typed(argument)) if is_record_batch(&argument.ty))
    {
        return syn::Error::new_spanned(
            &function.sig.inputs,
            "#[synapse] argument must be RecordBatch",
        )
        .to_compile_error()
        .into();
    }
    if !matches!(&function.sig.output, ReturnType::Type(_, ty) if is_record_batch(ty)) {
        return syn::Error::new_spanned(
            &function.sig.output,
            "#[synapse] return type must be RecordBatch",
        )
        .to_compile_error()
        .into();
    }

    let function_name = &function.sig.ident;
    quote! {
        #function

        #[no_mangle]
        pub extern "C" fn alloc_ffi(size: i32) -> i32 {
            ::kineplex_sdk::__private::allocate(size)
        }

        #[no_mangle]
        pub extern "C" fn free_ffi(pointer: i32, size: i32) {
            ::kineplex_sdk::__private::deallocate(pointer, size);
        }

        #[no_mangle]
        pub extern "C" fn kineplex_result_size() -> i32 {
            ::kineplex_sdk::__private::result_size()
        }

        #[no_mangle]
        pub extern "C" fn kineplex_run(input_ptr: i32, input_schema: i32) -> i32 {
            let result = ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| {
                let input = ::kineplex_sdk::__private::decode_input(input_ptr, input_schema)?;
                let output = #function_name(input);
                ::kineplex_sdk::__private::encode_output(&output)
            }));
            match result {
                Ok(Ok(pointer)) => pointer,
                Ok(Err(_)) | Err(_) => {
                    ::kineplex_sdk::__private::set_result_error();
                    0
                }
            }
        }
    }
    .into()
}

fn is_record_batch(ty: &Type) -> bool {
    match ty {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "RecordBatch"),
        _ => false,
    }
}
