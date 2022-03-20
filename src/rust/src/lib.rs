use extendr_api::prelude::*;

/// Return string `"Hello world!"` to R.
/// @export
#[extendr]
fn hello_world() -> &'static str {
    "Hello world!"
}

/// Start listening to remote execution requests
/// @export
#[extendr]
fn listen(port: &str) {
    println!("Listening...");
}

/// Execute 
/// @export
#[extendr]
fn execute(serialized_function: &[u8]) -> Robj {
    let function = call!("unserialize", serialized_function).unwrap();
    function.call(pairlist!(42)).unwrap()
}

// fun <- function(x) x + 1
// compiled_fun <- compiler::cmpfun(fun)
// serialized_fun <- serialize(compiled_fun, connection=NULL)
// uforemote::execute(serialized_fun)

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod uforemote;
    fn hello_world;
    fn listen;
    fn execute;
}

//     let expr = R!(function(a = 1, b) {c <- a + b}).unwrap();
//     let func = expr.as_func().unwrap();
//
//     let expected_formals = Pairlist {
//         names_and_values: vec![("a", r!(1.0)), ("b", missing_arg())] };
//     let expected_body = lang!(
//         "{", lang!("<-", sym!(c), lang!("+", sym!(a), sym!(b))));
//     assert_eq!(func.formals.as_pairlist().unwrap(), expected_formals);
//     assert_eq!(func.body, expected_body);
//     assert_eq!(func.env, global_env());

// fn fuck(code: Robj) -> Robj {

//     assert!(code.rtype() == RType::Function);
//     R!("{{code}}(42)")
// }