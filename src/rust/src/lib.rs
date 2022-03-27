mod rserialization;
mod server;

use extendr_api::prelude::*;

use server::Server;
use ufo_ipc;
use ufo_ipc::ProtocolCommand;
use ufo_ipc::FunctionToken;

use std::collections::HashMap;

#[macro_export]
macro_rules! r_error {
    ($($s:expr),*$(,)?) => {
        extendr_api::Error::Other(format!($($s,)+))
    };
}

#[macro_export]
macro_rules! r_err {
    ($($s:expr),*$(,)?) => {
        Err(extendr_api::Error::Other(format!($($s,)+)))
    };
}

#[macro_export]
macro_rules! r_bail {
    ($($s:expr),+) => {
        return Err(r_error!($($s,)+))
    };
}


// impl Function {
//     pub fn new(function: Robj, data_token: DataToken) -> Self {
//         Function()
//     }
//     pub fn r_deserialize_from(
//         serialized_function: &[u8],
//         serialized_user_data: &[u8],
//     ) -> Result<Self> {
//         let executable: Robj = serialized_function.r_deserialize(Rtype::Function)?;
//         // let user_data = serialized_user_data.r_deserialize(Rtype::Language)?;
//         Ok(Function(executable))
//     }
//     pub fn r_call(&self, arguments: Robj) -> Result<Robj> {
//         assert!(
//             arguments.rtype() == Rtype::Language,
//             "Cannot call function: arguments have to be a Language object"
//         );
//         let combined_arguments = pairlist!(&self.user_data, arguments);
//         self.executable.call(combined_arguments)
//     }
// }

// fn register_function(
//     functions: &mut HashMap<FunctionToken, Function>,
//     id: FunctionToken,
//     serialized_function: &[u8],
//     serialized_user_data: &[u8],
// ) -> Result<()> {
//     let id = id.into();
//     if functions.contains_key(&id) {
//         r_bail!("Cannot register function {:?}: already exists", id);
//     }

//     let function = Function::r_deserialize_from(serialized_function, serialized_user_data)?;
//     functions.insert(id, function);

//     Ok(())
// }

// fn unregister_function(
//     functions: &mut HashMap<FunctionToken, Function>,
//     id: FunctionToken,
// ) -> Result<Robj> {

// }

// fn call_function(
//     functions: &HashMap<FunctionToken, Function>,
//     id: FunctionToken,
//     serialized_arguments: Vec<u8>,
// ) -> Result<Robj> {
//     let argument_list = serialized_arguments.r_deserialize(Rtype::Language)?;

//     let function = functions
//         .get(&id)
//         .ok_or_else(|| r_error!("Cannot call function {:?}: not registered", id))?;

//     function.r_call(argument_list)
// }



/// Start the remote execution server
/// @export
#[extendr]
fn start() -> Result<()> {
    Server::new().listen()
}
    // let mut functions: HashMap<FunctionToken, Function> = HashMap::new();
    // let mut user_objects: HashMap<DataToken, UserData> = HashMap::new();

    // let mut server = ufo_ipc::subordinate_begin().unwrap(); // TODO return Result<()>?
    // loop {
    //     let request = server.recv_command().unwrap();    // TODO error handling    

    //     match &request.command {            
    //         ProtocolCommand::Define { token, function_blob, associated_data } => {
    //             if associated_data.len() != 1 {
    //                 r_bail!("Cannot register function {:?}: \
    //                          excepting 1 item of associated data, found: {}",
    //                          token, associated_data.len());
    //             }
    //             let associated_datum = associated_data.into_iter().next().unwrap();
    //             let user_data = associated_datum.expect_bytes_into().unwrap(); // TODO error handling

    //             register_function(&mut functions, token, function_blob, user_data).unwrap();

    //             // TODO reply
    //             server.respond_to_define(todo!()).unwrap() // TODO error handling
    //         },
    //         ProtocolCommand::Call { token, args } => {
    //             if args.len() != 1 {
    //                 r_bail!("Cannot register function {:?}: \
    //                          excepting 1 item of associated data, found: {}",
    //                          token, args.len());
    //             }
    //             let associated_datum = args.into_iter().next().unwrap();
    //             let argument_list = associated_datum.expect_bytes_into().unwrap(); // TODO error handling

    //             let result = call_function(&functions, token, argument_list)?;

    //             // TODO reply
    //             server.respond_to_call(todo!(), todo!()).unwrap() // TODO error handling
    //         },
    //         ProtocolCommand::Free(token) => {
                
    //         },
    //         ProtocolCommand::Peek(key) => (),
    //         ProtocolCommand::Poke { key: _, value } => {

    //         },
    //         ProtocolCommand::Shutdown => (),
    //     }

    //     // receive message
    //     // decode message
    //     // demux to processor functions
    // }

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
    fn start;
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
