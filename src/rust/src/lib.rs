use extendr_api::prelude::*;

use std::collections::HashMap;

macro_rules! r_error {
    ($($s:expr),*$(,)?) => {
        extendr_api::Error::Other(format!($($s,)+))
    };
}

macro_rules! r_err {
    ($($s:expr),*$(,)?) => {
        Err(extendr_api::Error::Other(format!($($s,)+)))
    };
}

macro_rules! r_bail {
    ($($s:expr),+) => {
        return Err(r_error!($($s,)+))
    };
}

type FunctionID = u64;

trait RDeserialize: Sized {
    fn r_deserialize_any(self) -> Result<Robj>;
    fn r_deserialize(self, expected_type: Rtype) -> Result<Robj> {
        let object: Robj = self.r_deserialize_any()?;
        if object.rtype() != expected_type {
            r_err!(
                "Cannot deserialize object: \
                   expecting R object of type {:?}, \
                   but encountered {:?}. Serialized form: {:?}",
                expected_type,
                object.rtype(),
                object
            )
        } else {
            Ok(object)
        }
    }
}

impl RDeserialize for &[u8] {
    fn r_deserialize_any(self) -> Result<Robj> {
        call!("unserialize", self)
    }
}

struct Function {
    executable: Robj, /* Robj::Function */
    user_data: Robj,  /* Robj::Language */
}

impl Function {
    pub fn r_deserialize_from(
        serialized_function: &[u8],
        serialized_user_data: &[u8],
    ) -> Result<Self> {
        let executable: Robj = serialized_function.r_deserialize(Rtype::Function)?;
        let user_data = serialized_user_data.r_deserialize(Rtype::Language)?;
        Ok(Function {
            executable,
            user_data,
        })
    }
    pub fn r_call(&self, arguments: Robj) -> Result<Robj> {
        assert!(
            arguments.rtype() == Rtype::Language,
            "Cannot call function: arguments have to be a Language object"
        );
        let combined_arguments = pairlist!(&self.user_data, arguments);
        self.executable.call(combined_arguments)
    }
}

fn register_function(
    functions: &mut HashMap<FunctionID, Function>,
    id: FunctionID,
    serialized_function: &[u8],
    serialized_user_data: &[u8],
) -> Result<()> {
    let id = id.into();
    if functions.contains_key(&id) {
        r_bail!("Cannot register function {}: already exists", id);
    }

    let function = Function::r_deserialize_from(serialized_function, serialized_user_data)?;
    functions.insert(id, function);

    Ok(())
}

fn call_function(
    functions: &HashMap<FunctionID, Function>,
    id: FunctionID,
    serialized_arguments: &[u8],
) -> Result<Robj> {
    let argument_list = serialized_arguments.r_deserialize(Rtype::Language)?;

    let function = functions
        .get(&id)
        .ok_or_else(|| r_error!("Cannot call function {}: not registered", id))?;

    function.r_call(argument_list)
}

// fn call_function(

// )

/// Start the remote execution server
/// @export
#[extendr]
fn start() {
    let mut functions: HashMap<FunctionID, Function> = HashMap::new();

    loop {
        // receive message
        // decode message
        // demux to processor functions
    }
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
