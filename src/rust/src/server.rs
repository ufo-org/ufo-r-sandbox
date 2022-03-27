use crate::r_bail;
use crate::r_error;
use crate::r_err;

use std::collections::HashMap;

use extendr_api::prelude::*;

use ufo_ipc::FunctionToken;
use ufo_ipc::ProtocolCommand;

use crate::rserialization::*;

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
struct DataToken(i64); 
struct UserData(Robj); /* Robj::PairList */    

impl From<i64> for DataToken {
    fn from(v: i64) -> Self {
        DataToken(v)
    }
}

struct Function {
    executable: Robj, /* Robj::Function */
    user_data: DataToken,
}

pub struct Server {
    functions: HashMap<FunctionToken, Function>,
    objects: HashMap<DataToken, UserData>,
}

macro_rules! expect_size {
    ($data:expr => $expected_size:expr, $error_message_header:literal, $($arg:expr),*$(,)?) => {
        if $data.len() != $expected_size {
            let header = format!($error_message_header, $($arg,)*);
            r_bail!("{}: excepting {} item of associated data, found: {}",
                     header, $expected_size, $data.len());
        }
    };
}

macro_rules! expect_type {
    ($data:expr => $expected_type:expr, $error_message_header:literal, $($arg:expr),*$(,)?) => {
        if $data.rtype() != $expected_type {
            let header = format!($error_message_header, $($arg,)*);
            r_bail!("{}: excepting R object of type {:?}, but found: {:?}",
                     header, $expected_type, $data.rtype());
        }
    };
}

impl Server {
    pub fn new() -> Self {
        Server {
            functions: HashMap::new(),
            objects: HashMap::new(),
        }
    }

    fn register_function(&mut self, token: FunctionToken, function: Robj, data_token: DataToken) -> Result<()> {
        expect_type!(function => Rtype::Function, "Cannot register function {:?}", token);

        todo!()
    }

    pub fn listen(&mut self) -> Result<()> {
        let mut server = ufo_ipc::subordinate_begin().unwrap(); // TODO return Result<()>?
        loop {
            let request = server.recv_command().unwrap();    // TODO error handling    
    
            match request.command {            
                ProtocolCommand::Define { token, function_blob, associated_data } => {
                    // Retrieve data from message
                    expect_size!(associated_data => 1, "Cannot register function {:?}", token);
                    let user_data: DataToken = associated_data.into_first()?
                        .expect_i64_into().map_err(|e| {
                            r_error!("Cannot register function {:?}: {}", token, e)
                        })?.into();

                    // Deserialize R data
                    let function: Robj = function_blob.r_deserialize(Rtype::Function)?;
    
                    // Do the thing
                    self.register_function(token, function, user_data)?;
    
                    // Reply
                    server.respond_to_define(todo!()).unwrap() // TODO error handling                    
                },
                ProtocolCommand::Call { token, args } => {
                    // if args.len() != 1 {
                    //     r_bail!("Cannot register function {:?}: \
                    //              excepting 1 item of associated data, found: {}",
                    //              token, args.len());
                    // }
                    // let associated_datum = args.into_first()?;
                    // let argument_list = associated_datum.expect_bytes_into().unwrap(); // TODO error handling
    
                    // let result = call_function(&functions, token, argument_list)?;
    
                    // // TODO reply
                    // server.respond_to_call(todo!(), todo!()).unwrap() // TODO error handling
                },
                ProtocolCommand::Free(token) => {
                    
                },
                ProtocolCommand::Peek(key) => (),
                ProtocolCommand::Poke { key: _, value } => {
    
                },
                ProtocolCommand::Shutdown => (),
            }
    
            // receive message
            // decode message
            // demux to processor functions
        }
    }
}

trait IntoElement<T> {
    fn into_first(self) -> Result<T>;
    fn into_first_two(self) -> Result<(T, T)>;
}
impl<T,I> IntoElement<T> for I where I: IntoIterator<Item=T> {
    fn into_first(self) -> Result<T> {
        self.into_iter().next().ok_or_else(|| {
            r_error!("Expecting an argument, but none were provided")
        })
    }
    fn into_first_two(self) -> Result<(T, T)> {
        let mut iter = self.into_iter();
        let first = iter.next().ok_or_else(|| {
            r_error!("Expecting two arguments, but none were provided")
        })?;
        let second = iter.next().ok_or_else(|| {
            r_error!("Expecting two arguments, but only one was provided")
        })?;
        Ok((first, second))
    }
}