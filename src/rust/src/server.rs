use std::process::Command;
use std::sync::{Mutex, Arc, MutexGuard};

use itertools::Itertools;

use ufo_ipc::{self, GenericValueRef, Request, ProtocolCommand};
use ufo_ipc::StartSubordinateProcess;
use ufo_ipc::ControllerProcess;
use ufo_ipc::GenericValue;
use ufo_ipc::DataToken;

pub use ufo_ipc::FunctionToken;

use extendr_api::*;

use crate::serder::{DeserdeR, SerdeR};
use crate::{r_error, r_bail_if, r_err, r_bail};

struct Function {
    executable: Robj, /* Robj::Function */
    user_data: DataToken,
    parameters: Vec<String>,
}

pub struct Server {
    functions: HashMap<FunctionToken, Function>,
    objects: HashMap<DataToken, Robj>,
}

impl Server {
    pub fn new() -> Self {
        Server {
            functions: HashMap::new(),
            objects: HashMap::new(),
        }
    }

    fn define_data(&mut self, token: DataToken, value: Vec<u8>) -> Result<()> {
        r_bail_if!(self.objects.contains_key(&token) => 
            "Sandbox server error: Cannot define user data {:?} because it is already defined.", token);        
        let value: Robj = value.deserialize()
            .rewrap(format!("Sandbox server error: Cannot define user data {:?}", token))?;
        self.objects.insert(token, value);
        Ok(())                
    }

    fn free_data(&mut self, token: DataToken) -> Result<()> {        
        self.objects.remove(&token)
            .rewrap(format!("Sandbox server error: Cannot remove user data {:?} because it does not exist", token))?;
        Ok(())
    }

    fn define_function(&mut self, token: FunctionToken, user_data: DataToken, function: Vec<u8>, parameters: Vec<String>) -> Result<()> {
        r_bail_if!(!self.objects.contains_key(&user_data) => 
            "Sandbox server error: Cannot define function {:?} because user data {:?} does not exist.", token, user_data);
        r_bail_if!(self.functions.contains_key(&token) =>
            "Sandbox server error: Cannot define function {:?} because it is already defined.", token);
        let executable = function.deserialize_into(Rtype::Function)
            .rewrap(format!("Sandbox server error: Cannot define function {:?}", token))?;
        self.functions.insert(token, Function { executable, user_data, parameters });
        Ok(())
    }

    fn call_function<Tv, Ts>(&mut self, token: FunctionToken, arguments: Vec<GenericValue<Tv, Ts>>) -> Result<Vec<u8>> where Tv: DeserdeR, Ts: ToVectorValue {
        let function = self.functions.get(&token)
            .rewrap(format!("Sandbox server error: Cannot call function {:?} because it is not defined.", token))?;

        r_bail_if!(function.parameters.len() != arguments.len() => 
            "Sandbox server error: Cannot call function {:?} because the number of arguments {} does not match the expected {}", 
            token, arguments.len(), function.parameters.len());

        todo!(); // Pass in user data as argument as well

        let arguments = arguments.into_iter().map(|generic| generic.deserialize()).collect::<Result<Vec<Robj>>>()?;
        let pairs: Vec<(&String, Robj)> = function.parameters.iter().zip(arguments.into_iter()).collect();
        let result = function.executable.call(Pairlist::from_pairs(pairs))?;

        result.serialize()
    }

    fn free_function(&mut self, token: FunctionToken) -> Result<()> {        
        self.functions.remove(&token)
            .rewrap(format!("Sandbox server error: Cannot remove function {:?} because it does not exist", token))?;
        Ok(())
    }


    pub fn listen(&mut self) -> Result<()> {
        let mut server = ufo_ipc::subordinate_begin().rewrap("Sandbox server error:")?;
        loop {
            let request = server.recv_command().rewrap("Sandbox server error:")?;
            match request.command {
                ProtocolCommand::DefineData { token, value } => {
                    let value = value.into_first()?.expect_bytes_into().rewrap("Sandbox server error: Cannot define user data")?;
                    self.define_data(token, value)?;
                    server.respond_to_define(&[]) // TODO is this the correct function?
                        .rewrap("Sandbox server error: Cannot respond to data definition")?
                }

                ProtocolCommand::FreeData(token) => {
                    self.free_data(token)?;
                    server.respond_to_unregister(&[]) // TODO is this the correct function?
                        .rewrap("Sandbox server error: Cannot respond to data free")?
                }

                ProtocolCommand::DefineFunction { token, function_blob: function, associated_data } => {
                    r_bail_if!(associated_data.len() < 1 => 
                        "Sandbox server error: Cannot define function: expecting at least one associated item (user data token)");
                    let user_data = associated_data[0].expect_token().rewrap("Sandbox server error: Cannot define function")?;
                    let parameters = associated_data[1..].into_iter().map(|generic| {
                        generic.expect_string().rewrap("Sandbox server error: Invalid function parameter").map(|s| s.to_owned())
                    }).collect::<Result<Vec<String>>>()?;
                    self.define_function(token, user_data.clone(), function, parameters)?;
                    server.respond_to_define(&[]) // TODO is this the correct function?
                        .rewrap("Sandbox server error: Cannot respond to function definition")?
                },

                ProtocolCommand::Call { token, args } => {
                    let result = self.call_function(token, args)?;
                    let generic = GenericValueRef::Vbytes(result.as_slice());
                    server.respond_to_call(&[generic], &[])
                        .rewrap("Sandbox server error: Cannot respond to function call")?;
                },

                ProtocolCommand::FreeFunction(token) => {
                    self.free_function(token)?;
                    server.respond_to_unregister(&[])
                        .rewrap("Sandbox server error: Cannot respond to function free")?
                }
                
                ProtocolCommand::Peek(_) => unimplemented!(),
                ProtocolCommand::Poke { key, value } => unimplemented!(),

                ProtocolCommand::Shutdown => { return Ok(()); },
            }
        }
    }
}

trait IntoServerError<T> {
    fn rewrap<S>(self, msg: S) -> Result<T>  where S: std::fmt::Display;
}

impl<T, E> IntoServerError<T> for std::result::Result<T, E> where E: std::fmt::Display {
    fn rewrap<S>(self, msg: S) -> Result<T> where S: std::fmt::Display{
        self.map_err(|e| r_error!("{}: {}", msg, e))
    }
}

impl<T> IntoServerError<T> for Option<T> {
    fn rewrap<S>(self, msg: S) -> Result<T> where S: std::fmt::Display{
        self.ok_or_else(|| r_error!("{}", msg))
    }
}

trait IntoElement<T> {
    fn into_first(self) -> Result<T>;
    fn into_first_two(self) -> Result<(T, T)>;
}
impl<T,I> IntoElement<T> for I where I: IntoIterator<Item=T> {
    fn into_first(self) -> Result<T> {
        self.into_iter().exactly_one().rewrap("Expecting an argument, but none were provided")
    }
    fn into_first_two(self) -> Result<(T, T)> {
        let mut iter = self.into_iter();
        let first = iter.next().ok_or_else(|| {
            r_error!("Expecting two arguments, but none were provided")
        })?;
        let second = iter.next().ok_or_else(|| {
            r_error!("Expecting two arguments, but only one was provided")
        })?;
        r_bail_if!(iter.next().is_some() => "Expecting two arguments, but more than two were proivided");
        Ok((first, second))
    }
}