use std::collections::VecDeque;
// use std::iter::FromIterator;
// use std::process::Command;
// use std::sync::{Mutex, Arc, MutexGuard};

use itertools::Itertools;

use ufo_ipc::{self, GenericValueRef, ProtocolCommand};
// use ufo_ipc::StartSubordinateProcess;
// use ufo_ipc::ControllerProcess;
use ufo_ipc::GenericValue;
use ufo_ipc::DataToken;

pub use ufo_ipc::FunctionToken;

use extendr_api::*;

use crate::serder::DeserdeR;
use crate::ufotype::{UfoType, UfoTypeChecker};
use crate::{r_error, r_bail_if}; // r_err, r_bail};
use crate::errors::*;

#[derive(Debug)]
struct Function {
    executable: Robj, /* Robj::Function */
    user_data: DataToken,
    parameters: VecDeque<String>,
    return_type: Option<UfoType>,
}

#[derive(Debug)]
pub struct Server {
    functions: HashMap<FunctionToken, Function>,
    objects: HashMap<DataToken, Pairlist>,
}

impl Server {
    pub fn new() -> Self {
        eprintln!("Server::new");

        Server {
            functions: HashMap::new(),
            objects: HashMap::new(),
        }
    }

    fn define_data(&mut self, token: DataToken, value: Vec<u8>) -> Result<()> {
        eprintln!("Server::define_data:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);
        eprintln!("   value:          {:?}", value);

        r_bail_if!(self.objects.contains_key(&token) => 
            "Sandbox server error: Cannot define user data {:?} because it is already defined.", token);        
        let value: Pairlist = value.deserialize()
            .rewrap(|| format!("Sandbox server error: Cannot define user data {:?}", token))?
            .as_pairlist()
            .rewrap(|| format!("Sandbox server error: Cannot define user data {:?}: expecting user data to be a pairlist", token))?;

        self.objects.insert(token, value);
        Ok(())                
    }

    fn free_data(&mut self, token: DataToken) -> Result<()> {        
        eprintln!("Server::free_data:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);

        self.objects.remove(&token)
            .rewrap(|| format!("Sandbox server error: Cannot remove user data {:?} because it does not exist", token))?;
        Ok(())
    }

    fn define_function(&mut self, token: FunctionToken, user_data: DataToken, function: Vec<u8>, parameters: VecDeque<String>, return_type: Option<&String>) -> Result<()> {
        eprintln!("Server::define_function:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);
        eprintln!("   user_data:      {:?}", user_data);
        eprintln!("   function:       {:?}", function);
        eprintln!("   parameters:     {:?}", parameters);

        r_bail_if!(!self.objects.contains_key(&user_data) => 
            "Sandbox server error: Cannot define function {:?} because user data {:?} does not exist.", token, user_data);
        r_bail_if!(self.functions.contains_key(&token) =>
            "Sandbox server error: Cannot define function {:?} because it is already defined.", token);
        let executable = function.deserialize_into(Rtype::Function)
            .rewrap(|| format!("Sandbox server error: Cannot define function {:?}", token))?;

        let return_type = return_type
            .map(|string| UfoType::try_from(string))
            .extract_result()
            .rewrap(|| format!("Sandbox server error: Cannot define function {:?} because of unknown return type {:?}", token, return_type))?;
        
        // parameters.push_front("user_data".to_owned());
        // parameters.push_front("user_function".to_owned());        

        self.functions.insert(token, Function { executable, user_data, parameters, return_type});
        Ok(())
    }

    fn call_function<Tv, Ts>(&mut self, token: FunctionToken, arguments: Vec<GenericValue<Tv, Ts>>) -> Result<&[u8]> where Tv: DeserdeR + std::fmt::Debug, Ts: ToVectorValue + std::fmt::Debug {
        eprintln!("Server::call_function:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);
        eprintln!("   arguments:      {:?}", arguments);

        let function = self.functions.get(&token)
            .rewrap(|| format!("Sandbox server error: Cannot call function {:?} because it is not defined.", token))?;

        eprintln!("   function:       {:?}", function);

        r_bail_if!(function.parameters.len() != arguments.len() => // 2 arguments are tacked on: user_function and user_data
            "Sandbox server error: Cannot call function {:?} because the number of arguments {} does not match the expected {}", 
            token, arguments.len(), function.parameters.len());

        let user_data = self.objects.get(&function.user_data)
            .rewrap(|| format!("Sandbox server error: Cannot call function {:?} because its user data {:?} is not defined.",
            token, function.user_data))?;

        let deserialized_arguments = arguments.into_iter()
            .map(|generic| generic.deserialize())
            .collect::<Result<Vec<Robj>>>()?;
 
        // Needed to shorten the lifetimes from 'static to '_.
        let user_data = user_data.iter()
            .map(|(name, value)| (name, value));

        let pairs = List::from_pairs(
            function.parameters.iter()
                .map(|e| e.as_str())
                .zip(deserialized_arguments)
                .chain(user_data)
                .collect::<Vec<(&str, Robj)>>()
        );

        eprintln!("Calling runtime: do.call({:?}, {:?}", function.executable, pairs);

        let result= call!("do.call", &function.executable, pairs)?;
        eprintln!("   result:         {:?}", result);

        // let serialized_result = result.serialize()?;        
        // eprintln!("   serialized:     {:?}", serialized_result);        

        r_bail_if!(!function.return_type.check_against(&result) => 
            "Sandbox server error: Cannot call function {:?} because cannot convert result {:?} into vector of expected type {:?}", 
            token, result, function.return_type);

        let size: usize = result.len() * function.return_type.map_or(0, |ty| ty.element_size());
        let slice: &[u8] = unsafe {
            let data_ptr = libR_sys::DATAPTR_RO(result.get());
            std::slice::from_raw_parts(data_ptr as *const u8, size)
        };
       
        // Ok(serialized_result)
        Ok(slice)
    }

    fn free_function(&mut self, token: FunctionToken) -> Result<()> {
        eprintln!("Server::free_function:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);        
        
        self.functions.remove(&token)
            .rewrap(|| format!("Sandbox server error: Cannot remove function {:?} because it does not exist", token))?;
        Ok(())
    }

    pub fn listen(&mut self) -> Result<()> {
        eprintln!("Server::listen:");
        eprintln!("   self:           {:?}", self);        

        let mut server = ufo_ipc::subordinate_begin().rewrap(|| "Sandbox server error:")?;
        loop {
            let request = server.recv_command().rewrap(|| "Sandbox server error:")?;
            match request.command {
                ProtocolCommand::DefineData { token, value } => {
                    let value = value.into_first()?.expect_bytes_into().rewrap(|| "Sandbox server error: Cannot define user data")?;
                    self.define_data(token, value)?;
                    server.respond_to_define(&[]) // TODO is this the correct function?
                        .rewrap(|| "Sandbox server error: Cannot respond to data definition")?
                }

                ProtocolCommand::FreeData(token) => {
                    self.free_data(token)?;
                    server.respond_to_unregister(&[]) // TODO is this the correct function?
                        .rewrap(|| "Sandbox server error: Cannot respond to data free")?
                }

                ProtocolCommand::DefineFunction { token, function_blob: function, associated_data } => {
                    r_bail_if!(associated_data.len() < 1 => 
                        "Sandbox server error: Cannot define function: expecting at least one associated items (user data token and return type)");
                    let user_data = associated_data[0].expect_token()
                        .rewrap(|| "Sandbox server error: Cannot define function")?;
                    let return_type = request.aux.first()
                        .map(|generic| generic.expect_string())
                        .extract_result()
                        .rewrap(|| "Sandbox server error: Cannot define function")?;

                    let parameters = associated_data[1..].into_iter()
                        .map(|generic| {
                            generic.expect_string()
                                .rewrap(|| "Sandbox server error: Invalid function parameter")
                                .map(|s| s.to_owned())
                        }).collect::<Result<VecDeque<String>>>()?;
                    self.define_function(token, user_data.clone(), function, parameters, return_type)?;
                    server.respond_to_define(&[]) // TODO is this the correct function?
                        .rewrap(|| "Sandbox server error: Cannot respond to function definition")?
                },

                ProtocolCommand::Call { token, args } => {
                    let result = self.call_function(token, args)?;
                    let generic = GenericValueRef::Vbytes(result);
                    server.respond_to_call(&[generic], &[])
                        .rewrap(|| "Sandbox server error: Cannot respond to function call")?;
                },

                ProtocolCommand::FreeFunction(token) => {
                    self.free_function(token)?;
                    server.respond_to_unregister(&[])
                        .rewrap(|| "Sandbox server error: Cannot respond to function free")?
                }
                
                ProtocolCommand::Peek(_) => unimplemented!(),
                ProtocolCommand::Poke {..} => unimplemented!(),

                ProtocolCommand::Shutdown => { return Ok(()); },
            }
        }
    }
}

trait IntoElement<T> {
    fn into_first(self) -> Result<T>;
    fn into_first_two(self) -> Result<(T, T)>;
}
impl<T,I> IntoElement<T> for I where I: IntoIterator<Item=T> {
    fn into_first(self) -> Result<T> {
        self.into_iter().exactly_one().rewrap(|| "Expecting an argument, but none were provided")
    }
    fn into_first_two(self) -> Result<(T, T)> {
        let mut iter = self.into_iter();
        let first = iter.next().rewrap(|| "Expecting two arguments, but none were provided")?;
        let second = iter.next().rewrap(|| "Expecting two arguments, but only one was provided")?;
        let _third = iter.next().rewrap(|| "Expecting two arguments, but more than two were provided")?;
        Ok((first, second))
    }
}