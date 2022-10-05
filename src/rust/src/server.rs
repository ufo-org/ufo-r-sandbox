use std::collections::VecDeque;
// use std::iter::FromIterator;
// use std::process::Command;
// use std::sync::{Mutex, Arc, MutexGuard};

use itertools::Itertools;

use ufo_ipc::{self, GenericValueBoxed, GenericValueRef, ProtocolCommand};
// use ufo_ipc::StartSubordinateProcess;
// use ufo_ipc::ControllerProcess;
use ufo_ipc::DataToken;
use ufo_ipc::GenericValue;

pub use ufo_ipc::FunctionToken;

use extendr_api::*;

use crate::errors::*;
use crate::serder::{DeserdeR, SmartDeserialization};
use crate::ufotype::{UfoType, UfoTypeChecker};
use crate::{r_bail_if, r_error}; // r_err, r_bail};

#[derive(Debug)]
struct Function {
    executable: Robj, /* Robj::Function */
    user_data: DataToken,
    parameters: VecDeque<String>,
    return_type: Option<UfoType>,
}

#[derive(Debug, Default)]
pub struct Server {
    functions: HashMap<FunctionToken, Function>,
    objects: HashMap<DataToken, List>,
}

impl Server {
    pub fn new() -> Self {
        log::debug!("Server::new");

        Server {
            functions: HashMap::new(),
            objects: HashMap::new(),
        }
    }

    fn define_data(&mut self, token: DataToken, value: Vec<u8>) -> Result<()> {
        log::debug!("Server::define_data:");
        log::debug!("   self:           {:?}", self);
        log::debug!("   token:          {:?}", token);
        log::debug!("   value:          {:?}", value);

        r_bail_if!(self.objects.contains_key(&token) =>
            "Sandbox server error: Cannot define user data {:?} because it is already defined.", token);
        let value: List = value.deserialize()
            .rewrap(|| format!("Sandbox server error: Cannot define user data {:?}", token))?
            .as_list()
            .rewrap(|| format!("Sandbox server error: Cannot define user data {:?}: expecting user data to be a list", token))?;

        self.objects.insert(token, value);
        Ok(())
    }

    fn free_data(&mut self, token: DataToken) -> Result<()> {
        log::debug!("Server::free_data:");
        log::debug!("   self:           {:?}", self);
        log::debug!("   token:          {:?}", token);

        self.objects.remove(&token).rewrap(|| {
            format!(
                "Sandbox server error: Cannot remove user data {:?} because it does not exist",
                token
            )
        })?;
        Ok(())
    }

    fn define_function(
        &mut self,
        token: FunctionToken,
        user_data: DataToken,
        function: Vec<u8>,
        parameters: VecDeque<String>,
        return_type: Option<&String>,
    ) -> Result<()> {
        log::debug!("Server::define_function:");
        log::debug!("   self:           {:?}", self);
        log::debug!("   token:          {:?}", token);
        log::debug!("   user_data:      {:?}", user_data);
        log::debug!("   function:       {:?}", function);
        log::debug!("   parameters:     {:?}", parameters);

        r_bail_if!(!self.objects.contains_key(&user_data) =>
            "Sandbox server error: Cannot define function {:?} because user data {:?} does not exist.", token, user_data);
        r_bail_if!(self.functions.contains_key(&token) =>
            "Sandbox server error: Cannot define function {:?} because it is already defined.", token);
        let executable = function
            .deserialize_into(Rtype::Function)
            .rewrap(|| format!("Sandbox server error: Cannot define function {:?}", token))?;

        let return_type = return_type
            .map(UfoType::try_from)
            .extract_result()
            .rewrap(|| format!("Sandbox server error: Cannot define function {:?} because of unknown return type {:?}", token, return_type))?;

        // parameters.push_front("user_data".to_owned());
        // parameters.push_front("user_function".to_owned());

        self.functions.insert(
            token,
            Function {
                executable,
                user_data,
                parameters,
                return_type,
            },
        );
        Ok(())
    }

    fn call_function<Tv, Ts>(
        &mut self,
        token: FunctionToken,
        arguments: Vec<GenericValue<Tv, Ts>>,
    ) -> Result<Vec<GenericValueBoxed>>
    where
        Tv: DeserdeR + SmartDeserialization + std::fmt::Debug,
        Ts: ToVectorValue + std::fmt::Debug + AsRef<str>,
    {
        log::debug!("Server::call_function:");
        log::debug!("   self:           {:?}", self);
        log::debug!("   token:          {:?}", token);
        log::debug!("   arguments:      {:?}", arguments);

        let function = self.functions.get(&token).rewrap(|| {
            format!(
                "Sandbox server error: Cannot call function {:?} because it is not defined.",
                token
            )
        })?;

        log::debug!("   function:       {:?}", function);

        r_bail_if!(function.parameters.len() != arguments.len() => // 2 arguments are tacked on: user_function and user_data
            "Sandbox server error: Cannot call function {:?} because the number of arguments {} does not match the expected {}", 
            token, arguments.len(), function.parameters.len());

        let user_data = self.objects.get(&function.user_data)
            .rewrap(|| format!("Sandbox server error: Cannot call function {:?} because its user data {:?} is not defined.",
            token, function.user_data))?;

        let deserialized_arguments = arguments
            .into_iter()
            .map(|generic| {
                generic.smart_deserialize()
            })
            .collect::<Result<Vec<Robj>>>()?;


        log::debug!("   user_data:  {:?}", user_data);
        log::debug!("   function parameters: {:?}", function.parameters);

        // Needed to shorten the lifetimes from 'static to '_.
        let user_data = user_data.iter().map(|(name, value)| (name, value));

        let pairs = List::from_pairs(
            function
                .parameters
                .iter()
                .map(|e| e.as_str())
                .zip(deserialized_arguments)
                .chain(user_data)
                .collect::<Vec<(&str, Robj)>>(),
        );

        log::debug!(
            "Calling runtime: do.call({:?}, {:?}",
            function.executable, pairs
        );

        let result = call!("do.call", &function.executable, pairs)?;
        log::debug!("   result:         {:?}", result);

        if let Some(return_type) = function.return_type {
            r_bail_if!(!function.return_type.check_against(&result) =>
                "Sandbox server error: Cannot call function {:?} because cannot convert result {:?} of type {:?} into vector of expected type {:?}", 
                token, result, result.rtype(), return_type);
            let result = return_type.pack_for_transport(result);            
            result
        } else {
            // If there isn't a return type, we return nothing without checking
            // or transforming anything - this allows the R function to returns
            // stuff in a procedure where we don't care, like writeback.
            Ok(Vec::new())
        }
    }

    fn free_function(&mut self, token: FunctionToken) -> Result<()> {
        log::debug!("Server::free_function:");
        log::debug!("   self:           {:?}", self);
        log::debug!("   token:          {:?}", token);

        self.functions.remove(&token).rewrap(|| {
            format!(
                "Sandbox server error: Cannot remove function {:?} because it does not exist",
                token
            )
        })?;
        Ok(())
    }

    pub fn listen(&mut self) -> Result<()> {
        log::debug!("Server::listen:");
        log::debug!("   self:           {:?}", self);

        let mut server = ufo_ipc::subordinate_begin().rewrap(|| "Sandbox server error:")?;
        loop {
            let request = server.recv_command().rewrap(|| "Sandbox server error:")?;
            match request.command {
                ProtocolCommand::DefineData { token, value } => {
                    let value = value
                        .into_first()?
                        .expect_bytes_into()
                        .rewrap(|| "Sandbox server error: Cannot define user data")?;
                    self.define_data(token, value)?;
                    server
                        .respond_to_define(&[]) // TODO is this the correct function?
                        .rewrap(|| "Sandbox server error: Cannot respond to data definition")?
                }

                ProtocolCommand::FreeData(token) => {
                    self.free_data(token)?;
                    server
                        .respond_to_unregister(&[]) // TODO is this the correct function?
                        .rewrap(|| "Sandbox server error: Cannot respond to data free")?
                }

                ProtocolCommand::DefineFunction {
                    token,
                    function_blob: function,
                    associated_data,
                } => {
                    r_bail_if!(associated_data.is_empty() =>
                        "Sandbox server error: Cannot define function: expecting at least one associated items (user data token and return type)");
                    let user_data = associated_data[0]
                        .expect_token()
                        .rewrap(|| "Sandbox server error: Cannot define function")?;
                    let return_type = request
                        .aux
                        .first()
                        .map(|generic| generic.expect_string())
                        .extract_result()
                        .rewrap(|| "Sandbox server error: Cannot define function")?;

                    let parameters = associated_data[1..]
                        .iter()
                        .map(|generic| {
                            generic
                                .expect_string()
                                .rewrap(|| "Sandbox server error: Invalid function parameter")
                                .map(|s| s.to_owned())
                        })
                        .collect::<Result<VecDeque<String>>>()?;
                    self.define_function(token, *user_data, function, parameters, return_type)?;
                    server
                        .respond_to_define(&[]) // TODO is this the correct function?
                        .rewrap(|| "Sandbox server error: Cannot respond to function definition")?
                }

                ProtocolCommand::Call { token, args } => {
                    let result: Vec<GenericValueBoxed> = self.call_function(token, args)?;
                    let result: Vec<GenericValueRef> =
                        result.iter().map(|e| GenericValueRef::from(e)).collect();
                    server
                        .respond_to_call(result.as_slice(), &[])
                        .rewrap(|| "Sandbox server error: Cannot respond to function call")?;
                }

                ProtocolCommand::FreeFunction(token) => {
                    self.free_function(token)?;
                    server
                        .respond_to_unregister(&[])
                        .rewrap(|| "Sandbox server error: Cannot respond to function free")?
                }

                ProtocolCommand::Peek(_) => unimplemented!(),
                ProtocolCommand::Poke { .. } => unimplemented!(),

                ProtocolCommand::Shutdown => {
                    return Ok(());
                }
            }
        }
    }
}

trait IntoElement<T> {
    fn into_first(self) -> Result<T>;
    fn into_first_two(self) -> Result<(T, T)>;
}
impl<T, I> IntoElement<T> for I
where
    I: IntoIterator<Item = T>,
{
    fn into_first(self) -> Result<T> {
        self.into_iter()
            .exactly_one()
            .rewrap(|| "Expecting an argument, but none were provided")
    }
    fn into_first_two(self) -> Result<(T, T)> {
        let mut iter = self.into_iter();
        let first = iter
            .next()
            .rewrap(|| "Expecting two arguments, but none were provided")?;
        let second = iter
            .next()
            .rewrap(|| "Expecting two arguments, but only one was provided")?;
        let _third = iter
            .next()
            .rewrap(|| "Expecting two arguments, but more than two were provided")?;
        Ok((first, second))
    }
}
