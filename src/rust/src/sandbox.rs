// use std::collections::VecDeque;
// use std::iter::FromIterator;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

// use itertools::Itertools;

use ufo_ipc;
use ufo_ipc::GenericValueBoxed;
// use ufo_ipc::GenericValueRef;
use ufo_ipc::ControllerProcess;
use ufo_ipc::DataToken;
use ufo_ipc::GenericValue;
use ufo_ipc::StartSubordinateProcess;

pub use ufo_ipc::FunctionToken;

use extendr_api::*;

use crate::r_bail_if;
use crate::r_error;

use crate::ufotype::*;

#[derive(Clone)]
pub struct Sandbox {
    process: Arc<Mutex<ControllerProcess>>,
}

impl std::fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Sandbox").field("process", &"...").finish()
    }
}

impl Sandbox {
    fn lock(&self) -> Result<MutexGuard<ControllerProcess>> {
        self.process
            .deref()
            .lock()
            .map_err(|e| r_error!("Cannot lock sandbox process: {}", e))
    }

    pub fn start() -> Result<Self> {
        eprintln!("Sandbox::start");

        let child = Command::new("R")
            .args(&["--vanilla", "--quiet", "-e", "ufosandbox:::start_sandbox()"])
            .env("RUST_BACKTRACE", "1")
            .start_subordinate_process()
            .map_err(|e| r_error!("Cannot start sandbox: {}", e))?;

        Ok(Sandbox {
            process: Arc::new(Mutex::new(child)),
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        eprintln!("Sandbox::shutdown:");
        eprintln!("   self:           {:?}", self);
        eprintln!("    pid:           {:?}", std::process::id());

        self.lock()?
            .shutdown(&[])
            .map_err(|e| r_error!("Cannot shutdown sandbox: {}", e))
    }

    pub fn register_function(
        &self,
        function: Vec<u8>,
        data_token: DataToken,
        parameters: &[&str],
        return_type: UfoType,
    ) -> Result<FunctionToken> {
        eprintln!("Sandbox::register_function:");
        eprintln!("   self:           {:?}", self);
        // eprintln!("   function:       {:?}", function);
        eprintln!("   data_token:     {:?}", data_token);
        eprintln!("   parameters:     {:?}", parameters);
        eprintln!("   return_type:    {:?}", return_type);

        let function_blob = function.as_slice();
        let parameters = parameters
            .iter()
            .map(|parameter| GenericValue::Vstring(parameter.to_owned()));
        // .collect::<VecDeque<GenericValue<&[u8],&str>>>();

        let associated_data = vec![GenericValue::Token(data_token)]
            .into_iter()
            .chain(parameters)
            .collect::<Vec<GenericValue<&[u8], &str>>>();

        let return_type = GenericValue::Vstring(return_type.as_str());

        let function_token = self
            .lock()?
            .define_function(function_blob, associated_data.as_slice(), &[return_type])
            .map_err(|e| r_error!("Cannot register function in sandbox: {}", e))?
            .value;

        Ok(function_token)
    }

    pub fn register_procedure(
        &self,
        function: Vec<u8>,
        data_token: DataToken,
        parameters: &[&str],
    ) -> Result<FunctionToken> {
        eprintln!("Sandbox::register_procedure:");
        eprintln!("   self:           {:?}", self);
        // eprintln!("   function:       {:?}", function);
        eprintln!("   data_token:     {:?}", data_token);
        eprintln!("   parameters:     {:?}", parameters);

        let function_blob = function.as_slice();
        let parameters = parameters
            .iter()
            .map(|parameter| GenericValue::Vstring(parameter.to_owned()));
        // .collect::<VecDeque<GenericValue<&[u8],&str>>>();

        let associated_data = vec![GenericValue::Token(data_token)]
            .into_iter()
            .chain(parameters)
            .collect::<Vec<GenericValue<&[u8], &str>>>();

        let function_token = self
            .lock()?
            .define_function(function_blob, associated_data.as_slice(), &[])
            .map_err(|e| r_error!("Cannot register function in sandbox: {}", e))?
            .value;

        Ok(function_token)
    }

    pub fn register_data(&self, serialized: Vec<u8>) -> Result<DataToken> {
        eprintln!("Sandbox::register_data:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   serialized:     {:?}", serialized);

        let value = GenericValue::Vbytes(serialized.as_slice());
        let data_token = self
            .lock()?
            .define_data(&[value], &[])
            .map_err(|e| r_error!("Cannot register function in sandbox: {}", e))?
            .value;

        Ok(data_token)
    }

    pub fn call_function(
        &self,
        function_token: FunctionToken,
        arguments: &[GenericValue<&[u8], &str>],
    ) -> Result<Vec<GenericValueBoxed>> {
        eprintln!("Sandbox::call_function:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   function_token: {:?}", function_token);
        eprintln!("   arguments:      {:?}", arguments);

        let result = self
            .lock()?
            .call_function(&function_token, arguments, &[])
            .map_err(|e| {
                r_error!(
                    "Cannot call function {:?} in sandbox: {}",
                    function_token,
                    e
                )
            })?
            .value;

        // let bytes = result.into_iter().exactly_one()
        //     .map_err(|e| r_error!("Invalid return value for function {:?} in sandbox: {}", function_token, e))?
        //     .expect_bytes_into()
        //     .map_err(|e| r_error!("Invalid return value for function {:?} in sandbox: {}", function_token, e))?;

        eprintln!("    result: {result:?}");

        Ok(result)
    }

    pub fn call_procedure(
        &self,
        function_token: FunctionToken,
        arguments: &[GenericValue<&[u8], &str>],
    ) -> Result<()> {
        eprintln!("Sandbox::call_procedure:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   function_token: {:?}", function_token);
        eprintln!("   arguments:      {:?}", arguments);

        let result = self
            .lock()?
            .call_function(&function_token, arguments, &[])
            .map_err(|e| {
                r_error!(
                    "Cannot call function {:?} in sandbox: {}",
                    function_token,
                    e
                )
            })?
            .value;

        r_bail_if!(result.is_empty() => "Invalid return for function {:?} in sandbox: expecting empty result", function_token);

        Ok(())
    }

    pub fn free_function(&self, function_token: &FunctionToken) -> Result<()> {
        eprintln!("Sandbox::free_function:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   function_token: {:?}", function_token);

        self.lock()?
            .free_function(function_token, &[])
            .map_err(|e| r_error!("Cannot free function in sandbox: {}", e))?;

        Ok(())
    }

    pub fn free_data(&self, data_token: &DataToken) -> Result<()> {
        eprintln!("Sandbox::free_data:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   data_token: {:?}", data_token);

        self.lock()?
            .free_data(data_token, &[])
            .map_err(|e| r_error!("Cannot free function in sandbox: {}", e))?;

        Ok(())
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        // self.shutdown().unwrap()
    }
}
