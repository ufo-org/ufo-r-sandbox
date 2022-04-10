use std::process::Command;
use std::sync::{Mutex, Arc, MutexGuard};

use itertools::Itertools;

use ufo_ipc;
use ufo_ipc::StartSubordinateProcess;
use ufo_ipc::ControllerProcess;
use ufo_ipc::GenericValue;
use ufo_ipc::DataToken;

pub use ufo_ipc::FunctionToken;

use extendr_api::*;

use crate::r_error;

#[derive(Clone)]
pub struct Sandbox { process: Arc<Mutex<ControllerProcess>> }

impl Sandbox {
    fn lock(&self) -> Result<MutexGuard<ControllerProcess>> {
        self.process.deref().lock().map_err(|e| {
            r_error!("Cannot lock sandbox process: {}", e)
        })
    }

    pub fn start() -> Result<Self> {
        let child = Command::new("R")
            .args(&["--vanilla", "--no-restore", "-e", "ufosandbox:::start_sandbox()"])
            .start_subordinate_process()
            .map_err(|e| r_error!("Cannot start sandbox: {}", e))?;        

        Ok(Sandbox { process: Arc::new(Mutex::new(child)) })
    }

    pub fn shutdown(&self) -> Result<()> {
        self.lock()?.shutdown(&[])
            .map_err(|e| r_error!("Cannot shutdown sandbox: {}", e))
    }

    pub fn register_function(&self, function: Vec<u8>, data_token: DataToken) -> Result<FunctionToken> {
        let function_blob = function.as_slice();
        let associated_data = GenericValue::Token(data_token);
        let function_token = self.lock()?
            .define_function(function_blob, &[associated_data], &[])
            .map_err(|e| r_error!("Cannot register function in sandbox: {}", e))?
            .value;

        Ok(function_token)
    }

    pub fn register_data(&self, serialized: Vec<u8>) -> Result<DataToken> {
        let value = GenericValue::Vbytes(serialized.as_slice());
        let data_token = self.lock()?
            .define_data(&[value], &[])
            .map_err(|e| r_error!("Cannot register function in sandbox: {}", e))?
            .value;

        Ok(data_token)
    }

    pub fn call_function(&self, function_token: FunctionToken) -> Result<Vec<u8>> {
        let result = self.lock()?
            .call_function(&function_token, &[], &[])
            .map_err(|e| r_error!("Cannot call function {:?} in sandbox: {}", function_token, e))?
            .value;

        let value = result.into_iter().exactly_one()
            .map_err(|e| r_error!("Invalid return value for function {:?} in sandbox: {}", function_token, e))?
            .expect_bytes_into()
            .map_err(|e| r_error!("Invalid return value for function {:?} in sandbox: {}", function_token, e))?;

        Ok(value)
    }

    pub fn free_function(&self, function_token: &FunctionToken) -> Result<()> {
        self.lock()?
            .free_function(function_token, &[])
            .map_err(|e| r_error!("Cannot free function in sandbox: {}", e))?;

        Ok(())
    }

    pub fn free_data(&self, data_token: &DataToken) -> Result<()> {
        self.lock()?
            .free_data(data_token, &[])
            .map_err(|e| r_error!("Cannot free function in sandbox: {}", e))?;

        Ok(())
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        self.shutdown().unwrap()
    }
}