use ufo_ipc;
use ufo_ipc::ProtocolCommand;
pub use ufo_ipc::FunctionToken;

use extendr_api::*;

// pub struct SandboxedFunction {
//     function_id: FunctionToken,
// }
pub struct DataToken(u64);

pub struct Sandbox {

}

impl Sandbox {
    pub fn new() -> Self {
        Sandbox {}
    }
    pub fn register_function(&self, _serialized: Vec<u8>) -> Result<FunctionToken> {
        todo!()
    }
    pub fn register_data(&self, _serialized: Vec<u8>) -> Result<DataToken> {
        todo!()
    }
    // pub fn register_writeback_function(&self, serialized: Vec<u8>) -> SandboxedFunction {
    //     todo!()
    // }
    // pub fn register_finalizer_function(&self, serialized: Vec<u8>) -> SandboxedFunction {
    //     todo!()
    // }
}