// mod rserialization;
// mod server;

use extendr_api::prelude::*;

use extendr_api::rtype_to_sxp;
use libR_sys::R_allocator;
use libR_sys::R_allocator_t;
use libc::c_void;

use ufo::*;
use ufo_core::UfoObjectParams;

// use server::Server;
use ufo_ipc;
use ufo_ipc::ProtocolCommand;
use ufo_ipc::FunctionToken;

use std::collections::HashMap;
use std::sync::Arc;
// use std::lazy::Lazy;

use libR_sys;

use libc;


// static UFO_CORE: Lazy<Vec<i32>> = Lazy::new(|| {
//     Vec::new()
// });


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

pub struct UfoDefinition {
    core: Arc<UfoCore>,
    vector_type: Rtype,
    length: usize,
    element_size: usize,
}

impl UfoDefinition {
    pub fn prototype(&self) -> UfoObjectParams {
        todo!()
    }
}

// typedef void *(*custom_alloc_t)(R_allocator_t *allocator, size_t);
// typedef void  (*custom_free_t)(R_allocator_t *allocator, void *);
// extern "C" {
//     type FuckAlloc = u32;
//     type FuckFree = u32;
// }

type MemAlloc = extern fn(*mut FuckAllocator, libc::size_t) -> *mut libc::c_void;
type MemFree = extern fn(*mut FuckAllocator, *mut libc::c_void);

#[repr(C)]
struct FuckAllocator {
    mem_alloc: MemAlloc,
    mem_free: MemFree,
    res: *mut libc::c_void,
    data: *mut libc::c_void,
}

impl From<UfoDefinition> for FuckAllocator {
    fn from(definition: UfoDefinition) -> Self {
        FuckAllocator {
            mem_alloc: ufo_alloc,
            mem_free: ufo_free,
            res: std::ptr::null_mut(),
            data: Box::into_raw(Box::new(definition)).cast(),
        }
    }
}

macro_rules!  try_or_null {
    ($stuff:expr) => {
        match ($stuff) {
            Err(e) => {
                eprintln!("Ufo error: {}", e);
                return std::ptr::null_mut();
            }
            Ok(result) => result
        }
    };
}

extern fn ufo_alloc(allocator: *mut FuckAllocator, size: libc::size_t) -> *mut libc::c_void {
    let definition: &UfoDefinition = unsafe { &*(*allocator).data.cast() };    
    let ufo = try_or_null!(definition.core.new_ufo(definition.prototype()));
    try_or_null!(ufo.header_ptr())
}

extern fn ufo_free(allocator: *mut FuckAllocator, object: *mut libc::c_void) {
    let definition: *mut UfoDefinition = unsafe { (*allocator).data.cast() };
}

fn construct_ufo(definition: UfoDefinition) -> Robj {
    let sexp_type = todo!(); //rtype_to_sxp(definition.vector_type) as u32; //rtype_as_sexptype(definition.vector_type);
    let sexp_length = todo!(); //definition.length as isize;

    let allocator = Box::into_raw(Box::new(FuckAllocator::from(definition)));
    let allocator: *mut R_allocator = allocator.cast();    
    
    unsafe {
        single_threaded(|| {
            Robj::from_sexp(libR_sys::Rf_allocVector3(sexp_type, sexp_length, allocator))
        })
    }
}

/// Create new UFO with custom populate and writeback functions
/// @export
#[extendr]
fn ufo_new(populate: Robj, writeback: Robj, finalizer: Robj) -> Robj {
    assert_eq!(populate.rtype(), Rtype::Function, "Expecting populate to be a function, but it is {:?}", populate.rtype());
    assert_eq!(writeback.rtype(), Rtype::Function, "Expecting populate to be a function, but it is {:?}", populate.rtype());

    // let function = call!("unserialize", serialized_function).unwrap();
    // function.call(pairlist!(42)).unwrap()

    let definition = todo!();

    construct_ufo(definition)    
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod ufosandbox;
    fn ufo_new;
}