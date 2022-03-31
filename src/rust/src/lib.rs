// mod rserialization;
// mod server;

use extendr_api::prelude::*;
use extendr_api::rtype_to_sxp;

use libc::NTF_SELF;
use libc::c_void;
use core::mem::size_of;
use std::ops::Add;

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

#[macro_export]
macro_rules! r_bail_if {
    ($cond:expr => $($s:expr),+) => {
        if $cond {
            return Err(r_error!($($s,)+))
        }
    };    
}

trait RtypeTools: Sized {
    fn copy(&self) -> Self;
    fn is_vector(&self) -> bool;
    fn as_sxp(&self) -> i32;
    fn element_size(&self) -> Option<usize>;
    // fn from_mode(mode: &str) -> Option<Self>;
}

impl RtypeTools for Rtype {
    fn copy(&self) -> Self {
        match self {
            Rtype::Null => Rtype::Null,
            Rtype::Symbol => Rtype::Symbol,
            Rtype::Pairlist => Rtype::Pairlist,
            Rtype::Function => Rtype::Function,
            Rtype::Environment => Rtype::Environment,
            Rtype::Promise => Rtype::Promise,
            Rtype::Language => Rtype::Language,
            Rtype::Special => Rtype::Special,
            Rtype::Builtin => Rtype::Builtin,
            Rtype::Rstr => Rtype::Rstr,
            Rtype::Logicals => Rtype::Logicals,
            Rtype::Integers => Rtype::Integers,
            Rtype::Doubles => Rtype::Doubles,
            Rtype::Complexes => Rtype::Complexes,
            Rtype::Strings => Rtype::Strings,
            Rtype::Dot => Rtype::Dot,
            Rtype::Any => Rtype::Any,
            Rtype::List => Rtype::List,
            Rtype::Expressions => Rtype::Expressions,
            Rtype::Bytecode => Rtype::Bytecode,
            Rtype::ExternalPtr => Rtype::ExternalPtr,
            Rtype::WeakRef => Rtype::WeakRef,
            Rtype::Raw => Rtype::Raw,
            Rtype::S4 => Rtype::S4,
            Rtype::Unknown => Rtype::Unknown,
        }
    }

    fn is_vector(&self) -> bool {
        match self {
            Rtype::Rstr => true,
            Rtype::Logicals => true,
            Rtype::Integers => true,
            Rtype::Doubles => true,
            Rtype::Complexes => true,
            Rtype::Strings => true,
            Rtype::List => true,
            Rtype::Raw => true,
            _ => false,
        }
    }

    fn as_sxp(&self) -> i32 {
        rtype_to_sxp(self.copy())
    }

    fn element_size(&self) -> Option<usize> {
        match self {
            Rtype::Rstr => Some(size_of::<libR_sys::Rbyte>()),
            Rtype::Logicals => Some(size_of::<libR_sys::Rboolean>()),
            Rtype::Integers => Some(size_of::<i32>()),
            Rtype::Doubles => Some(size_of::<f64>()),
            Rtype::Complexes => Some(size_of::<libR_sys::Rcomplex>()),
            Rtype::Strings => Some(size_of::<libR_sys::SEXP>()),
            Rtype::List => Some(size_of::<libR_sys::SEXP>()),
            Rtype::Raw => Some(size_of::<libR_sys::Rbyte>()),
            _ => None,
        }
    }       
}

pub struct Sandbox {

}

impl Sandbox {
    pub fn register_populate_function() {}
    pub fn register_writeback_function() {}
    pub fn register_finalizer_function() {}
}

pub struct UfoDefinition {
    core: Arc<UfoCore>,
    vector_type: Rtype,
    length: usize,              // in #elements
    chunk_length: Option<usize>,  // in B
    read_only: bool,
}

impl UfoDefinition {
    pub fn prototype(&self) -> Result<UfoObjectParams> {
        r_bail_if!(!self.vector_type.is_vector() => "UFO needs to be a vector type");

        let header_size: usize = size_of::<libR_sys::SEXPREC>();
        r_bail_if!(header_size > 0 => "SEXP header should be non-zero");

        let allocator_size: usize = size_of::<libR_sys::R_allocator>();
        r_bail_if!(allocator_size > 0 => "Custom allocator should be non-zero");

        eprintln!("Header_size: {}\nallocator_size:{}", header_size, allocator_size);

        Ok(UfoObjectParams { 
            header_size: header_size + allocator_size,
            stride: self.vector_type.element_size().unwrap(),
            element_ct: self.length,

            populate: todo!(), 
            writeback_listener: todo!(), 
            
            min_load_ct: self.chunk_length, 
            read_only: self.read_only, 
        })
    }

    pub fn construct_robj(self) -> Robj {
        let sexp_type = self.vector_type.as_sxp() as u32;
        let sexp_length = self.length as isize;

        let allocator = Box::into_raw(Box::new(CustomAllocator::from(self)));
        let allocator: *mut libR_sys::R_allocator = allocator.cast();    
        
        unsafe {
            single_threaded(|| {
                Robj::from_sexp(libR_sys::Rf_allocVector3(sexp_type, sexp_length, allocator))
            })
        }
    }

}

type MemAlloc = extern fn(*mut CustomAllocator, libc::size_t) -> *mut libc::c_void;
type MemFree = extern fn(*mut CustomAllocator, *mut libc::c_void);

#[repr(C)]
struct CustomAllocator {
    mem_alloc: MemAlloc,
    mem_free: MemFree,
    res: *mut libc::c_void,
    data: *mut libc::c_void,
}

impl From<UfoDefinition> for CustomAllocator {
    fn from(definition: UfoDefinition) -> Self {
        CustomAllocator {
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

extern fn ufo_alloc(allocator: *mut CustomAllocator, size: libc::size_t) -> *mut libc::c_void {
    let definition: &UfoDefinition = unsafe { &*(*allocator).data.cast() };    
    let prototype = try_or_null!(definition.prototype());
    let ufo = try_or_null!(definition.core.new_ufo(prototype));
    try_or_null!(ufo.header_ptr())
}

extern fn ufo_free(allocator: *mut CustomAllocator, object: *mut libc::c_void) {
    let definition: *mut UfoDefinition = unsafe { (*allocator).data.cast() };
}

// pub fn sandboxed_ufo(vector_type: Rtype, length: usize, populate: Robj, writeback: Robj, finalizer: Robj, read_only: bool, chunk_length: Option<usize>) -> Result<Robj> {

// }

/// Create new UFO with custom populate and writeback functions
/// @export
#[extendr]
fn _new(mode: &str, length: i64, populate: Robj, writeback: Robj, finalizer: Robj, read_only: bool, chunk_length: i64) -> Result<Robj> {
    r_bail_if!(length < 0 => "Attempting to create UFO with negative length {}", length);
    r_bail_if!(length == 0 => "Cannot create an empty UFO");
    r_bail_if!(length == 1 => "Cannot create a scalar UFO");
    let length = length as usize;

    r_bail_if!(populate.rtype() != Rtype::Function => 
               "Expecting populate to be a function, but it is {:?}", populate.rtype());
    r_bail_if!(writeback.rtype() != Rtype::Function && writeback.rtype() != Rtype::Null => 
               "Expecting writeback to be a function or NULL, but it is {:?}", writeback.rtype());
    r_bail_if!(finalizer.rtype() != Rtype::Function && finalizer.rtype() != Rtype::Null => 
               "Expecting finalizer to be a function or NULL, but it is {:?}", finalizer.rtype());

    let chunk_length = if chunk_length <= 0 { None } else { Some(chunk_length as usize) };

    let vector_type = match mode.to_lowercase().as_str() {
        "vector" => Rtype::List,
        "numeric" | "double" => Rtype::Doubles,
        "integer" => Rtype::Integers,
        "logical" => Rtype::Logicals,
        "character" => Rtype::Strings,
        "complex" => Rtype::Complexes,
        "raw" => Rtype::Raw,
        other => r_bail!("Cannot create a UFO with mode: {}", other),
    };

    let core = r!("rlang::pkg_env(\"ufosandbox\")$.ufo_core");

    // let function = call!("unserialize", serialized_function).unwrap();
    // function.call(pairlist!(42)).unwrap()
    let ufo = UfoDefinition { 
        core, 
        vector_type,
        length, 
        chunk_length, 
        read_only,
    }.construct_robj();

    Ok(ufo)
    // sandboxed_ufo(vector_type, length as usize, populate, writeback, finalizer, read_only, chunk_length)
}

#[extendr]
fn _ufo_initialize(high_watermark: i64, low_watermark: i64) -> Robj {
    r!("NULL")
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod ufosandbox;
    fn _new;
    fn _ufo_initialize;
}

/*
 * suggestions for extendr: 
 *  - Option<T> arguments
 *  - usize arguments
 */