// mod rserialization;
// mod server;

use extendr_api::prelude::*;
use extendr_api::rtype_to_sxp;

use libc::c_void;
use ufo_core::UfoCoreConfig;
// use ufo_core::UfoCore;
use core::mem::size_of;

use ufo_core::UfoCore;
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

macro_rules! r_or_bail {
    ($task:expr, $($s:expr),*$(,)?) => {
        match $task {
            Ok(ok) => ok,
            Err(error) => r_bail!("{}: {}", format!($($s,)+), error),
        }
    }
}

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

#[derive(Clone)]
pub struct UfoSystem {
    core: Arc<UfoCore>
}

impl std::fmt::Debug for UfoSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UfoSystem").field("core", &"...").finish()
    }
}

impl UfoSystem {
    pub fn new(path: impl Into<String>, high_watermark: usize, low_watermark: usize) -> Result<Self> {
        let config = UfoCoreConfig {
            writeback_temp_path: path.into(),
            high_watermark,
            low_watermark,
        };
        let core = r_or_bail!(UfoCore::new(config), "Cannot start UFO core");
        Ok(UfoSystem{ core })
    }

    pub fn shutdown(self) {
        self.core.shutdown()
    }

    pub fn create_ufo(&self, prototype: UfoObjectParams) -> Result<*mut c_void> {
        let config = prototype.new_config();
        let ufo = r_or_bail!(self.core.allocate_ufo(config), "Error constructing UFO");
        let locked_ufo = r_or_bail!(ufo.read(), "Error dereferencing newly created UFO");
        let ufo_pointer = locked_ufo.header_ptr();
        Ok(ufo_pointer)
    }

    pub fn free_ufo(&self, pointer: *mut c_void) -> Result<()> {
        let ufo = r_or_bail!(self.core.get_ufo_by_address(pointer as usize), "Error freeing UFO");
        let mut locked_ufo = r_or_bail!(ufo.write(), "Error locking UFO during free");
        let wait_group = r_or_bail!(locked_ufo.free(), "Error performing free on UFO");
        Ok(wait_group.wait())
    }
}


pub struct UfoDefinition {
    system: Arc<UfoSystem>,
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

macro_rules! try_or_yell_impotently {
    ($stuff:expr) => {
        match ($stuff) {
            Err(e) => {
                eprintln!("Ufo error: {}", e);
            }
            Ok(_) => ()
        }
    };
}

macro_rules! try_or_null {
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
    try_or_null!(definition.system.create_ufo(prototype))    
}

extern fn ufo_free(allocator: *mut CustomAllocator, pointer: *mut libc::c_void) {
    let definition: &UfoDefinition = unsafe { &*(*allocator).data.cast() };
    try_or_yell_impotently!(definition.system.free_ufo(pointer))
}

// pub fn sandboxed_ufo(vector_type: Rtype, length: usize, populate: Robj, writeback: Robj, finalizer: Robj, read_only: bool, chunk_length: Option<usize>) -> Result<Robj> {

// }

/// Create new UFO with custom populate and writeback functions
/// @export
#[extendr]
pub fn new(mode: &str, length: i64, populate: Robj, writeback: Robj, finalizer: Robj, read_only: bool, chunk_length: i64) -> Result<Robj> {
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

    let external_pointer: Robj = r!("rlang::pkg_env(\"ufosandbox\")$.ufo_core");    
    let system = unsafe {
        let ptr = libR_sys::R_ExternalPtrAddr(external_pointer.get()) as *const UfoSystem;
        &*ptr as &UfoSystem
    };

    let ufo = UfoDefinition { 
        system: todo!(), 
        vector_type,
        length, 
        chunk_length, 
        read_only,
    }.construct_robj();

    Ok(ufo)
    // sandboxed_ufo(vector_type, length as usize, populate, writeback, finalizer, read_only, chunk_length)
}

// Robj::make_external_ptr(Box::into_raw(boxed), r!(type_name), r!(()));

#[extendr]
pub fn system_initialize(high_watermark: i64, low_watermark: i64) -> Result<Robj> {

    r_bail_if!(high_watermark <= 0 => 
               "High watermark must be greater than zero (provided value:  {})", 
               high_watermark);

    r_bail_if!(low_watermark <= 0 => 
               "Low watermark must be greater than zero (provided value:  {})", 
               low_watermark);

    r_bail_if!(low_watermark >= high_watermark => 
              "High watermark must be greater than low watermark (currently low={} vs. high={})", 
              low_watermark, high_watermark);

    let system = r_or_bail!(
        UfoSystem::new("/tmp", high_watermark as usize, low_watermark as usize), 
        "Cannot start UFO system"
    );

    Ok(ExternalPtr::new(system).into())
}

pub fn system_finalize() {
    // Grab UfoCore from R
    // Extract from global variable wrapper
    // ufo_core.shutdown
}

struct Person {
    pub name: String,
    pub core: Robj,
}

#[extendr]
impl Person {
    fn new() -> Self {
        Self { name: "".to_string(), core: ExternalPtr::new("".to_string()).into() }
    }

    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod ufosandbox;
    fn new;
    fn system_initialize;
    // fn _ufo_finalize;
    impl Person;
}

/*
 * suggestions for extendr: 
 *  - Option<T> arguments
 *  - usize arguments
 */