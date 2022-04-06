pub mod serder;
pub mod typer;
pub mod allocr;
pub mod sandbox;

use extendr_api::prelude::*;

use libc::c_void;
use ufo_core::UfoCoreConfig;
use core::mem::size_of;
use std::path::PathBuf;

use ufo_core::UfoCore;
use ufo_core::UfoObjectParams;

// use ufo_ipc;
// use ufo_ipc::ProtocolCommand;
// use ufo_ipc::FunctionToken;

use std::sync::Arc;

use libR_sys;

use libc;

use allocr::*;
use typer::*;
use serder::*;
use sandbox::*;

#[macro_export]
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

#[derive(Clone)]
pub struct UfoSystem {
    core: Arc<UfoCore>,
    sandbox: Arc<Sandbox>,
}

trait RUnfriendlyAPI: Sized {
    fn create_ufo(&self, prototype: UfoObjectParams) -> Result<*mut c_void>;
    fn free_ufo(&self, pointer: *mut c_void) -> Result<()>;
}

impl std::fmt::Debug for UfoSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UfoSystem").field("core", &"...").finish()
    }
}

impl RUnfriendlyAPI for UfoSystem {
    fn create_ufo(&self, prototype: UfoObjectParams) -> Result<*mut c_void> {
        let config = prototype.new_config();
        let ufo = r_or_bail!(self.core.allocate_ufo(config), "Error constructing UFO");
        let locked_ufo = r_or_bail!(ufo.read(), "Error dereferencing newly created UFO");
        let ufo_pointer = locked_ufo.header_ptr();
        Ok(ufo_pointer)
    }

    fn free_ufo(&self, pointer: *mut c_void) -> Result<()> {
        let ufo = r_or_bail!(self.core.get_ufo_by_address(pointer as usize), "Error freeing UFO");
        let mut locked_ufo = r_or_bail!(ufo.write(), "Error locking UFO during free");
        let wait_group = r_or_bail!(locked_ufo.free(), "Error performing free on UFO");
        Ok(wait_group.wait())
    }
}

#[extendr]
impl UfoSystem {
    pub fn initialize(writeback_path: String, high_watermark: i64, low_watermark: i64) -> Result<Self> {

        println!("Ufo core is initializing...");

        let directory = PathBuf::from(writeback_path.as_str());
        r_bail_if!(!directory.exists() =>
                   "Specified writeback path {} does not exist", 
                   writeback_path);

        r_bail_if!(!directory.is_dir() =>
                   "Specified writeback path {} is not a directory", 
                   writeback_path);                   

        let meta = r_or_bail!(std::fs::metadata(directory), 
                    "Cannot access permissions for writeback path {}", 
                    writeback_path);

        r_bail_if!(meta.permissions().readonly() =>
                   "Specified writeback path {} is not writeable", 
                   writeback_path);

        r_bail_if!(high_watermark <= 0 => 
                   "High watermark must be greater than zero (provided value: {})", 
                   high_watermark);
    
        r_bail_if!(low_watermark <= 0 => 
                   "Low watermark must be greater than zero (provided value: {})", 
                   low_watermark);
    
        r_bail_if!(low_watermark >= high_watermark => 
                  "High watermark must be greater than low watermark (currently low={} vs. high={})", 
                  low_watermark, high_watermark);

        let config = UfoCoreConfig {
            writeback_temp_path: writeback_path,
            high_watermark: high_watermark as usize, 
            low_watermark: low_watermark as usize,
        };

        let sandbox = Sandbox::new();

        Ok(UfoSystem{ 
            core: r_or_bail!(UfoCore::new(config), "Cannot start UFO core system"),
            sandbox: Arc::new(sandbox),
        })
    }

    pub fn shutdown(&self) {
        eprintln!("Ufo core is shutting down...");
        self.core.shutdown()
    }

    pub fn new_ufo(&self, mode: &str, length: i64, populate: Robj, writeback: Robj, finalizer: Robj, read_only: bool, chunk_length: i64) -> Result<Robj> {
        r_bail_if!(length < 0 => "Attempting to create UFO with negative length {}", length);
        r_bail_if!(length == 0 => "Cannot create an empty UFO");
        r_bail_if!(length == 1 => "Cannot create a scalar UFO");
        let length = length as usize;
    
        r_bail_if!(populate.rtype() != Rtype::Function => 
                   "Expecting populate to be a function, but it is {:?}", populate.rtype());
        let populate = populate.as_function().unwrap();

        r_bail_if!(writeback.rtype() != Rtype::Function && writeback.rtype() != Rtype::Null => 
                   "Expecting writeback to be a function or NULL, but it is {:?}", writeback.rtype());
        let writeback = writeback.as_function();

        r_bail_if!(finalizer.rtype() != Rtype::Function && finalizer.rtype() != Rtype::Null => 
                   "Expecting finalizer to be a function or NULL, but it is {:?}", finalizer.rtype());
        let finalizer = finalizer.as_function();
    
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

        let populate_token = 
            self.sandbox.register_function(populate.serialize()?)?;

        let writeback_token = if let Some(writeback) = writeback {
            Some(self.sandbox.register_function(writeback.serialize()?)?)
        } else {
            None
        };

        let finalizer_token = if let Some(finalizer) = finalizer {
            Some(self.sandbox.register_function(finalizer.serialize()?)?)
        } else {
            None
        };
       
        let ufo = UfoDefinition { 
            system: self.clone(), // Cloning just involves copying a reference via an arc wrapped in UfoSystem.
            vector_type,
            length, 
            chunk_length, 
            read_only,
            requested_size: None,
            populate: populate_token,
            writeback: writeback_token,
            finalizer: finalizer_token,            
        }.construct_robj();
    
        Ok(ufo)
    }
}

pub struct UfoDefinition {
    system: UfoSystem,
    vector_type: Rtype,
    length: usize,                 // in #elements
    chunk_length: Option<usize>,   // in B
    read_only: bool,
    requested_size: Option<usize>, // in B - the exact amount of B R is asking for, filled in upon request
    populate: FunctionToken,
    writeback: Option<FunctionToken>,
    finalizer: Option<FunctionToken>,
    // sandbox_data: DataToken,
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

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod ufosandbox;
    impl UfoSystem;
}

/*
 * suggestions for extendr: 
 *  - Option<T> arguments
 *  - usize arguments
 */