pub mod serder;
pub mod typer;
pub mod allocr;
pub mod sandbox;
pub mod server;
mod errors;

use extendr_api::prelude::*;

use libR_sys::DATAPTR_RO;
use libc::c_void;

use ufo_core::UfoCoreConfig;
use ufo_core::UfoPopulateError;
// use ufo_core::UfoPopulateFn;
use ufo_core::UfoWriteListenerEvent;
use ufo_ipc::DataToken;
use ufo_ipc::GenericValue;
use core::mem::size_of;
use std::path::PathBuf;

use ufo_core::UfoCore;
use ufo_core::UfoObjectParams;

use errors::*;

use server::Server;

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
use errors::*;

#[derive(Clone)]
pub struct UfoSystem {
    core: Arc<UfoCore>,
    sandbox: Sandbox,
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
        eprintln!("UfoSystem::create_ufo:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   prototype:      ...");

        let config = prototype.new_config();
        let ufo = r_or_bail!(self.core.allocate_ufo(config), "Error constructing UFO");
        let locked_ufo = r_or_bail!(ufo.read(), "Error dereferencing newly created UFO");
        let ufo_pointer = locked_ufo.header_ptr();
        Ok(ufo_pointer)
    }

    fn free_ufo(&self, pointer: *mut c_void) -> Result<()> {
        eprintln!("UfoSystem::free_ufo:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   pointer         {:?}", pointer);

        let ufo = self.core.get_ufo_by_address(pointer as usize).rewrap(|| "Error freeing UFO")?;
        let mut locked_ufo = r_or_bail!(ufo.write(), "Error locking UFO during free");
        let wait_group = r_or_bail!(locked_ufo.free(), "Error performing free on UFO");
        Ok(wait_group.wait())
    }
}

#[extendr]
impl UfoSystem {
    pub fn initialize(writeback_path: String, high_watermark: i64, low_watermark: i64) -> Result<Self> {
        eprintln!("UfoSystem::initialize:");
        eprintln!("   writeback_path: {:?}", writeback_path);
        eprintln!("   high_watermark: {:?}", high_watermark);
        eprintln!("   low_watermark:  {:?}", low_watermark);

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

        Ok(UfoSystem{ 
            core: r_or_bail!(UfoCore::new(config), "Cannot start UFO core system"),
            sandbox: Sandbox::start()?,
        })
    }

    pub fn shutdown(&self) {
        eprintln!("UfoSystem::shutdown:");
        eprintln!("   self:           {:?}", self);

        eprintln!("Ufo core is shutting down...");
        self.core.shutdown()
    }

    pub fn new_ufo(&self, mode: &str, length: i64, user_data: Robj, populate: Robj, writeback: Robj, finalizer: Robj, read_only: bool, chunk_length: i64) -> Result<Robj> {       
        eprintln!("UfoSystem::new_ufo:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   mode:           {:?}", mode);
        eprintln!("   length:         {:?}", length);
        eprintln!("   user_data:      {:?}", user_data);
        eprintln!("   populate:       {:?}", populate);
        eprintln!("   writeback:      {:?}", writeback);
        eprintln!("   finalizer:      {:?}", finalizer);
        eprintln!("   read_only:      {:?}", read_only);
        eprintln!("   chunk_length:   {:?}", chunk_length);

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

        let serialized_data = user_data.serialize()?;
        let data_token = self.sandbox.register_data(serialized_data)?;
        
        let serialized_populate = populate.serialize()?;
        let populate_token = 
            self.sandbox.register_function(serialized_populate, data_token)?;

        let serialized_writeback = writeback
            .map(|function| function.serialize())
            .extract_result()?;
        let writeback_token = serialized_writeback.map(|serialized_function| {
                self.sandbox.register_function(serialized_function, data_token)
            }).extract_result()?;

        let serialized_finalizer = finalizer
            .map(|function| function.serialize())
            .extract_result()?;
        let finalizer_token = serialized_finalizer.map(|serialized_function| {
                self.sandbox.register_function(serialized_function, data_token)
            }).extract_result()?;
       
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
            user_data: data_token,         
        }.construct_robj();
    
        Ok(ufo)
    }
}

impl UfoSystem {
    pub fn sandbox_populate(&self, token: FunctionToken, start: usize, end: usize, memory: *mut u8) -> std::result::Result<(), UfoPopulateError> {
        eprintln!("UfoSystem::sandbox_populate:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);
        eprintln!("   start:          {:?}", start);
        eprintln!("   end:            {:?}", end);
        eprintln!("   memory:         {:?}", memory);

        let result = self.sandbox
            .call_function(token, &[GenericValue::Vusize(start), GenericValue::Vusize(end)])
            .map_err(|e| {
                eprintln!("UFO populate error: {}", e);
                UfoPopulateError
            })?;
        unsafe {
            std::ptr::copy_nonoverlapping(result.as_ptr(), memory, result.len());
        }
        Ok(())
    }

    pub fn sandbox_writeback(&self, token: FunctionToken, start: usize, end: usize, memory: *const u8) {
        eprintln!("UfoSystem::sandbox_writeback:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);
        eprintln!("   start:          {:?}", start);
        eprintln!("   end:            {:?}", end);
        eprintln!("   memory:         {:?}", memory);

        let data = GenericValue::Vbytes(unsafe {
            std::slice::from_raw_parts(memory, end - start)
        });
        let start = GenericValue::Vusize(start);
        let end = GenericValue::Vusize(end);
        let event_type = GenericValue::Vstring("writeback");

        let result = self.sandbox
            .call_procedure(token, &[event_type, start, end, data]);

        if let Err(e) = result {
            eprintln!("UFO writeback error: {}", e);
        }
    }

    pub fn sandbox_reset(&self, token: FunctionToken) {
        eprintln!("UfoSystem::sandbox_reset:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);

        let event_type = GenericValue::Vstring("reset");
        let result = self.sandbox
            .call_procedure(token, &[event_type]);
            if let Err(e) = result {
                eprintln!("UFO writeback error (reset): {}", e);
            }
    }

    pub fn sandbox_destroy(&self, token: FunctionToken) {
        eprintln!("UfoSystem::sandbox_destroy:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);

        let event_type = GenericValue::Vstring("destroy");
        let result = self.sandbox
            .call_procedure(token, &[event_type]);
            if let Err(e) = result {
                eprintln!("UFO writeback error (destroy): {}", e);
            }
    }
}

#[derive(Debug)]
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
    user_data: DataToken,    
}

impl UfoDefinition {
    pub fn prototype(&self) -> Result<UfoObjectParams> {
        eprintln!("UfoDefinition::prototype:");
        eprintln!("   self:           {:?}", self);

        r_bail_if!(!self.vector_type.is_vector() => "UFO needs to be a vector type");

        // FIXME I don't feel like dealing with regenerating the entirety of libR-sys just to get this one number.
        //let header_size: usize = size_of::<libR_sys::SEXPREC>(); 
        let header_size = r!(42).header_size();
        r_bail_if!(header_size == 0 => "SEXP header should be non-zero");

        // let allocator_size: usize = size_of::<libR_sys::R_allocator>();
        let allocator_size: usize = size_of::<CustomAllocator>();
        r_bail_if!(allocator_size == 0 => "Custom allocator should be non-zero");        

        // struct R_allocator {
        //     custom_alloc_t mem_alloc; /* malloc equivalent */                                size: 8
        //     custom_free_t  mem_free;  /* free equivalent */                                  size: 8
        //     void *res;                /* reserved (maybe for copy) - must be NULL */         size: 8
        //     void *data;               /* custom data for the allocator implementation */     size: 8
        // };
        assert_eq!(4 * 8, allocator_size); // This is in fact the size of R_allocator_t;    

        eprintln!("Header_size: {}\nallocator_size:{}", header_size, allocator_size);

        let system = self.system.clone();
        let token = self.populate.clone();
        let populate = move |start: usize, end: usize, memory: *mut u8| {
            system.sandbox_populate(token, start, end, memory)
        };

        if let Some(token) = self.writeback {
            let system = self.system.clone();
            let writeback_listener = 
                move |event: UfoWriteListenerEvent|
                    match event {
                        UfoWriteListenerEvent::Writeback { start_idx, end_idx, data } => 
                            system.sandbox_writeback(token, start_idx, end_idx, data),
                        UfoWriteListenerEvent::Reset => 
                            system.sandbox_reset(token),
                        UfoWriteListenerEvent::UfoWBDestroy => 
                            system.sandbox_destroy(token)
                    };
            Ok(UfoObjectParams { 
                header_size: header_size + allocator_size,
                stride: self.vector_type.element_size().unwrap(),
                element_ct: self.length,
                populate: Box::new(populate),
                writeback_listener: Some(Box::new(writeback_listener)),
                min_load_ct: self.chunk_length,
                read_only: self.read_only, 
            })  
        } else {
            Ok(UfoObjectParams { 
                header_size: header_size + allocator_size,
                stride: self.vector_type.element_size().unwrap(),
                element_ct: self.length,
                populate: Box::new(populate),
                writeback_listener: None,            
                min_load_ct: self.chunk_length,
                read_only: self.read_only, 
            })
        }
    }

    pub fn construct_robj(self) -> Robj {
        eprintln!("UfoDefinition::construct_robj:");
        eprintln!("   self:           {:?}", self);

        let sexp_type = self.vector_type.as_sxp() as u32;
        let length = self.length as isize;

        let allocator = 
            Box::into_raw(Box::new(CustomAllocator::from(self))).cast();
             
        unsafe {
            single_threaded(|| {
                Robj::from_sexp(libR_sys::Rf_allocVector3(sexp_type, length, allocator))
            })
        }
    }

    pub fn finalize(&self) -> Result<()> {
        eprintln!("UfoDefinition::finalize:");
        eprintln!("   self:           {:?}", self);

        if let Some(finalizer) = self.finalizer {
            self.system.sandbox.call_procedure(finalizer, &[])?;
            self.system.sandbox.free_function(&finalizer)?;
        }

        if let Some(writeback) = self.writeback {
            self.system.sandbox.free_function(&writeback)?;
        }

        self.system.sandbox.free_function(&self.populate)?;
        self.system.sandbox.free_data(&self.user_data)?;

        Ok(())
    }
}

#[extendr]
pub fn start_sandbox() -> Result<()> {
    Server::new().listen()
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod ufosandbox;
    impl UfoSystem;
    fn start_sandbox;
}

/*
 * suggestions for extendr: 
 *  - Option<T> arguments
 *  - usize arguments
 */