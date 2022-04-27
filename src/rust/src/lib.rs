pub mod serder;
pub mod typer;
pub mod allocr;
pub mod sandbox;
pub mod server;
pub mod ufotype;
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
use ufotype::UfoType;
use core::mem::size_of;
use std::path::PathBuf;

use ufo_core::UfoCore;
use ufo_core::UfoObjectParams;

use errors::*;

use server::Server;

const HIGH_WATERMARK_DEFAULT: usize = 100 * 1024 * 1024; //100MB
const LOW_WATERMARK_DEFAULT: usize  = 10  * 1024 * 1024; //10MB

// use ufo_ipc;
// use ufo_ipc::ProtocolCommand;
// use ufo_ipc::FunctionToken;

use std::sync::Arc;

// use libR_sys;

// use libc;

use allocr::*;
use typer::*;
use serder::*;
use sandbox::*;
// use errors::*;

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
        eprintln!("   prototype:      {:?}", prototype);

        let config = prototype.new_config();
        let ufo = self.core.allocate_ufo(config).rewrap(|| "Error constructing UFO")?;
        let locked_ufo = ufo.read().rewrap(|| "Error dereferencing newly created UFO")?;
        let ufo_pointer = locked_ufo.header_ptr();
        Ok(ufo_pointer)
    }

    fn free_ufo(&self, pointer: *mut c_void) -> Result<()> {
        eprintln!("UfoSystem::free_ufo:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   pointer         {:?}", pointer);

        let ufo = self.core.get_ufo_by_address(pointer as usize).rewrap(|| "Error freeing UFO")?;
        let mut locked_ufo = ufo.write().rewrap(|| "Error locking UFO during free")?;        
        let wait_group = locked_ufo.free().rewrap(|| "Error performing free on UFO")?;

        // If lock is not dropped before wait_group.wait(), there's a deadlock.
        std::mem::drop(locked_ufo);
        
        wait_group.wait();

        Ok(())
    }
}

#[extendr]
impl UfoSystem {
    pub fn initialize(writeback_path: String, high_watermark: i64, low_watermark: i64) -> Result<Self> {
        
        println!("Ufo core is initializing...");

        eprintln!("UfoSystem::initialize:");
        eprintln!("   writeback_path: {:?}", writeback_path);
        eprintln!("   high_watermark: {:?}", high_watermark);
        eprintln!("   low_watermark:  {:?}", low_watermark);

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

        r_bail_if!(high_watermark < 0 => 
                   "High watermark must be a positive value (provided value: {})", 
                   high_watermark);
    
        r_bail_if!(low_watermark < 0 => 
                   "Low watermark must be a positive value (provided value: {})", 
                   low_watermark);

        let high_watermark = if high_watermark != 0 { high_watermark as usize } else { HIGH_WATERMARK_DEFAULT };
        let low_watermark = if low_watermark != 0 { low_watermark as usize } else { LOW_WATERMARK_DEFAULT };
    
        r_bail_if!(low_watermark >= high_watermark => 
                  "High watermark must be greater than low watermark (currently low={} vs. high={})", 
                  low_watermark, high_watermark);

        let config = UfoCoreConfig {
            writeback_temp_path: writeback_path,
            high_watermark: high_watermark as usize, 
            low_watermark: low_watermark as usize,
        };

        eprintln!("   config:         {:?}", config);

        Ok(UfoSystem{ 
            core: r_or_bail!(UfoCore::new(config), "Cannot start UFO core system"),
            sandbox: Sandbox::start()?,
        })
    }

    pub fn shutdown(&self) {
        eprintln!("UfoSystem::shutdown:");
        eprintln!("   self:           {:?}", self);

        eprintln!("Ufo sandbox is shuttind down...");
        self.sandbox.shutdown().unwrap();

        eprintln!("Ufo core is shutting down...");       
        self.core.shutdown()

    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_ufo(&self, mode: &str, length: i64, user_data: Robj, populate: Robj, writeback: Robj, reset: Robj, destroy: Robj, finalizer: Robj, read_only: bool, chunk_length: i64) -> Result<Robj> {       
        eprintln!("UfoSystem::new_ufo:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   mode:           {:?}", mode);
        eprintln!("   length:         {:?}", length);
        eprintln!("   user_data:      {:?}", user_data);
        eprintln!("   populate:       {:?}", populate);
        eprintln!("   writeback:      {:?}", writeback);
        eprintln!("   reset:          {:?}", reset);
        eprintln!("   destroy:        {:?}", destroy);
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

        r_bail_if!(reset.rtype() != Rtype::Function && reset.rtype() != Rtype::Null => 
                   "Expecting reset to be a function or NULL, but it is {:?}", reset.rtype());
        let reset = reset.as_function();

        r_bail_if!(destroy.rtype() != Rtype::Function && destroy.rtype() != Rtype::Null => 
                   "Expecting destroy to be a function or NULL, but it is {:?}", destroy.rtype());
        let destroy = destroy.as_function();

        r_bail_if!(finalizer.rtype() != Rtype::Function && finalizer.rtype() != Rtype::Null => 
                   "Expecting finalizer to be a function or NULL, but it is {:?}", finalizer.rtype());
        let finalizer = finalizer.as_function();
    
        let chunk_length = if chunk_length <= 0 { None } else { Some(chunk_length as usize) };
    
        let vector_type = match  mode.to_lowercase().as_str() {
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
        let return_type = UfoType::try_from(mode)
            .rewrap(|| "Cannot derive a UFO return type from mode: {}")?;
        
        let serialized_populate = populate.serialize()?;
        let populate_token = 
            self.sandbox.register_function(serialized_populate, data_token, &["start", "end"], return_type)?;

        let serialized_writeback = writeback
            .map(|function| function.serialize())
            .extract_result()?;
        let writeback_token = serialized_writeback.map(|serialized_function| {
                self.sandbox.register_procedure(serialized_function, data_token, &["start", "end", "data"])
            }).extract_result()?;

        let serialized_reset = reset
            .map(|function| function.serialize())
            .extract_result()?;
        let reset_token = serialized_reset.map(|serialized_function| {
                self.sandbox.register_procedure(serialized_function, data_token, &[])
            }).extract_result()?;
            
        let serialized_destroy = destroy
            .map(|function| function.serialize())
            .extract_result()?;
        let destroy_token = serialized_destroy.map(|serialized_function| {
                self.sandbox.register_procedure(serialized_function, data_token, &[])
            }).extract_result()?;   

        let serialized_finalizer = finalizer
            .map(|function| function.serialize())
            .extract_result()?;
        let finalizer_token = serialized_finalizer.map(|serialized_function| {
                self.sandbox.register_procedure(serialized_function, data_token, &[])
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
            reset: reset_token,
            destroy: destroy_token,
            finalizer: finalizer_token,   
            user_data: data_token,         
        }.construct_robj();
    
        Ok(ufo)
    }
}

impl UfoSystem {
    /// # Safety
    /// Function operates on raw pointer to area of memory. The contents are written to from a deserialized byte vector retrieved from sandbox.
    pub unsafe fn sandbox_populate(&self, token: FunctionToken, start: usize, end: usize, memory: *mut u8) -> std::result::Result<(), UfoPopulateError> {
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

        std::ptr::copy_nonoverlapping(result.as_ptr(), memory, result.len());        

        Ok(())
    }

    /// # Safety
    /// Function operates on raw pointer to area of memory. The contents are serialized and sent to sandbox.
    pub unsafe fn sandbox_writeback(&self, token: FunctionToken, start: usize, end: usize, memory: *const u8) {
        eprintln!("UfoSystem::sandbox_writeback:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);
        eprintln!("   start:          {:?}", start);
        eprintln!("   end:            {:?}", end);
        eprintln!("   memory:         {:?}", memory);

        let data = GenericValue::Vbytes(std::slice::from_raw_parts(memory, end - start));
        let start = GenericValue::Vusize(start);
        let end = GenericValue::Vusize(end);
        // let event_type = GenericValue::Vstring("writeback");

        let result = self.sandbox
            .call_procedure(token, &[/*event_type, */ start, end, data]);

        if let Err(e) = result {
            eprintln!("UFO writeback error: {}", e);
        }
    }

    pub fn sandbox_reset(&self, token: FunctionToken) {
        eprintln!("UfoSystem::sandbox_reset:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);

        // let event_type = GenericValue::Vstring("reset");
        let result = self.sandbox
            .call_procedure(token, &[/*event_type*/]);
            if let Err(e) = result {
                eprintln!("UFO writeback error (reset): {}", e);
            }
    }

    pub fn sandbox_destroy(&self, token: FunctionToken) {
        eprintln!("UfoSystem::sandbox_destroy:");
        eprintln!("   self:           {:?}", self);
        eprintln!("   token:          {:?}", token);

        // let event_type = GenericValue::Vstring("destroy");
        let result = self.sandbox
            .call_procedure(token, &[/*event_type*/]);
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
    chunk_length: Option<usize>,   // in #elements
    read_only: bool,
    requested_size: Option<usize>, // in B - the exact amount of B R is asking for, filled in upon request
    populate: FunctionToken,
    writeback: Option<FunctionToken>,
    reset: Option<FunctionToken>,
    destroy: Option<FunctionToken>,
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
        let token = self.populate;
        // let return_type =  UfoType::try_from(&self.vector_type)?;

        let populate = move |start: usize, end: usize, memory: *mut u8| {
            unsafe { system.sandbox_populate(token, start, end, memory) }
        };

        let min_load_ct = self.chunk_length.and_then(|elements| if elements != 0 {
            self.vector_type.element_size().map(|size| elements * size)
        } else {
            None
        });

        let ufo_object_params = if self.writeback.is_some() || self.reset.is_some() || self.destroy.is_some() {
            let system = self.system.clone();
            let writeback = self.writeback;
            let reset = self.reset;
            let destroy = self.destroy;

            let writeback_listener = 
                move |event: UfoWriteListenerEvent|
                    match event {
                        UfoWriteListenerEvent::Writeback { start_idx, end_idx, data } => 
                            writeback.map(|token| unsafe {
                                system.sandbox_writeback(token, start_idx, end_idx, data)
                            }),
                        UfoWriteListenerEvent::Reset => 
                            reset.map(|token| system.sandbox_reset(token)),
                        UfoWriteListenerEvent::UfoWBDestroy => 
                            destroy.map(|token| system.sandbox_destroy(token)),
                    }.unwrap_or(());

            UfoObjectParams { 
                header_size: header_size + allocator_size,
                stride: self.vector_type.element_size().unwrap(),
                element_ct: self.length,
                populate: Box::new(populate),
                writeback_listener: Some(Box::new(writeback_listener)),
                min_load_ct,
                read_only: self.read_only, 
            }
        } else {
            UfoObjectParams { 
                header_size: header_size + allocator_size,
                stride: self.vector_type.element_size().unwrap(),
                element_ct: self.length,
                populate: Box::new(populate),
                writeback_listener: None,            
                min_load_ct,
                read_only: self.read_only, 
            }
        };

        Ok(ufo_object_params)
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