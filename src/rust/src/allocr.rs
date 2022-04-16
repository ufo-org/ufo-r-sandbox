use crate::*;

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

pub type MemAlloc = extern fn(*mut CustomAllocator, libc::size_t) -> *mut libc::c_void;
pub type MemFree = extern fn(*mut CustomAllocator, *mut libc::c_void);

#[repr(C)]
pub struct CustomAllocator {
    pub mem_alloc: MemAlloc,
    pub mem_free: MemFree,
    pub res: *mut libc::c_void,
    pub data: *mut libc::c_void,
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

extern fn ufo_alloc(allocator: *mut CustomAllocator, size: libc::size_t) -> *mut libc::c_void {
    let definition: &mut UfoDefinition = unsafe { &mut *(*allocator).data.cast() }; 
    definition.requested_size = Some(size as usize);
    let prototype = try_or_null!(definition.prototype());
    try_or_null!(definition.system.create_ufo(prototype))    
}

extern fn ufo_free(allocator: *mut CustomAllocator, pointer: *mut libc::c_void) {
    let definition: &UfoDefinition = unsafe { &*(*allocator).data.cast() };
    try_or_yell_impotently!(definition.finalize());
    try_or_yell_impotently!(definition.system.free_ufo(pointer));
}