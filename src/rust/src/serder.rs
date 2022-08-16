use std::ptr::copy_nonoverlapping;
use std::string;

// use crate::r_bail;
// use crate::r_or_bail;
use crate::r_error;
use crate::r_err;
use crate::r_bail_if;
use crate::typer::RtypeTools;

use extendr_api::*;
use libR_sys::DATAPTR;
// use libR_sys::INTEGER;
// use libR_sys::REAL;
// use libR_sys::RAW;
// use libR_sys::LOGICAL;
use libR_sys::SEXPREC;
use libR_sys::XLENGTH;
use libR_sys::Rf_mkChar;
use libR_sys::Rf_protect;
use libR_sys::Rf_unprotect;
use libR_sys::Rf_allocVector;
use libR_sys::SET_STRING_ELT;
use libR_sys::SEXPTYPE;
use libc::c_void;
use ufo_ipc::GenericValue;

pub trait SerdeR {
    fn serialize(self) -> Result<Vec<u8>>;
}

impl SerdeR for Function {
    fn serialize(self) -> Result<Vec<u8>> {
        self.as_robj().serialize()
    }
}

impl SerdeR for &Robj {
    fn serialize(self) -> Result<Vec<u8>> {
        let vector = call!("serialize", object = self, connection = r!(NULL))?;
        r_bail_if!(
            vector.rtype() != Rtype::Raw =>
            "Cannot serialize object: \
                expecting to produce an R object of type {:?}
                but produced {:?}. \
                Serialized form: {:?}", 
            Rtype::Raw, vector, self);
        let raw = vector.as_raw().unwrap();        
        Ok(Vec::from(raw.as_slice()))
    }
}

pub trait DeserdeR: Sized {
    fn deserialize(self) -> Result<Robj>;
    fn deserialize_into(self, expected_type: Rtype) -> Result<Robj> {
        let object: Robj = self.deserialize()?;
        if object.rtype() != expected_type {
            r_err!(
                "Cannot deserialize object: \
                   expecting R object of type {:?}, \
                   but encountered {:?}. \
                   Serialized form: {:?}",
                expected_type, object.rtype(), object)
        } else {
            Ok(object)
        }
    }
}

impl DeserdeR for &[u8] {
    fn deserialize(self) -> Result<Robj> {
        eprintln!("Calling unserializer on {:?}{}", self.iter().take(10).collect::<Vec<&u8>>(), if self.len() > 10 { ".." } else { "" });
        call!("unserialize", self)
    }
}

impl DeserdeR for &Vec<u8> {
    fn deserialize(self) -> Result<Robj> {
        eprintln!("Calling unserializer on {:?}{}", self.iter().take(10).collect::<Vec<&u8>>(), if self.len() > 10 { ".." } else { "" });
        eprintln!("Calling unserializer on {self:?}");
        call!("unserialize", self)
    }
}

impl DeserdeR for Vec<u8> {
    fn deserialize(self) -> Result<Robj> {
        eprintln!("Calling unserializer on {:?}{}", self.iter().take(10).collect::<Vec<&u8>>(), if self.len() > 10 { ".." } else { "" });
        call!("unserialize", self)
    }
}

impl<Tv, Ts> DeserdeR for GenericValue<Tv, Ts> where Tv: DeserdeR, Ts: ToVectorValue {
    fn deserialize(self) -> Result<Robj> {
        Ok(match self {
            GenericValue::Vu8(value) => Robj::from(value),
            GenericValue::Vi8(value) => Robj::from(value),
            GenericValue::Vu16(value) => Robj::from(value),
            GenericValue::Vi16(value) => Robj::from(value),
            GenericValue::Vu32(value) => Robj::from(value),
            GenericValue::Vi32(value) => Robj::from(value),
            GenericValue::Vu64(value) => Robj::from(value),
            GenericValue::Vi64(value) => Robj::from(value),
            GenericValue::Vf32(value) => Robj::from(value),
            GenericValue::Vf64(value) => Robj::from(value),
            GenericValue::Vusize(value) => Robj::from(value),
            GenericValue::Visize(value) => Robj::from(value as i64),
            GenericValue::Vbool(value) => Robj::from(value),
            GenericValue::Vstring(value) => Robj::from(value),
            GenericValue::Vbytes(value) => value.deserialize()?,
            GenericValue::Token(value) => Robj::from(value.0),
            GenericValue::Marker(value) => Robj::from(value),
        })
    }
}


impl<Tv, Ts> SmartDeserialization for GenericValue<Tv, Ts> where Tv: SmartDeserialization , Ts: AsRef<str> {
    fn smart_deserialize(&self) -> Result<Robj> {
        Ok(match self {
            GenericValue::Vu8(value) => Robj::from(value),
            GenericValue::Vi8(value) => Robj::from(value),
            GenericValue::Vu16(value) => Robj::from(value),
            GenericValue::Vi16(value) => Robj::from(value),
            GenericValue::Vu32(value) => Robj::from(value),
            GenericValue::Vi32(value) => Robj::from(value),
            GenericValue::Vu64(value) => Robj::from(value),
            GenericValue::Vi64(value) => Robj::from(value),
            GenericValue::Vf32(value) => Robj::from(value),
            GenericValue::Vf64(value) => Robj::from(value),
            GenericValue::Vusize(value) => Robj::from(value),
            GenericValue::Visize(value) => Robj::from(*value as i64),
            GenericValue::Vbool(value) => Robj::from(value),
            GenericValue::Vstring(value) => Robj::from(value.as_ref()),
            GenericValue::Vbytes(value) => value.smart_deserialize()?,
            GenericValue::Token(value) => Robj::from(value.0),
            GenericValue::Marker(value) => Robj::from(value),
        })
    }
}

impl SmartDeserialization for &[u8] {
    fn smart_deserialize(&self) -> Result<Robj> {
        eprintln!("Calling smaert_deserializer on {:?}{}", self.iter().take(10).collect::<Vec<&u8>>(), if self.len() > 10 { ".." } else { "" });
        
        let sxp = i32::from_le_bytes(self[0..4].try_into().unwrap());
        eprintln!("DESER SEXPTYPE {sxp}");
        let usize_size_in_bytes = usize::BITS as usize / 8;
        let length = usize::from_le_bytes(self[4..(4 + usize_size_in_bytes)].try_into().unwrap());
        
        eprintln!("DESER length {length}");       
        let data = &self[4 + usize_size_in_bytes..];
        eprintln!("DESER data {data:?}");

        let rtype = sxp_to_rtype(sxp);
        eprintln!("DESER Rtype {rtype:?}");
        let elements = length / rtype.element_size().unwrap();

        match rtype {
            Rtype::Logicals | Rtype::Integers | Rtype::Doubles |
            Rtype::Complexes | Rtype::Raw => {
                let vector = Robj::alloc_vector(sxp as u32, length);
                unsafe {
                    let vector_data: *mut u8 = DATAPTR(vector.get()) as *mut u8;
                    std::ptr::copy_nonoverlapping(data.as_ptr(), vector_data, data.len());
                }
                Ok(vector)
            }
            Rtype::Strings => {
                let /*mut*/ strings_sexp = unsafe { Rf_protect(Rf_allocVector(sxp as u32, elements as isize)) }; 
                let mut cursor = 0;
                let mut string_index = 0;
                while cursor < data.len() {
                    eprintln!("DESER cursor at {cursor}");
                    let character_length = usize::from_le_bytes(data[cursor..cursor + usize_size_in_bytes].try_into().unwrap());
                    eprintln!("DESER character_length {character_length}");
                    cursor += usize_size_in_bytes;
                    eprintln!("DESER cursor at {cursor}");
                    let character_data = &data[cursor..cursor + character_length];
                    eprintln!("DESER character_data {character_data:?}");
                    cursor += character_length;
                    eprintln!("DESER cursor at {cursor}");
                    let character_vector = character_data.iter().map(|u| (*u) as char).collect::<Vec<char>>();
                    eprintln!("String!!!: {:?}",  character_vector);
                    let character_sexp = unsafe { Rf_protect(Rf_mkChar(character_data.as_ptr() as *const i8)) };
                    eprintln!("String sexp: {:?}",  Robj::from_sexp(character_sexp));
                    unsafe { SET_STRING_ELT(strings_sexp, string_index, character_sexp) }
                    string_index+=1;
                }

                let strings = Robj::from_sexp(strings_sexp);
                unsafe { Rf_unprotect(1 + string_index as i32) }
                eprintln!("DESER produced string vector: {strings:?}");
                Ok(strings)
            }  
            
            rtype => {
                panic!("Unsupported vector type: {:?}", rtype) // TODO better error
            }
        }
    }
}

impl SmartDeserialization for &Vec<u8> {
    fn smart_deserialize(&self) -> Result<Robj> {
       self.as_slice().smart_deserialize()
    }
}

impl SmartDeserialization for Vec<u8> {
    fn smart_deserialize(&self) -> Result<Robj> {
       self.as_slice().smart_deserialize()
    }
}

pub trait SmartDeserialization {
    fn smart_deserialize(&self) -> Result<Robj>;
}

pub trait SmartSerialization {
    fn smart_serialize(&self, memory: *const u8, elements: usize) -> Vec<u8>;
}

impl SmartSerialization for Rtype {
    fn smart_serialize(&self, memory: *const u8, elements: usize) -> Vec<u8> {
        let length_in_bytes = elements * self.element_size().unwrap();
        eprintln!("   bytes:          {:?}", length_in_bytes);

        let mut vector = Vec::new();
        vector.extend_from_slice(&self.as_sxp().to_le_bytes());
        vector.extend_from_slice(&length_in_bytes.to_le_bytes());       

        match self {
            Rtype::Strings => {                              
                let sexp_data: &[SEXP] = unsafe { std::slice::from_raw_parts(memory as *const SEXP, elements) };
                println!("SEXP data: {:?}", sexp_data);
                for character_sexp in sexp_data {                    

                    let character_length = unsafe { XLENGTH(*character_sexp) as usize };
                    let character_data = unsafe{ DATAPTR(*character_sexp as SEXP) as *const u8 };
                    let character_bytes: &[u8] = unsafe { std::slice::from_raw_parts(character_data, character_length) };

                    unsafe { eprintln!("String!: {:?}",  std::ffi::CStr::from_ptr(character_data as *const i8)); }

                    vector.extend_from_slice(&(character_length + 1).to_le_bytes());
                    vector.extend_from_slice(character_bytes);
                    vector.push('\0' as u8);
                }
            }

            Rtype::Integers | Rtype::Logicals | Rtype::Doubles | Rtype::Complexes | Rtype::Raw => {
                eprintln!("{:?} ... len: {} bytes: {}", self, elements, length_in_bytes);
                let bytes: &[u8] = unsafe { 
                    std::slice::from_raw_parts(memory as *const u8, length_in_bytes) 
                };
                vector.extend_from_slice(bytes);    
            }

            rtype => {
                panic!("Unsupported vector type: {:?}", rtype) // TODO better error
            }
        }

        vector
    }    
}