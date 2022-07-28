use std::mem::size_of;

use extendr_api::prelude::*;
use itertools::Itertools;
use libR_sys::Rf_mkChar;
use ufo_ipc::{GenericValueBoxed, UnexpectedGenericType};

use crate::{errors::IntoServerError, r_error};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UfoType {
    Integer,
    Numeric,
    Character,
    Complex,
    Boolean,
    Raw,
    Vector,
}

impl std::fmt::Display for UfoType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "{}", self.as_str())
    }
}

impl UfoType {
    pub fn as_str(&self) -> &'static str {
        match self {
            UfoType::Integer => "integer",
            UfoType::Numeric => "numeric",
            UfoType::Character => "character",
            UfoType::Complex => "complex",
            UfoType::Boolean => "boolean",
            UfoType::Raw => "raw",
            UfoType::Vector => "vector",
        }
    }
    pub fn element_size(&self) -> usize {
        match self {
            Self::Boolean => size_of::<libR_sys::Rboolean>(),
            Self::Integer => size_of::<i32>(),
            Self::Numeric => size_of::<f64>(),
            Self::Complex => size_of::<libR_sys::Rcomplex>(),
            Self::Character => size_of::<libR_sys::SEXP>(),
            Self::Vector => size_of::<libR_sys::SEXP>(),
            Self::Raw => size_of::<libR_sys::Rbyte>(),
        }
    }

    pub fn as_rtype(&self) -> Rtype {
        match self {
            Self::Boolean => Rtype::Logicals,
            Self::Integer => Rtype::Integers,
            Self::Numeric => Rtype::Doubles,
            Self::Complex => Rtype::Complexes,
            Self::Character => Rtype::Strings,
            Self::Vector => Rtype::List,
            Self::Raw => Rtype::Raw,
        }
    }

    pub fn convert_to_data(&self, result: Vec<GenericValueBoxed>) -> Result<Vec<u8>> {
        match self {
            UfoType::Integer | 
            UfoType::Numeric | 
            UfoType::Complex | 
            UfoType::Boolean | 
            UfoType::Raw       => {
                let bytes = result.into_iter().exactly_one()
                    .map_err(|_e| r_error!("Expecting a function returning {} to send back a single byte vector as response", self))?
                    .expect_bytes_into()
                    .map_err(|_e| r_error!("Expecting a function returning {} to send back a byte vector as response", self))?;
                Ok(bytes)
            }
            UfoType::Character => self.convert_strings_to_data(result),
            UfoType::Vector    => todo!(),
        }
    }    

    fn convert_strings_to_data(&self, result: Vec<GenericValueBoxed>) -> Result<Vec<u8>> {
        // The types *const T, &T, Box<T>, Option<&T>, and Option<Box<T>> all
        // have the same size. If T is Sized, all of those types have the same
        // size as usize.
        // let element_count = size / std::mem::size_of::<usize>();
        // let strings = result.deserialize_into(Rtype::Strings)?.as_str_iter()
        //     .rewrap(|| "Cannot cast result into String of vectors, even though expecting a vector")?;

        let strings = result.into_iter()
            .map(|v| v.expect_string_into())
            .collect::<std::result::Result<Vec<String>, UnexpectedGenericType>>()
            .rewrap(|| r_error!("Expecting a function returning {} to send back a vector of strings", self))?;
        
        //r_bail_if!(unsafe { libR_sys::R_gc_running() == 1 } => "Cannot allocate character vectors when the GC is running.");

        let bytes: Vec<u8> = strings.into_iter().flat_map(|string| {
            // println!("str: {:?}", string);   
            let character_vector = unsafe { Rf_mkChar(string.as_ptr() as *const i8) }; // FIXME this can trigger GC
            // let character_vector = unsafe { r!(string).get() };
            // println!("character_vector: {:?}", character_vector);
            let ne_bytes = (character_vector as usize).to_ne_bytes();
            // println!("character_vector as ne_bytes: {:?}", ne_bytes);
            ne_bytes
        }).collect();

        Ok(bytes)
    }

    pub fn pack_for_transport(&self, result: Robj) -> Result<Vec<GenericValueBoxed>> {
        match self {
            UfoType::Integer |
            UfoType::Numeric |
            UfoType::Complex |
            UfoType::Boolean |
            UfoType::Raw       => {
                let size: usize = result.len() * self.element_size();
                let slice: &[u8] = unsafe {
                    let data_ptr = libR_sys::DATAPTR_RO(result.get());
                    std::slice::from_raw_parts(data_ptr as *const u8, size)
                };
                let vector = Vec::from(slice);
                Ok(vec![GenericValueBoxed::Vbytes(vector)])
            }
            UfoType::Character => self.pack_strings_for_transport(result),
            UfoType::Vector    => todo!(),
        }
    }

    pub fn pack_strings_for_transport(&self, result: Robj) -> Result<Vec<GenericValueBoxed>> {
        let strings: Vec<GenericValueBoxed> = result.as_str_iter()
            .rewrap(|| r_error!("A function returning {} was unable to construct a string iterator", self))?
            .map(|s| GenericValueBoxed::Vstring(s.to_owned()))
            .collect();
        Ok(strings)
    }
}

impl TryFrom<&Rtype> for UfoType {
    type Error = Error;
    fn try_from(value: &Rtype) -> Result<Self> {
        match value {            
            //Rtype::Rstr => todo!(),
            Rtype::Logicals => Ok(Self::Boolean),
            Rtype::Integers => Ok(Self::Integer),
            Rtype::Doubles => Ok(Self::Numeric),
            Rtype::Complexes => Ok(Self::Complex),
            Rtype::Strings => Ok(Self::Character),
            Rtype::List => Ok(Self::Vector),
            Rtype::Raw => Ok(Self::Raw),
            // TODO add more supported types
            erroneous_type => Err(Error::Other(format!("Expected logical, integer numeric, complex, string, vector or raw type, but encountered {:?}", erroneous_type)))
        }
    }
}

impl TryFrom<&str> for UfoType {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self> {
        match s {
            "integer" => Ok(Self::Integer),
            "numeric" | "double" => Ok(Self::Numeric),
            "character" => Ok(Self::Character),
            "complex" => Ok(Self::Complex),
            "boolean" | "logical" => Ok(Self::Boolean),
            "vector" => Ok(Self::Vector),
            "raw" => Ok(Self::Raw),
            t => Err(Error::Other(format!("Expected integer, numeric, character, complex, or boolean, but found {}", t)))
        }
    }
}

impl TryFrom<String> for UfoType {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::try_from(s.as_str())
    }
}

impl TryFrom<&String> for UfoType {
    type Error = Error;
    fn try_from(s: &String) -> Result<Self> {
        Self::try_from(s.as_str())
    }
}

pub trait UfoTypeChecker {
    fn check_against(&self, object: &Robj) -> bool;
}

impl UfoTypeChecker for UfoType {
    fn check_against(&self, object: &Robj) -> bool {
        matches!((self, object.rtype()), (UfoType::Integer, Rtype::Integers) | (UfoType::Numeric, Rtype::Doubles) | (UfoType::Character, Rtype::Strings) | (UfoType::Complex, Rtype::Complexes) | (UfoType::Boolean, Rtype::Logicals) | (UfoType::Raw, Rtype::Raw) | (UfoType::Vector, Rtype::List))
    }
}

impl UfoTypeChecker for Option<UfoType> {
    fn check_against(&self, object: &Robj) -> bool {
        match (self, object.rtype()) {
            (Some(ty), _) => ty.check_against(object),
            (None, Rtype::Null) => true,
            _ => false,
        }
    }
}

