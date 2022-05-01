use std::mem::size_of;

use extendr_api::prelude::*;
use extendr_api::rtype_to_sxp;
use ufo_ipc::GenericValueRef;

use crate::errors::IntoServerError;
use crate::r_err;

pub trait RtypeTools: Sized {
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
        matches!(self, Rtype::Rstr | Rtype::Logicals | Rtype::Integers | Rtype::Doubles | Rtype::Complexes | Rtype::Strings | Rtype::List | Rtype::Raw)
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

trait IntoGenericRefVector{
    fn serialize_into_generic_ref_vector(&'_ self) -> Result<Vec<GenericValueRef<'_>>>;
}

// impl IntoGenericRefVector for Rbool {
//     fn serialize_into_generic_ref_vector<'a>(&'a self) -> Result<Vec<GenericValueRef<'a>>> {
//         let vec: Vec<bool> = self.into();
//         todo!()
//     }
// }

impl IntoGenericRefVector for Robj{
    fn serialize_into_generic_ref_vector(&'_ self) -> Result<Vec<GenericValueRef<'_>>> {
        match self.rtype() {
            Rtype::Rstr => {
                todo!()
            }
            Rtype::Logicals => {
                let vector = self.as_raw_slice()
                    .rewrap(|| format!("Cannot represent vector of type {:?} as raw bytes", self))?
                    .iter()
                    .map(|value| GenericValueRef::Vu8(*value))
                    .collect();

                Ok(vector)
            }
            Rtype::Integers => {
                let vector = self.as_integer_vector()
                    .rewrap(|| format!("Cannot represent vector of type {:?} as integers", self))?
                    .into_iter()
                    .map(GenericValueRef::Vi32)
                    .collect();

                Ok(vector)
            }
            Rtype::Doubles => {
                let vector = self.as_real_iter()
                    .rewrap(|| format!("Cannot represent vector of type {:?} as doubles", self))?
                    .map(|value| GenericValueRef::Vf64(*value))
                    .collect();

                Ok(vector)
            }
            Rtype::Complexes => {
                todo!()
            }
            Rtype::Strings => {
                let vector = self.as_str_iter()
                    .rewrap(|| format!("Cannot represent vector of type {:?} as raw bytes", self))?
                    .map(GenericValueRef::Vstring)
                    .collect();

                Ok(vector)
            }
            Rtype::List => {
                todo!()
            }
            Rtype::Raw => {
                let vector = self.as_raw_slice()
                    .rewrap(|| format!("Cannot represent vector of type {:?} as raw bytes", self))?
                    .iter()
                    .map(|value| GenericValueRef::Vu8(*value))
                    .collect();

                Ok(vector)
            }
            rtype => r_err!("Cannot serialize object {:?} into a generic vector", rtype),
        }
    }
}
