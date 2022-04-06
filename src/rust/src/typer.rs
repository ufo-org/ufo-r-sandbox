use std::mem::size_of;

use extendr_api::prelude::*;
use extendr_api::rtype_to_sxp;

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