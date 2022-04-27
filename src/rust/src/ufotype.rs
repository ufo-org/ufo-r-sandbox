use std::mem::size_of;

use extendr_api::prelude::*;

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
            "boolean" => Ok(Self::Boolean),
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