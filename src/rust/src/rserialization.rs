use crate::r_bail;
use crate::r_error;
use crate::r_err;

use extendr_api::*;

pub trait RDeserialize: Sized {
    fn r_deserialize_any(self) -> Result<Robj>;
    fn r_deserialize(self, expected_type: Rtype) -> Result<Robj> {
        let object: Robj = self.r_deserialize_any()?;
        if object.rtype() != expected_type {
            r_err!(
                "Cannot deserialize object: \
                   expecting R object of type {:?}, \
                   but encountered {:?}. Serialized form: {:?}",
                expected_type,
                object.rtype(),
                object
            )
        } else {
            Ok(object)
        }
    }
}

impl RDeserialize for &[u8] {
    fn r_deserialize_any(self) -> Result<Robj> {
        call!("unserialize", self)
    }
}

impl RDeserialize for &Vec<u8> {
    fn r_deserialize_any(self) -> Result<Robj> {
        call!("unserialize", self)
    }
}

impl RDeserialize for Vec<u8> {
    fn r_deserialize_any(self) -> Result<Robj> {
        call!("unserialize", self)
    }
}