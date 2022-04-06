use crate::r_bail;
use crate::r_or_bail;
use crate::r_error;
use crate::r_err;
use crate::r_bail_if;

use extendr_api::*;

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
        call!("unserialize", self)
    }
}

impl DeserdeR for &Vec<u8> {
    fn deserialize(self) -> Result<Robj> {
        call!("unserialize", self)
    }
}

impl DeserdeR for Vec<u8> {
    fn deserialize(self) -> Result<Robj> {
        call!("unserialize", self)
    }
}