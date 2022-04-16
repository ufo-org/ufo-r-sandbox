use extendr_api::prelude::Result;

#[macro_export]
macro_rules! r_or_bail {
    ($task:expr, $($s:expr),*$(,)?) => {
        match $task {
            Ok(ok) => ok,
            Err(error) => r_bail!("{}: {}", format!($($s,)+), error),
        }
    }
}

#[macro_export]
macro_rules! r_error {
    ($($s:expr),*$(,)?) => {
        extendr_api::Error::Other(format!($($s,)+))
    };
}

#[macro_export]
macro_rules! r_err {
    ($($s:expr),*$(,)?) => {
        Err(extendr_api::Error::Other(format!($($s,)+)))
    };
}

#[macro_export]
macro_rules! r_bail {
    ($($s:expr),+) => {
        return Err(r_error!($($s,)+))
    };    
}

#[macro_export]
macro_rules! r_bail_if {
    ($cond:expr => $($s:expr),+) => {
        if $cond {
            return Err(r_error!($($s,)+))
        }
    };    
}

pub trait OptionResultInverter<T,E> {
    fn extract_result(self) -> std::result::Result<Option<T>, E>;
}

impl<T,E> OptionResultInverter<T,E> for Option<std::result::Result<T,E>>{
    fn extract_result(self) -> std::result::Result<Option<T>, E> {
        self.map_or(Ok(None), |result| result.map(|v| Some(v)))
    }
}

pub trait IntoServerError<T> {
    fn rewrap<S, F>(self, msg: F) -> Result<T> where F: Fn() -> S, S: std::fmt::Display;
}

impl<T, E> IntoServerError<T> for std::result::Result<T, E> where E: std::fmt::Display {
    fn rewrap<S, F>(self, msg: F) -> Result<T> where F: Fn() -> S, S: std::fmt::Display {
        self.map_err(|e| r_error!("{}: {}", msg(), e))
    }
}

impl<T> IntoServerError<T> for Option<T> {
    fn rewrap<S, F>(self, msg: F) -> Result<T> where F: Fn() -> S, S: std::fmt::Display {
        self.ok_or_else(|| r_error!("{}", msg()))
    }
}
