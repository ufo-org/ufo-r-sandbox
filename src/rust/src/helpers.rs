pub trait OptionResultInverter<T,E> {
    fn extract_result(self) -> Result<Option<T>, E>;
}

impl<T,E> OptionResultInverter<T,E> for Option<Result<T,E>>{
    fn extract_result(self) -> Result<Option<T>, E> {
        self.map_or(Ok(None), |result| result.map(|v| Some(v)))
    }
}