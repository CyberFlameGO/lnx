use std::fmt::Display;
use poem_openapi::Object;


#[derive(Object)]
pub struct Detailed {
    detail: String
}

impl<T: Display> From<T> for Detailed {
    fn from(v: T) -> Self {
        Self {
            detail: v.to_string()
        }
    }
}


