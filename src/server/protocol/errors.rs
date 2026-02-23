#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    Invalid(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Invalid(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<std::io::Error> for ParseError {
    fn from(error: std::io::Error) -> Self {
        ParseError::Invalid(error.to_string())
    }
}
