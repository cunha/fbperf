use std::convert::From;
use std::error::Error as StdError;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};

#[derive(Debug)]
pub(super) struct ParseError {
    pub kind: ParseErrorKind,
    pub message: String,
}

#[derive(Eq, PartialEq, Hash, Debug)]
pub(super) enum ParseErrorKind {
    Untracked,
    AddrParseError,
    VipMetroIsNull,
    ClientCountryIsNull,
    UnknownPeeringRelationship,
    HdRatioBootstrapDiffCiBoundsMismatch,
    RepeatedTimebin,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ParseError {:?} {}", self.kind, self.message)
    }
}

impl StdError for ParseError {}

impl From<ParseIntError> for ParseError {
    fn from(error: ParseIntError) -> Self {
        ParseError {
            kind: ParseErrorKind::Untracked,
            message: error.to_string(),
        }
    }
}

impl From<ParseFloatError> for ParseError {
    fn from(error: ParseFloatError) -> Self {
        ParseError {
            kind: ParseErrorKind::Untracked,
            message: error.to_string(),
        }
    }
}

impl From<ipnet::AddrParseError> for ParseError {
    fn from(error: ipnet::AddrParseError) -> Self {
        ParseError {
            kind: ParseErrorKind::AddrParseError,
            message: format!("Cannot parse address: {}", error.to_string()),
        }
    }
}
