//! Actix actors support for Actix Web.

#![deny(nonstandard_style)]

mod context;
pub mod ws;

pub use self::context::HttpContext;
