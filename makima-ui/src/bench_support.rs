//! A bench links this crate from outside, so the input's buffer and its row
//! layout get a public door here.

pub use crate::text_buffer::TextBuffer;
pub use crate::text_buffer::layout::{Layout, Row};
