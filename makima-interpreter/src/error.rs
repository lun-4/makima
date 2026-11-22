#[derive(Debug, thiserror::Error)]
pub enum InterpreterError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("runtime error: {0}")]
    Runtime(String),
    #[error("tool call failed: {tool}: {message}")]
    ToolCall { tool: String, message: String },
    #[error("sandboxed: {0}")]
    Sandboxed(String),
}
