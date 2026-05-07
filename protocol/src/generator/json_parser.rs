//! Module for parsing JSON protocol files into Rust structures.

use crate::generator::structs::*;
use std::path::Path;

/// Parse a single JSON protocol file into a MessageStruct.
/// Removes comment lines (//) before parsing.
pub fn parse_json_file(path: &Path) -> Result<MessageStruct, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;

    let cleaned = content
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");

    let message: MessageStruct = serde_json::from_str(&cleaned)?;
    Ok(message)
}

/// Parse all JSON files in a directory into MessageStruct objects
pub fn parse_all_json_files(
    directory: &Path,
) -> Result<Vec<MessageStruct>, Box<dyn std::error::Error>> {
    let mut messages = Vec::new();

    if directory.is_dir() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let msg = parse_json_file(&path)?;
                messages.push(msg);
            }
        }
    }

    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_file() {
        let path = Path::new("messages/MetadataRequest.json");
        let result = parse_json_file(path);
        assert!(
            result.is_ok(),
            "Failed to parse MetadataRequest.json: {:?}",
            result.err()
        );
        let msg = result.unwrap();
        assert_eq!(msg.name, "MetadataRequest");
        assert_eq!(msg.api_key, Some(3));
    }

    #[test]
    fn test_parse_all_json_files() {
        let path = Path::new("messages/");
        let result = parse_all_json_files(path);
        assert!(result.is_ok());
        let messages = result.unwrap();
        // Should parse most JSON files (some headers may fail in strict mode)
        assert!(
            !messages.is_empty(),
            "Expected at least some messages to be parsed"
        );
    }

    #[test]
    fn test_parse_header_file() {
        let path = Path::new("messages/RequestHeader.json");
        let result = parse_json_file(path);
        assert!(
            result.is_ok(),
            "Failed to parse RequestHeader.json: {:?}",
            result.err()
        );
        let msg = result.unwrap();
        assert_eq!(msg.name, "RequestHeader");
        assert_eq!(msg.api_key, None);
        assert_eq!(msg.message_type, MessageType::Header);
    }
}
