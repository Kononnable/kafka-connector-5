use std::path::Path;

/// Verify all 90 JSON files parse successfully
#[test]
fn test_all_90_json_files_parse() {
    let path = Path::new("messages/");
    let mut total = 0u32;
    let mut parsed = 0u32;

    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            total += 1;
            let content = std::fs::read_to_string(&path).unwrap();
            let cleaned: String = content
                .lines()
                .filter(|line| !line.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");

            let result: Result<protocol::generator::structs::MessageStruct, _> =
                serde_json::from_str(&cleaned);
            assert!(
                result.is_ok(),
                "Failed to parse {:?}: {}",
                path.file_name().unwrap(),
                result.unwrap_err()
            );
            parsed += 1;
            dbg!(&result);
        }
    }

    assert_eq!(total, 90, "Expected 90 JSON files");
    assert_eq!(parsed, 90, "All 90 JSON files should parse successfully");
    println!("All {} JSON files parsed successfully!", parsed);
}
