use std::path::Path;

#[test]
fn test_all_json_files_parse() {
    let path = Path::new("messages/");
    let mut total = 0u32;
    let mut failed = Vec::new();

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
            if let Err(e) = result {
                failed.push((path.file_name().unwrap().to_owned(), e));
            }
        }
    }

    for (name, err) in &failed {
        eprintln!("FAILED: {:?}: {err}", name);
    }
    assert!(
        failed.is_empty(),
        "{} of {total} files failed to parse",
        failed.len()
    );
    println!("All {total} JSON files parsed successfully!");
}
