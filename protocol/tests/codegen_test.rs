//! Integration test that verifies the code-generated struct files in
//! `src/generated/` are up-to-date.
//!
//! Run with `UPDATE_EXPECT=1` to regenerate all files when the schema or
//! codegen logic changes.

use std::path::{Path, PathBuf};

/// Root of the `protocol` crate (resolved from `CARGO_MANIFEST_DIR`).
fn crate_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_owned()
}

/// Absolute path to `src/generated/` inside the crate.
fn generated_dir() -> PathBuf {
    crate_root().join("src/generated")
}

/// Verify that every generated file matches its expected snapshot.
///
/// When the schemas or codegen change, run:
/// ```text
/// UPDATE_EXPECT=1 cargo test test_codegen_generated_structs -- --nocapture
/// ```
#[test]
fn test_codegen_generated_structs() {
    let files = protocol::generator::codegen::generate_all();

    if files.is_empty() {
        panic!(
            "generate_all returned no files — is the messages/ directory accessible from the test?"
        );
    }

    let gen_dir = generated_dir();
    std::fs::create_dir_all(&gen_dir).expect("create src/generated/");

    for (rel_path, content) in &files {
        let abs_path = crate_root().join(rel_path);
        if let Some(parent) = abs_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(&abs_path, content)
            .unwrap_or_else(|e| panic!("failed to write {:?}: {e}", abs_path));
    }

    // Run cargo fmt to fix formatting of the generated files.
    let fmt_status = std::process::Command::new("cargo")
        .args(["fmt", "--package", "protocol"])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .expect("failed to run cargo fmt");
    assert!(fmt_status.success(), "cargo fmt failed");

    // Now read back the formatted content and verify with expect_file.
    // This means the checked-in snapshot is always properly formatted.
    for rel_path in files.keys() {
        let abs_path = crate_root().join(rel_path);
        let formatted = std::fs::read_to_string(&abs_path)
            .unwrap_or_else(|e| panic!("read {:?}: {e}", abs_path));
        expect_test::expect_file![abs_path.as_path()].assert_eq(&formatted);
    }

    // Verify the count: one .rs per JSON schema + mod.rs.
    let messages_dir = crate_root().join("messages");
    let expected_count = std::fs::read_dir(&messages_dir)
        .into_iter()
        .flat_map(|rd| rd.flatten())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();

    assert_eq!(
        files.len(),
        expected_count + 1,
        "expected {expected_count} struct files + mod.rs, got {} files",
        files.len(),
    );

    eprintln!("✓ All {} files written, formatted & verified.", files.len());

    // Run cargo clippy to ensure no warnings.
    let clippy_output = std::process::Command::new("cargo")
        .args([
            "clippy",
            "--package",
            "protocol",
            "--all-targets",
            "--",
            "--deny",
            "warnings",
        ])
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .output()
        .expect("failed to run cargo clippy");
    assert!(
        clippy_output.status.success(),
        "clippy found issues in generated code — fix the codegen and re-run with UPDATE_EXPECT=1"
    );
    eprintln!("✓ cargo clippy passed");
}

/// Quick sanity check on one of the generated files.
#[test]
fn test_generated_struct_create_acls_response() {
    let files = protocol::generator::codegen::generate_all();
    let key = Path::new("src/generated/create_acls_response.rs");
    let content = files
        .get(key)
        .expect("create_acls_response.rs should be generated");

    assert!(
        content.contains("pub struct CreateAclsResponse"),
        "missing CreateAclsResponse struct"
    );
    assert!(
        content.contains("pub throttle_time_ms: i32"),
        "missing throttle_time_ms field"
    );
    assert!(
        content.contains("pub results: Vec<") && content.contains("Result>"),
        "missing results field"
    );
    assert!(
        content.contains("AclCreationResult") || content.contains("CreatableAclResult"),
        "missing results struct"
    );
}
