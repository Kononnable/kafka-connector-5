//! Code generator that emits Rust structs from parsed Kafka protocol schemas.
//!
//! The generated code lives in `src/generated/` — one `.rs` file per JSON schema file.
//! A companion test using `expect_test::expect_file![]` verifies the output and can
//! cleanly regenerate it when schemas or the codegen logic change.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use super::structs::{Field, MessageStruct, MessageType};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// All generated files keyed by their relative output path (e.g.
/// `"src/generated/create_acls_request.rs"`).
pub type GeneratedFiles = BTreeMap<PathBuf, String>;

/// Run the full codegen pipeline: parse all JSON schemas and produce
/// the Rust source files that should land in `src/generated/`.
pub fn generate_all() -> GeneratedFiles {
    let mut files = GeneratedFiles::new();

    // Read and parse every JSON file in the messages directory.
    let messages_dir = Path::new("messages/");
    if !messages_dir.is_dir() {
        return files;
    }

    let mut entries: Vec<_> = std::fs::read_dir(messages_dir)
        .unwrap_or_else(|e| panic!("failed to read messages directory {:?}: {e}", messages_dir))
        .map(|e| e.unwrap_or_else(|e| panic!("failed to read directory entry: {e}")))
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    entries.sort_by_key(|e| e.path());

    let mut module_names = Vec::new();

    // First pass: parse all messages into a vector so we can build
    // the request↔response pairing map before generating output.
    let mut parsed: Vec<MessageStruct> = Vec::new();
    for entry in &entries {
        let path = entry.path();
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {e}", path));
        let cleaned: String = content
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let msg = serde_json::from_str::<MessageStruct>(&cleaned)
            .unwrap_or_else(|e| panic!("failed to parse {:?}: {e}", path));
        parsed.push(msg);
    }

    // Build apiKey → (request_name, response_name) pairing.
    let mut pairs: HashMap<i16, (String, String)> = HashMap::new();
    for msg in &parsed {
        if let Some(ak) = msg.api_key {
            let entry = pairs
                .entry(ak)
                .or_insert_with(|| (String::new(), String::new()));
            match msg.message_type {
                MessageType::Request => entry.0 = msg.name.clone(),
                MessageType::Response => entry.1 = msg.name.clone(),
                MessageType::Header | MessageType::Data => {}
            }
        }
    }

    // Second pass: generate file content with pairing info available.
    for msg in &parsed {
        let file_name = message_file_name(&msg.name);
        let module_name = file_name
            .strip_suffix(".rs")
            .unwrap_or(&file_name)
            .to_string();
        let output_path = PathBuf::from("src/generated").join(&file_name);

        let pair_names = msg.api_key.and_then(|ak| pairs.get(&ak).cloned());
        let source = generate_file(msg, pair_names.as_ref());

        module_names.push((module_name.clone(), msg.name.clone()));
        files.insert(output_path, source);
    }

    // Generate mod.rs
    let mut mod_rs = String::new();
    mod_rs.push_str("//! Auto-generated Kafka protocol structs.\n");
    mod_rs.push_str("//!\n");
    mod_rs.push_str("//! **Do not edit by hand.** Regenerate by running:\n");
    mod_rs.push_str("//! ```text\n");
    mod_rs.push_str("//! cargo test test_codegen_generated_structs -- --nocapture\n");
    mod_rs
        .push_str("//! UPDATE_EXPECT=1 cargo test test_codegen_generated_structs -- --nocapture\n");
    mod_rs.push_str("//! ```\n");
    mod_rs.push('\n');

    for (module_name, _struct_name) in &module_names {
        mod_rs.push_str(&format!("pub mod {};\n", module_name));
    }
    mod_rs.push('\n');
    // Re-export all top-level structs
    mod_rs.push_str("// Re-exports\n");
    for (module_name, _struct_name) in &module_names {
        mod_rs.push_str(&format!("pub use {}::{};\n", module_name, _struct_name));
    }
    mod_rs.push('\n');

    // Generate is_flexible_api dispatch function
    mod_rs.push_str("use crate::traits::{ApiRequest, ApiResponse, ApiVersion as ApiVer};\n");
    mod_rs.push_str("use bytes::Bytes;\n");
    mod_rs.push_str(
        "/// Look up whether a given API key + version uses flexible (compact) wire encoding.\n",
    );
    mod_rs.push_str("/// Generated from each message's `flexibleVersions` field.\n");
    mod_rs.push_str("pub fn is_flexible_api(api_key: i16, api_version: i16) -> bool {\n");
    mod_rs.push_str("    match api_key {\n");

    for msg in &parsed {
        if let Some(ak) = msg
            .api_key
            .filter(|_| msg.message_type == MessageType::Request)
        {
            let struct_name = &msg.name;
            mod_rs.push_str(&format!(
                "        {} => api_version >= {}::get_min_flexible_version().0,\n",
                ak, struct_name
            ));
        }
    }
    mod_rs.push_str("        _ => false,\n");
    mod_rs.push_str("    }\n");
    mod_rs.push_str("}\n");
    mod_rs.push('\n');

    // ----- api_key_name -----
    mod_rs.push_str("/// Return the human-readable name for a given API key.\n");
    mod_rs.push_str("pub fn api_key_name(api_key: i16) -> &'static str {\n");
    mod_rs.push_str("    match api_key {\n");
    for msg in &parsed {
        if let Some(ak) = msg
            .api_key
            .filter(|_| msg.message_type == MessageType::Request)
        {
            let name = msg.name.strip_suffix("Request").unwrap_or(&msg.name);
            mod_rs.push_str(&format!("        {} => \"{}\",\n", ak, name));
        }
    }
    mod_rs.push_str("        _ => \"Unknown\",\n");
    mod_rs.push_str("    }\n");
    mod_rs.push_str("}\n");
    mod_rs.push('\n');

    // ----- decode_request_body -----
    mod_rs.push_str("/// Deserialize a request body for the given API key and version.\n");
    mod_rs.push_str("pub fn decode_request_body(api_key: i16, version: i16, body: &[u8]) -> Result<String, String> {\n");
    mod_rs.push_str("    let ver = ApiVer::new(version);\n");
    mod_rs.push_str("    let mut buf = Bytes::copy_from_slice(body);\n");
    mod_rs.push_str("    match api_key {\n");
    for msg in &parsed {
        if let Some(ak) = msg
            .api_key
            .filter(|_| msg.message_type == MessageType::Request)
        {
            let struct_name = &msg.name;
            mod_rs.push_str(&format!(
                "        {} => {}::deserialize(ver, &mut buf).map(|v| format!(\"{{v:?}}\")).map_err(|e| format!(\"{{e}}\")),\n",
                ak, struct_name
            ));
        }
    }
    mod_rs.push_str("        _ => Err(format!(\"unknown api key {api_key}\")),\n");
    mod_rs.push_str("    }\n");
    mod_rs.push_str("}\n");
    mod_rs.push('\n');

    // ----- decode_response_body -----
    mod_rs.push_str("/// Deserialize a response body for the given API key and version.\n");
    mod_rs.push_str("pub fn decode_response_body(api_key: i16, version: i16, body: &[u8]) -> Result<String, String> {\n");
    mod_rs.push_str("    let ver = ApiVer::new(version);\n");
    mod_rs.push_str("    let mut buf = Bytes::copy_from_slice(body);\n");
    mod_rs.push_str("    match api_key {\n");
    for msg in &parsed {
        if let Some(ak) = msg
            .api_key
            .filter(|_| msg.message_type == MessageType::Response)
        {
            let struct_name = &msg.name;
            mod_rs.push_str(&format!(
                "        {} => {}::deserialize(ver, &mut buf).map(|v| format!(\"{{v:?}}\")).map_err(|e| format!(\"{{e}}\")),\n",
                ak, struct_name
            ));
        }
    }
    mod_rs.push_str("        _ => Err(format!(\"unknown api key {api_key}\")),\n");
    mod_rs.push_str("    }\n");
    mod_rs.push_str("}\n");

    files.insert(PathBuf::from("src/generated/mod.rs"), mod_rs);

    files
}

// ---------------------------------------------------------------------------
// File-level generation
// ---------------------------------------------------------------------------

/// Derive the snake-case file name from a PascalCase message name.
fn message_file_name(name: &str) -> String {
    let mut out = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(ch.to_ascii_lowercase());
    }
    out.push_str(".rs");
    out
}

/// Generate the complete source text for one schema file.
///
/// `pair_names` is `Some((request_name, response_name))` when this message
/// has a known apiKey, so we can emit the `ApiRequest` / `ApiResponse` impl.
fn generate_file(msg: &MessageStruct, pair_names: Option<&(String, String)>) -> String {
    let mut code = String::new();

    // Module-level allow for unused imports (not all traits are used in every file).
    code.push_str("#![allow(unused_imports, unused_variables)]\n");
    code.push_str("use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};\n");
    code.push_str(
        "use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};\n",
    );
    code.push_str("use bytes::{Buf, BufMut, Bytes, BytesMut};\n");
    code.push_str("use indexmap::IndexMap;\n");
    code.push('\n');

    // Collect all nested structs that need to be emitted.
    let mut nested = BTreeMap::new();
    collect_nested_structs(&msg.fields, &mut nested);
    // Also collect common structs (shared struct definitions).
    for cs in &msg.common_structs {
        let name = if cs.field_type.is_empty() {
            cs.name.clone()
        } else {
            cs.field_type.clone()
        };
        nested.entry(name).or_insert_with(|| cs.fields.clone());
        // Recurse into common struct fields for deeper nesting
        collect_nested_structs(&cs.fields, &mut nested);
    }

    // Main struct
    code.push_str("// -------------------------------------------------------\n");
    code.push_str(&format!("// {}\n", msg.name));
    code.push_str("// -------------------------------------------------------\n");
    code.push_str(&generate_struct(
        &msg.name,
        &msg.fields,
        &msg.valid_versions,
    ));
    code.push('\n');

    // Helper: check whether any parent field referencing a given struct
    // uses overlapping composite mapKey fields (i.e. will become IndexMap).
    let should_strip = |struct_name: &str| -> bool {
        // Scan msg.fields recursively, plus common structs fields.
        let mut all_fields: Vec<&Field> = Vec::new();
        all_fields.extend(&msg.fields);
        for cs in &msg.common_structs {
            all_fields.push(cs);
        }
        while let Some(f) = all_fields.pop() {
            let inner = f.field_type.strip_prefix("[]");
            if inner == Some(struct_name) && !f.fields.is_empty() {
                let key_fields: Vec<&Field> = f.fields.iter().filter(|sf| sf.map_key).collect();
                if !key_fields.is_empty()
                    && key_fields.iter().all(|kf| !key_has_nullable(kf))
                    && (key_fields.len() == 1 || composite_key_versions_overlap(&key_fields))
                {
                    return true;
                }
            }
            all_fields.extend(&f.fields);
        }
        false
    };

    // For nested structs with composite mapKeys (2+ overlapping keys),
    // generate a dedicated key struct: `{StructName}Key`.
    // This replaces bare tuple types with named fields (e.g.
    // `IndexMap<AlterConfigsResourceKey, AlterConfigsResource>`).
    let mut composite_key_structs: BTreeMap<String, Vec<Field>> = BTreeMap::new();
    for (struct_name, struct_fields) in &nested {
        let key_fields: Vec<&Field> = struct_fields.iter().filter(|f| f.map_key).collect();
        if key_fields.len() > 1 && should_strip(struct_name) {
            let key_name = format!("{}Key", struct_name);
            composite_key_structs.insert(key_name, key_fields.into_iter().cloned().collect());
        }
    }
    // Emit composite key structs first (they have no dependencies).
    for (key_name, key_fields) in &composite_key_structs {
        code.push_str(&generate_key_struct(key_name, key_fields));
        code.push('\n');
    }

    // Nested structs -- strip mapKey fields from inner structs
    // that will become IndexMap values (key is stored separately).
    let mut nested_no_map_keys = BTreeMap::new();
    for (struct_name, struct_fields) in &nested {
        if should_strip(struct_name) {
            let mut stripped = Vec::new();
            for f in struct_fields {
                if !f.map_key {
                    stripped.push(f.clone());
                }
            }
            nested_no_map_keys.insert(struct_name.clone(), stripped);
        } else {
            nested_no_map_keys.insert(struct_name.clone(), struct_fields.clone());
        }
    }
    for (struct_name, struct_fields) in &nested_no_map_keys {
        code.push_str(&generate_struct(struct_name, struct_fields, ""));
        code.push('\n');
    }

    let (min_v, max_v) = parse_version_range(&msg.valid_versions);

    // ApiRequest/ApiResponse trait impls (only for messages with api_key)
    if let Some(ak) = msg.api_key {
        match msg.message_type {
            MessageType::Request => {
                let resp_name = pair_names
                    .and_then(|p| {
                        if p.0 == msg.name {
                            Some(p.1.as_str())
                        } else {
                            None
                        }
                    })
                    .unwrap_or("UNKNOWN_RESPONSE");

                code.push_str(&format!("impl ApiRequest for {} {{\n", msg.name));
                code.push_str(&format!(
                    "    type Response = crate::generated::{};\n",
                    resp_name
                ));
                code.push_str(&format!(
                    "    fn get_api_key() -> ApiKey {{ ApiKey::new({}) }}\n",
                    ak
                ));
                code.push_str(&format!(
                    "    fn get_min_supported_version() -> ApiVer {{ ApiVer::new({}) }}\n",
                    min_v
                ));
                code.push_str(&format!(
                    "    fn get_max_supported_version() -> ApiVer {{ ApiVer::new({}) }}\n",
                    max_v
                ));
                code.push_str(&format!(
                    "    fn get_min_flexible_version() -> ApiVer {{ ApiVer::new({}) }}\n",
                    min_flex_version(&msg.flexible_versions)
                ));

                // serialize
                code.push_str("    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {\n");
                if max_v >= min_v {
                    code.push_str("        assert!(");
                    code.push_str(&format!("{} <= version.0 && version.0 <= {}", min_v, max_v));
                    code.push_str(&format!(", \"version {{}} is not supported by {{}} (supported: {}-{})\", version.0, stringify!(Self));\n", min_v, max_v));
                }
                code.push_str(
                    "        let is_flexible = version.0 >= Self::get_min_flexible_version().0;\n",
                );

                for field in &msg.fields {
                    code.push_str(&generate_serialize_field(field, field, &msg.name));
                }
                code.push_str("        if is_flexible {\n");
                code.push_str(&generate_tagged_encode_body(&msg.fields));
                code.push_str("        }\n");
                code.push_str("        Ok(())\n");
                code.push_str("    }\n");

                // deserialize
                code.push_str("    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {\n");
                code.push_str(
                    "        let is_flexible = version.0 >= Self::get_min_flexible_version().0;\n",
                );
                for field in &msg.fields {
                    code.push_str(&generate_deserialize_field(field));
                }
                code.push_str("        if is_flexible {\n");
                code.push_str(&generate_tagged_decode_body(&msg.fields));
                code.push_str("        }\n");
                code.push_str(&format!(
                    "        Ok(Self {{ {} }})\n",
                    msg.fields
                        .iter()
                        .map(|f| escape_field_name(&camel_to_snake(&f.name)))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                code.push_str("    }\n");
                code.push_str("}\n");
            }
            MessageType::Response => {
                let req_name = pair_names
                    .and_then(|p| {
                        if p.1 == msg.name {
                            Some(p.0.as_str())
                        } else {
                            None
                        }
                    })
                    .unwrap_or("UNKNOWN_REQUEST");

                code.push_str(&format!("impl ApiResponse for {} {{\n", msg.name));
                code.push_str(&format!(
                    "    type Request = crate::generated::{};\n",
                    req_name
                ));
                code.push_str(&format!(
                    "    fn get_api_key() -> ApiKey {{ ApiKey::new({}) }}\n",
                    ak
                ));
                code.push_str(&format!(
                    "    fn get_min_supported_version() -> ApiVer {{ ApiVer::new({}) }}\n",
                    min_v
                ));
                code.push_str(&format!(
                    "    fn get_max_supported_version() -> ApiVer {{ ApiVer::new({}) }}\n",
                    max_v
                ));
                code.push_str(&format!(
                    "    fn get_min_flexible_version() -> ApiVer {{ ApiVer::new({}) }}\n",
                    min_flex_version(&msg.flexible_versions)
                ));

                // serialize
                code.push_str("    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {\n");
                if max_v >= min_v {
                    code.push_str("        assert!(");
                    code.push_str(&format!("{} <= version.0 && version.0 <= {}", min_v, max_v));
                    code.push_str(&format!(", \"version {{}} is not supported by {{}} (supported: {}-{})\", version.0, stringify!(Self));\n", min_v, max_v));
                }
                code.push_str(
                    "        let is_flexible = version.0 >= Self::get_min_flexible_version().0;\n",
                );

                for field in &msg.fields {
                    code.push_str(&generate_serialize_field(field, field, &msg.name));
                }
                code.push_str("        if is_flexible {\n");
                code.push_str(&generate_tagged_encode_body(&msg.fields));
                code.push_str("        }\n");
                code.push_str("        Ok(())\n");
                code.push_str("    }\n");

                // deserialize
                code.push_str("    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {\n");
                code.push_str(
                    "        let is_flexible = version.0 >= Self::get_min_flexible_version().0;\n",
                );
                for field in &msg.fields {
                    code.push_str(&generate_deserialize_field(field));
                }
                code.push_str("        if is_flexible {\n");
                code.push_str(&generate_tagged_decode_body(&msg.fields));
                code.push_str("        }\n");
                code.push_str(&format!(
                    "        Ok(Self {{ {} }})\n",
                    msg.fields
                        .iter()
                        .map(|f| escape_field_name(&camel_to_snake(&f.name)))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                code.push_str("    }\n");
                code.push_str("}\n");
            }
            MessageType::Header | MessageType::Data => {}
        }
    } // end if let Some(ak)

    // KafkaCodec for all message types (except headers which get inherent methods)
    if msg.message_type == MessageType::Header {
        code.push_str(&generate_header_impl(&msg.name, &msg.fields));
    } else {
        code.push_str(&generate_kafka_codec_impl(&msg.name, &msg.fields));
    }
    code.push('\n');
    // And for nested structs (using stripped versions without mapKey fields).
    for (struct_name, struct_fields) in &nested_no_map_keys {
        code.push_str(&generate_kafka_codec_impl(struct_name, struct_fields));
        code.push('\n');
    }
    // KafkaCodec impls for composite key structs.
    for (key_name, key_fields) in &composite_key_structs {
        code.push_str(&generate_kafka_codec_impl(key_name, key_fields));
        code.push('\n');
    }

    code
}

/// Parse a `validVersions` string like `"0-7"` or `"0+"` into (min, max).
fn parse_version_range(versions: &str) -> (i16, i16) {
    let v = versions.trim();
    if v == "none" {
        return (0, -1);
    }
    if let Some(range) = v.split_once('-') {
        let min = range.0.trim().parse::<i16>().unwrap_or(0);
        let max = range.1.trim().parse::<i16>().unwrap_or(i16::MAX);
        (min, max)
    } else if let Some(base) = v.strip_suffix('+') {
        let min = base.trim().parse::<i16>().unwrap_or(0);
        (min, i16::MAX)
    } else if let Ok(single) = v.parse::<i16>() {
        (single, single)
    } else {
        (0, -1)
    }
}

/// Generate a version-guard **condition** for a field, or `None` if the field
/// is available in all versions.  The caller wraps it with `if ...`.
fn field_version_condition(field: &Field) -> Option<String> {
    let v = field.versions.trim();
    if v == "0+" || v.is_empty() {
        return None;
    }
    if let Some(range) = v.split_once('-') {
        let min = range.0.trim();
        let max = range.1.trim();
        Some(format!("{} <= version.0 && version.0 <= {}", min, max))
    } else if let Some(base) = v.strip_suffix('+') {
        Some(format!("{} <= version.0", base.trim()))
    } else if let Ok(single) = v.parse::<i16>() {
        Some(format!("version.0 == {}", single))
    } else {
        None
    }
}

/// Return the minimum flexible version number from a `flexibleVersions` string.
/// Returns `i16::MAX` when never flexible, `0` when always flexible.
fn min_flex_version(flexible_versions: &Option<String>) -> i16 {
    match flexible_versions.as_deref() {
        None | Some("none") => i16::MAX,
        Some("0+") => 0i16,
        Some(v) => {
            if let Some(range) = v.split_once('-') {
                range.0.trim().parse::<i16>().unwrap_or(0)
            } else if let Some(base) = v.strip_suffix('+') {
                base.trim().parse::<i16>().unwrap_or(0)
            } else {
                v.parse::<i16>().unwrap_or(i16::MAX)
            }
        }
    }
}

#[allow(dead_code)]
/// Generate a boolean expression that checks if `version.0` falls within the
/// message's `flexibleVersions` range.  Returns `"false"` when not specified.
fn flexible_condition(flexible_versions: &Option<String>) -> String {
    match flexible_versions.as_deref() {
        None | Some("none") => "false".to_string(),
        Some("0+") => "true".to_string(),
        Some(v) => {
            if let Some(range) = v.split_once('-') {
                let min = range.0.trim();
                let max = range.1.trim();
                format!("{} <= version.0 && version.0 <= {}", min, max)
            } else if let Some(base) = v.strip_suffix('+') {
                format!("{} <= version.0", base.trim())
            } else if v.parse::<i16>().is_ok() {
                format!("version.0 == {}", v)
            } else {
                "false".to_string()
            }
        }
    }
}

/// Return true if the field's Kafka type has built-in Option flexible encoding
/// (i.e. `Option<String>`, `Option<Vec<u8>>`, `Option<Vec<T>>` all implement
/// `encode_flexible`/`decode_flexible` themselves).
fn nullable_has_builtin_flex(field: &Field) -> bool {
    let ft = &field.field_type;
    // String, bytes, and array types have built-in Option flexible encoding
    ft == "string" || ft == "bytes" || ft == "records" || ft.starts_with("[]")
}

/// Generate the tag buffer encode body for a list of fields that have `tag` set.
/// Returns code that counts non-default tagged fields and writes them.
fn generate_tagged_encode_body(fields: &[Field]) -> String {
    let mut code = String::new();
    let tagged: Vec<&Field> = fields.iter().filter(|f| f.tag.is_some()).collect();
    if tagged.is_empty() {
        code.push_str("            encode_unsigned_varint(0u64, buf);\n");
        return code;
    }
    code.push_str("            let mut tag_count = 0u64;\n");
    for f in &tagged {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        if let Some(check) = non_default_check(&rust_name, f) {
            code.push_str(&format!("            if {} {{ tag_count += 1; }}\n", check));
        }
    }
    code.push_str("            encode_unsigned_varint(tag_count, buf);\n");
    for f in &tagged {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        let _inner_type = map_field_type(f);
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        let tag_id = f.tag.unwrap();
        if let Some(check) = non_default_check(&rust_name, f) {
            code.push_str(&format!("            if {} {{\n", check));
            code.push_str(&format!(
                "                encode_unsigned_varint({}u64, buf);\n",
                tag_id
            ));
            code.push_str("                let mut tmp_buf = bytes::BytesMut::new();\n");
            if needs_presence {
                code.push_str(&format!(
                    "                if let Some(ref val) = self.{} {{\n",
                    rust_name
                ));
                code.push_str("                    val.encode(&mut tmp_buf, version, true)?;\n");
                code.push_str("                }\n");
            } else {
                code.push_str(&format!(
                    "                self.{}.encode(&mut tmp_buf, version, true)?;\n",
                    rust_name
                ));
            }
            code.push_str("                encode_unsigned_varint(tmp_buf.len() as u64, buf);\n");
            code.push_str("                buf.put_slice(&tmp_buf);\n");
            code.push_str("            }\n");
        }
    }
    code
}

/// Generate the tag buffer decode body for a list of fields that have `tag` set.
/// Returns code that iterates the tag buffer, matches tags, and decodes fields.
fn generate_tagged_decode_body(fields: &[Field]) -> String {
    let mut code = String::new();
    let tagged: Vec<&Field> = fields.iter().filter(|f| f.tag.is_some()).collect();
    if tagged.is_empty() {
        code.push_str("            let (_tag_count, _) = decode_unsigned_varint(buf)?;\n");
        return code;
    }
    code.push_str("            let (tag_count, _) = decode_unsigned_varint(buf)?;\n");
    code.push_str("            for _ in 0..tag_count {\n");
    code.push_str("                let (__tag_id, _) = decode_unsigned_varint(buf)?;\n");
    code.push_str("                let (__tag_len, _) = decode_unsigned_varint(buf)?;\n");
    code.push_str("                match __tag_id {\n");
    for f in &tagged {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        let rust_type = map_field_type(f);
        let tag_id = f.tag.unwrap();
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        if needs_presence {
            let _inner_type = strip_option_wrapper(&rust_type);
            code.push_str(&format!(
                "                    {} => {{ {} = Some(KafkaCodec::decode(buf, version, true)?); }}\n",
                tag_id, rust_name
            ));
        } else {
            code.push_str(&format!(
                "                    {} => {{ {} = KafkaCodec::decode(buf, version, true)?; }}\n",
                tag_id, rust_name
            ));
        }
    }
    code.push_str("                    _ => { buf.advance(__tag_len as usize); }\n");
    code.push_str("                }\n");
    code.push_str("            }\n");
    code
}

/// Return a boolean expression that tests whether a field has a non-default value.
fn non_default_check(rust_name: &str, field: &Field) -> Option<String> {
    let ft = &field.field_type;
    if field.nullable_versions.is_some() {
        // All nullable fields become Option<T> — check Some
        Some(format!("self.{}.is_some()", rust_name))
    } else if ft == "string" {
        Some(format!("!self.{}.is_empty()", rust_name))
    } else if ft == "bytes" || ft == "records" || ft.starts_with("[]") {
        // Vec<T> — is_empty works for any T without type inference issues
        Some(format!("!self.{}.is_empty()", rust_name))
    } else if ft == "bool" {
        Some(format!("self.{}", rust_name))
    } else if ft == "uuid" {
        // [u8; 16] — compare to zeroed array
        Some(format!("self.{} != [0u8; 16]", rust_name))
    } else if ["int8", "int16", "int32", "int64", "uint16", "float64"].contains(&ft.as_str()) {
        // Numeric types: != 0 works with type inference
        Some(format!("self.{} != 0", rust_name))
    } else {
        // Custom struct type — struct derives Default + PartialEq
        Some(format!("self.{} != Default::default()", rust_name))
    }
}

/// Generate one field's serialization code.
fn generate_serialize_field(field: &Field, _parent: &Field, msg_name: &str) -> String {
    let rust_name = escape_field_name(&camel_to_snake(&field.name));
    let cond = field_version_condition(field);
    let mut code = String::new();
    // For nullable struct fields (no builtin flexible), generate presence marker
    let needs_presence = field.nullable_versions.is_some() && !nullable_has_builtin_flex(field);
    let body = |code: &mut String, guard: &str| {
        if needs_presence {
            code.push_str(&format!("{}if is_flexible {{\n", guard));
            code.push_str(&format!(
                "{}if let Some(ref val) = self.{} {{\n",
                guard, rust_name
            ));
            code.push_str(&format!("{}encode_unsigned_varint(1u64, buf);\n", guard));
            code.push_str(&format!("{}val.encode(buf, version, true)?;\n", guard));
            code.push_str(&format!("{}}} else {{\n", guard));
            code.push_str(&format!("{}encode_unsigned_varint(0u64, buf);\n", guard));
            code.push_str(&format!("{}}}\n", guard));
            code.push_str(&format!("{}}} else {{\n", guard));
            code.push_str(&format!(
                "{}if let Some(ref val) = self.{} {{\n",
                guard, rust_name
            ));
            code.push_str(&format!("{}val.encode(buf, version, false)?;\n", guard));
            code.push_str(&format!("{}}}\n", guard));
            code.push_str(&format!("{}}}\n", guard));
        } else {
            code.push_str(&format!(
                "{}self.{}.encode(buf, version, is_flexible)?;\n",
                guard, rust_name
            ));
        }
    };
    if let Some(c) = cond {
        code.push_str(&format!("        if {} {{\n", c));
        body(&mut code, "            ");
        // When the field exists in a subset of versions, error if it has a non-default
        // value in a version where it doesn't belong.
        let check = non_default_check(&rust_name, field);
        if let Some(check_expr) = check {
            code.push_str(&format!(
                "        }} else if {} {{\n            return Err(SerializationError::FieldNotAvailable {{\n                field: \"{}\",\n                version,\n                api_name: \"{}\",\n            }});\n        }}\n",
                check_expr, field.name, msg_name
            ));
        } else {
            code.push_str("        }\n");
        }
    } else {
        body(&mut code, "        ");
    }
    code
}

/// Generate one field's deserialization code, returning the local variable name.
fn generate_deserialize_field(field: &Field) -> String {
    let rust_name = escape_field_name(&camel_to_snake(&field.name));
    let rust_type = map_field_type(field);
    let cond = field_version_condition(field);
    let mut code = String::new();
    let needs_presence = field.nullable_versions.is_some() && !nullable_has_builtin_flex(field);
    if needs_presence {
        let _inner_type = strip_option_wrapper(&rust_type);
        let decode_expr = |guard: &str| -> String {
            let mut c = String::new();
            c.push_str(&format!("{}if is_flexible {{\n", guard));
            c.push_str(&format!(
                "{}let (present, _) = decode_unsigned_varint(buf)?;\n",
                guard
            ));
            c.push_str(&format!("{}if present == 0 {{\n", guard));
            c.push_str(&format!("{}None\n", guard));
            c.push_str(&format!("{}}} else {{\n", guard));
            c.push_str(&format!(
                "{}Some(KafkaCodec::decode(buf, version, true)?)\n",
                guard
            ));
            c.push_str(&format!("{}}}\n", guard));
            c.push_str(&format!("{}}} else {{\n", guard));
            c.push_str(&format!(
                "{}Some(KafkaCodec::decode(buf, version, false)?)\n",
                guard
            ));
            c.push_str(&format!("{}}}\n", guard));
            c
        };
        if let Some(c) = cond {
            code.push_str(&format!("        let {} = if {} {{\n", rust_name, c));
            code.push_str(&decode_expr("            "));
            code.push_str("        } else {\n");
            code.push_str("            Default::default()\n");
            code.push_str("        };\n");
        } else {
            code.push_str(&format!("        let {} = \n", rust_name));
            code.push_str(&decode_expr("            "));
            code.push_str("        ;\n");
        }
    } else if let Some(c) = cond {
        if field.tag.is_some() {
            code.push_str(&format!("        let mut {} = if {} {{\n", rust_name, c));
        } else {
            code.push_str(&format!("        let {} = if {} {{\n", rust_name, c));
        }
        if field.tag.is_some() {
            code.push_str("            if is_flexible { Default::default() } else {\n");
            code.push_str("                KafkaCodec::decode(buf, version, is_flexible)?\n");
            code.push_str("            }\n");
        } else {
            code.push_str("            KafkaCodec::decode(buf, version, is_flexible)?\n");
        }
        code.push_str("        } else {\n");
        code.push_str("            Default::default()\n");
        code.push_str("        };\n");
    } else {
        if field.tag.is_some() {
            code.push_str(&format!(
                "        let mut {} = if is_flexible {{ Default::default() }} else {{\n",
                rust_name
            ));
            code.push_str("            KafkaCodec::decode(buf, version, is_flexible)?\n");
            code.push_str("        };\n");
        } else {
            code.push_str(&format!(
                "        let {} = KafkaCodec::decode(buf, version, is_flexible)?;\n",
                rust_name
            ));
        }
    }
    code
}

/// Generate a `KafkaCodec` impl for a struct (main or nested).
fn encode_impl_body(_struct_name: &str, fields: &[Field]) -> String {
    let mut code = String::new();
    code.push_str("    fn encode<B: BufMut>(&self, buf: &mut B, version: ApiVer, is_flexible: bool) -> Result<(), SerializationError> {\n");
    for f in fields {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        let cond = field_version_condition(f);
        let has_version_gate = cond.is_some();
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        let is_tagged = f.tag.is_some();

        if is_tagged && has_version_gate {
            // Collapse version gate and !is_flexible into one condition
            code.push_str(&format!(
                "        if {} && !is_flexible {{\n",
                cond.as_ref().unwrap()
            ));
        } else if let Some(ref c) = cond {
            code.push_str(&format!("        if {} {{\n", c));
        }
        if needs_presence {
            code.push_str("            if is_flexible {\n");
            code.push_str(&format!(
                "                if let Some(ref val) = self.{} {{\n",
                rust_name
            ));
            code.push_str("                    encode_unsigned_varint(1u64, buf);\n");
            code.push_str("                    val.encode(buf, version, true)?;\n");
            code.push_str("                } else {\n");
            code.push_str("                    encode_unsigned_varint(0u64, buf);\n");
            code.push_str("                }\n");
            code.push_str("            } else {\n");
            code.push_str(&format!(
                "                if let Some(ref val) = self.{} {{\n",
                rust_name
            ));
            code.push_str("                    val.encode(buf, version, false)?;\n");
            code.push_str("                }\n");
            code.push_str("            }\n");
        } else if is_tagged && !has_version_gate {
            code.push_str(&format!(
                "            if !is_flexible {{ self.{}.encode(buf, version, is_flexible)?; }}\n",
                rust_name
            ));
        } else {
            code.push_str(&format!(
                "            self.{}.encode(buf, version, is_flexible)?;\n",
                rust_name
            ));
        }
        if has_version_gate {
            code.push_str("        }\n");
        }
    }
    code.push_str("        if is_flexible {\n");
    code.push_str(&generate_tagged_encode_body(fields));
    code.push_str("        }\n");
    code.push_str("        Ok(())\n");
    code.push_str("    }\n");
    code
}

fn decode_impl_body(_struct_name: &str, fields: &[Field]) -> String {
    let mut code = String::new();
    code.push_str("    fn decode<B: Buf>(buf: &mut B, version: ApiVer, is_flexible: bool) -> Result<Self, SerializationError> {\n");
    for f in fields {
        let rust_name = escape_var_name(&camel_to_snake(&f.name));
        let rust_type = map_field_type(f);
        let cond = field_version_condition(f);
        let has_version_gate = cond.is_some();
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        let is_tagged = f.tag.is_some();
        let _inner_type = if needs_presence {
            strip_option_wrapper(&rust_type)
        } else {
            String::new()
        };
        if needs_presence {
            // Presence marker + sub-message decode
            if let Some(ref c) = cond {
                code.push_str(&format!("        let {} = if {} {{\n", rust_name, c));
            }
            code.push_str(&format!("        let {} = if is_flexible {{\n", rust_name));
            code.push_str("            let (present, _) = decode_unsigned_varint(buf)?;\n");
            code.push_str("            if present == 0 {\n");
            code.push_str("                None\n");
            code.push_str("            } else {\n");
            code.push_str("                Some(KafkaCodec::decode(buf, version, true)?)\n");
            code.push_str("            }\n");
            code.push_str("        } else {\n");
            code.push_str("            Some(KafkaCodec::decode(buf, version, false)?)\n");
            code.push_str("        };\n");
            if has_version_gate {
                code.push_str("        } else {\n");
                code.push_str("            Default::default()\n");
                code.push_str("        };\n");
            }
        } else if let Some(ref c) = cond {
            // Version-gated non-nullable field
            if is_tagged {
                code.push_str(&format!("        let mut {} = if {} {{\n", rust_name, c));
            } else {
                code.push_str(&format!("        let {} = if {} {{\n", rust_name, c));
            }
            if is_tagged {
                code.push_str("            if is_flexible { Default::default() } else {\n");
                code.push_str("                KafkaCodec::decode(buf, version, is_flexible)?\n");
                code.push_str("            }\n");
            } else {
                code.push_str("            KafkaCodec::decode(buf, version, is_flexible)?\n");
            }
            code.push_str("        } else {\n");
            code.push_str("            Default::default()\n");
            code.push_str("        };\n");
        } else if is_tagged {
            // Tagged field: skip when flexible
            code.push_str(&format!(
                "        let mut {} = if is_flexible {{ Default::default() }} else {{\n",
                rust_name
            ));
            code.push_str("            KafkaCodec::decode(buf, version, is_flexible)?\n");
            code.push_str("        };\n");
        } else {
            // Unconditional field
            code.push_str(&format!(
                "        let {} = KafkaCodec::decode(buf, version, is_flexible)?;\n",
                rust_name
            ));
        }
    }
    code.push_str("        if is_flexible {\n");
    code.push_str(&generate_tagged_decode_body(fields));
    code.push_str("        }\n");
    code.push_str(&format!(
        "        Ok(Self {{ {} }})\n",
        fields
            .iter()
            .map(|f| {
                let field_name = escape_field_name(&camel_to_snake(&f.name));
                let var_name = escape_var_name(&camel_to_snake(&f.name));
                if field_name == var_name {
                    field_name
                } else {
                    format!("{}: {}", field_name, var_name)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    ));
    code.push_str("    }\n");
    code
}

fn generate_header_impl(struct_name: &str, fields: &[Field]) -> String {
    let mut code = String::new();
    if struct_name == "RequestHeader" {
        // RequestHeader inherent encode/decode
        code.push_str("impl RequestHeader {\n");
        code.push_str("    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), SerializationError> {\n");
        code.push_str("        let version = ApiVer::new(self.request_api_version);\n");
        code.push_str(
            "        // Per KIP-511, ApiVersions (key 18) always uses header v1 (non-flexible)\n",
        );
        code.push_str("        let is_flexible = self.request_api_key != 18 && crate::generated::is_flexible_api(self.request_api_key, self.request_api_version);\n");
        for f in fields {
            let rust_name = escape_field_name(&camel_to_snake(&f.name));
            let cond = field_version_condition(f);
            if let Some(ref c) = cond {
                code.push_str(&format!("        if {} {{\n", c));
            }
            if f.name == "ClientId" {
                code.push_str(&format!(
                    "            self.{}.encode(buf, version, false)?;\n",
                    rust_name
                ));
            } else {
                code.push_str(&format!(
                    "            self.{}.encode(buf, version, is_flexible)?;\n",
                    rust_name
                ));
            }
            if cond.is_some() {
                code.push_str("        }\n");
            }
        }
        code.push_str("        if is_flexible {\n");
        code.push_str("            encode_unsigned_varint(0u64, buf);\n");
        code.push_str("        }\n");
        code.push_str("        Ok(())\n");
        code.push_str("    }\n");
        code.push_str(
            "    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, SerializationError> {\n",
        );
        code.push_str("        let request_api_key = i16::decode(buf, ApiVer::new(0), false)?;\n");
        code.push_str(
            "        let request_api_version = i16::decode(buf, ApiVer::new(0), false)?;\n",
        );
        code.push_str(
            "        // Header is flexible at v2+ (KIP-511: ApiVersions always uses v1)\n",
        );
        code.push_str(
            "        let is_flexible = request_api_key != 18 && crate::generated::is_flexible_api(request_api_key, request_api_version);\n",
        );
        code.push_str(
            "        let correlation_id = i32::decode(buf, ApiVer::new(0), is_flexible)?;\n",
        );
        code.push_str("        // ClientId is ALWAYS classic nullable string (KIP-511)\n");
        code.push_str("        let client_id = if 1 <= request_api_version {\n");
        code.push_str(
            "            <Option<String> as KafkaCodec>::decode(buf, ApiVer::new(0), false)?\n",
        );
        code.push_str("        } else {\n");
        code.push_str("            Default::default()\n");
        code.push_str("        };\n");
        code.push_str("        if is_flexible {\n");
        code.push_str("            let (_tag_count, _) = decode_unsigned_varint(buf)?;\n");
        code.push_str("        }\n");
        code.push_str("        Ok(Self { request_api_key, request_api_version, correlation_id, client_id })\n");
        code.push_str("    }\n");
        code.push_str("}\n");
    } else if struct_name == "ResponseHeader" {
        // ResponseHeader: peek_correlation_id + decode with is_flexible
        code.push_str("impl ResponseHeader {\n");
        code.push_str(
            "    pub fn peek_correlation_id(buf: &[u8]) -> Result<i32, SerializationError> {\n",
        );
        code.push_str("        let mut cur: &[u8] = buf;\n");
        code.push_str("        i32::decode(&mut cur, ApiVer::new(0), false)\n");
        code.push_str("    }\n");
        code.push_str("    pub fn decode<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, SerializationError> {\n");
        code.push_str("        let correlation_id = i32::decode(buf, ApiVer::new(0), false)?;\n");
        code.push_str("        if is_flexible {\n");
        code.push_str("            let (_tag_count, _) = decode_unsigned_varint(buf)?;\n");
        code.push_str("        }\n");
        code.push_str("        Ok(Self { correlation_id })\n");
        code.push_str("    }\n");
        code.push_str("}\n");
    }
    code
}

fn generate_kafka_codec_impl(struct_name: &str, fields: &[Field]) -> String {
    let mut code = String::new();
    code.push_str(&format!("impl KafkaCodec for {} {{\n", struct_name));
    code.push_str(&encode_impl_body(struct_name, fields));
    code.push('\n');
    code.push_str(&decode_impl_body(struct_name, fields));
    code.push_str("}\n");
    code
}

fn collect_nested_structs(fields: &[Field], out: &mut BTreeMap<String, Vec<Field>>) {
    for field in fields {
        if !field.fields.is_empty() {
            let struct_name = array_inner_type(&field.field_type)
                .unwrap_or_else(|| field_type_to_struct_name(&field.field_type));
            out.entry(struct_name.clone())
                .or_insert_with(|| field.fields.clone());
            // Recurse into nested fields
            collect_nested_structs(&field.fields, out);
        }
    }
}

// ---------------------------------------------------------------------------
// Struct generation
// ---------------------------------------------------------------------------

fn generate_struct(struct_name: &str, fields: &[Field], _versions: &str) -> String {
    let mut code = String::new();

    code.push_str("#[derive(Clone, Debug, Default, PartialEq)]\n");
    code.push_str(&format!("pub struct {} {{\n", struct_name));

    for field in fields {
        // Doc comment from the `about` field
        if let Some(about) = &field.about {
            code.push_str(&format!("    /// {}\n", about));
        } else {
            // Fallback doc: show name + type
            code.push_str(&format!(
                "    /// {}. Type: {}.\n",
                field.name, field.field_type
            ));
        }

        // For arrays with mapKey inner fields, document the key fields.
        // Only emit when the field will actually become IndexMap (skip
        // non-overlapping composite keys that fall back to Vec).
        let inner_ft = field.field_type.strip_prefix("[]");
        if inner_ft.is_some() && !field.fields.is_empty() {
            let key_fields: Vec<&Field> = field.fields.iter().filter(|f| f.map_key).collect();
            let will_be_indexmap = !key_fields.is_empty()
                && key_fields.iter().all(|kf| !key_has_nullable(kf))
                && (key_fields.len() == 1 || composite_key_versions_overlap(&key_fields));
            if will_be_indexmap {
                for kf in &key_fields {
                    let key_about = kf.about.as_deref().unwrap_or("");
                    if !key_about.is_empty() {
                        code.push_str(&format!(
                            "    /// IndexMap key `{}` ({}): {}\n",
                            kf.name, kf.field_type, key_about
                        ));
                    } else {
                        code.push_str(&format!(
                            "    /// IndexMap key `{}` ({})\n",
                            kf.name, kf.field_type,
                        ));
                    }
                }
            }
        }

        // Optional version note
        if !field.versions.is_empty() && field.versions != "0+" {
            code.push_str(&format!(
                "    /// Available in version {}.\n",
                field.versions
            ));
        }

        let rust_name = escape_field_name(&camel_to_snake(&field.name));
        let rust_type = map_field_type(field);
        code.push_str(&format!("    pub {}: {},\n", rust_name, rust_type));
    }

    code.push_str("}\n");
    code
}

/// Generate a key struct for composite IndexMap keys (multiple mapKey fields).
/// Uses `Hash` + `Eq` derives so it works as an IndexMap key.
fn generate_key_struct(struct_name: &str, fields: &[Field]) -> String {
    let mut code = String::new();
    code.push_str("#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]\n");
    code.push_str(&format!("pub struct {} {{\n", struct_name));
    for field in fields {
        if let Some(about) = &field.about {
            code.push_str(&format!("    /// {}\n", about));
        }
        if !field.versions.is_empty() && field.versions != "0+" {
            code.push_str(&format!(
                "    /// Available in version {}.\n",
                field.versions
            ));
        }
        let rust_name = escape_field_name(&camel_to_snake(&field.name));
        let rust_type = resolve_type(&field.field_type);
        code.push_str(&format!("    pub {}: {},\n", rust_name, rust_type));
    }
    code.push_str("}\n");
    code
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Strip the `Option<...>` wrapper from a type string, returning the inner type.
/// If the type is not wrapped, returns it unchanged.
fn strip_option_wrapper(ty: &str) -> String {
    if let Some(inner) = ty.strip_prefix("Option<") {
        inner.strip_suffix('>').unwrap_or(ty).to_string()
    } else {
        ty.to_string()
    }
}

/// Check whether a field's nullableVersions covers any of its supported versions.
/// If so, the field can be null on the wire and cannot be an IndexMap key.
fn key_has_nullable(field: &Field) -> bool {
    if let Some(ref nv) = field.nullable_versions {
        let field_range = parse_version_range(&field.versions);
        let nullable_range = parse_version_range(nv);
        // The field is nullable if the nullable range overlaps the field's version range.
        nullable_range.0 <= field_range.1 && field_range.0 <= nullable_range.1
    } else {
        false
    }
}

/// Check whether all mapKey fields in a composite key share overlapping
/// version ranges.  Non-overlapping keys (e.g. `Name` 0-12 + `TopicId` 13+)
/// cannot share a single tuple key type and fall back to `Vec`.
fn composite_key_versions_overlap(key_fields: &[&Field]) -> bool {
    if key_fields.len() <= 1 {
        return true;
    }
    let ranges: Vec<(i16, i16)> = key_fields
        .iter()
        .map(|f| parse_version_range(&f.versions))
        .collect();
    for i in 1..ranges.len() {
        if ranges[i].0 > ranges[0].1 || ranges[0].0 > ranges[i].1 {
            return false;
        }
    }
    true
}

/// Map a Kafka field definition to a Rust type string.
///
/// Arrays whose inner struct has `mapKey` fields become `IndexMap<K, V>`
/// where K is the key type and V is the struct (with the key field stripped).
/// Single mapKey produces `IndexMap<KeyType, StructName>`.
/// Multiple mapKey fields with overlapping version ranges produce a key struct:
/// `IndexMap<{StructName}Key, V>` (e.g. `IndexMap<AlterConfigsResourceKey, AlterConfigsResource>`).
/// Fields with non-overlapping composite keys fall back to `Vec`.
fn map_field_type(field: &Field) -> String {
    let inner = field.field_type.strip_prefix("[]");
    let is_map =
        inner.is_some() && !field.fields.is_empty() && field.fields.iter().any(|f| f.map_key);

    if is_map {
        let key_fields: Vec<&Field> = field.fields.iter().filter(|f| f.map_key).collect();
        let inner_name = inner.unwrap();
        let is_nullable = field.nullable_versions.is_some();

        // Non-overlapping composite keys, or any nullable mapKey, fall back to Vec
        if key_fields.iter().any(|kf| key_has_nullable(kf))
            || (key_fields.len() > 1 && !composite_key_versions_overlap(&key_fields))
        {
            let inner_rust = resolve_type(inner_name);
            let base = format!("Vec<{}>", inner_rust);
            return if is_nullable {
                format!("Option<{}>", base)
            } else {
                base
            };
        }

        let key_type = if key_fields.len() == 1 {
            resolve_type(&key_fields[0].field_type)
        } else {
            format!("{}Key", inner_name)
        };
        let base = format!("IndexMap<{}, {}>", key_type, inner_name);
        if is_nullable {
            format!("Option<{}>", base)
        } else {
            base
        }
    } else {
        let base_type = resolve_type(&field.field_type);
        let is_nullable = field.nullable_versions.is_some();
        if is_nullable {
            format!("Option<{}>", base_type)
        } else {
            base_type
        }
    }
}

/// Resolve a Kafka type string to its Rust representation.
fn resolve_type(t: &str) -> String {
    // Array types: []InnerType
    if let Some(inner) = t.strip_prefix("[]") {
        let inner_rust = resolve_type(inner);
        return format!("Vec<{}>", inner_rust);
    }

    match t {
        "int8" => "i8".into(),
        "uint16" => "u16".into(),
        "float64" => "f64".into(),
        "int16" => "i16".into(),
        "int32" => "i32".into(),
        "int64" => "i64".into(),
        "uint32" => "u32".into(),
        "bool" => "bool".into(),
        "string" => "String".into(),
        "bytes" | "records" => "Vec<u8>".into(),
        "uuid" => "[u8; 16]".into(),
        // Any other type name is treated as a nested struct name
        other => other.to_string(),
    }
}

/// For `[]Foo` return `Some("Foo")`, otherwise `None`.
fn array_inner_type(t: &str) -> Option<String> {
    t.strip_prefix("[]").map(|s| s.to_string())
}

/// Convert a Kafka type like `FetchableTopicResponse` into a struct name.
fn field_type_to_struct_name(t: &str) -> String {
    // Strip array prefix first
    let clean = t.strip_prefix("[]").unwrap_or(t);
    clean.to_string()
}

// ---------------------------------------------------------------------------
// Naming helpers
// ---------------------------------------------------------------------------

/// Convert `camelCase` or `PascalCase` to `snake_case`.
fn camel_to_snake(name: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (i, ch) in chars.iter().enumerate() {
        if ch.is_uppercase() && i > 0 {
            // Insert underscore unless preceded by an underscore or uppercase run
            let prev = chars[i - 1];
            if prev != '_' && !prev.is_uppercase() {
                out.push('_');
            } else if i + 1 < chars.len() && !chars[i + 1].is_uppercase() && prev.is_uppercase() {
                // End of an uppercase run — insert underscore before this char
                // (e.g. "APIVersion" → "api_version", not "apiversion")
                if out.len() > 1 {
                    out.push('_');
                }
            } else if prev == '_' {
                // already has underscore
            }
        }
        out.push(ch.to_ascii_lowercase());
    }
    // Cleanup: collapse multiple underscores, strip leading/trailing
    let out = out.replace("__", "_");
    let out = out.trim_matches('_').to_string();
    if out.is_empty() {
        name.to_lowercase()
    } else {
        out
    }
}

/// Escape a field name if it is a Rust keyword.
fn escape_field_name(name: &str) -> String {
    // Rust 2024 edition keywords that could plausibly appear as Kafka field names.
    match name {
        "type" | "ref" | "mut" | "move" | "abstract" | "async" | "await" | "become" | "box"
        | "do" | "dyn" | "enum" | "extern" | "final" | "for" | "impl" | "in" | "let" | "loop"
        | "macro" | "match" | "override" | "priv" | "pub" | "return" | "self" | "static"
        | "struct" | "super" | "trait" | "try" | "typeof" | "unsafe" | "unsized" | "use"
        | "virtual" | "where" | "while" | "yield" | "crate" | "union" => format!("r#{}", name),
        _ => name.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
/// Escape a variable name, also avoiding shadowing common parameters.
fn escape_var_name(name: &str) -> String {
    let base = escape_field_name(name);
    match base.as_str() {
        "version" => format!("{}_val", base),
        "is_flexible" => format!("{}_val", base),
        _ => base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camel_to_snake() {
        assert_eq!(camel_to_snake("throttleTimeMs"), "throttle_time_ms");
        assert_eq!(camel_to_snake("errorCode"), "error_code");
        assert_eq!(camel_to_snake("partitionIndex"), "partition_index");
        assert_eq!(camel_to_snake("APIVersion"), "api_version");
        assert_eq!(camel_to_snake("logStartOffset"), "log_start_offset");
        assert_eq!(camel_to_snake("TopicName"), "topic_name");
        assert_eq!(camel_to_snake("Name"), "name");
        assert_eq!(camel_to_snake("Host"), "host");
    }

    #[test]
    fn test_message_file_name() {
        assert_eq!(
            message_file_name("CreateAclsRequest"),
            "create_acls_request.rs"
        );
        assert_eq!(message_file_name("FetchResponse"), "fetch_response.rs");
        assert_eq!(message_file_name("MetadataRequest"), "metadata_request.rs");
    }

    #[test]
    fn test_resolve_type_primitives() {
        assert_eq!(resolve_type("int8"), "i8");
        assert_eq!(resolve_type("int16"), "i16");
        assert_eq!(resolve_type("int32"), "i32");
        assert_eq!(resolve_type("int64"), "i64");
        assert_eq!(resolve_type("uint32"), "u32");
        assert_eq!(resolve_type("bool"), "bool");
        assert_eq!(resolve_type("string"), "String");
        assert_eq!(resolve_type("bytes"), "Vec<u8>");
        assert_eq!(resolve_type("records"), "Vec<u8>");
        assert_eq!(resolve_type("uuid"), "[u8; 16]");
    }

    #[test]
    fn test_resolve_type_array() {
        assert_eq!(resolve_type("[]string"), "Vec<String>");
        assert_eq!(resolve_type("[]int32"), "Vec<i32>");
        assert_eq!(
            resolve_type("[]FetchableTopicResponse"),
            "Vec<FetchableTopicResponse>"
        );
        assert_eq!(
            resolve_type("[]CreatableAclResult"),
            "Vec<CreatableAclResult>"
        );
    }

    #[test]
    fn test_field_type_to_struct_name() {
        assert_eq!(
            field_type_to_struct_name("FetchableTopicResponse"),
            "FetchableTopicResponse"
        );
        assert_eq!(
            field_type_to_struct_name("[]CreatableAclResult"),
            "CreatableAclResult"
        );
    }

    #[test]
    fn test_array_inner_type() {
        assert_eq!(array_inner_type("[]string"), Some("string".into()));
        assert_eq!(array_inner_type("[]int32"), Some("int32".into()));
        assert_eq!(array_inner_type("int32"), None);
    }

    #[test]
    fn test_generate_simple_struct() {
        let fields = vec![
            Field::new("throttleTimeMs".into(), "int32".into(), "0+".into()),
            Field::new("errorCode".into(), "int16".into(), "0+".into()),
        ];
        let code = generate_struct("SimpleResponse", &fields, "0-1");
        assert!(code.contains("pub struct SimpleResponse {"));
        assert!(code.contains("pub throttle_time_ms: i32,"));
        assert!(code.contains("pub error_code: i16,"));
        assert!(code.contains("#[derive(Clone, Debug, Default, PartialEq)]"));
    }

    #[test]
    fn test_generate_with_nullable() {
        let mut field = Field::new("errorMessage".into(), "string".into(), "0+".into());
        field.nullable_versions = Some("0+".into());
        let code = generate_struct("Test", &[field], "0+");
        assert!(code.contains("pub error_message: Option<String>,"));
    }

    #[test]
    fn test_generate_with_nested() {
        let inner = Field::new("name".into(), "string".into(), "0+".into());
        let mut outer = Field::new("topics".into(), "[]TopicStruct".into(), "0+".into());
        outer.fields = vec![inner];
        let code = generate_struct("Outer", &[outer], "0+");
        assert!(code.contains("pub struct Outer {"));
        assert!(code.contains("pub topics: Vec<TopicStruct>,"));
    }

    #[test]
    fn test_collect_nested_structs() {
        let inner = Field::new("name".into(), "string".into(), "0+".into());
        let mut outer = Field::new("topics".into(), "[]TopicStruct".into(), "0+".into());
        outer.fields = vec![inner];

        let mut nested = BTreeMap::new();
        collect_nested_structs(&[outer], &mut nested);
        assert!(nested.contains_key("TopicStruct"));
        assert_eq!(nested["TopicStruct"].len(), 1);
    }

    #[test]
    fn test_generate_file_contains_both_structs() {
        let inner = Field::new("partitionIndex".into(), "int32".into(), "0+".into());
        let mut outer = Field::new("partitions".into(), "[]PartitionData".into(), "0+".into());
        outer.fields = vec![inner.clone()];
        outer.about = Some("The partitions.".into());

        let msg = MessageStruct {
            api_key: Some(1),
            message_type: MessageType::Response,
            name: "TestResponse".into(),
            valid_versions: "0-1".into(),
            flexible_versions: None,
            common_structs: Vec::new(),
            listeners: Vec::new(),
            latest_version_unstable: false,
            fields: vec![Field::new(
                "throttleTimeMs".into(),
                "int32".into(),
                "0+".into(),
            )],
        };

        // Add a field with nested data
        let mut msg = msg;
        msg.fields.push(outer);

        let source = generate_file(&msg, None);
        assert!(source.contains("pub struct TestResponse"));
        assert!(source.contains("pub struct PartitionData"));
        assert!(source.contains("pub throttle_time_ms: i32"));
        assert!(source.contains("pub partition_index: i32"));
    }
}
