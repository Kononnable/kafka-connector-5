//! Code generator that emits Rust structs from parsed Kafka protocol schemas.
//!
//! The generated code lives in `src/generated/` — one `.rs` file per JSON schema file.
//! A companion test using `expect_test::expect_file![]` verifies the output and can
//! cleanly regenerate it when schemas or the codegen logic change.

use super::structs::{Field, MessageStruct, MessageType};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
        .into_iter()
        .flat_map(|rd| rd.flatten())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    entries.sort_by_key(|e| e.path());

    let mut module_names = Vec::new();

    // First pass: parse all messages into a vector so we can build
    // the request↔response pairing map before generating output.
    let mut parsed: Vec<MessageStruct> = Vec::new();
    for entry in &entries {
        let path = entry.path();
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };
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
    let mut pairs: std::collections::HashMap<i16, (String, String)> =
        std::collections::HashMap::new();
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
    mod_rs.push_str("use crate::traits::ApiRequest;\n");
    mod_rs.push_str("use crate::traits::ApiResponse;\n");
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

    // ----- decode_request_body -----
    mod_rs.push_str("/// Deserialize a request body for the given API key and version.\n");
    mod_rs.push_str("pub fn decode_request_body(api_key: i16, version: i16, body: &[u8]) -> Result<String, String> {\n");
    mod_rs.push_str("    let ver = crate::traits::ApiVersion::new(version);\n");
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
    mod_rs.push_str("    let ver = crate::traits::ApiVersion::new(version);\n");
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
    code.push_str("use crate::protocol::serialization::{KafkaSerialize, KafkaDeserialize};\n");
    code.push_str("use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};\n");
    code.push_str("use bytes::{Buf, BufMut, Bytes, BytesMut};\n");
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

    // Nested structs
    for (struct_name, struct_fields) in &nested {
        code.push_str(&generate_struct(struct_name, struct_fields, ""));
        code.push('\n');
    }

    // --- Trait implementation (only for request/response, not headers) ---
    if msg.api_key.is_none() {
        return code; // header messages have no apiKey
    }
    let ak = msg.api_key.unwrap();

    let (min_v, max_v) = parse_version_range(&msg.valid_versions);

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
                "    fn get_min_supported_version() -> crate::traits::ApiVersion {{ crate::traits::ApiVersion::new({}) }}\n",
                min_v
            ));
            code.push_str(&format!(
                "    fn get_max_supported_version() -> crate::traits::ApiVersion {{ crate::traits::ApiVersion::new({}) }}\n",
                max_v
            ));
            code.push_str(&format!(
                "    fn get_min_flexible_version() -> crate::traits::ApiVersion {{ crate::traits::ApiVersion::new({}) }}\n",
                min_flex_version(&msg.flexible_versions)
            ));

            // serialize
            code.push_str("    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {\n");
            if max_v >= min_v {
                code.push_str("        assert!((");
                code.push_str(&format!(
                    "{}) <= version.0 && version.0 <= ({})",
                    min_v, max_v
                ));
                code.push_str(&format!(", \"version {{}} is not supported by {{}} (supported: {}-{})\", version.0, stringify!(Self));\n", min_v, max_v));
            }
            code.push_str(
                "        let is_flexible = version.0 >= Self::get_min_flexible_version().0;\n",
            );

            for field in &msg.fields {
                code.push_str(&generate_serialize_field(field, field));
            }
            code.push_str("        if is_flexible {\n");
            code.push_str(&generate_tagged_encode_body(&msg.fields));
            code.push_str("        }\n");
            code.push_str("        Ok(())\n");
            code.push_str("    }\n");

            // deserialize
            code.push_str("    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {\n");
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

            // Generate KafkaSerialize/KafkaDeserialize impls for this struct.
            code.push_str(&generate_kafka_serialize_impl(&msg.name, &msg.fields));
            code.push('\n');
            code.push_str(&generate_kafka_deserialize_impl(&msg.name, &msg.fields));
            code.push('\n');
            // And for nested structs.
            for (struct_name, struct_fields) in &nested {
                code.push_str(&generate_kafka_serialize_impl(struct_name, struct_fields));
                code.push('\n');
                code.push_str(&generate_kafka_deserialize_impl(struct_name, struct_fields));
                code.push('\n');
            }
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
                "    fn get_min_supported_version() -> crate::traits::ApiVersion {{ crate::traits::ApiVersion::new({}) }}\n",
                min_v
            ));
            code.push_str(&format!(
                "    fn get_max_supported_version() -> crate::traits::ApiVersion {{ crate::traits::ApiVersion::new({}) }}\n",
                max_v
            ));
            code.push_str(&format!(
                "    fn get_min_flexible_version() -> crate::traits::ApiVersion {{ crate::traits::ApiVersion::new({}) }}\n",
                min_flex_version(&msg.flexible_versions)
            ));

            // serialize
            code.push_str("    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {\n");
            if max_v >= min_v {
                code.push_str("        assert!((");
                code.push_str(&format!(
                    "{}) <= version.0 && version.0 <= ({})",
                    min_v, max_v
                ));
                code.push_str(&format!(", \"version {{}} is not supported by {{}} (supported: {}-{})\", version.0, stringify!(Self));\n", min_v, max_v));
            }
            code.push_str(
                "        let is_flexible = version.0 >= Self::get_min_flexible_version().0;\n",
            );

            for field in &msg.fields {
                code.push_str(&generate_serialize_field(field, field));
            }
            code.push_str("        if is_flexible {\n");
            code.push_str(&generate_tagged_encode_body(&msg.fields));
            code.push_str("        }\n");
            code.push_str("        Ok(())\n");
            code.push_str("    }\n");

            // deserialize
            code.push_str("    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {\n");
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

            // Generate KafkaSerialize/KafkaDeserialize impls for this struct.
            code.push_str(&generate_kafka_serialize_impl(&msg.name, &msg.fields));
            code.push('\n');
            code.push_str(&generate_kafka_deserialize_impl(&msg.name, &msg.fields));
            code.push('\n');
            // And for nested structs.
            for (struct_name, struct_fields) in &nested {
                code.push_str(&generate_kafka_serialize_impl(struct_name, struct_fields));
                code.push('\n');
                code.push_str(&generate_kafka_deserialize_impl(struct_name, struct_fields));
                code.push('\n');
            }
        }
        MessageType::Header | MessageType::Data => {}
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
        Some(format!("({}) <= version.0 && version.0 <= ({})", min, max))
    } else if let Some(base) = v.strip_suffix('+') {
        Some(format!("({}) <= version.0", base.trim()))
    } else if let Ok(single) = v.parse::<i16>() {
        Some(format!("version.0 == ({})", single))
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
                format!("({}) <= version.0 && version.0 <= ({})", min, max)
            } else if let Some(base) = v.strip_suffix('+') {
                format!("({}) <= version.0", base.trim())
            } else if v.parse::<i16>().is_ok() {
                format!("version.0 == ({})", v)
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
        code.push_str(
            "            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);\n",
        );
        return code;
    }
    code.push_str("            let mut __tag_count = 0u64;\n");
    for f in &tagged {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        if let Some(check) = non_default_check(&rust_name, f) {
            code.push_str(&format!(
                "            if {} {{ __tag_count += 1; }}\n",
                check
            ));
        }
    }
    code.push_str(
        "            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);\n",
    );
    for f in &tagged {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        let _inner_type = map_field_type(f);
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        let tag_id = f.tag.unwrap();
        if let Some(check) = non_default_check(&rust_name, f) {
            code.push_str(&format!("            if {} {{\n", check));
            code.push_str(&format!("                crate::protocol::serialization::encode_unsigned_varint({}u64, buf);\n", tag_id));
            code.push_str("                let mut __tmp = bytes::BytesMut::new();\n");
            if needs_presence {
                code.push_str(&format!(
                    "                if let Some(ref __val) = self.{} {{\n",
                    rust_name
                ));
                code.push_str("                    __val.encode(&mut __tmp, version, true)?;\n");
                code.push_str("                }\n");
            } else {
                code.push_str(&format!(
                    "                self.{}.encode(&mut __tmp, version, true)?;\n",
                    rust_name
                ));
            }
            code.push_str("                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);\n");
            code.push_str("                buf.put_slice(&__tmp);\n");
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
        code.push_str("            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;\n");
        return code;
    }
    code.push_str("            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;\n");
    code.push_str("            for _ in 0..__tag_count {\n");
    code.push_str("                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;\n");
    code.push_str("                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;\n");
    code.push_str("                match __tag_id {\n");
    for f in &tagged {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        let rust_type = map_field_type(f);
        let tag_id = f.tag.unwrap();
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        if needs_presence {
            let inner_type = strip_option_wrapper(&rust_type);
            code.push_str(&format!(
                "                    {} => {{ {} = Some(<{} as KafkaDeserialize>::decode(buf, version, true)?); }}\n",
                tag_id, rust_name, inner_type
            ));
        } else {
            code.push_str(&format!(
                "                    {} => {{ {} = <{} as KafkaDeserialize>::decode(buf, version, true)?; }}\n",
                tag_id, rust_name, rust_type
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
fn generate_serialize_field(field: &Field, _parent: &Field) -> String {
    let rust_name = escape_field_name(&camel_to_snake(&field.name));
    let cond = field_version_condition(field);
    let mut code = String::new();
    // For nullable struct fields (no builtin flexible), generate presence marker
    let needs_presence = field.nullable_versions.is_some() && !nullable_has_builtin_flex(field);
    let body = |code: &mut String, guard: &str| {
        if needs_presence {
            code.push_str(&format!("{}if is_flexible {{\n", guard));
            code.push_str(&format!(
                "{}if let Some(ref __val) = self.{} {{\n",
                guard, rust_name
            ));
            code.push_str(&format!(
                "{}crate::protocol::serialization::encode_unsigned_varint(1u64, buf);\n",
                guard
            ));
            code.push_str(&format!("{}__val.encode(buf, version, true)?;\n", guard));
            code.push_str(&format!("{}}} else {{\n", guard));
            code.push_str(&format!(
                "{}crate::protocol::serialization::encode_unsigned_varint(0u64, buf);\n",
                guard
            ));
            code.push_str(&format!("{}}}\n", guard));
            code.push_str(&format!("{}}} else {{\n", guard));
            code.push_str(&format!(
                "{}if let Some(ref __val) = self.{} {{\n",
                guard, rust_name
            ));
            code.push_str(&format!("{}__val.encode(buf, version, false)?;\n", guard));
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
                "        }} else if {} {{\n            return Err(SerializationError::Encode(\"field '{}' is not available in this version\"));\n        }}\n",
                check_expr, field.name
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
        let inner_type = strip_option_wrapper(&rust_type);
        let decode_expr = |guard: &str| -> String {
            let mut c = String::new();
            c.push_str(&format!("{}if is_flexible {{\n", guard));
            c.push_str(&format!("{}let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;\n", guard));
            c.push_str(&format!("{}if __present == 0 {{\n", guard));
            c.push_str(&format!("{}None\n", guard));
            c.push_str(&format!("{}}} else {{\n", guard));
            c.push_str(&format!(
                "{}Some(<{} as KafkaDeserialize>::decode(buf, version, true)?)\n",
                guard, inner_type
            ));
            c.push_str(&format!("{}}}\n", guard));
            c.push_str(&format!("{}}} else {{\n", guard));
            c.push_str(&format!(
                "{}Some(<{} as KafkaDeserialize>::decode(buf, version, false)?)\n",
                guard, inner_type
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
            code.push_str(&format!(
                "                <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?\n",
                rust_type
            ));
            code.push_str("            }\n");
        } else {
            code.push_str(&format!(
                "            <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?\n",
                rust_type
            ));
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
            code.push_str(&format!(
                "            <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?\n",
                rust_type
            ));
            code.push_str("        };\n");
        } else {
            code.push_str(&format!(
                "        let {} = <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?;\n",
                rust_name, rust_type
            ));
        }
    }
    code
}

/// Generate a `KafkaSerialize` impl for a struct (main or nested).
fn generate_kafka_serialize_impl(struct_name: &str, fields: &[Field]) -> String {
    let mut code = String::new();
    code.push_str(&format!("impl KafkaSerialize for {} {{\n", struct_name));
    code.push_str("    fn encode<B: BufMut>(&self, buf: &mut B, version: crate::traits::ApiVersion, is_flexible: bool) -> Result<(), crate::traits::SerializationError> {\n");
    for f in fields {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        let cond = field_version_condition(f);
        let has_version_gate = cond.is_some();
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        if let Some(ref c) = cond {
            code.push_str(&format!("        if {} {{\n", c));
        }
        if needs_presence {
            // Nullable struct: presence marker + sub-message encode
            code.push_str("            if is_flexible {\n");
            code.push_str(&format!(
                "                if let Some(ref __val) = self.{} {{\n",
                rust_name
            ));
            code.push_str("                    crate::protocol::serialization::encode_unsigned_varint(1u64, buf);\n");
            code.push_str("                    __val.encode(buf, version, true)?;\n");
            code.push_str("                } else {\n");
            code.push_str("                    crate::protocol::serialization::encode_unsigned_varint(0u64, buf);\n");
            code.push_str("                }\n");
            code.push_str("            } else {\n");
            code.push_str(&format!(
                "                if let Some(ref __val) = self.{} {{\n",
                rust_name
            ));
            code.push_str("                    __val.encode(buf, version, false)?;\n");
            code.push_str("                }\n");
            code.push_str("            }\n");
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
    code.push_str("}\n");
    code
}

fn generate_kafka_deserialize_impl(struct_name: &str, fields: &[Field]) -> String {
    let mut code = String::new();
    code.push_str(&format!("impl KafkaDeserialize for {} {{\n", struct_name));
    code.push_str("    fn decode<B: Buf>(buf: &mut B, version: crate::traits::ApiVersion, is_flexible: bool) -> Result<Self, crate::traits::SerializationError> {\n");
    for f in fields {
        let rust_name = escape_field_name(&camel_to_snake(&f.name));
        let rust_type = map_field_type(f);
        let cond = field_version_condition(f);
        let has_version_gate = cond.is_some();
        let needs_presence = f.nullable_versions.is_some() && !nullable_has_builtin_flex(f);
        let is_tagged = f.tag.is_some();
        let inner_type = if needs_presence {
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
            code.push_str("            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;\n");
            code.push_str("            if __present == 0 {\n");
            code.push_str("                None\n");
            code.push_str("            } else {\n");
            code.push_str(&format!(
                "                Some(<{} as KafkaDeserialize>::decode(buf, version, true)?)\n",
                inner_type
            ));
            code.push_str("            }\n");
            code.push_str("        } else {\n");
            code.push_str(&format!(
                "            Some(<{} as KafkaDeserialize>::decode(buf, version, false)?)\n",
                inner_type
            ));
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
                code.push_str(&format!("                <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?\n", rust_type));
                code.push_str("            }\n");
            } else {
                code.push_str(&format!(
                    "            <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?\n",
                    rust_type
                ));
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
            code.push_str(&format!(
                "            <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?\n",
                rust_type
            ));
            code.push_str("        };\n");
        } else {
            // Unconditional field
            code.push_str(&format!(
                "        let {} = <{} as KafkaDeserialize>::decode(buf, version, is_flexible)?;\n",
                rust_name, rust_type
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
            .map(|f| escape_field_name(&camel_to_snake(&f.name)))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    code.push_str("    }\n");
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

/// Map a Kafka field definition to a Rust type string.
fn map_field_type(field: &Field) -> String {
    let base_type = resolve_type(&field.field_type);
    let is_nullable = field.nullable_versions.is_some();
    if is_nullable {
        format!("Option<{}>", base_type)
    } else {
        base_type
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
    // Rust 2021 edition keywords that could plausibly appear as Kafka field names.
    match name {
        "type" | "ref" | "mut" | "move" | "abstract" | "async" | "await" | "become" | "box"
        | "do" | "dyn" | "enum" | "extern" | "final" | "for" | "impl" | "in" | "let" | "loop"
        | "macro" | "match" | "override" | "priv" | "pub" | "return" | "self" | "static"
        | "struct" | "super" | "trait" | "try" | "typeof" | "unsafe" | "unsized" | "use"
        | "virtual" | "where" | "while" | "yield" => format!("r#{}", name),
        _ => name.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
