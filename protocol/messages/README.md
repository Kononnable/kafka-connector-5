# Kafka Protocol Definitions (Kafka 2.2)

This directory contains Kafka protocol definition files copied from Apache Kafka version 2.2.

## Source Information

These files are copied from the Apache Kafka project, specifically version 2.2, and are used to define the binary protocol structure for the Rust Kafka client implementation.

The original files are located in the Kafka repository under:
`clients/src/main/resources/common/message/`

## Protocol Structure

The protocol definitions are in JSON format and describe:
- Request/response message structures
- Field types and their serialization formats
- API keys and versioning information
- Error codes and their meanings

## License

These files are subject to the Apache License, Version 2.0, as found in the LICENSE file at the root of this repository.

## Usage

The JSON definitions are processed by the Kafka client implementation to generate the appropriate binary serialization logic. This approach allows for maintaining compatibility with the official Kafka protocol while building a Rust-based client.