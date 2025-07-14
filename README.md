# Solana Block Engine

This repository contains a Solana Block Engine, designed to facilitate efficient transaction processing and MEV (Maximal Extractable Value) opportunities within the Solana ecosystem. It provides gRPC services for authentication, validator interactions, and relayer support.

## Features

*   **Authentication Service:** Secure authentication for clients interacting with the block engine.
*   **Validator Service:** Allows validators to subscribe to packet and bundle streams, and retrieve block builder fee information.
*   **Relayer Support:** Integrates with Jito-compatible transaction relayers to receive and process transactions, including Accounts of Interest (AOI) and Programs of Interest (POI) updates, and expiring packet streams.
*   **gRPC Interface:** All services are exposed via gRPC for high-performance, language-agnostic communication.

## Prerequisites

Before you begin, ensure you have the following installed:

*   **Rust and Cargo:** The Rust programming language and its package manager. You can install them using `rustup`:
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
*   **Protoc:** The Protocol Buffers compiler. This is required for building the `be-proto` crate. Installation instructions vary by OS:
    *   **Linux:** `sudo apt-get install protobuf-compiler`
    *   **macOS (Homebrew):** `brew install protobuf`
    *   **Windows:** Download from [Protobuf GitHub releases](https://github.com/protocolbuffers/protobuf/releases)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/shyunku/jito-block-engine.git
cd jito-block-engine
```

### 2. Build the Project

You can build the project in debug mode (faster compilation, less optimized) or release mode (slower compilation, optimized for performance).

#### Debug Build

```bash
cargo build
```

#### Release Build (Recommended for Production)

```bash
cargo build --release
```

The compiled executable will be located in `target/debug/block-engine` or `target/release/block-engine` respectively.

### 3. Running the Block Engine

To run the block engine, execute the compiled binary. It requires certain arguments to run.

```bash
# For debug build
./target/debug/block-engine --grpc-addr 0.0.0.0:9003 --builder-pubkey <YOUR_BUILDER_PUBKEY> --program <YOUR_PROGRAM_ID> --validator-keypair-path <PATH_TO_YOUR_VALIDATOR_KEYPAIR>

# For release build
./target/release/block-engine --grpc-addr 0.0.0.0:9003 --builder-pubkey <YOUR_BUILDER_PUBKEY> --program <YOUR_PROGRAM_ID> --validator-keypair-path <PATH_TO_YOUR_VALIDATOR_KEYPAIR>
```

You should see output similar to:
```
INFO  be_gateway - Serving gRPC on 0.0.0.0:9003
```

For a full list of configuration options, refer to the "Configuration" section below.

### 4. Integrating with Jito Transaction Relayer

This block engine is designed to work with the `jito-transaction-relayer`. You will need to set up the relayer separately. Refer to the [Jito Relayer GitHub repository](https://github.com/jito-foundation/jito-relayer) for detailed instructions on setting up the relayer.

When running the `jito-transaction-relayer`, ensure you configure it to connect to your block engine instance. Here's an example command:

```bash
jito-transaction-relayer \
   --keypair-path $JITO_HOME/keys/validator.json \
   --signing-key-pem-path ~/solana/jito-relayer/config/keys/private.pem \
   --verifying-key-pem-path ~/solana/jito-relayer/config/keys/public.pem \
   --block-engine-url http://localhost:9003 \
   # Add other relayer-specific arguments as needed
```

**Note:** The `--block-engine-url` should point to the address where your block engine is running (e.g., `http://localhost:9003`).

## Project Structure

*   `crates/bin`: Contains the main executable for the block engine.
*   `crates/core`: Core logic and utilities for the block engine.
*   `crates/gateway`: Implements the gRPC services (Auth, Validator, Relayer). This is where the primary server logic resides.
*   `crates/proto`: Defines the Protocol Buffer messages and gRPC service definitions for communication between components.

## Configuration

The `block-engine` executable supports the following command-line arguments for configuration:

*   `--grpc-addr <HOST:PORT>`: The address for the gRPC server to listen on.
*   `--rpc-url <RPC_URL>`: URL for the Solana RPC endpoint (default: `http://127.0.0.1:8899`).
*   `--tpu-ip <TPU_IP>`: IP address for the Transaction Processing Unit (TPU) (default: `127.0.0.1`).
*   `--tpu-port <TPU_PORT>`: Port for the Transaction Processing Unit (TPU) (default: `8000`).
*   `--tpu-forward-ip <TPU_FORWARD_IP>`: IP address to forward TPU traffic to (default: `127.0.0.1`).
*   `--tpu-forward-port <TPU_FORWARD_PORT>`: Port to forward TPU traffic to (default: `8001`).
*   `--builder-pubkey <PUBKEY>`: Public key of the block builder.
*   `--builder-commission <BUILDER_COMMISSION>`: Builder commission percentage (default: `100`).
*   `--min-bundle-size <MIN_BUNDLE_SIZE>`: Minimum bundle size (default: `100`).
*   `--min-tip-lamports <LAMPORTS>`: Minimum tip in lamports (default: `1000`).
*   `--program <PROGRAM_ID>...`: One or more program IDs to monitor.
*   `--validator-keypair-path <PATH>`: Path to the validator keypair file (e.g., `~/.config/solana/id.json`).

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
