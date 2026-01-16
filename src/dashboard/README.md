# Dashboard & Monitoring System

This module handles the real-time monitoring and management interface for the Nexo engine. It follows a **Control Plane** vs **Data Plane** architecture, separating the core message processing from the observation layer.

## Architecture

- **Models (`/models`)**: Defines the "Contract of Truth". These are the Rust structures that represent the state of various brokers (KV, Queues, PubSub, Streams). They are serialized to JSON and consumed by the React frontend.
- **Server (`server.rs`)**: An Axum-based web server that:
    - Serves the React frontend using `rust_embed` (files are compiled into the binary).
    - Provides a REST API (`/api/state`) to fetch the global system snapshot.
- **Frontend (`dashboard-web/`)**: A modern React application that visualizes the engine's state.

## How it works

1. **Snapshotting**: Every broker implements a `get_snapshot()` method that projects its current internal state into the models defined in this directory.
2. **Aggregating**: The `NexoEngine` collects these individual snapshots into a `SystemSnapshot`.
3. **Serving**: The dashboard server provides this snapshot via a JSON API.
4. **Embedding**: During the build process, the React `dist` folder is embedded into the Rust binary, making Nexo a single, zero-dependency executable.

## Development

To update the dashboard UI:
1. Navigate to `dashboard-web/`.
2. Run `npm run build`.
3. Recompile the Rust project.
