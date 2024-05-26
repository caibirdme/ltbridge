# ltbridge

## Overview

Grafana Loki and Tempo are excellent tools for log and trace management, providing significant cost savings and data reliability through object storage. However, they often fall short in terms of performance. While there are many log solutions available in the market that handle writing, storing, and querying logs, most of them do not include a user-friendly UI. Developing a good user interface is a complex task. This project aims to leverage the strengths of Grafana to offer more options for observability systems.

## Project Description

This project, named **ltbridge**, implements the HTTP interfaces of Grafana Loki and Tempo. The "l" stands for both "log" and "Loki," while the "t" stands for both "trace" and "Tempo." The term "bridge" signifies its role as a bridge, converting different data sources into the Loki and Tempo formats. ltbridge allows you to add Loki and Tempo as data sources in Grafana, providing services as if they were native Loki or Tempo instances. Internally, it parses LogQL and TraceQL queries, converts them into equivalent SQL or other DSLs, queries the corresponding systems for data, and then transforms the data back into the Loki or Tempo format.

## Advantages

1. **Leverages Grafana's Loki and Tempo Plugins:** By standing on the shoulders of giants, this project maximizes the use of Grafana's existing Loki and Tempo plugins.
2. **Provides an Abstraction Layer:** This intermediate abstraction layer allows users to switch to their preferred observability infrastructure seamlessly without significant changes.

## Current Progress

- **Databend Storage:** Implemented and functional, but requires polishing.
- **Quickwit Storage:** Implemented and functional, but requires polishing.

## Roadmap for near future

1. Add more detailed step-by-step documentation for running the project.
2. Integrate GitHub Actions for automated PR checks, nightly builds, and dependency updates.
3. Improve the current functionalities in Databend and Quickwit, focusing on intelligent completion of index keys and key values.
4. Make it GA

## Technology Stack

- **Language:** The project is developed using Rust.

## Quick Start

**Install Rust and Cross-rs:**

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install Cross-rs
cargo install cross
```

**Build the Project:**

```bash
cross build --target x86_64-unknown-linux-gnu
```

**Quickly start an environment for testing:**

1. **Databend Environment**: Includes MinIO, Databend, Grafana, and pre-configured Loki and Tempo data sources in Grafana
2. **Quickwit Environment**: Includes MinIO, Quickwit, Grafana, and pre-configured Loki and Tempo data sources in Grafana

```bash
cd devenv

# For Databend storage environment
docker compose up -f docker-compose-databend.yaml

# For Quickwit storage environment
docker compose up -f docker-compose-quickwit.yaml
```

**Note:** You need to run ltbridge locally and listen on port 6778 so that the processes in Docker can access it via `http://host.docker.internal:6778`.

## Contributing

contributions from the community are welcomed and really appreciated. Feel free to open issues, submit pull requests, or provide feedback to help us improve this project.

## License

This project is licensed under the [MIT license](./LICENSE).
