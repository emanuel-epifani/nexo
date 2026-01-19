# Rust Overview (Minimal)

## 1. `rustup` (toolchain manager)

`rustup` installa e gestisce tutto l’ecosistema Rust: `rustc`, `cargo`, stdlib e componenti.

### Installazione
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Comandi essenziali
```bash
rustup default stable
rustup toolchain list
rustup component add clippy rustfmt
```

## 2. `cargo` (build system e package manager)

### Comandi principali
```bash
cargo new myproj --bin     # crea nuovo progetto
cargo build                # build examples
cargo build --release      # build ottimizzata
cargo run                  # build + run
cargo check                # type check (without build)
cargo test                 # esegui test (in tests/)
cargo fmt                  # formatter ufficiale
cargo clippy               # linter ufficiale
cargo doc --open           # genera documentazione
```

## 3. Struttura Progetto

```
myproj/
├── Cargo.toml
├── Cargo.lock
└── src/
    ├── main.rs
    └── lib.rs   # opzionale
```

## 4. Manifest (`Cargo.toml`)

```toml
[package]
name = "myproj"
version = "0.1.0"
edition = "2021"

[dependencies]
serde = { version = "1", features = ["derive"] }
```

## 5. Gestione Dipendenze

```bash
cargo add <crate>
cargo rm <crate>
cargo update
```

## 6. Componenti ufficiali

- **rustc** — compilatore
- **cargo** — build system + package manager
- **rustfmt** — formatter
- **clippy** — linter statico
- **rust-analyzer** — language server per IDE

## 7. Output di Build

```
target/
  ├── debug/
  └── release/
```
