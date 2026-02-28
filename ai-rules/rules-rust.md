## 🦀 Rust Backend Standards

### File Organization (Per Broker)

**Mandatory Structure**:
```
src/brokers/{broker}/
├── mod.rs                    # ONLY exports (public API surface)
├── {broker}_manager.rs       # Facade pattern, routing, lifecycle
├── commands.rs               # Protocol command definitions
├── actor.rs                  # Actor implementation (if using actor model)
├── persistence/              # Persistence layer (if needed)
│   ├── mod.rs
│   ├── writer.rs
│   └── types.rs
└── {domain_entity}.rs        # Core business logic (e.g., queue.rs, topic.rs)
```

**Rules**:
- `mod.rs` is ONLY an exporter, no implementation
- Manager is the single entry point for external interaction
- Business logic isolated within broker directory
- No cross-broker direct dependencies (go through managers)

### Naming Conventions
**Structs/Enums**: `PascalCase` | **Functions**: `snake_case` | **Constants**: `SCREAMING_SNAKE_CASE` | **Files**: `snake_case.rs`

### Testing Strategy

**Rust Tests** (`tests/`):
- **Unit tests**: Inside file `#[cfg(test)]` modules for pure logic
- **Integration tests**: `tests/{broker}.rs` for multi-module flows

### Code Quality Gates
- No magic numbers/strings (use named constants)
- Symmetry maintained with similar brokers
- No refactoring leftovers (old comments, TODO, dead code)
- File structure matches mandatory pattern
- Manager is the only public entry point
