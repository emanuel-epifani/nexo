# NEXO - Unified Development Rules

> All-in-one high-performance broker for scale-up projects. Single instance, maximum hardware utilization, exceptional developer experience.

**Language:** Italian for communication, English for code/comments.

---

## 🎯 Project Vision & Core Priorities

1. **Performance Philosophy**: Throughput > Latency. Multithread everywhere, squeeze hardware to maximum
2. **Developer Experience First**: Intuitive SDK > raw performance. One broker does everything, zero external dependencies
3. **Scale-Up Not Scale-Out**: Single instance optimized to delay "need something else" moment as long as possible
4. **Indie Hacker Pragmatism**: Fast iteration, minimal abstraction, clean enough to maintain solo

**Core Brokers**: STORE (cache), QUEUE (jobs+DLQ), PUBSUB (realtime topics), STREAM (event logs), DASHBOARD (debug UI)

---

## 🏗️ Architecture Principles (Cross-Stack)

### Consistency is King
- **Symmetry Across Brokers**: If Queue has `QueueManager`, Store MUST have `StoreManager` (not `StoreHandler` or `StoreService`)
- **Uniform Structure**: Every broker follows identical file organization pattern
- **Predictable Naming**: Same conventions everywhere (no creative variations)

### No Magic Values
**Always use named constants for numbers/strings**:
- Thresholds, timeouts, limits → `MAX_RETRIES`, `TIMEOUT_MS`, `MAX_SIZE`
- Test timing → `TEST_OPERATION_TIMEOUT_MS` (not random `sleep(2000)`)
- Protocol values OK hardcoded → `HEADER_SIZE`, `MAGIC_BYTES`

### Configuration Over Hardcoding
**Configurable**: Thresholds, ports, paths, feature flags, test timing
**Hardcoded OK**: Protocol constants, business enums

### Code Splitting Rules (Indie Hacker Balance)
**Split if**: File >500 LOC, reused 2+ times, clear layer separation
**Don't split**: Single use, <50 LOC, no enterprise patterns unless needed

### Dependencies
Always consult before adding. Prefer removing when feasible.

---

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
- **Unit tests**: Inline `#[cfg(test)]` modules for pure logic
- **Integration tests**: `tests/integration/*.rs` for multi-module flows
- **Benchmarks**: `tests/benchmark_*.rs`

### Code Quality Gates
- No magic numbers/strings (use named constants)
- Symmetry maintained with similar brokers
- No refactoring leftovers (old comments, TODO, dead code)
- File structure matches mandatory pattern
- Manager is the only public entry point
- Comments only for complex logic, reflect current state, always English

---

## ⚛️ TypeScript SDK & Dashboard

### SDK Philosophy
- **Developer Experience Above All**: Should be intuitive without reading docs
- **Facade Pattern**: 1-2 export classes per broker type
- **Centralized Complexity**: Socket/parsing logic hidden in `send()` or few core methods
- **Clean Business Logic**: Use `switch` over nested `if/else if` when not compromising performance

### File Organization (SDK)

```
sdk/ts/
├── src/                      # SDK source code
│   ├── client.ts             # Main entry point
│   ├── brokers/
│   │   ├── queue.ts          # Queue broker facade
│   │   ├── store.ts          # Store broker facade
│   │   ├── pubsub.ts         # PubSub broker facade
│   │   └── stream.ts         # Stream broker facade
│   ├── protocol/             # Protocol implementation
│   └── types/                # Type definitions
├── tests/                    # Test suite (not published)
│   ├── brokers/              # Broker E2E tests (one file per broker)
│   │   ├── store.test.ts
│   │   ├── queue.test.ts
│   │   ├── pubsub.test.ts
│   │   ├── stream.test.ts
│   │   ├── cross-broker.test.ts
│   │   ├── reconnection.test.ts
│   │   └── dashboard-prefill.test.ts
│   ├── performance/          # Performance tests
│   ├── utils/                # Test utilities (wait-for, server, etc.)
│   ├── global-setup.ts       # Vitest global setup (build + start server)
│   └── nexo.ts               # Test client singleton (pre-connected)
├── dist/                     # Build output (published)
├── package.json
├── vitest.config.ts
└── .npmignore                # Exclude tests/ from npm package
```

### File Organization (Dashboard)

**Feature-Based Structure**:
```
dashboard/src/
├── app/
│   ├── {feature}/                    # Self-contained feature
│   │   ├── page.tsx
│   │   ├── components/               # {feature}-*.tsx
│   │   ├── hooks/                    # use-{feature}-*.ts
│   │   ├── lib/
│   │   │   ├── {feature}-constants.ts    # Enums, numbers (NO text)
│   │   │   ├── {feature}-texts.ts        # ALL user-visible text
│   │   │   ├── {feature}-logic.ts        # Pure business logic (Tier 1)
│   │   │   ├── {feature}-utils.ts        # Data transformation
│   │   │   └── {feature}-store.ts        # Zustand state (Tier 2)
│   │   └── types/
│   │       └── {feature}-types.ts
├── components/ui/                    # Reusable shadcn components
├── lib/utils.ts                      # Global utilities only
└── hooks/                            # Global reusable hooks
```

**Key Principles**:
- Each feature self-contained in own directory
- ALL files prefixed with feature name (`queue-card.tsx`, NOT `card.tsx`)
- Feature-specific code stays within feature directory
- Relative imports within features (`./`, `../`)
- Absolute imports only for global (`@/components/ui/*`, `@/lib/utils`)

### 3-Tier Architecture (Dashboard)
**Tier 1 - Pure Logic** (`lib/{feature}-logic.ts`): Zero React deps, pure functions
**Tier 2 - State & Hooks** (`hooks/use-{feature}.ts` + `lib/{feature}-store.ts`): React hooks + Zustand
**Tier 3 - UI** (`components/{feature}-*.tsx`): Dumb components, minimal logic

### Naming Conventions
**Components**: `PascalCase` | **Functions**: `camelCase` | **Constants**: `SCREAMING_SNAKE_CASE` | **Files**: `kebab-case`

### TypeScript Standards

**Type Usage**: `interface` for objects | `enum` for constants | `type` for utilities (Pick/Omit/&/|) | `class` for runtime behavior

**Strict Rules**:
- No `any` (use `unknown` - forces type guards before usage)
- No `@ts-ignore` (fix the issue)
- Props have interfaces (or inline for single prop: `{ data }: { data: QueueSnapshot }`)
- Explicit return types for complex functions

### CSS & Styling (Dashboard)

**Follow shadcn/ui Style System**:
- **Components**: ALWAYS use shadcn/ui components (Button, Card, Badge, etc.) - no custom alternatives
- **Colors**: Use ONLY semantic tokens (`bg-background`, `bg-card`, `bg-accent`, `text-foreground`, `text-muted-foreground`, `border`) - NEVER hardcoded colors or arbitrary Tailwind shades
- **Spacing**: Use Tailwind scale (`p-2/4/6/8`, `gap-2/4/6/8`, `space-y-2/4/6/8`) - NEVER custom values like `p-5`, `mt-[13px]`
- **Typography**: Use scale (`text-sm/base/lg/xl/2xl`, `font-normal/medium/semibold/bold`) - NEVER custom sizes like `text-[17px]`
- **Borders**: `rounded-lg` (cards), `rounded-md` (inputs/buttons) - consistent across all components
- **Shadows**: `shadow-sm` (subtle), `shadow-md` (elevated), `shadow-lg` (modals) - no custom shadows
- **Transitions**: ALWAYS add `transition-colors` or `transition-all` with `duration-200/300` for interactive elements
- **Hover States**: ALWAYS present on interactive elements (`hover:bg-accent`, `hover:scale-105`)
- **Inline styles**: ONLY for dynamic runtime values (e.g., `style={{ backgroundColor: team.color }}`)

### State Management
**Priority**: Local (`useState`) → Shared (Zustand) → Server (TanStack Query) → URL (React Router)

---

## 🧪 Testing Standards (Cross-Stack)

### Test Philosophy
- **Deterministic**: Calculate exact wait times (no random timeouts)
- **Independent**: Each test runnable alone, no shared state pollution
- **Complete Coverage**: ALL implemented features tested
- **Unique Names**: Always use `randomUUID()` for resource names (queues, topics, keys)

### Setup/Teardown
**Global** (`global-setup.ts`): Build binary (not in `--release` to be faster), start server once, kill after all tests
**Per-File**: Reuse singleton Nexo client from `nexo.ts` (avoid unnecessary `connect()`/`disconnect()`)
**Exception**: `stream.test.ts` creates 2 shared clients in `beforeAll` for consumer group tests

### Deterministic Test Timing
**Bad**: `await sleep(2000)` (random guess)
**Good**: `await sleep(PERSISTENCE_INTERVAL_MS + 100)` (calculated)
**Better**: Polling with `waitFor(() => condition, { maxAttempts, intervalMs })`

### Test Naming & Organization
**Structure**: `describe('BROKER')` → `describe('FEATURE')` → `it('specific scenario')`
**Naming**: UPPERCASE for macro features, explicit about what's tested, group by feature

### Test Cleanup
No leftover state. Delete created resources in `afterEach` if not auto-cleaned.

---

## 🛡️ Guardrails: Idiomatic Code & Stack Awareness

### Proactive Deviation Alerts
When the user proposes or asks for an approach that goes against established
patterns in the relevant language/framework, the AI MUST:
1. **Flag it explicitly** before implementing: "⚠️ This goes against [pattern/convention] in [language]. The idiomatic approach is [X]. Want to proceed anyway?"
2. **Explain the tradeoff** in 1-2 sentences (not a lecture)
3. **Suggest the idiomatic alternative** with a concrete code example

This applies to: error handling patterns, concurrency models, memory management,
type system usage, project structure, dependency choices, testing patterns.

### De Facto Standards
When multiple valid approaches exist for a problem, prefer the **de facto standard**
in that ecosystem over creative alternatives:
- If a well-known crate/library solves the problem, mention it before hand-rolling
- If the language has a conventional pattern (e.g., Rust's Error trait, Go's error wrapping,
  React's composition model), use it by default
- If the framework has an official recommendation, follow it unless there's a
  measured performance reason not to

### Anti-Footgun Rule
Never silently introduce patterns that are known footguns in the target language:
- Rust: unbounded collections in long-lived tasks, .unwrap() in non-test code,
  String where enums fit, missing Error/Display impls
- TypeScript: `any` types, unhandled promise rejections, mutable shared state
- React: prop drilling >3 levels, useEffect for derived state,
  inline object/function props causing re-renders

When the user explicitly asks for a footgun pattern, implement it but add a
one-line comment noting the risk.

### Brainstorming Mode
During architecture/design discussions, ALWAYS:
1. State the conventional/boring solution first
2. Only then suggest creative alternatives if they have concrete benefits
3. For each option, note: ecosystem maturity, community adoption, maintenance burden

---

**Remember**: Indie hacker project. Pragmatism over perfection. Fast iteration with clean, maintainable code. No enterprise over-engineering, no cowboy coding. Find the balance.