

## Tech Stack
- **React 19.1.0** (App Router, functional components only)
- **TypeScript 5.x** (explicit types always)
- **Tailwind CSS** + **shadcn/ui** + **Lucide React**
- **Zustand** (state), **React Hook Form** (forms), **TanStack Query** (server state)

## Code Quality Standards

### Core Principles
- **Maintainability**: Self-documenting code with clear naming
- **Clarity Over Brevity**: Explicit > cryptic identifiers
- **Concise but Readable**: Minimal code, no unnecessary boilerplate
- **Type Safety**: Explicit types always
- **DRY**: Centralize common values
- **No Hardcoded Text**: NEVER hardcode user-visible text in TSX/JSX. Use `{feature}-texts.ts`
- **Strategic Comments**: Only for complex business logic. Always in English
- **No ARIA rules**: keep code simple with less code, for now no aria rules in components

### CSS Philosophy
- **Tailwind First**: Always use Tailwind over custom CSS
- **No Redundant Styling**: Don't recreate Tailwind effects
- **Component Libraries**: Prefer shadcn/ui pre-styled components
- **Inline by Default**: Extract to files only when used 2+ times

### 🎨 Semantic Tokens (CRITICAL)

**ALWAYS use semantic tokens** instead of hardcoded colors:

```tsx
// ❌ NEVER
<div className="bg-blue-500">Content</div>
<div style={{ backgroundColor: 'var(--accent)' }}>Content</div>

// ✅ ALWAYS
<div className="bg-accent text-accent-foreground">Content</div>
<div className="bg-surface hover:bg-surface-hover">Content</div>
```

**Available Tokens:**
- Accent: `bg-accent`, `bg-accent-hover`, `bg-accent-active`, `text-accent-foreground`
- Surface: `bg-surface`, `bg-surface-hover`, `text-surface-foreground`
- Borders: `border`, `border-hover`, `border-focus`
- Muted: `bg-muted`, `text-muted-foreground`
- Brand: `bg-primary`, `bg-primary-dark`, `bg-primary-light`, `bg-secondary`
- Status: `bg-success/warning/error/info`, `text-{status}-foreground`
- Popover: `bg-popover`, `text-popover-foreground`

**Exception:** ONLY use inline styles for truly dynamic runtime values:
```tsx
// ✅ CORRECT - dynamic team color from DB
<div style={{ backgroundColor: team.color }}>Team {team.name}</div>
```

### UI/UX Guidelines

**Design Principles:**
- Clarity over complexity
- Consistent patterns
- Clear visual hierarchy
- Minimal cognitive load

**Color Hierarchy:**
1. Completed/Success = Green
2. Warning/Revealed = Orange
3. Active/Selected = Blue (semantic accent)
4. Default = Gray
5. Disabled = Muted

**Responsive (mobile-first):**
- Headings: `text-2xl md:text-3xl lg:text-4xl`
- Container padding: `px-4 md:px-6 lg:px-8`
- Touch targets: Min 44px
- Breakpoints: `md:` (768px), `lg:` (1024px), `xl:` (1280px)

## 🏗️ 3-Tier Feature Architecture (CRITICAL)

Each feature MUST follow strict 3-tier separation:

### Tier 1: Pure Logic (`lib/{feature}-logic.ts`)
- **Purpose**: Core business rules, pure functions
- **Characteristics**:
    - ✅ ZERO React dependencies
    - ✅ Easily testable in isolation
    - ✅ Portable (could run on server)
- **Example**: `validateWordSubmission(word, answer)`, `calculateScore(cards)`

### Tier 2: UI Logic & State (`hooks/use-{feature}-*.ts` + `lib/{feature}-store.ts`)
- **Purpose**: Connects logic + state to UI, manages interactions
- **Characteristics**:
    - ✅ ALWAYS depends on React (`useState`, `useEffect`, etc.)
    - ✅ Selects data from Zustand store
    - ✅ Provides ready-to-use functions to components
- **Example**: `useMemoryGame()`, `useCrosswordSettings()`

### Tier 3: Presentation (`components/{feature}-*.tsx`)
- **Purpose**: Renders TSX, displays data
- **Characteristics**:
    - ✅ Should be "dumb" - minimal logic
    - ✅ Calls hooks from Tier 2
    - ✅ Passes functions to event handlers
- **Example**: `MemoryBoard.tsx`, `CrosswordGrid.tsx`

## File Organization (Feature-Based)

```
src/
├── app/
│   ├── {feature}/              # Self-contained feature
│   │   ├── page.tsx
│   │   ├── components/         # {feature}-*.tsx
│   │   ├── hooks/              # use-{feature}-*.ts
│   │   ├── lib/
│   │   │   ├── {feature}-constants.ts    # Enums, numbers (NO text)
│   │   │   ├── {feature}-texts.ts        # ALL user-visible text
│   │   │   ├── {feature}-logic.ts        # Pure business logic (Tier 1)
│   │   │   ├── {feature}-utils.ts        # Data transformation
│   │   │   └── {feature}-store.ts        # Zustand state (Tier 2)
│   │   └── types/
│   │       └── {feature}-types.ts
├── components/ui/              # Reusable shadcn components
├── lib/utils.ts                # Global utilities only
└── hooks/                      # Global reusable hooks
```

**Key Principles:**
- Each feature self-contained in own directory
- ALL files prefixed with feature name (`memory-card.tsx`, NOT `card.tsx`)
- Game-specific code stays within game directory
- Relative imports within features (`./`, `../`)
- Absolute imports only for global (`@/components/ui/*`, `@/lib/utils`)

## Naming Conventions

- Components: `PascalCase` (`MemoryCard`)
- Functions: `camelCase` (`flipCard`)
- Constants: `SCREAMING_SNAKE_CASE` (`MAX_TEAMS`)
- Files: `kebab-case` (`memory-card.tsx`, `use-memory-game.ts`)

## Hooks Strategy

### Placement Rules
- **Feature-Specific**: `src/app/{feature}/hooks/` when uses feature store/logic/types
- **Shared**: `src/hooks/` for generic utility logic (localStorage, API calls, keyboard)

### Best Practices
- **Single Responsibility**: One hook = one clear purpose
- **Descriptive Naming**: `use-{feature}-{purpose}.ts` for feature, `use-{purpose}.ts` for shared
- **Return Pattern**: Objects for complex hooks, arrays for simple (like useState)
- **Composability**: Small composable hooks > monolithic ones

### Anti-Patterns (AVOID)
❌ **Conditional Hooks**: Never call hooks inside conditions/loops/nested functions
❌ **Hook Monoliths**: Avoid hooks that do too many things
❌ **Feature Logic in Shared Hooks**: Don't put game-specific logic in global hooks
❌ **Inconsistent Naming**: Pick one convention and stick to it
❌ **Hook Duplication**: Extract to shared when used in 2+ features

## State Management Strategy

Priority order:
1. **Local state**: `useState` for component-specific data
2. **Shared state**: Zustand for cross-component data
3. **Server state**: TanStack Query for API data
4. **URL state**: Next.js router for navigation data

## Avoiding Duplication

- **Routes**: Use `AppRoutes` from `@/config/routes.ts` for all navigation
- **Constants**: Define reusable values in appropriate config files
- **Enums/Constants**: Use TypeScript enums (no const objects)
- **Types**: Create shared types for common structures
- **String literals**: Never repeat strings across codebase. Centralize in enums or constants

## Development Best Practices

### TypeScript Strict
- No `any` types (use `unknown` instead)
- No `@ts-ignore` (fix the actual issue)
- All props have interfaces
- Return types explicit for complex functions

## Type Usage
- **CLASS**: Runtime behavior/methods only
- **INTERFACE**: Plain object shapes
- **ENUM**: Constants (keys UPPERCASE, values lowercase)
- **TYPE**: Utility types only (Pick, Omit, Partial, &, |)

## Examples
```typescript
// ✅ Good
enum Status { IDLE = 'idle', PLAYING = 'playing' }
interface User { id: string; name: string }
type PartialUser = Pick<User, 'name'> & { extra: boolean }

// ❌ Bad
type Status = 'idle' | 'playing' // Use enum
type User = { name: string } // Use interface
const Status = { IDLE: 'idle' } as const // Use enum
```

### Component Quality
- Single responsibility per component
- Props drilling avoided using Zustand for shared state
- Custom hooks for reusable logic
- For component with single props usa direct approach without interfact Props (ex. export function QueueList({ data }: { data: QueueBrokerSnapshot }) { ... })

### Performance (only when real needed, not alwsy and complicate code)
- `React.memo()` for frequently re-rendering components
- `useMemo()` for expensive calculations
- `useCallback()` for event handlers passed as props
- No inline objects in JSX props (causes re-renders)






