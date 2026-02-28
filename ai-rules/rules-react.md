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
