# PubSub Broker

## Architecture: Actor per Root Topic

```
┌──────────────────────────────────────────────────────────┐
│                    PubSubManager                         │
│       DashMap<root, Sender<RootCommand>>                 │
│                                                          │
│   publish("home/kitchen/temp")                           │
│            │                                             │
│            ▼                                             │
│   ┌────────────────┐  ┌────────────────┐                │
│   │ RootActor      │  │ RootActor      │  ...           │
│   │ "home"         │  │ "sensors"      │                │
│   │ ┌────────────┐ │  │ ┌────────────┐ │                │
│   │ │ Radix Tree │ │  │ │ Radix Tree │ │                │
│   │ │  +/temp    │ │  │ │  /#        │ │                │
│   │ │  kitchen/* │ │  │ └────────────┘ │                │
│   │ └────────────┘ │  └────────────────┘                │
│   └────────────────┘                                    │
│                                                          │
│   Global "#" subscribers → RwLock<HashSet<ClientId>>     │
└──────────────────────────────────────────────────────────┘
```

## Design Rationale

**Why Actor per Root?**
- Radix tree traversal for wildcard matching can be expensive
- Actor serializes all operations on its tree → no internal locks needed
- Different roots (`home/...` vs `sensors/...`) process in parallel
- Natural isolation: one slow tree doesn't block others

**Why separate Global "#"?**
- `#` at root matches ALL topics across ALL trees
- Storing in every tree would be wasteful and complex
- Separate `RwLock<HashSet>` is simple and rarely contended

## Wildcard Matching

| Pattern | Matches | Example |
|---------|---------|---------|
| `home/+/temp` | Single level wildcard | `home/kitchen/temp` ✓ `home/a/b/temp` ✗ |
| `sensors/#` | Multi-level wildcard | `sensors/cpu` ✓ `sensors/a/b/c` ✓ |
| `#` | Everything (global) | All topics on all roots |

## Data Flow

1. **Publish** → extract root → send to `RootActor` → match subscribers → push to clients
2. **Subscribe** → navigate/create path in radix tree → store `ClientId` at node
3. **Retained** → stored at tree node → sent to new subscribers on subscribe

## Cleanup

- `PubSubSession` RAII guard triggers `disconnect()` on drop
- Disconnect removes client from all subscribed patterns via reverse index
