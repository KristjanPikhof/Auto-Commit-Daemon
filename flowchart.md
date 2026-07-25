flowchart TB
  Work(["AI tool makes many<br/>small file edits"])
  Capture["1. Capture immediately<br/>Every edit is stored locally and durably"]
  Strategy{"2. Form commit boundaries"}

  Event["Event mode · default<br/>1 capture → 1 commit"]
  Intent["Intent mode<br/>Group related captures by purpose<br/>Example: 10 captures → 3 logical commits"]

  Replay["3. Replay safely<br/>Rebuild each commit in a scratch index"]
  Safe{"Safe to update<br/>the Git branch?"}

  History(["Normal local Git history<br/>Atomic · reviewable · traceable"])
  Heal["Self-heal the complete chain<br/>Prove · archive · reseed · recapture"]
  Preserve["Stop safely<br/>Preserve queue, HEAD, index, and worktree"]

  Work --> Capture --> Strategy
  Strategy -->|Strict traceability| Event
  Strategy -->|Cleaner commit history| Intent
  Event --> Replay
  Intent --> Replay
  Replay --> Safe
  Safe -->|Yes| History
  Safe -->|No| Heal
  Heal -.->|Resume normal flow| Capture
  Heal -->|Cannot prove safely| Preserve

  classDef external fill:#1e3a5f,stroke:#60a5fa,color:#f8fafc,stroke-width:2px
  classDef process fill:#164e3b,stroke:#34d399,color:#ecfdf5,stroke-width:2px
  classDef decision fill:#78350f,stroke:#fbbf24,color:#fffbeb,stroke-width:2px
  classDef intent fill:#4c1d95,stroke:#c084fc,color:#faf5ff,stroke-width:3px
  classDef guard fill:#3f3f46,stroke:#a1a1aa,color:#fafafa,stroke-width:2px

  class Work external
  class Capture,Replay,History,Heal process
  class Strategy,Safe decision
  class Intent intent
  class Event,Preserve guard
