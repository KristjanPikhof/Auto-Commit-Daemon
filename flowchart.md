flowchart TB
  Work(["Your coding agent<br/>edits files"])
  Capture["ACD saves every edit as it happens<br/>Your work is safe before planning starts"]
  Strategy{"How should these edits<br/>be committed?"}

  Event["Event mode - default<br/>Each saved edit gets its own commit"]
  Intent["Intent mode<br/>Related edits become logical commits<br/>10 saved edits might become 3 commits"]

  Replay["ACD builds each commit safely<br/>using a temporary Git index"]
  Safe{"Is the branch still<br/>safe to update?"}

  History(["Your branch gets normal Git commits<br/>ready to review or undo"])
  Heal["ACD checks all pending work<br/>It finds what landed and saves the rest"]
  Preserve["ACD cannot verify the result<br/>It keeps the captured work and leaves your files alone"]

  Work --> Capture --> Strategy
  Strategy -->|Keep every edit separate| Event
  Strategy -->|Group edits by task| Intent
  Event --> Replay
  Intent --> Replay
  Replay --> Safe
  Safe -->|Yes| History
  Safe -->|No| Heal
  Heal -.->|Continue when safe| Capture
  Heal -->|Still uncertain| Preserve

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
