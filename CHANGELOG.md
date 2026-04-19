# Changelog

> Auto-generated from git history. Prefer conventional commits.

## 2026-04-19

### Chores

- `1ff2cc9` chore: consolidate dev dependency manifests

### Fixes

- `5f931c2` fix: rank ai json rails before rendering
- `e558857` fix: skip deploy webhook when secret is missing
- `f1a8645` fix: lazy-load source tiers for test runners
- `6ee9c55` fix: install pytest in deploy workflow
- `6826ef8` fix: scrub catch-up digest rails consistently
- `c2d61db` fix: gate low-quality also-moving lines
- `4e28215` fix: tune digest recency scoring visibility
- `74055fa` fix: harden digest headline ranking and scrubbers

### Features

- `4577497` feat: add auto-deploy webhook workflow
- `d3fdb77` feat: add cross-digest headline dedup memory
- `c5414a0` feat: derive digest source tiers from corroboration history

### Tests

- `cf9dc79` test: add rail order integration coverage

## 2026-04-16

### Fixes

- `e711113` fix(digest): harden translation cleanup
- `2db462d` fix(digest): clean forwarded headline noise
- `0df927c` fix(intake): ignore inbound media posts

### Other

- `d7ca5c7` Add headline-rail cleaning and filtering

## 2026-04-15

### Chores

- `87d57c4` chore: remove project metadata and documentation files

### Other

- `50e6124` Filter low-value quotes/rants from digests

## 2026-04-10

### Other

- `6a1c984` Remove obsolete repo metadata and documentation

## 2026-04-07

### Other

- `53dc4a7` Add strategic trend analysis for queries

## 2026-04-06

### Fixes

- `1a309bf` fix(query): Answer strategic conflict queries
- `614f057` fix: Reject dangling headline fragments
- `73fafed` fix: Preserve complete digest headlines
- `a290b15` fix: Clean digest headlines and text output
- `48e3a5d` fix: Remove media delivery support
- `9585fb4` fix: Correct query digest window titles

### Chores

- `4ccf01a` chore: Remove obsolete project files
- `d13213e` chore: Remove obsolete project scaffolding
- `88340f5` chore: Remove repo metadata and docs
- `8989850` chore: Remove repo config and documentation files
- `0cd7fba` chore: Remove FFmpeg and OCR dependencies

### Refactors

- `8b830de` refactor: Remove OCR support and references
- `1c790e4` refactor: Remove media pipeline and OCR support
- `0d7b833` refactor: Remove media handling paths

## 2026-04-05

### Other

- `b8f01b2` Update README for rolling digests and startup auto-update
- `27bb696` Add smart catch-up recovery for digest backlog
- `5637105` Fix digest backlog recovery and checkpoint handling
- `840d44d` Prefer concrete sentences for fallback headlines
- `8c751bd` Limit headline rail lines & window claim bounds
- `d6011dd` Pull repo on startup and restart if updated
- `f0482b9` Update test to expect IST timezone in timestamp
- `1be18cd` Include timezone in formatted timestamps
- `afba17b` Disable digest queue clearing
- `c95f1ae` Add tests for digest answer title wrapping
- `53568c1` Include query_text in digest answer wrapper
- `ff364eb` Improve digest title generation
- `0c87db0` Add start/end timestamp filters to query context
- `60e5b0c` Add start/end timestamps to query time parsing
- `40db75e` Make format_ts use runtime timezone
- `aa71049` Use runtime timezone in digest window label
- `240774d` Update test to expect raw_text key
- `0c0d647` tests: timezone in archive query date labels
- `f758dd6` Add test for runtime timezone in digest
- `bb927fc` Use runtime timezone helpers for timestamps
- `3b131ae` Remove QUERY_WEB_FALLBACK_ENABLED from .env.example

## 2026-04-04

### Other

- `9461ec5` Add TIMEZONE env var to .env.example
- `9e00eee` Adjust expected hours_back in timezone test
- `2d9c0d5` Add runtime timezone support and docs
- `d4b3617` Add tests for 30-day web search window
- `ffdfac3` Allow up to 30-day verification window
- `03c13fe` Parameterize web crosscheck hours with 30d cap
- `c0eb703` Make web cross-check mandatory; update docs & tests
- `1570c47` Rename 'fallback' to 'cross-check' in news search logs
- `6baf447` Rename query web fallback to crosscheck
- `2c2b34b` Update query web config comments and remove options
- `12410d8` Strip inline 'reported by' source aliases
- `aba3b8d` Add test for seven-day news web query
- `ddf0b88` Use constant for max web query hours
- `c4309ee` Enforce 7-day cap on web query lookback
- `f23ac20` Improve removal of 'according to' attributions
- `000da00` Add tests for digest formatting and output style
- `e5b78fe` Lower digest headline threshold to 30 minutes
- `29156d0` Always require web cross-check for queries
- `af36600` Allow multiple headlines per post in digest
- `2bce5bd` Improve source alias attribution handling
- `8c84bd1` Remove 'follow' promo lines and tweak regex
- `15c31a9` Show headline rail & sentence splitting in digests
- `4f7a28b` Handle highlights-only digest sections
- `07ed2ac` Support headline-rail digest layout and checks
- `3100215` Add digest_output_style and conditional formats
- `33442ac` Allow brief attribution; strip Telegram aliases
- `96b7772` Per-line cleaning for Telegram citations
- `ca02022` Improve query fallback & 7-day expansion logic
- `b2893d0` Refactor query status messages into helpers

## 2026-04-03

### Other

- `8ecc69c` Refactor digest to narrative-first layout

## 2026-04-02

### Other

- `825f7bc` Digest: strip citations, dedupe & refine rules
- `7afd8d8` Refactor digest generation and cleanup
- `0d930c6` Add pytest.ini to set asyncio fixture loop scope
- `b283348` Queue media posts for digest with severity
- `d516eca` Switch to 30-minute rolling digest pipeline
- `e2d57af` Enhance alert labels with emoji prefixes
- `f19b283` Improve AI filter fallback & retry logic
- `d8f9548` Default Codex model set to gpt-5.1-codex-mini
- `7852aeb` Set CODEX_MODEL to gpt-5.1-codex-mini
- `2731a1e` Add runtime presenter, buffer, and dashboard
- `ebad191` Add runtime logging with rotation and redaction

## 2026-03-31

### Other

- `8c6aebd` Tighten exception handling and add debug logs

## 2026-03-30

### Other

- `d782015` Add .github to .gitignore
- `7df0e0c` Limit album upload size and fallback to text
- `6c16ed4` Remove brackets and fallback for oversized media
- `7b762e8` Revamp .env.example with defaults & docs
- `4e62cd0` Increase low-memory threshold and auto-throttle media
- `f8dddf0` Split and thread long query answers
- `f7be6a5` Add PIPELINE_MEDIA_CONCURRENCY and semaphore
- `c3c61b2` Calibrate severity scores and use thresholds
- `28ff3a0` Add low-memory runtime guards and compact refs
- `d2bf021` Retry and reset shared HTTP client on errors
- `ae6cb6c` Strip source promos and paginate media captions
- `5cd1045` Upgrade Codex model and adjust emoji config

## 2026-03-27

### Other

- `4aaa351` Revamp README and harden install scripts

## 2026-03-26

### Other

- `ea17ac9` Add ontology-backed taxonomy and signal analysis
- `72ac1d1` Add context evidence + dynamic delivery context

## 2026-03-25

### Other

- `f78d9af` Add breaking story clustering and handling
- `eb4188b` Add editorial_card layout and editorial formatting
- `3213cb3` Use strftime for defaults; pass created_at in inserts

## 2026-03-24

### Other

- `7b41d58` Add news taxonomy and integrate matching
- `77d5770` Add news_signals and refine feed/explainer logic
- `7aa8864` Support contextual 'Why it matters' in unhinged mode

## 2026-03-23

### Other

- `dcfc10e` Add tests for breaking headline styles
- `b795050` Add 'unhinged' breaking style mode
- `00be567` Interactive OpenAI auth startup repair

## 2026-03-22

### Other

- `c27415f` Refactor TeleUserBot intake pipeline

## 2026-03-20

### Other

- `20e92ce` Add install-all scripts for Ubuntu and Windows

## 2026-03-10

### Other

- `f98e89d` label OCR media translations
- `f7081d8` clean follow-up media headers
- `b103daf` rework why-it-matters into story bridges

## 2026-03-08

### Other

- `13d38ee` refresh README for setup and operations
- `d4d4e5b` add digest pin rotation controls
- `009e9ac` fix digest headline counter regex
- `acca3d8` clarify digest header counts

## 2026-03-07

### Other

- `a6046c2` retry transient Telegram transport errors
- `c001af9` extend OCR language defaults
- `66a0676` retry Telegram login on invalid phone numbers
- `db0e9bf` improve multilingual OCR extraction
- `57c671e` clean follow-up media context phrasing
- `d5dda06` refresh albums before processing captions
- `4ff4a33` harden visual media duplicate detection
- `7c8980a` preserve source reply context for media follow-ups
- `d5afb3b` strengthen media duplicate suppression
- `8ea3391` remove tracked local test files
- `9627b45` tune prompt voice for more human news writing
- `dde4539` improve why-it-matters specificity
- `df5133d` harden env auth refresh failures
- `9bca2ca` refine alert headers and prompt tone
- `bd338e9` fix env auth refresh token reuse
- `0b2f049` add OpenAI auth login and logout commands
- `1ee9f0d` clean up digest language and links
- `34889fa` fix truncated breaking headlines

### Docs

- `541cb0b` docs: update README clone URL

## 2026-03-06

### Other

- `7af7da9` restore OCR translation for media-only posts
- `7922ce7` archive immediate breaking alerts for query search
- `c17cb18` upgrade hybrid query retrieval and answer grounding
- `c705cb6` improve query answers and topical search mode
- `156a572` fix trusted web source filtering
- `59996c2` improve query retrieval for city and topic searches
- `64694cf` improve query evidence ranking and answer quality
- `b3ac921` drop stale bot query updates on startup
- `df5cb5c` clear active webhook before bot query polling
- `9c380d4` route bot pm queries through bot updates
- `eb0d7d3` improve bot query reply target matching
- `46a70a4` fix query reply threading in bot pm
- `ba626d7` refine query replies and simplify media handling

## 2026-03-05

### Docs

- `b9b13ab` docs: document OCR setup and media context behavior
- `97ef200` docs: refresh README for web fallback, auth, and runtime defaults
- `4244e98` docs: refine README for current runtime and deployment flow
- `36d0625` docs: rewrite README with complete usage and operations guide

### Features

- `8f04336` feat(media): generate contextual captions for image/video evidence
- `9be4ef0` feat(ocr): add optional media OCR runtime and config flags
- `00c9d5c` feat: tighten severity routing and add digest schedulers

### Other

- `d762cd1` query: add trusted web fallback and high-risk answer safeguards
- `758d657` config: add web fallback + no-HF dedupe runtime settings
- `d649853` query: send bot-PM answers via bot identity
- `63cab2c` query: send standalone assistant messages instead of self-reply bubbles
- `5fe5e3d` startup: add preflight UX and local auth env setup command
- `14b200f` db: remove dynamic SQL IN update in claim batch
- `08dd6ba` startup: enforce strict preflight config gating before source sync
- `ab61b0b` auth: add cloud-safe bootstrap flow for OAuth env secrets
- `09d806f` digest: fix queue clearing and add reliable hourly/daily cycles
- `dbd4665` ops: add Replit runtime and status web server files
- `bcd0f21` auth: support env-driven OAuth bootstrap and endpoint overrides

## 2026-03-04

### Chores

- `d136a3c` chore: remove typing indicator behavior from query pipeline
- `c8a8924` chore: keep premium emoji map local and untracked
- `f541f7f` chore: add global dedupe env and config flags

### Features

- `21a0817` feat: add humanized vital context and harden session lock handling
- `402f2c4` feat: add premium emoji map rendering for outbound telegram messages
- `0fbbd34` feat: enforce top-level duplicate gate in message routing
- `9e1d39a` feat: implement hybrid global duplicate suppression engine
- `eac3731` feat: add persistent recent_breaking dedupe storage
- `81603f6` feat: add live codex streaming for query and digest outputs
- `300926c` feat: add conversational query assistant over monitored channels
- `c0c978c` feat: implement digest userbot with oauth auth, dedupe, severity routing, and env config

### Fixes

- `57e518b` fix: restrict query replies to saved messages and own bot pm
