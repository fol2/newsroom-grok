# Increment 4B entity-resolution authority operations

**Status:** implementation review unit for issue #226
**Parent:** #144
**Authorised base:** `main@da65dbef7b8d6707555f820e7835aada64ed061f`
**Execution boundary:** deterministic repository-owned bilingual fixtures and retained Increment 4A proposals only

Increment 4B records explicit Entity Mentions, Canonical Entities, aliases, resolution proposals and immutable editorial decisions. It preserves uncertainty instead of treating a name, transliteration, confidence score or extractor result as identity authority. SQLite ledger events and immutable decision rows remain authoritative; preferred identity and later graph state are rebuildable derivatives.

No operation in this unit admits an editorial relation, writes governed Neo4j state, invokes Graphiti or a model, creates a Candidate, publishes content or activates production.
This unit authorises no real Graphiti, model or embedding execution.

## Public boundary

Open the authority through the dedicated submodule:

```python
from newsroom.authority.entity_system import (
    open_governed_entity_authority_system,
)
```

The returned `system.entities` facade exposes only typed, authenticated operations:

```text
admit_mention
propose_resolution
decide_resolution
bind_resolution_dependency
merge_entities
split_entity
reverse_lineage
mention / proposal / proposal_version / decision
entity / entity_version / aliases
admission_guard / dependency / dependent_admission_guard
preferred / projection_events_after
merge_decision / split_decision / reversal_decision
```

It exposes no SQLite connection, capability issuer, command grant, arbitrary SQL or Cypher, Graphiti workspace, model provider, graph writer, Candidate writer, publication writer or projection-rebuild method.

Preferred-identity recovery is intentionally outside the ordinary facade:

```python
from newsroom.authority.entity_projection_rebuild import (
    rebuild_governed_entity_preferred_projection,
)
```

That operational function acquires the same sole-writer lock, validates the complete checked authority, revalidates current source and governed-object rights for every entity, inserts only missing derivative rows, and refuses to overwrite divergent state.

## Commands and scopes

| Operation | Required scope | Durable meaning |
| --- | --- | --- |
| `admit_mention` | `authority.entity.mention` | Retain one exact mention derived from one retained 4A Entity Mention Proposal and evidence range. |
| `propose_resolution` | `authority.entity.propose` | Add one immutable version of an explicit identity proposal. |
| `decide_resolution` | `authority.entity.decide` | Commit accept, reject, hold or unresolved editorial authority. |
| `bind_resolution_dependency` | `authority.entity.dependency` | Bind a relation Proposal to the exact entity-resolution Proposal Version whose uncertainty changes its meaning. |
| `merge_entities` | `authority.entity.merge` | Append a merge decision, successor and exact predecessor versions. |
| `split_entity` | `authority.entity.split` | Append a split decision, successors and complete mention partition. |
| `reverse_lineage` | `authority.entity.reverse` | Append a reversal restoring exact identities/versions and superseding prior lineage. |
| Proposal reads | `authority.entity.read_proposals` | Read mentions, proposals, decisions and dependency records without exposing admitted or projected surfaces. |
| Admitted reads | `authority.entity.read_admitted` | Read admitted entities, aliases, lineage decisions and admission guards. |
| Projection reads | `authority.entity.read_projection` | Read preferred identities and ordered admitted projection events. |

The three read scopes are required to be distinct. Every read authenticates and authorises before storage access, verifies the returned authorization provenance, enforces the allow-listed principal and applies finite result limits.

## Checked schema v14

Migration `entity_resolution_authority_v14` advances the authority from schema v13 to v14.

```text
Migration checksum:
sha256:546e81c2419ecb895a1eea7f9c9556931a3e8ad85efe61e878b2fcc25ad72ee9

Complete schema fingerprint:
sha256:47a0421affa099c11cc478220c26d8ce6164cb621057fe9760f410c7dcea8233
```

The migration creates 23 tables plus the checked `entity_dependent_admission_guard` view:

```text
entity_mentions
entity_resolution_proposals
entity_resolution_proposal_versions
entity_resolution_proposal_heads
entity_resolution_decisions
entity_resolution_decision_heads
canonical_entities
canonical_entity_versions
canonical_entity_heads
entity_aliases
entity_mention_resolutions
entity_merge_decisions
entity_merge_predecessors
entity_split_decisions
entity_split_successors
entity_split_allocations
entity_reversal_decisions
entity_reversal_expected_versions
entity_reversal_restorations
entity_reversal_supersessions
entity_resolution_dependencies
entity_preferred_identities
entity_projection_events
```

The upgrade is forward-only, atomic and checked. A newer schema, incomplete migration, changed checksum, missing table/view/trigger, foreign-key violation or different fingerprint fails closed. Historical tables are immutable. Only guarded head/current projections may advance, and they must point to exact retained versions.

## Mention and alias admission

A mention is not accepted from caller text. The authority resolves the exact retained Increment 4A Proposal Envelope and verifies:

- Proposal kind is `ENTITY_MENTION`;
- exact Proposal, Proposal Set, Output, Run and Run Version identities;
- exact Source Definition Version, Item, Revision and Representation;
- one permitted passage and exact evidence byte range;
- exact placeholder bytes, digest, language and normalisation contract; and
- complete current-use permission for every required passage in the immutable Extraction Run.

The caller provides only typed identity, expected digest, language/script/kind, approved normalisation and idempotency data. Source expression is excluded from safe object representations.

A Canonical Entity is created only by an explicit accepted decision. Its identity is independent of names, aliases, URLs, external identifiers, digests, Neo4j IDs and Graphiti private state. An alias retains exact language, script, kind, validity, mention provenance, decision provenance and uncertainty.

## Resolution states and decisions

Resolution Proposals remain `PROPOSED` until a decision is committed. Supported actions are:

| Action | Current state | Entity effect | Terminal |
| --- | --- | --- | --- |
| `ACCEPT` | `ACCEPTED` | Creates or links exact admitted identity and alias. | Yes |
| `REJECT` | `REJECTED` | Creates no admitted identity. | Yes |
| `HOLD` | `HELD` | Preserves ambiguity for later versioned review. | No |
| `UNRESOLVED` | `UNRESOLVED` | Explicitly records that available evidence cannot resolve identity. | No |

A later non-terminal decision must name the exact current decision and version. Concurrent incompatible decisions serialize; one commits and the other receives a typed stale conflict. Concurrent identical commands produce one durable event plus exact replay.

Name equality, Unicode normalisation, translation, transliteration, confidence, embeddings and extractor output never allocate or merge identity automatically. Unknown continuity remains separate identity plus explicit uncertainty.

## Bilingual identity handling

Repository fixtures exercise English and Hong Kong Traditional Chinese mentions. The normaliser performs only the fixed approved operations:

```text
Unicode NFC
Unicode whitespace collapse
case-folding
```

It performs no translation, transliteration, fuzzy matching, embedding comparison or automatic equivalence. A bilingual equivalence proposal can be accepted only after the referenced object mention already has an admitted entity/version, and the accepted identity must match it exactly.

Same-name or similar-name records remain separate unless an explicit decision with exact basis and provenance changes that state.

The separately versioned repository-owned homonym fixture contains two people with the same English transliteration `Chan Chi Ming` and two Traditional Chinese mentions named `陳志明`. Each Proposal Envelope binds a different byte occurrence and organisational context. Cross-context bilingual equivalence is rejected even when placeholder and normalised text are identical; the exact pair of evidence ranges must match the selected mentions.

## Merge, split and reversal

All lineage changes are append-only.

A merge records:

- at least two exact predecessor `{entity_id, entity_version_id}` pairs;
- all basis Resolution Proposal identities;
- one new successor identity/version;
- explicit preferred-continuation semantics; and
- immutable predecessor, successor and projection-event history.

A split records:

- one exact current source entity/version;
- at least two new successor identity/version pairs;
- a complete, non-overlapping partition of every currently admitted source mention; and
- immutable source, successor, allocation and projection-event history.

A reversal targets one exact merge or split decision, verifies every affected current version, appends restoration and supersession rows, advances affected heads, and never deletes or rewrites the target decision.

Separately sorted IDs and versions are not accepted. Each version is carried in a typed entity/version pair, preventing accidental cross-pairing.

## Dependent admission guard

A relation Proposal may bind to one or more exact entity-resolution Proposal Versions. Each dependency records:

- stable dependency identity;
- exact retained `RELATION` Proposal identity and digest;
- exact current entity-resolution Proposal identity, version and digest;
- whether the dependency is material; and
- one authenticated authority event.

A material dependency blocks later relation admission unless its current entity-resolution state is `ACCEPTED`. `PROPOSED`, `HELD`, `UNRESOLVED`, `REJECTED` and `REVERSED` remain blocking because none supplies a current admitted identity for the dependent meaning. Non-material dependencies remain traceable without blocking.

Increment 4B supplies only this prerequisite/guard contract. Predicate registration, relation decisions, assertions, revocation and supersession remain Increment 4C.

## Current rights and deletion

Every public mention, proposal, entity, alias, lineage, dependency and projection read revalidates the exact retained 4A proposal provenance. Because 4A Runs bind all required passages, revoking or tombstoning one required passage, expiring rights, or changing the current Source Definition Version blocks use of every downstream identity derived from that Run.

Historical rows remain retained for audit subject to lawful retention, but blocked material cannot be read as current authority, used by a dependent admission guard or resurrected by projection rebuild.

A deletion in `REQUESTED` state remains usable only while the governed blob and rights remain active. `TOMBSTONED` or `PHYSICALLY_REMOVED` state blocks current use.

## Projection events and rebuild

Every admitted entity version has one or more immutable `EntityProjectionEvent` rows ordered by source ledger sequence. The public stream returns admitted-only typed events after an exact ledger cutoff and never proposal-workspace state.

`entity_preferred_identities` is a derivative current view. Checked startup requires it to agree with the latest immutable event and authoritative entity head. Recovery may recreate missing rows only when:

1. the full schema and retained authority pass integrity checks;
2. no existing row diverges;
3. every entity passes current-rights validation before the first insert; and
4. the reconstructed row exactly matches the latest event and current head.

Rebuild creates no new ledger or projection events and does not rerun extraction. If any rights check or integrity check fails, the transaction inserts no rows.

## Integrity and tamper response

Startup reconstructs and verifies mentions, proposals, decisions, dependencies, entities, versions, aliases, merge/split/reversal decisions, heads, mention resolutions, preferred projections and projection events. It checks canonical bytes/digests, event envelopes, exact upstream identities, chronology, version chains, typed event coverage and current-head consistency.

Raw-SQL evidence covers:

- immutable merge, split, reversal, dependency and projection records;
- trigger-bypassed request/canonical mismatch;
- missing projection-event coverage;
- divergent preferred projection; and
- an entity authority event with its typed record removed.

Any mismatch fails checked reopen. Operators must not repair authoritative history in place.

## Recovery and rollback

### Before merge

A 4B branch can be abandoned without affecting `main`. Delete the branch and its disposable test databases. Do not copy schema-v14 databases into environments still running v13 code.

### After migration, before 4C authority exists

Schema v14 is forward-only. Rollback means:

1. stop all writers;
2. retain the complete database and governed-object store as evidence;
3. restore a pre-v14 database backup for v13 code, or keep v14 code deployed with 4B commands disabled by authorization policy;
4. diagnose and correct through a later migration/version, never by deleting or rewriting committed v14 rows; and
5. rerun checked open, focused entity evidence and all permanent repository gates.

### Projection-only loss

Use `rebuild_governed_entity_preferred_projection` only for missing derivative rows. Do not use it to overwrite divergent rows. A divergent projection indicates integrity failure and requires investigation from an unchanged copy.

### Rights or deletion incident

Immediately remove command/read authorization for the affected principal or source, preserve the ledger, complete the governed deletion process, and verify that ordinary reads, dependent guards and projection rebuild all fail closed. Historical authority must not be manually rewritten to simulate deletion.

## Operational checks

Before enabling any 4B writer in a controlled environment:

- confirm schema version 14, migration checksum and schema fingerprint;
- confirm `PRAGMA foreign_key_check` is empty and `PRAGMA quick_check` is `ok`;
- confirm the exact seven entity command definitions and three distinct read scopes;
- run focused contract, migration, authority, lineage, lifecycle, dependency, integrity, security, concurrency, projection and traceability tests;
- verify no Graphiti/model/network/Neo4j runtime import entered the entity boundary;
- verify zero unresolved P1/P2 review findings and zero unresolved review threads; and
- retain exact-head CI, Authority, Projection, authenticated Neo4j and signed SDLC evidence.

## Permanent workflow hooks

Increment 4B has dedicated files in each applicable permanent focused lane:

```text
newsroom/tests/test_authority_a2a_entity.py
newsroom/tests/test_authority_a2b_entity.py
newsroom/tests/test_projection_b1_entity.py
```

A2a proves exact authenticated command and ledger-event envelopes for mention admission, resolution proposal and resolution decision. A2b proves governed-object revocation blocks all current entity, alias and preferred-identity reads while immutable history remains retained. Projection B1 proves proposed identity emits no event and one explicit accepted decision emits one admitted, typed projection event. Actual Neo4j entity projection remains deliberately deferred to Increment 4E.

## First-boot live-official ACCEPT (Increment 4B operator path)

The fixture lane remains the bilingual Increment 4A Proposal Envelopes used by `newsroom/tests/entity_4b_helpers.py`. That lane must not run against live first-boot extracts.

A separate live-official operator reads the six retained `extraction_runs` / `extraction_outputs` already written by `extract-leads` and records Entity Mentions, resolution proposals, aliases and immutable editorial ACCEPT decisions. A Canonical Entity is created only by an explicit ACCEPT row. The operator binds Source Revision, Discovery Representation and the same admitted representation passages the 4A runs already used. JSON identity is not a passage; mention evidence is the exact 4A byte range inside that passage.

It does not remint `publication.bundle.with_body`, rewrite ledger events 2/3/4/7/15, rerun `extract-leads`, invent RAD-02 / UK-10 rows, admit 4C relations, write Neo4j, enable Graphiti, `AUTO_PUBLISH`, `public_adapter`, Discord, or X-as-publisher. Name equality, transliteration and extractor placeholders never allocate identity.

`REAL_GRAPHITI_RUNTIME_ENABLED` stays `False`. Predicate registration and relation ACCEPT remain Increment 4C / #84.

The operator first applies checked schema **v34** (`live_official_entity_mention_authority_v34`) if the host is still on v33. That migration is trigger-only: it DROP/CREATE `entity_mention_lineage_guard` so a live-official JSON value span (`json.dumps(placeholder)`, including quotes) can be stored as `mention_text` while still matching the exact evidence byte range CHECK. Fixture mentions that copy the placeholder into the evidence span remain valid. It does not rewrite `extraction_runs`, `extraction_outputs`, `news_leads`, or ledger events 2/3/4/7/15. A backup-gated file is written next to the ledger:

```text
/home/box/newsroom/data/authority.sqlite3.pre-v34.sqlite3
```

Do not remint seq 15. Do not rerun `extract-leads`.

After merge, Daniel runs this on the first-boot host (`fol2/newsroom-grok`, home `/home/box/newsroom`) that already has the six extracts. Do not invent those rows. Hold is up; this command stops the host if it is up, writes 4B authority, then restarts if it was up. RAD-02 / UK-10 stay out.

The six extracts already bind these revision/representation pairs:

```text
HK-01        rev=275f12de-193f-49b4-8c3c-93c41964d363  rep=709c3f45-57b8-4945-8114-0dd46cdafdf7
HK-04        rev=f96c14c7-0d4a-4e41-8d55-a6c8b77df0b8  rep=ec15beac-6a33-497b-b167-342390720c56
UK-01        rev=f7bfc7de-ad38-4409-83fb-70d0c5fc8e7c  rep=3a73f1c8-babf-420c-98d4-fef8b8235af1
UK-05        rev=a82909ec-fdd3-4082-aef1-cd4ae783d27d  rep=65a55030-cdf4-46ae-88fb-9a04b1519503
RAD-01       rev=2a4f86a7-866a-47ce-bd01-1aff75ffc2ff  rep=6e1dfac0-07d9-403d-b386-08ca0ff2e00a
X-SEARCH-POSTS rev=d7e2e286-a0a2-4c22-8b50-e53f85e541eb  rep=ef939597-2789-4146-bdbb-e73630be64c4
```

```text
uv run python3 scripts/newsroom_first_boot.py resolve-leads --home /home/box/newsroom
```

Equivalent console script:

```text
newsroom-first-boot resolve-leads --home /home/box/newsroom
```

Expected host result after a successful run:

```text
ok: true
schema_version: 34
extraction_runs: 6
extraction_outputs: 6
entity_mentions: 12
canonical_entities: 12
entity_resolution_decisions: 12 ACCEPT
resolved source_ids: HK-01, HK-04, UK-01, UK-05, RAD-01, X-SEARCH-POSTS
RAD-02 / UK-10: not resolved
editorial_relation_decisions: 0
graphiti / auto_publish / discord / public_adapter / x_as_publisher: false
seq 15 / publication.bundle.with_body.minted: unchanged
PRAGMA user_version = 34
authority.sqlite3.pre-v34.sqlite3 present beside the ledger
```

How to tell 4B landed for the six:

```text
PRAGMA user_version = 34
entity_mentions JOIN extraction_runs JOIN news_leads
  on revision_id + representation_id + passage_id
  = 12 rows, those six pairs only
mention_text bytes = exact 4A evidence span (JSON value including quotes)
canonical_entities.created_by_kind = RESOLUTION
canonical_entities.created_by_decision_id = entity_resolution_decisions.decision_id
entity_resolution_decisions.action = ACCEPT
no extractor placeholder equals a Canonical Entity id
editorial_relation_decisions = 0
```

A second run is idempotent: accepted mentions are skipped with `already-resolved`.

## Stop boundary

Issue #227 / Increment 4C must not begin until #226 is merged to `main` and closed with exact evidence. Completion of 4B does not authorise real Graphiti, model or embedding execution, relation admission, Graphiti proposal-workspace integration, actual-Neo4j bilingual proof, publication or production effects.
