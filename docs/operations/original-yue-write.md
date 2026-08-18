# Original 粵語 write (live-official first-boot)

**Status:** live-official operator path for issue #96
**Parent:** #15 / Original 粵語 write
**Execution boundary:** the six already-live first-boot Evidence Packages only

An original 粵語 write is a Cantonese story body bound to an existing Evidence Package. It is not a remint of seq 15 / #71. It carries no publication authority and invents no evidence. AUTO_PUBLISH stays off. No public write.

This document is the live-official path from the six Evidence Packages to original write rows in SQLite. It does not mint a publication.decision, remint `publication.bundle.with_body`, run Increment 5 retrieval, or invent new sources.

SQLite is authority. This operator does not write Neo4j and does not extend the projector. `#76` stays closed. `#87` later.

## Why a dedicated live-official worker

The fixture Increment 7/8/10 Evidence Package and write facades require fixture identities, Increment 5 retrieval contexts, or canary intake. Those are not live first-boot identity. Using them would mint fixture aggregates, extend the projector, invent evidence, remint seq 15, or treat fixture identity as live.

The live-official worker therefore:

- reads the six existing Evidence Packages written by `evidence-leads`;
- binds Source Revision, Discovery Representation, the admitted representation passage, the two 4B ACCEPT Canonical Entities, the 4C ACCEPT Relation Assertion, the Story Candidate, and the Evidence Package;
- writes original 粵語 title + body grounded only in those admitted mention texts and the 4C predicate;
- inserts write identity into `original_writes`, `original_write_receipts`, and `original_write_heads`;
- applies checked v36 `live_official_original_write_authority_v36` (requires `.pre-v36.sqlite3` on a live v35 host);
- leaves v5 `story_candidate_versions` at 0;
- reuses each package's existing binding (no remint, no rewrite of seq 2/3/4/7/15);
- does not invent evidence and does not write a publication.decision.

## First-boot live-official original 粵語 write

The operator reads the six retained Evidence Packages already written by `admit-leads`, `extract-leads`, `resolve-leads`, `relate-leads`, `triage-leads`, and `evidence-leads`. Binding is not `item_id` alone. JSON identity is not a passage.

It does not remint `publication.bundle.with_body`, rewrite ledger events 2/3/4/7/15, rerun `admit-leads` / `extract-leads` / `resolve-leads` / `relate-leads` / `triage-leads` / `evidence-leads` / `project-neo4j`, invent RAD-02 / UK-10 rows, write Neo4j, extend the projector, enable Graphiti, `AUTO_PUBLISH`, `public_adapter`, Discord, or X-as-publisher. It does not grant publication authority.

`REAL_GRAPHITI_RUNTIME_ENABLED` stays `False`. Schema becomes v36 via the checked original-write migration.

After merge, Daniel runs this on the first-boot host (`fol2/newsroom-grok`, home `/home/box/newsroom`) that already has the six Evidence Packages. Do not invent those rows. Hold is up; this command stops the host if it is up, writes original 粵語 authority, then restarts if it was up. RAD-02 / UK-10 stay out.

The six packages already bind these revision/representation pairs:

```text
HK-01          rev=275f12de-193f-49b4-8c3c-93c41964d363  rep=709c3f45-57b8-4945-8114-0dd46cdafdf7
HK-04          rev=f96c14c7-0d4a-4e41-8d55-a6c8b77df0b8  rep=ec15beac-6a33-497b-b167-342390720c56
UK-01          rev=f7bfc7de-ad38-4409-83fb-70d0c5fc8e7c  rep=3a73f1c8-babf-420c-98d4-fef8b8235af1
UK-05          rev=a82909ec-fdd3-4082-aef1-cd4ae783d27d  rep=65a55030-cdf4-46ae-88fb-9a04b1519503
RAD-01         rev=2a4f86a7-866a-47ce-bd01-1aff75ffc2ff  rep=6e1dfac0-07d9-403d-b386-08ca0ff2e00a
X-SEARCH-POSTS rev=d7e2e286-a0a2-4c22-8b50-e53f85e541eb  rep=ef939597-2789-4146-bdbb-e73630be64c4
```

```text
uv run python3 scripts/newsroom_first_boot.py write-leads --home /home/box/newsroom
```

Equivalent console script:

```text
newsroom-first-boot write-leads --home /home/box/newsroom
```

Expected host result after a successful run:

```text
ok: true
schema_version: 36
news_leads: 6
extraction_runs: 6
extraction_outputs: 6
entity_mentions: 12
canonical_entities: 12
entity_resolution_decisions: 12 ACCEPT
editorial_relation_assertions: 6
triage_work_items: 6
story_candidates: 6
story_candidate_heads: 6
story_candidate_admission_receipts_v2: 6
story_candidate_versions: 0
evidence_packages: 6
evidence_package_heads: 6
evidence_package_receipts: 6
original_writes: 6
original_write_heads: 6
original_write_receipts: 6
event_hypotheses_v2: 0
evaluation_handoffs: 0
admitted source_ids: HK-01, HK-04, UK-01, UK-05, RAD-01, X-SEARCH-POSTS
RAD-02 / UK-10: not written
language: yue
authorises_publication / authorises_evidence / authorises_egress / auto_publish: false
invented_evidence / publication_decision / remint: false
graphiti / neo4j / discord / public_adapter / x_as_publisher: false
seq 15 / publication.bundle.with_body.minted: unchanged
PRAGMA user_version = 36
```

How to tell original 粵語 write landed for the six:

```text
original_writes = 6
original_write_heads = 6
original_write_receipts = 6
evidence_packages stays 6
story_candidates stays 6
story_candidate_versions (v5 fixture lane) = 0
event_hypotheses_v2 = 0
evaluation_handoffs = 0
write_bytes bind package_id + candidate_id + revision_id + representation_id + passage_id
  for those six pairs; none of those identities equals item_id
story_title / story_body are original 粵語, not the seq 15 official remint
language = yue
canonical_entity_ids = the two 4B ACCEPT entities on that package
relation_assertion_id = the 4C ACCEPT assertion on that package
authorises_publication = false
invented_evidence = false
publication_decision = false
remint = false
news_leads stays 6
extraction_runs stays 6
canonical_entities stays 12
editorial_relation_assertions stays 6
seq 15 unchanged
schema v36 after the checked original-write migration
```

A second run is idempotent: already-written packages are skipped with `already-written`.

Do not remint seq 15. Do not rerun `admit-leads`, `extract-leads`, `resolve-leads`, `relate-leads`, `triage-leads`, `evidence-leads`, or `project-neo4j`. Do not write Neo4j. Do not AUTO_PUBLISH.
