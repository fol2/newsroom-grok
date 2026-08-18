# First-boot live official extract (`extract-signal`)

**Issue:** #82
**Parent lock:** #81
**Extract path:** Increment 4A proposal envelopes (`newsroom.extraction`)
**Not extract:** `mint-bundle-body` / `publication_bundle.py` / ledger seq 15

## What this command does

```text
newsroom-first-boot extract-signal --source-id HK-04
```

On an already-admitted first-boot item only, the command:

1. Reads the admitted `item_id` from `ledger_events` (fail closed if missing).
2. Loads official retained bytes for that item (RSS/Atom title+summary, or the
   seq-14 X-SEARCH URL).
3. Runs `LiveOfficialExtractor` to produce Increment 4A `ProposalDraft`
   envelopes whose evidence ranges occur in those bytes.
4. Appends one idempotent `extraction.run.executed` ledger event.

Cover: `HK-04`, `RAD-01`, `RAD-02`, `UK-01`, `UK-05`, `X-SEARCH-POSTS`.
`UK-10` stays skip. `HK-01` is out of this pass so seq 15 is not reminted.

## What this is not

- Not `DeterministicFixtureExtractor` and not fixture bilingual rows marked live.
- Not another official-RSS title+body publication copy.
- Not News Lead / editorial relation admission (#83).
- Not Neo4j populate (#76 stays closed; #84 later).
- Not Graphiti / model spend. `REAL_GRAPHITI_RUNTIME_ENABLED` stays False.

The governed Increment 4A facade remains `FIXTURE_REPLAY_ONLY` for its
checked schema. This first-boot command records live-official 4A proposal
envelopes on the host ledger without inventing fixture entities as live
`extraction_*` increment rows.

## After merge

Run on the Grok Bot host only after this PR merges. Do not rewrite ledger
rows 2/3/4/7/15. Do not call `mint-bundle-body`.
