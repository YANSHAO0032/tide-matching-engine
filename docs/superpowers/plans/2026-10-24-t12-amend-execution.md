# T12 Amend Execution Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` to execute one task at a time. Every task follows RED → GREEN → REVIEW → GATE.

**Goal:** Implement the complete v6 Amend matching-core semantics, with independently implemented Reference and Production paths and deterministic failure atomicity.

**Binding specification:** The user-provided `T12 P4 Amend — Codex 实施任务书` attached on 2026-10-24 is the authority for this plan. It supplements and supersedes the older mentor-mode wording in `assignment/tasks/T12_P4_amend.md` for this authorized implementation.

**Architecture:** `AmendOrder` is transformed through immutable snapshot, candidate calculation, complete read-only preflight and `AmendPlan`; the commit boundary begins only after every business failure has been ruled out. Reference uses its own scan/Vec plans; Production uses its own arena/index plans. Neither calls the other's mutation helpers.

**Global constraints:**

- No implementation after a failed Hard Gate A or B. Hard Gate A must freeze zero-remaining semantics; Hard Gate B fixes event order as `OrderAmended → Trade* → order events` with pre-match candidate remaining.
- Every failure preserves all deterministic business state: orders, quantities, FIFO, priority values and allocators, depth, expiry/session refs and reservation state/version.
- All arithmetic is checked; no system clock, network, RPC, database, account/settlement service, WAL, snapshot, T13 CancelReplace, command dedup, or other future-task scope.
- Reservation verification is real, local, deterministic Ed25519 verification against a preloaded versioned key set. No bypass verifier, optimistic `None`, mock signature algorithm or token-consumption `HashSet`.
- Never parallelize mutation tasks. Every task needs an implementer, task review, and clean review loop before the next task.

## Task 0 — Hard-gate audit and frozen semantics

- Inspect `Qty`, existing event payloads and v6 §7.4.2 to resolve whether `new_total_qty == already_filled` can be represented. Write RED compile/test evidence. If unresolved, record it in handoff and stop before mutation work.
- Confirm event order and precise old/new remaining semantics against the existing event model and v6. Record the frozen order in tests before implementation.
- Inspect actual repository state and record the conflict resolution that the user-supplied implementation taskbook overrides the historical mentor-only task file.

**Gate:** expected RED evidence is meaningful; no production Amend mutation exists.

## Task 1 — Reservation prerequisite

- Add exact v6 `ReservationAdjustmentToken`, minimal `ReservationState`, preloaded versioned `RiskKeySet`, canonical signing bytes and read-only verifier in the prescribed domain/risk boundaries.
- Perform dependency review before adding an Ed25519 crate; cover version/features/default features/transitives/unsafe/network/clock/randomness/platform impact.
- Test every identity/version/fee/coverage/signature rejection, read-only verify and one-time successful apply through version mismatch on replay.

**Gate:** local deterministic signature verification and checked commit are real; no account or remote integration.

## Task 2 — Domain Amend protocol and candidate math

- Add absolute-target `AmendOrder`, explicit `TriggerExpiryPatch` and `MarketProtectionPatch`, mode validation, checked target-total math, `RegularAmendKind`, stable reasons and `OrderAmended` event.
- Test target total after partial fills, zero/invalid targets, no quantity-mode switch, patch semantics, classification and frozen event payload/order.

**Gate:** all domain behaviour is RED→GREEN and wire reasons remain stable.

## Task 3 — Immutable candidates and plan purity

- Before candidate construction, add the user-approved per-order `ReservationState` ownership boundary to both books. Legacy placement stays compatible but never invents a reservation; only a new reservation-aware initialization path associates state with an active order, and Amend rejects missing state. Identity maps never drive business order.
- Build independent Reference and Production snapshots/candidates/plans for regular, iceberg and Stop state without allocation or mutation.
- Add deterministic-state snapshot tests proving plan construction does not touch indexes, queues, priorities, expiry or reservation state.

**Gate:** a plan is strictly read-only and contains all values its no-fail commit needs.

## Task 4 — Reference regular Amend

- Independently implement in-place reduce, same-price requeue and reprice-and-match in the Vec reference model.
- Preserve same-price reduce FIFO; requeue increase at tail with new priority; reprice calculates ordered fills and only assigns a new priority for actual residual rest.

**Gate:** reference unit/property coverage and checked math failures preserve reference state.

## Task 5 — Production regular Amend

- Implement production `AmendPlan` and no-fail commit for arena/index/price-level paths, preserving or reallocating priority exactly as specified.
- Precompute fills, trade-id feasibility, aggregates, expiry change and reservation decision before the commit point; emit `OrderAmended` before reprice trades.

**Gate:** production validators pass after every success and failure; no ordinary business `Err` after commit begins.

## Task 6 — Regular differential and TIF/expiry migration

- Add `Command::Amend` and per-command Reference/Production comparisons, then implement legal GTC/GTD/DAY transitions atomically while retaining priority when only TIF changes.
- Cover all regular fixed scenarios from the binding taskbook, including partial-fill targets, invalid time and event order.

**Gate:** original-order events, orders, depth, FIFO, priorities and expiry/session indexes agree.

## Task 7 — Stop Amend

- Add independent Stop candidate/plan/action. Child quantity/budget/price/TIF/protection/trigger-expiry changes retain StopPriority; stop price/reference changes move trigger queue tail with new StopPriority.
- Atomically migrate trigger expiry and enforce `StopMarketByQuote` mode/budget restrictions.

**Gate:** priority matrix, expiry changes and failed stop amendments preserve all observable state.

## Task 8 — Iceberg Amend and reservation integration

- Extend candidates/plans to maintain visible + hidden = total remaining. Apply exact iceberg priority matrix.
- Integrate the verified reservation preflight and post-preflight apply rule across regular, Stop and iceberg paths.

**Gate:** successful updates change reservation version exactly once; every later failure leaves it unchanged.

## Task 9 — Failure-atomicity properties

- Snapshot and compare all deterministic business state for every invalid quantity, numeric, time, protection, priority, Stop and reservation failure specified by the binding taskbook.

**Gate:** property tests demonstrate no state or allocator drift on rejection.

## Task 10 — Full differential and deterministic tapes

- Extend independent differential commands and projections for every T12 state component.
- Run fixed boundary scenarios, 100k fixed-seed tape and two 10k duplicate-run reproducibility tapes.

**Gate:** all transcripts and state projections match, in original event order.

## Task 11 — Documentation, final review and final gate

- Update actual flow/module documentation, the unique handoff and numerical formulas only if formula semantics changed.
- Run task suites plus `cargo fmt --check`, strict workspace Clippy and workspace tests. Perform whole-branch review and use the finishing-branch workflow; do not merge or push without separate user authorization.

**Gate:** only evidence-backed local completion is reported; T13 remains untouched.
