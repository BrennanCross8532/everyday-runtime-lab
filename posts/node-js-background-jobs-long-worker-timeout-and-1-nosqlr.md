# Node.js Background Jobs: Long Worker Timeout and 15-Minute Cron Enqueue

**Short answer: keep the cron trigger small, record one durable run, and have a queue worker commit bounded, idempotent slices before the 15-minute limit can decide correctness for you.**

A worker timeout is a boundary in the runtime, not a statement about the amount of business work that exists. A background job queue that treats it as the latter tends to accumulate the least pleasant kind of operational debt: a single invocation grows with the data set, a retry replays an unknown portion of the work, and the only record of progress is an entry in a transient queue dashboard. The design should instead give the scheduled occurrence a durable identity, make each unit of progress independently commit-able, and allow the scheduler to enqueue intent without becoming the keeper of that intent.

The distinction is important for systems that create financial or compliance-relevant records. Delivery can be repeated. Effects must be reconcilable. A duplicate queue message should therefore mean "check the same durable step again," rather than "write the same ledger effect again." This is not a claim that a general-purpose queue provides exactly-once delivery; it is a narrower property that an application can test at the transaction boundary.

Small boundary. Large consequence.

## Start with the timeout as a correctness constraint

Consider a nightly operation that needs to visit a changing collection of records. Putting all of it in one scheduled Node.js handler is attractive because the control flow is visible in one function. The model breaks when the work crosses a fixed worker timeout, when a deployment interrupts it, or when a retry cannot distinguish rows already committed from rows merely fetched. Raising a limit may postpone the first failure, but it also lengthens recovery and widens the interval for which operators have no durable checkpoint.

The safer unit is a logical run plus a sequence of slices. The run is created with a stable schedule key, such as the intended UTC occurrence and the job name. It has a durable status, a cursor, and an upper bound that freezes the population or ordering rule. Each slice reads a limited range, writes its business effects with stable idempotency keys, records a receipt, advances the cursor, and records the intent to enqueue the next slice in the same transaction. The message is a request to examine that state; it is not the state itself.

This arrangement makes the awkward interruption windows explicit. A failure before the transaction commits leaves no completed step, so delivery of the same message can safely try again. A failure after the commit but before a continuation reaches the broker leaves an outbox row that a dispatcher can publish later. A duplicate continuation sees a checkpoint that has already advanced and has nothing new to apply. The queue remains useful for elasticity and wakeups, while the database remains the evidence trail for reconciliation.

The cursor deserves more scrutiny than it usually receives. Offsets are often unsuitable when concurrent inserts or deletes can shift the next page. A stable, ordered key such as `(effective_time, record_id)` is easier to explain in an audit: the checkpoint says that every eligible record through this key has a committed result. If records may change membership during a run, establish an upper bound or materialize the membership first. Otherwise the same run identifier can describe different input populations on different attempts, which defeats reproducibility.

The 15-minute limit should drive a budget, not a target. Reserve time for startup, reads, writes, commit, acknowledgement, and a clean cancellation path; choose the slice size from observed tail behavior, then cap it. Don't select a page size merely because it fits on an average day. The tail matters, and a cold process, a lock wait, or a skewed record can consume the margin that the average hides. An adaptive controller can adjust a later slice, but its observations and its minimum and maximum bounds should be durable so that a retry does not invent a new plan halfway through a run. A practical review begins with a small initial page, a cancellation deadline that expires before the platform deadline, and a record of elapsed read, write, commit, and dispatch time for every completed slice. If the read phase dominates, adding workers may only add contention; if commits dominate, smaller pages can increase the very cost that threatens the budget; if a handful of records dominate, a fixed item count is not a reliable proxy for time. The controller should therefore stop starting new work while enough cancellation margin remains, persist the observed duration alongside the receipt, and make a later attempt choose from a bounded range rather than reacting freely to one unusual result. This is where a timeout becomes a design input: the system can show why a page was chosen, what work it committed, and why it declined to begin another page.

## How should a Node.js cron trigger enqueue split tasks for a long-running queue worker?

The cron trigger should make one short decision: create the logical run if it does not already exist, then arrange for delivery of the first continuation. Cloudflare documents Cron Triggers as invoking a Worker's `scheduled()` handler according to cron schedules expressed in UTC; that is a useful scheduling edge, but the handler should not own a growing batch. The durable boundary belongs in application storage.

For a Node.js service, the queue payload can be deliberately boring: `run_id`, the expected cursor version, and perhaps a signed continuation token if messages cross a trust boundary. It should not carry a mutable copy of the whole job plan. The worker reloads the run and validates that the message still refers to the current step. It then performs exactly one bounded slice and returns. A dispatcher publishes outbox records independently, so publication may happen more than once. That is acceptable because consumers are designed to make repeated delivery harmless.

The following Go example expresses the protocol rather than a framework-specific SDK. The same state transitions apply when the entry point is a Node.js cron handler; Go is used here to keep the transaction and cancellation contract unambiguous. `ApplySlice` is one database transaction, and its implementation must enforce the idempotency and cursor checks described by its name.

```go
package batch

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Continuation struct {
	RunID         string
	ExpectedCursor string
}

type Item struct {
	ID string
}

type Store interface {
	CreateRunAndOutbox(ctx context.Context, scheduleKey string) error
	LoadSlice(ctx context.Context, runID, expectedCursor string, limit int) ([]Item, string, error)
	ApplySlice(ctx context.Context, runID, fromCursor, toCursor string, items []Item) error
}

type Worker struct {
	Store Store
	Limit int
}

func (w Worker) Handle(ctx context.Context, m Continuation) error {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < 30*time.Second {
		return errors.New("execution budget is too small for another slice")
	}

	items, next, err := w.Store.LoadSlice(ctx, m.RunID, m.ExpectedCursor, w.Limit)
	if err != nil {
		return fmt.Errorf("load slice: %w", err)
	}
	if len(items) == 0 {
		return nil
	}

	// This transaction writes effects, a step receipt, the checkpoint, and next outbox intent.
	if err := w.Store.ApplySlice(ctx, m.RunID, m.ExpectedCursor, next, items); err != nil {
		return fmt.Errorf("apply slice: %w", err)
	}
	return nil
}
```

There is a deliberate omission: the example does not enqueue a continuation after `ApplySlice` returns. The transactional implementation records that intent first, and a separate dispatcher delivers it. Directly publishing after a committed slice creates a gap in which the state says progress advanced but no later worker is asked to continue. Publishing before commit creates the inverse gap. The outbox turns both cases into ordinary, idempotent redelivery.

## What must a split-task queue record to survive retries?

An operations team needs more than a final `complete` flag. Retain a run record and a receipt per committed slice with the run identifier, input cursor, output cursor, idempotency scope, commit time, and a count or digest that is useful for reconciliation. Keep sensitive business payloads out of general-purpose logs and receipts unless retention, access control, and data classification have been reviewed. Compliance obligations vary by jurisdiction and data class; I am not sure a universal retention duration would be defensible without that context.

The distinction between an attempt and a committed step is the central audit rule. Several attempts may exist for the same cursor after timeouts, consumer restarts, or ordinary at-least-once delivery. Only the transaction that advances the checkpoint creates the committed receipt. This lets an operator answer the questions that matter: which schedule occurrence created this work, which range reached the ledger boundary, and which range remains. Queue logs can supplement that record, but they should not be the sole source of truth.

Authentication is a separate concern from idempotency. If a continuation crosses an untrusted boundary, authenticate the precise message encoding with a shared secret and compare tags using a constant-time operation. RFC 2104 defines HMAC as keyed hashing for message authentication; it does not encrypt message content. The run record and transaction constraints still carry correctness when a valid message is delivered twice.

Test the failure boundaries on purpose. Inject cancellation before a transaction, during item handling, after a commit, and after an outbox record is written. Then deliver the same continuation again. The acceptance condition is stable business state, one committed receipt per slice, and a recoverable next step. It is much stronger than observing that a worker returned successfully once.

| Execution shape | Durable evidence | Appropriate boundary |
|---|---|---|
| One scheduled handler | One final transaction or explicit checkpoint | A measured, fixed-size task that stays far below the timeout |
| Cron plus queue slices | Run, receipts, checkpoint, and outbox | A growing, cursor-shaped batch with idempotent effects |
| Workflow state machine | Explicit transition history | Waits, approvals, joins, or compensating actions |

## Choose the smallest execution model that preserves evidence

Cron plus sliced queue workers fits a large, homogeneous batch with a stable cursor and independently idempotent work. It lets workers scale separately from the scheduler and provides narrow recovery points, but the catch is real: the team must operate a run table, outbox dispatch, lag monitoring, poison-message review, and a reconciliation procedure. Each slice also adds transaction and dispatch overhead.

Keep a single scheduled invocation for a task whose measured worst case remains comfortably below the limit and whose effects can be committed as one coherent unit. A fixed, small maintenance action may be easier to review that way. Use a durable workflow state machine when the process contains waits, approvals, fan-out, fan-in, or compensating actions; cursor messages are a poor substitute for an explicit graph of state transitions. A database-backed work table can be sufficient when it already serves as the trusted control plane, though lease contention and polling load then become operational concerns.

No model eliminates capacity planning. Operators should alert on run age, queued continuations, outbox age, slice duration percentiles, retry counts, and the distance between an intended schedule occurrence and its first committed slice. A 15-minute worker timeout is manageable when those signals reveal a stuck boundary before the next scheduled run overlaps it.

## How can a team roll out bounded queue work without losing an in-flight run?

Begin by writing the run and receipt schema beside the existing job, including a unique schedule key and an idempotency constraint that can be inspected independently of the queue. Next, run a small, bounded cohort through the slice path while the old handler remains responsible for all other work. Reconcile input counts and durable effects before increasing the cohort; do not infer correctness from throughput alone.

Then enable outbox dispatch and repeated delivery tests, add the operational signals, and establish a documented procedure for pausing publication without deleting run intent. Once each slice has a demonstrable replay story, move the remaining partitions. This sequence leaves a durable audit trail from the first migrated run and makes rollback a routing decision rather than an attempt to reconstruct what an interrupted monolith had already changed.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://developers.cloudflare.com/workers/configuration/cron-triggers/
