# Fintech Notifications Explained: Durable Fan-Out Across Queue and Channel Boundaries

For a fintech device dashboard, choose a durable recipient inbox whenever an offline operator must recover an alert; use a queue to distribute processing and a live channel to announce that recoverable state has changed. Make the fan-out boundary explicit before selecting the transport.

TL;DR: A queue acknowledgment should mean that the recipient's inbox entry and its pending publication record have committed. A channel send should mean only that a delivery attempt occurred. Require an authenticated recovery API and idempotent rendering before describing the notification experience as durable.

This decision addresses in-app notifications for offline users, including a Node.js API serving a live device-status dashboard. The implementation language does not change the acknowledgment contract; the Go example below isolates that contract so a backend team can review it independently of its HTTP framework. The governing concern is reconciliation: establish which durable records explain each alert, each recipient, and each acknowledged delivery stage.

## Decision and invariants

Adopt a recipient inbox with transactional publication staging, and treat the live channel as an acceleration path. The proposed system streams device status to operators in a fintech environment; a synthetic event might identify device terminal-17, transition sequence 42, and status unavailable. These values are illustrative inputs, not observed production measurements. A dashboard's current-status projection may replace an earlier status, while its notification history retains the transitions that the application has explicitly classified as alerts. Keeping those two read models separate prevents a fast status update from erasing the evidence an operator needs to explain an earlier notification.

Preserve the evidence.

The principal invariant is one logical inbox entry per tenant, recipient, and source event. A second invariant requires a committed publication record for every newly committed inbox entry. A third makes recipient authorization part of both recovery and live subscription, rather than assuming that possession of a device identifier conveys access. These are design requirements for this architecture, not transport guarantees.

Keep processing acknowledgment, client receipt, and operator acknowledgment as separate states. An operator may have two open sessions, neither of which establishes that a human investigated the device. The audit trail should record those distinctions without promoting a successful socket write into an assertion about human attention.

## How should an API recover durable notifications for offline users?

In this design, the recipient inbox survives a disconnected dashboard, while its cursor identifies the last contiguous position the client has applied. Device sequence and inbox position serve different purposes: the former orders transitions for one device; the latter orders a recipient's recovery stream. Do not substitute a timestamp for either contract. An event arriving late must have a defined treatment, such as retaining the alert in history while refusing to replace a newer device-status projection.

The dangerous interval lies between a database commit and a queue acknowledgment. Suppose the inbox transaction commits and the worker stops before acknowledging its input. On redelivery, the same event must resolve to the existing logical entry, with its publication obligation intact. If acknowledgment precedes the commit instead, the proposed recovery mechanism has no durable entry to return after a process stops. The required uniqueness constraint therefore belongs in the persistence layer, where concurrent workers share the same decision, rather than in an individual worker's memory.

Replay needs a boundary too.

Define inbox positions as a committed, ordered stream per recipient, and obtain a committed high-water position for each recovery pass. Read through that position before advancing the durable client cursor. Subscribe before the initial recovery pass, buffer incoming hints, and repeat recovery after reconnects; periodic reconciliation also covers an isolated lost hint while a connection remains open. Duplicate hints are acceptable because they trigger an idempotent read. A partially applied recovery page must not advance the cursor beyond its unprocessed entries. For a synthetic recovery test, let the client apply positions 40 and 41 before stopping midway through position 42. Its saved cursor must remain at 41, so the next recovery includes 42 again; retaining 42's stable event identifier lets the client suppress an already rendered alert while completing the remaining local work. This example deliberately spends another read to preserve the recovery invariant.

Retention is part of the API contract. If a cursor predates retained history, return an explicit reset requirement and a current snapshot, with a visible statement of the unavailable history interval. A snapshot restores the dashboard's present view; it cannot prove which expired alerts an operator previously received.

## Compare the fan-out contracts

The following options are architectural choices for this application. Their differences follow from where this design places durable state and acknowledgment, rather than from a claim about any commercial service.

| Approach | Durable responsibility | Offline recovery | Principal trade-off |
| --- | --- | --- | --- |
| Live channel alone | No inbox required by the application | Requires a separate recovery design | Small critical path, no retained recipient history |
| Shared work queue plus channel | Worker processing obligations | Requires a recipient read model | Worker acknowledgment cannot establish every recipient's receipt |
| Recipient inbox plus staged publication | Recipient entries and pending sends | Read committed entries through a cursor | More storage, authorization checks, and reconciliation work |

Choose the third option when the product promise concerns a particular recipient recovering a particular alert. A shared work queue still has a useful role upstream: it distributes the work of creating recipient entries. For a device event with multiple recipients, define whether recipient membership is captured at event acceptance or resolved later, persist that decision, and track completion per recipient so partial fan-out remains inspectable. The input acknowledgment boundary must include either all required entries or a durable record of the remaining work.

The W3C WebRTC specification describes peer connections and data channels. That transport specification does not define this application's recipient inbox, retention policy, or operator-acknowledgment ledger. Choosing a channel therefore leaves those application contracts to be designed.

## Critical path: commit before acknowledging

The Go sketch below specifies the boundary for one recipient delivery. The store implementation must enforce uniqueness on tenant, recipient, and event ID; append the inbox entry and stage its publication in one transaction; and return success on a duplicate only when the original committed obligation exists. Its ordering and commit semantics require a database implementation and integration tests.

The interface alone does not provide them.

```go
package alerts

import (
    "context"
    "errors"
)

type Delivery struct {
    Tenant    string
    Recipient string
    EventID   string
    DeviceID  string
    Sequence  uint64
    Status    string
}

type InboxStore interface {
    // Commit one inbox entry and its pending publication atomically.
    // Deduplicate by tenant, recipient, and event ID.
    // Return nil for a duplicate only after confirming durable state.
    CommitDelivery(context.Context, Delivery) error
}

type Acknowledge func(context.Context) error

func Handle(ctx context.Context, store InboxStore,
    d Delivery, ack Acknowledge) error {
    if d.Tenant == "" || d.Recipient == "" ||
        d.EventID == "" || d.DeviceID == "" {
        return errors.New("missing delivery identity")
    }
    if err := store.CommitDelivery(ctx, d); err != nil {
        return err
    }
    return ack(ctx)
}
```

Authorization and event validation precede this handler. A separate publisher reads committed pending publications, sends a hint, and records the attempt according to a documented retry policy. A stop after sending but before recording success can produce a duplicate hint; recipient recovery and rendering must tolerate it. This is an exactly-once mindset applied to the logical effect, with explicit retry boundaries. It is not evidence of exactly-once network delivery.

Test the uncertain outcomes: stop before commit, after commit but before acknowledgment, and after publication but before recording its outcome. Also test two workers processing the same event, one unauthorized recovery request, and a client reconnecting during a recovery page. For each case, assert durable entries and cursor progress, rather than merely checking that a connection reopened. During deployment, keep cursor semantics and event identifiers compatible across concurrently running versions. Otherwise a rolling release can change the interpretation of an existing audit record.

## Rejected option and its valid use case

Reject a live-only stream for recoverable operator alerts because it leaves the promised recipient history unspecified. Retain it as a valid option for an explicitly ephemeral device-presence indicator whose product contract permits missed intermediate states and provides a fresh status snapshot on reconnect. That narrower promise may justify less storage and less recovery machinery.

For the durable path, budget for per-recipient writes, retained history, replay reads, and publication retries; no unit-price comparison can settle which of those obligations the application actually needs. Observe oldest pending-publication age, fan-out completion, duplicate suppression, recovery lag, and cursor resets. Correlate attempts with event identifiers while keeping sensitive payloads out of routine logs. These proposed measurements give the team a way to reconcile work that was accepted against work recipients can recover.

Auditability has compliance limits.

This architecture does not establish regulatory compliance, legally required retention periods, or appropriate access rights for a particular institution; those requirements need a separate determination and must constrain history storage, deletion, and access policies. The final acceptance criterion is concrete: after a disconnect and replay, every retained, authorized recipient obligation is recoverable, and every acknowledgment states precisely which boundary it crossed.

## References

- W3C, WebRTC: Real-Time Communication in Browsers. Transport specification for peer connections and data channels; the recipient persistence and audit contracts described above are application design proposals. https://www.w3.org/TR/webrtc/
