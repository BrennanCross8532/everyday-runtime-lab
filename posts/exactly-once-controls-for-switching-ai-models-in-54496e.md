# Exactly-Once Controls for Switching AI Models in a Node.js Express API

The operational constraint is ambiguity: after an Express request times out, the backend may not know whether the model invocation ran. **Short answer: put one server-side credential behind a narrow internal API, select models by a versioned capability alias, and persist an idempotent intent before dispatch; keep direct credentials when provider-specific semantics or contractual isolation matter more than easy switching.**

This architecture decision does not make OpenAI, Claude, and Gemini interchangeable. It makes authentication, policy changes, evidence, and reconciliation consistent while allowing each adapter to preserve the semantics that the application has explicitly accepted. The unit of correctness is not “some text came back.” It is a traceable transition from one canonical request to one recorded outcome, including an honest `uncertain` state when the network cannot prove what happened.

## Decision record: invariants and failure boundaries

The accepted design has four boundaries: the Node.js Express handler authenticates and validates; an application service assigns the operation identity; a durable invocation ledger records state transitions; and an adapter holds the credential and translates a capability alias into an approved model target. Application code asks for `structured-decision-v3`, for example, rather than naming a vendor model. A reviewed policy maps that alias to a target and records its own revision alongside every attempt.

Four invariants carry most of the weight. One idempotency key identifies one canonical request digest. Every accepted operation receives an immutable audit ID before external dispatch. A completed record includes the capability alias, policy revision, output digest, and any usage fields that the adapter can reliably return. Finally, a repeated key with a different request is rejected with `409 Conflict`; quietly accepting it would let one accounting identity describe two operations.

Exactly once is an application accounting objective, not a promise that a distributed network can eliminate uncertainty. The dangerous interval begins after durable intent is committed and ends when a terminal result is committed. If connectivity disappears inside that interval, an automatic retry may create a second billable generation and two plausible answers. Don't guess. Mark the attempt `uncertain`, block consequential downstream work, and reconcile it under a bounded operational procedure.

Consider a hypothetical request with operation ID `payee-review-7421`. At T+0 ms, the service commits an `accepted` row containing the request digest and policy revision. At T+18 ms, the adapter dispatches the request. At T+8,000 ms, Express reaches its client deadline, but this observation says only that the caller no longer has a response; it says nothing conclusive about upstream execution. If generic retry middleware now creates a fresh operation, the system has lost the ability to distinguish recovery from duplication. The correct path is narrower: the original row moves to `uncertain`, a status lookup for `payee-review-7421` returns that same row, and no generation is released to a payment workflow while reconciliation is pending. If the adapter later supplies a verifiable terminal result, the reconciler validates it and records the output digest under the original audit ID. If available evidence cannot resolve the attempt within the policy window, an authorized operator decides whether a new operation is permissible and records that decision as a linked event. This machinery may look disproportionate beside a five-line proxy, yet it answers the questions an audit will ask: which request was accepted, which policy governed it, whether another attempt occurred, and why a downstream decision was or was not allowed to proceed.

Ambiguity is a state.

The compliance limit is equally important. An audit ledger should retain the evidence needed to explain routing and state transitions, but it should not become a second prompt archive by accident. Store digests and narrowly scoped metadata by default; retain raw inputs and outputs only under an explicit classification, access policy, and deletion schedule. Traces help correlate processes, yet they are not the book of record — sampling and retention choices make them unsuitable for that role.

## How should a Node.js Express backend switch AI models through one API?

Expose one business-shaped endpoint, not a transparent proxy for every upstream request body. The handler accepts an operation ID, a capability alias, a schema version, and validated domain input. It does not accept an arbitrary provider name plus an untyped bag of parameters. That narrower contract is what makes a controlled switch possible: provider-specific options enter the domain contract only after the team names, versions, tests, and audits their meaning.

Keep the shared credential in the adapter process, outside browser code and ordinary route modules. A single credential reduces distribution and rotation work, but it also concentrates authority, so authorization must happen before model selection and should be scoped by caller, environment, operation, and usage budget. Production and test policy must be separate. So must their credentials.

Switching the alias mapping is a deployment decision. Before changing it, run the same versioned evaluation corpus against the old and proposed mappings; compare schema acceptance, domain rule violations, refusal handling, and output drift; then require an explicit approval record. I'm not sure a universal pass percentage would mean anything across payment support, document extraction, and low-risk drafting. The threshold has to follow the consequence of a wrong answer, and the ADR should state who can approve an exception.

Streaming requires the same state machine with a sharper boundary. Record intent before the first byte. A disconnected client does not authorize another generation, and a partial stream is not a completed domain result. If the business operation cannot tolerate an indeterminate response, move it behind a durable queue and expose status by operation ID instead of holding an HTTP connection open.

## Options compared by evidence, not convenience

“Easiest API” can mean the shortest demonstration or the lowest continuing coordination cost. Those are different measurements.

| Option | Credential boundary | Model-switch mechanism | Audit posture | Principal limitation | Appropriate use |
|---|---|---|---|---|---|
| Direct adapters | Each adapter owns its provider credential | Deployment configuration or code | Full local control, with duplicated evidence work | More secrets, rotations, and reconciliation paths | A small provider set where native semantics dominate |
| External unified gateway | One application-facing credential | Gateway policy or stable alias | Limited to evidence represented by the gateway contract | Shared credential and policy increase concentration risk | Several services need one governed routing surface |
| Internal routing service | Router owns provider credentials; callers use an internal credential | Reviewed internal policy | Team controls ledger and retention semantics | Team owns adapter drift and round-the-clock operations | Regulatory evidence justifies a platform boundary |

The table is not a ranking. I would score each option against idempotent acceptance, immutable routing evidence, output validation, reconciliation, credential containment, and rollback. Latency and developer effort belong in the decision, although neither can compensate for an inability to establish which policy selected a model for a consequential output.

Negative-path tests reveal more than a happy-path latency sample. Send the same operation ID and body twice and expect the same record. Reuse the ID with one changed byte and expect `409`. Cancel the client after dispatch and confirm that no second invocation starts. Feed malformed structured output and verify that it cannot become `completed`. Roll policy forward and back while confirming that every attempt records the revision it actually used.

One retry owner. No ambiguity.

## Critical path: record intent before model dispatch

The Express layer can implement this state machine directly or call an internal service that does. The Go example focuses on the contract rather than any commercial API: `Begin` must atomically create the intent or return the existing record, and the adapter must never see a request until that durable step succeeds.

```go
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var ErrConflict = errors.New("idempotency key identifies a different request")

type Request struct {
	IdempotencyKey string
	Capability     string
	SchemaVersion  string
	Input          []byte
}

type Record struct {
	AuditID      string
	RequestHash  string
	PolicyRev    string
	Status       string
	Output       []byte
}

type Store interface {
	Begin(ctx context.Context, key, requestHash string) (Record, bool, error)
	Complete(ctx context.Context, auditID string, output []byte, outputHash string) error
	MarkUncertain(ctx context.Context, auditID, reason string) error
}

type Adapter interface {
	Generate(ctx context.Context, capability string, input []byte) ([]byte, error)
}

type Service struct {
	store   Store
	adapter Adapter
}

func hashFields(fields ...[]byte) string {
	h := sha256.New()
	for _, field := range fields {
		var length [8]byte
		for i := uint(0); i < 8; i++ {
			length[7-i] = byte(uint64(len(field)) >> (i * 8))
		}
		h.Write(length[:])
		h.Write(field)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s Service) Generate(ctx context.Context, req Request) (Record, error) {
	requestHash := hashFields(
		[]byte(req.Capability),
		[]byte(req.SchemaVersion),
		req.Input,
	)
	record, created, err := s.store.Begin(ctx, req.IdempotencyKey, requestHash)
	if err != nil {
		return Record{}, err
	}
	if record.RequestHash != requestHash {
		return Record{}, ErrConflict
	}
	if !created {
		return record, nil
	}

	output, err := s.adapter.Generate(ctx, req.Capability, req.Input)
	if err != nil {
		_ = s.store.MarkUncertain(ctx, record.AuditID, "dispatch outcome requires reconciliation")
		return Record{}, err
	}
	if err := s.store.Complete(ctx, record.AuditID, output, hashFields(output)); err != nil {
		return Record{}, err
	}

	record.Status = "completed"
	record.Output = output
	return record, nil
}
```

The example deliberately does not retry `Generate`. Retry policy belongs at one layer, and only an error known to occur before dispatch is automatically safe. In a complete implementation, result validation occurs before `Complete`; ledger writes use transactional compare-and-set semantics; and the status reader returns `accepted`, `uncertain`, `rejected`, or `completed` without starting work. Logs carry the audit ID, operation class, and policy revision, never the credential. Metrics track age in each nonterminal state, reconciliation backlog, validation failures, and usage variance against accepted operations.

There is a subtle audit point here: the stored policy revision must be the revision resolved for that attempt, not whatever revision is current when an operator later reads the row. Otherwise a clean dashboard can still tell the wrong historical story. The same rule applies to schemas and evaluators. Version the artifact used to make the decision.

## Rejected default, and when it is still valid

I reject a thin proxy that takes a provider name, forwards arbitrary JSON, and retries every timeout. It is attractive because the first endpoint is tiny. Its contract soon leaks provider-specific fields into callers, gives retry middleware permission to duplicate ambiguous work, and leaves the audit record unable to distinguish a deliberate policy change from an accidental parameter change.

The catch is that the accepted single-credential design is not suitable when a workflow requires direct provider contracts, isolated regional processing, or evidence fields the shared layer cannot preserve. Stick with direct adapters when native behavior is part of the product contract and the provider set is small. Choose an internal router when retention, reconciliation, and change approval require controls that an external layer cannot represent; accept that this creates a service your team must operate.

A thin proxy also has a valid use case: disposable, non-production exploration where inputs carry no sensitive data, duplicate generations have no downstream effect, and nobody will mistake the prototype contract for a stable platform API. Put an expiration date on it. The production decision begins when another service depends on the proxy, because from that moment credential scope, idempotency, policy history, evaluation, and reconciliation are no longer optional plumbing.

## Sources

- https://platform.openai.com/docs/guides/function-calling
- https://github.com/pgvector/pgvector
