# Node.js PDF/DOCX Uploads to Private Object Storage with Scoped Download URLs

Short answer: keep the bucket private, let the Node.js service own authorization and document state, and give clients short-lived signed URLs only after a database decision; treat PDF and DOCX bytes as untrusted until verification completes.

The important boundary is not the upload endpoint. It is the transition from “bytes were presented” to “this document may be used.” A signed URL is a scoped bearer credential, not an identity assertion, and object metadata is an operational hint, not an authorization record. That distinction prevents a surprisingly common design error: allowing a storage key, filename, or `user_id` supplied by a client to become the thing that decides ownership.

## Begin with the constraint: private bytes, accountable decisions

For a document service, the application has facts that object storage does not: the authenticated actor, the document's intended owner, retention policy, idempotency key, inspection state, and audit history. Store those facts in a database. Store the bytes under an opaque, application-generated key. The key should not contain an email address, original filename, tenant label, or sequential account identifier.

The data path and the control path can then be separate. The control path creates a pending document, validates the request, and signs a narrowly scoped upload. The data path transfers the bytes directly to private storage. A later finalize operation reads the stored object's observed properties, compares them with the pending record, and advances the document only when the policy checks pass. Downloads follow the reverse order: authenticate, authorize by document ID, record the decision, then sign a GET for the stored key.

That state transition is the design.

I model at least `pending`, `available`, `rejected`, and `deleted`. Retries are idempotent on `(owner_id, idempotency_key)`, and a conditional update ensures that two finalizers cannot both publish the same object. This is an exactly-once mindset applied to a system whose individual components do not offer one transaction across the database, storage service, scanner, and audit sink.

The audit event should include actor, document ID, request ID, outcome, and timestamp. It may include the opaque object key for reconciliation, but it should never include the signed query string: that string grants access until it expires.

## How should a Node.js service handle PDF and DOCX uploads to private object storage?

First validate the request against an allow-list. The declared media type and extension can guide the initial decision, but neither proves what the body contains. PDF parsing and DOCX archive handling are security-sensitive operations, so quarantine new objects, inspect magic bytes and archive structure, apply size and decompression limits, and publish only after the inspection result is recorded. A download should use a safe content disposition and a sanitized display name.

For direct uploads, the sequence is deliberately boring:

1. Authenticate the caller and validate the declared type and size.
2. Create one pending row with a generated document ID and opaque object key.
3. Return a signed PUT constrained to that key, method, expected size, and required headers.
4. Let the client upload directly to the private bucket.
5. Finalize by document ID; compare storage observations with the pending row, then inspect or queue inspection.
6. Mark the row available only after the policy succeeds.
7. On download, authorize the document ID and issue a short-lived signed GET.

The client must not choose the owner, storage key, or final availability state. It may provide an idempotency key and a proposed filename, but the service decides how those values are persisted. A repeated create request should return the existing logical document or a clear conflict, rather than allocate another object.

Here is the narrow service boundary I would review in a Node.js system. The example is in Go because the editorial constraint for this note is explicit error handling; the interface is provider-neutral and is intended to make ordering visible.

```go
package documents

import (
	"context"
	"errors"
	"time"
)

const (
	pdf  = "application/pdf"
	docx = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)

type Document struct {
	ID, OwnerID, ObjectKey, MediaType, Status string
	Size                                      int64
}

type Repository interface {
	CreatePending(context.Context, Document, string) (Document, error)
	Find(context.Context, string) (Document, error)
	MarkAvailable(context.Context, string) error
}

type ObjectStore interface {
	SignUpload(context.Context, string, string, int64, time.Duration) (string, error)
	SignDownload(context.Context, string, time.Duration) (string, error)
}

type Service struct {
	documents Repository
	objects   ObjectStore
}

func (s Service) BeginUpload(ctx context.Context, actorID, id, mediaType string, size int64, idem string) (string, error) {
	if actorID == "" || id == "" || idem == "" {
		return "", errors.New("actor, document ID, and idempotency key are required")
	}
	if mediaType != pdf && mediaType != docx {
		return "", errors.New("unsupported document type")
	}
	if size <= 0 {
		return "", errors.New("document size must be positive")
	}

	doc, err := s.documents.CreatePending(ctx, Document{
		ID: id, OwnerID: actorID, ObjectKey: "documents/" + id,
		MediaType: mediaType, Size: size, Status: "pending",
	}, idem)
	if err != nil {
		return "", err
	}
	return s.objects.SignUpload(ctx, doc.ObjectKey, doc.MediaType, doc.Size, 10*time.Minute)
}

func (s Service) DownloadURL(ctx context.Context, actorID, id string) (string, error) {
	doc, err := s.documents.Find(ctx, id)
	if err != nil {
		return "", err
	}
	if doc.OwnerID != actorID || doc.Status != "available" {
		return "", errors.New("document is not available to this actor")
	}
	return s.objects.SignDownload(ctx, doc.ObjectKey, 5*time.Minute)
}
```

The ten-minute and five-minute durations are policy examples, not universal defaults. I would test them with a controllable clock, verify the exact headers used by the signer, and reject an authorization request before any signing call. If an application needs to revoke access immediately after issuing a link, it should stream through an authorization layer instead; an already issued link remains a bearer credential for its validity window.

## What do signed URLs, bucket metadata, and user IDs actually prove?

A signed upload proves that a signer authorized a particular request under particular constraints. It does not prove that the uploaded bytes are safe, that the caller owns the filename, or that the resulting row should become available. A signed download proves possession of a valid link, not the current identity of the person presenting it. The application must make the ownership decision before signing.

Metadata is useful for operations. A non-sensitive internal document ID can help an operator correlate an object with a database row, while media type and checksum can support verification. Copying a raw `user_id` into object metadata is a different choice: it expands the locations where a personal identifier is stored and governed, without replacing the database authorization check. Keep the authoritative owner relationship in the database and make metadata minimization part of the data-classification review.

S3-compatible behavior should be treated as a compatibility claim to test, not an assumption to inherit from a product label. Check the signing algorithm, required headers, expiration semantics, range requests, checksum handling, clock skew, and whether repeated use within the validity window is expected. The relevant standard behavior must be documented in the adapter and covered by integration tests against every storage implementation the service may use.

Expiry isn't revocation.

## Where do uploads fail, and how does reconciliation recover them?

There is no atomic commit spanning a row, an object, inspection, and an audit record. The system therefore needs a repairable state machine. A pending row with no object can expire. An object with a pending row can be rechecked and finalized. An object with no corresponding row can be quarantined and removed according to a documented retention rule. A failed inspection must leave evidence and prevent publication, rather than silently turning an uncertain result into an available document.

The difficult case is a lost response. The client may upload successfully and time out before receiving the finalize response; a retry must use the same document ID and idempotency key. The finalizer should be safe to run repeatedly, and its database update should require the expected prior state. I also treat a `413` response as a policy signal to observe and explain, not as proof that no object exists: reconciliation still needs to account for any transfer that reached storage before the request was rejected or disconnected.

Metrics should distinguish pending age, verification failures, orphaned objects, denied signing requests, incomplete multipart transfers, and transferred bytes. Logs should correlate request ID, document ID, and storage request ID while excluding credentials and signed URLs. The audit trail should answer a narrow question for every retained object: which logical document owns it, who authorized the last transition, and why it remains retained.

For payment and ledger backends, I put this question beside reconciliation because document retention can affect a financial record even when the document body is not part of the ledger. Compliance limits still apply: residency, encryption-key ownership, legal hold, retention duration, access logging, and deletion evidence must be decided before implementation. Signed URLs solve controlled transfer; they do not solve those obligations.

## A rollout that preserves the audit trail

Start with one document class and a small, explicit policy: accepted types, maximum size, quarantine duration, retention, and who may download. Add contract tests for the storage adapter before enabling direct client uploads. Exercise duplicate create requests, interrupted transfers, lost finalize responses, stale download links, range reads, and deletion retries. The test should assert both the user-visible decision and the audit event.

During migration, keep the old read path available until every object has a database row, a verified checksum where feasible, and an owner that can be explained. Inventory orphaned keys before changing lifecycle rules. Then enable direct upload for a measured cohort, compare pending and reconciliation metrics, and make rollback mean “stop issuing new signed URLs,” not “delete the bucket.”

The catch is that direct transfer is unsuitable when every byte must pass through synchronous application policy or when immediate revocation is mandatory; use an application-mediated path in those cases. It is also a poor fit for teams that cannot operate reconciliation and lifecycle cleanup. A private bucket with signed links is a useful mechanism, but accountability comes from the state model around it.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://cloud.google.com/storage/docs
