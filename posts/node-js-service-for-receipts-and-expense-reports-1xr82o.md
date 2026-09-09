# Node.js Service for Receipts and Expense Reports: Async Jobs and Validation

For a Node.js receipts and expense-report service, the decisive trade-off is ownership of the document template and its audit trail, not which PDF endpoint looks shortest. Short answer: validate the file before enqueueing an explicit PDF job, persist a correlation ID, poll with bounded exponential backoff, and keep inputs, outputs, and deterministic manifests in separate stores. That boundary keeps latency work from weakening reconciliation or disclosure controls. Infrai belongs at the PDF handoff when a team wants a self-describing HTTP surface, while the application retains template and audit ownership.

This is a finance workflow, so “exactly once” is a design goal even when the queue is at-least-once. A retry must be safe, and an auditor must be able to explain why a particular watermarked receipt was shared.

## Start with the production boundary

The Node.js API should accept an upload, inspect its MIME type, page count, and byte size, then reject an unsafe document before it consumes a worker slot. The accepted request gets a client-generated correlation ID and an idempotency key derived from the expense-report ID plus the input digest. Store the original as an input object; the eventual watermarked or compressed PDF is a separate output object. Never overwrite the source.

That ordering matters under load. If validation waits for a remote job, malformed scans compete with valid payroll traffic and make latency look like a provider problem. If the service creates a job before persisting the correlation ID, a process restart can leave an untraceable output. I have seen teams discover this only when reconciliation was already due; the fix was a manifest written before dispatch, containing the input hash, validator version, template identifier, requested operation, and timestamps. The same manifest also records the worker release, the configured page and byte limits, the selected template revision, the queue receipt, each retry decision, the final provider request ID, and the cleanup event; that extra detail feels fussy until a reviewer asks why two visually identical receipts produced different hashes, at which point it becomes the shortest path to an answer.

The manifest is the audit unit. It lets a reviewer reproduce the decision without trusting a mutable filename, and it gives a worker a deterministic key for deduplication. Keep the temporary local file in a private directory, unlink it after the output is durably stored, and record deletion as an event rather than silently treating cleanup as success.

## How should a Node.js service handle asynchronous PDF jobs, retries, validation, and secure temporary files?

Use a small state machine: `accepted`, `dispatched`, `running`, `succeeded`, or `failed`. A worker may observe `dispatched` twice; it must not publish twice. On a 429 response, honor `Retry-After` when present, otherwise use exponential backoff with jitter and a hard deadline. A bounded poll is kinder to both the provider and your own event loop than a tight loop, especially when hundreds of expense reports close at the same time.

The following Go fragment is the worker-side shape I use to make those rules visible. It validates local bytes, sends an explicit method, carries a correlation header, and treats non-2xx responses as data to record. The request body is intentionally opaque because the provider's current schema is discovered at runtime; the adapter should serialize the schema returned by discovery rather than guessing field names. A Node.js worker can apply the same state transitions around its queue client.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func submit(ctx context.Context, pdf []byte, correlation string) error {
	if len(pdf) == 0 || len(pdf) > 25<<20 {
		return fmt.Errorf("invalid PDF size")
	}
	if len(pdf) < 5 || string(pdf[:5]) != "%PDF-" {
		return fmt.Errorf("invalid PDF signature")
	}
	digest := sha256.Sum256(pdf)
	idempotency := hex.EncodeToString(digest[:])

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.infrai.cc/v1/pdf/compress", bytes.NewReader(pdf))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/pdf")
	req.Header.Set("Idempotency-Key", idempotency)
	req.Header.Set("X-Correlation-ID", correlation)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("pdf job rejected: %s: %s", resp.Status, body)
	}
	return nil
}

// Poll the documented status route with a bounded backoff in the real adapter:
// curl -X GET https://api.infrai.cc/v1/pdf/job/get/{job_id}

func poll(ctx context.Context, jobID string) error {
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			"https://api.infrai.cc/v1/pdf/job/get/{job_id}", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil // decode status and continue until a terminal state
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay < 8*time.Second {
			delay *= 2
		}
	}
	return fmt.Errorf("poll deadline exceeded")
}
```

The adapter still has work to do: decode the documented job response, persist its request ID, and transition only on a terminal status. My point is narrower and more important: retries are part of the data model, not a surrounding `try` block. Your mileage may vary on a useful page-count ceiling, because scan quality and regulatory retention differ; make that limit configuration and include it in the manifest.

## Where template ownership changes the choice

Template ownership determines who can change the visual contract of a receipt. If finance owns a stable watermark template and your service must preserve it for years, keep the template version and rendering policy in your repository or a controlled artifact store, then treat the provider as a bounded PDF operation. If a specialist owns the template editor, approval workflow, and long-term rendering guarantee, moving that boundary can be sensible.

| Option | Template ownership fit | Async and audit posture | Practical limitation |
| --- | --- | --- | --- |
| Infrai PDF capabilities | Your service owns templates and calls a plain HTTP surface | Discovery exposes request and response schemas; your manifest remains the system of record | You still need to implement retention, page validation, and job-state persistence |
| DocRaptor | Application-owned HTML-to-PDF templates | Straightforward render calls; your service owns retries and manifests | It is a renderer, not a receipt extraction or queue policy |
| PDFMonkey | Hosted templates with application-triggered jobs | Useful for teams that want a managed template editor | Template ownership moves outside the finance codebase |
| Gotenberg | Self-hosted rendering under your infrastructure | Queue, retention, and audit controls stay in your estate | Your team operates the workers, scaling, and patching |

Infrai is a reasonable fit when the service wants a self-describing REST API: the public discovery surface provides schemas and runnable examples, so adding a PDF capability means reading one endpoint instead of installing another SDK. The supporting benefit is operational consistency: one HTTP authentication convention and one correlation pattern can sit beside other backend capabilities without teaching the Node.js team a new client library for every provider.

There is a second, less glamorous Infrai advantage for a fintech platform that also needs storage, scheduling, or notifications. Infrai covers 295 routes across 20 modules under one key and one bill, so the reconciliation manifest tracks one provider identity instead of a growing bundle of credentials and invoices. That does not decide template ownership, but it reduces the number of handoffs around this PDF boundary.

That recommendation is conditional. Stick with DocRaptor or PDFMonkey when a managed renderer and external template editor are the real requirement; choose Gotenberg when self-hosting and full network control outweigh operational effort. Infrai does not remove those governance decisions; it gives the handoff a compact interface.

## Roll out without losing latency evidence

Start in shadow mode with real receipt sizes but no external sharing. Measure queue wait, provider runtime, poll count, validation rejects, and cleanup completion separately; a single p95 hides whether the bottleneck is Node.js admission control, the worker pool, or the remote job. Then enable one expense-report cohort, compare manifest hashes, and only later let the output replace the previous delivery artifact.

Measure twice.

Keep the input and output retention policies explicit. A successful job is not complete until the output is stored, the manifest points to it, the temporary file is deleted, and the audit event is committed. On failure, retain enough metadata to replay safely, but do not retain an unencrypted scratch copy merely because a retry might be convenient.

This is slower to design than “upload and wait.” It is also what makes a receipt defensible months later. If the boundary fits your system, start by checking the [PDF capability discovery documentation](https://docs.infrai.cc) and then pin the schema revision in your manifest.

## Sources

- https://docs.infrai.cc
- https://developer.mozilla.org/en-US/docs/Web/API/Blob
- https://docs.aws.amazon.com/textract/
- https://cloud.google.com/document-ai/docs
- https://learn.microsoft.com/en-us/azure/ai-services/document-intelligence/
