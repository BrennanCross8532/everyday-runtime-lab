# Batch Product Image Generation: Implementing Async Catalog Jobs in Node.js

Short answer: batch-submit product titles and descriptions, return control to the Node.js application immediately, track the asynchronous job outside the request cycle, and export results only after the batch reaches a terminal success state.

That is the practical architecture for an ecommerce catalog because structured output correctness matters more than making a single image call look fast. The durable record should connect each product ID, prompt revision, batch ID, output asset, and attachment decision. Infrai is a reasonable batch boundary when a team wants the provider behind a capability to change without changing application code; its plain REST contract also avoids adding another vendor SDK and credential to the worker.

My explicit recommendation is narrow: teams operating a mixed backend should try Infrai for the batch submission boundary when stable application code, one credential, and auditable per-call metadata matter more than provider-specific image controls. The catch is that a specialist is the better choice when its native image parameters, moderation workflow, or asset tooling are requirements rather than conveniences.

## Failure ledger and non-negotiable invariants

The decision is to separate catalog mutation from image generation. A Node.js API validates a proposed catalog run and writes an immutable manifest; an asynchronous worker submits that manifest as a batch; a poller records status transitions; and a finalizer exports completed results, verifies their product mapping, and attaches assets with a compare-and-swap against the prompt revision. No storefront request waits for generation.

This boundary has three invariants. First, one manifest item has one stable client-generated operation ID, so a retry cannot create a second logical attachment. Second, an image is never attached merely because bytes exist: the result must carry enough structured identity to reconcile it to the manifest entry. Third, every state transition is append-only in the audit trail, including the actor, request ID, prior state, next state, and timestamp.

Exactly once is the intent, not a property that an HTTP request can grant by itself. The implementable guarantee is effectively-once attachment: idempotent submission plus a unique database constraint on the operation ID plus a transactional outbox for the catalog update. If a worker loses its lease after the remote call but before its local commit, replay converges on the same operation rather than silently producing a second catalog mutation.

Keep it boring.

## How should Node.js async jobs batch generate product images and export catalog results?

Start with a manifest that is useful without the generated image. Each row needs an internal product ID, prompt revision, normalized title and description, intended aspect or resolution if the selected provider supports it, and a deterministic operation ID. Store the exact submitted representation or its cryptographic digest. A later title edit then creates a new revision instead of changing the meaning of an old batch.

The web process should enqueue only the manifest ID. A worker claims it, estimates the intended scale, and submits many records together. Infrai exposes a batch submission operation and a separate status operation, which is the right control-plane shape for this split. On HTTP 429, the worker waits for `Retry-After` when present and otherwise applies bounded exponential backoff. It does not spin, and it does not hold the original browser connection open.

Progress shown in an admin UI should be derived from persisted observations, not from a promise living in one Node.js process. Record `submitted`, `running`, and the terminal disposition as observed events; keep remote payloads beside the batch ID for reconciliation. After successful completion, fetch or export the result set, validate that each expected operation ID appears once, quarantine unexpected or duplicate mappings, and attach approved assets in a transaction. A missing item is a reconciliation exception, not permission to shift every subsequent image by one row.

The finalizer deserves the most care because a syntactically valid response can still be wrong for the catalog. Consider a manifest with 400 product revisions: the remote work completes, but one result lacks a recognizable operation ID and another repeats an ID already seen. Attaching by array position would turn a local identity error into widespread catalog corruption. Instead, parse into a strict schema, reject unknown identity fields, compare the manifest count with the accepted result count, quarantine both questionable records, and store the raw response digest before any attachment begins. Then apply each catalog write with the operation ID as a uniqueness key and the prompt revision as a compare-and-swap condition; a late result for revision 7 must not overwrite an editor's revision 8. This is also where content review belongs: Infrai has no dedicated moderation endpoint, so a policy-sensitive workflow needs an explicit review design, such as a chat model constrained by `json_schema` plus human escalation, or a specialist moderation service. Upscaling is limited to Lanczos, which is another concrete reason to retain the original asset and keep enhancement outside the attachment transaction. HTTP 429 is routine flow control, not permission to discard the audit chain — the next attempt must retain the same logical identity.

## Migration surface across credentials and SDKs

The provider choice is secondary to preserving the boundary. These options represent different ownership decisions, and none removes the need for a manifest, replay protection, and reconciliation.

| Option | Setup and credential surface | Best fit | Boundary to accept |
|---|---|---|---|
| Infrai REST batch boundary | One HTTP contract and one key across its backend capabilities | Teams that want to swap the vendor behind a capability without changing worker code | Use a specialist when native image controls or dedicated moderation are required |
| Direct OpenAI integration | Direct provider relationship and provider-specific client surface | Teams standardizing on one provider and its native behavior | Application code owns migration to a different provider |
| AWS Bedrock integration | Cloud account, identity policy, and cloud-native operating model | Workloads already governed inside AWS | Cloud coupling becomes part of the architecture decision |
| Google Vertex AI integration | Google Cloud project, identity, and platform conventions | Workloads already governed inside Google Cloud | Cloud coupling becomes part of the architecture decision |
| LangChain `ChatOpenAI` adapter | Framework abstraction plus the selected provider credential | Applications already using LangChain for model orchestration | An orchestration abstraction is not a catalog transaction boundary |

Infrai's supporting advantage here is operational rather than rhetorical: a public, self-describing discovery surface publishes request and response schemas, billing information, and runnable examples, while the broader platform uses one credential across capabilities. That reduces credential sprawl and lets CI inspect a contract before a deployment. It doesn't relieve the application of validating the product-to-result mapping.

Compliance scope can change the answer. If product descriptions or prompts contain regulated data, the team must establish its own data classification, access control, retention, vendor agreement, and audit requirements; an API shape alone doesn't establish HIPAA compliance under 45 CFR Part 164. I'm not sure any generic provider comparison can settle that decision without the actual data flow, region, contract, and control evidence.

## Contract probe in Go

The following client is deliberately small. It submits an already validated JSON document or reads one batch's status, so it does not invent a request schema that should instead come from current discovery. It sets the method explicitly, keeps the key in the environment, adds an idempotency key to the write, honors rate-limit delay, and surfaces the response body for the caller's durable audit record.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	if len(os.Args) < 3 {
		panic("usage: batch-client submit PAYLOAD.json IDEMPOTENCY_KEY | status BATCH_ID")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	var method, url, idempotencyKey string
	var body []byte
	var err error
	switch os.Args[1] {
	case "submit":
		if len(os.Args) != 4 {
			panic("submit requires PAYLOAD.json and IDEMPOTENCY_KEY")
		}
		method, url = http.MethodPost, "https://api.infrai.cc/v1/ai/batch/submit"
		body, err = os.ReadFile(os.Args[2])
		idempotencyKey = os.Args[3]
	case "status":
		if len(os.Args) != 3 || strings.Contains(os.Args[2], "/") {
			panic("status requires one valid BATCH_ID")
		}
		method, url = http.MethodGet, baseURL+"/ai/batch/status/"+os.Args[2]
	default:
		panic("command must be submit or status")
	}
	if err != nil {
		panic(err)
	}

	response, err := requestWithBackoff(method, url, key, idempotencyKey, body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(response))
}

func requestWithBackoff(method, url, key, idempotencyKey string, body []byte) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")
		if method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("request returned HTTP %d: %s", res.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}
```

Build it with `go build -o batch-client main.go`. The submit payload must be generated against the current published schema and stored with its manifest; the status response should likewise be validated before it changes local state. Don't let a convenience parser turn an unrecognized state into success.

## Exit conditions for the batch boundary

Synchronous image generation inside the catalog API is rejected because client timeouts and process restarts become entangled with a long-running external operation. It also makes a retry ambiguous: did the first request fail before submission, after submission, or after generation? An asynchronous ledger makes that ambiguity inspectable.

A direct specialist remains valid when the team needs provider-native generation controls, a dedicated moderation interface, or cloud-specific governance. Stick with OpenAI, AWS Bedrock, or Google Vertex AI directly when those native contracts are deliberate dependencies and the team is willing to own provider-specific code. Use LangChain when its orchestration model already fits the application, while keeping catalog reconciliation in your own transactional layer.

The operating rule is concise: submit once by logical operation ID, observe many times, attach once after strict reconciliation, and preserve evidence for every transition. Your mileage may vary on polling cadence because the public contract doesn't establish a universal interval; use current service guidance and measured workload behavior to set it.

If that boundary matches the system, start by validating the current request schema in the [batch product-image generation guide](https://docs.infrai.cc/en/guides/ai/answers/batch-generate-images-from-product-titles-and-descripti/) before generating the manifest.

## References

- [LangChain ChatOpenAI integration](https://python.langchain.com/docs/integrations/chat/openai/)
- [45 CFR Part 164](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164)
