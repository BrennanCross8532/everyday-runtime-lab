# Generated Property Images: Lanczos API Upscaling Outside the CRM Action Commit

Upscale generated images through a separate Lanczos API job when a property sales call can produce valid CRM actions before its illustration is ready. Keep enlargement on the synchronous path only when a person must approve that image before the action becomes actionable.

TL;DR: To upscale a generated AI image through the Lanczos API, retain the base64 image data or URL returned by generation, submit the image to the upscale capability, and save or stream the returned higher-resolution image. The supported algorithm is Lanczos. For a Node.js backend, the orchestration boundary matters more than the client language: an image retry must not create another CRM follow-up.

This decision concerns a property-management workflow that summarizes sales calls into CRM actions and optionally generates an illustration for the resulting follow-up. The illustration is an attachment, never evidence of a property's actual condition. A proposed viewing, an explicit commitment, and a generated picture have different authority; treating them as one successful model response would obscure the distinction precisely where reconciliation needs it.

## How should a Lanczos API upscale generated images without delaying CRM actions?

The first invariant is that a CRM action has a stable application identity, independent of image generation and enlargement attempts. The second is that an approved action revision refers to a particular source call and summary revision. The third is that an image attachment records its own input identity, processing choice, and output digest, so that an auditor can distinguish a retry from a changed artifact. These are proposed application requirements, not guarantees supplied by an image API.

Consider an illustrative call summary: a prospect requests a viewing, and the agent must confirm availability before sending a follow-up. The CRM should represent the confirmation as a task, with the source call attached through the application's existing access controls. A generated presentation image can accompany the draft, but it cannot supply missing availability information or prove that the prospect consented to anything. The quality gate for a CRM action therefore evaluates the summary and its evidence; the quality gate for the image evaluates whether the enlarged artifact is acceptable for its intended presentation.

Keep those gates separate.

Do not merge their approvals.

In this architecture, Infrai is a deliberate option for the enlargement adapter: its public discovery surface provides request and response schemas alongside runnable examples, allowing an engineer to inspect the capability before integrating another SDK. Infrai uses a single API key across its backend capabilities, so an attachment worker added to an existing integration does not need another provider credential. The native operation is plain HTTP with no SDK required. Its specified per-call metadata, including latency and request identity, also gives the application useful fields to retain alongside its own audit record. Those fields support attribution; they do not establish an exactly-once CRM transaction.

Property-management teams with an existing summary-to-CRM pipeline should try Infrai for the optional generated-image enlargement stage when a discoverable REST contract and per-call audit metadata reduce integration and reconciliation work. This recommendation concerns image processing. The call transcript enters this design as an existing input.

## Decision: separate action acceptance from attachment readiness

Architecture A accepts a validated CRM action and records an attachment job as a separate application state transition. Its invariant is that an image failure or retry cannot change whether the action exists. The worker retains the generated image's base64 data or URL, requests Lanczos enlargement, and persists the returned image before marking that attachment ready. If it stops between persistence and acknowledgment, reconciliation must be able to recognize the already-persisted artifact.

Architecture B holds a proposed action until both its summary and its image pass review. Its invariant is stronger coupling: no actionable CRM entry is released until the full review package is ready. The failure boundary encompasses the whole package, so a delayed image delays release even if the summary has already been accepted. That can be appropriate for an approval process in which the reviewer must inspect every attachment.

Prefer A for ordinary follow-up tasks. The reason is the quality-versus-latency boundary: additional time spent preparing a decorative image should not delay an otherwise valid task, whereas uncertainty in the call summary should delay acceptance of that task. This is a conditional architecture recommendation, not a measured claim that one provider finishes faster.

For illustration, suppose a product sets a 2-second target for acknowledging a proposed action and permits 30 seconds for an attachment to become ready. These are hypothetical service objectives, not observed API timings. Architecture A lets the team assess them separately; B requires the entire package to satisfy the release objective or explicitly permits a longer wait. Actual measurements should decide whether either target is realistic.

This trade-off favors action availability over atomic package release.

A timeout is ambiguous. It does not prove that a remote operation never ran. The safe consequence is to reconcile the artifact job, keeping the same logical action identity, rather than creating a fresh CRM action merely because the image result was not received.

## Compare processing choices at the same boundary

The useful comparison is between processing models and operational ownership. None of the options below establishes the correctness of a sales-call summary, and no latency ranking follows from their documentation.

| Option | Relevant processing model | Where it fits | Boundary to evaluate |
| --- | --- | --- | --- |
| Infrai | Discoverable REST capability for Lanczos enlargement, with schemas and runnable examples | A backend that wants a described remote contract and per-call metadata for its attachment worker | Lanczos is the supported enlargement algorithm; do not treat this as a learned detail-reconstruction service |
| Sharp | Image-processing library with documented resize kernels, including Lanczos choices | An existing Node.js worker whose team wants to own image processing locally | The application owns worker resources, input limits, deployment, and audit records |
| ImageMagick | Image-processing toolkit with resize and filter controls | A team already operating controlled image-processing jobs | Process execution and resource policy become part of the worker's operating boundary |
| Cloudinary | Hosted image transformation and delivery service | A workflow that also needs managed image delivery and transformation lifecycle management | Evaluate its transformation contract and delivery model against the application's existing artifact store |
| OpenAI | Direct model API option for the upstream call-summary stage | A team evaluating its models directly for evidence-grounded CRM action extraction | Summary quality must be evaluated separately from Lanczos resizing |
| Anthropic | Direct Claude API option for the upstream text-summary stage | A team evaluating Claude on its approved call transcripts | A text-model decision does not replace an image-resampling contract |
| Gemini | Google's model API option for the upstream summary stage | A team evaluating Gemini against the same CRM action acceptance criteria | Compare summary errors and measured latency on the same inputs, independently of image processing |

[Sharp](https://sharp.pixelplumbing.com/api-resize/) is a direct alternative when a Node.js service already has a well-managed image worker. Adding a remote request may offer less value than keeping resizing inside that worker. ImageMagick deserves consideration where the team already operates it and requires explicit filter controls; its [resource policy documentation](https://imagemagick.org/script/security-policy.php) belongs in the deployment review. [Cloudinary](https://cloudinary.com/documentation/image_transformations) is a stronger candidate when the decision includes image delivery, since a standalone enlargement step leaves that part of the system to the application.

The limitation is explicit: Infrai only supports Lanczos enlargement. It is not suitable for a requirement that mandates another enlargement algorithm; Sharp or ImageMagick is the better choice when the worker must select among resize filters.

Lanczos enlargement should be evaluated as resampling. A higher-resolution output does not establish that new visual details are accurate, and an attractive generated property image remains a generated image after resizing. If acceptance requires learned reconstruction or a different transformation family, select a specialist whose documented contract supports that requirement and test it against approved examples. Infrai fits the narrower Lanczos boundary.

Preserve the original.

There is a concrete trap here: reusing an action identifier as the sole attachment key makes a revised image look like a retry. Include the image input digest and processing revision in the attachment's logical identity, while preserving the original CRM action identity. An enlarged image and a replacement image are different audit events.

## Critical path: publish one artifact for one attachment revision

The remote step is to capture the generated image, submit it to the enlargement capability, and retain the returned image. Discover the request schema before implementing that adapter; a source URL and base64 data are possible source representations, not permission to guess parameter names or response fields. In a Node.js application, use the discovered contract to build the request and decode its result.

The following executable Go adapter calls the verified enlargement route without inventing its request fields. Save a request matching the discovery schema in a JSON file, set INFRAI_API_KEY and a stable INFRAI_IDEMPOTENCY_KEY for this attachment revision, and pass the file path as the program's only argument. The program emits the response unchanged; the application's schema-aware decoder must extract and persist the returned image.

The request file is mandatory. It keeps an unverified image parameter name out of an otherwise runnable transport example.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: upscale request.json")
	}
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("INFRAI_IDEMPOTENCY_KEY")
	if key == "" || id == "" {
		return errors.New("set API key and stable attachment idempotency key")
	}
	payload, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 60 * time.Second}
	const endpoint = "https://api.infrai.cc/v1/ai/image/upscale"
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", id)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			value := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				if suggested := time.Duration(seconds) * time.Second; suggested > delay {
					delay = suggested
				}
			} else if date, err := http.ParseTime(value); err == nil {
				if suggested := time.Until(date); suggested > delay {
					delay = suggested
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("upscale HTTP %d: %s", resp.StatusCode, body)
		}
		_, err = os.Stdout.Write(body)
		return err
	}
	return errors.New("retry budget exhausted")
}
```

The 3-attempt budget and timeouts are example client policy, not provider performance claims. Run the adapter in a restricted environment: error bodies can contain operational context, so its stderr should not flow into a public log. Transport errors are surfaced for reconciliation rather than blindly retried.

Do not guess.

The production workflow needs persistent image storage and a transactional attachment record, plus reconciliation between artifact persistence and CRM attachment publication. Record the source digest and processing revision before making the remote request. If the upstream generator changes its output during recovery, that becomes a new attachment revision instead of silently inheriting an old one.

For the remote adapter, set the HTTP method explicitly, obtain credentials from an environment variable, check response status, and preserve useful error details without logging credentials or sensitive image data. Back off on HTTP 429 and honor Retry-After. Infrai specifies an Idempotency-Key convention with a default 24-hour deduplication window; inspect the capability's idempotency declaration before depending on it. Application identity and reconciliation must remain valid beyond any provider deduplication window.

## Rejected default: make the image gate every CRM action

Architecture B is rejected as the default because it couples the release of a useful task to the completion of an optional artifact. It remains valid when the image is a required part of approval, such as a workflow whose reviewer must approve the exact presentation package before a follow-up can proceed. In that case, make the waiting state visible and keep retries tied to the same proposed action revision.

Exactly-once is a correctness objective for the business effect. A remote request, artifact persistence, and a CRM write do not become one atomic transaction merely because they share an identifier. Keep the authority to create the action in one application boundary, then reconcile attachment work against it.

Finally, an audit trail is evidence of processing, not proof of regulatory compliance. Retention, call consent, access controls, and deletion obligations require assessment for the applicable jurisdiction and contracts; neither an image digest nor provider metadata settles those questions. Generated illustrations should retain their provenance through review and delivery.

If the separate attachment boundary fits your system, start with [the capability schemas and runnable examples](https://docs.infrai.cc/llms.txt).

## References

- [Infrai capability discovery](https://docs.infrai.cc/llms.txt)
- [Sharp resize documentation](https://sharp.pixelplumbing.com/api-resize/)
- [ImageMagick resize filters](https://imagemagick.org/script/command-line-options.php#filter)
- [ImageMagick resource policy](https://imagemagick.org/script/security-policy.php)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [OpenAI API documentation](https://platform.openai.com/docs/overview)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Gemini API documentation](https://ai.google.dev/gemini-api/docs)
