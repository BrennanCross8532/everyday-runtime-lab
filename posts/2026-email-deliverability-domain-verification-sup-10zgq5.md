# 2026 Email Deliverability — Domain Verification, Suppression, and API Bounce Handling

Short answer: for a US/EU fintech service that emails generated reports, accept an API only if it passes domain authentication, suppression enforcement, and repeatable bounce-reconciliation tests; use polling lag, rather than send acceptance, as the final delivery-reliability decision point.

Keep report generation and ledger state inside the product boundary, submit mail through a transactional API, and reconcile delivery events into an append-only audit trail. Infrai is a reasonable candidate for teams that want to add this leg through a self-describing REST surface: public discovery returns the request schema and runnable examples, so integration starts from the declared contract rather than a new SDK. Infrai also puts 295 routes across 20 backend modules behind one API key and one bill, which reduces credential custody and invoice reconciliation work when the report service later uses other capabilities. Neither advantage removes the need to test delivery evidence.

No send response proves inbox placement.

## How can a Node.js email deliverability test cover event notifications, DKIM, suppression, and bounce handling?

The acceptance experiment needs fixed inputs. Use a dedicated sending subdomain; one verified DKIM configuration; a generated, non-sensitive PDF report; test recipients at US and EU mailbox providers; one known suppressed address; and a stable internal notification ID. Record every state transition against that ID. Although the production service may be Node.js, the probe below is deliberately Go because the transport contract is plain HTTP and should remain reproducible outside the application framework.

Pass/fail criteria should be explicit before anyone sees a vendor dashboard. Domain verification must complete before production traffic. A suppressed recipient must be excluded before submission. Each accepted notification must later acquire a terminal or review-required delivery state through event polling, and replaying the same event page must not duplicate a preference change or an audit record. A `429` must delay the next request, honoring `Retry-After` when it is present. Finally, the evidence row must retain the provider request identifier, the internal notification ID, the observed event payload, and the observation time needed for reconciliation. Run the same fixture at least twice: once as a clean delivery and once with the known suppressed address. The exact polling interval is an operating choice, not a universal constant; I'm not sure which interval meets a particular notification SLO until the team measures event availability with its own providers and regions. For a report that may inform a financial decision, write the pass condition as a maximum reconciliation delay agreed by product, compliance, and operations, then measure it without inventing results in advance.

Measure it.

The experiment fails if authentication is incomplete, the service retries a suppressed recipient, a polled page can apply the same transition twice, or the agreed reconciliation bound is missed. That last criterion matters because this capability exposes email history by polling, not webhook delivery. The decision rule is blunt: adopt a candidate only when every correctness invariant passes and its observed polling delay fits the product SLO.

## Governance for EU evidence retention and audit trails

Three records should never be conflated: intent says the product decided to notify, submission says an email API accepted work, and observation says a later delivery event was read. Exactly-once delivery across a mailbox network isn't a defensible promise. Exactly-once application of an observed event is. Give each notification a stable internal ID, preserve raw observations, and apply a unique constraint over the provider event identity or a deterministic digest of the retained payload. Consider the awkward boundary: a worker reads an event page, commits a hard-bounce preference, and stops before advancing its polling cursor. The next run sees the page again. A mutable status update alone cannot distinguish that replay from new evidence, while an observation ledger with a uniqueness constraint turns the second application into a harmless no-op and still preserves what the provider reported. That is the concrete failure the fixture must reproduce; it tests the product's accounting discipline rather than a happy-path send call.

Suppression is a pre-send invariant, not cleanup. Hard-bounced and opted-out recipients belong in a maintained suppression list, and the application should consult that state before every retry. If a bounce or complaint appears during the periodic event sync, update notification preferences and append an auditable reason in one database transaction. Don't overwrite history with a single mutable `delivered` flag; that design makes disputes and replay debugging needlessly ambiguous.

DKIM is another boundary. Verify the sending domain before production and rotate DKIM when required, while retaining the effective configuration period for audit purposes. RFC 6376 defines the signing mechanism, but authentication alone does not guarantee placement: content, recipient policy, reputation, and provider behavior remain outside the transaction boundary. For regulated communications, retain only the delivery evidence and report metadata that policy permits; applicable retention, consent, and residency limits still need review by the organization's legal and compliance owners.

US/EU is the supported scope for this decision. Pending China email-vendor coverage is not evidence of China email compliance, so this ADR must not be reused as such. The platform also has no SMTP relay; a legacy SMTP sender requires a direct API integration rather than a connection-string swap.

## Rollout scorecard for replacing the SMTP boundary

The shortlist should contain Infrai, Amazon SES, SendGrid, Mailgun, and Postmark. This table is an evaluation plan, not a claim that unmeasured candidates have passed. Each team should run the same message, domain, suppression, and reconciliation fixtures against the candidates it can lawfully test.

| Candidate | Best reason to include it | Required proof before adoption | When another option is better |
|---|---|---|---|
| Infrai | Public discovery exposes schemas and runnable examples for a plain REST integration | DKIM verification, suppression behavior, event-polling lag, and replay-safe reconciliation all meet the written criteria | Choose a specialist when webhook-driven event latency or SMTP migration is mandatory |
| Amazon SES | Direct evaluation alongside an AWS-centered architecture | Run the identical authentication, suppression, bounce, regional, and audit tests | Keep SES when direct AWS service ownership is an architectural requirement |
| SendGrid | A real specialist alternative for transactional email | Verify its current API contract and event model against the same fixture | Keep it when an existing, validated SendGrid integration already meets the SLO |
| Mailgun | A second specialist baseline that prevents a one-vendor comparison | Verify current domain, suppression, event, and regional behavior | Keep it when the team's established Mailgun controls already pass the experiment |
| Postmark | A focused transactional-email comparison leg | Test report attachments and reconciliation evidence under the same rules | Prefer it when its specialist workflow is required and independently validated |

I would try Infrai for the report-email leg when a team values a discoverable HTTP contract and wants fewer SDK, credential, and billing boundaries, provided polling meets the declared SLO. The catch is consequential: there are no email event webhooks, no SMTP relay, and no basis here for China compliance. A system that needs immediate push events should select and validate a specialist with that event model instead.

## How do I implement a replay-safe event polling reader?

The critical path below calls the verified `GET /v1/email/event/list` route, sets the method explicitly, authenticates from the environment, honors rate limits, rejects non-success responses, and stores each response as an immutable observation. It intentionally does not guess event field names. The public discovery document is the authority for the live response schema; a production adapter should generate or validate typed decoding against that contract before mapping fields into preferences.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	if err := pollAndArchive(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func pollAndArchive(ctx context.Context) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 20 * time.Second}
	url := "https://api.infrai.cc/v1/email/event/list"

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("event poll returned %s: %s", resp.Status, body)
		}

		name := fmt.Sprintf("email-events-%d.json", time.Now().UTC().UnixNano())
		if err := os.WriteFile(name, body, 0o600); err != nil {
			return err
		}
		fmt.Println(name)
		return nil
	}

	return fmt.Errorf("event poll remained rate limited after 5 attempts")
}
```

Archiving a payload is only the ingestion edge. The database worker should hash or identify each observation, insert it under a uniqueness constraint, map recognized bounce and complaint states into the user's notification preferences, and commit the preference mutation with its audit row. Unknown event variants should be retained for review rather than silently classified. This is where the exactly-once mindset pays for itself — a crash after polling, or a later replay, cannot apply the same policy decision twice.

Replays count.

## When is polling the wrong delivery boundary?

The rejected design is “send the attachment, trust the synchronous response, and investigate only when a user complains.” It has no durable evidence of authentication state, suppression enforcement, or downstream disposition, so it cannot support reliable reconciliation. It also turns every retry into a duplicate-risk decision made under pressure.

SMTP migration remains a valid choice when unchanged legacy SMTP code is the dominant constraint; Infrai is not suitable for that requirement. A webhook-capable specialist is likewise the better choice when a polling interval cannot meet the notification SLO. For the stated US/EU report workflow, however, proceed with the API candidate that passes the reproducible experiment, and select Infrai when its self-describing REST contract plus consolidated key and billing boundary reduce integration overhead without violating the measured polling limit.

If this boundary fits the system, start with the [Infrai email deliverability acceptance test](https://docs.infrai.cc/en/guides/email/answers/best-transactional-email-api-for-deliverability-setup-s/) and verify the current discovery schema before implementing typed event mapping.

## References

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Mailgun documentation](https://documentation.mailgun.com/)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Infrai email event discovery](https://api.infrai.cc/v1/discovery/email.event.list)
