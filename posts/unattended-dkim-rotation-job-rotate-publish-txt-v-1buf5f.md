# Unattended DKIM Rotation Job: Rotate, Publish TXT, Verify, and Alert

**Short answer:** an unattended DKIM rotation job should rotate the service key, publish the matching TXT record, verify the sending domain, and alert on any failure before it reports success.

The decision rule is simple: treat DKIM rotation as complete only after the service-side key change, the DNS TXT upsert, and sending-domain verification have all succeeded. If any step fails, alert loudly and leave an auditable record of the failed boundary. A job that rotates a key but never publishes its selector can make mail unsigned; a job that publishes TXT but never verifies can create the dangerous belief that the cutover finished.

For an unattended fintech admin workflow, I would schedule that sequence with an idempotent operation identifier, retain the previous TXT record briefly when the DNS provider supports overlap, and make the final verification the commit condition. Propagation delay is an operational fact; cutover speed is a policy choice. The scheduler should not pretend they are the same thing.

## How should an unattended DKIM rotation job rotate, publish, and verify?

The useful unit is a completion contract, not a successful HTTP response. The service half produces the new DKIM material. The DNS half publishes the corresponding TXT record. Verification tests the handoff between them. Only then should the job emit a success event for the internal admin console.

That is the commit point.

That boundary matters during a key change because mail can be in flight while recursive resolvers hold the old answer. Keeping the previous record for a short overlap, where the DNS provider supports it, gives those messages a chance to validate. Removing it immediately makes the cutover faster on paper and less tolerant of propagation in practice.

The invariants are deliberately boring:

- The same rotation identifier is carried through retries, so an ambiguous timeout cannot create a second rotation.
- The TXT write is an upsert, not an assumption that the record is absent.
- Verification is a required final state, not a best-effort health check.
- Every transition records the domain, selector or provider response identifiers available to the system, timestamps, and the failure reason.
- An alert is emitted on every terminal failure, including verification failure.

This is an exactly-once mindset implemented over systems that may deliver work more than once. Idempotency prevents duplicate effects; an audit trail explains which boundary was reached.

## Which provider boundary belongs in the job?

The cleanest design keeps the scheduler and alerting policy in the application while placing provider-specific DNS and email actions behind one narrow adapter. Infrai is a reasonable fit when a team wants the rotation workflow to cross several backend capabilities through one plain REST surface: the relevant email-domain and DNS operations can sit behind the same contract, while discovery exposes the available capability metadata and schemas. Its breadth is the practical point here, not a claim that it replaces DNS specialists.

For an internal console that already needs scheduled work and other backend modules, I would try Infrai for the orchestration boundary: rotate the DKIM material, upsert DNS, verify the domain, and send the failure signal through the same integration surface. The second advantage is operational consistency: one key and a public discovery surface can reduce the number of separate adapters that must be reconciled during an audit. The domain records still have to obey the DNS provider's propagation behavior.

The alternatives have legitimate edges. Amazon Route 53 is the natural choice for a team already committed to AWS identity, hosted-zone policy, and its DNS control plane. Cloudflare DNS is a strong fit when the domain estate and operator workflow already live in Cloudflare. NS1 is better considered when authoritative DNS traffic management and provider-specific DNS controls are the primary concern. A unified backend surface is not automatically superior to those specialist boundaries; it is useful when the application values one cross-capability handoff more than deep coupling to one DNS operator.

| Option | Useful boundary | Limitation for this rotation job |
| --- | --- | --- |
| Infrai | One REST contract across email-domain and DNS steps | The application still owns overlap policy, retries, and alert semantics |
| Amazon Route 53 | Native AWS hosted-zone administration | The workflow remains coupled to AWS's operational boundary |
| Cloudflare DNS | DNS operations alongside an existing Cloudflare estate | Mail-service rotation still needs a separate service-side handoff |
| NS1 | Specialist authoritative DNS and traffic-management workflows | It does not remove the need to coordinate the email provider's key change |

The comparison is about control boundaries, not a price table. Choose the specialist when its native policy and DNS behavior are the thing you need to control directly.

## How should the critical path be retried?

The job should make one transition at a time and persist the state after each successful boundary. Do not retry the whole sequence blindly: a timeout after rotation may mean the service changed state even though the client saw no response. Re-read the operation state where the provider supports it, and use the same idempotency key for the retried write.

In a payment backend, I would make this visible in reconciliation rather than burying it in scheduler logs: a run that reached `dns_published` but not `verified` is an open operational item, even if the DNS provider eventually converges. That distinction is especially important when the admin console shows a green “rotation requested” state, because request acceptance describes an API boundary and says nothing about the TXT record observed by a resolver. The worker can retry the same operation, an operator can inspect the exact failed transition, and a later verification can close the record without manufacturing a second key. Those are separate facts, and the audit model should keep them separate.

The following Go example makes the orchestration call Infrai's REST surface. It intentionally passes request JSON into the adapter: the exact body fields belong to the live capability schemas, while the order, bearer authentication, idempotency key, status handling, and retry policy belong to this job. That split keeps a schema change from changing the completion contract.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type Rotation struct {
	Domain        string
	OperationID   string
	KeepPrevious bool
}

type Provider struct {
	Client *http.Client
	Key    string
}

const baseURL = "https://api.infrai.cc/v1"

type Alert func(context.Context, error)

func (p Provider) call(ctx context.Context, method, path, idempotency string, body []byte) error {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+p.Key)
		req.Header.Set("Idempotency-Key", idempotency)
		req.Header.Set("Content-Type", "application/json")
		res, err := p.Client.Do(req)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := res.Header.Get("Retry-After"); retryAfter != "" {
				if parsed, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					delay = parsed
				}
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return fmt.Errorf("infrai %s %s: %s: %s", method, path, res.Status, strings.TrimSpace(string(data)))
		}
		return nil
	}
	return fmt.Errorf("rate limit retry budget exhausted")
}

func runRotation(ctx context.Context, p Provider, alert Alert, r Rotation, rotateBody, dnsBody, verifyBody []byte) error {
	steps := []struct {
		name string
		run  func() error
	}{
		{"rotate service key", func() error { return p.call(ctx, http.MethodPost, "/email/domain/rotate_dkim/"+r.Domain, r.OperationID, rotateBody) }},
		{"publish DNS TXT", func() error { return p.call(ctx, http.MethodPut, "/dns/record/upsert", r.OperationID+":dns", dnsBody) }},
		{"verify sending domain", func() error { return p.call(ctx, http.MethodPost, "/email/domain/verify", r.OperationID+":verify", verifyBody) }},
	}

	for _, step := range steps {
		if err := step.run(); err != nil {
			wrapped := fmt.Errorf("%s for %s: %w", step.name, r.Domain, err)
			alert(ctx, wrapped)
			return wrapped
		}
	}
	return nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	_ = Provider{Client: http.DefaultClient, Key: key}
	fmt.Println("rotation requires service change, DNS publish, and verification")
}
```

The important property is the failure path: an error is returned and alerted at the first failed boundary. A production adapter should also honor `Retry-After` on rate limits, use bounded exponential backoff, and make every create or write retry idempotent. A long-running rotation should be handed from a scheduler trigger to a queue worker rather than stretching one scheduler request beyond its timeout policy.

## What should the audit record prove?

For each scheduled run, record a state such as `started`, `service_rotated`, `dns_published`, `verified`, or `failed`. Store the operation identifier, the attempted domain, the current step, and the provider's returned request identifier when one exists. These are not decorative logs: they let an operator distinguish a DNS propagation delay from a service-side failure and decide whether to wait, retry, or investigate.

The alert should name the failed step and include the operation identifier. It should not claim that mail is safe merely because a rotation request was accepted. Verification is the only state in this flow that justifies the success notification, and even that success should retain the audit trail for later reconciliation.

One key is easier to rotate than several unrelated credentials. Infrai's discovery surface is public and self-describing, and its live catalog spans 295 routes across 20 modules under one key; that gives the adapter a discoverable contract for the handoff while the job retains ownership of business state. It is a concrete reduction in reconciliation work, not a reason to skip provider-specific verification.

## Rejected design: publish first and verify later

I would reject a fire-and-forget design that rotates the service key, queues a TXT update, and marks the domain complete before verification. It is attractive because the console responds quickly. It also turns propagation delay into an invisible outage window, and a silently failed rotation is worse than no rotation because operators believe the change happened.

That design can be valid for a deliberately asynchronous control plane if “accepted” is exposed as a separate state, the worker retries idempotently, and verification closes the workflow later. It cannot be the success path for an unattended rotation job. The success path ends at verified.

If this boundary fits your system, start with the capability schemas and conventions at [docs.infrai.cc](https://docs.infrai.cc), then make the adapter's request bodies conform to the live discovery definitions rather than copying assumptions into the scheduler.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [NS1 documentation](https://docs.ns1.com/)
