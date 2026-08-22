# Next.js Serverless Health Checks: Regional Uptime Monitoring with Metrics Dashboards

Short answer: a checkout service needs a cheap Next.js `/api/health` Route Handler, independent EU and US polling, request and error counters, a periodic availability gauge, and separately grouped exceptions; this combination supports basic uptime monitoring and incident reconstruction without turning the health check into another production dependency.

The operational constraint changes the design: a probe must be cheap enough to run continuously, yet a checkout incident demands more evidence than one green or red response. Don't make the route execute a payment, scan a ledger, or perform an expensive dependency query. Return the deployed app version, a timestamp, and bounded dependency status, then preserve failures in metrics and error records where they can be correlated after the fact.

## How should a Next.js serverless health check feed an uptime metrics dashboard?

Treat the Route Handler as a narrow observation boundary. `/api/health` should answer whether this deployment can accept useful work now, not whether every historical checkout reconciles correctly. Its response needs three stable facts: the application version, the server timestamp, and dependency status. A non-success HTTP status should mean that at least one required dependency cannot support checkout; optional dependencies should remain visible but should not silently redefine availability.

Keep the dependency checks bounded. A cached database readiness result or a lightweight connection check can be appropriate, while a full order query is not: frequent probes multiply that query across serverless instances and regions, potentially worsening the incident they are meant to observe. The timestamp also needs a precise interpretation. It is the time the server produced the response, not proof that a scheduled checkout task ran and not a substitute for a client-side latency measurement.

The dashboard should derive recent availability from poll outcomes and show failure spikes from counters. Record at least the probe result, observed HTTP status, region, deployment version, and observed duration at the collection boundary. Use counters for total requests and failures because increments survive aggregation; publish a periodic gauge for the latest availability state because operators need a compact current view. A 30-second probe interval and a five-minute window are reasonable starting policy choices, not universal truths — traffic, recovery objectives, and provider limits should determine the production values.

One point matters disproportionately: **EU and US checks must remain separate series**. A global average can look healthy while one region cannot reach a dependency. Region is therefore evidence, not dashboard decoration.

Green is not proof.

## Define the reconstruction ledger before choosing a collector

An uptime chart establishes when an external observer could reach the checkout path. It does not establish why a checkout failed, whether a retry duplicated a write, or which deployment introduced the change. For reconstruction, join the probe timeline with request and error counters by deployment version and region, then inspect exceptions grouped by stable failure identity. Sentry documents why grouping and fingerprints matter: many raw events may represent one underlying fault, while an unstable fingerprint can fragment one incident into noise.

The exactly-once mindset belongs here even though HTTP delivery is not exactly once. Metric reporting and error capture can be retried, so the collector should assign a stable observation ID derived from region, scheduled probe time, and target deployment; downstream storage can then deduplicate retries and retain an audit trail of the original observation, retry count, and final disposition. Without that lineage, a network timeout followed by a successful retry can inflate both the failure and success counters, which is precisely the kind of small accounting error that distorts an incident review.

Retry identity is accounting.

Capture exceptions independently from health responses. A health route should not serialize stack traces or checkout details to an unauthenticated caller, and an HTTP 200 from the route must not erase a payment exception that occurred seconds earlier. Conversely, an HTTP 503 from a dependency check is an availability observation, not automatically an application exception. Store both signals, correlate them by time, region, version, and trace identifiers where available, and keep sensitive payment data out of either payload.

This is the catch: logs that carry `trace_id` and `span_id` can support correlation, but they don't create a distributed trace query or span tree. If reconstruction requires causal traversal across services, choose a tracing system that provides it. Compliance also sets a harder boundary than convenience: a log system without deletion by user is not suitable as the sole store for personal data subject to erasure requests, and a retention error code is not the same thing as an operator-controlled retention policy. Minimize data at ingestion and keep the authoritative audit record in a store whose retention and deletion controls have been reviewed.

## Implement the alert read path as a bounded Go adapter

The Next.js implementation can remain a Route Handler while collection and notification stay language-independent. The following Go program performs the scheduled metrics read needed by an alert adapter. It calls the verified query route without inventing undeclared filter parameters, takes both the API base URL and key from the environment, uses an explicit method, surfaces non-success bodies, and retries HTTP 429 with `Retry-After` or exponential backoff. The response is emitted unchanged because the query response shape is not specified here; a production adapter should validate it against discovery before evaluating a threshold.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		fail(errors.New("INFRAI_BASE_URL and INFRAI_API_KEY are required"))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	body, err := queryMetrics(ctx, strings.TrimRight(baseURL, "/"), apiKey)
	if err != nil {
		fail(err)
	}
	fmt.Println(string(body))
}

func queryMetrics(ctx context.Context, baseURL, apiKey string) ([]byte, error) {
	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx, http.MethodGet, baseURL+"/v1/metrics/query", nil,
		)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("metrics query returned HTTP %d: %s", resp.StatusCode, body)
		}

		wait := retryDelay(resp.Header.Get("Retry-After"), backoff)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		backoff *= 2
	}
	return nil, errors.New("metrics query remained rate limited after 5 attempts")
}

func retryDelay(header string, fallback time.Duration) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(header); err == nil {
		if delay := time.Until(deadline); delay > 0 {
			return delay
		}
	}
	return fallback
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
```

This adapter is deliberately only the read side. The health collectors still create EU and US observations with distinct identities and network paths; only retries of the same scheduled regional observation should deduplicate. Keep the raw observation ID in the audit record even if the dashboard retains only rolled-up counters, and keep notification disposition beside it so an incident review can distinguish “threshold evaluated” from “message delivered.”

## Compare ownership boundaries after the evidence model is fixed

No single row wins every incident. The useful comparison is the evidence each option contributes and the boundary that forces another component, not the number of dashboard widgets.

| Product | Evidence it can contribute here | Decision boundary |
|---|---|---|
| Sentry | Documented event grouping and fingerprint mechanics for consolidating related exceptions | Use it when exception grouping is the primary reconstruction problem; verify the separate uptime path against your own acceptance test |
| Healthchecks.io | Missing-heartbeat coverage for detecting work that should have run but did not | Prefer it for silent scheduled-job failure, which an on-demand `/api/health` response cannot prove |
| Datadog | A real candidate for the same regional probe, metric, error, alert, and trace acceptance tests | This source set does not establish its behavior, so I'm not sure it should win without a documented trial using the checkout failure cases |
| Grafana | A real candidate for the metrics-dashboard layer and the same regional-series acceptance test | Validate its data-source and notification path in a documented trial before assigning it ownership of checkout evidence |
| Infrai | Metrics reporting and querying plus error capture and grouping through one REST API under one key, with no SDK required; its broader surface spans 295 routes in 20 modules | There is no built-in alert delivery, heartbeat monitoring, distributed trace tree, source-map decoding, crash symbolication, or Session Replay; poll the query surfaces for notifications, and choose dedicated tools when those capabilities are required |

The final option's practical advantage is breadth behind a simple surface: adding an operational capability follows the same HTTP contract rather than adding another integration. That can be valuable for a small platform team when consistent conventions simplify its audit. It is not suitable when the checkout on-call rotation requires native threshold rules, phone, SMS, or webhook delivery, and it should not replace Healthchecks.io for “the job never ran” detection or a tracing product for service-to-service causality.

There is another boundary. Session Replay, source-map decoding, and crash symbolication answer different questions from server-side uptime, so a browser-heavy checkout may rationally keep a dedicated error product even while centralizing counters elsewhere. Stick with Sentry when its grouping workflow and richer client-side debugging evidence are the deciding requirements. Use the dashboard as an index into evidence, not as the evidence itself.

## Migrate by proving one checkout failure at a time

Start with the Route Handler in one deployment, but exercise it from outside the hosting platform. Confirm that its payload contains version, timestamp, and bounded dependency status; that it returns quickly under a dependency failure; and that it reveals neither credentials nor customer data. Then deploy one EU and one US collector with stable scheduled observation IDs. Run them long enough to see a deployment boundary and a controlled dependency failure in a non-production environment.

Next, wire total requests, failures, and the periodic availability gauge into a small dashboard, preserving region and version as dimensions. Send application exceptions through the separate error path and verify that repeated instances form the intended groups. Finally, make notification behavior explicit: because the metrics and error surfaces do not deliver alerts, a scheduler must poll them and hand threshold breaches to an approved notification system. Define retry and deduplication rules before enabling that path; an alert that fires twice is annoying, while a checkout mutation that applies twice is an accounting incident.

Keep the rollout compact.

The acceptance test is a reconstructable timeline: an operator can identify the affected region, deployment version, first and last failed probes, counter spike, grouped exception, retry history, and notification disposition without querying customer payment data. If the test cannot produce that chain, more dashboard panels won't repair the missing audit evidence.

## References

- https://nextjs.org/docs/app/building-your-application/routing/route-handlers
- https://docs.sentry.io/concepts/data-management/event-grouping/
- https://healthchecks.io/docs/
- https://docs.datadoghq.com/synthetics/api_tests/http_tests/
- https://grafana.com/docs/grafana/latest/dashboards/
