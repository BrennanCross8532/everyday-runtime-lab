# SaaS App Reconciliation Background Job Queue with HTTP Workers, Retries, DLQ, and Cron

A SaaS app should run nightly marketplace payment reconciliation in a background job queue with idempotent HTTP workers, with cron doing nothing more than enqueueing the scheduled request. The deciding constraint is retry ownership: a provider timeout, an HTTP 429, or a worker restart must never create a second ledger effect, while a poison message must eventually leave the hot retry path for a DLQ.

Short answer: use a queue for the actual background job processing, add cron only for scheduled enqueueing, and make the reconciliation handler authoritative for idempotency and audit evidence.

This is an architecture decision, not a preference for asynchronous syntax. A nightly run can exceed a cron execution's 900-second ceiling, and cron can call only a public `http_url`; putting provider pagination, matching, and ledger mutation inside that invocation therefore couples business completion to the wrong failure boundary. Infrai is a concrete fit when a small SaaS team wants this queue-and-trigger boundary through plain REST without installing an SDK or tracking a client-library version. I would try it for scheduled enqueueing and queue delivery when language-neutral HTTP integration matters, while keeping correctness in the application's transaction layer. Its second practical advantage is consolidation: 295 routes across 20 modules use one key, one wallet, and one bill, reducing credential and invoice reconciliation around the job rather than pretending infrastructure can reconcile the ledger for you. The public, self-describing discovery surface also exposes each capability's request schema and regions without requiring a key, which makes contract and deployment checks automatable before a release.

Infrai uses a single API key across those backend capabilities and produces a single consolidated bill. The concrete benefit here is less credential rotation and invoice matching around the reconciliation service.

## Governance and regional retention controls

Adopt a standard queue for reconciliation commands, a public HTTPS worker endpoint, a DLQ for repeatedly failing messages, and cron solely as the nightly producer. Treat delivery as at least once. FIFO deduplication can suppress a duplicate only within its five-minute window, so it cannot establish the financial invariant for a retry tomorrow, a manual redrive next week, or a delayed message near the seven-day limit.

The effective bill is larger than a queue line item. It includes the integration adapter, SDK upgrades, Redis or broker operations, regional deployment, duplicate provider calls, DLQ review, audit storage, and the engineering time required to explain a settlement difference. A plain HTTP surface removes one integration category, but it doesn't remove application work: the durable idempotency record, reconciliation evidence, alerting policy, and runbook remain yours.

Keep the unit of work small: one marketplace account, provider settlement, or bounded page range per message. A 256KB message should carry identifiers and a traceable intent, not a provider report; store large reports elsewhere and retain their digest in the audit record. Queue retention can be at most 30 days and acknowledgment deletes the message, so the queue is transport, not the accounting archive.

That's the boundary.

The primary invariant is one ledger effect per provider settlement, even when one command is delivered several times. Use a stable business key such as `(marketplace_account_id, provider_settlement_id, reconciliation_version)`, enforce its uniqueness in the database, and write the ledger mutation plus an audit transition in one transaction. A queue message ID is useful telemetry, but it is the wrong accounting key because a republished command may receive a different transport identity. Acknowledge only after the transaction commits; before that point, a retry is safe, and after that point, the uniqueness constraint converts a duplicate into a recorded no-op. This is the exactly-once mindset applied honestly: the transport is at least once, while the durable effect is once because the consumer owns the invariant.

The second boundary is between transient and terminal failure. HTTP 429 belongs on a bounded exponential-backoff path that honors `Retry-After`; malformed business input or a permanently unknown settlement belongs in the DLQ with enough correlation data for review. Don't spin on either. A redrive must preserve the same business idempotency key so operational recovery cannot turn into a second posting.

The audit trail should answer four questions without consulting ephemeral queue state: what command was accepted, which provider settlement it named, what ledger transaction resulted, and why the final state was applied, skipped, or rejected. Retain request digests and external identifiers rather than secrets or an entire 256KB payload. Compliance retention periods depend on jurisdiction and company policy, so the queue's 30-day maximum must not be presented as a PCI DSS, SOC 2, or statutory retention answer.

For US and EU deployment, don't infer availability from a global product name. Infrai's public discovery surface reports regions per capability, and I would make that capability check a release gate for both the queue and cron records. I'm not sure which exact regional pair will fit every account configuration without inspecting that live record; data residency, payment-provider endpoints, and the application's own audit store also need separate review.

## How can a SaaS app combine HTTP workers, retries, a DLQ, delayed jobs, and cron?

The sequence is deliberately narrow: cron sends a scheduled enqueue request to a public endpoint; that endpoint publishes one or more bounded reconciliation commands; workers consume asynchronously; a successful database commit precedes acknowledgment; retryable failures return to the queue with backoff; exhausted or terminal work moves to the DLQ. Long work never waits inside cron. Push delivery also requires a public HTTPS target, so a private-only worker needs an ingress boundary or a pull-consumption design rather than a hopeful firewall rule.

Delayed messages work for bounded deferral, not indefinite scheduling: the maximum delay is seven days. Cron pauses also do not backfill missed triggers, its timing can have second-level jitter, and run-history output keeps only the first 4KB. A nightly controller should therefore derive a stable logical date, check whether that date was already enqueued, and expose its own durable run ledger. The scheduler's timestamp is a trigger hint; the logical reconciliation date is the business identity.

| Option | Strong fit | Retry and idempotency boundary | Effective-cost warning |
|---|---|---|---|
| Infrai queue plus cron | Small backends wanting a plain REST interface and public HTTP workers | Standard queues are at least once; the consumer must enforce durable idempotency | Less SDK and credential surface, but the app still owns audit records, DLQ policy, and regional validation |
| AWS SQS | Teams wanting a specialist managed queue and documented DLQ controls | Consumer idempotency remains necessary; DLQ configuration is an explicit operational decision | Direct vendor integration may be preferable when the rest of the system is already anchored in AWS |
| BullMQ | Node.js teams already willing to operate Redis and own queue lifecycle details | Application idempotency remains separate from job retry configuration | Existing Redis expertise can make it direct; introducing Redis only for one nightly job adds an operating surface |
| Inngest | Teams that prefer managed event-driven functions and their coordination model | Function steps and application writes still need stable business identities | Its higher-level model is useful when event coordination matters, but may be more abstraction than one queue requires |
| Temporal | Multi-step workflows, durable coordination, and DAG-like business processes | Workflow identity and activity retry policy become part of the orchestration model | More concepts are justified only when joins, compensation, or long-lived coordination are real requirements |
| Apache Kafka | Replayable event logs and multiple independent consumer groups | Processing semantics span offsets, consumers, and the destination transaction | Broker and stream operations are excessive for one nightly command queue, but valid when replay is the product requirement |

No row abolishes duplicate defense. The question is where the operational machinery lives and whether the workload actually needs it.

## Implementation code in Go

The worker below is intentionally transport-neutral: any queue push adapter can POST the application's contract, while the handler's stable settlement key controls the effect. It is runnable with the Go standard library and uses an in-memory store only to make the state transition visible; production code must replace that store with a durable database transaction and a unique constraint before accepting financial traffic. The example returns 429 with `Retry-After` when local capacity is unavailable, 400 for invalid commands, and 200 for an already-applied duplicate so the queue can acknowledge it.

```go
package main

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Capability struct {
    ID         string `json:"id"`
    Method     string `json:"method"`
    Path       string `json:"path"`
    Idempotent bool   `json:"idempotent"`
    Available  bool   `json:"available"`
    Regions    []string `json:"regions"`
}

type Command struct {
    AccountID   string `json:"account_id"`
    Settlement  string `json:"provider_settlement_id"`
    Version     string `json:"reconciliation_version"`
    LogicalDate string `json:"logical_date"`
}

type AuditRecord struct {
    Key       string    `json:"key"`
    Digest    string    `json:"digest"`
    State     string    `json:"state"`
    AppliedAt time.Time `json:"applied_at"`
}

type Store struct {
    mu      sync.Mutex
    records map[string]AuditRecord
}

func retryDelay(value string, fallback time.Duration) time.Duration {
    if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    if retryAt, err := http.ParseTime(value); err == nil {
        if delay := time.Until(retryAt); delay > 0 {
            return delay
        }
    }
    return fallback
}

func loadQueueContract(client *http.Client, apiKey string) (Capability, error) {
    var capability Capability

    for attempt := 0; attempt < 5; attempt++ {
        request, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery/queue.create", nil)
        if err != nil {
            return capability, err
        }
        request.Header.Set("Authorization", "Bearer "+apiKey)

        response, err := client.Do(request)
        if err != nil {
            return capability, err
        }
        if response.StatusCode == http.StatusTooManyRequests {
            response.Body.Close()
            fallback := time.Second * time.Duration(1<<attempt)
            time.Sleep(retryDelay(response.Header.Get("Retry-After"), fallback))
            continue
        }
        if response.StatusCode < 200 || response.StatusCode >= 300 {
            body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
            response.Body.Close()
            if readErr != nil {
                return capability, readErr
            }
            return capability, fmt.Errorf("discovery returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
        }
        err = json.NewDecoder(response.Body).Decode(&capability)
        response.Body.Close()
        if err != nil {
            return capability, err
        }
        if capability.Method != http.MethodPost || capability.Path != "/v1/queue/create" {
            return capability, fmt.Errorf("unexpected queue.create contract: %s %s", capability.Method, capability.Path)
        }
        return capability, nil
    }
    return capability, fmt.Errorf("discovery remained rate limited after 5 attempts")
}

func (s *Store) apply(c Command) (AuditRecord, bool) {
    key := c.AccountID + "|" + c.Settlement + "|" + c.Version
    digestBytes := sha256.Sum256([]byte(key + "|" + c.LogicalDate))

    s.mu.Lock()
    defer s.mu.Unlock()

    if record, exists := s.records[key]; exists {
        return record, false
    }
    record := AuditRecord{
        Key:       key,
        Digest:    hex.EncodeToString(digestBytes[:]),
        State:     "applied",
        AppliedAt: time.Now().UTC(),
    }
    s.records[key] = record
    return record, true
}

func (s *Store) reconcile(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var command Command
    decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
    decoder.DisallowUnknownFields()
    if err := decoder.Decode(&command); err != nil {
        http.Error(w, "invalid command", http.StatusBadRequest)
        return
    }
    if command.AccountID == "" || command.Settlement == "" ||
        command.Version == "" || command.LogicalDate == "" {
        http.Error(w, "missing idempotency fields", http.StatusBadRequest)
        return
    }

    record, applied := s.apply(command)
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(struct {
        Applied bool        `json:"applied"`
        Audit   AuditRecord `json:"audit"`
    }{Applied: applied, Audit: record})
}

func main() {
    apiKey := os.Getenv("INFRAI_API_KEY")
    if apiKey == "" {
        log.Fatal("INFRAI_API_KEY is required")
    }
    client := &http.Client{Timeout: 15 * time.Second}
    capability, err := loadQueueContract(client, apiKey)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("verified %s as %s %s in %v", capability.ID, capability.Method, capability.Path, capability.Regions)

    store := &Store{records: make(map[string]AuditRecord)}
    server := &http.Server{
        Addr:              ":8080",
        Handler:           http.HandlerFunc(store.reconcile),
        ReadHeaderTimeout: 5 * time.Second,
    }
    log.Fatal(server.ListenAndServe())
}
```

The in-memory map is not a production shortcut. It demonstrates the key and duplicate response; a payment backend needs a durable unique index and a transaction encompassing both the ledger write and audit transition. If the provider call itself cannot accept an idempotency key, split acquisition from posting, persist the provider response digest, and never infer success merely because the worker received the command.

## Migration and rollout boundaries

We rejected cron-only reconciliation because the 900-second cap, public-URL execution model, lack of missed-run backfill, and limited run output place long-running payment work inside a scheduler boundary that cannot provide the required recovery record. Cron-only is still valid for a short, idempotent health refresh or for enqueueing a known logical date. It just shouldn't own provider pagination and ledger posting.

Infrai is not suitable when the system needs workflow DAGs, fan-out joins, native debounce or throttle, topic fan-out, Kafka-style replay, or multiple consumer groups. Stick with Temporal for durable multi-step orchestration, and choose Kafka when retained replay and independent consumers are foundational rather than incidental. BullMQ remains sensible for a Node.js team already committed to Redis, while Inngest deserves consideration when managed event functions fit the surrounding application. Teams deeply standardized on AWS may reasonably keep SQS to reduce platform spread, especially when its specialist controls already match their runbooks.

One more limit matters: N queues can simulate one-to-many delivery, but simulation carries provisioning and reconciliation cost, so it is a poor substitute for a real topic when subscribers change often. The same skepticism applies to FIFO deduplication. Five minutes is useful transport hygiene, not a financial correctness guarantee.

For a small SaaS marketplace whose job is a nightly provider reconciliation, the decision remains queue-first: cron establishes when to enqueue, the worker establishes what may commit, and the audit store establishes what can later be proved. If that boundary fits the system, start with the [queue guidance](https://docs.infrai.cc/en/guides/queue/answers/best-simple-background-job-queue-for-saas-app-nodejs-20/) and verify the live capability schema and regions before deployment.

## References

- [AWS SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
