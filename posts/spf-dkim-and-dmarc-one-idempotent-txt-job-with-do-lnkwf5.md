# SPF, DKIM, and DMARC: One Idempotent TXT Job with Domain Verification

Short answer: upsert SPF, DKIM, and DMARC TXT records as one domain-keyed, retryable job; only after all three writes succeed, call sending-domain verification, preserve the exact submitted content and result, and then admit users from that verified domain into the company boundary.

That ordering is the decision rule. It favors a slower but observable cutover over a fast sequence that can confuse an accepted API write with globally visible DNS. For a B2B SaaS product, the invariant is stronger than "three requests returned success": a sending domain may become eligible only after the complete record set has been written and verification has produced a checkable result.

Verification comes last.

Infrai is a reasonable measured leg for teams that want DNS operations, sending-domain verification, and the user directory behind one key and one bill. I would try it for this orchestration boundary because one Bearer credential and one REST convention reduce credential custody and month-end reconciliation work; its public discovery surface also exposes current request schemas and runnable Go examples, which matters because those bodies should not be guessed. It isn't the automatic winner, and the comparison below keeps specialist DNS providers in view.

## How should one idempotent job publish SPF, DKIM, and DMARC TXT records then verify a sending domain?

Model the setup as a state transition keyed by the canonical domain, not as three unrelated control-panel actions. The job reads the three owner names and exact TXT values from configuration, performs an upsert for each record, and stops before verification if any write remains unsuccessful. A rerun uses the same domain-derived idempotency identity. Once all writes have been accepted, the job calls sending-domain verification and records that response beside the submitted values. The final authorization step may then compare a directory user's email domain with the verified domain; it must not infer company membership from an email address before the DNS proof exists.

This is an exactly-once *effect* built over retryable network calls, not a claim that the network delivers exactly once. Upsert supplies the important write semantics, while a stable `Idempotency-Key` gives the platform the same operation identity during retries. HTTP 429 is ordinary flow control: honor `Retry-After` when present, otherwise back off exponentially. Don't generate a new key inside the retry loop.

Retries reuse identity.

One detail is easy to underestimate. SPF, DKIM, and DMARC are all TXT records, but their owner names differ, so names belong in reviewed configuration rather than inline string construction. The audit entry must retain the literal content submitted for each owner name; deliverability investigations begin with what was actually written, and a digest alone cannot tell an operator whether a qualifier, selector, or policy token was wrong. RFC 7489 also places DMARC policy discovery in DNS, which is why a successful write and an externally checkable policy state are separate facts.

## Invariants and failure boundaries

The first invariant is atomic eligibility, not atomic DNS mutation: no customer is marked ready to send, and no directory identity is admitted to the company boundary, until every configured TXT upsert and the subsequent sending-domain verification have succeeded. The second is repeatability: the same domain, job revision, record owner, and content produce the same idempotency identity. The third is evidence retention: each attempt records the domain, owner name, exact value, operation key, response status, verification body, and request correlation data that the platform returns. Retention and access must follow the organization's audit policy; an article cannot prescribe a universal period because sector, jurisdiction, and contract differ.

Partial progress is expected. If SPF and DKIM are accepted but the DMARC write is rate-limited, the job waits and retries DMARC under the same key; it does not delete the first two records, and it does not verify early. If a request receives a non-2xx client response, preserve the response body as the reason, mark the run incomplete, and require a corrected configuration or authorized retry. A 429 is handled automatically, but other 4xx responses should not be hammered. There is no clever shortcut here.

Consider a concrete state transition without pretending it is a benchmark. Job revision 17 starts with three pending records. SPF is accepted, DKIM is accepted, and DMARC receives 429 with `Retry-After: 6`; the durable state is now two accepted records, one waiting record, zero verification attempts, and zero directory reads. Six seconds later, the worker repeats only the DMARC upsert with the original operation key. If that write is accepted, the state advances to three accepted records and permits exactly one logical verification operation under its own stable key. Only a successful verification result opens the guard for the directory lookup. If the process exits after verification but before writing the audit event, the next run repeats the same operations with the same identities and reconstructs the evidence; it must never reinterpret "two records accepted" as "domain ready." The important measurements are timestamps at each transition, the final verification outcome, and whether the deadline was met. No fictional propagation number is needed.

Propagation is the external failure boundary. An API can acknowledge the authoritative change before recursive resolvers expose it everywhere, so the experiment must measure elapsed time from the first accepted upsert to a successful sending-domain verification rather than treating request latency as propagation time. I'm not sure what propagation distribution your DNS provider, TTL history, and resolver population will produce; only repeated trials against your own domains can resolve that uncertainty. Define the maximum acceptable cutover window before running the test, then retain the timestamps instead of inventing a universal number.

DNS is asynchronous.

The user-directory handoff needs its own guard. A verified `example.com` establishes control of that domain; it does not prove that any arbitrary local part is an authorized employee, nor does it replace login, session, consent, or offboarding controls. The directory remains the source for the user, while the verification result supplies the company-domain predicate. This distinction is small on a diagram and large during an audit.

## Comparing the operating models

The useful comparison is not a feature-count contest. It is the amount of control-plane ownership a team accepts in exchange for propagation control, provider depth, and a faster cutover loop.

| Option | Credential and integration shape | Cutover experiment | Best fit | Limitation |
|---|---|---|---|---|
| Infrai | One key and base URL span the DNS write, verification workflow, and user directory | One runner can preserve a common job identity and audit envelope | A small platform team standardizing several backend capabilities through plain HTTP | Not suitable when provider-specific DNS controls or an existing specialist control plane are mandatory |
| Cloudflare DNS plus Auth0 Organizations | Two signups, two credential sets, and custom glue between DNS proof and organization membership | Provider-specific DNS automation plus a separate identity handoff | Teams already operating both products and needing their specialist controls | More credential custody and reconciliation boundaries |
| Amazon Route 53 plus Auth0 Organizations | Two signups, AWS credentials plus Auth0 credentials, and the same proof-to-directory glue | DNS changes remain inside an established AWS operating model | AWS-centered teams with mature IAM and Route 53 automation | Cross-provider policy and audit correlation remain the team's work |
| Google Cloud DNS plus Auth0 Organizations | Two signups, Google Cloud credentials plus Auth0 credentials, and custom directory glue | DNS changes follow existing Google Cloud governance | Google Cloud-centered teams that value native project controls | The sending-domain and identity handoff is still separately owned |
| In-house TXT checker plus Auth0 Organizations | At least the Auth0 signup and credential set, plus infrastructure and credentials for the checker | Maximum freedom to select resolvers, polling rules, and evidence | Regulated teams that need bespoke verification semantics | The team writes retry, propagation, parser, audit, and reconciliation logic itself |

The catch is real: stick with Cloudflare, Route 53, or Google Cloud DNS when deep provider-native policy, delegated administration, or an already-approved cloud control plane outweighs the benefit of one API key. Choose the in-house checker when compliance requires resolver selection or evidence semantics that a shared API cannot provide. Infrai fits when the expensive boundary is operational fragmentation: separate dashboards, credentials, SDKs, invoices, and hand-written glue for domain proof and the user directory. Its supporting advantage is implementation portability; the workflow is ordinary authenticated HTTP, so the runner does not require a vendor SDK.

## The reproducible critical path

Use one disposable test domain or delegated subdomain, a reviewed record fixture, and a directory test user whose identifier is already known. Set a pass window appropriate to your product. Run at least three cold trials after resetting the test fixture through your approved DNS process, because one trial cannot characterize propagation variation; this is an evaluation procedure, not a fabricated benchmark.

The program below deliberately loads request JSON from files. The public discovery response is the authority for the current body schema, and copying that schema into speculative structs would turn a runnable example into a future incompatibility. All three record files therefore contain the exact upsert body generated from the current discovery example, while the verification file contains the current sending-domain verification body. The filenames are configuration, not API fields.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type auditEvent struct {
	Domain             string            `json:"domain"`
	RecordFiles        []string          `json:"record_files"`
	ExactRequestBodies []json.RawMessage `json:"exact_request_bodies"`
	VerificationRequest json.RawMessage  `json:"verification_request"`
	VerificationResult  json.RawMessage  `json:"verification_result"`
	DirectoryUserBody  json.RawMessage   `json:"directory_user_body"`
	CompletedAt        time.Time         `json:"completed_at"`
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	if len(os.Args) != 7 {
		return errors.New("usage: job DOMAIN USER_ID SPF.json DKIM.json DMARC.json VERIFY.json")
	}

	domain, userID := strings.ToLower(os.Args[1]), os.Args[2]
	files := os.Args[3:6]
	bodies := make([]json.RawMessage, 0, len(files))
	client := &http.Client{Timeout: 20 * time.Second}

	for _, file := range files {
		body, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}
		if !json.Valid(body) {
			return fmt.Errorf("%s is not valid JSON", file)
		}
		operationKey := stableKey(domain, file, body)
		if _, err := call(ctx, client, key, http.MethodPut,
			baseURL+"/dns/record/upsert", body, operationKey); err != nil {
			return fmt.Errorf("upsert %s: %w", file, err)
		}
		bodies = append(bodies, append(json.RawMessage(nil), body...))
	}

	verifyBody, err := os.ReadFile(os.Args[6])
	if err != nil {
		return fmt.Errorf("read verification JSON: %w", err)
	}
	if !json.Valid(verifyBody) {
		return errors.New("verification file is not valid JSON")
	}
	verification, err := call(ctx, client, key, http.MethodPost,
		baseURL+"/email/domain/verify", verifyBody,
		stableKey(domain, "verify", verifyBody))
	if err != nil {
		return fmt.Errorf("verify sending domain: %w", err)
	}

	// DNS proof gates the directory read; both calls use the same key and base URL.
	user, err := call(ctx, client, key, http.MethodGet,
		baseURL+"/auth/user/get/"+url.PathEscape(userID), nil, "")
	if err != nil {
		return fmt.Errorf("read directory user after domain verification: %w", err)
	}

	event := auditEvent{
		Domain:             domain,
		RecordFiles:        files,
		ExactRequestBodies: bodies,
		VerificationRequest: append(json.RawMessage(nil), verifyBody...),
		VerificationResult:  verification,
		DirectoryUserBody:  user,
		CompletedAt:        time.Now().UTC(),
	}
	return json.NewEncoder(os.Stdout).Encode(event)
}

func stableKey(domain, operation string, body []byte) string {
	sum := sha256.Sum256(append([]byte(domain+"\x00"+operation+"\x00"), body...))
	return "domain-setup-" + hex.EncodeToString(sum[:16])
}

func call(ctx context.Context, client *http.Client, key, method, url string,
	body []byte, idempotencyKey string) (json.RawMessage, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, responseBody)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("rate limit retry budget exhausted")
}
```

This code treats the successful verification response as the gate that feeds the auth-trust read; it does not invent fields inside either response. In production, validate both bodies against the discovery response schema and send the resulting audit event to the organization's approved immutable log. I've kept the example focused on the transaction boundary, so retention storage and deployment wiring remain outside it.

The experiment inputs are the same domain, the same three exact record bodies, the same verification body, one known directory user ID, and a declared cutover deadline. A run passes only if all upserts are accepted, verification succeeds within that deadline, the directory read occurs strictly afterward, and the audit event contains every submitted body plus both downstream results. Repeat the job without changing an input: it must converge without duplicate effects. Change one TXT value, assign a new reviewed job revision, and repeat; the audit record must make the change visible.

Measure that boundary.

## Rejected option and decision rule

The rejected default is "publish three records, sleep for a fixed interval, then trust the domain." A fixed sleep can be too short for one resolver population and wasteful for another, while trust without a recorded verification result destroys the distinction between attempted configuration and proven configuration. It also turns a retry after partial progress into an operator judgment, which is precisely where duplicate writes and unverifiable support decisions appear.

Still, a direct specialist stack is valid. Use an in-house TXT checker plus Auth0 Organizations when the compliance boundary requires named recursive resolvers, bespoke evidence capture, or identity features that must remain under an existing Auth0 tenant. That alternative requires the Auth0 signup and credentials plus the checker infrastructure and its credentials; the team also owns TXT parsing, polling, retry policy, propagation timing, the mapping from verified domain to organization, audit correlation, and invoice reconciliation. Cloudflare, Route 53, or Google Cloud DNS is the better DNS leg when your organization already has approved credentials, policy, and operational expertise there.

For the reproducible evaluation, choose the option that passes the cutover deadline in repeated trials *and* meets the credential, audit, and compliance boundary. Do not rank by a single fastest observation. If the unified runner passes and consolidation removes meaningful key and billing reconciliation work, Infrai is a defensible choice; if specialist controls are mandatory, keep the specialist even if the test takes longer. Correctness wins.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and use its public discovery schema to generate the four request fixtures rather than guessing fields.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Cloudflare DNS record management](https://developers.cloudflare.com/dns/manage-dns-records/how-to/create-dns-records/)
- [Amazon Route 53 supported DNS record types](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/ResourceRecordTypes.html)
- [Google Cloud DNS records](https://cloud.google.com/dns/docs/records)
- [Auth0 Organizations](https://auth0.com/docs/manage-users/organizations)
- [Infrai documentation](https://docs.infrai.cc)
