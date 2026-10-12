# Multi-model adapter stage 1

Baseline: 240bef780b3997884451b4a8f3ccd8fcbfc3734c. Qualified executor/worker/isolation unchanged.

Controller-side model_adapter.py accepts a fixed synthetic CSV spec. The model emits only files; identity, deadline, oracle and UPDATE hash belong to the controller. HTTP endpoints are fixed to DeepSeek/OpenRouter; credentials come from environment and never enter candidate containers. No Core V2 import or dependency. No mock or ChatGPT fallback exists in the actual llm_cycle.py route.

Adapter journal freezes the model result and task hash. Repeated requests with identical IDs/spec return the saved result without a model call. Changed payload rejects. Explicit HTTP/schema failure can switch provider once; ambiguous network outcome stops and requires reconciliation, not blind replay. Prepared status is never behavioral PASS. Actual executor verification and delivered oracle remain mandatory.

`python3 -m unittest -v test_executor test_model_adapter` uses explicitly synthetic TEST_DOUBLE_NOT_LLM fixtures for provider tests. Passing these tests does not prove a real API invocation. Secret pattern checks are a heuristic, not a complete data-loss prevention solution.

For a separately authorized synthetic host with rootless Podman and an already-present image:

```
python3 llm_cycle.py --root /separate/synthetic/state --image sha256:ACTUAL_ID --models models.example.json
```

Provide DEEPSEEK_API_KEY and OPENROUTER_API_KEY only through an operator-controlled secret facility, never chat/public files. models.example.json is an operator example, not an attestation that a model is available. Confirm current model IDs and existing account budget before calling. Max output 2048 tokens per call; no endless retries; costs are recorded only when provider usage reports them. Unknown charges remain unknown.

CREATE prefers DeepSeek; UPDATE prefers OpenRouter/Qwen, explicit failures may fall back. Retain private controller receipts (raw model output, prompt/task hashes, usage and provider response ID) outside public Git. llm_cycle.py intentionally requires both credentials for this two-provider stage. Missing credentials returns BLOCKED before candidate execution. It never substitutes sample_tasks.py output.

The generated cycle is host-level. To qualify remote multi-model delivery, obtain authentic provider receipts and host/run attestation and run verify_delivery.py on a second disposable VM. cycle-evidence.json is compatible with the existing delivery checker but does not self-assert remote PASS. Use a separate credentialless verification job. Never upload controller-private/ or general user prompts; only reviewed synthetic source and sanitized receipt metadata may be shared.

At preparation time no actual LLM result is qualified. Keep previously proven executor readiness separate from model integration readiness. Engineer Access is a separate design project, not part of this runtime.
