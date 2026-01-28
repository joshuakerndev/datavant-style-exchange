# ADR 0001 — Tokenization Canonicalization Rules

## Status
Accepted — 2026-01-28

## Context
Tokenizer output must be deterministic and PII-safe. Canonicalization ensures consistent tokens for
semantically identical inputs (e.g., casing and extra whitespace).

## Decision
Tokenizer canonicalizes input fields before HMAC-SHA256 tokenization:

- `given_name`, `family_name`
  - Trim leading/trailing whitespace
  - Collapse internal whitespace to single spaces
  - Lowercase the result
- `dob`
  - Trim whitespace
  - Parse as `YYYY-MM-DD` only
  - Reformat to `YYYY-MM-DD`
- `ssn` (optional)
  - Trim whitespace
  - Strip all non-digit characters
  - If the normalized value is empty, treat as missing

The tokenizer builds a stable input string with explicit field labels:

```
given_name=<given>|family_name=<family>|dob=<dob>|ssn=<ssn>
```

The HMAC-SHA256 of this input (hex-encoded) is returned as `patient_token`.

## Consequences
- Tokenization is deterministic across callers and environments.
- Inputs that differ only in case or whitespace produce the same token.
- DOB must be a valid `YYYY-MM-DD` date; invalid dates are rejected.
- SSN formatting differences (e.g., dashes) do not change the token.
