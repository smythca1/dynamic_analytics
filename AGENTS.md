# AGENTS.md

# Dynamic Analytics Engineering Rules

This repository contains a production automated trading platform.

Correctness, deterministic behavior, and production safety are more important than implementation speed.

---

# REQUIRED WORKFLOW

Every request has TWO phases.

## Phase 1 — Analysis

Before modifying ANY code you MUST:

1. Read the relevant files.
2. Explain your understanding of the current implementation.
3. Explain the requested change.
4. List every file you intend to modify.
5. Identify risks and edge cases.
6. Explain how you will test the change.
7. STOP.

Do NOT modify any files during Phase 1.

Wait for the exact approval phrase:

APPROVED TO IMPLEMENT

Approval to investigate is NOT approval to implement.

---

## Phase 2 — Implementation

After receiving:

APPROVED TO IMPLEMENT

You may:

- Modify only the approved files.
- Keep changes as small as possible.
- Preserve backwards compatibility whenever possible.

If additional files become necessary:

STOP

Explain why.

Request approval again.

---

# PRODUCTION SAFETY

Never without explicit approval:

- Change live Alpaca execution
- Change reconciliation
- Change production configuration
- Change environment variables
- Change secrets
- Delete files
- Rename files
- Install dependencies
- Upgrade packages
- Commit
- Push
- Merge
- Deploy

---

# ARCHITECTURE

The platform follows:

Bronze
→ Raw immutable data

Silver
→ Canonical business logic

Gold
→ Analytics and reporting

Rules:

- Bronze never calculates business logic.
- Silver contains ONE canonical implementation.
- Gold never recreates Silver logic.

Backtesting and production must use the same Silver components.

---

# ACCOUNTING

Canonical reversal rules:

BUY + BUY

- First BUY closes a SHORT.
- Second BUY opens a LONG.

SELL + SELL

- First SELL closes a LONG.
- Second SELL opens a SHORT.

Realized PnL is calculated ONLY on the closing fill.

The second fill establishes the new position basis.

Never count both fills as independent position changes.

---

# TESTING

Before declaring success:

- Run relevant unit tests.
- Run regression tests if applicable.
- Explain exactly what was tested.
- Explain what was NOT tested.

Never claim production readiness without evidence.

---

# RESPONSE FORMAT

For analysis requests respond with:

1. Current behavior
2. Proposed solution
3. Files to modify
4. Risks
5. Test plan

Then STOP.

Wait for:

APPROVED TO IMPLEMENT
