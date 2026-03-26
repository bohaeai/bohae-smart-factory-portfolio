# Manufacturing DX Portfolio

> Public-safe software portfolio excerpt for manufacturing operations.
> Company names, employee information, production identifiers, credentials, and on-site evidence have been removed or generalized.

## Overview

This repository is a sanitized portfolio snapshot of a manufacturing software stack. It keeps the technical shape of the work while removing customer-specific and operator-specific details.

Included areas:

- APS scheduling engine excerpts in Python
- API server samples in FastAPI
- MES and kiosk UI samples in Next.js and TypeScript
- ESP32 edge publisher example for counter telemetry
- SQL schema fragments and test samples
- Public-safe engineering notes and agent guidance

## Repository Layout

```text
manufacturing-dx-portfolio/
├── api/                 # FastAPI samples
├── aps-engine/          # APS scheduling engine excerpts
├── docs/                # Public-safe notes and contributor guidance
├── firmware/            # ESP32 telemetry example
├── mes-frontend/        # UI samples for ops, kiosk, twin, and AI screens
├── sql-sample/          # Schema and migration excerpts
└── tests-sample/        # Focused regression and guardrail tests
```

## Technical Focus

- Constraint-based production scheduling with OR-Tools CP-SAT
- FastAPI backend patterns for operations workflows
- Next.js App Router UI composition for operations tooling
- MQTT-based edge telemetry publishing from ESP32 devices
- SQL contracts for workflow, telemetry, and audit features

## Public-Safety Notes

- Real credentials, tokens, secrets, and internal domains were removed.
- Company, plant, line, and SKU names were generalized.
- Personal profile details and employment history were removed.
- On-site photos and screenshots were removed to avoid visual leakage and metadata exposure.
- Sample identifiers such as `PLANT_A`, `LINE_A_*`, and `BASELINE` are placeholders only.

## Usage

This repository is intended for review and discussion, not for direct deployment. Some paths, environment variables, and inputs are intentionally generalized to keep the snapshot public-safe.
