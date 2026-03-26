# AGENTS.md — Public Portfolio Guide

This repository is a public-safe portfolio excerpt. Treat it as sample code, not as a live production system.

## Primary Rule

Do not reintroduce identifying or sensitive data.

That includes:

- real company names
- plant and line names from production
- employee names, contact details, or personal history
- secrets, tokens, passwords, domains, IP addresses, or internal hosts
- on-site photos, screenshots, or exported operational evidence

## Allowed Content

- generalized code excerpts
- placeholder configuration values
- sample identifiers such as `PLANT_A`, `LINE_A_01`, and `BASELINE`
- high-level domain notes that do not expose customer-specific operations
- tests and SQL fragments that remain generic

## Editing Rules

- Prefer placeholders like `YOUR_WIFI_SSID`, `broker.example.com`, and `demo_user`.
- Keep public-facing copy generic: use terms like `manufacturer`, `plant`, `operator`, and `factory`.
- If a code path needs sample identifiers, use neutral values rather than real production names.
- Remove or generalize exact business metrics unless they are already public and intentionally disclosed.
- Avoid adding raw dumps from logs, spreadsheets, dashboards, or field devices.

## Repository Structure

- `aps-engine/`: scheduling and extraction samples
- `api/`: backend route and workflow samples
- `mes-frontend/`: UI components and page excerpts
- `firmware/`: edge telemetry example
- `sql-sample/`: schema excerpts
- `tests-sample/`: focused regression samples
- `docs/`: public-safe notes

## Verification Before Publishing

Run a final pass for:

- secrets and credentials
- internal domains and IPs
- company and personal names
- photos or screenshots with metadata
- customer-specific SKU, plant, and line identifiers

If a detail is not required to explain the engineering work, remove it.
