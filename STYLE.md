# Style Guide

This repository prefers a small set of collection and optional-value conventions.

## Collection Semantics

- Prefer `collectionx` whenever code is expressing collection semantics:
  - map
  - filter
  - filter-map
  - associate / key-by
  - ordered map / set / list building
  - list merging and cloning
- Do not default to raw `for + append` for collection transformation.
- Keep collection-oriented APIs in `collectionx.List`, `collectionx.Map`, and `collectionx.OrderedMap` where that matches the surrounding code.

## General Helpers

- Prefer `lo` for small general-purpose helpers that are not the main collection abstraction:
  - string/value cleanup
  - tuple-style helpers
  - conditional helper expressions
  - small slice utilities when the code is not already centered on `collectionx`
- Do not mix `lo` and `collectionx` for the same transformation pipeline without a reason.

## Optional and Result Semantics

- Prefer `mo.Option[T]` when the API is representing an explicit maybe-present value.
- Keep `mo` at boundaries where optionality matters to callers.
- Do not use sentinel zero values where `mo.Option[T]` makes the contract clearer.

## Functional Options

- Use `github.com/arcgolabs/pkg/option` for internal functional-option plumbing.
- Do not use `pkg/option` as a replacement for `mo.Option`.

## Raw Loops

- Keep raw loops for process control, state machines, and side-effect-heavy orchestration.
- For data transformation, default back to `collectionx` first.
