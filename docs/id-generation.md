---
title: 'ID Generation'
linkTitle: 'ID Generation'
description: 'Typed ID generation strategies in dbx'
weight: 9
---

## ID Generation

`dbx` supports typed ID generation strategies for primary keys.  
Configure ID behavior directly in schema fields with `IDColumn[..., ..., Marker]`, not string tags.

## When to Use

- You need deterministic ID strategy per table/entity.
- You want runtime control of generator behavior at DB scope.
- You deploy multi-instance services and need explicit Snowflake node separation.

## Marker Types

| Marker | ID type | Behavior |
| --- | --- | --- |
| `dbx.IDAuto` | `int64` | Database auto-increment/identity (default for `int64` PK) |
| `idgen.IDSnowflake` | `int64` | App-generated Snowflake ID |
| `dbx.IDUUID` | `string` | App-generated UUID (defaults to v7) |
| `idgen.IDUUIDv7` | `string` | App-generated UUIDv7 |
| `dbx.IDUUIDv4` | `string` | App-generated UUIDv4 |
| `idgen.IDULID` | `string` | App-generated ULID |
| `dbx.IDKSUID` | `string` | App-generated KSUID |

## Recommended Usage

```go
type Event struct {
    ID   int64  `dbx:"id"`
    Name string `dbx:"name"`
}

type EventSchema struct {
    schemax.Schema[Event]
    ID   columnx.IDColumn[Event, int64, idgen.IDSnowflake] `dbx:"id,pk"`
    Name columnx.Column[Event, string]                   `dbx:"name"`
}
var Events = schemax.MustSchema("events", EventSchema{})
```

## Minimal Project Layout

```text
.
├── go.mod
└── main.go
```

## Defaults

- `int64` primary key with `dbx:"id,pk"` defaults to `db_auto`.
- `string` primary key with `dbx:"id,pk"` defaults to `uuid` with version `v7`.

## Production Guidance

- Single-instance can rely on the default node id behavior.
- Multi-instance should set an explicit stable node id with `dbx.WithNodeID(...)`.
- Keep declaration and execution separated: schema uses `IDColumn`, runtime generation uses DB options.
- `WithNodeID` and `WithIDGenerator` are mutually exclusive; configuring both returns an error.

## Migration Note

`idgen` and `uuidv` tag parameters are removed.  
Use marker types on `IDColumn` for explicit ID strategy configuration.

## Pitfalls

- Configuring both `WithNodeID` and `WithIDGenerator` in one DB instance returns error.
- Using out-of-range node ID results in `NodeIDOutOfRangeError`.
- Declaring marker type in schema but expecting package-global generator behavior is no longer valid.

## Verify

```bash
go test ./dbx/... -run ID
```
