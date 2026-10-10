<h1 align="center">Resonance</h1>

<p align="center">
  <strong>The Rust gateway for osu! stable and lazer in Illyaverse.</strong>
</p>

<p align="center">
  <a href="Cargo.toml"><img src="https://img.shields.io/badge/Rust-1.96-ce824d?style=flat-square&logo=rust" alt="Rust 1.96"></a>
  <a href="https://github.com/Illyavrs/Illyaverse"><img src="https://img.shields.io/badge/Part%20of-Illyaverse-black?style=flat-square" alt="Part of Illyaverse"></a>
  <a href="https://github.com/Illyavrs/Illyaverse/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-MIT-lightgrey?style=flat-square" alt="MIT License"></a>
</p>

Resonance handles game-client protocols and realtime transport.
[Sanctuary](https://github.com/Illyavrs/Sanctuary) provides the community REST API
and administration. Both services share PostgreSQL accounts, beatmaps, scores
and chat, plus stored media, map packages and replays.

## What it handles

| Client | Implemented responsibilities |
| --- | --- |
| stable | Bancho login, presence, channels, private messages and friends; legacy web endpoints, encrypted score submission, map/replay downloads, multiplayer and spectating. |
| lazer | OAuth and an API v2 subset, beatmap lookup/download, solo scores and replay capture/export, native notification chat, friends, HeadToHead room scores and SignalR hub subsets. |

The stack is Rust (2024 edition), Axum, Tokio and SQLx/PostgreSQL, with shared
domain types and a stable packet codec. Client compatibility remains experimental:
isolated fixtures do not certify native behavior. Replay playback, multiplayer
and private messages still need client verification. See the
[current integration record](https://github.com/Illyavrs/Illyaverse/blob/main/apps/docs/content/docs/reference/release-readiness.md)
and [protocol compatibility guide](https://github.com/Illyavrs/Illyaverse/blob/main/apps/docs/content/docs/reference/protocol-compatibility.md).

## Run with Illyaverse

Use a complete parent checkout and Docker Compose **2.24.4 or newer**. Follow
[Illyaverse setup](https://github.com/Illyavrs/Illyaverse/blob/main/README.md), then
run from the parent root:

```sh
docker compose up -d --build --wait
```

Compose exposes Resonance on `127.0.0.1:8081`. `GET /health` checks database
connectivity and returns `{"status":"ok","service":"resonance"}`.
[Deployment](https://github.com/Illyavrs/Illyaverse/blob/main/apps/docs/content/docs/deploy/deployment.md)
covers public HTTPS; [connection setup](https://github.com/Illyavrs/Illyaverse/blob/main/apps/docs/content/docs/clients/connection.md)
covers game connectivity and the launcher.

## Develop

Use Rust **1.96** from the parent root:

```sh
cargo build -p resonance --locked
cargo run -p resonance --locked
cargo test -p resonance --locked
cargo clippy -p resonance --all-targets --locked -- -D warnings
```

Local execution needs PostgreSQL, Sanctuary and configured `DATABASE_URL`,
`INTERNAL_SERVICE_KEY`, `SANCTUARY_INTERNAL_URL`, `PUBLIC_ORIGIN` and `STORAGE_PATH`.
Both services must share storage. `RESONANCE_BIND` defaults to `0.0.0.0:8081`.
The [development overlay](https://github.com/Illyavrs/Illyaverse/blob/main/compose.dev.yaml)
exposes loopback database/API ports; [architecture](https://github.com/Illyavrs/Illyaverse/blob/main/apps/docs/content/docs/deploy/architecture.md)
explains routing. Database and realtime integration tests need isolated fixtures.

## Repository boundary

[Illyavrs/Resonance](https://github.com/Illyavrs/Resonance) is pinned at
`services/resonance` in [Illyaverse](https://github.com/Illyavrs/Illyaverse).
The parent owns workspace manifests, lockfiles, shared crates, migrations and
deployment configuration. Docker builds use its root as the context; a child
clone lacks those inputs. See the [repository guide](https://github.com/Illyavrs/Illyaverse/blob/main/apps/docs/content/docs/deploy/repository-layout.md).

Code is original and [MIT-licensed](https://github.com/Illyavrs/Illyaverse/blob/main/LICENSE).
References inform protocol research; their code is not copied. Use scoped
Conventional Commits such as `fix(resonance): validate packet lengths`.
Contributor requirements are in [AGENTS.md](AGENTS.md).
