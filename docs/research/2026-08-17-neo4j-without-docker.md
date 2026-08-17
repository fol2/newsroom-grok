# Can Neo4j 5.x run as a process on this computer without Docker?

- Role: Dated research finding for [issue #8](https://github.com/fol2/newsroom-grok/issues/8)
- Status: Completed
- Owner: fol2
- Canonical language: English
- Date: 2026-08-17
- Related operations: [`../operations/neo4j-b2-qualification.md`](../operations/neo4j-b2-qualification.md)
- Related pin: `newsroom/projection/neo4j/models.py` (`NEO4J_B2_*`)
- Source policy: Neo4j 5 official install docs and newsroom-grok projection notes via `gh` API. Neo4j was not installed. The runtime was not implemented.

## Research question

Can Neo4j 5.x run as a process on this Grok Bot computer (Debian 13, x86_64, 16 GiB, no swap, no Docker, unprivileged container with passwordless sudo) without the pinned ARM64 image?

## Verdict

**No.**

**Blocking constraint:** this host has no Java 17 or 21 runtime. Neo4j 5.x cannot start as a process without a compatible JVM.

Two independent follow-on constraints, neither of which is the start blocker:

1. The official Debian *service* path (`systemctl enable|start neo4j`) needs systemd. PID 1 here is `tini`, not systemd.
2. Even if a 5.x process were started, newsroom-grok would reject it. The exact B2 target is Community **2026.06.0**, not 5.x.

## Computer facts used

Already verified for this ticket; re-checked 2026-08-17 (Europe/London):

| Fact | Value |
|---|---|
| OS | Debian GNU/Linux 13 (trixie), x86_64 |
| Memory | 15 GiB RAM, 0 swap |
| Container | `/.dockerenv` present; PID 1 is `tini` |
| Docker / Podman | not installed |
| Java | not installed (`java` not on `PATH`) |
| sudo | passwordless (`sudo -n true`) |
| Open files | `ulimit -n` = 524288 |
| Root filesystem | overlay |

## What newsroom-grok actually pins

Not Neo4j 5.x, and not an ARM64-only digest.

From `newsroom/projection/neo4j/models.py`:

```text
NEO4J_B2_IMAGE = "neo4j:2026.06.0-community-trixie"
NEO4J_B2_SERVER_VERSION = "2026.06.0"
NEO4J_B2_DRIVER_VERSION = "6.2.0"
```

[`docs/operations/neo4j-b2-qualification.md`](../operations/neo4j-b2-qualification.md) repeats the same container tag, server version, and `neo4j==6.2.0` driver pin. `pyproject.toml` pins the driver at `neo4j==6.2.0`.

The adapter fail-closes on any other server:

```text
if compatibility.server_version != NEO4J_B2_SERVER_VERSION:
    raise Neo4jCompatibilityError(
        "Neo4j server is not the exact B2 qualification target"
    )
```

Source: `newsroom/projection/neo4j/_adapter.py` (`verify_compatibility`). Qualifying profiles repeat the same check in `newsroom/projection/neo4j/qualification.py`.

The official `neo4j` image tag is multi-arch (`amd64` and `arm64v8`), not ARM64-only. This host is x86_64 and has no Docker, so the image is unused either way. The "ARM64" wording in the ticket is the macm4/Hermes context, not a single-arch pin in this fork.

This fork also does not ship the historical `.github/workflows/projection-b2-neo4j.yml` start path (`d950336`). There is no repository-owned container or process supervisor for Neo4j here.

## Official Neo4j 5.x process paths (no Docker)

Primary sources, Neo4j 5.26 LTS operations manual:

- [System requirements](https://neo4j.com/docs/operations-manual/5/installation/requirements/)
- [Debian package](https://neo4j.com/docs/operations-manual/5/installation/linux/debian/)
- [Linux tarball](https://neo4j.com/docs/operations-manual/5/installation/linux/tarball/)
- [systemd service](https://neo4j.com/docs/operations-manual/5/installation/linux/systemd/)
- [Memory configuration](https://neo4j.com/docs/operations-manual/5/performance/memory-configuration/)
- [Docker introduction](https://neo4j.com/docs/operations-manual/5/docker/introduction/) (image-only; cited to show what the pin is *not*)

### Java is mandatory

Neo4j 5.x requires Java SE 17. From 5.14 / 5.26 LTS, Java 21 is also supported. The Debian and tarball docs both say a compatible JRE must exist before Neo4j can start. This host has none. That is the start blocker.

Debian 13's default JDK line is 21, which 5.26 LTS accepts. Java was not installed (ticket rule).

### Two documented non-Docker installs

1. **Debian package** (`apt-get install neo4j=1:5.26.29`). The `.deb` is architecture-independent (`neo4j_5.26.29_all.deb`). Auto-start is `sudo systemctl enable neo4j`. Non-systemd hosts are acknowledged only for `/etc/default/neo4j` env overrides, not as a supported service manager. This path is closed here: no systemd as init.

2. **Unix tarball.** After Java 17 or 21 is present:

   - console process: `<NEO4J_HOME>/bin/neo4j console`
   - background process: `<NEO4J_HOME>/bin/neo4j start`

   systemd is optional and only used if an operator writes a unit. This is the only official path that matches "run as a process" on a tini/unprivileged container. It was not installed.

The official Docker image is the same unix tarball plus a bundled JRE. Without Docker, the missing piece the image would have supplied is Java.

### Hardware and OS: not the blocker

| Requirement (Neo4j 5 docs) | This computer | Blocks start? |
|---|---|---|
| x86_64 or ARM | x86_64 | No |
| Personal/dev memory: 2 GiB min, 16 GiB recommended | 15 GiB | No |
| Swap off recommended for a dedicated server | 0 swap | No (aligned) |
| `ulimit -n` 40000+ / 60000 | 524288 | No |
| Supported native OS: Debian 11, 12 (not 13) | Debian 13 | Support gap, not a start proof |
| Supported filesystems: EXT4, XFS | overlay root | Support gap; official image also runs on overlay |
| Containerized platforms | unprivileged Debian 13 container | Officially in-scope as a platform class |

Debian 13 is absent from the Neo4j 5.x *native* OS matrix. It is, however, the default Docker base from Neo4j 5.26.20 (`debian:trixie-slim`). That is a support/qualification gap for a native 5.x package, not evidence that the JVM binary cannot execute.

16 GiB with no swap is inside the personal/dev guideline. Other agents share this machine and currently leave about 11 GiB available; a later runtime ticket would still need explicit heap and page-cache caps so the heuristic startup sizes do not crowd the host. That is capacity planning, not the start blocker.

## What this does not authorise

- Installing Java or Neo4j.
- Implementing a Grok Bot Neo4j runtime, supervisor, or first-boot step.
- Treating 5.x as an admitted substitute for `2026.06.0` Community.
- Changing map issue #1.

A later implementation ticket, if any, would install a compatible JRE and start the **pinned** Community **2026.06.0** unix tarball as a console/background process. That is a different question from "can 5.x run here without the image."

## Sources

1. Neo4j 5 system requirements — <https://neo4j.com/docs/operations-manual/5/installation/requirements/>
2. Neo4j 5 Debian install — <https://neo4j.com/docs/operations-manual/5/installation/linux/debian/>
3. Neo4j 5 Linux tarball — <https://neo4j.com/docs/operations-manual/5/installation/linux/tarball/>
4. Neo4j 5 systemd service — <https://neo4j.com/docs/operations-manual/5/installation/linux/systemd/>
5. Neo4j 5 memory configuration — <https://neo4j.com/docs/operations-manual/5/performance/memory-configuration/>
6. Neo4j 5 Docker introduction (trixie base from 5.26.20; multi-arch image) — <https://neo4j.com/docs/operations-manual/5/docker/introduction/>
7. Docker Hub official `neo4j` image (architectures `amd64`, `arm64v8`) — <https://hub.docker.com/_/neo4j/>
8. newsroom-grok B2 pin and projection notes — `newsroom/projection/neo4j/models.py`, `newsroom/projection/neo4j/_adapter.py`, `newsroom/projection/neo4j/qualification.py`, `docs/operations/neo4j-b2-qualification.md`, `pyproject.toml`
