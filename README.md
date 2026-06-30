# Kafka Reset Offsets

Interactive TypeScript CLI for safely resetting a Kafka consumer group's offsets to a backdated date, datetime, or epoch timestamp.

The tool wraps Kafka's official `kafka-consumer-groups --reset-offsets --to-datetime` command, but adds guardrails:

- Guided wizard when called without arguments.
- Direct Kafka or Kubernetes `kubectl port-forward` connection modes.
- Searchable Kubernetes context, namespace, and service selection.
- Searchable checkbox topic selector: type to filter, press `space` to toggle topics, press `enter` to confirm.
- Dry-run first, always.
- Final confirmation phrase before applying changes.
- Post-execute verification by comparing current offsets with the dry-run plan.

## Operator Requirements

- Kafka command line tools installed locally.
- `kafka-consumer-groups` or `kafka-consumer-groups.sh` available on `PATH`, or pass `--kafka-bin`.
- `kafka-topics` or `kafka-topics.sh` for topic discovery.
- `kubectl` when connecting through a Kubernetes port-forward.
- The target consumer group must be stopped before executing the reset.

Operators should not need to know pnpm or npm. The intended operator experience is a single command:

```bash
kafka-reset-offsets
```

pnpm is only the development tool used to build, test, and package the CLI.

## Distribution Options

For people who have never used pnpm or npm, prefer one of these distribution paths:

- **Homebrew formula:** best for macOS teams. Users run `brew install kafka-reset-offsets`, then `kafka-reset-offsets`.
- **Release artifact:** publish a built package or single executable in GitHub Releases. Users download it and run `kafka-reset-offsets`.
- **Internal package registry:** useful if the team already has a standard Node package flow, but not required for operators.

The current implementation builds a Node CLI. If you distribute the built JavaScript CLI directly, operators need Node.js 20 or newer. If you want zero Node.js requirement for operators, package it as a standalone binary in a release step.

## Developer Requirements

- Node.js 20 or newer.
- pnpm 11 or newer.

## Local Development

```bash
pnpm install
pnpm build
pnpm dev
```

Run tests and typecheck:

```bash
pnpm check
```

After building, run the packaged CLI:

```bash
node dist/cli.js --help
```

## Recommended Usage

Start the guided flow after installing the packaged command:

```bash
kafka-reset-offsets
```

The wizard asks for:

- Connection mode: direct bootstrap server or Kubernetes port-forward.
- Kafka CLI/client config if needed.
- Consumer group prefix and searchable checkbox group selection.
- Topics inferred from each selected consumer group, or manually selected when needed.
- Backdated date/time or timestamp, followed by an explicit local-vs-UTC timezone choice.
- Whether to apply after reviewing the dry-run output.

## Connection Modes

Use direct Kafka access when Kafka is reachable from your machine:

```bash
kafka-reset-offsets \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --datetime 2026-06-29
```

Use Kubernetes mode when Kafka is reachable through a service in a cluster:

```bash
kafka-reset-offsets \
  --kube-context example-dev \
  --namespace kafka \
  --kafka-service kafka-bootstrap \
  --group-prefix corex_ \
  --select-groups \
  --datetime 2026-06-29
```

The CLI starts a temporary `kubectl port-forward`, uses `127.0.0.1:<local-port>` as the Kafka bootstrap server, and cleans the port-forward up on exit.

The reset command runs locally. It does not `kubectl exec` into pods. Kubernetes only provides the network path to the Kafka service.

## Kubernetes Discovery

In the wizard, the CLI can discover options dynamically:

- Contexts via `kubectl config get-contexts -o name`.
- Namespaces via `kubectl get namespaces -o json`.
- Services via `kubectl -n <namespace> get services -o json`.

If Kubernetes permissions block discovery, the wizard falls back to manual entry.

## Client Properties

If `client.properties` exists in the directory where you run the CLI, the wizard uses it by default. It will only ask for a different client properties file if you choose to override it.

Pass `--command-config <file>` to use a specific file non-interactively.

## Timestamp Formats

Accepted values for `--datetime`, `-d`, or `--timestamp`:

- `2026-06-29`, normalized to `2026-06-29T00:00:00.000`
- `2026-06-29 13:45`, normalized to `2026-06-29T13:45:00.000`
- `2026-06-29T13:45:00`, normalized to `2026-06-29T13:45:00.000`
- `2026-06-29T13:45:00.123`, passed to Kafka in that format
- `1782737100`, treated as epoch seconds
- `1782737100123`, treated as epoch milliseconds

Epoch timestamps are converted to Kafka's `YYYY-MM-DDTHH:mm:ss.SSS` format before running the Kafka CLI. By default they use your local timezone. Use `--timezone utc` to convert the epoch and run the Kafka CLI with `TZ=UTC`.

```bash
kafka-reset-offsets \
  -b localhost:9092 \
  -g my-consumer-group \
  -t orders \
  --timestamp 1782737100123 \
  --timezone utc
```

## Multi-Consumer-Group Resets

Offset resets are scoped to consumer groups. In the guided flow, you can enter a consumer group prefix, then select multiple matching groups from a searchable checkbox list:

- Type to filter.
- Press `space` to select or unselect consumer groups.
- Press `enter` to confirm.

For each selected group, the CLI runs `kafka-consumer-groups --describe --group <group>` and infers the topic or topics consumed by that group. It then runs one dry-run reset command per consumer group.

If a selected group has no committed-offset rows yet, the CLI can fall back to the consumer group naming convention `groupPrefix-topicName`. The prefix is only used for consumer group filtering and is stripped before creating the reset command. For example, prefix `apf_cert-data_stg` and group `apf_cert-data_stg-endeavour_bonuses_user_profile_bonuses` map to source topic `endeavour_bonuses_user_profile_bonuses`.

If you leave the filtering prefix blank and select groups manually, the wizard asks for an optional prefix to strip for topic inference. Leaving that blank uses a fallback that drops the first two hyphen-separated group segments, so `source-target-topic` maps to `topic`.

Example:

```bash
kafka-reset-offsets \
  -b localhost:9092 \
  --group-prefix corex_ \
  --select-groups \
  -d 2026-06-29
```

## Manual Topic Resets

Interactive topic selection uses a searchable checkbox list:

- Type to filter.
- Press `space` to select or unselect topics.
- Press `enter` to confirm.

Internally, the selected topic array is translated into Kafka CLI-compatible repeated `--topic` arguments. Dry-run and execute use the same selected topic array to avoid drift.

## Execution Safety

By default, commands are dry-run only:

```bash
kafka-reset-offsets \
  -b localhost:9092 \
  -g my-consumer-group \
  -t orders \
  -d 2026-06-29
```

To apply the reset from flags, pass `--execute`. The CLI still runs a dry-run first and asks for confirmation:

```bash
kafka-reset-offsets \
  -b localhost:9092 \
  -g my-consumer-group \
  -t orders \
  -d 2026-06-29T13:45:00.000 \
  --execute
```

For controlled automation, add `--no-interactive --execute --yes`. Use this only when the inputs are already reviewed:

```bash
kafka-reset-offsets \
  --no-interactive \
  -b localhost:9092 \
  -g my-consumer-group \
  -t orders \
  -d 2026-06-29 \
  --execute \
  --yes
```

Kafka resets each partition to the earliest offset whose record timestamp is greater than or equal to the provided datetime.

After execution, the CLI runs `kafka-consumer-groups --describe` for each selected group and compares `CURRENT-OFFSET` with the offsets from the dry-run `NEW-OFFSET` plan. If the output cannot be parsed, it still prints the describe output so the operator can verify manually.

## Legacy Bash Script

`reset_offsets_from_date.sh` is kept as a legacy fallback for now. The packaged TypeScript CLI is the primary implementation going forward.
