#!/usr/bin/env node
import { Command, InvalidArgumentError } from "commander";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { normalizeDateTime } from "./datetime.js";
import {
  extractCurrentOffsetsFromGroupDescription,
  extractPlannedOffsetsFromResetOutput,
  extractTopicsFromGroupDescription,
  inferTopicFromConsumerGroupName,
  type PartitionOffset,
} from "./groupDescription.js";
import {
  buildDescribeGroupCommand,
  buildGroupStateCommand,
  buildListGroupsCommand,
  buildListTopicsCommand,
  buildResetCommand,
  resolveKafkaTool,
} from "./kafkaCli.js";
import { currentKubeContext, listKubeContexts, listNamespaces, listServices, startPortForward, type PortForwardHandle } from "./kube.js";
import { runBuffered } from "./process.js";
import {
  askConfirm,
  askInput,
  askOptionalInput,
  askSearchCheckbox,
  askSearchSelect,
  askSelect,
  hasInteractiveTerminal,
} from "./prompts.js";
import { confirmBatchExecute, confirmExecute, printBatchPlanSummary, printCommandPreview, printPlanSummary, warnIfGroupAppearsActive } from "./safety.js";
import type { CliOptions, ConnectionConfig, ResetPlan, TimezoneMode, TopicSelection } from "./types.js";

const program = new Command();

program
  .name("kafka-reset-offsets")
  .description("Safely reset Kafka consumer group offsets to a backdated date, datetime, or epoch timestamp.")
  .option("-b, --bootstrap-server <host:port>", "Kafka bootstrap server")
  .option("--kube-context <context>", "Kubernetes context for port-forwarding to Kafka")
  .option("-n, --namespace <namespace>", "Kubernetes namespace containing the Kafka service")
  .option("--kafka-service <service>", "Kafka service name or resource, e.g. kafka-bootstrap or svc/kafka-bootstrap")
  .option("--kafka-port <port>", "Kafka service port", parsePort, 9092)
  .option("--local-port <port>", "Local port for the port-forward", parsePort, 19092)
  .option("-g, --group <group>", "Consumer group id to reset")
  .option("--group-prefix <prefix>", "Filter consumer groups by prefix before multi-select")
  .option("--select-groups", "Search and checkbox-select one or more consumer groups")
  .option("-t, --topic <topic>", "Topic to reset. Can be specified multiple times", collect, [])
  .option("--select-topics", "Search and checkbox-select one or more topics")
  .option("--all-topics", "Reset offsets for all topics in the consumer group")
  .option("-d, --datetime <value>", "Target date/time or epoch timestamp")
  .option("--timestamp <value>", "Alias for --datetime")
  .option("--timezone <mode>", "Timezone for epoch timestamps: local or utc", parseTimezone, "local")
  .option("--command-config <file>", "Kafka client properties file")
  .option("--kafka-bin <dir>", "Directory containing Kafka CLI scripts")
  .option("--execute", "Apply the reset after dry-run and confirmation")
  .option("--yes", "Skip execute confirmation. Requires --execute")
  .option("--interactive", "Force prompts for missing values")
  .option("--no-interactive", "Fail instead of prompting for missing values")
  .showHelpAfterError();

program.parse();

const rawOptions = program.opts<{
  bootstrapServer?: string;
  kubeContext?: string;
  namespace?: string;
  kafkaService?: string;
  kafkaPort: number;
  localPort: number;
  group?: string;
  groupPrefix?: string;
  selectGroups?: boolean;
  topic: string[];
  selectTopics?: boolean;
  allTopics?: boolean;
  datetime?: string;
  timestamp?: string;
  timezone: TimezoneMode;
  commandConfig?: string;
  kafkaBin?: string;
  execute?: boolean;
  yes?: boolean;
  interactive?: boolean;
  noInteractive?: boolean;
}>();

const originalArgCount = process.argv.slice(2).length;

main({
  bootstrapServer: rawOptions.bootstrapServer,
  kubeContext: rawOptions.kubeContext,
  namespace: rawOptions.namespace,
  kafkaService: rawOptions.kafkaService,
  kafkaPort: rawOptions.kafkaPort,
  localPort: rawOptions.localPort,
  group: rawOptions.group,
  groupPrefix: rawOptions.groupPrefix,
  selectGroups: Boolean(rawOptions.selectGroups),
  topics: rawOptions.topic,
  selectTopics: Boolean(rawOptions.selectTopics),
  allTopics: Boolean(rawOptions.allTopics),
  datetime: rawOptions.datetime ?? rawOptions.timestamp,
  commandConfig: rawOptions.commandConfig,
  kafkaBin: rawOptions.kafkaBin,
  execute: Boolean(rawOptions.execute),
  yes: Boolean(rawOptions.yes),
  timezone: rawOptions.timezone,
  interactive: Boolean(rawOptions.interactive),
  noInteractive: Boolean(rawOptions.noInteractive),
}).catch((error: unknown) => {
  console.error(`Error: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});

async function main(options: CliOptions): Promise<void> {
  const fullWizard = options.interactive || originalArgCount === 0;
  const canPrompt = !options.noInteractive && (options.interactive || originalArgCount === 0 || hasInteractiveTerminal());

  if (options.yes && !options.execute) {
    throw new Error("--yes only makes sense with --execute");
  }

  if (options.interactive && options.noInteractive) {
    throw new Error("Use either --interactive or --no-interactive, not both");
  }

  if (fullWizard) {
    console.log("");
    console.log("Kafka offset reset wizard");
    console.log("This tool always dry-runs first, then asks again before executing.");
    console.log("");
  }

  if (fullWizard) {
    await collectOptionalSettings(options);
  }

  validateNonInteractiveMinimum(options, canPrompt);

  let portForward: PortForwardHandle | undefined;

  try {
    const connection = await collectConnection(options, canPrompt, fullWizard);

    if (connection.mode === "kubernetes") {
      portForward = await startPortForward({
        kubeContext: connection.kubeContext,
        namespace: connection.namespace,
        service: connection.kafkaService,
        kafkaPort: connection.kafkaPort,
        localPort: connection.localPort,
      });
      connection.bootstrapServer = portForward.bootstrapServer;
      console.log(`Connected to Kafka through Kubernetes port-forward at ${connection.bootstrapServer}.`);
    }

    const kafkaConsumerGroups = await resolveKafkaTool("kafka-consumer-groups", options.kafkaBin);
    const groups = await collectGroups(options, canPrompt, connection.bootstrapServer, kafkaConsumerGroups);
    const sharedTopicSelection = await collectTopicSelection(options, canPrompt, fullWizard, connection.bootstrapServer, groups.length);
    const normalizedDateTime = await collectDateTime(options, canPrompt);

    const plans = await buildResetPlans({
      connection,
      groups,
      sharedTopicSelection,
      normalizedDateTime,
      options,
      kafkaConsumerGroups,
    });

    for (const [index, plan] of plans.entries()) {
      console.log(`Checking consumer group state ${index + 1}/${plans.length}: ${plan.group}`);
      await warnAboutGroupState(plan, kafkaConsumerGroups);
    }
    printBatchPlanSummary(plans);

    const plannedOffsetsByGroup = new Map<string, PartitionOffset[]>();
    const runSummary = createRunSummary(plans);

    for (const [index, plan] of plans.entries()) {
      console.log(`Running dry-run ${index + 1}/${plans.length}: ${plan.group}`);
      const dryRunCommand = buildResetCommand(plan, kafkaConsumerGroups, "dry-run");
      printCommandPreview(`Dry-run command for ${plan.group}`, dryRunCommand);

      const dryRunResult = await runBuffered(dryRunCommand);
      printCommandOutput(dryRunResult.stdout, dryRunResult.stderr);
      const plannedOffsets = extractPlannedOffsetsFromResetOutput(dryRunResult.stdout);
      updateGroupSummary(runSummary, plan.group, {
        dryRun: "passed",
        plannedPartitions: plannedOffsets.length,
        warnings: extractWarningLines(dryRunResult.stdout, dryRunResult.stderr),
      });
      if (dryRunResult.exitCode !== 0) {
        updateGroupSummary(runSummary, plan.group, { dryRun: "failed" });
        printRunSummary(runSummary);
        throw new Error(`Dry-run failed for group '${plan.group}' with exit code ${dryRunResult.exitCode}. No offsets were changed.`);
      }

      plannedOffsetsByGroup.set(plan.group, plannedOffsets);
    }

    const shouldExecute = options.execute || (fullWizard && (await askConfirm("Apply this reset now?", false)));
    if (!shouldExecute) {
      console.log("Dry-run only. No offsets were changed.");
      printRunSummary(runSummary);
      return;
    }

    if (plans.length === 1) {
      await confirmExecute(plans[0]!);
    } else {
      await confirmBatchExecute(plans);
    }

    for (const [index, plan] of plans.entries()) {
      console.log(`Executing reset ${index + 1}/${plans.length}: ${plan.group}`);
      const executeCommand = buildResetCommand(plan, kafkaConsumerGroups, "execute");
      printCommandPreview(`Execute command for ${plan.group}`, executeCommand);

      const executeResult = await runBuffered(executeCommand);
      printCommandOutput(executeResult.stdout, executeResult.stderr);
      updateGroupSummary(runSummary, plan.group, {
        executed: executeResult.exitCode === 0 ? "passed" : "failed",
        warnings: extractWarningLines(executeResult.stdout, executeResult.stderr),
      });
      if (executeResult.exitCode !== 0) {
        printRunSummary(runSummary);
        throw new Error(`Execute failed for group '${plan.group}' with exit code ${executeResult.exitCode}.`);
      }

      console.log(`Verifying reset ${index + 1}/${plans.length}: ${plan.group}`);
      const verification = await verifyReset(plan, kafkaConsumerGroups, plannedOffsetsByGroup.get(plan.group) ?? []);
      updateGroupSummary(runSummary, plan.group, { verification });
    }

    printRunSummary(runSummary);
  } finally {
    await portForward?.stop();
  }
}

async function collectOptionalSettings(options: CliOptions): Promise<void> {
  if (!options.kafkaBin && (await askConfirm("Use a custom Kafka CLI bin directory?", false))) {
    options.kafkaBin = await askInput("Kafka bin directory");
  }

  if (!options.commandConfig) {
    const defaultClientProperties = resolve(process.cwd(), "client.properties");
    if (existsSync(defaultClientProperties)) {
      console.log(`Using Kafka client properties file: ${defaultClientProperties}`);
      if (await askConfirm("Use a different Kafka client properties file?", false)) {
        options.commandConfig = await askInput("Path to client properties", defaultClientProperties);
      } else {
        options.commandConfig = defaultClientProperties;
      }
    } else if (await askConfirm("Use a Kafka client properties file?", false)) {
      options.commandConfig = await askInput("Path to client properties");
    }
  }
}

async function collectConnection(options: CliOptions, canPrompt: boolean, fullWizard: boolean): Promise<ConnectionConfig> {
  if (options.bootstrapServer && options.kafkaService) {
    throw new Error("Use either --bootstrap-server or Kubernetes connection options, not both");
  }

  if (!options.bootstrapServer && !options.kafkaService) {
    requirePrompt(canPrompt, "Provide --bootstrap-server or --kafka-service with --namespace");
    const mode = await askSelect("How should this connect to Kafka?", ["Direct bootstrap server", "Kubernetes port-forward"]);
    if (mode === "Direct bootstrap server") {
      options.bootstrapServer = await askInput("Kafka bootstrap server", "localhost:9092");
    } else {
      options.kafkaService = "__prompt__";
    }
  }

  if (options.bootstrapServer) {
    return {
      mode: "direct",
      bootstrapServer: options.bootstrapServer,
    };
  }

  if (!options.kafkaService) {
    if (options.kubeContext || options.namespace) {
      requirePrompt(canPrompt, "--kube-context and --namespace require --kafka-service");
      options.kafkaService = "__prompt__";
    } else {
      throw new Error("Provide --bootstrap-server or --kafka-service with --namespace");
    }
  }

  if (!options.kubeContext && fullWizard) {
    const contexts = await listKubeContexts();
    const current = await currentKubeContext();
    if (contexts.length > 0) {
      options.kubeContext = await askSearchSelect("Kubernetes context", current ? prioritize(contexts, current) : contexts);
    } else {
      options.kubeContext = await askOptionalInput("Kubernetes context, blank for current context");
    }
  }

  if (!options.namespace) {
    requirePrompt(canPrompt, "--namespace is required when using --kafka-service");
    const namespaces = await listNamespaces(options.kubeContext);
    options.namespace = await askSearchSelect("Kubernetes namespace", namespaces, "Kubernetes namespace");
  }

  if (options.kafkaService === "__prompt__") {
    const services = await listServices(options.namespace, options.kubeContext);
    const selectedService = await askSearchSelect("Kafka service", services, "Kafka service");
    options.kafkaService = selectedService;
  }

  if (fullWizard) {
    options.kafkaPort = Number(await askInput("Kafka service port", String(options.kafkaPort)));
    options.localPort = Number(await askInput("Local port for port-forward", String(options.localPort)));
  }

  return {
    mode: "kubernetes",
    bootstrapServer: `127.0.0.1:${options.localPort}`,
    kubeContext: options.kubeContext,
    namespace: options.namespace,
    kafkaService: options.kafkaService,
    kafkaPort: options.kafkaPort,
    localPort: options.localPort,
  };
}

async function collectGroups(options: CliOptions, canPrompt: boolean, bootstrapServer: string, kafkaConsumerGroups: string): Promise<string[]> {
  if (options.group) {
    return [options.group];
  }

  requirePrompt(canPrompt, "--group is required");

  const result = await runBuffered(buildListGroupsCommand(bootstrapServer, kafkaConsumerGroups, options.commandConfig));
  const groups = result.exitCode === 0 ? kafkaResultLines(result.stdout) : [];

  if (groups.length > 0) {
    let filteredGroups = groups;

    if (options.groupPrefix === undefined) {
      options.groupPrefix = await askOptionalInput("Consumer group prefix, blank to show all groups");
    }

    if (options.groupPrefix) {
      filteredGroups = groups.filter((group) => group.startsWith(options.groupPrefix!));
      if (filteredGroups.length === 0) {
        throw new Error(`No consumer groups found with prefix '${options.groupPrefix}'`);
      }
    }

    const shouldSelectMultiple = options.selectGroups || filteredGroups.length > 1;
    if (shouldSelectMultiple) {
      const selectedGroups = await askSearchCheckbox("Consumer groups", filteredGroups);
      if (selectedGroups.length === 0) {
        throw new Error("No consumer groups selected");
      }
      if (!options.groupPrefix) {
        options.groupPrefix = await askOptionalInput("Consumer group prefix to strip before deriving source topics, blank to use source-target-topic fallback");
      }
      return selectedGroups;
    }

    return [filteredGroups[0]!];
  }

  const errorOutput = [result.stderr, result.stdout].map((value) => value.trim()).filter(Boolean).join("\n");
  if (errorOutput) {
    console.warn(`Could not list consumer groups:\n${errorOutput}`);
  }

  return [await askInput("Consumer group")];
}

async function collectTopicSelection(
  options: CliOptions,
  canPrompt: boolean,
  fullWizard: boolean,
  bootstrapServer: string,
  groupCount: number,
): Promise<TopicSelection> {
  const modes = [options.topics.length > 0, options.selectTopics, options.allTopics].filter(Boolean).length;

  if (modes > 1) {
    throw new Error("Use exactly one topic selection mode: --topic, --select-topics, or --all-topics");
  }

  if (modes === 0 && groupCount > 1) {
    return { mode: "topics", topics: [] };
  }

  if (modes === 0) {
    requirePrompt(canPrompt, "Use one topic selection mode: --topic, --select-topics, or --all-topics");
    const mode = await askSelect("Which topics should be reset?", [
      "Infer topics from selected consumer group",
      "Search and checkbox-select topics",
      "Type topic names",
      "All topics in group",
    ]);
    if (mode === "Infer topics from selected consumer group") {
      return { mode: "topics", topics: [] };
    }
    options.selectTopics = mode === "Search and checkbox-select topics";
    options.allTopics = mode === "All topics in group";

    if (mode === "Type topic names") {
      options.topics = parseTopicList(await askInput("Topic names, comma-separated"));
    }
  }

  if (options.allTopics) {
    if (canPrompt && fullWizard) {
      const confirmed = await askConfirm("This targets all topics in the group. Continue?", false);
      if (!confirmed) {
        throw new Error("All-topics reset was not confirmed");
      }
    }
    return { mode: "all" };
  }

  if (options.selectTopics) {
    const kafkaTopics = await resolveKafkaTool("kafka-topics", options.kafkaBin);
    const result = await runBuffered(buildListTopicsCommand(bootstrapServer, kafkaTopics, options.commandConfig));
    if (result.exitCode !== 0) {
      throw new Error(`Could not list Kafka topics: ${result.stderr.trim() || "unknown error"}`);
    }

    const selectedTopics = await askSearchCheckbox("Kafka topics", lines(result.stdout));
    if (selectedTopics.length === 0) {
      throw new Error("No topics selected");
    }

    return {
      mode: "topics",
      topics: selectedTopics,
    };
  }

  if (options.topics.length === 0) {
    options.topics = parseTopicList(await askInput("Topic names, comma-separated"));
  }

  if (options.topics.length === 0) {
    throw new Error("At least one topic is required");
  }

  return {
    mode: "topics",
    topics: options.topics,
  };
}

async function collectDateTime(options: CliOptions, canPrompt: boolean) {
  if (!options.datetime) {
    requirePrompt(canPrompt, "--datetime is required");
    options.datetime = await askInput("Backdated date/time or epoch timestamp, e.g. 2026-06-30 15:15");
  }

  if (canPrompt) {
    const choice = await askSelect("How should this date/time be interpreted by Kafka?", [
      "Local timezone (Kafka CLI default)",
      "UTC (runs Kafka CLI with TZ=UTC)",
    ]);
    options.timezone = choice.startsWith("UTC") ? "utc" : "local";
  }

  return normalizeDateTime(options.datetime, options.timezone);
}

async function warnAboutGroupState(plan: ResetPlan, kafkaConsumerGroups: string): Promise<void> {
  const result = await runBuffered(buildGroupStateCommand(plan.connection.bootstrapServer, plan.group, kafkaConsumerGroups, plan.commandConfig), 15_000);
  if (result.exitCode === 0) {
    warnIfGroupAppearsActive(result.stdout, plan.group);
  } else if (result.exitCode === 124) {
    console.warn(`Warning: timed out checking state for '${plan.group}'. Continuing to dry-run.`);
  }
}

type VerificationStatus = "passed" | "failed" | "unknown";

interface VerificationResult {
  status: VerificationStatus;
  verifiedPartitions: number;
  mismatchedPartitions: number;
}

interface GroupRunSummary {
  group: string;
  topics: string[];
  dryRun: "pending" | "passed" | "failed";
  executed: "skipped" | "passed" | "failed";
  verification: VerificationResult;
  plannedPartitions: number;
  warnings: string[];
}

interface RunSummary {
  groups: GroupRunSummary[];
}

async function verifyReset(plan: ResetPlan, kafkaConsumerGroups: string, plannedOffsets: PartitionOffset[]): Promise<VerificationResult> {
  console.log(`Verification for ${plan.group}:`);

  const describeCommand = buildDescribeGroupCommand(plan.connection.bootstrapServer, plan.group, kafkaConsumerGroups, plan.commandConfig);
  const describeResult = await runBuffered(describeCommand);
  printCommandOutput(describeResult.stdout, describeResult.stderr);

  if (describeResult.exitCode !== 0) {
    console.warn(`  Could not verify group '${plan.group}' because describe failed with exit code ${describeResult.exitCode}.`);
    return { status: "unknown", verifiedPartitions: 0, mismatchedPartitions: 0 };
  }

  const currentOffsets = extractCurrentOffsetsFromGroupDescription(describeResult.stdout);
  if (plannedOffsets.length === 0) {
    console.warn("  Could not parse planned dry-run offsets; review the describe output above.");
    return { status: "unknown", verifiedPartitions: 0, mismatchedPartitions: 0 };
  }

  const mismatches = compareOffsets(plannedOffsets, currentOffsets);
  if (mismatches.length === 0) {
    console.log(`  Verified ${plannedOffsets.length} partition offset(s) match the dry-run plan.`);
    return { status: "passed", verifiedPartitions: plannedOffsets.length, mismatchedPartitions: 0 };
  }

  console.warn(`  Verification found ${mismatches.length} mismatched partition offset(s):`);
  for (const mismatch of mismatches) {
    console.warn(`    ${mismatch.topic}[${mismatch.partition}] planned=${mismatch.plannedOffset} current=${mismatch.currentOffset ?? "missing"}`);
  }

  return {
    status: "failed",
    verifiedPartitions: plannedOffsets.length - mismatches.length,
    mismatchedPartitions: mismatches.length,
  };
}

function compareOffsets(plannedOffsets: PartitionOffset[], currentOffsets: PartitionOffset[]): Array<{
  topic: string;
  partition: number;
  plannedOffset: string;
  currentOffset?: string;
}> {
  const currentByKey = new Map(currentOffsets.map((offset) => [offsetKey(offset), offset]));

  return plannedOffsets
    .map((plannedOffset) => {
      const currentOffset = currentByKey.get(offsetKey(plannedOffset));
      if (currentOffset?.offset === plannedOffset.offset) {
        return undefined;
      }

      return {
        topic: plannedOffset.topic,
        partition: plannedOffset.partition,
        plannedOffset: plannedOffset.offset,
        currentOffset: currentOffset?.offset,
      };
    })
    .filter((mismatch): mismatch is NonNullable<typeof mismatch> => Boolean(mismatch));
}

function offsetKey(offset: PartitionOffset): string {
  return `${offset.group}\0${offset.topic}\0${offset.partition}`;
}

function printCommandOutput(stdout: string, stderr: string): void {
  if (stdout) {
    process.stdout.write(stdout.endsWith("\n") ? stdout : `${stdout}\n`);
  }

  if (stderr) {
    process.stderr.write(stderr.endsWith("\n") ? stderr : `${stderr}\n`);
  }
}

function createRunSummary(plans: ResetPlan[]): RunSummary {
  return {
    groups: plans.map((plan) => ({
      group: plan.group,
      topics: plan.topicSelection.mode === "all" ? ["all topics"] : plan.topicSelection.topics,
      dryRun: "pending",
      executed: "skipped",
      verification: { status: "unknown", verifiedPartitions: 0, mismatchedPartitions: 0 },
      plannedPartitions: 0,
      warnings: [],
    })),
  };
}

function updateGroupSummary(
  summary: RunSummary,
  group: string,
  updates: Partial<Omit<GroupRunSummary, "group" | "topics" | "warnings">> & { warnings?: string[] },
): void {
  const groupSummary = summary.groups.find((candidate) => candidate.group === group);
  if (!groupSummary) {
    return;
  }

  if (updates.dryRun) {
    groupSummary.dryRun = updates.dryRun;
  }
  if (updates.executed) {
    groupSummary.executed = updates.executed;
  }
  if (updates.verification) {
    groupSummary.verification = updates.verification;
  }
  if (updates.plannedPartitions !== undefined) {
    groupSummary.plannedPartitions = updates.plannedPartitions;
  }
  if (updates.warnings) {
    groupSummary.warnings.push(...updates.warnings);
  }
}

function printRunSummary(summary: RunSummary): void {
  const dryRunPassed = summary.groups.filter((group) => group.dryRun === "passed").length;
  const executed = summary.groups.filter((group) => group.executed === "passed").length;
  const verified = summary.groups.filter((group) => group.verification.status === "passed").length;
  const failed = summary.groups.filter(
    (group) => group.dryRun === "failed" || group.executed === "failed" || group.verification.status === "failed",
  ).length;
  const warningCount = summary.groups.reduce((count, group) => count + group.warnings.length, 0);

  console.log("");
  console.log("Run summary:");
  console.log(`  Groups selected:       ${summary.groups.length}`);
  console.log(`  Dry-runs passed:       ${dryRunPassed}/${summary.groups.length}`);
  console.log(`  Executes passed:       ${executed}/${summary.groups.length}`);
  console.log(`  Verifications passed:  ${verified}/${summary.groups.length}`);
  console.log(`  Failures:              ${failed}`);
  console.log(`  Kafka warnings:        ${warningCount}`);
  console.log("");

  for (const group of summary.groups) {
    const verification =
      group.verification.status === "passed"
        ? `verified ${group.verification.verifiedPartitions} partition(s)`
        : group.verification.status;
    const topics = group.topics.join(", ");
    console.log(`  ${group.group}`);
    console.log(`    topics: ${topics}`);
    console.log(`    dry-run: ${group.dryRun}; execute: ${group.executed}; verification: ${verification}`);
    if (group.warnings.length > 0) {
      console.log(`    warnings: ${group.warnings.length}`);
      for (const warning of unique(group.warnings).slice(0, 3)) {
        console.log(`      - ${warning}`);
      }
      if (unique(group.warnings).length > 3) {
        console.log(`      - ...and ${unique(group.warnings).length - 3} more`);
      }
    }
  }
  console.log("");
}

function extractWarningLines(stdout: string, stderr: string): string[] {
  return `${stdout}\n${stderr}`
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => /^(warn|warning):/i.test(line));
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

interface BuildResetPlansArgs {
  connection: ConnectionConfig;
  groups: string[];
  sharedTopicSelection: TopicSelection;
  normalizedDateTime: ReturnType<typeof normalizeDateTime>;
  options: CliOptions;
  kafkaConsumerGroups: string;
}

async function buildResetPlans(args: BuildResetPlansArgs): Promise<ResetPlan[]> {
  const plans: ResetPlan[] = [];

  for (const [index, group] of args.groups.entries()) {
    console.log(`Preparing reset plan ${index + 1}/${args.groups.length}: ${group}`);
    const topicSelection =
      args.sharedTopicSelection.mode === "topics" && args.sharedTopicSelection.topics.length === 0
        ? await inferTopicSelectionForGroup(args.connection.bootstrapServer, group, args.kafkaConsumerGroups, args.options.commandConfig, args.options.groupPrefix)
        : args.sharedTopicSelection;

    plans.push({
      connection: args.connection,
      group,
      topicSelection,
      normalizedDateTime: args.normalizedDateTime,
      commandConfig: args.options.commandConfig,
      kafkaBin: args.options.kafkaBin,
      execute: args.options.execute,
      yes: args.options.yes,
    });
  }

  return plans;
}

async function inferTopicSelectionForGroup(
  bootstrapServer: string,
  group: string,
  kafkaConsumerGroups: string,
  commandConfig?: string,
  groupPrefix?: string,
): Promise<TopicSelection> {
  console.log(`  Inferring topic(s) for ${group}...`);
  const result = await runBuffered(buildDescribeGroupCommand(bootstrapServer, group, kafkaConsumerGroups, commandConfig));
  if (result.exitCode !== 0) {
    const errorOutput = [result.stderr, result.stdout].map((value) => value.trim()).filter(Boolean).join("\n");
    throw new Error(`Could not infer topics for consumer group '${group}': ${errorOutput || "unknown error"}`);
  }

  const topics = extractTopicsFromGroupDescription(result.stdout);
  if (topics.length === 0) {
    const inferredTopic = inferTopicFromConsumerGroupName(group, groupPrefix);
    if (inferredTopic) {
      console.warn(`Could not infer topics from committed offsets for '${group}'. Falling back to source topic '${inferredTopic}' from the consumer group name.`);
      return {
        mode: "topics",
        topics: [inferredTopic],
      };
    }

    throw new Error(`Could not infer any topics for consumer group '${group}'. Specify --topic manually.`);
  }

  return {
    mode: "topics",
    topics,
  };
}

function parseTopicList(value: string): string[] {
  return value
    .split(",")
    .map((topic) => topic.trim())
    .filter(Boolean);
}

function lines(value: string): string[] {
  return value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

function kafkaResultLines(value: string): string[] {
  return lines(value).filter((line) => !/^(error|exception|warning):/i.test(line));
}

function requirePrompt(canPrompt: boolean, message: string): void {
  if (!canPrompt) {
    throw new Error(message);
  }
}

function validateNonInteractiveMinimum(options: CliOptions, canPrompt: boolean): void {
  if (canPrompt) {
    return;
  }

  if (!options.group) {
    throw new Error("--group is required");
  }

  if (!options.datetime) {
    throw new Error("--datetime is required");
  }

  const topicModes = [options.topics.length > 0, options.selectTopics, options.allTopics].filter(Boolean).length;
  if (topicModes !== 1) {
    throw new Error("Use exactly one topic selection mode: --topic, --select-topics, or --all-topics");
  }
}

function prioritize(values: string[], preferred: string): string[] {
  return [preferred, ...values.filter((value) => value !== preferred)];
}

function collect(value: string, previous: string[]): string[] {
  previous.push(value);
  return previous;
}

function parsePort(value: string): number {
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new InvalidArgumentError("port must be an integer between 1 and 65535");
  }
  return port;
}

function parseTimezone(value: string): TimezoneMode {
  if (value !== "local" && value !== "utc") {
    throw new InvalidArgumentError("timezone must be 'local' or 'utc'");
  }
  return value;
}
