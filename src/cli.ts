#!/usr/bin/env node
import { Command, InvalidArgumentError } from "commander";
import { normalizeDateTime } from "./datetime.js";
import {
  buildGroupStateCommand,
  buildListGroupsCommand,
  buildListTopicsCommand,
  buildResetCommand,
  resolveKafkaTool,
} from "./kafkaCli.js";
import { currentKubeContext, listKubeContexts, listNamespaces, listServices, startPortForward, type PortForwardHandle } from "./kube.js";
import { runBuffered, runInherited } from "./process.js";
import {
  askConfirm,
  askInput,
  askOptionalInput,
  askSearchCheckbox,
  askSearchSelect,
  askSelect,
  hasInteractiveTerminal,
} from "./prompts.js";
import { confirmExecute, printCommandPreview, printPlanSummary, warnIfGroupAppearsActive } from "./safety.js";
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
    const group = await collectGroup(options, canPrompt, connection.bootstrapServer, kafkaConsumerGroups);
    const topicSelection = await collectTopicSelection(options, canPrompt, fullWizard, connection.bootstrapServer);
    const normalizedDateTime = await collectDateTime(options, canPrompt);

    const plan: ResetPlan = {
      connection,
      group,
      topicSelection,
      normalizedDateTime,
      commandConfig: options.commandConfig,
      kafkaBin: options.kafkaBin,
      execute: options.execute,
      yes: options.yes,
    };

    await warnAboutGroupState(plan, kafkaConsumerGroups);
    printPlanSummary(plan);

    const dryRunCommand = buildResetCommand(plan, kafkaConsumerGroups, "dry-run");
    printCommandPreview("Dry-run command", dryRunCommand);

    const dryRunExitCode = await runInherited(dryRunCommand);
    if (dryRunExitCode !== 0) {
      throw new Error(`Dry-run failed with exit code ${dryRunExitCode}. No offsets were changed.`);
    }

    const shouldExecute = options.execute || (fullWizard && (await askConfirm("Apply this reset now?", false)));
    if (!shouldExecute) {
      console.log("Dry-run only. No offsets were changed.");
      return;
    }

    await confirmExecute(plan);

    const executeCommand = buildResetCommand(plan, kafkaConsumerGroups, "execute");
    printCommandPreview("Execute command", executeCommand);

    const executeExitCode = await runInherited(executeCommand);
    if (executeExitCode !== 0) {
      throw new Error(`Execute failed with exit code ${executeExitCode}.`);
    }
  } finally {
    await portForward?.stop();
  }
}

async function collectOptionalSettings(options: CliOptions): Promise<void> {
  if (!options.kafkaBin && (await askConfirm("Use a custom Kafka CLI bin directory?", false))) {
    options.kafkaBin = await askInput("Kafka bin directory");
  }

  if (!options.commandConfig && (await askConfirm("Use a Kafka client properties file?", false))) {
    options.commandConfig = await askInput("Path to client properties");
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

async function collectGroup(options: CliOptions, canPrompt: boolean, bootstrapServer: string, kafkaConsumerGroups: string): Promise<string> {
  if (options.group) {
    return options.group;
  }

  requirePrompt(canPrompt, "--group is required");

  const result = await runBuffered(buildListGroupsCommand(bootstrapServer, kafkaConsumerGroups, options.commandConfig));
  const groups = result.exitCode === 0 ? lines(result.stdout) : [];

  if (groups.length > 0) {
    return askSearchSelect("Consumer group", groups);
  }

  return askInput("Consumer group");
}

async function collectTopicSelection(
  options: CliOptions,
  canPrompt: boolean,
  fullWizard: boolean,
  bootstrapServer: string,
): Promise<TopicSelection> {
  const modes = [options.topics.length > 0, options.selectTopics, options.allTopics].filter(Boolean).length;

  if (modes > 1) {
    throw new Error("Use exactly one topic selection mode: --topic, --select-topics, or --all-topics");
  }

  if (modes === 0) {
    requirePrompt(canPrompt, "Use one topic selection mode: --topic, --select-topics, or --all-topics");
    const mode = await askSelect("Which topics should be reset?", ["Search and checkbox-select topics", "Type topic names", "All topics in group"]);
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
    options.datetime = await askInput("Backdated date/time or epoch timestamp");
  }

  if (/^\d+$/.test(options.datetime) && canPrompt) {
    const choice = await askSelect("How should the epoch timestamp be shown to Kafka?", [
      "Local timezone (Kafka CLI default)",
      "UTC (runs Kafka CLI with TZ=UTC)",
    ]);
    options.timezone = choice.startsWith("UTC") ? "utc" : "local";
  }

  return normalizeDateTime(options.datetime, options.timezone);
}

async function warnAboutGroupState(plan: ResetPlan, kafkaConsumerGroups: string): Promise<void> {
  const result = await runBuffered(buildGroupStateCommand(plan.connection.bootstrapServer, plan.group, kafkaConsumerGroups, plan.commandConfig));
  if (result.exitCode === 0) {
    warnIfGroupAppearsActive(result.stdout, plan.group);
  }
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
