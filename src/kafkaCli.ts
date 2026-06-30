import { access } from "node:fs/promises";
import { constants } from "node:fs";
import { delimiter, join } from "node:path";
import type { ResetAction, ResetPlan, TimezoneMode, TopicSelection } from "./types.js";

export interface CommandSpec {
  command: string;
  args: string[];
  env?: NodeJS.ProcessEnv;
}

export async function resolveKafkaTool(toolName: string, kafkaBin?: string): Promise<string> {
  const names = [toolName, `${toolName}.sh`];

  if (kafkaBin) {
    for (const name of names) {
      const candidate = join(kafkaBin, name);
      if (await isExecutable(candidate)) {
        return candidate;
      }
    }

    throw new Error(`Could not find ${toolName} or ${toolName}.sh in ${kafkaBin}`);
  }

  const pathDirectories = (process.env.PATH ?? "").split(delimiter).filter(Boolean);
  for (const directory of pathDirectories) {
    for (const name of names) {
      const candidate = join(directory, name);
      if (await isExecutable(candidate)) {
        return candidate;
      }
    }
  }

  throw new Error(`Could not find ${toolName} or ${toolName}.sh on PATH. Install Kafka CLI tools or pass --kafka-bin.`);
}

export function buildResetCommand(plan: ResetPlan, kafkaConsumerGroups: string, action: ResetAction): CommandSpec {
  const args = [
    "--bootstrap-server",
    plan.connection.bootstrapServer,
    "--group",
    plan.group,
    "--reset-offsets",
    "--to-datetime",
    plan.normalizedDateTime.kafkaDateTime,
  ];

  addCommandConfig(args, plan.commandConfig);
  addTopicSelection(args, plan.topicSelection);
  args.push(action === "execute" ? "--execute" : "--dry-run");

  return withTimezone({
    command: kafkaConsumerGroups,
    args,
  }, plan.normalizedDateTime.timezone);
}

export function buildListGroupsCommand(bootstrapServer: string, kafkaConsumerGroups: string, commandConfig?: string): CommandSpec {
  const args = ["--bootstrap-server", bootstrapServer, "--list"];
  addCommandConfig(args, commandConfig);

  return {
    command: kafkaConsumerGroups,
    args,
  };
}

export function buildGroupStateCommand(bootstrapServer: string, group: string, kafkaConsumerGroups: string, commandConfig?: string): CommandSpec {
  const args = ["--bootstrap-server", bootstrapServer, "--describe", "--group", group, "--state"];
  addCommandConfig(args, commandConfig);

  return {
    command: kafkaConsumerGroups,
    args,
  };
}

export function buildListTopicsCommand(bootstrapServer: string, kafkaTopics: string, commandConfig?: string): CommandSpec {
  const args = ["--bootstrap-server", bootstrapServer, "--list"];
  addCommandConfig(args, commandConfig);

  return {
    command: kafkaTopics,
    args,
  };
}

export function addTopicSelection(args: string[], topicSelection: TopicSelection): void {
  if (topicSelection.mode === "all") {
    args.push("--all-topics");
    return;
  }

  for (const topic of topicSelection.topics) {
    args.push("--topic", topic);
  }
}

export function formatCommand(spec: CommandSpec): string {
  const envPrefix = spec.env?.TZ ? `TZ=${quote(spec.env.TZ)} ` : "";
  return `${envPrefix}${[spec.command, ...spec.args].map(quote).join(" ")}`;
}

function addCommandConfig(args: string[], commandConfig?: string): void {
  if (commandConfig) {
    args.push("--command-config", commandConfig);
  }
}

function withTimezone(spec: CommandSpec, timezone: TimezoneMode): CommandSpec {
  if (timezone !== "utc") {
    return spec;
  }

  return {
    ...spec,
    env: {
      ...process.env,
      TZ: "UTC",
    },
  };
}

async function isExecutable(path: string): Promise<boolean> {
  try {
    await access(path, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function quote(value: string): string {
  if (/^[A-Za-z0-9_./:=@%+-]+$/.test(value)) {
    return value;
  }

  return `'${value.replaceAll("'", "'\\''")}'`;
}
