import type { ResetPlan } from "./types.js";
import type { CommandSpec } from "./kafkaCli.js";
import { formatCommand } from "./kafkaCli.js";
import { askConfirm, askInput } from "./prompts.js";

export function printPlanSummary(plan: ResetPlan): void {
  console.log("");
  console.log("Review the reset target:");

  if (plan.connection.mode === "kubernetes") {
    console.log("  Connection: Kubernetes port-forward");
    console.log(`  Context:    ${plan.connection.kubeContext ?? "current kubectl context"}`);
    console.log(`  Namespace:  ${plan.connection.namespace}`);
    console.log(`  Service:    ${plan.connection.kafkaService}`);
    console.log(`  Bootstrap:  ${plan.connection.bootstrapServer}`);
  } else {
    console.log("  Connection: Direct Kafka bootstrap server");
    console.log(`  Bootstrap:  ${plan.connection.bootstrapServer}`);
  }

  console.log(`  Group:      ${plan.group}`);

  if (plan.topicSelection.mode === "all") {
    console.log("  Topics:     all topics in group");
  } else {
    console.log(`  Topics:     ${plan.topicSelection.topics.length} selected`);
    for (const topic of plan.topicSelection.topics.slice(0, 20)) {
      console.log(`              ${topic}`);
    }
    if (plan.topicSelection.topics.length > 20) {
      console.log(`              ...and ${plan.topicSelection.topics.length - 20} more`);
    }
  }

  console.log(`  Input time: ${plan.normalizedDateTime.input}`);
  console.log(`  Kafka time: ${plan.normalizedDateTime.kafkaDateTime}`);
  console.log(`  Timezone:   ${plan.normalizedDateTime.timezoneLabel}`);
  console.log(`  UTC time:   ${plan.normalizedDateTime.utcPreview}`);
  console.log(`  Time note:  ${plan.normalizedDateTime.note}`);
  console.log("");
}

export function printBatchPlanSummary(plans: ResetPlan[]): void {
  if (plans.length === 1) {
    printPlanSummary(plans[0]!);
    return;
  }

  const firstPlan = plans[0]!;

  console.log("");
  console.log("Review the reset targets:");

  if (firstPlan.connection.mode === "kubernetes") {
    console.log("  Connection: Kubernetes port-forward");
    console.log(`  Context:    ${firstPlan.connection.kubeContext ?? "current kubectl context"}`);
    console.log(`  Namespace:  ${firstPlan.connection.namespace}`);
    console.log(`  Service:    ${firstPlan.connection.kafkaService}`);
    console.log(`  Bootstrap:  ${firstPlan.connection.bootstrapServer}`);
  } else {
    console.log("  Connection: Direct Kafka bootstrap server");
    console.log(`  Bootstrap:  ${firstPlan.connection.bootstrapServer}`);
  }

  console.log(`  Groups:     ${plans.length} selected`);
  for (const plan of plans) {
    const topics = plan.topicSelection.mode === "all" ? "all topics" : plan.topicSelection.topics.join(", ");
    console.log(`              ${plan.group} -> ${topics}`);
  }
  console.log(`  Input time: ${firstPlan.normalizedDateTime.input}`);
  console.log(`  Kafka time: ${firstPlan.normalizedDateTime.kafkaDateTime}`);
  console.log(`  Timezone:   ${firstPlan.normalizedDateTime.timezoneLabel}`);
  console.log(`  UTC time:   ${firstPlan.normalizedDateTime.utcPreview}`);
  console.log(`  Time note:  ${firstPlan.normalizedDateTime.note}`);
  console.log("");
}

export function printCommandPreview(label: string, spec: CommandSpec): void {
  console.log(`${label}:`);
  console.log(formatCommand(spec));
  console.log("");
}

export async function confirmExecute(plan: ResetPlan): Promise<void> {
  if (plan.yes) {
    return;
  }

  const consumersStopped = await askConfirm(`Have all consumers for group '${plan.group}' been stopped?`, false);
  if (!consumersStopped) {
    throw new Error("Consumers must be stopped before executing the reset");
  }

  const expected = `RESET ${plan.group}`;
  const actual = await askInput(`Type exactly '${expected}' to apply the reset`);

  if (actual !== expected) {
    throw new Error("Confirmation did not match");
  }
}

export async function confirmBatchExecute(plans: ResetPlan[]): Promise<void> {
  if (plans.length === 0) {
    throw new Error("No reset plans selected");
  }

  if (plans.every((plan) => plan.yes)) {
    return;
  }

  const consumersStopped = await askConfirm(`Have all consumers for ${plans.length} selected group(s) been stopped?`, false);
  if (!consumersStopped) {
    throw new Error("Consumers must be stopped before executing the reset");
  }

  const expected = `RESET ${plans.length} GROUPS`;
  const actual = await askInput(`Type exactly '${expected}' to apply the resets`);

  if (actual !== expected) {
    throw new Error("Confirmation did not match");
  }
}

export function warnIfGroupAppearsActive(stateOutput: string, group: string): void {
  const activeStates = ["Stable", "PreparingRebalance", "CompletingRebalance"];

  if (activeStates.some((state) => stateOutput.includes(state))) {
    console.warn(`Warning: consumer group '${group}' may have active members. Stop consumers before executing the reset.`);
  }
}
