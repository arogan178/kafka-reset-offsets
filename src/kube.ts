import { spawn, type ChildProcess } from "node:child_process";
import type { CommandSpec } from "./kafkaCli.js";
import { runBuffered } from "./process.js";

export interface PortForwardConfig {
  kubeContext?: string;
  namespace: string;
  service: string;
  kafkaPort: number;
  localPort: number;
}

export interface PortForwardHandle {
  bootstrapServer: string;
  stop: () => Promise<void>;
}

export async function listKubeContexts(): Promise<string[]> {
  const result = await runBuffered({
    command: "kubectl",
    args: ["config", "get-contexts", "-o", "name"],
  });

  if (result.exitCode !== 0) {
    return [];
  }

  return lines(result.stdout);
}

export async function currentKubeContext(): Promise<string | undefined> {
  const result = await runBuffered({
    command: "kubectl",
    args: ["config", "current-context"],
  });

  if (result.exitCode !== 0) {
    return undefined;
  }

  return result.stdout.trim() || undefined;
}

export async function listNamespaces(kubeContext?: string): Promise<string[]> {
  const result = await runBuffered(kubectlJsonCommand(["get", "namespaces", "-o", "json"], kubeContext));

  if (result.exitCode !== 0) {
    return [];
  }

  return parseKubernetesNames(result.stdout);
}

export async function listServices(namespace: string, kubeContext?: string): Promise<string[]> {
  const result = await runBuffered(kubectlJsonCommand(["-n", namespace, "get", "services", "-o", "json"], kubeContext));

  if (result.exitCode !== 0) {
    return [];
  }

  return parseKubernetesNames(result.stdout);
}

export async function startPortForward(config: PortForwardConfig): Promise<PortForwardHandle> {
  const target = config.service.includes("/") ? config.service : `svc/${config.service}`;
  const args = [
    ...contextArgs(config.kubeContext),
    "-n",
    config.namespace,
    "port-forward",
    target,
    `${config.localPort}:${config.kafkaPort}`,
  ];

  const child = spawn("kubectl", args, {
    stdio: ["ignore", "pipe", "pipe"],
  });

  await waitForPortForward(child);

  return {
    bootstrapServer: `127.0.0.1:${config.localPort}`,
    stop: () => stopChild(child),
  };
}

function kubectlJsonCommand(args: string[], kubeContext?: string): CommandSpec {
  return {
    command: "kubectl",
    args: [...contextArgs(kubeContext), ...args],
  };
}

function contextArgs(kubeContext?: string): string[] {
  return kubeContext ? ["--context", kubeContext] : [];
}

function waitForPortForward(child: ChildProcess): Promise<void> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let output = "";

    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        stopChild(child).catch(() => undefined);
        reject(new Error(`Timed out waiting for kubectl port-forward. Output: ${output.trim() || "none"}`));
      }
    }, 15_000);

    const onData = (chunk: Buffer) => {
      output += chunk.toString("utf8");
      if (!settled && output.includes("Forwarding from")) {
        settled = true;
        clearTimeout(timer);
        resolve();
      }
    };

    child.stdout?.on("data", onData);
    child.stderr?.on("data", onData);
    child.on("error", (error) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(error);
      }
    });
    child.on("exit", (code) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(new Error(`kubectl port-forward exited with code ${code ?? "unknown"}. Output: ${output.trim() || "none"}`));
      }
    });
  });
}

async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.killed) {
    return;
  }

  child.kill("SIGTERM");
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve();
    }, 2_000);

    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

function parseKubernetesNames(json: string): string[] {
  try {
    const parsed = JSON.parse(json) as { items?: Array<{ metadata?: { name?: string } }> };
    return (parsed.items ?? [])
      .map((item) => item.metadata?.name)
      .filter((name): name is string => Boolean(name))
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

function lines(value: string): string[] {
  return value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}
