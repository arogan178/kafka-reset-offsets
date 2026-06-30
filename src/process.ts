import { spawn } from "node:child_process";
import type { CommandSpec } from "./kafkaCli.js";

export interface CommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export async function runBuffered(spec: CommandSpec, timeoutMs = 60_000): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(spec.command, spec.args, {
      env: spec.env ?? process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) {
        return;
      }

      settled = true;
      child.kill("SIGTERM");
      setTimeout(() => {
        if (child.exitCode === null) {
          child.kill("SIGKILL");
        }
      }, 2_000);
      resolve({
        stdout,
        stderr: `${stderr}${stderr.endsWith("\n") || stderr.length === 0 ? "" : "\n"}Command timed out after ${timeoutMs / 1000}s`,
        exitCode: 124,
      });
    }, timeoutMs);

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (exitCode) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      resolve({
        stdout,
        stderr,
        exitCode: exitCode ?? 1,
      });
    });
  });
}

export async function runInherited(spec: CommandSpec): Promise<number> {
  return new Promise((resolve, reject) => {
    const child = spawn(spec.command, spec.args, {
      env: spec.env ?? process.env,
      stdio: "inherit",
    });

    child.on("error", reject);
    child.on("close", (exitCode) => {
      resolve(exitCode ?? 1);
    });
  });
}
