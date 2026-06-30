import { spawn } from "node:child_process";
import type { CommandSpec } from "./kafkaCli.js";

export interface CommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export async function runBuffered(spec: CommandSpec): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(spec.command, spec.args, {
      env: spec.env ?? process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (exitCode) => {
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
