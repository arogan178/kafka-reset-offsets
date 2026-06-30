import { checkbox, confirm, input, select } from "@inquirer/prompts";
import { searchCheckbox, searchSelect } from "./searchCheckbox.js";

export function hasInteractiveTerminal(): boolean {
  return Boolean(process.stdin.isTTY && process.stdout.isTTY);
}

export async function askInput(message: string, defaultValue?: string): Promise<string> {
  return input({
    message,
    default: defaultValue,
    required: true,
  });
}

export async function askOptionalInput(message: string, defaultValue?: string): Promise<string | undefined> {
  const value = await input({
    message,
    default: defaultValue,
  });

  return value.trim() || undefined;
}

export async function askConfirm(message: string, defaultValue = false): Promise<boolean> {
  return confirm({
    message,
    default: defaultValue,
  });
}

export async function askSelect<T extends string>(message: string, choices: T[]): Promise<T> {
  return select({
    message,
    choices: choices.map((choice) => ({
      name: choice,
      value: choice,
    })),
  });
}

export async function askSearchSelect(message: string, choices: string[], fallbackMessage?: string): Promise<string> {
  if (choices.length === 0) {
    if (!fallbackMessage) {
      throw new Error(`No choices available for ${message}`);
    }
    return askInput(fallbackMessage);
  }

  return searchSelect({
    message,
    choices,
  });
}

export async function askSearchCheckbox(message: string, choices: string[]): Promise<string[]> {
  if (choices.length === 0) {
    return [];
  }

  return searchCheckbox({
    message,
    choices,
  });
}

export async function askCheckbox(message: string, choices: string[]): Promise<string[]> {
  return checkbox({
    message,
    choices: choices.map((choice) => ({
      name: choice,
      value: choice,
    })),
    required: true,
  });
}
