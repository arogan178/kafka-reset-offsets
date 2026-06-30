import readline from "node:readline";
import { stdin as input, stdout as output } from "node:process";

export interface SearchCheckboxOptions {
  message: string;
  choices: string[];
  pageSize?: number;
}

export interface SearchSelectOptions {
  message: string;
  choices: string[];
  pageSize?: number;
}

export async function searchCheckbox(options: SearchCheckboxOptions): Promise<string[]> {
  if (!input.isTTY || !output.isTTY) {
    throw new Error("searchable checkbox selection requires an interactive terminal");
  }

  const choices = options.choices;
  const selected = new Set<string>();
  let query = "";
  let cursor = 0;
  let viewportStart = 0;
  const pageSize = options.pageSize ?? 12;

  if (choices.length === 0) {
    return [];
  }

  readline.emitKeypressEvents(input);
  const shouldPauseInput = input.isPaused();
  input.resume();
  input.setRawMode(true);

  const render = () => {
    const filtered = filterChoices(choices, query);
    cursor = clampCursor(cursor, filtered.length);
    viewportStart = ensureCursorVisible(cursor, viewportStart, pageSize);

    output.write("\x1b[2J\x1b[H");
    output.write(`? ${options.message}\n`);
    output.write(`  Search: ${query || "(type to filter)"}\n`);
    output.write("  Up/down scroll, space toggles, enter confirms, ctrl-c cancels\n\n");

    const visibleChoices = filtered.slice(viewportStart, viewportStart + pageSize);
    for (let index = 0; index < visibleChoices.length; index++) {
      const choice = visibleChoices[index]!;
      const absoluteIndex = viewportStart + index;
      const isCursor = absoluteIndex === cursor;
      const checked = selected.has(choice) ? "x" : " ";
      output.write(`${isCursor ? ">" : " "} [${checked}] ${choice}\n`);
    }

    if (filtered.length > 0) {
      const visibleEnd = Math.min(viewportStart + visibleChoices.length, filtered.length);
      output.write(`\n  Showing ${viewportStart + 1}-${visibleEnd} of ${filtered.length} matches.\n`);
    }

    if (filtered.length > pageSize) {
      output.write("  Use arrow keys or page up/down to see more.\n");
    }

    if (filtered.length === 0) {
      output.write("  No matches.\n");
    }

    output.write(`\n  Selected: ${selected.size}\n`);
  };

  render();

  return new Promise((resolve, reject) => {
    const cleanup = () => {
      input.setRawMode(false);
      input.off("keypress", onKeypress);
      if (shouldPauseInput) {
        input.pause();
      }
      output.write("\x1b[2J\x1b[H");
    };

    const onKeypress = (character: string | undefined, key: readline.Key) => {
      const filtered = filterChoices(choices, query);

      if (key.ctrl && key.name === "c") {
        cleanup();
        reject(new Error("Selection cancelled"));
        return;
      }

      if (key.name === "return" || key.name === "enter") {
        cleanup();
        resolve([...selected]);
        return;
      }

      if (key.name === "backspace") {
        query = query.slice(0, -1);
        cursor = 0;
        viewportStart = 0;
        render();
        return;
      }

      if (key.name === "up") {
        cursor = Math.max(0, cursor - 1);
        render();
        return;
      }

      if (key.name === "down") {
        cursor = Math.min(Math.max(0, filtered.length - 1), cursor + 1);
        render();
        return;
      }

      if (key.name === "pageup") {
        cursor = Math.max(0, cursor - pageSize);
        render();
        return;
      }

      if (key.name === "pagedown") {
        cursor = Math.min(Math.max(0, filtered.length - 1), cursor + pageSize);
        render();
        return;
      }

      if (key.name === "home") {
        cursor = 0;
        render();
        return;
      }

      if (key.name === "end") {
        cursor = Math.max(0, filtered.length - 1);
        render();
        return;
      }

      if (key.name === "space") {
        const choice = filtered[cursor];
        if (choice) {
          if (selected.has(choice)) {
            selected.delete(choice);
          } else {
            selected.add(choice);
          }
        }
        render();
        return;
      }

      if (character && character >= " " && !key.ctrl && !key.meta) {
        query += character;
        cursor = 0;
        viewportStart = 0;
        render();
      }
    };

    input.on("keypress", onKeypress);
  });
}

export function filterChoices(choices: string[], query: string): string[] {
  const tokens = query
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);

  if (tokens.length === 0) {
    return choices;
  }

  return choices.filter((choice) => {
    const haystack = choice.toLowerCase();
    return tokens.every((token) => haystack.includes(token));
  });
}

export function clampCursor(cursor: number, itemCount: number): number {
  if (itemCount === 0) {
    return 0;
  }

  return Math.min(Math.max(0, cursor), itemCount - 1);
}

export function ensureCursorVisible(cursor: number, viewportStart: number, pageSize: number): number {
  if (cursor < viewportStart) {
    return cursor;
  }

  if (cursor >= viewportStart + pageSize) {
    return cursor - pageSize + 1;
  }

  return viewportStart;
}

export async function searchSelect(options: SearchSelectOptions): Promise<string> {
  if (!input.isTTY || !output.isTTY) {
    throw new Error("search selection requires an interactive terminal");
  }

  const choices = options.choices;
  let query = "";
  let cursor = 0;
  let viewportStart = 0;
  const pageSize = options.pageSize ?? 12;

  if (choices.length === 0) {
    throw new Error("No choices available");
  }

  readline.emitKeypressEvents(input);
  const shouldPauseInput = input.isPaused();
  input.resume();
  input.setRawMode(true);

  const render = () => {
    const filtered = filterChoices(choices, query);
    cursor = clampCursor(cursor, filtered.length);
    viewportStart = ensureCursorVisible(cursor, viewportStart, pageSize);

    output.write("\x1b[2J\x1b[H");
    output.write(`? ${options.message}\n`);
    output.write(`  Search: ${query || "(type to filter)"}\n`);
    output.write("  Up/down scroll, enter selects, ctrl-c cancels\n\n");

    const visibleChoices = filtered.slice(viewportStart, viewportStart + pageSize);
    for (let index = 0; index < visibleChoices.length; index++) {
      const choice = visibleChoices[index]!;
      const absoluteIndex = viewportStart + index;
      const isCursor = absoluteIndex === cursor;
      output.write(`${isCursor ? ">" : " "} ${choice}\n`);
    }

    if (filtered.length > 0) {
      const visibleEnd = Math.min(viewportStart + visibleChoices.length, filtered.length);
      output.write(`\n  Showing ${viewportStart + 1}-${visibleEnd} of ${filtered.length} matches.\n`);
    }

    if (filtered.length > pageSize) {
      output.write("  Use arrow keys or page up/down to see more.\n");
    }

    if (filtered.length === 0) {
      output.write("  No matches.\n");
    }
  };

  render();

  return new Promise((resolve, reject) => {
    const cleanup = () => {
      input.setRawMode(false);
      input.off("keypress", onKeypress);
      if (shouldPauseInput) {
        input.pause();
      }
      output.write("\x1b[2J\x1b[H");
    };

    const onKeypress = (character: string | undefined, key: readline.Key) => {
      const filtered = filterChoices(choices, query);

      if (key.ctrl && key.name === "c") {
        cleanup();
        reject(new Error("Selection cancelled"));
        return;
      }

      if (key.name === "return" || key.name === "enter") {
        const choice = filtered[cursor];
        if (choice) {
          cleanup();
          resolve(choice);
        }
        return;
      }

      if (key.name === "backspace") {
        query = query.slice(0, -1);
        cursor = 0;
        viewportStart = 0;
        render();
        return;
      }

      if (key.name === "up") {
        cursor = Math.max(0, cursor - 1);
        render();
        return;
      }

      if (key.name === "down") {
        cursor = Math.min(Math.max(0, filtered.length - 1), cursor + 1);
        render();
        return;
      }

      if (key.name === "pageup") {
        cursor = Math.max(0, cursor - pageSize);
        render();
        return;
      }

      if (key.name === "pagedown") {
        cursor = Math.min(Math.max(0, filtered.length - 1), cursor + pageSize);
        render();
        return;
      }

      if (key.name === "home") {
        cursor = 0;
        render();
        return;
      }

      if (key.name === "end") {
        cursor = Math.max(0, filtered.length - 1);
        render();
        return;
      }

      if (character && character >= " " && !key.ctrl && !key.meta) {
        query += character;
        cursor = 0;
        viewportStart = 0;
        render();
      }
    };

    input.on("keypress", onKeypress);
  });
}
