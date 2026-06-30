export interface PartitionOffset {
  group: string;
  topic: string;
  partition: number;
  offset: string;
}

export function extractTopicsFromGroupDescription(output: string): string[] {
  const topics = new Set<string>();
  let inTable = false;

  for (const line of output.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || /^Consumer group /i.test(trimmed)) {
      continue;
    }

    if (/^GROUP\s+TOPIC\s+/i.test(trimmed)) {
      inTable = true;
      continue;
    }

    if (!inTable) {
      continue;
    }

    const columns = trimmed.split(/\s+/);
    const topic = columns[1];
    if (topic && topic !== "-" && !/^(error|warning|topic)$/i.test(topic)) {
      topics.add(topic);
    }
  }

  return [...topics].sort((a, b) => a.localeCompare(b));
}

export function inferTopicFromConsumerGroupName(group: string, groupPrefix?: string): string | undefined {
  const normalizedPrefix = groupPrefix?.endsWith("-") ? groupPrefix : groupPrefix ? `${groupPrefix}-` : undefined;
  if (normalizedPrefix && group.startsWith(normalizedPrefix)) {
    const topic = group.slice(normalizedPrefix.length).trim();
    return topic || undefined;
  }

  const parts = group.split("-");
  if (parts.length < 3) {
    return undefined;
  }

  const topic = parts.slice(2).join("-").trim();
  return topic || undefined;
}

export function extractCurrentOffsetsFromGroupDescription(output: string): PartitionOffset[] {
  return extractOffsetsFromTable(output, "CURRENT-OFFSET");
}

export function extractPlannedOffsetsFromResetOutput(output: string): PartitionOffset[] {
  return extractOffsetsFromTable(output, "NEW-OFFSET");
}

function extractOffsetsFromTable(output: string, offsetColumnName: "CURRENT-OFFSET" | "NEW-OFFSET"): PartitionOffset[] {
  const offsets: PartitionOffset[] = [];
  let columnIndexes: { group: number; topic: number; partition: number; offset: number } | undefined;

  for (const line of output.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || /^Consumer group /i.test(trimmed)) {
      continue;
    }

    const columns = trimmed.split(/\s+/);
    const maybeOffsetIndex = columns.findIndex((column) => column.toUpperCase() === offsetColumnName);
    if (columns[0]?.toUpperCase() === "GROUP" && maybeOffsetIndex !== -1) {
      columnIndexes = {
        group: columns.findIndex((column) => column.toUpperCase() === "GROUP"),
        topic: columns.findIndex((column) => column.toUpperCase() === "TOPIC"),
        partition: columns.findIndex((column) => column.toUpperCase() === "PARTITION"),
        offset: maybeOffsetIndex,
      };
      continue;
    }

    if (!columnIndexes) {
      continue;
    }

    const group = columns[columnIndexes.group];
    const topic = columns[columnIndexes.topic];
    const partition = Number(columns[columnIndexes.partition]);
    const offset = columns[columnIndexes.offset];

    if (!group || !topic || !Number.isInteger(partition) || !offset || offset === "-") {
      continue;
    }

    offsets.push({
      group,
      topic,
      partition,
      offset,
    });
  }

  return offsets;
}
