import type { NormalizedDateTime, TimezoneMode } from "./types.js";

const DATE_ONLY = /^(\d{4})-(\d{2})-(\d{2})$/;
const DATE_HOUR_MINUTE = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})$/;
const DATE_SECONDS = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/;
const DATE_FRACTIONAL_SECONDS = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})\.(\d{1,3})$/;
const EPOCH_SECONDS = /^\d{10}$/;
const EPOCH_MILLISECONDS = /^\d{11,}$/;

export function normalizeDateTime(input: string, timezone: TimezoneMode): NormalizedDateTime {
  const trimmed = input.trim();

  if (!trimmed) {
    throw new Error("datetime cannot be empty");
  }

  if (EPOCH_SECONDS.test(trimmed)) {
    const kafkaDateTime = formatEpoch(Number(trimmed), 0, timezone);
    return {
      input: trimmed,
      kafkaDateTime,
      note: `Epoch seconds converted using ${timezone} timezone`,
      timezone,
      timezoneLabel: timezoneDescription(timezone),
      utcPreview: kafkaDateTimeToUtcPreview(kafkaDateTime, timezone),
    };
  }

  if (EPOCH_MILLISECONDS.test(trimmed)) {
    const seconds = Number(trimmed.slice(0, -3));
    const millis = Number(trimmed.slice(-3));

    const kafkaDateTime = formatEpoch(seconds, millis, timezone);
    return {
      input: trimmed,
      kafkaDateTime,
      note: `Epoch milliseconds converted using ${timezone} timezone`,
      timezone,
      timezoneLabel: timezoneDescription(timezone),
      utcPreview: kafkaDateTimeToUtcPreview(kafkaDateTime, timezone),
    };
  }

  let match = DATE_ONLY.exec(trimmed);
  if (match) {
    assertDateParts(match[1], match[2], match[3]);
    const kafkaDateTime = `${match[1]}-${match[2]}-${match[3]}T00:00:00.000`;
    return {
      input: trimmed,
      kafkaDateTime,
      note: "Date normalized to start of day",
      timezone,
      timezoneLabel: timezoneDescription(timezone),
      utcPreview: kafkaDateTimeToUtcPreview(kafkaDateTime, timezone),
    };
  }

  match = DATE_HOUR_MINUTE.exec(trimmed);
  if (match) {
    assertDateParts(match[1], match[2], match[3]);
    assertTimeParts(match[4], match[5], "00");
    const kafkaDateTime = `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:00.000`;
    return {
      input: trimmed,
      kafkaDateTime,
      note: "Date/time normalized with seconds and milliseconds",
      timezone,
      timezoneLabel: timezoneDescription(timezone),
      utcPreview: kafkaDateTimeToUtcPreview(kafkaDateTime, timezone),
    };
  }

  match = DATE_SECONDS.exec(trimmed);
  if (match) {
    assertDateParts(match[1], match[2], match[3]);
    assertTimeParts(match[4], match[5], match[6]);
    const kafkaDateTime = `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}.000`;
    return {
      input: trimmed,
      kafkaDateTime,
      note: "Date/time normalized with milliseconds",
      timezone,
      timezoneLabel: timezoneDescription(timezone),
      utcPreview: kafkaDateTimeToUtcPreview(kafkaDateTime, timezone),
    };
  }

  match = DATE_FRACTIONAL_SECONDS.exec(trimmed);
  if (match) {
    assertDateParts(match[1], match[2], match[3]);
    assertTimeParts(match[4], match[5], match[6]);
    const fractionalSeconds = match[7] ?? "";
    const kafkaDateTime = `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}.${fractionalSeconds.padEnd(3, "0")}`;
    return {
      input: trimmed,
      kafkaDateTime,
      note: "Provided as Kafka datetime",
      timezone,
      timezoneLabel: timezoneDescription(timezone),
      utcPreview: kafkaDateTimeToUtcPreview(kafkaDateTime, timezone),
    };
  }

  throw new Error("datetime must be a date, Kafka datetime, epoch seconds, or epoch milliseconds");
}

function assertDateParts(yearValue: string | undefined, monthValue: string | undefined, dayValue: string | undefined): void {
  const year = Number(yearValue);
  const month = Number(monthValue);
  const day = Number(dayValue);
  const date = new Date(Date.UTC(year, month - 1, day));

  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw new Error("datetime contains an invalid calendar date");
  }
}

function assertTimeParts(hourValue: string | undefined, minuteValue: string | undefined, secondValue: string | undefined): void {
  const hour = Number(hourValue);
  const minute = Number(minuteValue);
  const second = Number(secondValue);

  if (hour > 23 || minute > 59 || second > 59) {
    throw new Error("datetime contains an invalid time");
  }
}

function formatEpoch(seconds: number, millis: number, timezone: TimezoneMode): string {
  const date = new Date(seconds * 1000 + millis);

  const year = timezone === "utc" ? date.getUTCFullYear() : date.getFullYear();
  const month = timezone === "utc" ? date.getUTCMonth() + 1 : date.getMonth() + 1;
  const day = timezone === "utc" ? date.getUTCDate() : date.getDate();
  const hour = timezone === "utc" ? date.getUTCHours() : date.getHours();
  const minute = timezone === "utc" ? date.getUTCMinutes() : date.getMinutes();
  const second = timezone === "utc" ? date.getUTCSeconds() : date.getSeconds();

  return `${year}-${pad(month)}-${pad(day)}T${pad(hour)}:${pad(minute)}:${pad(second)}.${String(millis).padStart(3, "0")}`;
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

function timezoneDescription(timezone: TimezoneMode): string {
  if (timezone === "utc") {
    return "UTC";
  }

  const timezoneName = Intl.DateTimeFormat().resolvedOptions().timeZone ?? "local";
  const offsetMinutes = -new Date().getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absoluteOffset = Math.abs(offsetMinutes);
  const hours = Math.floor(absoluteOffset / 60);
  const minutes = absoluteOffset % 60;

  return `${timezoneName} (UTC${sign}${pad(hours)}:${pad(minutes)})`;
}

function kafkaDateTimeToUtcPreview(kafkaDateTime: string, timezone: TimezoneMode): string {
  if (timezone === "utc") {
    return `${kafkaDateTime} UTC`;
  }

  const date = new Date(kafkaDateTime);
  if (Number.isNaN(date.getTime())) {
    return "Unable to calculate UTC preview";
  }

  return `${date.toISOString().replace("Z", "")} UTC`;
}
