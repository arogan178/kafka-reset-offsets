export type TimezoneMode = "local" | "utc";

export type ResetAction = "dry-run" | "execute";

export type ConnectionConfig =
  | {
      mode: "direct";
      bootstrapServer: string;
    }
  | {
      mode: "kubernetes";
      bootstrapServer: string;
      kubeContext?: string;
      namespace: string;
      kafkaService: string;
      kafkaPort: number;
      localPort: number;
    };

export type TopicSelection =
  | {
      mode: "topics";
      topics: string[];
    }
  | {
      mode: "all";
    };

export interface NormalizedDateTime {
  input: string;
  kafkaDateTime: string;
  note: string;
  timezone: TimezoneMode;
  timezoneLabel: string;
  utcPreview: string;
}

export interface CliOptions {
  bootstrapServer?: string;
  kubeContext?: string;
  namespace?: string;
  kafkaService?: string;
  kafkaPort: number;
  localPort: number;
  group?: string;
  groupPrefix?: string;
  selectGroups: boolean;
  topics: string[];
  selectTopics: boolean;
  allTopics: boolean;
  datetime?: string;
  commandConfig?: string;
  kafkaBin?: string;
  execute: boolean;
  yes: boolean;
  timezone: TimezoneMode;
  interactive: boolean;
  noInteractive: boolean;
}

export interface ResetPlan {
  connection: ConnectionConfig;
  group: string;
  topicSelection: TopicSelection;
  normalizedDateTime: NormalizedDateTime;
  commandConfig?: string;
  kafkaBin?: string;
  execute: boolean;
  yes: boolean;
}
