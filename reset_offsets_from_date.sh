#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Reset a Kafka consumer group's offsets to offsets matching a backdated date or timestamp.

Run without arguments for an interactive wizard:
  ./reset_offsets_from_date.sh

Direct Kafka access:
  ./reset_offsets_from_date.sh --bootstrap-server HOST:PORT --group GROUP --topic TOPIC --datetime DATETIME

Kubernetes port-forward access:
  ./reset_offsets_from_date.sh --kube-context CONTEXT --namespace NAMESPACE --kafka-service SERVICE --group GROUP --select-topics --datetime DATETIME

Required in non-interactive mode:
  -g, --group GROUP                  Consumer group id to reset
  -d, --datetime VALUE               Target date/time or epoch timestamp

Connection:
  -b, --bootstrap-server HOST:PORT   Kafka bootstrap server
      --kube-context CONTEXT         Kubernetes context for port-forwarding to Kafka
  -n, --namespace NAMESPACE          Kubernetes namespace containing the Kafka service
      --kafka-service SERVICE        Kafka service name or resource, e.g. kafka-bootstrap or svc/kafka-bootstrap
      --kafka-port PORT              Kafka service port. Default: 9092
      --local-port PORT              Local port for the port-forward. Default: 19092

Topic selection:
  -t, --topic TOPIC                  Topic to reset. Can be specified multiple times
      --select-topics                Fuzzy-select one or more topics with fzf
      --all-topics                   Reset offsets for all topics in the consumer group

Options:
      --execute                      Apply the reset after dry-run and confirmation
      --yes                          Skip execute confirmation. Requires --execute
      --timezone local|utc           Timezone used when normalizing epoch timestamps. Default: local
      --timestamp VALUE              Alias for --datetime VALUE
      --command-config FILE          Kafka client properties file
      --kafka-bin DIR                Directory containing Kafka CLI scripts
      --interactive                  Force prompts for missing values
      --no-interactive               Fail instead of prompting for missing values
  -h, --help                         Show this help

Accepted date/time values:
  2026-06-29                         Date, normalized to 2026-06-29T00:00:00.000
  2026-06-29 13:45                   Date and time, normalized to milliseconds
  2026-06-29T13:45:00.123            Kafka datetime format
  1782737100                         Epoch seconds
  1782737100123                      Epoch milliseconds

Safety:
  - The script always runs a dry-run first.
  - The consumer group must be stopped before executing the reset.
  - Executing requires typing a confirmation phrase unless --yes is passed.
EOF
}

error() {
  echo "Error: $*" >&2
  echo >&2
  usage >&2
  exit 1
}

abort() {
  echo "Aborted: $*" >&2
  exit 1
}

info() {
  echo "==> $*" >&2
}

warn() {
  echo "Warning: $*" >&2
}

trim_string() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "$value"
}

has_tty() {
  [[ -t 0 && -t 1 ]]
}

has_fzf() {
  has_tty && command -v fzf >/dev/null 2>&1
}

prompt_text() {
  local prompt="$1"
  local default="${2:-}"
  local reply=""

  if [[ -n "$default" ]]; then
    read -r -p "? ${prompt} [${default}]: " reply
    printf '%s\n' "${reply:-$default}"
  else
    read -r -p "? ${prompt}: " reply
    printf '%s\n' "$reply"
  fi
}

prompt_required() {
  local prompt="$1"
  local default="${2:-}"
  local reply=""

  while true; do
    reply="$(prompt_text "$prompt" "$default")"
    reply="$(trim_string "$reply")"
    if [[ -n "$reply" ]]; then
      printf '%s\n' "$reply"
      return 0
    fi
    echo "Please enter a value." >&2
  done
}

prompt_confirm() {
  local prompt="$1"
  local default="${2:-no}"
  local reply=""
  local suffix="[y/N]"

  if [[ "$default" == "yes" ]]; then
    suffix="[Y/n]"
  fi

  while true; do
    read -r -p "? ${prompt} ${suffix}: " reply
    reply="${reply:-$default}"
    case "$reply" in
      y|Y|yes|YES) return 0 ;;
      n|N|no|NO) return 1 ;;
      *) echo "Please answer yes or no." >&2 ;;
    esac
  done
}

choose_one() {
  local prompt="$1"
  shift
  local choices=("$@")
  local selected=""
  local index=""
  local i=0

  if [[ "${#choices[@]}" -eq 0 ]]; then
    return 1
  fi

  if has_fzf; then
    selected="$(printf '%s\n' "${choices[@]}" | fzf --prompt "${prompt}> " --height 40% --border)" || return 1
    printf '%s\n' "$selected"
    return 0
  fi

  echo "? ${prompt}" >&2
  for ((i = 0; i < ${#choices[@]}; i++)); do
    printf '  %d) %s\n' "$((i + 1))" "${choices[$i]}" >&2
  done

  while true; do
    read -r -p "Choose 1-${#choices[@]}: " index
    if [[ "$index" =~ ^[0-9]+$ ]] && (( index >= 1 && index <= ${#choices[@]} )); then
      printf '%s\n' "${choices[$((index - 1))]}"
      return 0
    fi
    echo "Invalid selection." >&2
  done
}

cleanup() {
  if [[ -n "${port_forward_pid:-}" ]] && kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
  fi

  if [[ -n "${port_forward_log:-}" ]]; then
    rm -f "$port_forward_log"
  fi
}

resolve_kafka_tool() {
  local tool_name="$1"
  local candidate=""

  if [[ -n "$kafka_bin" ]]; then
    for candidate in "${kafka_bin}/${tool_name}" "${kafka_bin}/${tool_name}.sh"; do
      if [[ -x "$candidate" ]]; then
        printf '%s\n' "$candidate"
        return 0
      fi
    done
  else
    for candidate in "$tool_name" "${tool_name}.sh"; do
      if command -v "$candidate" >/dev/null 2>&1; then
        command -v "$candidate"
        return 0
      fi
    done
  fi

  return 1
}

add_command_config() {
  if [[ -n "$command_config" ]]; then
    cmd+=("--command-config" "$command_config")
  fi
}

run_kafka_cmd() {
  local -a run_cmd=("$@")

  printf 'Command:'
  if [[ "$timestamp_timezone" == "utc" ]]; then
    printf ' TZ=UTC'
  fi
  printf ' %q' "${run_cmd[@]}"
  printf '\n\n'

  if [[ "$timestamp_timezone" == "utc" ]]; then
    TZ=UTC "${run_cmd[@]}"
  else
    "${run_cmd[@]}"
  fi
}

format_epoch() {
  local seconds="$1"
  local millis="$2"
  local formatted=""

  if [[ "$timestamp_timezone" == "utc" ]]; then
    formatted="$(TZ=UTC date -r "$seconds" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || TZ=UTC date -d "@$seconds" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || true)"
  else
    formatted="$(date -r "$seconds" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || date -d "@$seconds" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || true)"
  fi

  [[ -n "$formatted" ]] || error "Could not convert epoch timestamp '${datetime_input}'"
  printf '%s.%s\n' "$formatted" "$millis"
}

normalize_datetime() {
  local input="$1"
  local value=""
  local seconds=""
  local millis=""
  local fractional=""

  input="$(trim_string "$input")"
  [[ -n "$input" ]] || error "--datetime cannot be empty"
  datetime_input="$input"
  datetime_note="Provided as Kafka datetime"

  if [[ "$input" =~ ^[0-9]{10}$ ]]; then
    datetime_note="Epoch seconds converted using ${timestamp_timezone} timezone"
    normalized_datetime="$(format_epoch "$input" "000")"
    return 0
  fi

  if [[ "$input" =~ ^[0-9]{11,}$ ]]; then
    seconds="${input:0:${#input}-3}"
    millis="${input:${#input}-3:3}"
    datetime_note="Epoch milliseconds converted using ${timestamp_timezone} timezone"
    normalized_datetime="$(format_epoch "$seconds" "$millis")"
    return 0
  fi

  value="${input/ /T}"

  if [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    datetime_note="Date normalized to start of day"
    normalized_datetime="${value}T00:00:00.000"
    return 0
  fi

  if [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}$ ]]; then
    datetime_note="Date/time normalized with seconds and milliseconds"
    normalized_datetime="${value}:00.000"
    return 0
  fi

  if [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$ ]]; then
    datetime_note="Date/time normalized with milliseconds"
    normalized_datetime="${value}.000"
    return 0
  fi

  if [[ "$value" =~ ^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})\.([0-9]{1,3})$ ]]; then
    fractional="${BASH_REMATCH[2]}"
    while [[ ${#fractional} -lt 3 ]]; do
      fractional="${fractional}0"
    done
    normalized_datetime="${BASH_REMATCH[1]}.${fractional}"
    return 0
  fi

  error "--datetime must be a date, Kafka datetime, epoch seconds, or epoch milliseconds"
}

current_kube_context() {
  kubectl config current-context 2>/dev/null || true
}

start_kube_port_forward() {
  local target="$kafka_service"
  local log_text=""
  local ready=false
  local kubectl_cmd=(kubectl)

  command -v kubectl >/dev/null 2>&1 || error "kubectl is required for Kubernetes connection mode"

  if [[ -n "$kube_context" ]]; then
    kubectl_cmd+=("--context" "$kube_context")
  fi

  [[ -n "$kube_namespace" ]] || error "--namespace is required when using --kafka-service"

  if [[ "$target" != */* ]]; then
    target="svc/${target}"
  fi

  port_forward_log="$(mktemp -t kafka-reset-port-forward.XXXXXX)"
  trap cleanup EXIT
  trap 'cleanup; exit 130' INT
  trap 'cleanup; exit 143' TERM

  info "Opening Kubernetes port-forward ${target} ${local_port}:${kafka_port}..."
  "${kubectl_cmd[@]}" -n "$kube_namespace" port-forward "$target" "${local_port}:${kafka_port}" >"$port_forward_log" 2>&1 &
  port_forward_pid=$!

  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
    if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
      log_text="$(<"$port_forward_log")"
      error "kubectl port-forward exited early: ${log_text:-no output}"
    fi

    log_text="$(<"$port_forward_log")"
    if [[ "$log_text" == *"Forwarding from"* ]]; then
      ready=true
      break
    fi

    sleep 0.5
  done

  if [[ "$ready" != true ]]; then
    log_text="$(<"$port_forward_log")"
    error "Timed out waiting for kubectl port-forward: ${log_text:-no output}"
  fi

  bootstrap_server="127.0.0.1:${local_port}"
  info "Connected to Kafka through Kubernetes port-forward at ${bootstrap_server}."
}

maybe_collect_optional_settings() {
  if [[ "$full_wizard" != true ]]; then
    return 0
  fi

  if [[ -z "$kafka_bin" ]] && prompt_confirm "Use a custom Kafka CLI bin directory?" "no"; then
    kafka_bin="$(prompt_required "Kafka bin directory")"
    kafka_bin="${kafka_bin%/}"
  fi

  if [[ -z "$command_config" ]] && prompt_confirm "Use a Kafka client properties file?" "no"; then
    command_config="$(prompt_required "Path to client properties")"
  fi
}

collect_connection() {
  local mode=""
  local default_context=""

  if [[ -n "$bootstrap_server" && -n "$kafka_service" ]]; then
    error "Use either --bootstrap-server or Kubernetes connection options, not both"
  fi

  if [[ -z "$bootstrap_server" && -z "$kafka_service" ]]; then
    if [[ "$interactive" != true ]]; then
      error "Provide --bootstrap-server or --kafka-service with --namespace"
    fi

    mode="$(choose_one "How should this connect to Kafka?" "Direct bootstrap server" "Kubernetes port-forward")" || abort "Connection selection was cancelled"
    if [[ "$mode" == "Direct bootstrap server" ]]; then
      bootstrap_server="$(prompt_required "Kafka bootstrap server" "localhost:9092")"
    else
      kafka_service="__prompt__"
    fi
  fi

  if [[ -n "$kube_context" && -z "$kafka_service" ]]; then
    if [[ "$interactive" != true ]]; then
      error "--kube-context requires --kafka-service"
    fi
    kafka_service="__prompt__"
  fi

  if [[ -n "$kube_namespace" && -z "$kafka_service" ]]; then
    if [[ "$interactive" != true ]]; then
      error "--namespace requires --kafka-service"
    fi
    kafka_service="__prompt__"
  fi

  if [[ -n "$kafka_service" ]]; then
    default_context="$(current_kube_context)"

    if [[ "$kafka_service" == "__prompt__" || -z "$kafka_service" ]]; then
      kafka_service="$(prompt_required "Kafka Kubernetes service" "kafka-bootstrap")"
    fi

    if [[ -z "$kube_namespace" ]]; then
      kube_namespace="$(prompt_required "Kubernetes namespace" "kafka")"
    fi

    if [[ -z "$kube_context" && "$full_wizard" == true ]]; then
      kube_context="$(prompt_text "Kubernetes context, blank for current context" "$default_context")"
      kube_context="$(trim_string "$kube_context")"
    fi

    if [[ "$full_wizard" == true ]]; then
      kafka_port="$(prompt_required "Kafka service port" "$kafka_port")"
      local_port="$(prompt_required "Local port for port-forward" "$local_port")"
    fi
  fi
}

list_consumer_groups() {
  local -a cmd=(
    "$kafka_consumer_groups"
    "--bootstrap-server" "$bootstrap_server"
    "--list"
  )

  if [[ -n "$command_config" ]]; then
    cmd+=("--command-config" "$command_config")
  fi

  "${cmd[@]}"
}

collect_group() {
  local groups_text=""
  local group=""
  local groups=()

  if [[ -n "$group_id" ]]; then
    return 0
  fi

  if [[ "$interactive" != true ]]; then
    error "--group is required"
  fi

  groups_text="$(list_consumer_groups 2>/dev/null || true)"
  while IFS= read -r group; do
    group="$(trim_string "$group")"
    [[ -n "$group" ]] && groups+=("$group")
  done <<< "$groups_text"

  if [[ "${#groups[@]}" -gt 0 ]]; then
    group_id="$(choose_one "Consumer group" "${groups[@]}")" || abort "Consumer group selection was cancelled"
  else
    warn "Could not list consumer groups. You can still enter one manually."
    group_id="$(prompt_required "Consumer group")"
  fi
}

split_topics_input() {
  local raw="$1"
  local item=""
  local old_ifs="$IFS"
  local items=()

  raw="${raw//$'\n'/,}"
  IFS=','
  read -r -a items <<< "$raw"
  IFS="$old_ifs"

  topics=()
  for item in "${items[@]}"; do
    item="$(trim_string "$item")"
    [[ -n "$item" ]] && topics+=("$item")
  done
}

select_topics_with_fzf() {
  local selected_topics=""
  local topic=""
  local -a list_topics_cmd=()

  command -v fzf >/dev/null 2>&1 || error "--select-topics requires fzf. Install it with 'brew install fzf' or type topics with --topic."

  if ! kafka_topics="$(resolve_kafka_tool kafka-topics)"; then
    error "Could not find kafka-topics or kafka-topics.sh. Add Kafka CLI tools to PATH or pass --kafka-bin"
  fi

  list_topics_cmd=(
    "$kafka_topics"
    "--bootstrap-server" "$bootstrap_server"
    "--list"
  )

  if [[ -n "$command_config" ]]; then
    list_topics_cmd+=("--command-config" "$command_config")
  fi

  selected_topics="$("${list_topics_cmd[@]}" | sort | fzf --multi --prompt "Kafka topics> " --height 40% --border)" || abort "Topic selection was cancelled"
  [[ -n "$selected_topics" ]] || abort "No topics selected"

  topics=()
  while IFS= read -r topic; do
    topic="$(trim_string "$topic")"
    [[ -n "$topic" ]] && topics+=("$topic")
  done <<< "$selected_topics"
}

collect_topic_selection() {
  local topic_selection_modes=0
  local mode=""
  local raw_topics=""
  local can_fuzzy=false
  local choices=()

  if [[ "${#topics[@]}" -gt 0 ]]; then
    topic_selection_modes=$((topic_selection_modes + 1))
  fi
  if [[ "$all_topics" == true ]]; then
    topic_selection_modes=$((topic_selection_modes + 1))
  fi
  if [[ "$select_topics" == true ]]; then
    topic_selection_modes=$((topic_selection_modes + 1))
  fi

  if [[ "$topic_selection_modes" -gt 1 ]]; then
    error "Use exactly one topic selection mode: --topic, --select-topics, or --all-topics"
  fi

  if [[ "$topic_selection_modes" -eq 0 ]]; then
    if [[ "$interactive" != true ]]; then
      error "Use one topic selection mode: --topic, --select-topics, or --all-topics"
    fi

    if has_fzf && resolve_kafka_tool kafka-topics >/dev/null 2>&1; then
      can_fuzzy=true
    fi

    if [[ "$can_fuzzy" == true ]]; then
      choices+=("Fuzzy-select topics")
    fi
    choices+=("Type topic names")
    choices+=("All topics in group")

    mode="$(choose_one "Which topics should be reset?" "${choices[@]}")" || abort "Topic mode selection was cancelled"
    case "$mode" in
      "Fuzzy-select topics") select_topics=true ;;
      "Type topic names") ;;
      "All topics in group") all_topics=true ;;
    esac
  fi

  if [[ "$select_topics" == true ]]; then
    select_topics_with_fzf
  elif [[ "$all_topics" == true ]]; then
    if [[ "$interactive" == true ]]; then
      prompt_confirm "This targets all topics in group '${group_id}'. Continue?" "no" || abort "All-topics reset was not confirmed"
    fi
  elif [[ "${#topics[@]}" -eq 0 ]]; then
    raw_topics="$(prompt_required "Topic names, comma-separated")"
    split_topics_input "$raw_topics"
    [[ "${#topics[@]}" -gt 0 ]] || error "At least one topic is required"
  fi
}

maybe_prompt_timestamp_timezone() {
  local choice=""

  if [[ ! "$datetime_input" =~ ^[0-9]+$ ]]; then
    return 0
  fi

  if [[ "$interactive" != true || "$timestamp_timezone_was_set" == true ]]; then
    return 0
  fi

  choice="$(choose_one "How should the epoch timestamp be shown to Kafka?" "Local timezone (Kafka CLI default)" "UTC (runs Kafka CLI with TZ=UTC)")" || abort "Timezone selection was cancelled"
  if [[ "$choice" == "UTC (runs Kafka CLI with TZ=UTC)" ]]; then
    timestamp_timezone="utc"
  else
    timestamp_timezone="local"
  fi
}

collect_datetime() {
  if [[ -z "$datetime_input" ]]; then
    if [[ "$interactive" != true ]]; then
      error "--datetime is required"
    fi
    datetime_input="$(prompt_required "Backdated date/time or epoch timestamp")"
  fi

  maybe_prompt_timestamp_timezone
  normalize_datetime "$datetime_input"
}

check_group_state() {
  local state_output=""
  local -a state_cmd=(
    "$kafka_consumer_groups"
    "--bootstrap-server" "$bootstrap_server"
    "--describe"
    "--group" "$group_id"
    "--state"
  )

  if [[ -n "$command_config" ]]; then
    state_cmd+=("--command-config" "$command_config")
  fi

  state_output="$("${state_cmd[@]}" 2>/dev/null || true)"
  if [[ "$state_output" == *"Stable"* || "$state_output" == *"PreparingRebalance"* || "$state_output" == *"CompletingRebalance"* ]]; then
    warn "Consumer group '${group_id}' may have active members. Stop consumers before executing the reset."
  fi
}

print_summary() {
  echo
  echo "Review the reset target:"
  if [[ -n "$kafka_service" ]]; then
    echo "  Connection: Kubernetes port-forward"
    echo "  Context:    ${kube_context:-current kubectl context}"
    echo "  Namespace:  ${kube_namespace}"
    echo "  Service:    ${kafka_service}"
    echo "  Bootstrap:  ${bootstrap_server}"
  else
    echo "  Connection: Direct Kafka bootstrap server"
    echo "  Bootstrap:  ${bootstrap_server}"
  fi
  echo "  Group:      ${group_id}"
  if [[ "$all_topics" == true ]]; then
    echo "  Topics:     all topics in group"
  else
    echo "  Topics:     ${#topics[@]} selected"
    printf '              %s\n' "${topics[@]}"
  fi
  echo "  Input time: ${datetime_input}"
  echo "  Kafka time: ${normalized_datetime}"
  echo "  Time note:  ${datetime_note}"
  echo
}

build_reset_command() {
  local action="$1"
  local topic=""

  cmd=(
    "$kafka_consumer_groups"
    "--bootstrap-server" "$bootstrap_server"
    "--group" "$group_id"
    "--reset-offsets"
    "--to-datetime" "$normalized_datetime"
  )

  add_command_config

  if [[ "$all_topics" == true ]]; then
    cmd+=("--all-topics")
  else
    for topic in "${topics[@]}"; do
      cmd+=("--topic" "$topic")
    done
  fi

  cmd+=("$action")
}

confirm_execute() {
  local confirmation=""
  local expected="RESET ${group_id}"

  if [[ "$yes" == true ]]; then
    return 0
  fi

  if [[ "$interactive" != true ]]; then
    error "--execute requires interactive confirmation or --yes"
  fi

  prompt_confirm "Have all consumers for group '${group_id}' been stopped?" "no" || abort "Consumers must be stopped before executing"

  echo
  echo "To apply the reset, type exactly: ${expected}" >&2
  read -r -p "> " confirmation
  [[ "$confirmation" == "$expected" ]] || abort "Confirmation did not match"
}

interactive=false
full_wizard=false
interactive_requested=false
no_interactive=false
bootstrap_server=""
group_id=""
datetime_input=""
normalized_datetime=""
datetime_note=""
all_topics=false
select_topics=false
execute=false
yes=false
command_config=""
kafka_bin=""
kube_context=""
kube_namespace=""
kafka_service=""
kafka_port="9092"
local_port="19092"
port_forward_pid=""
port_forward_log=""
timestamp_timezone="local"
timestamp_timezone_was_set=false
kafka_consumer_groups=""
kafka_topics=""
cmd=()
topics=()
original_arg_count=$#

while [[ $# -gt 0 ]]; do
  case "$1" in
    -b|--bootstrap-server)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      bootstrap_server="$2"
      shift 2
      ;;
    --kube-context)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      kube_context="$2"
      shift 2
      ;;
    -n|--namespace)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      kube_namespace="$2"
      shift 2
      ;;
    --kafka-service)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      kafka_service="$2"
      shift 2
      ;;
    --kafka-port)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      kafka_port="$2"
      shift 2
      ;;
    --local-port)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      local_port="$2"
      shift 2
      ;;
    -g|--group)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      group_id="$2"
      shift 2
      ;;
    -t|--topic)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      topics+=("$2")
      shift 2
      ;;
    -d|--datetime|--timestamp)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      datetime_input="$2"
      shift 2
      ;;
    --all-topics)
      all_topics=true
      shift
      ;;
    --select-topics)
      select_topics=true
      shift
      ;;
    --execute)
      execute=true
      shift
      ;;
    --yes)
      yes=true
      shift
      ;;
    --timezone)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      timestamp_timezone="$2"
      timestamp_timezone_was_set=true
      shift 2
      ;;
    --command-config)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      command_config="$2"
      shift 2
      ;;
    --kafka-bin)
      [[ $# -ge 2 ]] || error "$1 requires a value"
      kafka_bin="${2%/}"
      shift 2
      ;;
    --interactive)
      interactive_requested=true
      shift
      ;;
    --no-interactive)
      no_interactive=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      error "Unknown option: $1"
      ;;
  esac
done

case "$timestamp_timezone" in
  local|utc) ;;
  *) error "--timezone must be local or utc" ;;
esac

if [[ "$yes" == true && "$execute" != true ]]; then
  error "--yes only makes sense with --execute"
fi

if [[ "$no_interactive" == true && "$interactive_requested" == true ]]; then
  error "Use either --interactive or --no-interactive, not both"
fi

if [[ "$no_interactive" == true ]]; then
  interactive=false
elif [[ "$interactive_requested" == true || "$original_arg_count" -eq 0 || "$(has_tty && echo yes || echo no)" == "yes" ]]; then
  interactive=true
fi

if [[ "$interactive_requested" == true || "$original_arg_count" -eq 0 ]]; then
  full_wizard=true
fi

if [[ "$full_wizard" == true ]]; then
  echo
  echo "Kafka offset reset wizard"
  echo "This tool will dry-run first, then ask again before executing."
  echo
fi

maybe_collect_optional_settings
collect_connection

if [[ -n "$kafka_service" ]]; then
  start_kube_port_forward
fi

if ! kafka_consumer_groups="$(resolve_kafka_tool kafka-consumer-groups)"; then
  error "Could not find kafka-consumer-groups or kafka-consumer-groups.sh. Add Kafka CLI tools to PATH or pass --kafka-bin"
fi

collect_group
collect_topic_selection
collect_datetime
check_group_state
print_summary

info "Running dry-run. Review Kafka's proposed partition offsets carefully."
build_reset_command "--dry-run"
run_kafka_cmd "${cmd[@]}"

if [[ "$execute" == true ]]; then
  confirm_execute
  info "Executing Kafka offset reset..."
  build_reset_command "--execute"
  run_kafka_cmd "${cmd[@]}"
elif [[ "$full_wizard" == true || "$interactive_requested" == true ]]; then
  echo
  if prompt_confirm "Apply this reset now?" "no"; then
    confirm_execute
    info "Executing Kafka offset reset..."
    build_reset_command "--execute"
    run_kafka_cmd "${cmd[@]}"
  else
    info "Dry-run only. No offsets were changed."
  fi
else
  info "Dry-run only. Re-run with --execute to apply the reset."
fi
