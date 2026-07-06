#!/usr/bin/env bash
set -u

# Full local Spark 3.5 fixture validation runner for the Java spark-submit port.
#
# Build the jar first with:
#   GRADLE_USER_HOME=/tmp/recon-gradle ./gradlew jar

SPARK_SHELL_BIN="${SPARK_SHELL_BIN:-spark-shell}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
FIXTURE_ROOT="${FIXTURE_ROOT:-/tmp/recon-kafka-offset-fixtures-java}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/recon-kafka-offset-evidence-java}"
RUN_DATE="${RUN_DATE:-2026-07-02}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHECKER_JAR="${CHECKER_JAR:-$WORKSPACE_ROOT/build/libs/recon-kafka-offset-gap-checker-1.0.0.jar}"
CHECKER_CLASS="${CHECKER_CLASS:-com.reconciliation.kafka.KafkaOffsetGapChecker}"
GENERATOR="$WORKSPACE_ROOT/tests/fixtures/generate_kafka_offset_gap_fixtures.scala"
PROD_WRAPPER="$WORKSPACE_ROOT/scripts/run_java_kafka_offset_gap_check_prod.sh"
SCENARIO_ROOT="$EVIDENCE_ROOT/scenarios"
RESULTS_TSV="$EVIDENCE_ROOT/scenario_results.tsv"
ASSERTIONS_TSV="$EVIDENCE_ROOT/assertion_results.tsv"

print_command() {
  local output_file="$1"
  shift
  printf '%q ' "$@" > "$output_file"
  printf '\n' >> "$output_file"
}

run_spark_shell_command() {
  local script_path="$1"
  shift

  "$SPARK_SHELL_BIN" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    -i "$script_path"
}

run_java_checker_command() {
  local roots="$1"
  shift

  "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    --conf "spark.recon.inputRoots=$roots" \
    --conf "spark.recon.runDate=$RUN_DATE" \
    "$@" \
    "$CHECKER_JAR"
}

run_java_checker_raw_command() {
  "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    "$CHECKER_JAR"
}

run_java_checker_yaml_command() {
  local yaml_file="$1"
  shift

  SPRING_CONFIG_LOCATION="file:$yaml_file" \
  "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    "$CHECKER_JAR"
}

write_listing() {
  local path="$1"
  local output_file="$2"

  if [[ -d "$path" ]]; then
    find "$path" -maxdepth 7 -print | sort > "$output_file"
  else
    printf 'missing: %s\n' "$path" > "$output_file"
  fi
}

write_yaml_config() {
  local output_file="$1"
  local roots="$2"
  local fail_on_gaps="$3"
  local missing_offsets_limit="${4:-1000}"
  local root
  local root_items=()

  IFS=',' read -r -a root_items <<< "$roots"
  {
    printf 'recon:\n'
    printf '  input-roots:\n'
    for root in "${root_items[@]}"; do
      printf '    - "%s"\n' "$root"
    done
    printf '  metadata-column: cactus__metadata\n'
    printf '  date-partition-column: timestampcolumn\n'
    printf '  run-date: "%s"\n' "$RUN_DATE"
    printf '  normalized-offsets-overwrite: true\n'
    printf '  fail-on-invalid-rows: true\n'
    printf '  fail-on-gaps: %s\n' "$fail_on_gaps"
    printf '  missing-offsets-limit: %s\n' "$missing_offsets_limit"
    printf '  exit-on-completion: true\n'
  } > "$output_file"
}

write_common_env_file() {
  local output_file="$1"
  local roots="${2:-}"

  {
    printf 'SPARK_SUBMIT_BIN=%q\n' "$SPARK_SUBMIT_BIN"
    printf 'SPARK_SHELL_BIN=%q\n' "$SPARK_SHELL_BIN"
    printf 'SPARK_MASTER=%q\n' "$SPARK_MASTER"
    printf 'CHECKER_JAR=%q\n' "$CHECKER_JAR"
    printf 'CHECKER_CLASS=%q\n' "$CHECKER_CLASS"
    printf 'RUN_DATE=%q\n' "$RUN_DATE"
    printf 'INPUT_ROOTS_CSV=%q\n' "$roots"
  } > "$output_file"
}

run_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"
  shift 3

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  write_common_env_file "$env_file" "$roots"
  print_command "$command_file" \
    "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    --conf "spark.recon.inputRoots=$roots" \
    --conf "spark.recon.runDate=$RUN_DATE" \
    "$@" \
    "$CHECKER_JAR"

  echo "[recon-java-test] scenario=$name expected_exit=$expected"
  run_java_checker_command "$roots" "$@" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-java-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

run_raw_expected() {
  local name="$1"
  local expected="$2"
  shift 2

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  write_common_env_file "$env_file"
  print_command "$command_file" \
    "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    "$CHECKER_JAR"

  echo "[recon-java-test] scenario=$name expected_exit=$expected"
  run_java_checker_raw_command "$@" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-java-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

run_yaml_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"
  local fail_on_gaps="$4"
  shift 4

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local yaml_file="$scenario_dir/application.yml"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  write_yaml_config "$yaml_file" "$roots" "$fail_on_gaps"
  write_common_env_file "$env_file" "$roots"
  printf 'SPRING_CONFIG_LOCATION=%q\n' "file:$yaml_file" >> "$env_file"
  print_command "$command_file" \
    env "SPRING_CONFIG_LOCATION=file:$yaml_file" \
    "$SPARK_SUBMIT_BIN" \
    --class "$CHECKER_CLASS" \
    --master "$SPARK_MASTER" \
    --conf "spark.sql.session.timeZone=UTC" \
    "$@" \
    "$CHECKER_JAR"

  echo "[recon-java-test] scenario=$name expected_exit=$expected"
  run_java_checker_yaml_command "$yaml_file" "$@" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-java-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

run_wrapper_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  write_common_env_file "$env_file" "$roots"
  printf 'FAIL_ON_GAPS=true\n' >> "$env_file"
  {
    printf 'env \\\n'
    printf '  SPARK_SUBMIT_BIN=%q \\\n' "$SPARK_SUBMIT_BIN"
    printf '  SPARK_MASTER=%q \\\n' "$SPARK_MASTER"
    printf '  CHECKER_JAR=%q \\\n' "$CHECKER_JAR"
    printf '  INPUT_ROOTS_CSV=%q \\\n' "$roots"
    printf '  RUN_DATE=%q \\\n' "$RUN_DATE"
    printf '  FAIL_ON_GAPS=true \\\n'
    printf '  %q\n' "$PROD_WRAPPER"
  } > "$command_file"

  echo "[recon-java-test] scenario=$name expected_exit=$expected"
  SPARK_SUBMIT_BIN="$SPARK_SUBMIT_BIN" \
  SPARK_MASTER="$SPARK_MASTER" \
  CHECKER_JAR="$CHECKER_JAR" \
  INPUT_ROOTS_CSV="$roots" \
  RUN_DATE="$RUN_DATE" \
  FAIL_ON_GAPS=true \
  "$PROD_WRAPPER" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-java-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

run_wrapper_yaml_expected() {
  local name="$1"
  local expected="$2"
  local roots="$3"
  local fail_on_gaps="$4"

  local scenario_dir="$SCENARIO_ROOT/$name"
  rm -rf "$scenario_dir"
  mkdir -p "$scenario_dir"

  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local command_file="$scenario_dir/command.txt"
  local env_file="$scenario_dir/env.txt"
  local yaml_file="$scenario_dir/application.yml"
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
  write_yaml_config "$yaml_file" "$roots" "$fail_on_gaps"
  write_common_env_file "$env_file" "$roots"
  printf 'APPLICATION_YML=%q\n' "$yaml_file" >> "$env_file"
  {
    printf 'env \\\n'
    printf '  SPARK_SUBMIT_BIN=%q \\\n' "$SPARK_SUBMIT_BIN"
    printf '  SPARK_MASTER=%q \\\n' "$SPARK_MASTER"
    printf '  CHECKER_JAR=%q \\\n' "$CHECKER_JAR"
    printf '  APPLICATION_YML=%q \\\n' "$yaml_file"
    printf '  %q\n' "$PROD_WRAPPER"
  } > "$command_file"

  echo "[recon-java-test] scenario=$name expected_exit=$expected"
  SPARK_SUBMIT_BIN="$SPARK_SUBMIT_BIN" \
  SPARK_MASTER="$SPARK_MASTER" \
  CHECKER_JAR="$CHECKER_JAR" \
  APPLICATION_YML="$yaml_file" \
  "$PROD_WRAPPER" > "$stdout_file" 2> "$stderr_file"
  local code=$?
  printf '%s\n' "$code" > "$exit_file"

  if [[ "$code" == "$expected" ]]; then
    verdict="pass"
  fi

  printf '%s\n' "$verdict" > "$verdict_file"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$expected" "$code" "$verdict" "$stdout_file" "$stderr_file" >> "$RESULTS_TSV"

  echo "[recon-java-test] scenario=$name observed_exit=$code verdict=$verdict evidence=$scenario_dir"

  [[ "$verdict" == "pass" ]]
}

assert_stdout_regex() {
  local name="$1"
  local regex="$2"
  local label="$3"
  local scenario_dir="$SCENARIO_ROOT/$name"
  local stdout_file="$scenario_dir/stdout.log"
  local stderr_file="$scenario_dir/stderr.log"
  local assertion_file="$scenario_dir/assertion_$label.txt"

  if grep -Eq "$regex" "$stdout_file" "$stderr_file"; then
    printf 'pass\nregex=%s\n' "$regex" > "$assertion_file"
    printf '%s\t%s\tpass\t%s\n' "$name" "$label" "$regex" >> "$ASSERTIONS_TSV"
    echo "[recon-java-test] scenario=$name assertion=$label verdict=pass"
    return 0
  fi

  printf 'fail\nregex=%s\nstdout=%s\nstderr=%s\n' "$regex" "$stdout_file" "$stderr_file" > "$assertion_file"
  printf '%s\t%s\tfail\t%s\n' "$name" "$label" "$regex" >> "$ASSERTIONS_TSV"
  echo "[recon-java-test] scenario=$name assertion=$label verdict=fail evidence=$assertion_file"
  return 1
}

if [[ -z "$EVIDENCE_ROOT" || "$EVIDENCE_ROOT" == "/" ]]; then
  echo "[recon-java-test] refusing unsafe EVIDENCE_ROOT=$EVIDENCE_ROOT"
  exit 2
fi

if [[ -z "$FIXTURE_ROOT" || "$FIXTURE_ROOT" == "/" ]]; then
  echo "[recon-java-test] refusing unsafe FIXTURE_ROOT=$FIXTURE_ROOT"
  exit 2
fi

if [[ ! -f "$CHECKER_JAR" ]]; then
  echo "[recon-java-test] checker jar not found: $CHECKER_JAR"
  exit 2
fi

rm -rf "$EVIDENCE_ROOT"
mkdir -p "$SCENARIO_ROOT"
printf 'scenario\texpected_exit\tobserved_exit\tverdict\tstdout\tstderr\n' > "$RESULTS_TSV"
printf 'scenario\tassertion\tverdict\tpattern\n' > "$ASSERTIONS_TSV"

version_dir="$EVIDENCE_ROOT/spark_version"
mkdir -p "$version_dir"
print_command "$version_dir/command.txt" "$SPARK_SUBMIT_BIN" --version
"$SPARK_SUBMIT_BIN" --version > "$version_dir/stdout.log" 2> "$version_dir/stderr.log"
version_code=$?
printf '%s\n' "$version_code" > "$version_dir/exit_code.txt"
if [[ "$version_code" -ne 0 ]]; then
  printf 'fail\n' > "$version_dir/verdict.txt"
  echo "[recon-java-test] spark-version observed_exit=$version_code verdict=fail evidence=$version_dir"
  exit 1
fi
if grep -Eiq 'version 3\.5\.|Spark 3\.5\.' "$version_dir/stdout.log" "$version_dir/stderr.log"; then
  printf 'pass\n' > "$version_dir/verdict.txt"
else
  printf 'fail\n' > "$version_dir/verdict.txt"
  echo "[recon-java-test] spark-version did not report Spark 3.5.x evidence=$version_dir"
  exit 1
fi
echo "[recon-java-test] spark-version verdict=pass evidence=$version_dir"

generator_dir="$EVIDENCE_ROOT/generator"
mkdir -p "$generator_dir"
print_command "$generator_dir/command.txt" \
  "$SPARK_SHELL_BIN" \
  --master "$SPARK_MASTER" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "recon.fixtureOutputRoot=$FIXTURE_ROOT" \
  --conf "recon.fixtureRunDate=$RUN_DATE" \
  -i "$GENERATOR"

run_spark_shell_command "$GENERATOR" \
  --conf "recon.fixtureOutputRoot=$FIXTURE_ROOT" \
  --conf "recon.fixtureRunDate=$RUN_DATE" \
  > "$generator_dir/stdout.log" \
  2> "$generator_dir/stderr.log"
generator_code=$?
printf '%s\n' "$generator_code" > "$generator_dir/exit_code.txt"
if [[ "$generator_code" -ne 0 ]]; then
  printf 'fail\n' > "$generator_dir/verdict.txt"
  echo "[recon-java-test] fixture-generation observed_exit=$generator_code verdict=fail evidence=$generator_dir"
  exit 1
fi
printf 'pass\n' > "$generator_dir/verdict.txt"
echo "[recon-java-test] fixture-generation observed_exit=0 verdict=pass evidence=$generator_dir"

cache_path="$FIXTURE_ROOT/cache/normalized_offsets"
case "$cache_path" in
  /*) cache_conf_path="file://$cache_path" ;;
  *) cache_conf_path="$cache_path" ;;
esac

failures=0

run_expected "continuous_pass" 0 "$FIXTURE_ROOT/pass/root_a,$FIXTURE_ROOT/pass/root_b" || failures=$((failures + 1))
run_expected "cross_root_split_offsets" 0 "$FIXTURE_ROOT/split/root_a,$FIXTURE_ROOT/split/root_b" || failures=$((failures + 1))
if run_yaml_expected "yaml_continuous_pass" 0 "$FIXTURE_ROOT/pass/root_a,$FIXTURE_ROOT/pass/root_b" true; then
  assert_stdout_regex "yaml_continuous_pass" 'recon.runDateSource=application_yml:recon.runDate' "yaml_run_date_source" || failures=$((failures + 1))
  assert_stdout_regex "yaml_continuous_pass" 'RESULT: PASS no gaps detected' "yaml_pass_result" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_yaml_expected "yaml_missing_offsets" 1 "$FIXTURE_ROOT/gap/root_a" true; then
  assert_stdout_regex "yaml_missing_offsets" 'recon.runDateSource=application_yml:recon.runDate' "yaml_run_date_source" || failures=$((failures + 1))
  assert_stdout_regex "yaml_missing_offsets" 'partition=0 .*missing_offset_count=1 .*has_gaps=true .*missing_offsets=\[1\]' "yaml_gap_reported" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_yaml_expected "yaml_spark_conf_overrides_yaml" 1 "$FIXTURE_ROOT/pass/root_a,$FIXTURE_ROOT/pass/root_b" false \
  --conf "spark.recon.inputRoots=$FIXTURE_ROOT/gap/root_a" \
  --conf "spark.recon.failOnGaps=true"; then
  assert_stdout_regex "yaml_spark_conf_overrides_yaml" 'recon.failOnGaps=true' "spark_conf_override_fail_flag" || failures=$((failures + 1))
  assert_stdout_regex "yaml_spark_conf_overrides_yaml" 'partition=0 .*missing_offset_count=1 .*has_gaps=true .*missing_offsets=\[1\]' "spark_conf_override_gap" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_expected "missing_offsets" 1 "$FIXTURE_ROOT/gap/root_a"; then
  assert_stdout_regex "missing_offsets" 'partition=0 .*missing_offset_count=1 .*has_gaps=true .*missing_offsets=\[1\] .*missing_offsets_limit=1000 .*missing_offsets_truncated=false' "missing_value_partition_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_expected "fail_on_gaps_false_allows_gap" 0 "$FIXTURE_ROOT/gap/root_a" \
  --conf "spark.recon.failOnGaps=false"; then
  assert_stdout_regex "fail_on_gaps_false_allows_gap" 'partition=0 .*missing_offset_count=1 .*has_gaps=true .*missing_offsets=\[1\]' "gap_reported_without_failure" || failures=$((failures + 1))
  assert_stdout_regex "fail_on_gaps_false_allows_gap" 'recon.failOnGaps=false' "fail_flag_config" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_expected "missing_offsets_over_limit" 1 "$FIXTURE_ROOT/gap_over_limit/root_a" \
  --conf "spark.recon.missingOffsetsLimit=2"; then
  assert_stdout_regex "missing_offsets_over_limit" 'partition=0 .*missing_offset_count=3 .*has_gaps=true .*missing_offsets=\[1,2\] .*missing_offsets_limit=2 .*missing_offsets_truncated=true' "truncated_partition_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_expected "missing_offsets_zero_limit" 1 "$FIXTURE_ROOT/gap/root_a" \
  --conf "spark.recon.missingOffsetsLimit=0"; then
  assert_stdout_regex "missing_offsets_zero_limit" 'partition=0 .*missing_offset_count=1 .*has_gaps=true .*missing_offsets=\[\] .*missing_offsets_limit=0 .*missing_offsets_truncated=true' "zero_limit_partition_0" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_expected "duplicate_offsets" 0 "$FIXTURE_ROOT/duplicate/root_a"; then
  assert_stdout_regex "duplicate_offsets" 'partition=0 .*missing_offset_count=0 .*has_gaps=false .*missing_offsets=\[\] .*missing_offsets_limit=1000 .*missing_offsets_truncated=false' "no_false_missing_values" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
run_expected "today_run_date_partition_skipped" 0 "$FIXTURE_ROOT/today_skipped/root_a" || failures=$((failures + 1))
run_expected "scan_ignores_invalid_date_and_nonmatching_children" 0 "$FIXTURE_ROOT/scan_noise/root_a" || failures=$((failures + 1))
run_expected "persisted_normalized_offsets" 0 "$FIXTURE_ROOT/pass/root_a,$FIXTURE_ROOT/pass/root_b" \
  --conf "spark.recon.normalizedOffsetsPath=$cache_conf_path" || failures=$((failures + 1))
run_expected "normalized_offsets_overwrite_false_existing_path" 2 "$FIXTURE_ROOT/pass/root_a,$FIXTURE_ROOT/pass/root_b" \
  --conf "spark.recon.normalizedOffsetsPath=$cache_conf_path" \
  --conf "spark.recon.normalizedOffsetsOverwrite=false" || failures=$((failures + 1))
run_expected "malformed_json" 1 "$FIXTURE_ROOT/invalid/malformed_json/root_a" || failures=$((failures + 1))
if run_expected "fail_on_invalid_rows_false_allows_invalid_metadata" 0 "$FIXTURE_ROOT/invalid/malformed_json/root_a" \
  --conf "spark.recon.failOnInvalidRows=false"; then
  assert_stdout_regex "fail_on_invalid_rows_false_allows_invalid_metadata" 'recon.failOnInvalidRows=false' "fail_flag_config" || failures=$((failures + 1))
  assert_stdout_regex "fail_on_invalid_rows_false_allows_invalid_metadata" 'RESULT: PASS' "pass_result" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
run_expected "missing_metadata_value" 1 "$FIXTURE_ROOT/invalid/missing_metadata/root_a" || failures=$((failures + 1))
run_expected "missing_partition" 1 "$FIXTURE_ROOT/invalid/missing_partition/root_a" || failures=$((failures + 1))
run_expected "missing_offset" 1 "$FIXTURE_ROOT/invalid/missing_offset/root_a" || failures=$((failures + 1))
run_expected "non_numeric_partition" 1 "$FIXTURE_ROOT/invalid/non_numeric_partition/root_a" || failures=$((failures + 1))
run_expected "non_numeric_offset" 1 "$FIXTURE_ROOT/invalid/non_numeric_offset/root_a" || failures=$((failures + 1))
run_expected "all_invalid_metadata" 2 "$FIXTURE_ROOT/invalid/all_invalid/root_a" || failures=$((failures + 1))
run_expected "empty_readable_parquet" 2 "$FIXTURE_ROOT/empty/readable_empty/root_a" || failures=$((failures + 1))
run_expected "only_run_date_partitions" 2 "$FIXTURE_ROOT/empty/only_run_date/root_a" || failures=$((failures + 1))
run_expected "no_eligible_old_partitions" 2 "$FIXTURE_ROOT/empty/no_eligible/root_a" || failures=$((failures + 1))
run_raw_expected "missing_input_roots" 2 \
  --conf "spark.recon.runDate=$RUN_DATE" || failures=$((failures + 1))
run_expected "invalid_run_date" 2 "$FIXTURE_ROOT/pass/root_a" \
  --conf "spark.recon.runDate=not-a-date" || failures=$((failures + 1))
run_expected "invalid_fail_flag" 2 "$FIXTURE_ROOT/pass/root_a" \
  --conf "spark.recon.failOnGaps=maybe" || failures=$((failures + 1))
run_expected "invalid_missing_offsets_limit" 2 "$FIXTURE_ROOT/pass/root_a" \
  --conf "spark.recon.missingOffsetsLimit=-1" || failures=$((failures + 1))
run_expected "nonexistent_root" 2 "$FIXTURE_ROOT/does_not_exist" || failures=$((failures + 1))
run_expected "root_not_directory" 2 "$RESULTS_TSV" || failures=$((failures + 1))
run_expected "missing_metadata_column" 2 "$FIXTURE_ROOT/pass/root_a" \
  --conf "spark.recon.metadataColumn=not_present" || failures=$((failures + 1))
if run_wrapper_expected "production_wrapper_gap_config_capture" 1 "$FIXTURE_ROOT/gap/root_a"; then
  assert_stdout_regex "production_wrapper_gap_config_capture" 'recon.inputRoots=.*gap/root_a' "wrapper_input_roots" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_gap_config_capture" 'recon.failOnGaps=true' "wrapper_fail_on_gaps" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_gap_config_capture" 'missing_offsets=\[1\]' "wrapper_missing_offset" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi
if run_wrapper_yaml_expected "production_wrapper_yaml_config_capture" 0 "$FIXTURE_ROOT/pass/root_a,$FIXTURE_ROOT/pass/root_b" true; then
  assert_stdout_regex "production_wrapper_yaml_config_capture" 'recon.runDateSource=application_yml:recon.runDate' "wrapper_yaml_run_date_source" || failures=$((failures + 1))
  assert_stdout_regex "production_wrapper_yaml_config_capture" 'RESULT: PASS no gaps detected' "wrapper_yaml_pass_result" || failures=$((failures + 1))
else
  failures=$((failures + 1))
fi

write_listing "$FIXTURE_ROOT" "$EVIDENCE_ROOT/fixture_listing.txt"
write_listing "$cache_path" "$EVIDENCE_ROOT/cache_listing.txt"

echo "[recon-java-test] scenario_results=$RESULTS_TSV"
echo "[recon-java-test] assertion_results=$ASSERTIONS_TSV"
echo "[recon-java-test] fixture_listing=$EVIDENCE_ROOT/fixture_listing.txt"
echo "[recon-java-test] cache_listing=$EVIDENCE_ROOT/cache_listing.txt"
echo "[recon-java-test] evidence_root=$EVIDENCE_ROOT"

if [[ "$failures" -ne 0 ]]; then
  echo "[recon-java-test] failures=$failures"
  exit 1
fi

echo "[recon-java-test] all fixture checks matched expected outcomes"
