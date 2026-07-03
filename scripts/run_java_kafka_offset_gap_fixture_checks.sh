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
SCENARIO_ROOT="$EVIDENCE_ROOT/scenarios"
RESULTS_TSV="$EVIDENCE_ROOT/scenario_results.tsv"

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

write_listing() {
  local path="$1"
  local output_file="$2"

  if [[ -d "$path" ]]; then
    find "$path" -maxdepth 7 -print | sort > "$output_file"
  else
    printf 'missing: %s\n' "$path" > "$output_file"
  fi
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
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
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
  local expected_file="$scenario_dir/expected_exit.txt"
  local exit_file="$scenario_dir/exit_code.txt"
  local verdict_file="$scenario_dir/verdict.txt"
  local verdict="fail"

  printf '%s\n' "$expected" > "$expected_file"
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

assert_stdout_regex() {
  local name="$1"
  local regex="$2"
  local label="$3"
  local scenario_dir="$SCENARIO_ROOT/$name"
  local stdout_file="$scenario_dir/stdout.log"
  local assertion_file="$scenario_dir/assertion_$label.txt"

  if grep -Eq "$regex" "$stdout_file"; then
    printf 'pass\nregex=%s\n' "$regex" > "$assertion_file"
    echo "[recon-java-test] scenario=$name assertion=$label verdict=pass"
    return 0
  fi

  printf 'fail\nregex=%s\nstdout=%s\n' "$regex" "$stdout_file" > "$assertion_file"
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

write_listing "$FIXTURE_ROOT" "$EVIDENCE_ROOT/fixture_listing.txt"
write_listing "$cache_path" "$EVIDENCE_ROOT/cache_listing.txt"

echo "[recon-java-test] scenario_results=$RESULTS_TSV"
echo "[recon-java-test] fixture_listing=$EVIDENCE_ROOT/fixture_listing.txt"
echo "[recon-java-test] cache_listing=$EVIDENCE_ROOT/cache_listing.txt"
echo "[recon-java-test] evidence_root=$EVIDENCE_ROOT"

if [[ "$failures" -ne 0 ]]; then
  echo "[recon-java-test] failures=$failures"
  exit 1
fi

echo "[recon-java-test] all fixture checks matched expected outcomes"
