# Java Code Style

## Immutable Value Access

Immutable value, config, and result classes keep state in `private final` fields
and expose values through Lombok `@Getter`. Use getters at call sites rather
than direct field reads.

This rule applies to `CheckerConfig`, `GapAnalysisResult`, `SideTopicConfig`,
`EligiblePartition`, `RunDateResult`, `ResolvedConfValue`,
`MissingOffsetReport`, `NormalizeResult`, `RootScan`, `MissingOffsetKey`,
`SideTopicClassification`, `SideTopicRecord`, and `ReconExit`.

These classes remain immutable and do not need setters. Do not add setters or
mutable replacement APIs for them.

## Generic Inference

Use Java 8 diamond inference for constructor calls where the target type already
provides the generic type, such as `new ArrayList<>()` or `new LinkedHashMap<>()`.
Use the same style in Gradle Groovy code where Groovy supports diamond syntax.

Use `Optional.empty()` instead of `Optional.<T>empty()` where Java inference can
derive the target type. Keep explicit generic arguments only when they are needed
for Java 8 compatibility or readability. Do not use diamond syntax for anonymous
inner classes such as `new Supplier<LocalDate>() { ... }`.

## Utility Classes

Static-only helper classes use Lombok `@UtilityClass` instead of boilerplate
private no-op constructors. Only apply `@UtilityClass` to classes that are not
intended to be instantiated and whose constructors do not carry behavior.
