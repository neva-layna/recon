package com.reconciliation.kafka.support;

import lombok.Getter;

/**
 * Runtime signal used to stop checker flow with an intended exit code.
 */
@Getter
public final class ReconExit extends RuntimeException {
    /**
     * Process exit code requested by the checker.
     */
    private final int code;
    /**
     * Whether the top-level entrypoint should call System.exit.
     */
    private final boolean exitJvm;

    /**
     * Creates a checker termination signal.
     *
     * @param code requested process exit code
     * @param message operator-facing reason
     * @param exitJvm whether the entrypoint should exit the JVM
     */
    public ReconExit(int code, String message, boolean exitJvm) {
        super(message);
        this.code = code;
        this.exitJvm = exitJvm;
    }
}
