package com.reconciliation.kafka.support;

public final class ReconExit extends RuntimeException {
    public final int code;
    public final boolean exitJvm;

    public ReconExit(int code, String message, boolean exitJvm) {
        super(message);
        this.code = code;
        this.exitJvm = exitJvm;
    }
}
