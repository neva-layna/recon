package com.reconciliation.synthdata;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Test utility for decoding a captured side-topic Avro payload file.
 */
public final class KafkaSideTopicPayloadFileDecodeCheck {
    private KafkaSideTopicPayloadFileDecodeCheck() {
    }

    public static void main(String[] args) throws Exception {
        String payloadFile = null;
        KafkaSideTopicKind kind = null;
        String sideTopic = "<unknown>";
        for (int index = 0; index < args.length; index++) {
            String arg = args[index];
            if ("--payload-file".equals(arg)) {
                payloadFile = value(args, ++index, arg);
            } else if ("--kind".equals(arg)) {
                kind = KafkaSideTopicKind.parse(value(args, ++index, arg));
            } else if ("--side-topic".equals(arg)) {
                sideTopic = value(args, ++index, arg);
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }
        if (payloadFile == null) {
            throw new IllegalArgumentException("missing --payload-file");
        }
        if (kind == null) {
            throw new IllegalArgumentException("missing --kind");
        }

        SideTopicDecodedRecord record = SideTopicPayloadDecodeSupport.decodeSingle(
            Files.readAllBytes(Paths.get(payloadFile)),
            kind,
            sideTopic
        );
        SideTopicPayloadDecodeSupport.printDecoded(System.out, record, "[synth-data-test] decoded_payload_file");
    }

    private static String value(String[] args, int index, String flag) {
        if (index >= args.length || args[index].startsWith("--")) {
            throw new IllegalArgumentException("missing value for " + flag);
        }
        return args[index];
    }
}
