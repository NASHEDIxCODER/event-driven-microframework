package com.framework.tracing;

import java.time.Instant;

public class EventTracer {

    public static void trace(String message) {

        String cid = TraceContext.get();
        String timestamp = Instant.now().toString();

        System.out.println(
                "[" + timestamp + "] "
                        + "[CID=" + (cid != null ? cid : "N/A") + "] "
                        + message
        );
    }
}