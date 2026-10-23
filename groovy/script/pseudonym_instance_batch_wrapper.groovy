
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (flowFile == null) {
    return
}

try {
    // 1. Read JSON array produced by MergeRecord
    String json = ""

    session.read(flowFile).withCloseable { input ->
        json = input.getText("UTF-8")
    }

    def events = new JsonSlurper().parseText(json)

    if (!(events instanceof List)) {
        throw new IllegalArgumentException(
                "Expected a JSON array from MergeRecord"
        )
    }

    if (events.isEmpty() || events.size() > 100) {
        throw new IllegalArgumentException(
                "Invalid batch size: ${events.size()}"
        )
    }

    // 2. Validate correlation and existing event fields
    String orderId = flowFile.getAttribute("dcma.order.id")
    String retrieveId = flowFile.getAttribute("dcma.retrieve.id")

    if (!orderId?.trim() || !retrieveId?.trim()) {
        throw new IllegalArgumentException(
                "Missing dcma.order.id or dcma.retrieve.id"
        )
    }

    events.each { event ->
        if (!(event instanceof Map)
                || !event.eventId
                || !event.eventType
                || !event.retrieveId
                || !event.studyInstanceUid
                || !event.seriesInstanceUid
                || !event.originalSopInstanceUid) {
            throw new IllegalArgumentException(
                    "Invalid instance event in batch"
            )
        }

        if (event.retrieveId != retrieveId) {
            throw new IllegalArgumentException(
                    "Batch contains different retrieveIds"
            )
        }
    }

    // 3. Wrap events without modifying event IDs
    byte[] body = JsonOutput.toJson([
            events: events
    ]).getBytes(StandardCharsets.UTF_8)

    flowFile = session.write(
            flowFile,
            { output ->
                output.write(body)
            } as OutputStreamCallback
    )

    flowFile = session.putAttribute(
            flowFile,
            "mime.type",
            "application/json"
    )

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error(
            "Failed to create instance event batch: " +
                    e.getMessage(),
            e
    )

    session.rollback()
    throw e
}
