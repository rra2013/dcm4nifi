
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.Locale

def flowFile = session.get()

if (flowFile == null) {
    return
}

try {

    // --------------------------------------------------
    // 1. Required FlowFile attributes
    // --------------------------------------------------

    def required = { String key ->

        String value = flowFile.getAttribute(key)

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required attribute: ${key}"
            )
        }

        return value.trim()
    }

    String orderId = required("dcma.order.id")
    String studyUid = required("dcma.study.uid")
    String seriesUid = required("dcma.series.uid")
    String retrieveId = required("dcma.retrieve.id")

    UUID.fromString(orderId)
    UUID.fromString(retrieveId)

    // --------------------------------------------------
    // 2. Validate final C-GET response
    // --------------------------------------------------

    String status = required("dcma.cget.status")
            .toUpperCase(Locale.ROOT)

    if (!status.matches("[0-9A-F]{4}")) {
        throw new IllegalArgumentException(
                "Invalid C-GET status: ${status}"
        )
    }

    String finished = required("dcma.cget.finished")

    if (finished != "true") {
        throw new IllegalArgumentException(
                "C-GET has no final response"
        )
    }

    // --------------------------------------------------
    // 3. Read required C-GET counters
    // --------------------------------------------------

    def readCount = { String key ->

        String value = required(key)

        int count = Integer.parseInt(value)

        if (count < 0) {
            throw new IllegalArgumentException(
                    "Invalid C-GET counter ${key}: ${count}"
            )
        }

        return count
    }

    int completed = readCount("dcma.cget.completed")
    int failed = readCount("dcma.cget.failed")
    int warning = readCount("dcma.cget.warning")

    // --------------------------------------------------
    // 4. Read optional Remaining counter
    //
    // NumberOfRemainingSuboperations may be absent
    // in the final C-GET response.
    //
    // -1 = not provided
    // --------------------------------------------------

    String remainingValue =
            flowFile.getAttribute("dcma.cget.remaining")

    int remaining = remainingValue == null ||
            remainingValue.isBlank()
            ? -1
            : Integer.parseInt(remainingValue.trim())

    if (remaining < -1) {
        throw new IllegalArgumentException(
                "Invalid C-GET remaining counter: ${remaining}"
        )
    }

    // --------------------------------------------------
    // 5. Validate retrieval success
    // --------------------------------------------------

    if (status != "0000" ||
            completed == 0 ||
            failed != 0 ||
            warning != 0 ||
            (remaining != 0 && remaining != -1)) {

        throw new IllegalStateException(
                "C-GET not successful: " +
                        "status=${status}, " +
                        "completed=${completed}, " +
                        "failed=${failed}, " +
                        "warning=${warning}, " +
                        "remaining=${remaining}"
        )
    }

    // --------------------------------------------------
    // 6. Read optional expected instance count
    //
    // This attribute may originate from C-FIND:
    // NumberOfSeriesRelatedInstances (0020,1209)
    // --------------------------------------------------

    String expectedValue =
            flowFile.getAttribute("dcma.series.expected")

    Integer expected = null

    if (expectedValue != null &&
            !expectedValue.isBlank()) {

        expected = Integer.parseInt(
                expectedValue.trim()
        )

        if (expected < 0) {
            throw new IllegalArgumentException(
                    "Invalid expected instance count: ${expected}"
            )
        }
    }

    // --------------------------------------------------
    // 7. Generate or reuse stable Event ID
    //
    // The ID must remain unchanged during retries.
    // --------------------------------------------------

    String eventId =
            flowFile.getAttribute("dcma.event.id")

    if (eventId == null || eventId.isBlank()) {

        eventId = UUID.randomUUID().toString()

        flowFile = session.putAttribute(
                flowFile,
                "dcma.event.id",
                eventId
        )
    }

    UUID.fromString(eventId)

    // --------------------------------------------------
    // 8. Create SERIES_RETRIEVED event
    // --------------------------------------------------

    def event = [
            eventId            : eventId,
            eventType          : "SERIES_RETRIEVED",
            studyInstanceUid   : studyUid,
            seriesInstanceUid  : seriesUid,
            retrieveId         : retrieveId,
            expectedInstances  : expected,
            retrievedInstances : completed,
            completedInstances : 0,
            failedInstances    : failed,
            warningInstances   : warning,
            cgetStatus         : status
    ]

    String jsonString = JsonOutput.toJson(event)

    byte[] json = jsonString.getBytes(
            StandardCharsets.UTF_8
    )

    // --------------------------------------------------
    // 9. Replace FlowFile content with JSON
    // --------------------------------------------------

    flowFile = session.write(
            flowFile,
            { output ->
                output.write(json)
            } as OutputStreamCallback
    )

    // --------------------------------------------------
    // 10. Set output FlowFile attributes
    // --------------------------------------------------

    Map<String, String> attributes = [
            "mime.type"                    : "application/json",
            "dcma.event.id"                : eventId,
            "dcma.event.type"              : "SERIES_RETRIEVED",
            "dcma.study.uid"               : studyUid,
            "dcma.series.uid"              : seriesUid,
            "dcma.retrieve.id"             : retrieveId,
            "dcma.series.retrieve.status"  : "COMPLETED",
            "dcma.series.retrieved"        : completed.toString(),
            "dcma.cget.status"             : status,
            "dcma.cget.completed"          : completed.toString(),
            "dcma.cget.failed"             : failed.toString(),
            "dcma.cget.warning"            : warning.toString(),
            "dcma.cget.remaining"          : remaining.toString()
    ]

    if (expected != null) {
        attributes["dcma.series.expected"] =
                expected.toString()
    }

    flowFile = session.putAllAttributes(
            flowFile,
            attributes
    )

    // --------------------------------------------------
    // 11. Route successful retrieval result
    // --------------------------------------------------

    log.info(
            "C-GET series retrieved: " +
                    "orderId={}, studyUid={}, seriesUid={}, " +
                    "retrieveId={}, completed={}, failed={}, " +
                    "warning={}, remaining={}",
            [
                    orderId,
                    studyUid,
                    seriesUid,
                    retrieveId,
                    completed,
                    failed,
                    warning,
                    remaining
            ] as Object[]
    )

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {

    // --------------------------------------------------
    // 12. Error handling
    // --------------------------------------------------

    log.error(
            "Failed to prepare C-GET series result",
            e
    )

    flowFile = session.putAttribute(
            flowFile,
            "dcma.series.result.error",
            e.message ?: "Unknown error"
    )

    session.transfer(flowFile, REL_FAILURE)
}
