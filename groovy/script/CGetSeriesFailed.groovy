/*
 * CGetSeriesFailed.groovy
 *
 * Input:  FlowFile from
 *           - GetScu relationship "failure" (C-GET did not finish), or
 *           - SERIES RETRIEVED relationship "failure": the final C-GET
 *             response was received but rejected by CGetSeriesResult
 *             (e.g. status B000 with warnings, failed sub-operations,
 *             0 completed instances); the reason is in
 *             dcma.series.result.error.
 * Output: SERIES_RETRIEVE_FAILED event as JSON for
 *         POST #{dcma.api.url}/orders/${dcma.order.id}/events
 *
 * GetScu sets dcma.retrieve.id BEFORE the C-GET starts, so instances that
 * were received before the failure carry the same retrieveId. The backend
 * records them, but the series ends as FAILED and no ZIP is built.
 *
 * dcma.cget.* attributes exist only if a final C-GET response was received
 * (e.g. status B000/A7xx/C000). On connect/association errors they are
 * missing; then only dicom.get.error describes the failure.
 *
 * The event ID is stored in dcma.failure.event.id and reused on retries,
 * so the backend can process the callback idempotently.
 */
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (flowFile == null) {
    return
}

def required = { String key ->
    String value = flowFile.getAttribute(key)
    if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("Missing required attribute: ${key}")
    }
    value.trim()
}

def optionalInt = { String key ->
    String value = flowFile.getAttribute(key)
    if (value == null || value.isBlank()) {
        return null
    }
    int n = Integer.parseInt(value.trim())
    n < 0 ? null : n          // remaining = -1 means "not provided"
}

String orderId = required("dcma.order.id")
String studyUid = required("dcma.study.uid")
String seriesUid = required("dcma.series.uid")
String retrieveId = required("dcma.retrieve.id")

UUID.fromString(orderId)
UUID.fromString(retrieveId)

String status = flowFile.getAttribute("dcma.cget.status")?.trim()?.toUpperCase(Locale.ROOT)
if (status != null && !status.matches("[0-9A-F]{4}")) {
    status = null
}

Integer completed = optionalInt("dcma.cget.completed")
Integer failed = optionalInt("dcma.cget.failed")
Integer warning = optionalInt("dcma.cget.warning")
Integer expected = optionalInt("dcma.series.expected")

// Diagnostic text only: GetScu error message, DIMSE error comment and the
// rejection reason of CGetSeriesResult. Must not contain PHI (no patient ID / name).
String message = [
        flowFile.getAttribute("dicom.get.error"),
        flowFile.getAttribute("dcma.cget.error.comment"),
        flowFile.getAttribute("dcma.series.result.error")
].findAll { it?.trim() }.join(" | ")

if (message.length() > 1000) {
    message = message.substring(0, 1000)
}

String eventId = flowFile.getAttribute("dcma.failure.event.id")
if (eventId == null || eventId.isBlank()) {
    eventId = UUID.randomUUID().toString()
}
UUID.fromString(eventId)

def event = [
        eventId           : eventId,
        eventType         : "SERIES_RETRIEVE_FAILED",
        studyInstanceUid  : studyUid,
        seriesInstanceUid : seriesUid,
        retrieveId        : retrieveId,
        expectedInstances : expected,
        retrievedInstances: completed ?: 0,
        failedInstances   : failed ?: 0,
        warningInstances  : warning ?: 0,
        cgetStatus        : status,
        message           : message ?: "C-GET failed without details"
]

byte[] json = JsonOutput.toJson(event).getBytes(StandardCharsets.UTF_8)

flowFile = session.write(flowFile, { output -> output.write(json) } as OutputStreamCallback)

flowFile = session.putAllAttributes(flowFile, [
        "mime.type"            : "application/json",
        "dcma.failure.event.id": eventId,
        "dcma.event.type"      : "SERIES_RETRIEVE_FAILED",
        "dcma.series.retrieve.status": "FAILED"
])

log.warn("C-GET series failed: orderId={}, retrieveId={}, status={}, completed={}, failed={}, warning={}",
        [orderId, retrieveId, status, completed, failed, warning] as Object[])

session.transfer(flowFile, REL_SUCCESS)
// Errors (missing correlation attributes) propagate: with
// Failure Strategy "transfer to failure" the FlowFile goes to "failure".
// Do NOT auto-terminate that relationship.