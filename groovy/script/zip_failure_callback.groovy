/*
 * zip_failure_callback.groovy
 *
 * Input:  any FlowFile that leaves PACKAGE_ORDER through an error path:
 *           - single DICOM files (manifest entry not available, invalid
 *             fragment, incomplete series bin of Create Series ZIP)
 *           - the merged series ZIP (complete manifest not available,
 *             validation failed)
 * Output: JSON for
 *         POST #{dcma.api.url}/orders/${dcma.order.id}/retrievals/${dcma.retrieve.id}/zip-failure
 *
 * The backend sets the series to FAILED and stores the first reason on the
 * retrieval. One broken series usually produces one report per DICOM file;
 * the backend answers all but the first with duplicate=true, and the
 * manifest endpoints then answer 410 for late fragments.
 *
 * The FlowFile content is replaced by the callback JSON. To keep the DICOM
 * file / ZIP for analysis, leave the existing connection to the error port
 * and add a second connection of the same relationship to this processor
 * (NiFi clones the FlowFile for both connections).
 *
 * Error code, in this order:
 *   1. dcma.zip.error.code (Assign ZIP Fragment, Validate Series ZIP,
 *      Mark ZIP Manifest Retry Exhausted)
 *   2. invokehttp.status.code not 2xx      -> MANIFEST_HTTP_<code>
 *   3. invokehttp.java.exception.class set -> MANIFEST_UNREACHABLE
 *   4. fragment.count set (Create Series ZIP failure, bin incomplete
 *      after Max Bin Age)                  -> ZIP_INCOMPLETE
 *   5. otherwise: NOT reported. Every error path of PACKAGE_ORDER sets
 *      one of the attributes above; a FlowFile without them comes from a
 *      normal relationship that was connected by mistake (e.g. "Response"
 *      or "Original" of Get ZIP Manifest Entry). Reporting it would set a
 *      healthy series to FAILED, so it goes to failure with
 *      dcma.zip.report.error instead.
 *
 * Messages are technical texts from NiFi/backend; no PHI is added here.
 * Missing correlation attributes throw an exception: with Failure Strategy
 * "transfer to failure" the FlowFile goes to failure (no session.rollback()).
 */
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (flowFile == null) {
    return
}

def required = { String name ->
    String value = flowFile.getAttribute(name)
    if (!value?.trim()) {
        throw new IllegalArgumentException("Missing required attribute: ${name}")
    }
    value.trim()
}

def optional = { String name ->
    String value = flowFile.getAttribute(name)
    value?.trim() ? value.trim() : null
}

String orderId = required("dcma.order.id")
String retrieveId = required("dcma.retrieve.id")
String studyUid = required("dcma.study.uid")
String seriesUid = required("dcma.series.uid")

UUID.fromString(orderId)
UUID.fromString(retrieveId)

// --------------------------------------------------
// Determine error code and message
// --------------------------------------------------

String zipErrorCode = optional("dcma.zip.error.code")
String httpStatus = optional("invokehttp.status.code")
String exceptionClass = optional("invokehttp.java.exception.class")
String fragmentCount = optional("fragment.count")

boolean httpError = httpStatus != null && !httpStatus.startsWith("2")

String errorCode
String errorMessage

if (zipErrorCode != null) {
    errorCode = zipErrorCode.toUpperCase(Locale.ROOT)
            .replaceAll("[^A-Z0-9_]", "_")
    errorMessage = optional("dcma.zip.error.message")
} else if (httpError) {
    errorCode = "MANIFEST_HTTP_" + httpStatus.replaceAll("[^0-9]", "")
    errorMessage = [
            "ZIP manifest request failed",
            optional("invokehttp.status.message"),
            optional("invokehttp.request.url")
    ].findAll { it }.join(": ")
} else if (exceptionClass != null) {
    errorCode = "MANIFEST_UNREACHABLE"
    errorMessage = [
            exceptionClass,
            optional("invokehttp.java.exception.message")
    ].findAll { it }.join(": ")
} else if (fragmentCount != null) {
    errorCode = "ZIP_INCOMPLETE"
    errorMessage = "Series ZIP not created: not all " + fragmentCount +
            " fragments arrived within Max Bin Age (Create Series ZIP)"
} else {
    String reason = "No error indicator on FlowFile (invokehttp.status.code=" +
            (httpStatus ?: "none") + ", no dcma.zip.error.code, no fragment.count): " +
            "probably connected from a non-error relationship, not reported"

    log.error("ZIP failure not reported: orderId={}, retrieveId={}: {}",
            [orderId, retrieveId, reason] as Object[])

    flowFile = session.putAttribute(flowFile, "dcma.zip.report.error", reason)
    session.transfer(flowFile, REL_FAILURE)
    return
}

if (errorCode.length() > 50) {
    errorCode = errorCode.substring(0, 50)
}

if (errorMessage != null && errorMessage.length() > 1000) {
    errorMessage = errorMessage.substring(0, 1000)
}

// --------------------------------------------------
// Callback JSON
// --------------------------------------------------

def report = [
        studyInstanceUid : studyUid,
        seriesInstanceUid: seriesUid,
        errorCode        : errorCode,
        errorMessage     : errorMessage
]

byte[] json = JsonOutput.toJson(report).getBytes(StandardCharsets.UTF_8)

flowFile = session.write(flowFile, { output -> output.write(json) } as OutputStreamCallback)

flowFile = session.putAllAttributes(flowFile, [
        "mime.type"             : "application/json",
        "dcma.zip.failure.code" : errorCode
])

// Body of the previous InvokeHTTP call is not needed any more.
flowFile = session.removeAttribute(flowFile, "dcma.zip.manifest.json")

log.warn("ZIP packaging failed: orderId={}, retrieveId={}, errorCode={}",
        [orderId, retrieveId, errorCode] as Object[])

session.transfer(flowFile, REL_SUCCESS)
