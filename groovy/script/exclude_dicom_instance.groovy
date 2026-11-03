import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets
import java.util.UUID

def flowFile = session.get()

if (flowFile == null) {
    return
}

try {
    def required = { String name ->
        String value = flowFile.getAttribute(name)

        if (!value?.trim()) {
            throw new IllegalArgumentException(
                    "Missing required attribute: ${name}"
            )
        }

        return value.trim()
    }

    String orderId = required("dcma.order.id")
    String retrieveId = required("dcma.retrieve.id")
    String studyUid = required("dcma.study.uid")
    String seriesUid = required("dcma.series.uid")

    String sopClassUid = required("AffectedSOPClassUID")

    // This processor handles ALL excluded DICOM instances.
    // SOP Class filtering must be performed upstream.
    // No restriction to Ultrasound Single-frame.

    // Before pseudonymization, this must be the original SOP UID.
    String originalSopUid =
            required("AffectedSOPInstanceUID")

    String eventId = UUID.randomUUID().toString()

    def event = [
            eventId                : eventId,
            eventType              : "INSTANCE_SKIPPED",
            retrieveId             : retrieveId,
            studyInstanceUid       : studyUid,
            seriesInstanceUid      : seriesUid,
            originalSopInstanceUid : originalSopUid,
            pseudonymSopInstanceUid: null
    ]

    byte[] json = JsonOutput.toJson(event)
            .getBytes(StandardCharsets.UTF_8)

    def callback = session.create(flowFile)

    callback = session.write(
            callback,
            { output ->
                output.write(json)
            } as OutputStreamCallback
    )

    String batchGroup = orderId + ":" + retrieveId

    Map<String, String> attributes = [
            "mime.type"                  : "application/json",
            "dcma.callback.type"         : "instance-event",
            "dcma.event.id"              : eventId,
            "dcma.event.type"            : "INSTANCE_SKIPPED",
            "dcma.batch.group"           : batchGroup,
            "dcma.excluded.sop.class.uid": sopClassUid
    ]

    callback = session.putAllAttributes(
            callback,
            attributes
    )

    session.transfer(callback, REL_SUCCESS)

    // Excluded DICOM instance must not reach
    // pseudonymizer, C-STORE or ZIP packaging.
    session.remove(flowFile)

} catch (Exception e) {
    log.error(
            "Failed to create INSTANCE_SKIPPED callback: " +
                    e.getMessage(),
            e
    )

    session.rollback()
    throw e
}