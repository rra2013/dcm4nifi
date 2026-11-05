
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

        value.trim()
    }

    // --------------------------------------------------
    // 1. Order and retrieval information
    // --------------------------------------------------

    String orderId = required("dcma.order.id")
    String retrieveId = required("dcma.retrieve.id")

    // Original study and series UIDs from HL7 order.
    String studyUid = required("dcma.study.uid")
    String seriesUid = required("dcma.series.uid")

    // --------------------------------------------------
    // 2. Original and pseudonymized SOP Instance UIDs
    // --------------------------------------------------

    String originalSopUid =
            required("dcma.original.sop.uid")

    String pseudonymSopUid =
            required("AffectedSOPInstanceUID")

    // --------------------------------------------------
    // 3. Generate instance callback
    // --------------------------------------------------

    String eventId = UUID.randomUUID().toString()

    def event = [
            eventId                : eventId,
            eventType              : "INSTANCE_COMPLETED",
            retrieveId             : retrieveId,
            studyInstanceUid       : studyUid,
            seriesInstanceUid      : seriesUid,
            originalSopInstanceUid : originalSopUid,
            pseudonymSopInstanceUid: pseudonymSopUid
    ]

    byte[] json = JsonOutput.toJson(event)
            .getBytes(StandardCharsets.UTF_8)

    // --------------------------------------------------
    // 4. Create separate callback FlowFile
    // --------------------------------------------------

    def callback = session.create(flowFile)

    callback = session.write(
            callback,
            { output ->
                output.write(json)
            } as OutputStreamCallback
    )

    String batchGroup = orderId + ":" + retrieveId

    Map<String, String> attributes = [
            "mime.type"         : "application/json",
            "dcma.callback.type": "instance-event",
            "dcma.event.id"     : eventId,
            "dcma.event.type"   : "INSTANCE_COMPLETED",
            "dcma.batch.group" : batchGroup
    ]

    callback = session.putAllAttributes(callback, attributes)

    // --------------------------------------------------
    // 5. Route callback and DICOM separately
    // --------------------------------------------------

    session.transfer(callback, REL_SUCCESS)

    flowFile = session.putAttribute(
            flowFile,
            "dcma.callback.type",
            "dicom"
    )

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("Failed to create INSTANCE_COMPLETED callback", e)
    session.rollback()
    throw e
}
