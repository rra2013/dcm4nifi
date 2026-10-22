import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.model.v25.message.ORM_O01
import ca.uhn.hl7v2.parser.PipeParser
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (flowFile == null) {
    return
}

// --------------------------------------------------
// 1. Read and parse HL7 ORM^O01
// --------------------------------------------------

def context = new DefaultHapiContext()

try {
    context.getParserConfiguration().setValidating(false)

    String hl7 = ""

    session.read(flowFile).withCloseable { input ->
        hl7 = input.getText("UTF-8")
    }

    PipeParser parser = context.getPipeParser()
    def parsed = parser.parse(hl7)

    if (!(parsed instanceof ORM_O01)) {
        throw new IllegalArgumentException(
                "Expected HL7 v2.5 ORM^O01 message"
        )
    }

    ORM_O01 message = (ORM_O01) parsed

    // Validate HL7 version (MSH-12)
    String hl7Version = message.getMSH()
            .getVersionID()
            .getVersionID()
            .getValue()

    if (hl7Version != "2.5") {
        throw new IllegalArgumentException(
                "Unsupported HL7 version: ${hl7Version}. Expected 2.5"
        )
    }

    // --------------------------------------------------
    // 2. Extract message-level information
    // --------------------------------------------------

    String messageControlId =
            message.getMSH()
                    .getMessageControlID()
                    .getValue()

    String patientId =
            message.getPATIENT()
                    .getPID()
                    .getPatientIdentifierList(0)
                    .getIDNumber()
                    .getValue()

    // --------------------------------------------------
    // 3. Prepare all output items before writing
    // --------------------------------------------------

    def items = []

    for (int i = 0; i < message.getORDERReps(); i++) {

        def order = message.getORDER(i)
        def detail = order.getORDER_DETAIL()

        String studyOrderId =
                order.getORC()
                        .getPlacerOrderNumber()
                        .getEntityIdentifier()
                        .getValue()

        // Extract observations by identifier.
        def observations = [:].withDefault { [] }

        for (int j = 0;
             j < detail.getOBSERVATIONReps();
             j++) {

            def obx = detail.getOBSERVATION(j).getOBX()

            String code =
                    obx.getObservationIdentifier()
                            .getIdentifier()
                            .getValue()

            String value =
                    obx.getObservationValue(0)
                            .getData()
                            .encode()

            observations[code] << value
        }

        def single = { String key ->
            def values = observations[key]

            if (values.size() != 1 ||
                    !values[0]?.trim()) {
                throw new IllegalArgumentException(
                        "Expected exactly one value for ${key}"
                )
            }

            values[0]
        }

        String orderId = single("ORDER_ID")
        String projectId = single("PROJECT_ID")
        String pseudonymPid = single("PSEUDONYM_PID")
        String pseudonymName = single("PSEUDONYM_NAME")
        String sourcePacs = single("SOURCE_PACS")
        String destinationAet = single("DEST_AET")
        String studyUid = single("STUDY_UID")

        def seriesUids = observations["SERIES_UID"]

        if (seriesUids.isEmpty()) {
            throw new IllegalArgumentException(
                    "No SERIES_UID provided for ${studyUid}"
            )
        }

        // Attributes shared by generated FlowFiles.
        def baseAttributes = [
                "dcma.order.id"          : orderId,
                "dcma.order.study.id"    : studyOrderId,
                "dcma.hl7.message.id"    : messageControlId,
                "dcma.project.id"        : projectId,
                "dcma.patient.id"        : patientId,
                "dcma.pseudonym.pid"     : pseudonymPid,
                "dcma.pseudonym.name"    : pseudonymName,
                "dcma.source.pacs"       : sourcePacs,
                "dcma.destination.aet"   : destinationAet,
                "dcma.study.uid"         : studyUid,
                "mime.type"              : "text/csv"
        ]

        // One FlowFile per Study/Series combination.
        for (String seriesUid : seriesUids) {

                if (!seriesUid?.trim()) {
                    throw new IllegalArgumentException(
                            "Empty SERIES_UID for ${studyUid}"
                    )
                }

                items << [
                        content    : "${studyUid},${seriesUid}",
                        attributes : baseAttributes + [
                                "dcma.series.uid" : seriesUid
                        ]
                ]
            }

    }

    if (items.isEmpty()) {
        throw new IllegalArgumentException(
                "No DICOM studies in ORM message"
        )
    }

    // --------------------------------------------------
    // 4. Create NiFi FlowFiles
    // --------------------------------------------------

    for (def item : items) {

        def child = session.create(flowFile)

        byte[] content =
                item.content.getBytes(StandardCharsets.UTF_8)

        child = session.write(
                child,
                { output ->
                    output.write(content)
                } as OutputStreamCallback
        )

        child = session.putAllAttributes(
                child,
                item.attributes
        )

        session.transfer(child, REL_SUCCESS)
    }

    // Remove original HL7 FlowFile after splitting.
    session.remove(flowFile)

} catch (Exception e) {

    log.error(
            "Failed to process HL7 ORM message",
            e
    )

    // Roll back the entire session, including any
    // generated FlowFiles, to avoid partial output.
    session.rollback()

    throw e

} finally {
    context.close()
}
