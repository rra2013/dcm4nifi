/*
 * assign_zip_fragment.groovy
 *
 * Input:  pseudonymized DICOM FlowFile with the ZIP manifest entry of this
 *         instance in dcma.manifest.entry (Get ZIP Manifest Entry).
 * Output: success - fragment.identifier/index/count and filename set for
 *                   MergeContent (Create Series ZIP, Defragment)
 *         failure - entry does not match the DICOM file; attributes
 *                   dcma.zip.error.code / dcma.zip.error.message are set
 *
 * Errors are routed to failure directly, without session.rollback():
 * after a rollback the FlowFile goes back to the input queue and is
 * processed again immediately, endlessly. The failure relationship must
 * be connected (not auto-terminated); the series ZIP is incomplete then
 * and Create Series ZIP routes the other fragments to failure after its
 * Max Bin Age.
 */
import groovy.json.JsonSlurper

def flowFile = session.get()
if (flowFile == null) return

String errorCode = null
String errorMessage = null
Map<String, String> attributes = null

try {
    String json = flowFile.getAttribute("dcma.manifest.entry")

    if (!json?.trim()) {
        throw new IllegalArgumentException(
                "Missing dcma.manifest.entry"
        )
    }

    def entry = new JsonSlurper().parseText(json)

    String originalUid =
            flowFile.getAttribute("dcma.original.sop.uid")

    String pseudonymUid =
            flowFile.getAttribute("AffectedSOPInstanceUID")

    String group =
            flowFile.getAttribute("dcma.zip.group")

    if (!originalUid?.trim()
            || !pseudonymUid?.trim()
            || !group?.trim()) {
        throw new IllegalArgumentException(
                "Missing DICOM ZIP correlation attributes"
        )
    }

    if (entry.originalSopInstanceUid != originalUid) {
        throw new IllegalArgumentException(
                "Original SOP Instance UID mismatch"
        )
    }

    if (entry.pseudonymSopInstanceUid != pseudonymUid) {
        throw new IllegalArgumentException(
                "Pseudonym SOP Instance UID mismatch"
        )
    }

    if (entry.fragmentIndex == null || entry.fragmentCount == null) {
        throw new IllegalArgumentException(
                "Missing ZIP fragment index/count"
        )
    }

    int index = entry.fragmentIndex as int
    int count = entry.fragmentCount as int

    if (count <= 0 || index < 0 || index >= count) {
        throw new IllegalArgumentException(
                "Invalid ZIP fragment index/count"
        )
    }

    String filename = entry.zipEntryName?.toString()

    if (filename != pseudonymUid + ".dcm") {
        throw new IllegalArgumentException(
                "ZIP filename does not match pseudonym SOP UID"
        )
    }

    attributes = [
            "fragment.identifier": group,
            "fragment.index"     : Integer.toString(index),
            "fragment.count"     : Integer.toString(count),
            "filename"           : filename
    ]

} catch (Exception e) {
    // Invalid JSON, missing attributes or mismatching manifest entry.
    errorCode = "ZIP_FRAGMENT_INVALID"
    errorMessage = (e.message ?: e.toString()).take(2000)
}

if (attributes != null) {
    flowFile = session.putAllAttributes(flowFile, attributes)
    flowFile = session.removeAttribute(flowFile, "dcma.manifest.entry")
    session.transfer(flowFile, REL_SUCCESS)
    return
}

log.error("Failed to assign ZIP fragment: orderId={}, retrieveId={}: {}",
        [flowFile.getAttribute("dcma.order.id"),
         flowFile.getAttribute("dcma.retrieve.id"),
         errorMessage] as Object[])

// dcma.manifest.entry stays on the FlowFile for analysis.
flowFile = session.putAllAttributes(flowFile, [
        "dcma.zip.error.code"   : errorCode,
        "dcma.zip.error.message": errorMessage
])

session.transfer(flowFile, REL_FAILURE)
