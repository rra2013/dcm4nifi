
import groovy.json.JsonSlurper

def flowFile = session.get()
if (flowFile == null) return

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

    Map<String, String> attributes = [
            "fragment.identifier": group,
            "fragment.index"     : Integer.toString(index),
            "fragment.count"     : Integer.toString(count),
            "filename"           : filename
    ]

    flowFile = session.putAllAttributes(
            flowFile,
            attributes
    )

    flowFile = session.removeAttribute(
            flowFile,
            "dcma.manifest.entry"
    )

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error(
            "Failed to assign ZIP fragment: " + e.message,
            e
    )

    session.rollback()
    throw e
}
