/*
 * pseudonym_instance_failed.groovy
 *
 * Input:  DICOM-FlowFile aus einem Fehlerpfad der Instanzverarbeitung:
 *           - MaskMultiFrameUS  "failure"      (Maskierung fehlgeschlagen)
 *           - MaskMultiFrameUS  "unsupported"  (z. B. US-Multiframe mit 1 Frame)
 *           - Pseudonymize DICOM Data "failure"
 *           - Instance Complete "failure"
 * Output: INSTANCE_FAILED-Event als JSON für den bestehenden Batch-Pfad
 *         (MergeRecord -> Instance Batch Wrapper -> instances/events/batch).
 *
 * Das Backend setzt die Instanz damit auf FAILED; Serie und Auftrag können
 * abgeschlossen werden statt dauerhaft offen zu bleiben.
 *
 * Die DICOM-Datei selbst wird entfernt: sie ist ggf. nicht maskiert bzw.
 * nicht pseudonymisiert und darf weder in pseudo_out noch in die ZIP-
 * Verpackung gelangen. Soll sie zur Analyse erhalten bleiben, die
 * Fehler-Relationship im Flow zusätzlich mit einem Port verbinden
 * (NiFi klont das FlowFile dann für beide Verbindungen).
 *
 * Fehlerquelle und errorCode werden aus den Attributen abgeleitet, die der
 * jeweilige Processor setzt. Die Fehlertexte stammen aus den Processoren
 * (technische Meldungen) und werden auf 1000 Zeichen gekürzt; sie sollten
 * keine Patientendaten enthalten.
 */
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets
import java.util.UUID

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

// Vor der Pseudonymisierung ist AffectedSOPInstanceUID die Original-UID;
// danach steht sie in dcma.original.sop.uid (Remember SOP Instance UID).
String originalSopUid = optional("dcma.original.sop.uid")
        ?: required("AffectedSOPInstanceUID")

// --------------------------------------------------
// Fehlerquelle bestimmen
// --------------------------------------------------

String errorCode
String errorMessage

String storeErrorCode = optional("dicom.store.error.code")
String maskUnsupported = optional("maskmultiframeus.unsupported.reason")
String maskError = optional("maskmultiframeus.error")
String pseudonymizerError = optional("pseudonymizer.error.message")

if (storeErrorCode != null) {
    // StoreScu "failure" (Lieferung per DICOM C-STORE): Instanz ist
    // pseudonymisiert, aber beim Store-SCP nicht angekommen.
    String target = optional("dcma.delivery.aet")
    String storeStatus = optional("dicom.store.status")
    String storeError = optional("dicom.store.error")
    errorCode = "STORE_FAILED"
    errorMessage = "DICOM C-STORE" + (target ? " an " + target : "") + " fehlgeschlagen (" +
            storeErrorCode + (storeStatus ? ", Status " + storeStatus : "") + ")" +
            (storeError ? ": " + storeError : "")
} else if (maskUnsupported != null) {
    // Nicht maskierbar (z. B. Single-Frame): darf nicht unmaskiert weiterlaufen.
    errorCode = "MASK_UNSUPPORTED"
    errorMessage = "Pixelmaskierung nicht anwendbar: " + maskUnsupported
} else if (maskError != null) {
    errorCode = "MASK_FAILED"
    errorMessage = "Pixelmaskierung fehlgeschlagen: " + maskError
} else if (pseudonymizerError != null) {
    errorCode = "PSEUDONYMIZE_FAILED"
    errorMessage = "Pseudonymisierung fehlgeschlagen: " + pseudonymizerError
} else {
    // z. B. Instance Complete "failure": ExecuteGroovyScript setzt ERROR_MESSAGE
    String scriptError = optional("ERROR_MESSAGE")
    errorCode = "PROCESSING_FAILED"
    errorMessage = "Instanzverarbeitung fehlgeschlagen" + (scriptError ? ": " + scriptError : "")
}

if (errorMessage.length() > 1000) {
    errorMessage = errorMessage.substring(0, 1000)
}

// --------------------------------------------------
// INSTANCE_FAILED-Event erzeugen
// --------------------------------------------------

String eventId = UUID.randomUUID().toString()

def event = [
        eventId                : eventId,
        eventType              : "INSTANCE_FAILED",
        retrieveId             : retrieveId,
        studyInstanceUid       : studyUid,
        seriesInstanceUid      : seriesUid,
        originalSopInstanceUid : originalSopUid,
        pseudonymSopInstanceUid: null,
        errorCode              : errorCode,
        errorMessage           : errorMessage
]

byte[] json = JsonOutput.toJson(event).getBytes(StandardCharsets.UTF_8)

def callback = session.create(flowFile)

callback = session.write(callback, { output ->
    output.write(json)
} as OutputStreamCallback)

callback = session.putAllAttributes(callback, [
        "mime.type"            : "application/json",
        "dcma.callback.type"   : "instance-event",
        "dcma.event.id"        : eventId,
        "dcma.event.type"      : "INSTANCE_FAILED",
        "dcma.batch.group"     : orderId + ":" + retrieveId,
        "dcma.instance.error.code": errorCode
])

log.warn("Instance failed: orderId={}, retrieveId={}, errorCode={}, error={}",
        [orderId, retrieveId, errorCode, errorMessage] as Object[])

session.transfer(callback, REL_SUCCESS)

// Nicht maskierte / nicht pseudonymisierte DICOM-Datei verwerfen.
session.remove(flowFile)

// Fehlende Korrelationsattribute werfen eine Exception. Ohne eigenes
// session.rollback(): ExecuteGroovyScript ("transfer to failure") verwirft
// dann das erzeugte Callback und leitet das Original nach "failure".
