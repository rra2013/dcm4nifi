/*
 * validate_series_zip_file.groovy
 *
 * Prüft das Serien-ZIP gegen das Manifest des Backends im Attribut
 * dcma.zip.manifest.json. Zwei Formen:
 *   - Zusammenfassung (GET .../zip-manifest/summary, empfohlen):
 *     {orderId, retrieveId, fragmentCount, entryNamesSha256}
 *     entryNamesSha256 = SHA-256 (hex) der aufsteigend sortierten
 *     ZIP-Eintragsnamen, mit "\n" verbunden, UTF-8.
 *   - Vollständiges Manifest (GET .../zip-manifest) mit entries[]: nur für
 *     kleine Serien; bei vielen tausend Instanzen ist die Antwort grösser
 *     als die "Response Body Attribute Size" von InvokeHTTP und wird
 *     abgeschnitten.
 */
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.InputStreamCallback

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.zip.ZipInputStream
import java.util.zip.ZipException

def flowFile = session.get()
if (flowFile == null) return

try {
    String json = flowFile.getAttribute(
            "dcma.zip.manifest.json"
    )

    if (!json?.trim()) {
        throw new IllegalArgumentException(
                "Missing ZIP manifest JSON"
        )
    }

    def manifest
    try {
        manifest = new JsonSlurper().parseText(json)
    } catch (Exception parseError) {
        // Meist abgeschnittene Antwort (Response Body Attribute Size zu klein)
        throw new IllegalStateException(
                "ZIP manifest is not valid JSON (length ${json.length()}); " +
                        "use .../zip-manifest/summary or a larger Response Body Attribute Size")
    }

    String orderId = flowFile.getAttribute("dcma.order.id")
    String retrieveId = flowFile.getAttribute("dcma.retrieve.id")

    if (!orderId?.trim() || !retrieveId?.trim()) {
        throw new IllegalArgumentException(
                "Missing orderId or retrieveId"
        )
    }

    if (manifest.orderId != orderId ||
            manifest.retrieveId != retrieveId) {
        throw new IllegalStateException(
                "Manifest order/retrieval mismatch"
        )
    }

    int expectedCount = manifest.fragmentCount as int
    String expectedHash = manifest.entryNamesSha256?.toString()?.toLowerCase()
    Set<String> expectedNames = null

    if (expectedHash) {
        if (expectedCount <= 0 || !(expectedHash ==~ /[0-9a-f]{64}/)) {
            throw new IllegalStateException("Invalid ZIP manifest summary")
        }
    } else {
        if (!(manifest.entries instanceof List)) {
            throw new IllegalStateException(
                    "Missing manifest entries"
            )
        }

        expectedNames = manifest.entries.collect {
            it.zipEntryName.toString()
        }.toSet()

        if (expectedCount <= 0 ||
                manifest.entries.size() != expectedCount ||
                expectedNames.size() != expectedCount) {
            throw new IllegalStateException(
                    "Invalid ZIP manifest entries"
            )
        }
    }

    Set<String> actualNames = new HashSet<>()

    session.read(flowFile, { input ->

        ZipInputStream zip = new ZipInputStream(input)

        try {
            byte[] buffer = new byte[65536]
            def entry

            while ((entry = zip.nextEntry) != null) {

                if (entry.isDirectory()) {
                    throw new IllegalStateException(
                            "Unexpected ZIP directory: ${entry.name}"
                    )
                }

                String name = entry.name

                if (!actualNames.add(name)) {
                    throw new IllegalStateException(
                            "Duplicate ZIP entry: ${name}"
                    )
                }

                // Read the complete entry in streaming mode.
                while (zip.read(buffer) != -1) {
                    // Consumes content and checks ZIP entry integrity.
                }

                zip.closeEntry()
            }
        } finally {
            zip.close()
        }

    } as InputStreamCallback)

    if (actualNames.size() != expectedCount) {
        throw new IllegalStateException(
                "ZIP entry count mismatch: expected=" +
                        expectedCount + ", actual=" + actualNames.size()
        )
    }

    if (expectedHash) {
        String joined = actualNames.toList().sort().join("\n")
        String actualHash = MessageDigest.getInstance("SHA-256")
                .digest(joined.getBytes(StandardCharsets.UTF_8))
                .encodeHex().toString()
        if (actualHash != expectedHash) {
            throw new IllegalStateException(
                    "ZIP entry names do not match manifest (SHA-256 mismatch, ${actualNames.size()} entries)")
        }
    } else if (actualNames != expectedNames) {
        Set<String> missing = expectedNames - actualNames
        Set<String> unexpected = actualNames - expectedNames

        throw new IllegalStateException(
                "ZIP contents do not match manifest. " +
                        "Missing=${missing.take(20)}, Unexpected=${unexpected.take(20)}"
        )
    }

    // Successful validation
    flowFile = session.putAllAttributes(flowFile, [
            "dcma.zip.validated"   : "true",
            "dcma.zip.entry.count": Integer.toString(expectedCount),
            "mime.type"           : "application/zip"
    ])

    flowFile = session.removeAttribute(
            flowFile,
            "dcma.zip.manifest.json"
    )

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {

    log.error(
            "ZIP validation failed: " + e.message,
            e
    )

    // Preserve the original ZIP and attach error diagnostics.
    flowFile = session.putAllAttributes(flowFile, [
            "dcma.zip.validated"     : "false",
            "dcma.zip.error.code"    : "ZIP_VALIDATION_FAILED",
            "dcma.zip.error.message" :
                    (e.message ?: e.toString()).take(1000),
            "dcma.zip.error.class"   :
                    e.getClass().getName()
    ])

    session.transfer(flowFile, REL_FAILURE)
}
