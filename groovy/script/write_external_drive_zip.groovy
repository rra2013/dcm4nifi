/*
 * write_external_drive_zip.groovy
 *
 * PERSIST_ORDER_FILES: schreibt ein Serien-ZIP auf einen externen Datenträger
 * (dcma-order Lieferart DIRECTORY), der auf der NiFi-VM unter einem
 * Mount-Point eingehängt ist, z. B. /mnt/DLF090902343424.
 *
 * Input:  das Serien-ZIP mit
 *           dcma.order.id, dcma.retrieve.id  (wie für zip-stored/zip-failure)
 *           dcma.zip.target                  Antwort von
 *               GET #{dcma.api.url}/orders/${dcma.order.id}/retrievals/${dcma.retrieve.id}/zip-target
 *               (InvokeHTTP "Response Body Attribute Name" = dcma.zip.target)
 *           dcma.external.root               Wurzel der Mount-Points, z. B. #{dcma.external.root}
 *                                            (UpdateAttribute); ohne Angabe /mnt
 *
 * Output (success):
 *   DOWNLOAD:   unverändert, dcma.zip.delivery=DOWNLOAD -> bisheriger Weg ins Portal
 *   DIRECTORY:  ZIP geschrieben nach
 *                 <root>/<directory>/<folder>/<subfolder>/<fileName>
 *               dcma.zip.delivery=DIRECTORY, dcma.zip.path, dcma.zip.size.bytes
 *               -> POST .../zip-stored?sizeBytes=${dcma.zip.size.bytes}
 * Output (failure): dcma.zip.error.code und dcma.zip.error.message
 *               -> zip_failure_callback.groovy -> POST .../zip-failure
 *   EXTERNAL_DRIVE_UNAVAILABLE    Datenträger nicht eingehängt, fehlt oder ist ein Link
 *   EXTERNAL_DRIVE_FULL           zu wenig freier Platz
 *   EXTERNAL_DRIVE_WRITE_FAILED   Schreiben fehlgeschlagen (Rechte, E/A-Fehler)
 *   EXTERNAL_DRIVE_INVALID_TARGET Ziel fehlt, ist kein gültiges JSON oder passt nicht
 *
 * Schutz der VM-Platte: Geschrieben wird nur, wenn der Mount-Point ein anderes
 * Gerät (unix:dev) hat als die Wurzel; ein leerer Ordner /mnt/xyz ohne
 * Datenträger liegt auf der VM und wird abgelehnt. In die Wurzel selbst wird
 * nie geschrieben, der Mount-Point wird nie angelegt. Ordner darunter werden
 * einzeln angelegt und jeweils geprüft (kein Link, gleiches Gerät wie der
 * Datenträger). Das ZIP entsteht als versteckte .part-Datei, wird auf den
 * Datenträger geschrieben (fsync), die Prüfung wiederholt und erst dann
 * umbenannt. Alle Namen werden hier noch einmal geprüft: kein '/', kein '..'.
 *
 * Meldungen und Logs enthalten nur Namen und Codes, keine Patientendaten.
 */
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.InputStreamCallback

import java.nio.channels.FileChannel
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption

def flowFile = session.get()
if (flowFile == null) {
    return
}

class DriveException extends Exception {
    final String code

    DriveException(String code, String message) {
        super(message)
        this.code = code
    }
}

final String DRIVE = /[A-Za-z0-9][A-Za-z0-9._-]{0,63}/
final String SEGMENT = /[A-Za-z0-9]([A-Za-z0-9._-]{0,98}[A-Za-z0-9_-])?/
final String WINDOWS_RESERVED = /(?i)(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?/
final String UUID_PATTERN = /[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/
// Platz über der ZIP-Grösse, damit der Datenträger nicht randvoll wird
final long RESERVE_BYTES = 16L * 1024 * 1024

def fail = { String code, String message ->
    throw new DriveException(code, message)
}

def invalid = { String message -> fail("EXTERNAL_DRIVE_INVALID_TARGET", message) }

def name = { Object value, String pattern, String what ->
    String text = value instanceof String ? (String) value : null
    if (text == null || !(text ==~ pattern) || text.contains("..") || text ==~ WINDOWS_RESERVED) {
        invalid("Invalid ${what} in zip-target")
    }
    text
}

def device = { Path path ->
    Files.getAttribute(path, "unix:dev", LinkOption.NOFOLLOW_LINKS)
}

/** Ordner direkt unter parent anlegen bzw. prüfen: kein Link, auf dem Datenträger. */
def directoryOnDrive = { Path parent, String child, Object driveDevice ->
    Path dir = parent.resolve(child)
    if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
        try {
            Files.createDirectory(dir)
        } catch (java.nio.file.FileAlreadyExistsException ignored) {
            // parallel angelegt, wird unten geprüft
        }
    }
    if (Files.isSymbolicLink(dir) || !Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
        fail("EXTERNAL_DRIVE_WRITE_FAILED", "${dir} is not a directory on the external drive")
    }
    if (device(dir) != driveDevice) {
        fail("EXTERNAL_DRIVE_UNAVAILABLE", "${dir} is not on the external drive")
    }
    dir
}

Path part = null
try {
    String orderId = flowFile.getAttribute("dcma.order.id")?.trim()
    String retrieveId = flowFile.getAttribute("dcma.retrieve.id")?.trim()
    if (!(orderId ==~ UUID_PATTERN) || !(retrieveId ==~ UUID_PATTERN)) {
        invalid("Missing or invalid dcma.order.id / dcma.retrieve.id")
    }

    String json = flowFile.getAttribute("dcma.zip.target")
    if (!json?.trim()) {
        invalid("Missing dcma.zip.target (response of GET .../zip-target)")
    }
    def target
    try {
        target = new JsonSlurper().parseText(json)
    } catch (Exception e) {
        invalid("dcma.zip.target is not valid JSON")
    }
    if (!(target instanceof Map)) {
        invalid("dcma.zip.target is not a JSON object")
    }
    if (!orderId.equalsIgnoreCase(String.valueOf(target.orderId))
            || !retrieveId.equalsIgnoreCase(String.valueOf(target.retrieveId))) {
        invalid("zip-target belongs to another order or retrieval")
    }

    String type = target.deliveryType
    if (type == "DOWNLOAD") {
        flowFile = session.putAttribute(flowFile, "dcma.zip.delivery", "DOWNLOAD")
        session.transfer(flowFile, REL_SUCCESS)
        return
    }
    if (type != "DIRECTORY") {
        invalid("Unsupported deliveryType in zip-target: ${type}")
    }

    String directory = name(target.directory, DRIVE, "directory")
    String folder = name(target.folder, SEGMENT, "folder")
    String subfolder = name(target.subfolder, SEGMENT, "subfolder")
    String fileName = target.fileName instanceof String ? target.fileName : null
    if (!(fileName ==~ (UUID_PATTERN + /\.zip/)) || !fileName.equalsIgnoreCase(retrieveId + ".zip")) {
        invalid("Invalid fileName in zip-target")
    }

    // ------------------------------------------------------------------
    // Datenträger prüfen: Wurzel vorhanden, Mount-Point ein eigenes Gerät
    // ------------------------------------------------------------------
    String rootText = flowFile.getAttribute("dcma.external.root")?.trim() ?: "/mnt"
    Path root
    try {
        root = Paths.get(rootText).toRealPath()
    } catch (Exception e) {
        fail("EXTERNAL_DRIVE_UNAVAILABLE", "External drive root ${rootText} not found")
    }
    if (!Files.isDirectory(root)) {
        fail("EXTERNAL_DRIVE_UNAVAILABLE", "External drive root ${root} is not a directory")
    }

    Path drive = root.resolve(directory)
    if (drive.getParent() != root) {
        invalid("directory is not directly below the root")
    }

    def checkMounted = {
        if (!Files.exists(drive, LinkOption.NOFOLLOW_LINKS)) {
            fail("EXTERNAL_DRIVE_UNAVAILABLE", "External drive ${directory} not found under ${root}")
        }
        if (Files.isSymbolicLink(drive) || !Files.isDirectory(drive, LinkOption.NOFOLLOW_LINKS)) {
            fail("EXTERNAL_DRIVE_UNAVAILABLE", "External drive ${directory} is not a mount point directory")
        }
        def driveDevice = device(drive)
        if (driveDevice == device(root)) {
            fail("EXTERNAL_DRIVE_UNAVAILABLE",
                    "External drive ${directory} is not mounted (${drive} is on the VM disk)")
        }
        driveDevice
    }

    def driveDevice = checkMounted()

    long size = flowFile.getSize()
    long usable = Files.getFileStore(drive).getUsableSpace()
    if (usable < size + RESERVE_BYTES) {
        fail("EXTERNAL_DRIVE_FULL",
                "External drive ${directory}: ${usable} bytes free, ZIP needs ${size} bytes")
    }

    // ------------------------------------------------------------------
    // Ordner unter dem Mount-Point, .part schreiben, prüfen, umbenennen
    // ------------------------------------------------------------------
    Path folderDir
    Path patientDir
    try {
        folderDir = directoryOnDrive(drive, folder, driveDevice)
        patientDir = directoryOnDrive(folderDir, subfolder, driveDevice)
    } catch (DriveException e) {
        throw e
    } catch (IOException e) {
        fail("EXTERNAL_DRIVE_WRITE_FAILED",
                "Directory on external drive ${directory} not created: ${e.class.simpleName}")
    }

    Path file = patientDir.resolve(fileName)
    part = patientDir.resolve("." + fileName + ".part")
    try {
        session.read(flowFile, { InputStream input ->
            Files.copy(input, part, StandardCopyOption.REPLACE_EXISTING)
        } as InputStreamCallback)
        FileChannel.open(part, StandardOpenOption.WRITE).withCloseable { it.force(true) }
    } catch (IOException e) {
        fail("EXTERNAL_DRIVE_WRITE_FAILED",
                "ZIP not written to external drive ${directory}: ${e.class.simpleName}")
    }

    long written = Files.size(part)
    if (written != size) {
        fail("EXTERNAL_DRIVE_WRITE_FAILED",
                "ZIP on external drive ${directory} has ${written} bytes, expected ${size}")
    }

    // Erneut prüfen: in der Zwischenzeit ausgehängt?
    if (checkMounted() != driveDevice || device(part) != driveDevice) {
        fail("EXTERNAL_DRIVE_UNAVAILABLE", "External drive ${directory} changed while writing")
    }

    try {
        Files.move(part, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    } catch (AtomicMoveNotSupportedException e) {
        Files.move(part, file, StandardCopyOption.REPLACE_EXISTING)
    }
    part = null

    flowFile = session.putAllAttributes(flowFile, [
            "dcma.zip.delivery"  : "DIRECTORY",
            "dcma.zip.path"      : file.toString(),
            "dcma.zip.size.bytes": String.valueOf(written)
    ])
    log.info("Series ZIP written to external drive {}: order {}, retrieve {}, {} bytes",
            [directory, orderId, retrieveId, written] as Object[])
    session.transfer(flowFile, REL_SUCCESS)

} catch (DriveException e) {
    if (part != null) {
        try { Files.deleteIfExists(part) } catch (IOException ignored) { }
    }
    log.error("Series ZIP not written to external drive: {} {}", [e.code, e.message] as Object[])
    flowFile = session.putAllAttributes(flowFile, [
            "dcma.zip.error.code"   : e.code,
            "dcma.zip.error.message": e.message.length() > 1000 ? e.message.substring(0, 1000) : e.message
    ])
    session.transfer(flowFile, REL_FAILURE)
} catch (Exception e) {
    // E/A-Fehler, unix:dev nicht verfügbar u. ä.: als Schreibfehler melden
    if (part != null) {
        try { Files.deleteIfExists(part) } catch (IOException ignored) { }
    }
    log.error("Series ZIP not written to external drive: {}", [e.class.simpleName] as Object[])
    flowFile = session.putAllAttributes(flowFile, [
            "dcma.zip.error.code"   : "EXTERNAL_DRIVE_WRITE_FAILED",
            "dcma.zip.error.message": "Writing to the external drive failed: " + e.class.simpleName
    ])
    session.transfer(flowFile, REL_FAILURE)
}
