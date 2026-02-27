/*
Copyright Reza Rastégar (reza.rastegar@insel.ch) 2026

Das Script kopiert DICOM Attribute in die flow File Attribute
*/
import org.apache.nifi.processor.io.StreamCallback
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.dcm4che3.data.UID
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomOutputStream
import java.security.MessageDigest

def flowFile = session.get()
if (!flowFile) return

// Variablen im Outer Scope, damit wir sie nach session.write setzen können
String patientID = null
String hexSeriesUIDAttr = null
String hexStudyUIDAttr  = null
String seriesUID = null
String studyUID = null
String modality = null


try {
    flowFile = session.write(flowFile, { inputStream, outputStream ->
        def din = new DicomInputStream(inputStream)
        din.setIncludeBulkData(DicomInputStream.IncludeBulkData.YES)
        //
        Attributes fmi = din.readFileMetaInformation()
        Attributes ds  = din.readDataset()
        //
        String tsuid = fmi?.getString(Tag.TransferSyntaxUID)
        if (!tsuid) {
            tsuid = UID.ExplicitVRLittleEndian
        }
        din.close()

        // Lesen der Attribute
        patientID = ds.getString(Tag.PatientID)
        seriesUID = ds.getString(Tag.SeriesInstanceUID)
        studyUID = ds.getString(Tag.StudyInstanceUID)
        modality = ds.getString(Tag.Modality)

        def md = MessageDigest.getInstance("MD5")
        def seriesUIDhash = md.digest(seriesUID.bytes)
        def studyUIDhash = md.digest(studyUID.bytes)

        hexSeriesUIDAttr = seriesUIDhash.collect { String.format("%02x", it) }.join()
        hexStudyUIDAttr = studyUIDhash.collect { String.format("%02x", it) }.join()

        if (fmi == null) {
            fmi = ds.createFileMetaInformation(tsuid.toString())
        }
        OutputStream out = (OutputStream) outputStream

        // avoid closing NiFi-OutputStream
        def shield = new FilterOutputStream(out) {
            @Override
            void close() { flush() }
        }
        // FMI always in Explicit VR Little Endian
        def doutFmi = new DicomOutputStream(shield, UID.ExplicitVRLittleEndian)
        doutFmi.writeFileMetaInformation(fmi)
        doutFmi.close()   // close shield (flush), not out
        // Dataset with Original TransferSyntax
        String ts = tsuid.toString()
        def doutDs = new DicomOutputStream(shield, ts)
        doutDs.writeDataset(null, ds)
        doutDs.close()
    } as StreamCallback)

    // Attribute setzen (nach session.write)
    if (patientID != null) {
        flowFile = session.putAttribute(flowFile, "PatientID", patientID)
    }
    if (studyUID != null) {
        flowFile = session.putAttribute(flowFile, "StudyInstanceUID", studyUID)
    }
    if (seriesUID != null) {
        flowFile = session.putAttribute(flowFile, "SeriesInstanceUID", seriesUID)
    }
    // Hash Werte
    if (hexSeriesUIDAttr != null) {
        flowFile = session.putAttribute(flowFile, "HexSeriesIUID", hexSeriesUIDAttr)
    }
    if (hexStudyUIDAttr != null) {
        flowFile = session.putAttribute(flowFile, "HexStudyIUID", hexStudyUIDAttr)
    }
    // Modality
    if (modality != null){
        flowFile = session.putAttribute(flowFile, "Modality", modality)
    }

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("Fehler beim Setzen der SeriesInstanceUID im DICOM-Body {}", e.getMessage())
    session.transfer(flowFile, REL_FAILURE)
}
