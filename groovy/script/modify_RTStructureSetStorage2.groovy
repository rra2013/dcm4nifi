/*
Copyright Reza Rastégar (reza.rastegar@insel.ch) 2026

Das Script ersetzt eine SeriesInstanceUID durch eine neuen Wert und fügt diesen Wert standardkonform 
innerhalb der DICOM-RT-Referenzstruktur, ohne die eigentlichen Bild- oder Pixelinformationen zu verändern.
Die Implementierung basiert auf dcm4che Version 5.34.1 und berücksichtigt die DICOM-Spezifikation hinsichtlich 
Transfer Syntax, File Meta Information und BulkData-Behandlung.
Insbesondere wird sichergestellt, dass PixelData unverändert erhalten bleibt, 
während ausschließlich die adressierte Metadaten-Struktur aktualisiert wird.
*/
import org.apache.nifi.processor.io.StreamCallback
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR
import org.dcm4che3.data.UID
import org.dcm4che3.util.UIDUtils
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomOutputStream

def flowFile = session.get()
if (!flowFile) return

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
        // Read the sequence of RT
        def rforSeq = ds.getSequence(Tag.ReferencedFrameOfReferenceSequence)
        if (rforSeq == null || rforSeq.isEmpty()) {
            throw new IllegalStateException("ReferencedFrameOfReferenceSequence fehlt/leer")
        }
        def rforItem = rforSeq.get(0)  // Attributes
        def rtRefStudySeq = rforItem.getSequence(Tag.RTReferencedStudySequence)
        if (rtRefStudySeq == null || rtRefStudySeq.isEmpty()) {
            throw new IllegalStateException("RTReferencedStudySequence fehlt/leer")
        }
        def rtRefStudyItem = rtRefStudySeq.get(0)
        def rtRefSeriesSeq = rtRefStudyItem.getSequence(Tag.RTReferencedSeriesSequence)
        if (rtRefSeriesSeq == null || rtRefSeriesSeq.isEmpty()) {
            throw new IllegalStateException("RTReferencedSeriesSequence fehlt/leer")
        }
        def rtRefSeriesItem = rtRefSeriesSeq.get(0)
        // SeriesInstanceUID in der Sequence
        def seriesUID = rtRefSeriesItem.getString(Tag.SeriesInstanceUID);
        def remapSeriesUID = UIDUtils.remapUID(seriesUID)
        rtRefSeriesItem.setString(Tag.SeriesInstanceUID, VR.UI, remapSeriesUID)
        log.info("Sequence SeriesInstanceUID gesetzt von {} auf {}", seriesUID, remapSeriesUID)
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

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    log.error("Fehler beim Setzen der SeriesInstanceUID im DICOM-Body {}", e.getMessage())
    session.transfer(flowFile, REL_FAILURE)
}
