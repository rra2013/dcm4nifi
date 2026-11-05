import org.apache.nifi.processor.io.StreamCallback
import org.dcm4che3.data.Attributes
import org.dcm4che3.data.Tag
import org.dcm4che3.data.VR
import org.dcm4che3.data.UID
import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.io.DicomOutputStream

def flowFile = session.get()
if (!flowFile) return

try {
    def seriesUID = flowFile.getAttribute("SeriesInstanceUID")
    if (!seriesUID) {
        log.error("Attribut 'SeriesInstanceUID' fehlt oder ist leer")
        session.transfer(flowFile, REL_FAILURE)
        return
    }
    log.info("Attribut SeriesInstanceUID = {}", seriesUID)
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
        rtRefSeriesItem.setString(Tag.SeriesInstanceUID, VR.UI, seriesUID)
        log.info("Sequence SeriesInstanceUID gesetzt auf {}", seriesUID)
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
