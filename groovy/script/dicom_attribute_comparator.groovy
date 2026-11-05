import org.dcm4che3.io.DicomInputStream
import org.dcm4che3.data.Tag
/*
    Only thin slices will be successful
 */

def flowFile = session.get()
if (!flowFile) return

try {
    def sliceThickness

    session.read(flowFile) { inputStream ->
        DicomInputStream din = new DicomInputStream(inputStream)
        def dataset = din.readDatasetUntilPixelData()
        din.close()
        sliceThickness = dataset.getFloat(Tag.SliceThickness, 0.0)
    }

    log.info("SliceThickness: {}", sliceThickness)

    if (sliceThickness<=0.0f) {
        session.transfer(flowFile, REL_FAILURE)
    }else if (sliceThickness<=1.0f){
        session.transfer(flowFile, REL_SUCCESS)
    }else{
        session.transfer(flowFile, REL_FAILURE)
    }

} catch (Exception e) {
    log.error("Fehler beim Auslesen von SliceThickness", e)
    session.transfer(flowFile, REL_FAILURE)
}
