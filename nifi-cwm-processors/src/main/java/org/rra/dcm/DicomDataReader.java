package org.rra.dcm;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.*;
import org.dcm4che3.io.DicomInputStream;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class DicomDataReader {
    @Getter
    private final Attributes attributes;
    @Getter
    private final Attributes fmi;
    private final ElementDictionary dict = ElementDictionary.getStandardElementDictionary();

    public DicomDataReader(InputStream source, boolean readPixelData) throws IOException {
        DicomInputStream dis = new DicomInputStream(source);
        try {
            dis.setIncludeBulkData(DicomInputStream.IncludeBulkData.URI);
            fmi = dis.getFileMetaInformation();
            if ( readPixelData){
                attributes = dis.readDataset();
                VR.Holder vr = new VR.Holder();
                Object pixelData = attributes.getValue(Tag.PixelData, vr);

                if (pixelData instanceof byte[]) {
                    log.debug("Read Byte data");
                } else if (pixelData instanceof BulkData) {
                    log.debug("Read Bulk data");
                } else {
                    log.debug("Read Other data");
                }
            }else{
                attributes = dis.readDatasetUntilPixelData();
            }

            printDebugInfo();

        } finally {
            dis.close();
        }
    }

    private void printDebugInfo() {
        log.debug("+ + + FMI :{} ", fmi);
        printTag(Tag.PatientName);
        printTag(Tag.PatientID);
        printTag(Tag.IssuerOfPatientID);
        printTag(Tag.PatientSex);
        printTag(Tag.AccessionNumber);
        printTag(Tag.StudyDate);
        printTag(Tag.StudyDescription);
        printTag(Tag.SeriesDate);
        printTag(Tag.SeriesDescription);
        printTag(Tag.SOPInstanceUID);
        printTag(Tag.Modality);
        printTag(Tag.NumberOfFrames);
        printTag(Tag.MediaStorageSOPClassUID);
        printTag(Tag.SOPClassUID);
        printTag(Tag.PhotometricInterpretation);
        printTag(Tag.AcquisitionDate);
        printTag(Tag.AcquisitionTime);
        printTag(Tag.StudyID);
    }

    private void printTag(int tag) {
        log.debug("- "+
                fixedLengthString(dict.keywordOf(tag)+":\t")+
                attributes.getString(tag, "No_Value"));
    }

    public int getNumberOfFrames() {
        return attributes.getInt(Tag.NumberOfFrames, 1);
    }

    public String getModatity() {
        return attributes.getString(Tag.Modality, "No_Value");
    }

    public String getSOPInstanceUID() {
        return attributes.getString(Tag.SOPInstanceUID, "No_Value");
    }

    public String getSOPClassUID() {
        return attributes.getString(Tag.SOPClassUID, "No_Value");
    }

    private static String fixedLengthString(String string) {
        return fixedLengthString(string,25);
    }
    private static String fixedLengthString(String string, int length) {
        return String.format("%1$"+length+ "s", string);
    }
}