package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.SafeClose;

import java.io.*;

public class DicomUtils {
    public static void copyAttributesToOutput(Attributes attributes, BufferedOutputStream outputStream) {
        try {
            DicomOutputStream out = new DicomOutputStream(outputStream, UID.ExplicitVRLittleEndian);
            try {
                attributes.writeTo(out);
            } finally {
                SafeClose.close(out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static Attributes byteArrayToAttributes(byte[] readAnonym) {
        Attributes dcm = new Attributes();
        try (ByteArrayInputStream ba = new ByteArrayInputStream(readAnonym)) {
            try (BufferedInputStream bif = new BufferedInputStream(ba)) {
                DicomDataReader data = new DicomDataReader(bif, true);
                dcm = data.getAttributes();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dcm;
    }
    public static Attributes streamToAttributes(InputStream input) {
        Attributes dcm = new Attributes();
        try(BufferedInputStream bis = new BufferedInputStream(input)){
            dcm = byteArrayToAttributes( bis.readAllBytes() );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dcm;
    }
    public static byte[] attributesToByteArrayEVRLE(Attributes dcm, Attributes fmi) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (DicomOutputStream dos = new DicomOutputStream(baos,UID.ExplicitVRLittleEndian)) {
                dos.setEncodingOptions(DicomEncodingOptions.DEFAULT);
                dos.writeDataset(fmi, dcm);
            }
            return baos.toByteArray();
        }
    }
    public static Attributes readDicomObjectUntilPixelData(InputStream in) throws IOException {
        DicomInputStream din = new DicomInputStream(in);
        return din.readDatasetUntilPixelData();
    }
}
