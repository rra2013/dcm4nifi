package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.SafeClose;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;

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
}
