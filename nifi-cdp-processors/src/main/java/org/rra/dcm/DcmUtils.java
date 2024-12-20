package org.rra.dcm;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.SafeClose;

import java.io.BufferedOutputStream;

public class DcmUtils {
    public static void copyAttributesToOutput(Attributes attributes, BufferedOutputStream outputStream) {
        try {
            DicomOutputStream out = new DicomOutputStream(outputStream, UID.ExplicitVRLittleEndian);
            try {
                attributes.writeTo(out);
            } finally {
                SafeClose.close(out);
            }
        } catch (Exception e) {
        }
    }
}
