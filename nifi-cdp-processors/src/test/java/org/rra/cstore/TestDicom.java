package org.rra.cstore;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.util.UIDUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Small synthetic CT instances (no patient data) for C-STORE tests. */
public final class TestDicom {

    private TestDicom() {
    }

    public record Instance(String iuid, byte[] bytes) {}

    public static Instance ct(int pixelBytes) throws IOException {
        return part10(UID.CTImageStorage, UID.ExplicitVRLittleEndian, pixelBytes);
    }

    /**
     * A Part-10 file with the given SOP class and the transfer syntax written
     * into the file meta information. The data set itself is always encoded
     * explicit VR little endian; for tests that never reach the SCP (rejected
     * transfer syntax) only the header matters.
     */
    public static Instance part10(String cuid, String tsuid, int pixelBytes) throws IOException {
        String iuid = UIDUtils.createUID();
        Attributes ds = new Attributes();
        ds.setString(Tag.SOPClassUID, VR.UI, cuid);
        ds.setString(Tag.SOPInstanceUID, VR.UI, iuid);
        ds.setString(Tag.StudyInstanceUID, VR.UI, "2.25.1");
        ds.setString(Tag.SeriesInstanceUID, VR.UI, "2.25.1.1");
        ds.setString(Tag.PatientID, VR.LO, "TEST-PSEUDO");
        ds.setString(Tag.Modality, VR.CS, "CT");
        ds.setInt(Tag.Rows, VR.US, 1);
        ds.setInt(Tag.Columns, VR.US, Math.max(1, pixelBytes / 2));
        ds.setInt(Tag.BitsAllocated, VR.US, 16);
        ds.setInt(Tag.BitsStored, VR.US, 12);
        ds.setInt(Tag.HighBit, VR.US, 11);
        ds.setInt(Tag.PixelRepresentation, VR.US, 0);
        ds.setInt(Tag.SamplesPerPixel, VR.US, 1);
        ds.setString(Tag.PhotometricInterpretation, VR.CS, "MONOCHROME2");
        ds.setBytes(Tag.PixelData, VR.OW, new byte[Math.max(2, pixelBytes)]);

        Attributes fmi = ds.createFileMetaInformation(UID.ExplicitVRLittleEndian);
        fmi.setString(Tag.TransferSyntaxUID, VR.UI, tsuid);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DicomOutputStream dos = new DicomOutputStream(out, UID.ExplicitVRLittleEndian)) {
            dos.writeDataset(fmi, ds);
        }
        return new Instance(iuid, out.toByteArray());
    }
}
