package org.rra.cstore;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Fragments;
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
        Attributes ds = dataSet(cuid, iuid, pixelBytes);

        Attributes fmi = ds.createFileMetaInformation(UID.ExplicitVRLittleEndian);
        fmi.setString(Tag.TransferSyntaxUID, VR.UI, tsuid);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DicomOutputStream dos = new DicomOutputStream(out, UID.ExplicitVRLittleEndian)) {
            dos.writeDataset(fmi, ds);
        }
        return new Instance(iuid, out.toByteArray());
    }

    /**
     * A CT data set without file meta information, encoded in {@code tsuid}
     * (implicit or explicit VR little endian), as GetScu and the
     * pseudonymizer write it.
     */
    public static Instance bare(String tsuid, int pixelBytes) throws IOException {
        String iuid = UIDUtils.createUID();
        return new Instance(iuid, write(dataSet(UID.CTImageStorage, iuid, pixelBytes), tsuid));
    }

    /** As {@link #bare} but with encapsulated (compressed) pixel data, encoded explicit VR little endian. */
    public static Instance bareEncapsulated(int fragmentBytes) throws IOException {
        String iuid = UIDUtils.createUID();
        Attributes ds = dataSet(UID.CTImageStorage, iuid, 2);
        ds.remove(Tag.PixelData);
        Fragments fragments = ds.newFragments(Tag.PixelData, VR.OB, 2);
        fragments.add(new byte[0]); // empty basic offset table
        fragments.add(new byte[fragmentBytes]);
        return new Instance(iuid, write(ds, UID.ExplicitVRLittleEndian));
    }

    /** CT data set as GetScu receives it, with patient data to pseudonymize. */
    public static Attributes ctWithPatient(String iuid) {
        Attributes ds = dataSet(UID.CTImageStorage, iuid, 64);
        ds.setString(Tag.PatientID, VR.LO, "0012345678");
        ds.setString(Tag.PatientName, VR.PN, "Muster^Hans");
        ds.setString(Tag.PatientBirthDate, VR.DA, "19700101");
        ds.setString(Tag.StudyDate, VR.DA, "20260101");
        ds.setString(Tag.StudyInstanceUID, VR.UI, "1.2.826.0.1.3680043.10.999.1");
        ds.setString(Tag.SeriesInstanceUID, VR.UI, "1.2.826.0.1.3680043.10.999.1.1");
        return ds;
    }

    public static byte[] write(Attributes ds, String tsuid) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DicomOutputStream dos = new DicomOutputStream(out, tsuid)) {
            dos.writeDataset(null, ds);
        }
        return out.toByteArray();
    }

    private static Attributes dataSet(String cuid, String iuid, int pixelBytes) {
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
        return ds;
    }
}
