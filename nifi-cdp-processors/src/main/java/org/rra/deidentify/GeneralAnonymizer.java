package org.rra.deidentify;

import lombok.extern.slf4j.Slf4j;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.io.DicomOutputStream;
import org.rra.dcm.DicomDataReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * @author Reza Rastégar
 * @since jan 22
 */
@Slf4j
public class GeneralAnonymizer {

    private static final boolean retainPixelData = true;
    private static final DicomEncodingOptions encOpts = DicomEncodingOptions.DEFAULT;

    public static Attributes anonymize(InputStream inputStream, OutputStream outputStream) throws IOException {
        final DicomDataReader data = new DicomDataReader(inputStream, retainPixelData);
        final Deidentify deidentify = new Deidentify();
        final Attributes anonym = deidentify.deidentifyAttributes(data.getAttributes(), true);
        Attributes fmi = data.getFmi();
        if (null != fmi) {
            fmi = anonym.createFileMetaInformation(fmi.getString(Tag.TransferSyntaxUID));
        }
        try (DicomOutputStream dos = new DicomOutputStream(outputStream, "1.2.840.10008.1.2.1")) {
            dos.setEncodingOptions(encOpts);
            dos.writeDataset(fmi, anonym);
        }
        log.info("+ + + Deidentify done + + +");
        return anonym;
    }

    public static Attributes pseudonymize(InputStream inputStream, OutputStream outputStream, PIDLookup lookup) throws Exception {
        final DicomDataReader data = new DicomDataReader(inputStream, retainPixelData);
        final Attributes attributes = data.getAttributes();
        if (null == lookup) {
            throw new Exception("No lookup callback is set");
        }
        String pid = attributes.getString(Tag.PatientID);
        if (null == pid || pid.isEmpty()) {
            throw new Exception("PID is not found");
        }
        final PseudonymLookupData lookupData = lookup.lookup(pid);
        final Deidentify deidentify = new Deidentify();
        final Attributes anonym = deidentify.deidentifyAttributes(attributes, true, lookupData.getPrefix(), lookupData.getPostfix());
        Attributes fmi = data.getFmi();
        if (null != fmi) {
            fmi = anonym.createFileMetaInformation(fmi.getString(Tag.TransferSyntaxUID));
        }
        try (DicomOutputStream dos = new DicomOutputStream(outputStream, "1.2.840.10008.1.2.1")) {
            dos.setEncodingOptions(encOpts);
            dos.writeDataset(fmi, anonym);
        }
        log.info("+ + + Deidentify done + + +");
        return anonym;
    }

    @FunctionalInterface
    public interface PIDLookup {
        PseudonymLookupData lookup(String pid) throws SQLException;
    }
}
