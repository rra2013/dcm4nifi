package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.io.DicomOutputStream;
import org.junit.jupiter.api.Assertions;
import org.rra.dcm.DicomDataReader;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
@Slf4j
public class Utils {
    private static byte[] readDicomFile(File f) {
        byte[] result = new byte[0];
        try (FileInputStream fis = new FileInputStream(f)) {
            try (BufferedInputStream bif = new BufferedInputStream(fis)) {
                DicomDataReader data = new DicomDataReader(bif, true);
                Attributes dcm = data.getAttributes();
                log.debug(" + + + SOPInstanceUID: {}", dcm.getString(Tag.SOPInstanceUID));
                Attributes fmi = data.getFmi();
                if (null != fmi) {
                    fmi = dcm.createFileMetaInformation(fmi.getString(Tag.TransferSyntaxUID));
                }
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    try (DicomOutputStream dos = new DicomOutputStream(baos, "1.2.840.10008.1.2.1")) {
                        dos.setEncodingOptions(DicomEncodingOptions.DEFAULT);
                        dos.writeDataset(fmi, dcm);
                    }
                    result = baos.toByteArray();
                }
            }
            log.debug("Read DICOM Object OK");
        } catch (Exception e) {
            log.error("File not found...{}", e.getMessage());
        }
        return result;
    }

    public static void readDicomFiles(List<byte[]> dcmObjects, String path) {
        Assertions.assertNotNull(dcmObjects);

        File dir = new File(path);
        Collection<File> files = FileUtils.listFiles(dir, null, true);
        files.forEach(file -> {
            //log.debug("DICOM FIle: {}", file.getAbsolutePath());
            dcmObjects.add(readDicomFile(file));
        });
        log.info("Read {} DICOM Files OK.", files.size());
    }
}
