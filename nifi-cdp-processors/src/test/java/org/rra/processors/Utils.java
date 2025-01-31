package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.io.DicomOutputStream;
import org.junit.jupiter.api.Assertions;
import org.rra.dcm.DicomDataReader;

import java.io.*;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Slf4j
public class Utils {
    private static byte[] readDicomFile(File f, StringBuffer transfersyntax) {
        byte[] result = new byte[0];
        try (FileInputStream fis = new FileInputStream(f)) {
            try (BufferedInputStream bif = new BufferedInputStream(fis)) {
                DicomDataReader data = new DicomDataReader(bif, true);
                Attributes dcm = data.getAttributes();
                log.debug(" + + + SOPInstanceUID: {}", dcm.getString(Tag.SOPInstanceUID));
                Attributes fmi = data.getFmi();
                if (null != fmi) {
                    String ts = fmi.getString(Tag.TransferSyntaxUID);
                    transfersyntax.append(ts);
                    log.info("FMI : {} - {}", transfersyntax, f.getName());
                    fmi = dcm.createFileMetaInformation(fmi.getString(Tag.TransferSyntaxUID));
                }
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    try (DicomOutputStream dos = new DicomOutputStream(baos,UID.ExplicitVRLittleEndian)) {
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
            log.debug("DICOM FIle: {}", file.getAbsolutePath());
            StringBuffer ts = new StringBuffer();
            dcmObjects.add(readDicomFile(file, ts));
            log.info("Transfersyntax: {}", ts.toString());
        });
        log.info("Read {} DICOM Files OK.", files.size());
    }
    public static void readDicomFiles(Map<FileInfo, byte[]> dcmObjects, String path) {
        Assertions.assertNotNull(dcmObjects);

        File dir = new File(path);
        Collection<File> files = FileUtils.listFiles(dir, null, true);
        files.forEach(file -> {
            //log.debug("DICOM FIle: {}", file.getAbsolutePath());
            StringBuffer tsuid = new StringBuffer();
            byte[] bytes = readDicomFile(file, tsuid);
            dcmObjects.put(() -> tsuid.toString(), bytes);
            log.info("Transfersyntax: {}", tsuid);
        });
        log.info("Read {} DICOM Files OK.", files.size());
    }
    @FunctionalInterface
    public interface FileInfo{
        String getTransferSyntax();
    }
    public static int provideRandomPort() {
        ServerSocket server;
        try {
            server = new ServerSocket(0);
            int port = server.getLocalPort();
            server.close();
            return port;
        } catch (IOException e) {
            throw new Error(e);
        }
    }
}
