package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomOutputStream;
import org.rra.dcm.Dicom2JsonTransformer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.rra.processors.Utils.readDicomFiles;

@Slf4j
public class Dcm2JsonTest {
    private static List<byte[]> dcmObjectsCT = new ArrayList<>();
    private static List<byte[]> dcmObjectsMR = new ArrayList<>();
    private TestRunner testRunner;

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        //readDicomFiles(dcmObjectsCT, DataForTest.DICOM_PATH_DEF);
        dcmObjectsCT = DataForTest.DCMOBJECTS_IVRLE;
        Assertions.assertTrue(dcmObjectsCT.size() > 0);
        //readDicomFiles(dcmObjectsMR, DataForTest.DICOM_PATH_MR);
        dcmObjectsMR = DataForTest.DCM_MR_OBJECTS;
        Assertions.assertTrue(dcmObjectsMR.size() > 0);
    }

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Json.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("$ $ $ $ Run DCM2JSON $ $ $ $ $");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(Dcm2Json.BULK_DATA, Dcm2Json.NO_BULK_DATA);
        testRunner.setProperty(Dcm2Json.INDENT_JSON, "true");
        testRunner.setProperty(Dcm2Json.ENCODE_AS_NUMBER, "false");
        testRunner.setProperty(Dcm2Json.REMOVE_PRIVAT, "true");
        testRunner.setProperty(Dcm2Json.PRINT_TAG_NAMES, "true");
        //First test CT
        dcmObjectsCT.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_SUCCESS);
        log.info("Size of success: {}", success.size());
        List<MockFlowFile> failed = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_FAILURE);
        log.info("Size of failed: {}", failed.size());

        testRunner.setProperty(Dcm2Json.INDENT_JSON, "true");
        testRunner.setProperty(Dcm2Json.ENCODE_AS_NUMBER, "false");
        testRunner.setProperty(Dcm2Json.REMOVE_PRIVAT, "false");
        testRunner.setProperty(Dcm2Json.BULK_DATA, Dcm2Json.NO_BULK_DATA);
        dcmObjectsMR.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });

        testRunner.assertAllFlowFilesTransferred(Dcm2Json.REL_SUCCESS);
    }
    @Test
    public void testJsonOutput() throws IOException {

        dcmObjectsCT.forEach(bytes -> {
            try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
                try(BufferedInputStream bis = new BufferedInputStream(byteArrayInputStream)) {
                    Dicom2JsonTransformer.transform(bis,System.out, Boolean.FALSE, true, true, true, false);
                }
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
        });
    }

    @Test
    public void testDatasetWithoutFmiAndSopInstanceUid() throws IOException {
        byte[] dcm = createDataset(false, false, UID.ImplicitVRLittleEndian);
        runSingle(dcm, "true");
        MockFlowFile ff = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_SUCCESS).get(0);
        String json = ff.getContent();
        Assertions.assertTrue(json.contains("\"00100010\""), "PatientName expected");
        Assertions.assertFalse(json.contains("\"00080018\""), "SOPInstanceUID not expected");
        Assertions.assertFalse(json.contains("\"00091010\""), "private tag must be removed");
        Assertions.assertFalse(json.contains("\"00090010\""), "private creator must be removed");
    }

    @Test
    public void testDatasetWithoutFmiAndSopInstanceUidKeepPrivate() throws IOException {
        byte[] dcm = createDataset(false, false, UID.ImplicitVRLittleEndian);
        runSingle(dcm, "false");
        String json = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_SUCCESS).get(0).getContent();
        Assertions.assertTrue(json.contains("\"00091010\""), "private tag must be kept");
    }

    @Test
    public void testDatasetWithoutFmiExplicitVr() throws IOException {
        byte[] dcm = createDataset(false, false, UID.ExplicitVRLittleEndian);
        runSingle(dcm, "true");
        String json = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_SUCCESS).get(0).getContent();
        Assertions.assertTrue(json.contains("\"00100010\""), "PatientName expected");
        Assertions.assertFalse(json.contains("\"00091010\""), "private tag must be removed");
    }

    @Test
    public void testDatasetWithoutFmiWithSopInstanceUid() throws IOException {
        byte[] dcm = createDataset(true, false, UID.ImplicitVRLittleEndian);
        runSingle(dcm, "true");
        String json = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_SUCCESS).get(0).getContent();
        Assertions.assertTrue(json.contains("\"00080018\""), "SOPInstanceUID expected");
        Assertions.assertFalse(json.contains("\"00091010\""), "private tag must be removed");
    }

    @Test
    public void testFileWithFmiWithoutSopInstanceUid() throws IOException {
        byte[] dcm = createDataset(false, true, UID.ExplicitVRLittleEndian);
        runSingle(dcm, "true");
        String json = testRunner.getFlowFilesForRelationship(Dcm2Json.REL_SUCCESS).get(0).getContent();
        Assertions.assertTrue(json.contains("\"00100010\""), "PatientName expected");
        Assertions.assertFalse(json.contains("\"00091010\""), "private tag must be removed");
    }

    @Test
    public void testTransformerWithoutFmiAndSopInstanceUid() throws IOException {
        byte[] dcm = createDataset(false, false, UID.ImplicitVRLittleEndian);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ByteArrayInputStream in = new ByteArrayInputStream(dcm)) {
            Assertions.assertDoesNotThrow(() ->
                    Dicom2JsonTransformer.transform(in, out, Boolean.FALSE, false, false, true, false));
        }
        String json = out.toString(StandardCharsets.UTF_8);
        Assertions.assertTrue(json.contains("\"00100010\""), "PatientName expected");
        Assertions.assertFalse(json.contains("\"00091010\""), "private tag must be removed");
    }

    private void runSingle(byte[] dcm, String removePrivate) {
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(Dcm2Json.BULK_DATA, Dcm2Json.NO_BULK_DATA);
        testRunner.setProperty(Dcm2Json.INDENT_JSON, "false");
        testRunner.setProperty(Dcm2Json.ENCODE_AS_NUMBER, "false");
        testRunner.setProperty(Dcm2Json.REMOVE_PRIVAT, removePrivate);
        testRunner.setProperty(Dcm2Json.PRINT_TAG_NAMES, "false");
        testRunner.enqueue(dcm);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(Dcm2Json.REL_SUCCESS, 1);
    }

    /**
     * Builds a minimal DICOM object in memory.
     *
     * @param withSopInstanceUid include SOPInstanceUID (0008,0018)
     * @param withFmi            write a Part 10 file (preamble + FMI) instead of a bare dataset
     * @param tsuid              transfer syntax of the dataset
     */
    private static byte[] createDataset(boolean withSopInstanceUid, boolean withFmi, String tsuid) throws IOException {
        Attributes attrs = new Attributes();
        attrs.setString(Tag.SOPClassUID, VR.UI, UID.CTImageStorage);
        if (withSopInstanceUid) {
            attrs.setString(Tag.SOPInstanceUID, VR.UI, "1.2.826.0.1.3680043.2.1125.1.1");
        }
        attrs.setString(Tag.Modality, VR.CS, "CT");
        attrs.setString(0x00090010, VR.LO, "TEST PRIVATE");
        attrs.setString(0x00091010, VR.LO, "private value");
        attrs.setString(Tag.PatientName, VR.PN, "Test^Patient");
        attrs.setString(Tag.PatientID, VR.LO, "4711");
        attrs.setString(Tag.StudyInstanceUID, VR.UI, "1.2.826.0.1.3680043.2.1125.1.2");
        attrs.setString(Tag.SeriesInstanceUID, VR.UI, "1.2.826.0.1.3680043.2.1125.1.3");

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        if (withFmi) {
            Attributes fmi = Attributes.createFileMetaInformation(
                    "1.2.826.0.1.3680043.2.1125.1.4", UID.CTImageStorage, tsuid);
            try (DicomOutputStream dos = new DicomOutputStream(bos, UID.ExplicitVRLittleEndian)) {
                dos.writeDataset(fmi, attrs);
            }
        } else {
            try (DicomOutputStream dos = new DicomOutputStream(bos, tsuid)) {
                dos.writeDataset(null, attrs);
            }
        }
        return bos.toByteArray();
    }
}
