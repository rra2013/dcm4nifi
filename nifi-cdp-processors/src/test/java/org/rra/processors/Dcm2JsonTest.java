package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomEncodingOptions;
import org.dcm4che3.io.DicomOutputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rra.dcm.Dicom2JsonTransformer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.rra.processors.Utils.readDicomFiles;

@Slf4j
public class Dcm2JsonTest {
    private static List<byte[]> dcmObjects = new ArrayList<>();
    private static List<byte[]> dcmObjectsUS = new ArrayList<>();
    private static List<byte[]> dcmObjectsMR = new ArrayList<>();
    private TestRunner testRunner;

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
        readDicomFiles(dcmObjectsUS, DataForTest.DICOM_PATH_US);
        Assertions.assertTrue(dcmObjectsUS.size() > 0);
        readDicomFiles(dcmObjectsMR, DataForTest.DICOM_PATH_MR);
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
        //First test US
        dcmObjectsUS.forEach(dcmFileArray -> {
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

        dcmObjectsUS.forEach(bytes -> {
            try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
                try(BufferedInputStream bis = new BufferedInputStream(byteArrayInputStream)) {
                    Dicom2JsonTransformer.transform(bis,System.out, Boolean.FALSE, true, true, true, false);
                }
            } catch (IOException e) {
                e.printStackTrace(System.err);
            }
        });
    }
}
