package org.rra.processors;

import ca.uhn.hl7v2.util.XMLUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class Dcm2XmlTest {

    private static List<byte[]> dcmObjects = new ArrayList<>();
    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
    }

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Xml.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("$ $ $ $ Run DCM2XML $ $ $ $ $");
        dcmObjects.forEach(dcmFileArray -> {
            testRunner.enqueue(dcmFileArray);
            testRunner.run();
            log.info("Run with size {}", dcmFileArray.length);
        });
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Dcm2Xml.REL_SUCCESS);
        log.info("Size of success: {}", success.size());
        success.forEach(mockFlowFile -> {
            byte[] readAnonym = mockFlowFile.toByteArray();
            try(ByteArrayInputStream ba = new ByteArrayInputStream(readAnonym)){
                try(BufferedInputStream bif = new BufferedInputStream(ba)){
                    String xml = IOUtils.toString(bif, StandardCharsets.UTF_8);
                    log.info("size of XML: {}", xml.getBytes().length);
                    System.out.println(xml);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        testRunner.assertAllFlowFilesTransferred(Dcm2Xml.REL_SUCCESS);
    }
}
