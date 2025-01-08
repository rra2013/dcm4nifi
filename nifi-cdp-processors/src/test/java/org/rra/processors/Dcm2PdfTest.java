package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.rra.dcm.Dicom2PdfTransformer.transform;
import static org.rra.processors.Dcm2Pdf.REL_SUCCESS;

@Slf4j
public class Dcm2PdfTest {
    private static List<byte[]> pdf_dcmObjects = new ArrayList<>();
    private TestRunner testRunner;

    @BeforeAll
    public static void readData() {
        //Get DICOM Files
        pdf_dcmObjects = DataForTest.PDF_OBJECTS;
        Assertions.assertTrue(pdf_dcmObjects.size() > 0);
    }
    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Pdf.class);
    }
    @Test
    public void testProcessorSuccess() {
        log.info("$ $ $ $ Run DCM2PDF $ $ $ $ $");
        pdf_dcmObjects.forEach(bytes -> {
            testRunner.enqueue(bytes);
            testRunner.run();
        });
        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, pdf_dcmObjects.size());
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(REL_SUCCESS);
        success.forEach(mockFlowFile -> {
            String attribute = mockFlowFile.getAttribute(CoreAttributes.FILENAME.key());
            log.info("$ $ $ $ {} $ $ $ $", attribute);
            Assertions.assertTrue(attribute.endsWith(".pdf"));
        });
    }
        @Test
    public void testTransformer() {
        pdf_dcmObjects.forEach(bytes -> {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                    String fileExt = transform(bis, bos);
                    log.info("File extension (Type): {}", fileExt);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
