package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

@Slf4j
public class DICOMRelaisTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(DICOMRelais.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("Begin DICOM Relais Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(DICOMRelais.ROUTE_AET, "DCM4NIFI");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("CalledAET", "DCM4NIFI");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DICOMRelais.REL_SUCCESS);
    }

    @Test
    public void testProcessorFailed() {
        log.info("Begin DICOM Relais Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(DICOMRelais.ROUTE_AET, "DCM4NIFI");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("CalledAET", "OTHER_AET");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DICOMRelais.REL_FAILURE);

    }
}
