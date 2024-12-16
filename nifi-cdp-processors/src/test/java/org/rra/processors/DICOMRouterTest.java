package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

@Slf4j
public class DICOMRouterTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(DICOMRouter.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("Begin DICOM Router Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(DICOMRouter.ROUTE_AET, "STORE_SCU");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("CallingAET", "STORE_SCU");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DICOMRouter.REL_SUCCESS);
    }

    @Test
    public void testProcessorFailed() {
        log.info("Begin DICOM router Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(DICOMRouter.ROUTE_AET, "STORE_SCU");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("CallingAET", "STORE_SCU_OTHER");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DICOMRouter.REL_FAILURE);

    }
}
