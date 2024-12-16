package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

@Slf4j
public class SOPClassFilterTest {
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(SOPCLassFilter.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("Begin SOP Class Filter Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(SOPCLassFilter.FILTER_SOP_CLASS, "*");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("AffectedSOPClassUID", "1.2.3.4");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();

        testRunner.setProperty(SOPCLassFilter.FILTER_SOP_CLASS, "1.2.3.4");
        testRunner.enqueue("Test Data", attr);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SOPCLassFilter.REL_SUCCESS, 2);
    }
    @Test
    public void testProcessorFailed() {
        log.info("Begin SOP Class Filter Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(SOPCLassFilter.FILTER_SOP_CLASS, "1.2.840.10008.5.1.4.1.1.4");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("AffectedSOPClassUID", "1.2.3.4.5.6.7.8");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(SOPCLassFilter.REL_FAILURE, 1);

    }
}
