package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.dcm4che3.data.UID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.rra.processors.TransfersyntaxFilter.*;

@Slf4j
public class TransferSyntaxFilterTest {
    private TestRunner testRunner;
    private static Map<String, byte[]> dcmObjects = new HashMap<>();

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(TransfersyntaxFilter.class);
    }

    @Test
    public void testProcessorSuccess() {
        log.info("Begin Transfer syntax Filter Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(TRANSFER_SYNTAX, "*");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("TransferSyntax", "1.2.840.10008.1.2");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();

        testRunner.setProperty(TRANSFER_SYNTAX, "1.2.840.10008.1.2");
        testRunner.enqueue("Test Data", attr);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
    }
    @Test
    public void testProcessorFailed() {
        log.info("Begin Transfer syntax Filter Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(TRANSFER_SYNTAX, "1.2.840.10008.1.2");

        HashMap<String, String> attr = new HashMap<>();
        attr.put("TransferSyntax", "1.2.3.4.5.6.7.8");
        /*
            Data is normaly DICOM Object, but here we don't care of the data
            the routing is our focus
        */
        testRunner.enqueue("Test Data", attr);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(REL_FAILURE, 1);
    }
    @Test
    public void testProcessorSuccessWithData() {
        log.info("Begin SOP Class Filter Processor Test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(SOPCLassFilter.FILTER_SOP_CLASS, UID.ImplicitVRLittleEndian);
        Set<String> keys = dcmObjects.keySet();
        keys.forEach(ts -> {
            HashMap<String, String> attr = new HashMap<>();
            attr.put("TransferSyntax", ts);
            testRunner.enqueue("Test Data", attr);
            testRunner.run();
        });

        testRunner.assertAllFlowFilesTransferred(SOPCLassFilter.REL_SUCCESS, dcmObjects.size());
    }
}
