package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.rra.processors.Dcm2Dcm.*;

@Slf4j
public class Dcm2DcmTest {

    private static List<byte[]> dcmObjects = new ArrayList<>();

    private TestRunner testRunner;

    @BeforeAll
    public static void readData() {
        dcmObjects = DataForTest.DCMOBJECTS_IVRLE;

        Assertions.assertFalse(
                dcmObjects.isEmpty(),
                "Es wurden keine DICOM-Testdateien geladen"
        );
    }

    private static void appendAttributeIfPresent(
            StringBuilder diagnostic,
            MockFlowFile flowFile,
            String attributeName
    ) {
        String value = flowFile.getAttribute(attributeName);

        if (value != null) {
            diagnostic
                    .append(attributeName)
                    .append(": ")
                    .append(value)
                    .append(System.lineSeparator());
        }
    }

    private static String stackTraceOf(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();

        try (PrintWriter printWriter =
                     new PrintWriter(stringWriter)) {

            throwable.printStackTrace(printWriter);
        }

        return stringWriter.toString();
    }

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Dcm2Dcm.class);
        testRunner.setValidateExpressionUsage(false);
    }

    @Test
    public void testProcessor() {
        log.info(
                "Begin Dcm2Dcm Processor Test with {} DICOM objects",
                dcmObjects.size()
        );
        runAndAssertSuccess(JPLL);
        testRunner.clearTransferState();
        runAndAssertSuccess(JP2KR);
    }

    private void runAndAssertSuccess(String transferSyntax) {
        log.info(
                "Testing transfer syntax {} with {} DICOM objects",
                transferSyntax,
                dcmObjects.size()
        );

        testRunner.setProperty(
                TRANSFER_SYNTAX,
                transferSyntax
        );

        for (int index = 0; index < dcmObjects.size(); index++) {
            byte[] dcmFileArray = dcmObjects.get(index);

            HashMap<String, String> attributes = new HashMap<>();
            attributes.put(
                    "TransferSyntax",
                    "TEST_RUNNER from byte Array"
            );
            attributes.put(
                    "test.transfer.syntax",
                    transferSyntax
            );
            attributes.put(
                    "test.object.index",
                    Integer.toString(index)
            );

            testRunner.enqueue(dcmFileArray, attributes);

            testRunner.run();

            log.info(
                    "Run {} with transfer syntax {} and size {} bytes",
                    index,
                    transferSyntax,
                    dcmFileArray.length
            );

            failWithDiagnosticsWhenNecessary(
                    transferSyntax,
                    index,
                    dcmFileArray.length
            );
        }

        List<MockFlowFile> success =
                testRunner.getFlowFilesForRelationship(
                        REL_SUCCESS
                );

        log.info(
                "Transfer syntax {}: {} FlowFiles succeeded",
                transferSyntax,
                success.size()
        );

        testRunner.assertAllFlowFilesTransferred(
                REL_SUCCESS,
                dcmObjects.size()
        );

        testRunner.assertQueueEmpty();
    }

    private void failWithDiagnosticsWhenNecessary(
            String transferSyntax,
            int objectIndex,
            int inputSize
    ) {
        List<MockFlowFile> failures =
                testRunner.getFlowFilesForRelationship(
                        REL_FAILURE
                );

        if (failures.isEmpty()) {
            return;
        }

        StringBuilder diagnostic = new StringBuilder();

        diagnostic
                .append(System.lineSeparator())
                .append("DICOM transformation failed")
                .append(System.lineSeparator())
                .append("Transfer syntax: ")
                .append(transferSyntax)
                .append(System.lineSeparator())
                .append("DICOM object index: ")
                .append(objectIndex)
                .append(System.lineSeparator())
                .append("Input size: ")
                .append(inputSize)
                .append(" bytes")
                .append(System.lineSeparator());

        for (MockFlowFile failure : failures) {
            diagnostic
                    .append("Failure FlowFile attributes: ")
                    .append(failure.getAttributes())
                    .append(System.lineSeparator());

            appendAttributeIfPresent(
                    diagnostic,
                    failure,
                    "dcm.error.class"
            );

            appendAttributeIfPresent(
                    diagnostic,
                    failure,
                    "dcm.error.message"
            );
        }

        diagnostic
                .append("Processor error messages:")
                .append(System.lineSeparator());

        testRunner.getLogger()
                .getErrorMessages()
                .forEach(message -> {
                    diagnostic
                            .append(message.getMsg())
                            .append(System.lineSeparator());

                    Throwable throwable = message.getThrowable();

                    if (throwable != null) {
                        diagnostic
                                .append(stackTraceOf(throwable))
                                .append(System.lineSeparator());
                    }
                });

        Assertions.fail(diagnostic.toString());
    }
}