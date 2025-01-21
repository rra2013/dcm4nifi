package org.rra.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class Xml2HL7Test {
    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Xml2HL7.class);
    }

    @Test
    public void testProcessor() {
        File xml = new File("src/test/resources/hl7xml/ADT_A04_v23.xml");
        try(FileInputStream fis = new FileInputStream(xml)){
            String msg_xml = IOUtils.toString(fis, StandardCharsets.UTF_8);
            testRunner.enqueue(msg_xml);
            testRunner.run();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        testRunner.assertAllFlowFilesTransferred(Xml2HL7.REL_SUCCESS, 1);
    }
}
