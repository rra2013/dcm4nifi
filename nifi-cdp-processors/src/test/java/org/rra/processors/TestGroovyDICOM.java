package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript;
import org.apache.nifi.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class TestGroovyDICOM {
    protected ExecuteGroovyScript proc;
    protected TestRunner runner;
    public static final String TEST_RESOURCE_LOCATION = "target/test/resources/groovy/";
    public static final String LIB_RESOURCE_LOCATION = "target/test/resources/lib/";

    private static List<byte[]> dcmObjects = new ArrayList<>();

    /**
     * Copies all scripts to the target directory because when they are
     * compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @BeforeAll
    public static void setupBeforeClass() throws Exception {
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test/resources"));
        FileUtils.copyDirectory(new File("../groovy/script"), new File("target/test/resources/groovy/"));
        FileUtils.copyDirectory(new File("../groovy/lib"), new File(LIB_RESOURCE_LOCATION));

        //Get DICOM Files
        dcmObjects = DataForTest.DCMOBJECTS;
        Assertions.assertTrue(dcmObjects.size() > 0);
    }

    @Test
    public void test_dicom_compare_groovy(){
        runner.setProperty(ExecuteGroovyScript.ADD_CLASSPATH, LIB_RESOURCE_LOCATION);
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "dicom_attribute_comparator.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();

        dcmObjects.forEach(bytesArrayDcm -> {
            runner.enqueue(bytesArrayDcm);
            runner.run();
        });
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS);
        runner.assertTransferCount(ExecuteGroovyScript.REL_SUCCESS, success.size());

        List<MockFlowFile> failed = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_FAILURE);
        runner.assertTransferCount(ExecuteGroovyScript.REL_FAILURE, failed.size());
        log.info("Count of success:{}, failed:{}", success.size(), failed.size());
    }

    @BeforeEach
    public void setup() {
        //init processor
        proc = new ExecuteGroovyScript();
        MockProcessContext context = new MockProcessContext(proc);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(proc, context);
        proc.initialize(initContext);
        assertNotNull(proc.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(proc);
    }

}
