package org.rra.processors;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisabledOnOs(OS.WINDOWS)
public class TestGroovyHL7 {
    protected ExecuteGroovyScript proc;
    protected TestRunner runner;
    public static final String TEST_RESOURCE_LOCATION = "target/test/resources/groovy/";
    public static final String HL7_RESOURCE_LOCATION = "target/test/resources/hl7/";
    public static final String LIB_RESOURCE_LOCATION = "target/test/resources/lib/";

    @Test
    public void test_hl7_ADT_A04_modify_groovy(){
        System.out.println("Test ADT_A04_modify_groovy");
        runner.setProperty(ExecuteGroovyScript.ADD_CLASSPATH, LIB_RESOURCE_LOCATION);
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, TEST_RESOURCE_LOCATION + "hl7_ADT_A04_v23_modify.groovy");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();

        try(FileInputStream hl7_adt_a04 = new FileInputStream(new File(HL7_RESOURCE_LOCATION + "ADT_A04_v23.hl7"))){
            try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(hl7_adt_a04.readAllBytes())){
                byte[] bytes = byteArrayInputStream.readAllBytes();
                runner.enqueue(bytes);
                runner.run();
                runner.assertAllFlowFilesTransferred(ExecuteGroovyScript.REL_SUCCESS.getName(), 1);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        FileUtils.copyDirectory(new File("../groovy/hl7"), new File(HL7_RESOURCE_LOCATION));
        FileUtils.copyDirectory(new File("../groovy/lib"), new File(LIB_RESOURCE_LOCATION));
    }
}
