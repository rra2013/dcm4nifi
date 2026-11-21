package org.rra.processors;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * write_external_drive_zip.groovy (PERSIST_ORDER_FILES, Lieferart DIRECTORY):
 * schreibt nur auf einen eingehängten Datenträger, nie auf die VM-Platte.
 *
 * <p>"Nicht eingehängt" ist ein gewöhnlicher Ordner in einem Temp-Verzeichnis.
 * Für "eingehängt" dient /dev/shm (tmpfs) unter /dev (devtmpfs): ein echter
 * Mount-Point mit eigenem Gerät. Fehlt er oder ist er nicht beschreibbar,
 * werden diese Tests übersprungen.</p>
 */
@DisabledOnOs(OS.WINDOWS)
public class TestGroovyExternalDrive {

    private static final String SCRIPTS = "target/test/resources/groovy/";
    private static final String SCRIPT = "write_external_drive_zip.groovy";
    private static final String ORDER_ID = "11111111-2222-3333-4444-555555555555";
    private static final String RETRIEVE_ID = "66666666-7777-8888-9999-000000000000";

    @TempDir
    Path vm;

    private String folder;

    @BeforeAll
    public static void copyScripts() throws Exception {
        FileUtils.copyDirectory(new File("../groovy/script"), new File(SCRIPTS));
    }

    @BeforeEach
    void folder() {
        folder = "IT_" + UUID.randomUUID().toString().substring(0, 8);
    }

    @AfterEach
    void cleanUp() throws IOException {
        Path written = Paths.get("/dev/shm", folder);
        if (Files.exists(written, LinkOption.NOFOLLOW_LINKS)) {
            if (Files.isSymbolicLink(written)) {
                Files.delete(written);
            } else {
                FileUtils.deleteDirectory(written.toFile());
            }
        }
    }

    private static TestRunner runner() {
        TestRunner runner = TestRunners.newTestRunner(new ExecuteGroovyScript());
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, SCRIPTS + SCRIPT);
        runner.setProperty(ExecuteGroovyScript.FAIL_STRATEGY, "transfer to failure");
        runner.setAllowSynchronousSessionCommits(true);
        runner.assertValid();
        return runner;
    }

    private static Map<String, Object> target(String drive, String folder) {
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("orderId", ORDER_ID);
        target.put("retrieveId", RETRIEVE_ID);
        target.put("deliveryType", "DIRECTORY");
        target.put("directory", drive);
        target.put("folder", folder);
        target.put("subfolder", "PSN-0001");
        target.put("fileName", RETRIEVE_ID + ".zip");
        return target;
    }

    private static String json(Map<String, Object> target) {
        StringBuilder json = new StringBuilder("{");
        target.forEach((key, value) -> {
            if (json.length() > 1) json.append(',');
            json.append('"').append(key).append("\":");
            json.append(value == null ? "null" : "\"" + value.toString().replace("\\", "\\\\") + "\"");
        });
        return json.append('}').toString();
    }

    private static MockFlowFile run(TestRunner runner, String root, Map<String, Object> target, byte[] zip,
                                    String expectedRelationship) {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("dcma.order.id", ORDER_ID);
        attributes.put("dcma.retrieve.id", RETRIEVE_ID);
        attributes.put("dcma.study.uid", "1.2.840.10001");
        attributes.put("dcma.series.uid", "1.2.840.20001");
        if (root != null) attributes.put("dcma.external.root", root);
        if (target != null) attributes.put("dcma.zip.target", json(target));
        runner.enqueue(zip, attributes);
        runner.run();

        List<MockFlowFile> out = runner.getFlowFilesForRelationship(expectedRelationship);
        assertEquals(1, out.size(), "expected one FlowFile in " + expectedRelationship);
        runner.clearTransferState();
        return out.get(0);
    }

    private static MockFlowFile failure(TestRunner runner, String root, Map<String, Object> target) {
        return run(runner, root, target, new byte[] {'P', 'K'}, "failure");
    }

    private static void assumeShmIsAMountPoint() throws IOException {
        Path dev = Paths.get("/dev");
        Path shm = Paths.get("/dev/shm");
        assumeTrue(Files.isDirectory(shm, LinkOption.NOFOLLOW_LINKS) && Files.isWritable(shm),
                "/dev/shm not available");
        assumeTrue(!Files.getAttribute(shm, "unix:dev", LinkOption.NOFOLLOW_LINKS)
                .equals(Files.getAttribute(dev, "unix:dev", LinkOption.NOFOLLOW_LINKS)),
                "/dev/shm is not a mount point here");
    }

    private static long count(Path dir) throws IOException {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.count();
        }
    }

    // --------------------------------------------------
    // Nie auf die VM-Platte
    // --------------------------------------------------

    @Test
    public void unmountedDriveIsNotWritten() throws Exception {
        Path empty = Files.createDirectory(vm.resolve("DLF090902343424"));

        MockFlowFile flowFile = failure(runner(), vm.toString(), target("DLF090902343424", folder));

        flowFile.assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_UNAVAILABLE");
        assertTrue(flowFile.getAttribute("dcma.zip.error.message").contains("not mounted"));
        assertEquals(0, count(empty), "nothing may be written to the VM disk");
        // Inhalt bleibt das ZIP, für die Fehleranalyse
        flowFile.assertContentEquals(new byte[] {'P', 'K'});
    }

    @Test
    public void missingDriveIsNotCreated() throws Exception {
        MockFlowFile flowFile = failure(runner(), vm.toString(), target("USB-FEHLT", folder));

        flowFile.assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_UNAVAILABLE");
        assertFalse(Files.exists(vm.resolve("USB-FEHLT")));
        assertEquals(0, count(vm));
    }

    @Test
    public void symbolicLinkAsDriveIsRejected() throws Exception {
        Files.createSymbolicLink(vm.resolve("LINK"), Paths.get("/dev/shm"));

        failure(runner(), vm.toString(), target("LINK", folder))
                .assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_UNAVAILABLE");
    }

    @Test
    public void missingRootIsUnavailable() {
        failure(runner(), vm.resolve("gibt-es-nicht").toString(), target("USB1", folder))
                .assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_UNAVAILABLE");
    }

    @Test
    public void folderLinkingOutOfTheDriveIsRejected() throws Exception {
        assumeShmIsAMountPoint();
        Files.createSymbolicLink(Paths.get("/dev/shm", folder), vm);

        failure(runner(), "/dev", target("shm", folder))
                .assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_WRITE_FAILED");
        assertEquals(0, count(vm));
    }

    // --------------------------------------------------
    // Ungültiges Ziel
    // --------------------------------------------------

    @Test
    public void invalidTargetsAreRejected() {
        TestRunner runner = runner();
        List<Map<String, Object>> invalid = List.of(
                with("directory", ".."), with("directory", "../etc"), with("directory", "a/b"),
                with("directory", "CON"), with("directory", ""), with("folder", ".."),
                with("folder", "a/b"), with("folder", "a\\b"), with("folder", ".versteckt"),
                with("folder", null), with("subfolder", "../../etc"), with("subfolder", "x."),
                with("fileName", "x.zip"), with("fileName", "99999999-7777-8888-9999-000000000000.zip"),
                with("orderId", "99999999-2222-3333-4444-555555555555"),
                with("retrieveId", "99999999-7777-8888-9999-000000000000"),
                with("deliveryType", "DICOM_STORE"));
        for (Map<String, Object> target : invalid) {
            failure(runner, "/dev", target)
                    .assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_INVALID_TARGET");
        }
    }

    @Test
    public void missingOrBrokenTargetIsRejected() {
        TestRunner runner = runner();
        failure(runner, "/dev", null)
                .assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_INVALID_TARGET");

        runner.enqueue(new byte[] {1}, Map.of(
                "dcma.order.id", ORDER_ID, "dcma.retrieve.id", RETRIEVE_ID, "dcma.zip.target", "{kaputt"));
        runner.run();
        runner.getFlowFilesForRelationship("failure").get(0)
                .assertAttributeEquals("dcma.zip.error.code", "EXTERNAL_DRIVE_INVALID_TARGET");
    }

    private Map<String, Object> with(String key, Object value) {
        Map<String, Object> target = target("shm", folder);
        target.put(key, value);
        return target;
    }

    // --------------------------------------------------
    // DOWNLOAD unverändert, DIRECTORY schreibt
    // --------------------------------------------------

    @Test
    public void downloadPassesThroughUnchanged() throws Exception {
        Map<String, Object> target = target(null, null);
        target.put("deliveryType", "DOWNLOAD");
        target.remove("subfolder");

        MockFlowFile flowFile = run(runner(), null, target, new byte[] {'P', 'K'}, "success");

        flowFile.assertAttributeEquals("dcma.zip.delivery", "DOWNLOAD");
        flowFile.assertAttributeNotExists("dcma.zip.path");
        flowFile.assertAttributeNotExists("dcma.zip.error.code");
        flowFile.assertContentEquals(new byte[] {'P', 'K'});
    }

    @Test
    public void mountedDriveGetsTheZip() throws Exception {
        assumeShmIsAMountPoint();
        byte[] zip = new byte[200_000];
        new Random(7).nextBytes(zip);

        MockFlowFile flowFile = run(runner(), "/dev", target("shm", folder), zip, "success");

        Path written = Paths.get("/dev/shm", folder, "PSN-0001", RETRIEVE_ID + ".zip");
        flowFile.assertAttributeEquals("dcma.zip.delivery", "DIRECTORY");
        flowFile.assertAttributeEquals("dcma.zip.path", written.toString());
        flowFile.assertAttributeEquals("dcma.zip.size.bytes", "200000");
        assertArrayEquals(zip, Files.readAllBytes(written));
        assertEquals(1, count(written.getParent()), "no .part file may remain");
    }

    @Test
    public void retryReplacesTheZip() throws Exception {
        assumeShmIsAMountPoint();
        TestRunner runner = runner();
        run(runner, "/dev", target("shm", folder), new byte[] {1, 2, 3}, "success");
        run(runner, "/dev", target("shm", folder), new byte[] {4, 5}, "success");

        assertArrayEquals(new byte[] {4, 5},
                Files.readAllBytes(Paths.get("/dev/shm", folder, "PSN-0001", RETRIEVE_ID + ".zip")));
    }
}
