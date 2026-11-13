package org.rra.processors;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.v25.datatype.ST;
import ca.uhn.hl7v2.model.v25.group.ORM_O01_ORDER_DETAIL;
import ca.uhn.hl7v2.model.v25.message.ORM_O01;
import ca.uhn.hl7v2.model.v25.segment.OBX;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lieferung per DICOM C-STORE (dcma-order DELIVERY_*):
 * extract_pseudonym_order_ORM_O01.groovy setzt dcma.delivery.*,
 * pseudonym_instance_failed.groovy meldet StoreScu-Fehler als STORE_FAILED.
 */
@DisabledOnOs(OS.WINDOWS)
public class TestGroovyDelivery {

    private static final String SCRIPTS = "target/test/resources/groovy/";
    private static final String ORDER_ID = "11111111-2222-3333-4444-555555555555";

    @BeforeAll
    public static void copyScripts() throws Exception {
        FileUtils.copyDirectory(new File("../groovy/script"), new File(SCRIPTS));
    }

    private static TestRunner runner(String script) {
        TestRunner runner = TestRunners.newTestRunner(new ExecuteGroovyScript());
        runner.setProperty(ExecuteGroovyScript.SCRIPT_FILE, SCRIPTS + script);
        runner.setProperty(ExecuteGroovyScript.FAIL_STRATEGY, "transfer to failure");
        // "transfer to failure" ruft intern session.commit() auf (ProcessSessionWrap),
        // das nifi-mock seit 1.14 nur nach ausdrücklicher Freigabe erlaubt
        runner.setAllowSynchronousSessionCommits(true);
        runner.assertValid();
        return runner;
    }

    // --------------------------------------------------
    // extract_pseudonym_order_ORM_O01.groovy
    // --------------------------------------------------

    @Test
    public void dicomStoreTargetIsSetOnEverySeries() throws Exception {
        TestRunner runner = runner("extract_pseudonym_order_ORM_O01.groovy");
        runner.enqueue(orm(delivery("DICOM_STORE", "CAIRO_HPC", "hpc.insel.ch", "11112")));
        runner.run();

        runner.assertTransferCount(ExecuteGroovyScript.REL_FAILURE, 0);
        List<MockFlowFile> series = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS);
        assertEquals(3, series.size());
        for (MockFlowFile flowFile : series) {
            flowFile.assertAttributeEquals("dcma.delivery.type", "DICOM_STORE");
            flowFile.assertAttributeEquals("dcma.delivery.aet", "CAIRO_HPC");
            flowFile.assertAttributeEquals("dcma.delivery.host", "hpc.insel.ch");
            flowFile.assertAttributeEquals("dcma.delivery.port", "11112");
            flowFile.assertAttributeEquals("dcma.order.id", ORDER_ID);
            flowFile.assertAttributeEquals("dcma.destination.aet", "IDSC DCMA-Auftragsportal");
        }
    }

    @Test
    public void downloadHasNoStoreTarget() throws Exception {
        TestRunner runner = runner("extract_pseudonym_order_ORM_O01.groovy");
        runner.enqueue(orm(delivery("DOWNLOAD", null, null, null)));
        runner.run();

        List<MockFlowFile> series = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS);
        assertEquals(3, series.size());
        for (MockFlowFile flowFile : series) {
            flowFile.assertAttributeEquals("dcma.delivery.type", "DOWNLOAD");
            flowFile.assertAttributeNotExists("dcma.delivery.aet");
            flowFile.assertAttributeNotExists("dcma.delivery.host");
            flowFile.assertAttributeNotExists("dcma.delivery.port");
        }
    }

    @Test
    public void messageWithoutDeliveryIsDownload() throws Exception {
        TestRunner runner = runner("extract_pseudonym_order_ORM_O01.groovy");
        runner.enqueue(orm(new LinkedHashMap<>()));
        runner.run();

        List<MockFlowFile> series = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS);
        assertEquals(3, series.size());
        series.forEach(flowFile -> flowFile.assertAttributeEquals("dcma.delivery.type", "DOWNLOAD"));
    }

    @Test
    public void invalidDeliveryFailsTheWholeMessage() throws Exception {
        List<Map<String, String>> invalid = List.of(
                delivery("DICOM_STORE", "${dcma.order.id}", "hpc.insel.ch", "11112"),
                delivery("DICOM_STORE", "AET_WITH_17_CHARS", "hpc.insel.ch", "11112"),
                delivery("DICOM_STORE", "CAIRO_HPC", "hpc.insel.ch:11112", "11112"),
                delivery("DICOM_STORE", "CAIRO_HPC", "hpc.insel.ch", "0"),
                delivery("DICOM_STORE", "CAIRO_HPC", "hpc.insel.ch", "70000"),
                delivery("DICOM_STORE", null, "hpc.insel.ch", "11112"),
                delivery("DOWNLOAD", "CAIRO_HPC", null, null),
                delivery("NAS", null, null, null));

        for (Map<String, String> values : invalid) {
            TestRunner runner = runner("extract_pseudonym_order_ORM_O01.groovy");
            runner.enqueue(orm(values));
            runner.run();

            runner.assertTransferCount(ExecuteGroovyScript.REL_SUCCESS, 0);
            runner.assertTransferCount(ExecuteGroovyScript.REL_FAILURE, 1);
        }
    }

    // --------------------------------------------------
    // pseudonym_instance_failed.groovy
    // --------------------------------------------------

    @Test
    public void storeFailureIsReportedAsStoreFailed() {
        TestRunner runner = runner("pseudonym_instance_failed.groovy");
        Map<String, String> attributes = instance();
        attributes.put("dicom.store.error.code", "STORE_REJECTED");
        attributes.put("dicom.store.status", "A700");
        attributes.put("dicom.store.error", "Out of resources");
        runner.enqueue(new byte[] {1, 2, 3}, attributes);
        runner.run();

        runner.assertTransferCount(ExecuteGroovyScript.REL_FAILURE, 0);
        List<MockFlowFile> callbacks = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS);
        assertEquals(1, callbacks.size());
        MockFlowFile callback = callbacks.get(0);
        callback.assertAttributeEquals("dcma.instance.error.code", "STORE_FAILED");
        callback.assertAttributeEquals("dcma.event.type", "INSTANCE_FAILED");
        String json = new String(callback.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"errorCode\":\"STORE_FAILED\""), json);
        assertTrue(json.contains(
                "DICOM C-STORE an CAIRO_HPC fehlgeschlagen (STORE_REJECTED, Status A700): Out of resources"), json);
        assertTrue(json.contains("\"originalSopInstanceUid\":\"1.2.840.30001\""), json);
    }

    @Test
    public void otherFailuresAreUnchanged() {
        TestRunner runner = runner("pseudonym_instance_failed.groovy");
        Map<String, String> attributes = instance();
        attributes.put("pseudonymizer.error.message", "bad tag");
        runner.enqueue(new byte[] {1, 2, 3}, attributes);
        runner.run();

        MockFlowFile callback = runner.getFlowFilesForRelationship(ExecuteGroovyScript.REL_SUCCESS).get(0);
        callback.assertAttributeEquals("dcma.instance.error.code", "PSEUDONYMIZE_FAILED");
        assertFalse(new String(callback.toByteArray(), StandardCharsets.UTF_8).contains("STORE_FAILED"));
    }

    // --------------------------------------------------

    private static Map<String, String> instance() {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("dcma.order.id", ORDER_ID);
        attributes.put("dcma.retrieve.id", "66666666-7777-8888-9999-000000000000");
        attributes.put("dcma.study.uid", "1.2.840.10001");
        attributes.put("dcma.series.uid", "1.2.840.20001");
        attributes.put("dcma.original.sop.uid", "1.2.840.30001");
        attributes.put("AffectedSOPInstanceUID", "2.25.123");
        attributes.put("dcma.delivery.aet", "CAIRO_HPC");
        return attributes;
    }

    private static Map<String, String> delivery(String type, String aet, String host, String port) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("DELIVERY_TYPE", type);
        if (aet != null) values.put("DELIVERY_AET", aet);
        if (host != null) values.put("DELIVERY_HOST", host);
        if (port != null) values.put("DELIVERY_PORT", port);
        return values;
    }

    /**
     * ORM^O01 wie dcma-order (OrmMessageBuilder): zwei Studien, drei Serien,
     * Lieferfelder nach den SERIES_UID jeder Studie.
     */
    private static byte[] orm(Map<String, String> delivery) throws Exception {
        try (HapiContext context = new DefaultHapiContext()) {
            ORM_O01 message = context.newMessage(ORM_O01.class);
            message.initQuickstart("ORM", "O01", "P");
            message.getMSH().getCharacterSet(0).setValue("UNICODE UTF-8");
            message.getMSH().getMessageControlID().setValue("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
            message.getPATIENT().getPID().getPatientIdentifierList(0).getIDNumber().setValue("12345678");

            String[][] studies = {
                    {"1.2.840.10001", "1.2.840.20001", "1.2.840.20002"},
                    {"1.2.840.10002", "1.2.840.20003"}};
            for (int s = 0; s < studies.length; s++) {
                message.getORDER(s).getORC().getOrderControl().setValue("NW");
                message.getORDER(s).getORC().getPlacerOrderNumber().getEntityIdentifier()
                        .setValue(ORDER_ID + "-" + (s + 1));
                ORM_O01_ORDER_DETAIL detail = message.getORDER(s).getORDER_DETAIL();
                detail.getOBR().getSetIDOBR().setValue(String.valueOf(s + 1));

                Map<String, String> values = new LinkedHashMap<>();
                values.put("ORDER_ID", ORDER_ID);
                values.put("PROJECT_ID", "CAIRO");
                values.put("PSEUDONYM_PID", "CAIRO-001");
                values.put("PSEUDONYM_NAME", "CAIRO^001");
                values.put("SOURCE_PACS", "SYNEDRA");
                values.put("DEST_AET", "IDSC DCMA-Auftragsportal");
                values.put("STUDY_UID", studies[s][0]);
                values.put("SHAREPOINT_USER", "researcher@insel.ch");
                values.put("SHAREPOINT_EMAIL", "researcher@insel.ch");
                int index = 0;
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    index = obx(detail, index, entry.getKey(), entry.getValue());
                }
                for (int i = 1; i < studies[s].length; i++) {
                    index = obx(detail, index, "SERIES_UID", studies[s][i]);
                }
                for (Map.Entry<String, String> entry : delivery.entrySet()) {
                    index = obx(detail, index, entry.getKey(), entry.getValue());
                }
            }
            return message.encode().getBytes(StandardCharsets.UTF_8);
        }
    }

    private static int obx(ORM_O01_ORDER_DETAIL detail, int index, String code, String value) throws Exception {
        OBX obx = detail.getOBSERVATION(index).getOBX();
        obx.getSetIDOBX().setValue(String.valueOf(index + 1));
        obx.getValueType().setValue("ST");
        obx.getObservationIdentifier().getIdentifier().setValue(code);
        obx.getObservationIdentifier().getText().setValue(code);
        obx.getObservationIdentifier().getNameOfCodingSystem().setValue("99LOCAL");
        ST st = new ST(obx.getMessage());
        st.setValue(value);
        obx.getObservationValue(0).setData(st);
        obx.getObservationResultStatus().setValue("F");
        return index + 1;
    }
}
