
package org.rra.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.cfind.NifiFindScu;
import org.rra.dcmconfig.DcmConfig;
import org.rra.dcm.DicomUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.rra.dcmconfig.DcmConfig.LEVEL;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@Tags({"DICOM", "Find-SCU", "CDP"})
@CapabilityDescription("Make C-Find query to remote SCP. There are three levels of query. patient/Study, Series and Image level. The input is the Flow File body as String. It will be interpreted in different level. " +
        "In Pat/Study Level is the input the PatientID. In Series and Image level is the input the StudyIUID or (StudyIUID, SeriesIUID). The query result creates a flow file for each result item. " +
        "If the query succeeds without any match, the incoming flow file is routed unchanged to 'not found'.")
@WritesAttributes({
        @WritesAttribute(attribute = FindScu.RESULT_COUNT, description = "Number of C-FIND matches (0 on 'not found')."),
        @WritesAttribute(attribute = FindScu.RESULT_INDEX, description = "0-based index of this match (success only)."),
        @WritesAttribute(attribute = FindScu.SERIES_INSTANCES, description = "NumberOfSeriesRelatedInstances (0020,1209) of this match, "
                + "if the SCP returned it (success only)."),
        @WritesAttribute(attribute = FindScu.FIND_ERROR, description = "Error message (failure only).")
})
@UseCase(description = "DICOM C-Find to query remote archives. Can be used for DICOM Query/Retrieve process.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class FindScu extends AbstractProcessor {
    public static final String PATSTUDY_LEVEL = "PatientStudy";
    public static final String SERIES_LEVEL = "Series";
    public static final String IMAGE_LEVEL = "Image";

    public static final String RESULT_COUNT = "dicom.find.result.count";
    public static final String RESULT_INDEX = "dicom.find.result.index";
    public static final String FIND_ERROR = "dicom.find.error";
    public static final String SERIES_INSTANCES = "dicom.find.series.instances";

    public static final PropertyDescriptor REMOTE_HOST = new PropertyDescriptor.Builder()
            .name("remote-address")
            .displayName("Remote Address")
            .description("The address of the Remote Find-SCP server.")
            .required(true)
            .defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("remote-port")
            .displayName("Remote Port")
            .description("The Server Port of DICOM Find-SCP")
            .required(true)
            .defaultValue("11112")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLED_AET = new PropertyDescriptor.Builder()
            .name("called-AET")
            .displayName("Called AET")
            .description("The AE Title of the remote SCP")
            .defaultValue("DCM4CHEE")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLING_AET = new PropertyDescriptor.Builder()
            .name("calling-AET")
            .displayName("Calling AET")
            .description("The AE Title of this SCU")
            .defaultValue("NIFI_SCU")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor QUERY_LEVEL = new PropertyDescriptor.Builder()
            .name("query-level")
            .displayName("Query Level")
            .description("The Level of C-Find")
            .required(true)
            .allowableValues(PATSTUDY_LEVEL, SERIES_LEVEL, IMAGE_LEVEL)
            .defaultValue(PATSTUDY_LEVEL)
            .build();
    //------------------------------------------------------------------------------------------------------------------
    public static final PropertyDescriptor NOT_ASYNC = new PropertyDescriptor.Builder()
            .name("not-async")
            .displayName("Not Async")
            .description("Do not use asynchronous mode")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    //--------------------------------------------
    public static final PropertyDescriptor NOT_PACK_PDV = new PropertyDescriptor.Builder()
            .name("not-pack-pdv")
            .displayName("Not Pack PDV")
            .description("Send only one PDV in one P-Data-TF PDU; pack command and data PDV in one P-DATA-TF PDU by default.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    //--------------------------------------------
    public static final PropertyDescriptor TCP_DELAY = new PropertyDescriptor.Builder()
            .name("tcp-delay")
            .displayName("TCP Delay")
            .description("Set TCP_NO_DELAY socket option to false, true by default")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    //--------------------------------------------
    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connect-timeout")
            .displayName("CONNECT TIMEOUT")
            .description("Timeout in ms for TCP connect. (0) is no timeout")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("30000")
            .build();
    //--------------------------------------------
    public static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("request-timeout")
            .displayName("REQUEST TIMEOUT")
            .description("Timeout in ms for receiving A-ASSOCIATE-RQ. (0) is no timeout.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("20000")
            .build();

    //--------------------------------------------
    public static final PropertyDescriptor ACCEPT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("accept-timeout")
            .displayName("ACCEPT TIMEOUT")
            .description("Timeout in ms for receiving A-ASSOCIATE-AC. (0) is no timeout.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("20000")
            .build();

    //--------------------------------------------
    public static final PropertyDescriptor RELEASE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("release-timeout")
            .displayName("RELEASE TIMEOUT")
            .description("Timeout in ms for receiving A-RELEASE-RP. (0) is no timeout.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("2000")
            .build();

    //--------------------------------------------
    public static final PropertyDescriptor SEND_TIMEOUT = new PropertyDescriptor.Builder()
            .name("send-timeout")
            .displayName("SEND TIMEOUT")
            .description("Timeout in ms for sending other DIMSE RQs than C-STORE RQs. (0) is no timeout.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8000")
            .build();

    //--------------------------------------------
    public static final PropertyDescriptor STORE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("store-timeout")
            .displayName("STORE TIMEOUT")
            .description("Timeout in ms for sending other DIMSE RQs than C-STORE RQs. (0) is no timeout.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8000")
            .build();


    //--------------------------------------------
    public static final PropertyDescriptor RESPONSE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("response-timeout")
            .displayName("RESPONSE TIMEOUT")
            .description("Timeout in ms for receiving other outstanding DIMSE RSPs than C-MOVE  or C-GET RSPs. (0) is no timeout.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8000")
            .build();


    //--------------------------------------------
    public static final PropertyDescriptor IDLE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("idle-timeout")
            .displayName("IDLE TIMEOUT")
            .description("Timeout in ms for aborting of idle Associations. (0) is no timeout.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("8000")
            .build();

    public static final PropertyDescriptor SND_BUFFER = new PropertyDescriptor.Builder()
            .name("snd-buffer")
            .displayName("SND BUFFER")
            .description("Set the SO_SNDBUF socket option to specified value. Default 0.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    public static final PropertyDescriptor RCV_BUFFER = new PropertyDescriptor.Builder()
            .name("rcv-buffer")
            .displayName("RCV BUFFER")
            .description("Set the SO_RCVBUF socket option to specified value. Default 0.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();

    //------------------------------------------------------------------------------------------------------------------
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Sending success relationship of the SCU")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("C-FIND could not be executed (connection, association, non-success status) " +
                    "or the input is invalid; see " + FIND_ERROR + ".").build();

    /**
     * Without this relationship a query with zero matches emitted nothing and the
     * incoming flow file was silently dropped, e.g. a series deleted in the PACS.
     * Auto-terminate it to keep the old behaviour.
     */
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("The C-FIND succeeded but returned no match. The incoming flow file is routed " +
                    "unchanged, with " + RESULT_COUNT + "=0.").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REMOTE_HOST, PORT, CALLED_AET, CALLING_AET, QUERY_LEVEL,
                NOT_ASYNC,
                NOT_PACK_PDV,
                TCP_DELAY,
                CONNECT_TIMEOUT,
                REQUEST_TIMEOUT,
                ACCEPT_TIMEOUT,
                RELEASE_TIMEOUT,
                SEND_TIMEOUT,
                STORE_TIMEOUT,
                RESPONSE_TIMEOUT,
                IDLE_TIMEOUT,
                SND_BUFFER,
                RCV_BUFFER
        );
        relationships = Set.of(REL_SUCCESS, REL_FAILURE, REL_NOT_FOUND);
    }

    @OnScheduled
    protected void start(final ProcessContext context) {
        final ComponentLog log = getLogger();
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog log = getLogger();
        String patientID;
        String studyIUID = null;
        String seriesIUID = null;
        try (InputStream inputStream = session.read(flowFile)) {
            patientID = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            if (patientID == null || patientID.isBlank()) {
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        String[] split = patientID.split(",");
        if (split.length > 0) {
            studyIUID = split[0].trim();
        }
        if (split.length > 1) {
            seriesIUID = split[1].trim();
        }
        String called_aet = context.getProperty(CALLED_AET).evaluateAttributeExpressions().getValue();
        String calling_aet = context.getProperty(CALLING_AET).evaluateAttributeExpressions().getValue();
        String remoteHost = context.getProperty(REMOTE_HOST).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();

        DcmConfig dcmConfig = new DcmConfig();
        dcmConfig.NOT_ASYNC = context.getProperty(NOT_ASYNC).asBoolean();
        dcmConfig.NOT_PACK_PDV = context.getProperty(NOT_PACK_PDV).asBoolean();
        dcmConfig.TCP_DELAY = context.getProperty(TCP_DELAY).asBoolean();
        dcmConfig.CONNECT_TIMEOUT = context.getProperty(CONNECT_TIMEOUT).asInteger();
        dcmConfig.REQUEST_TIMEOUT = context.getProperty(REQUEST_TIMEOUT).asInteger();
        dcmConfig.ACCEPT_TIMEOUT = context.getProperty(ACCEPT_TIMEOUT).asInteger();
        dcmConfig.RELEASE_TIMEOUT = context.getProperty(RELEASE_TIMEOUT).asInteger();
        dcmConfig.SEND_TIMEOUT = context.getProperty(SEND_TIMEOUT).asInteger();
        dcmConfig.STORE_TIMEOUT = context.getProperty(STORE_TIMEOUT).asInteger();
        dcmConfig.RESPONSE_TIMEOUT = context.getProperty(RESPONSE_TIMEOUT).asInteger();
        dcmConfig.IDLE_TIMEOUT = context.getProperty(IDLE_TIMEOUT).asInteger();
        dcmConfig.SND_BUFFER = context.getProperty(SND_BUFFER).asInteger();
        dcmConfig.RCV_BUFFER = context.getProperty(RCV_BUFFER).asInteger();

        NifiFindScu findSCU;
        String level = context.getProperty(QUERY_LEVEL).evaluateAttributeExpressions().getValue();

        if (level.equals(PATSTUDY_LEVEL)) {
            dcmConfig.FIND_LEVEL = LEVEL.STUDY;
            findSCU = new NifiFindScu(calling_aet, called_aet, remoteHost, port, dcmConfig);
            findSCU.getQueryFilter().setPatientID(patientID);
        } else if (level.equals(SERIES_LEVEL)) {
            dcmConfig.FIND_LEVEL = LEVEL.SERIES;
            findSCU = new NifiFindScu(calling_aet, called_aet, remoteHost, port, dcmConfig);
            if (studyIUID != null && !studyIUID.isBlank()) {
                findSCU.getQueryFilter().setStudyInstanceUID(studyIUID);
                log.info("StudyInstanceUID set to {}", studyIUID);
            }

            if (seriesIUID != null && !seriesIUID.isBlank()) {
                findSCU.getQueryFilter().setSeriesInstanceUID(seriesIUID);
                log.info("SeriesInstanceUID set to {}", seriesIUID);
            }

            if (studyIUID == null || studyIUID.isBlank()) {
                log.info("# # # No Values for the series Level is set # # # studyIUID {}, seriesIUID {}", studyIUID, seriesIUID);
                log.error("# # # No Values for the series Level is set # # #");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

        } else if (level.equals(IMAGE_LEVEL)) {
            dcmConfig.FIND_LEVEL = LEVEL.IMAGE;
            findSCU = new NifiFindScu(calling_aet, called_aet, remoteHost, port, dcmConfig);
            if (studyIUID == null || studyIUID.isBlank() || seriesIUID == null || seriesIUID.isBlank()) {
                log.error("IMAGE level requires StudyInstanceUID,SeriesInstanceUID");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            findSCU.getQueryFilter().setStudyInstanceUID(studyIUID);
            findSCU.getQueryFilter().setSeriesInstanceUID(seriesIUID);
        } else {
            log.error("# # # No Level is set # # #");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        List<org.dcm4che3.data.Attributes> results;
        try {
            results = executeQuery(findSCU);
        } catch (Exception e) {
            log.error("DICOM C-FIND failed", e);
            flowFile = session.putAttribute(flowFile, FIND_ERROR, String.valueOf(e.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (results.isEmpty()) {
            // Only the level is logged: the input may be a PatientID.
            log.warn("C-FIND on level {} returned no match; routing to '{}'", level, REL_NOT_FOUND.getName());
            flowFile = session.putAttribute(flowFile, RESULT_COUNT, "0");
            session.transfer(flowFile, REL_NOT_FOUND);
            return;
        }

        // The query has finished. Only now create and transfer FlowFiles on this thread.
        List<FlowFile> created = new ArrayList<>();
        String count = String.valueOf(results.size());
        try {
            for (int index = 0; index < results.size(); index++) {
                org.dcm4che3.data.Attributes attributes = results.get(index);
                FlowFile item = session.create(flowFile);
                created.add(item);
                item = session.write(item, outputStream -> {
                    BufferedOutputStream bos = new BufferedOutputStream(outputStream);
                    DicomUtils.copyAttributesToOutput(attributes, bos);
                    bos.flush();
                });
                created.set(created.size() - 1, item);
                // An error of an earlier, retried attempt does not belong to the match.
                item = session.removeAttribute(item, FIND_ERROR);
                // Expected instances of the series, e.g. for the C-GET result;
                // only if the SCP returned a valid count.
                String instances = seriesInstances(attributes);
                item = instances == null
                        ? session.removeAttribute(item, SERIES_INSTANCES)
                        : session.putAttribute(item, SERIES_INSTANCES, instances);
                item = session.putAllAttributes(item, Map.of(
                        CoreAttributes.MIME_TYPE.key(), "application/dicom",
                        RESULT_COUNT, count,
                        RESULT_INDEX, String.valueOf(index)));
                created.set(created.size() - 1, item);
            }
            for (FlowFile item : created) {
                session.transfer(item, REL_SUCCESS);
            }
            session.remove(flowFile);
        } catch (Exception e) {
            log.error("Could not create C-FIND result FlowFiles", e);
            for (FlowFile item : created) {
                session.remove(item);
            }
            flowFile = session.putAttribute(flowFile, FIND_ERROR, String.valueOf(e.getMessage()));
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    /** NumberOfSeriesRelatedInstances as text, or null if absent or invalid. */
    static String seriesInstances(org.dcm4che3.data.Attributes attributes) {
        if (!attributes.containsValue(org.dcm4che3.data.Tag.NumberOfSeriesRelatedInstances)) {
            return null;
        }
        int count = attributes.getInt(org.dcm4che3.data.Tag.NumberOfSeriesRelatedInstances, -1);
        return count < 0 ? null : String.valueOf(count);
    }

    /**
     * Runs the query and collects all matches. C-FIND callbacks execute on a dcm4che
     * thread, so the ProcessSession must not be used there. Protected for tests.
     */
    protected List<org.dcm4che3.data.Attributes> executeQuery(NifiFindScu findSCU) throws Exception {
        List<org.dcm4che3.data.Attributes> results = new ArrayList<>();
        findSCU.doQuery(null, attributes -> {
            synchronized (results) {
                results.add(new org.dcm4che3.data.Attributes(attributes));
            }
        });
        synchronized (results) {
            return new ArrayList<>(results);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
}