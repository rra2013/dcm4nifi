package org.rra.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.cstore.DicomStoreBatch;
import org.rra.cstore.DicomStoreBatch.Destination;
import org.rra.cstore.DicomStoreBatch.Result;
import org.rra.dcmconfig.DcmConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.CPU)

@Tags({"DICOM", "C-STORE", "Store-SCU", "CDP"})

@CapabilityDescription(
        "Sends DICOM instances from incoming FlowFiles to a remote DICOM Storage SCP " +
                "using the DICOM C-STORE protocol. " +
                "Supports dynamic destination configuration through NiFi Expression Language, " +
                "allowing the remote hostname, port, Called AE Title, and Calling AE Title " +
                "to be resolved individually for each FlowFile. " +
                "Provides configurable DICOM association, network timeout, and socket settings. " +
                "Up to 'Batch Size' FlowFiles per run are sent; FlowFiles with the same destination " +
                "share one association. A FlowFile goes to 'success' only if the SCP answered its " +
                "C-STORE with Success or a Warning status; everything else (failure status, SOP class " +
                "or transfer syntax not accepted, association rejected or lost, invalid DICOM) goes " +
                "to 'failure' with an error code. Nothing is transcoded or skipped silently."
)

@UseCase(
        description = "Transfer DICOM images and other DICOM objects to PACS, " +
                "VNA, research archives, or other DICOM Storage SCP systems. " +
                "Dynamically select the destination based on FlowFile attributes, " +
                "enabling project-specific routing, research workflows, " +
                "and automated DICOM forwarding.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED
)

@ReadsAttributes({
        @ReadsAttribute(
                attribute = "pacs.host",
                description = "Optional FlowFile attribute used to dynamically resolve the destination hostname."
        ),
        @ReadsAttribute(
                attribute = "pacs.port",
                description = "Optional FlowFile attribute used to dynamically resolve the destination port."
        ),
        @ReadsAttribute(
                attribute = "pacs.aet",
                description = "Optional FlowFile attribute used to dynamically resolve the Called AE Title."
        )
})

@WritesAttributes({
        @WritesAttribute(
                attribute = StoreScu.STORE_STATUS,
                description = "DIMSE status of the C-STORE-RSP as 4 hex digits (e.g. 0000, B000, A700); "
                        + "missing if no response was received."
        ),
        @WritesAttribute(
                attribute = StoreScu.STORE_WARNING,
                description = "Warning comment of the SCP (success with warning status only)."
        ),
        @WritesAttribute(
                attribute = StoreScu.STORE_ERROR_CODE,
                description = "Failure only: INVALID_DICOM, CONFIG_INVALID, CONNECTION_FAILED, "
                        + "ASSOCIATION_REJECTED, NO_PRESENTATION_CONTEXT, TRANSFER_SYNTAX_NOT_ACCEPTED, "
                        + "STORE_REJECTED or ASSOCIATION_ABORTED."
        ),
        @WritesAttribute(
                attribute = StoreScu.STORE_ERROR,
                description = "Failure only: error message describing why the C-STORE failed."
        ),
        @WritesAttribute(
                attribute = StoreScu.STORE_ERROR_TYPE,
                description = "Failure only: same as dicom.store.error.code (kept for existing flows)."
        )
})
public class StoreScu extends AbstractProcessor {

    public static final String STORE_STATUS = "dicom.store.status";
    public static final String STORE_WARNING = "dicom.store.warning";
    public static final String STORE_ERROR_CODE = "dicom.store.error.code";
    public static final String STORE_ERROR = "dicom.store.error";
    public static final String STORE_ERROR_TYPE = "dicom.store.error.type";

    /** Destination properties could not be evaluated for a FlowFile (e.g. port not a number). */
    public static final String CONFIG_INVALID = "CONFIG_INVALID";

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("Maximum number of FlowFiles sent per run. FlowFiles with the same destination "
                    + "(host, port, Called and Calling AE Title) share one DICOM association; each "
                    + "FlowFile still gets its own C-STORE and its own result.")
            .required(true)
            .defaultValue("50")
            .addValidator(StandardValidators.createLongValidator(1, 1000, true))
            .build();

    public static final PropertyDescriptor REMOTE_HOST = new PropertyDescriptor.Builder()
            .name("remote-address")
            .displayName("Remote Address")
            .description("The address of the Remote Store-SCP server.")
            .required(false)
            .defaultValue("localhost")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("remote-port")
            .displayName("Remote Port")
            .description("The Server Port of DICOM Store-SCP")
            .description("Supports NiFi Expression Language using FlowFile attributes.")
            .required(true)
            .defaultValue("11115")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLED_AET = new PropertyDescriptor.Builder()
            .name("called-AET")
            .displayName("Called AET")
            .description("The AE Title of the remote SCP")
            .description("Supports NiFi Expression Language using FlowFile attributes.")
            .defaultValue("DCM4NIFI")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLING_AET = new PropertyDescriptor.Builder()
            .name("calling-AET")
            .displayName("Calling AET")
            .description("The Calling AE Title of this SCU")
            .description("Supports NiFi Expression Language using FlowFile attributes.")
            .defaultValue("NIFI_SCU")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    //------------------------------------------------------------------------------------------------------------------
    public static final PropertyDescriptor NOT_ASYNC = new PropertyDescriptor.Builder()
            .name("not-async")
            .displayName("Not Async")
            .description("Ignored: C-STOREs are always sent one at a time, so that every response "
                    + "belongs to exactly one FlowFile.")
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
            .description("Failed to send DICOM Data.").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REMOTE_HOST, PORT, CALLED_AET, CALLING_AET,
                BATCH_SIZE,
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
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @OnScheduled
    public void start(final ProcessContext context) {
        final ComponentLog log = getLogger();
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }
        final ComponentLog log = getLogger();

        // Group by destination, keeping the order of the FlowFiles
        Map<Destination, List<FlowFile>> groups = new LinkedHashMap<>();
        for (FlowFile flowFile : flowFiles) {
            Destination destination;
            try {
                destination = destination(context, flowFile);
            } catch (RuntimeException e) {
                log.error("Invalid C-STORE destination for {}: {}", flowFile, e.getMessage());
                fail(session, flowFile, CONFIG_INVALID,
                        "Invalid destination: " + e.getMessage(), -1);
                continue;
            }
            groups.computeIfAbsent(destination, d -> new ArrayList<>()).add(flowFile);
        }

        DicomStoreBatch sender = new DicomStoreBatch(dcmConfig(context));
        for (Map.Entry<Destination, List<FlowFile>> group : groups.entrySet()) {
            List<FlowFile> members = group.getValue();
            List<DicomStoreBatch.Instance> instances = new ArrayList<>(members.size());
            for (FlowFile flowFile : members) {
                instances.add(() -> session.read(flowFile));
            }

            List<Result> results = sender.send(group.getKey(), instances);

            int stored = 0;
            for (int i = 0; i < members.size(); i++) {
                FlowFile flowFile = members.get(i);
                Result result = results.get(i);
                if (result.stored()) {
                    stored++;
                    succeed(session, flowFile, result, group.getKey());
                } else {
                    fail(session, flowFile, result.errorCode(), result.message(), result.status());
                }
            }
            if (stored < members.size()) {
                log.warn("C-STORE to {}: {} of {} instances stored, {} failed: {}",
                        group.getKey(), stored, members.size(), members.size() - stored,
                        DicomStoreBatch.failureSummary(results));
            } else {
                log.debug("C-STORE to {}: {} instances stored", group.getKey(), stored);
            }
        }
    }

    /** Host, port and AE Titles of one FlowFile (Expression Language). */
    static Destination destination(ProcessContext context, FlowFile flowFile) {
        String host = context.getProperty(REMOTE_HOST).evaluateAttributeExpressions(flowFile).getValue();
        String port = context.getProperty(PORT).evaluateAttributeExpressions(flowFile).getValue();
        String called = context.getProperty(CALLED_AET).evaluateAttributeExpressions(flowFile).getValue();
        String calling = context.getProperty(CALLING_AET).evaluateAttributeExpressions(flowFile).getValue();
        int portNumber;
        try {
            portNumber = Integer.parseInt(port == null ? "" : port.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("port is not a number: '" + port + "'");
        }
        return new Destination(host == null ? "localhost" : host, portNumber, calling, called);
    }

    private static DcmConfig dcmConfig(ProcessContext context) {
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
        return dcmConfig;
    }

    private static void succeed(ProcessSession session, FlowFile flowFile, Result result, Destination destination) {
        // Errors of an earlier, retried attempt do not belong to this result
        flowFile = session.removeAllAttributes(flowFile,
                Set.of(STORE_ERROR_CODE, STORE_ERROR, STORE_ERROR_TYPE, STORE_WARNING));
        Map<String, String> attributes = new HashMap<>();
        attributes.put(STORE_STATUS, String.format("%04X", result.status() & 0xFFFF));
        if (result.outcome() == DicomStoreBatch.Outcome.WARNING && result.message() != null) {
            attributes.put(STORE_WARNING, result.message());
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().send(flowFile,
                "dicom://" + destination.calledAet() + "@" + destination.host() + ":" + destination.port());
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static void fail(ProcessSession session, FlowFile flowFile, String code, String message, int status) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(STORE_ERROR_CODE, code);
        attributes.put(STORE_ERROR_TYPE, code);
        attributes.put(STORE_ERROR, message == null ? code : message);
        flowFile = session.removeAllAttributes(flowFile, Set.of(STORE_STATUS, STORE_WARNING));
        if (status >= 0) {
            attributes.put(STORE_STATUS, String.format("%04X", status & 0xFFFF));
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        // Retries via RetryFlowFile do not hammer an unavailable SCP
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
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
