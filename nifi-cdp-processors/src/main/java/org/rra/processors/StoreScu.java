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
import org.rra.cstore.NifiStoreSCU;
import org.rra.dcmconfig.DcmConfig;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)

@Tags({"DICOM", "Store-SCU", "CDP"})
@CapabilityDescription("DICOM Store-SCU. Store a Flow File with DICOM body to remote destination.")
@UseCase(description = "DICOM Store-SCU can be used for Sending DICOM Data to remote SCP",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
public class StoreScu extends AbstractProcessor {

    public static final PropertyDescriptor REMOTE_HOST = new PropertyDescriptor.Builder()
            .name("remote-address")
            .displayName("Remote Address")
            .description("The address of the Remote Store-SCP server.")
            .required(false)
            .defaultValue("localhost")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("remote-port")
            .displayName("Remote Port")
            .description("The Server Port of DICOM Store-SCP")
            .required(true)
            .defaultValue("11115")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLED_AET = new PropertyDescriptor.Builder()
            .name("called-AET")
            .displayName("Called AET")
            .description("The AE Title of the remote SCP")
            .defaultValue("DCM4NIFI")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLING_AET = new PropertyDescriptor.Builder()
            .name("calling-AET")
            .displayName("Calling AET")
            .description("The Calling AE Title of this SCU")
            .defaultValue("NIFI_SCU")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
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
            .description("Failed to send DICOM Data.").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REMOTE_HOST, PORT, CALLED_AET, CALLING_AET,
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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog log = getLogger();
        log.info("+ + + On Data from AET: {} + + +", flowFile.getAttribute("CallingAET"));
        String called_aet = context.getProperty(CALLED_AET).evaluateAttributeExpressions().getValue();
        String calling_aet = context.getProperty(CALLING_AET).evaluateAttributeExpressions().getValue();
        String remoteHost = context.getProperty(REMOTE_HOST).evaluateAttributeExpressions().getValue();
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


        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        try {
            try (InputStream inputStream = session.read(flowFile)) {
                try (BufferedInputStream bis = new BufferedInputStream(inputStream)) {
                    new NifiStoreSCU(remoteHost, port, calling_aet, called_aet, bis, dcmConfig);
                }
            } catch (Exception e) {
                throw e;
            }
            session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
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
