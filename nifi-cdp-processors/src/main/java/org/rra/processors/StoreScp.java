package org.rra.processors;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.cstore.NifiStoreScp;
import org.rra.dcmconfig.DcmConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"DICOM", "Store-SCP", "CDP"})
@CapabilityDescription("DICOM Store-SCP. Store a DICOM Object to a Flow File.")
@UseCase(description = "Receives DICOM Images via TCP/IP. Listening on port and bind to IP address. This DICOM Store-SCP receives the DICOM objects and create a Flow File with the dcm4che Attributes as File content. Images or PDFs will be included. The hole DICOM Object will be transferred in Default Transfer syntax Explicit Little Endian.")
@WritesAttributes({
        @WritesAttribute(attribute = "AffectedSOPClassUID", description = "The Affected SOP Class UID"),
        @WritesAttribute(attribute = "AffectedSOPInstanceUID", description = "The Affected SOP Instance UID"),
        @WritesAttribute(attribute = "TransferSyntax", description = "The Transfer Syntax of the DICOM Object"),
        @WritesAttribute(attribute = "CallingAET", description = "The Calling AET of the Associate AC"),
        @WritesAttribute(attribute = "CalledAET", description = "The Called AET of the Associate AC"),
        @WritesAttribute(attribute = "StudyInstanceUID", description = "The StudyInstanceUID of the data"),
        @WritesAttribute(attribute = "SeriesInstanceUID", description = "The SeriesInstanceUID of the data"),
        @WritesAttribute(attribute = "PatientID", description = "The PatientID of the data"),
        @WritesAttribute(attribute = "Modality", description = "The Modality of the data"),
        @WritesAttribute(attribute = "HexStudyIUID", description = "The Hex value of StudyInstanceUID"),
        @WritesAttribute(attribute = "HexSeriesIUID", description = "The Hex value of SeriesInstanceUID")
})

public class StoreScp extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor BIND_ADDRESS = new PropertyDescriptor.Builder()
            .name("bind-address")
            .displayName("Bind Address")
            .description("The address the SCP server should be bound to. If not set (or set to 0.0.0.0), "
                    + "the server binds to all available addresses (i.e. all network interfaces of the host machine).")
            .required(false)
            .defaultValue("0.0.0.0")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("listening-port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming connections. On Linux, root privileges are required to use port numbers below 1024.")
            .required(true)
            .defaultValue("11113")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor AET = new PropertyDescriptor.Builder()
            .name("AET")
            .displayName("AET")
            .description("The AE Title of the SCP. A '*' will accept any Association")
            .defaultValue("DCM4NIFI")
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
            .description("Receive DICOM Object success")
            .build();


    private final AtomicReference<ProcessSessionFactory> sessionFactory = new AtomicReference<>();
    private volatile CountDownLatch sessionFactorySetSignal;
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private NifiStoreScp nifiStoreScp;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(BIND_ADDRESS, PORT, AET,
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

        relationships = Set.of(REL_SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void startStoreSCP(final ProcessContext context) {
        final ComponentLog log = getLogger();
        if (null == nifiStoreScp) {
            sessionFactory.set(null);
            String aet = context.getProperty(AET).evaluateAttributeExpressions().getValue();
            String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
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

            log.info("+ + + Start the Store SCP {}@{}:{} + + +", aet, bindAddress, port);
            try {
                sessionFactorySetSignal = new CountDownLatch(1);
                nifiStoreScp = new NifiStoreScp(bindAddress, port, aet, dcmConfig);
                nifiStoreScp.setSessionFactory(sessionFactory);
                nifiStoreScp.setSessionFactorySetSignal(sessionFactorySetSignal);
                nifiStoreScp.setRelationshipSuccess(REL_SUCCESS);
                nifiStoreScp.start();
            } catch (ProcessException processException) {
                log.error(processException.getMessage(), processException);
                stopStoreSCP();
                throw processException;
            }
        } else {
            getLogger().warn("SCP server already started.");
        }

    }

    @OnStopped
    public void stopStoreSCP() {
        final ComponentLog log = getLogger();
        log.info("+ + + Stop the Store SCP + + + ");
        if (null != nifiStoreScp) nifiStoreScp.shutDown();
        nifiStoreScp = null;
        sessionFactory.set(null);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.sessionFactory.compareAndSet(null, sessionFactory)) {
            sessionFactorySetSignal.countDown();
        }
        context.yield();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>(3);

        validateBindAddress(context, results);

        return results;
    }

    private void validateBindAddress(ValidationContext context, Collection<ValidationResult> validationResults) {
        String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
        try {
            InetAddress.getByName(bindAddress);
        } catch (UnknownHostException e) {
            String explanation = String.format("'%s' is unknown", BIND_ADDRESS.getDisplayName());
            validationResults.add(createValidationResult(BIND_ADDRESS.getDisplayName(), explanation));
        }
    }

    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }
}
