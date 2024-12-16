package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.store.DcmStoreScp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Tags({"DICOM","Store-SCP", "CDP"})
@CapabilityDescription("DICOM Store-SCP based on dcm4che. Store a DICOM Object to a Flow File.")
@UseCase(description = "Receives DICOM Images via TCP/IP. Listening on port and bind to IP address. This DICOM Store-SCP receives the DICOM objects and create a Flow File with the dcm4che Attributes as File content. Images or PDFs will be included. The hole DICOM Object will be transferred in Default Transfer syntax Explicit Little Endian.")
@WritesAttributes({
        @WritesAttribute(attribute="AffectedSOPClassUID", description="The Affected SOP Class UID"),
        @WritesAttribute(attribute="AffectedSOPInstanceUID", description="The Affected SOP Instance UID"),
        @WritesAttribute(attribute="TransferSyntax", description="The Transfer Syntax of the DICOM Object"),
        @WritesAttribute(attribute="CallingAET", description="The Calling AET of the Associate AC"),
        @WritesAttribute(attribute="CalledAET", description="The Called AET of the Associate AC")
})

public class StoreScp extends AbstractSessionFactoryProcessor {

    private final AtomicReference<ProcessSessionFactory> sessionFactory = new AtomicReference<>();
    private volatile CountDownLatch sessionFactorySetSignal;

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
            .defaultValue("11115")
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Receive DICOM Object success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private DcmStoreScp dcmStoreScp;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(BIND_ADDRESS, PORT, AET);

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
        String aet = context.getProperty(AET).evaluateAttributeExpressions().getValue();
        String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        log.info("+ + + Start the Store SCP {}@{}:{} + + +", aet, bindAddress, port);
        if (null == dcmStoreScp) {
            sessionFactorySetSignal = new CountDownLatch(1);
            sessionFactory.set(null);
            dcmStoreScp = new DcmStoreScp(bindAddress, port, aet);
            dcmStoreScp.setSessionFactory(sessionFactory);
            dcmStoreScp.setSessionFactorySetSignal(sessionFactorySetSignal);
            dcmStoreScp.setRelationshipSuccess(REL_SUCCESS);
        }

    }
    @OnStopped
    public void stopStoreSCP(){
        log.info("+ + + Stop the Store SCP + + + ");
        if (null != dcmStoreScp) dcmStoreScp.shutDown();
        dcmStoreScp = null;
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
