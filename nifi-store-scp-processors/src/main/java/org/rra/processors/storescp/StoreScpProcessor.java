package org.rra.processors.storescp;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.RelationshipConfiguration;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.processors.storescp.dcm.StoreScp;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Tags({"DICOM","Store-SCP"})
@CapabilityDescription("DICOM Store-SCP based on dcm4che")
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class StoreScpProcessor extends AbstractSessionFactoryProcessor {

    private final AtomicReference<ProcessSessionFactory> sessionFactory = new AtomicReference<>();
    private volatile CountDownLatch sessionFactorySetSignal;

    public static final PropertyDescriptor BIND_ADDRESS = new PropertyDescriptor.Builder()
            .name("bind-address")
            .displayName("Bind Address")
            .description("The address the SCP server should be bound to. If not set (or set to 0.0.0.0), "
                    + "the server binds to all available addresses (i.e. all network interfaces of the host machine).")
            .required(false)
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
            .description("The AE Title of the SCP")
            .defaultValue("DCM4NIFI")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Example success relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private StoreScp storeScp;

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
        if (null == storeScp) {
            sessionFactory.set(null);
            sessionFactorySetSignal = new CountDownLatch(1);
            storeScp = new StoreScp(bindAddress, port, aet);
            storeScp.setSessionFactory(sessionFactory);
            storeScp.setSessionFactorySetSignal(sessionFactorySetSignal);
            storeScp.setRelationshipSuccess(REL_SUCCESS);
        }

    }
    @OnStopped
    public void stopStoreSCP(){
        log.info("+ + + Stop the Store SCP + + + ");
        if (null != storeScp) storeScp.shutDown();
        storeScp = null;
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
