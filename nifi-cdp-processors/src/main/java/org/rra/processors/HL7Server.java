package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.hl7.NifiHL7HapiServer;
import org.rra.hl7.IHL7Server;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Tags({"HL7","server","MLLP", "CDP"})
@CapabilityDescription("A HL7 MLLP server based on hapi")
@UseCase(description = "Receives HL7 Objects via TCP/IP and MLLP. Listening on port and bind to IP address. ")
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class HL7Server extends AbstractSessionFactoryProcessor {

    private final AtomicReference<ProcessSessionFactory> sessionFactory = new AtomicReference<>();
    private volatile CountDownLatch sessionFactorySetSignal;
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("listening-port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming connections. On Linux, root privileges are required to use port numbers below 1024.")
            .required(true)
            .defaultValue("5000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("message-type")
            .displayName("Message Type")
            .description("The message type of the HL7 than only be accepted. For example ADT. With * all types will be accepted")
            .required(true)
            .defaultValue("*")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor TRIGGER_EVENT = new PropertyDescriptor.Builder()
            .name("trigger-event")
            .displayName("Trigger Event")
            .description("The trigger event of the message that only be accepted. For example A01. With * all events would be accepted.")
            .required(false)
            .defaultValue("*")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Receive HL7 success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private IHL7Server server;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(PORT, MESSAGE_TYPE, TRIGGER_EVENT);
        relationships = Set.of(REL_SUCCESS);
    }
    @OnScheduled
    public void startHL7Server(final ProcessContext context) {
        if (null == server) {
            sessionFactory.set(null);
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            String msgType = context.getProperty(MESSAGE_TYPE).evaluateAttributeExpressions().getValue();
            String trigger = context.getProperty(TRIGGER_EVENT).evaluateAttributeExpressions().getValue();
            try {
                sessionFactorySetSignal = new CountDownLatch(1);
                server = new NifiHL7HapiServer(sessionFactory, sessionFactorySetSignal, REL_SUCCESS);
                server.startServer(port, msgType, trigger);
            } catch (ProcessException processException) {
                log.error(processException.getMessage(), processException);
                stopHL7Server();
                throw processException;
            }
        }else{
            log.info("Server is all ready started");
        }
    }
    @OnStopped
    public void stopHL7Server(){
        log.info("+ + + Stop the HL7 Server + + + ");
        if (null != server) server.stopServer();
        server = null;
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
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

}
