package org.rra.processors;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.llp.LLPException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.cstore.NifiStoreSCU;
import org.rra.hl7.HL7Sender;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@Slf4j
@Tags({"HL7", "HL7Send", "CDP"})
@CapabilityDescription("Transform a HL7 MLLP Message to remote destination.")
@UseCase(description = "Send HL7 Messages",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
public class HL7Send extends AbstractProcessor {
    public static final PropertyDescriptor REMOTE_HOST = new PropertyDescriptor.Builder()
            .name("remote-address")
            .displayName("Remote Address")
            .description("The address of the Remote HL7 server.")
            .required(false)
            .defaultValue("localhost")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("remote-port")
            .displayName("Remote Port")
            .description("The Server Port of remote HL7 Server")
            .required(true)
            .defaultValue("5000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Transform success")
            .build();

    public static final Relationship REL_ACKNOWLEDGE  = new Relationship.Builder()
            .name("acknowledge")
            .description("Transform acknowledgment")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to XML transform.").build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private String remoteHost;
    private Integer port;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REMOTE_HOST, PORT);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (BufferedInputStream bis = new BufferedInputStream(in)) {
                    String hl7 = IOUtils.toString(bis, StandardCharsets.UTF_8);
                    String response = HL7Sender.send(remoteHost, port, hl7);
                    try (OutputStream buffOut = new BufferedOutputStream(out)) {
                        IOUtils.write(response, buffOut);
                    }
                } catch (HL7Exception e) {
                    throw new ProcessException(e);
                } catch (LLPException e) {
                    throw new ProcessException(e);
                }
            });

            session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @OnScheduled
    public void start(final ProcessContext context) {
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
        remoteHost = context.getProperty(REMOTE_HOST).evaluateAttributeExpressions().getValue();
        port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
    }
}
