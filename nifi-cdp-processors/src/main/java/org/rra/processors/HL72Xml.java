package org.rra.processors;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.parser.XMLParser;
import ca.uhn.hl7v2.util.idgenerator.FileBasedHiLoGenerator;
import ca.uhn.hl7v2.util.idgenerator.InMemoryIDGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.eclipse.jetty.util.IO;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@Slf4j
@Tags({"HL7", "HL72Xml", "CDP"})
@CapabilityDescription("Transform a HL7 MLLP Message to XML.")
@UseCase(description = "Processing HL7 Messages",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class HL72Xml extends AbstractProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Transform success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to XML transform.").build();

    private final static HapiContext context = new DefaultHapiContext();
    private final static PipeParser parser = context.getPipeParser();
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    static {
        context.getParserConfiguration().setIdGenerator(new InMemoryIDGenerator());
        context.getParserConfiguration().setValidating(false);
    }

    private static void transformAndcopyOutput(InputStream in, OutputStream buffOut) throws Exception {

        try (BufferedInputStream bis = new BufferedInputStream(in)) {
            String result = IOUtils.toString(bis, StandardCharsets.UTF_8);
            log.debug("Result: \n {}", result.replaceAll("\\r", "\r\n"));
            Message hapiMsg = parser.parse(result);
            XMLParser xmlParser = context.getXMLParser();
            String xmlDoc = xmlParser.encode(hapiMsg, "XML");
            log.debug(xmlDoc);
            try (ByteArrayInputStream xmlArray = new ByteArrayInputStream(xmlDoc.getBytes(StandardCharsets.UTF_8))) {
                try (BufferedInputStream xmlIn = new BufferedInputStream(xmlArray)) {
                    IO.copy(xmlIn, buffOut);
                }
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try {
                        transformAndcopyOutput(in, buffOut);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".xml";
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/xml");
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            log.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
        descriptors = new ArrayList<>();
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
