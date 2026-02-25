package org.rra.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.rra.hl7.HL7Transformer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "HL7", "xml2hl7", "xml"})
@CapabilityDescription("A XML Converter. Will convert a XML object in HL7 MLLP message during the NIFI Workflows")
@UseCase(description = "Convert a XML Object in HL7",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class Xml2HL7 extends AbstractProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the DICOM 2 XML process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DICOM 2 XML Failed").build();


    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog log = getLogger();
        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try {
                        HL7Transformer.transformFromXml(in, buffOut);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            String fileName = flowFile.getAttribute(CoreAttributes.UUID.key()) + ".hl7";
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "text/plain");
            session.getProvenanceReporter().modifyContent(flowFile, "xml2hl7");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            log.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
