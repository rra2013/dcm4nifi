package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.dcm.Dicom2XmlTransformer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "dcm2xml","xml"})
@CapabilityDescription("A DICOM XML Converter based on dcm4che. Will convert a DICOM object in XML during the NIFI Workflows")
@UseCase(description = "Convert a DICOM Object in XML",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class Dcm2Xml extends AbstractProcessor {

    public static final String INCLUDE_BULK_DATA = "Include Bulk Data";
    public static final String NO_BULK_DATA = "No Bulk Data";

    public static final PropertyDescriptor BULK_DATA = new PropertyDescriptor
            .Builder()
            .name("bulk-data")
            .displayName("Bulk Data")
            .description("Include bulkdata in XML output; by default, references to bulkdata are included.")
            .required(true)
            .allowableValues(NO_BULK_DATA, INCLUDE_BULK_DATA)
            .defaultValue(NO_BULK_DATA)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor XSL_TRANSFORM_PATH = new PropertyDescriptor
            .Builder()
            .name("xsl-file")
            .displayName("XSL File Path")
            .description("Apply XSLT stylesheet specified by file path or URL.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the DICOM 2 XML process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DICOM 2 XML Failed").build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final boolean inclBulk;
        final String xslTransformPath;
        if (context.getProperty(BULK_DATA).isSet()){
            String selectedType = context.getProperty(BULK_DATA).evaluateAttributeExpressions().getValue();
            if (selectedType.equalsIgnoreCase(INCLUDE_BULK_DATA)){
              inclBulk = true;
            } else{
                inclBulk = false;
            }
            //
            xslTransformPath = context.getProperty(XSL_TRANSFORM_PATH).evaluateAttributeExpressions(flowFile).getValue();
        }else{
            return;
        }
        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try {
                        Dicom2XmlTransformer.transform(in, buffOut, inclBulk, xslTransformPath);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            String fileName = flowFile.getAttribute(CoreAttributes.UUID.key()) + ".xml";
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/xml");
            session.getProvenanceReporter().modifyContent(flowFile, "dcm2xml");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            log.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(BULK_DATA, XSL_TRANSFORM_PATH);
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

}
