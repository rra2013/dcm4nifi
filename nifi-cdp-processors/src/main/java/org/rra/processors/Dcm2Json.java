package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.dcm.Dicom2JsonTransformer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;


@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "dcm2json", "json"})
@CapabilityDescription("A DICOM JSON Converter. Will convert a DICOM object in JSON during the NIFI Workflows")
@UseCase(description = "Convert a DICOM Object in JSON",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
public class Dcm2Json extends AbstractProcessor {
    public static final String INCLUDE_BULK_DATA = "Include Bulk Data";
    public static final String NO_BULK_DATA = "No Bulk Data";
    public static final String DEFAULT_BULK_URI = "Default";
    public static final PropertyDescriptor BULK_DATA = new PropertyDescriptor
            .Builder()
            .name("bulk-data")
            .displayName("Bulk Data")
            .description("Include bulkdata in XML output; by default, references to bulkdata are included.")
            .required(true)
            .allowableValues(DEFAULT_BULK_URI, NO_BULK_DATA, INCLUDE_BULK_DATA)
            .defaultValue(NO_BULK_DATA)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDENT_JSON = new PropertyDescriptor
            .Builder()
            .name("indent")
            .displayName("Indent")
            .description("Use additional whitespace in JSON output.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENCODE_AS_NUMBER = new PropertyDescriptor
            .Builder()
            .name("encode-as-number")
            .displayName("Encode as number")
            .description("Encode IS, SV and UV values in the range [-(2^53)+1, (2^53)-1] and valid DS values as JSON numbers. By default DS, IS, SV and UV values are encoded as JSON strings.")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRINT_TAG_NAMES = new PropertyDescriptor
            .Builder()
            .name("print-tag-names")
            .displayName("Print Tag Names")
            .description("Use Tag Names in JSON output.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor REMOVE_PRIVAT = new PropertyDescriptor
            .Builder()
            .name("remove-privat")
            .displayName("Remove Privat Tags")
            .description("Don't include private Tags into the JSON output.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the DICOM 2 XML process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DICOM 2 JSON Failed").build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        Boolean inclBulk;
        boolean indent;
        boolean printTagNames;
        boolean removePrivateAttributes;
        boolean encodeAsNumber;
        if (context.getProperty(BULK_DATA).isSet()) {
            String selectedType = context.getProperty(BULK_DATA).evaluateAttributeExpressions().getValue();
            if (selectedType.equalsIgnoreCase(INCLUDE_BULK_DATA)) {
                inclBulk = Boolean.TRUE;
            } else if (selectedType.equalsIgnoreCase(NO_BULK_DATA)) {
                inclBulk = Boolean.FALSE;
            } else {
                inclBulk = null;
            }
            //
        } else {
            inclBulk = null;
        }
        indent = context.getProperty(INDENT_JSON).evaluateAttributeExpressions().asBoolean();
        printTagNames = context.getProperty(PRINT_TAG_NAMES).evaluateAttributeExpressions().asBoolean();
        removePrivateAttributes = context.getProperty(REMOVE_PRIVAT).evaluateAttributeExpressions().asBoolean();
        encodeAsNumber = context.getProperty(ENCODE_AS_NUMBER).evaluateAttributeExpressions().asBoolean();
        getLogger().info("inclBulk:{}, indent:{}, printTagNames:{}", inclBulk, indent, printTagNames);

        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try {
                        Dicom2JsonTransformer.transform(in, buffOut, inclBulk, indent, printTagNames, removePrivateAttributes, encodeAsNumber);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            String fileName = flowFile.getAttribute(CoreAttributes.UUID.key()) + ".json";
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
            session.getProvenanceReporter().modifyContent(flowFile, "dcm2json");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(BULK_DATA, INDENT_JSON, ENCODE_AS_NUMBER, PRINT_TAG_NAMES, REMOVE_PRIVAT);
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


    @OnScheduled
    protected void start(final ProcessContext context) {
        getLogger().info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }
}
