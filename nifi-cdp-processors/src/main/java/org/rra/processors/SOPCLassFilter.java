package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Slf4j
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "filter"})
@CapabilityDescription("A DICOM SOP Class Filter. Will route on Affected SOP Class UID during the NIFI Workflows")
@UseCase(description = "DICOM SOP Class Filter can be used for routing of selected SOP Classes to other processors.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class SOPCLassFilter extends AbstractProcessor {
    public static final PropertyDescriptor FILTER_SOP_CLASS = new PropertyDescriptor
            .Builder()
            .name("SOPClassUID")
            .displayName("SOP Class UID")
            .description("The The SOP Class that will be filtered. A '*' will bypass the object")
            .required(true)
            .defaultValue("*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the filtering process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Filter failed.").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (context.getProperty(FILTER_SOP_CLASS).isSet()) {
            String filterSopIuid = context.getProperty(FILTER_SOP_CLASS).evaluateAttributeExpressions(flowFile).getValue();
            if (filterSopIuid.equals("*")){
                session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                session.transfer(flowFile, REL_SUCCESS);
            }else{
                String affectedSOPClassUID = flowFile.getAttribute("AffectedSOPClassUID");
                if (affectedSOPClassUID.equalsIgnoreCase(filterSopIuid)){
                    session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                    session.transfer(flowFile, REL_SUCCESS);
                } else {
                    session.getProvenanceReporter().route(flowFile, REL_FAILURE);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        }else{
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(FILTER_SOP_CLASS);
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
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>(3);
        validateSOPClassUID(context, results);
        return results;
    }

    private void validateSOPClassUID(ValidationContext context, Collection<ValidationResult> validationResults) {
        String bindAddress = context.getProperty(FILTER_SOP_CLASS).evaluateAttributeExpressions().getValue();
        if (null == bindAddress || bindAddress.equals("")) {
            String explanation = String.format("'%s' is unknown", FILTER_SOP_CLASS.getDisplayName());
            validationResults.add(createValidationResult(FILTER_SOP_CLASS.getDisplayName(), explanation));
        }
    }
    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }
}
