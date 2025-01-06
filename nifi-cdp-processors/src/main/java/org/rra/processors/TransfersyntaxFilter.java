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
import org.rra.dcm.TransfersyntaxInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Slf4j
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "filter", "Transfer-syntax"})
@CapabilityDescription("A Transfer-syntax Filter. Will route on Transfer-syntax during the NIFI Workflows.")
@UseCase(description = "DICOM Transfer-syntax filter can be used for routing of selected Transfer-syntax to other processors.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class TransfersyntaxFilter extends AbstractProcessor {
    public static final String UNCOMPRESSED = "Uncompressed";
    public static final String VALUE = "Value";

    public static final PropertyDescriptor OBJECT_TYPE = new PropertyDescriptor
            .Builder()
            .name("objectType")
            .displayName("Object Type")
            .description("Type of Transfer-syntax that will be filtered. Select Uncompressed or other by Value of Transfer-Syntax. If Uncompressed is selected the Transfer-Syntax will be ignored.")
            .required(true)
            .allowableValues(VALUE, UNCOMPRESSED)
            .defaultValue(VALUE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRANSFER_SYNTAX = new PropertyDescriptor
            .Builder()
            .name("transfer-syntax")
            .displayName("Transfer-Syntax")
            .description("The The Transfer-syntax that will be filtered. An '*' will bypass the object")
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

        String tsUID = flowFile.getAttribute("TransferSyntax");
        if (null == tsUID || tsUID.isEmpty()) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE, "TransferSyntax not valid");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (context.getProperty(OBJECT_TYPE).isSet()) {
            String selectedType = context.getProperty(OBJECT_TYPE).evaluateAttributeExpressions().getValue();
            if (selectedType.equals(UNCOMPRESSED)){
                if (TransfersyntaxInfo.isUncompressed(tsUID)){
                    session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                    session.transfer(flowFile, REL_SUCCESS);
                }else{
                    session.getProvenanceReporter().route(flowFile, REL_FAILURE);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }else if (selectedType.equals(VALUE)){
                if (context.getProperty(TRANSFER_SYNTAX).isSet()) {
                    String filterTSyntax = context.getProperty(TRANSFER_SYNTAX).evaluateAttributeExpressions(flowFile).getValue();
                    if (filterTSyntax.equals("*")){
                        session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                        session.transfer(flowFile, REL_SUCCESS);
                    }else{
                        String transferSyntax = flowFile.getAttribute("TransferSyntax");
                        if (transferSyntax.equalsIgnoreCase(filterTSyntax)){
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
        }else{
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(OBJECT_TYPE, TRANSFER_SYNTAX);
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
        String sopClass = context.getProperty(TRANSFER_SYNTAX).evaluateAttributeExpressions().getValue();
        if (null == sopClass || sopClass.equals("")) {
            String explanation = String.format("'%s' is unknown", TRANSFER_SYNTAX.getDisplayName());
            validationResults.add(createValidationResult(TRANSFER_SYNTAX.getDisplayName(), explanation));
        }
    }
    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }
}
