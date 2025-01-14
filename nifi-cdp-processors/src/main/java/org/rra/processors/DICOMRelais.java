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
import org.apache.nifi.expression.ExpressionLanguageScope;
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
@Tags({"CDP", "DICOM", "relais"})
@CapabilityDescription("A DICOM Relais. Will route on CALLED AET during the NIFI Workflows")
@UseCase(description = "DICOM Relais can be used for routing of DICOM 3 Objects to remote destinations.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class DICOMRelais extends AbstractProcessor {
    public static final PropertyDescriptor ROUTE_AET = new PropertyDescriptor
            .Builder()
            .name("RouteAET")
            .displayName("Route Called AET")
            .description("The Called AET to Route")
            .required(true)
            .defaultValue("")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the routing process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Routing failed.").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        if (context.getProperty(ROUTE_AET).isSet()) {
            String routeAET = context.getProperty(ROUTE_AET).evaluateAttributeExpressions(flowFile).getValue();
            String calledAET = flowFile.getAttribute("CalledAET");
            if (routeAET.equalsIgnoreCase(calledAET)) {
                session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.getProvenanceReporter().route(flowFile, REL_FAILURE);
                session.transfer(flowFile, REL_FAILURE);
            }
        } else {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(ROUTE_AET);
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
        validateRouteAet(context, results);
        return results;
    }

    private void validateRouteAet(ValidationContext context, Collection<ValidationResult> validationResults) {
        String bindAddress = context.getProperty(ROUTE_AET).evaluateAttributeExpressions().getValue();
        if (null == bindAddress || bindAddress.equals("")) {
            String explanation = String.format("'%s' is unknown", ROUTE_AET.getDisplayName());
            validationResults.add(createValidationResult(ROUTE_AET.getDisplayName(), explanation));
        }
    }
    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }
}
