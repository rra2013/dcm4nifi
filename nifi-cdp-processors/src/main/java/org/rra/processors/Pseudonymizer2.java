package org.rra.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.rra.deidentify.GeneralAnonymizer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree // Valid only if GeneralAnonymizer.pseudonymize2 has no external side effects.
@SystemResourceConsideration(resource = SystemResource.CPU)
@Tags({"CDP", "DICOM", "deidentify", "pseudonymizer"})
@CapabilityDescription("De-identifies and pseudonymizes DICOM objects, replacing PatientID and PatientName "
        + "with values supplied by FlowFile attributes. Supports optional date shifting and retained DICOM tags.")
@UseCase(description = "De-identify and pseudonymize DICOM objects in a NiFi workflow.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
public class Pseudonymizer2 extends AbstractProcessor {

    public static final String RESULT_ERROR_MESSAGE = "pseudonymizer.error.message";

    public static final PropertyDescriptor PSEUDO_PID = new PropertyDescriptor.Builder()
            .name("pseudo-pid")
            .displayName("Pseudonymization PID")
            .description("Pseudonym PatientID (supports FlowFile Expression Language).")
            .required(true)
            .defaultValue("${dcma.pseudonym.pid}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PSEUDO_NAME = new PropertyDescriptor.Builder()
            .name("pseudo-name")
            .displayName("Pseudonymization Name")
            .description("Pseudonym PatientName (supports FlowFile Expression Language).")
            .required(true)
            .defaultValue("${dcma.pseudonym.name}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATE_SHIFT = new PropertyDescriptor.Builder()
            .name("date-shift")
            .displayName("Date shift")
            .description("Date shift applied by GeneralAnonymizer (units defined by that implementation).")
            .required(false)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETAIN_TAGS = new PropertyDescriptor.Builder()
            .name("Retain Tags")
            .description("Comma-separated DICOM tags retained during pseudonymization, e.g. StudyDate,ContrastBolusAgent.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully pseudonymized DICOM object.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Input cannot be pseudonymized due to invalid or missing parameters.")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(PSEUDO_PID, PSEUDO_NAME, DATE_SHIFT, RETAIN_TAGS);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String pseudoPID;
        final String pseudoName;
        final Integer dateShift;
        final String retainTags;
        try {
            pseudoPID = context.getProperty(PSEUDO_PID).evaluateAttributeExpressions(flowFile).getValue();
            pseudoName = context.getProperty(PSEUDO_NAME).evaluateAttributeExpressions(flowFile).getValue();
            dateShift = context.getProperty(DATE_SHIFT).evaluateAttributeExpressions(flowFile).asInteger();
            retainTags = context.getProperty(RETAIN_TAGS).isSet()
                    ? context.getProperty(RETAIN_TAGS).getValue() : null;
        } catch (IllegalArgumentException e) {
            routeFailure(session, flowFile, "Invalid pseudonymization property value");
            return;
        }

        if (pseudoPID == null || pseudoPID.isBlank() || pseudoName == null || pseudoName.isBlank()) {
            routeFailure(session, flowFile, "Missing pseudonym PID or name");
            return;
        }
        if (dateShift == null) {
            routeFailure(session, flowFile, "Invalid date shift");
            return;
        }

        final AtomicReference<String> studyUID = new AtomicReference<>();
        final AtomicReference<String> seriesUID = new AtomicReference<>();
        final AtomicReference<String> sopUID = new AtomicReference<>();

        // Do not route a FlowFile after a failed session.write: allow NiFi to roll back the session.
        // This also prevents partially rewritten DICOM content from being emitted as success.
        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (BufferedInputStream bufferedIn = new BufferedInputStream(in);
                     BufferedOutputStream bufferedOut = new BufferedOutputStream(out)) {
                    Attributes dcm = GeneralAnonymizer.pseudonymize2(
                            bufferedIn, bufferedOut, retainTags, pseudoPID, pseudoName, dateShift);
                    if (dcm == null) {
                        throw new IOException("Anonymizer returned no DICOM attributes");
                    }
                    String study = dcm.getString(Tag.StudyInstanceUID);
                    String series = dcm.getString(Tag.SeriesInstanceUID);
                    String sop = dcm.getString(Tag.SOPInstanceUID);
                    if (study == null || study.isBlank()
                            || series == null || series.isBlank()
                            || sop == null || sop.isBlank()) {
                        throw new IOException("Anonymized DICOM object is missing Study/Series/SOP Instance UID");
                    }
                    studyUID.set(study);
                    seriesUID.set(series);
                    sopUID.set(sop);
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException("DICOM pseudonymization failed", e);
                }
            });

            flowFile = session.putAttribute(flowFile, "StudyInstanceUID", studyUID.get());
            flowFile = session.putAttribute(flowFile, "SeriesInstanceUID", seriesUID.get());
            flowFile = session.putAttribute(flowFile, "AffectedSOPInstanceUID", sopUID.get());
            flowFile = session.removeAttribute(flowFile, RESULT_ERROR_MESSAGE);
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("DICOM pseudonymization or NiFi session processing failed; rolling back FlowFile", e);
            throw new ProcessException("Pseudonymization failed; NiFi will roll back the session", e);
        }
    }

    private void routeFailure(final ProcessSession session, FlowFile flowFile, final String message) {
        getLogger().warn("Pseudonymization rejected: {}", message);
        flowFile = session.putAttribute(flowFile, RESULT_ERROR_MESSAGE, message);
        session.transfer(flowFile, REL_FAILURE);
    }
}
