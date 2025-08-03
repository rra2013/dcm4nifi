package org.rra.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.rra.deidentify.GeneralAnonymizer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@Tags({"CDP","DICOM", "deidentify"})
@CapabilityDescription("A DICOM De-Identifier. Will deidentify DICOM Objects during the NIFI Workflows")
@UseCase(description = "The De-Identifier can be used for anonymizing DICOM Meta Data of DICOM 3 Objects",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
public class Deidentify extends AbstractProcessor {


   /* public static final PropertyDescriptor DEIDENT_MODEL = new PropertyDescriptor
            .Builder()
            .name("Model")
            .displayName("Model")
            .description("The deidentify model for the process")
            .required(true)
            .defaultValue("deidentify.json")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();*/

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the de-identification process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to deidentify attributes.").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();//List.of(DEIDENT_MODEL);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        ComponentLog log = getLogger();
        log.info("+ + + On Data from AET: {} + + +", flowFile.getAttribute("CallingAET"));
        try {
            AtomicReference<String> anonymStudyIUID = new AtomicReference("NO_UID");
            AtomicReference<String> anonymSeriesIUID = new AtomicReference("NO_UID");
            AtomicReference<String> anonymSOPIUID = new AtomicReference("NO_UID");
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try (InputStream buffIn = new BufferedInputStream(in)) {
                        Attributes dcm = GeneralAnonymizer.anonymize(buffIn, buffOut);
                        String sopIUID = dcm.getString(Tag.SOPInstanceUID);
                        anonymSOPIUID.set(sopIUID);

                        String studyIUID = dcm.getString(Tag.StudyInstanceUID);
                        anonymStudyIUID.set(studyIUID);

                        String seriesIUID = dcm.getString(Tag.SeriesInstanceUID);
                        anonymSeriesIUID.set(seriesIUID);
                        log.debug(" + + + StudyInstanceUID: {}", studyIUID);
                        log.debug(" + + + SeriesInstanceUID:{}", seriesIUID);
                        log.debug(" + + + SOPInstanceUID: {}", sopIUID);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception exception) {
                    throw exception;
                }
            });
            flowFile = session.putAttribute(flowFile, "StudyInstanceUID", anonymStudyIUID.get());
            flowFile = session.putAttribute(flowFile, "SeriesInstanceUID", anonymSeriesIUID.get());
            flowFile = session.putAttribute(flowFile, "AffectedSOPInstanceUID", anonymSOPIUID.get());
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            log.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }


}
