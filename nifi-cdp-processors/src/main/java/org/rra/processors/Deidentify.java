package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.rra.deidentify.GeneralAnonymizer;
import org.rra.deidentify.model.DeidentifyModel;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@Tags({"CDP","DICOM", "deidentify"})
@CapabilityDescription("A DICOM De-Identifier based on dcm4che. Will deidentify DICOM Objects during the NIFI Workflows")
@UseCase(description = "De-Identifier can be used for anonymizing DICOM Meta Data of DICOM 3 Objects",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
public class Deidentify extends AbstractProcessor {


    public static final PropertyDescriptor DEIDENT_MODEL = new PropertyDescriptor
            .Builder()
            .name("Model")
            .displayName("Model")
            .description("The deidentify model for the process")
            .required(true)
            .defaultValue("deidentify.json")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

   /* public static final PropertyDescriptor BOOLEAN = new PropertyDescriptor.Builder()
            .name("Pretty Print")
            .displayName("Pretty Print")
            .description("Apply pretty print formatting to the output.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(DEIDENT_MODEL)
            .build();
*/
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
        descriptors = List.of(DEIDENT_MODEL/*, BOOLEAN*/);
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
        DeidentifyModel deidentifyModel = DeidentifyModel.getModel();
        if (null == deidentifyModel) {
            log.error("The Deidentify Model is null");
            return;
        }
        log.info("+ + + On Data from AET: {} + + +", flowFile.getAttribute("CallingAET"));
        try {
            AtomicReference<String> anonymSOPIUID = new AtomicReference("NO_UID");
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try (InputStream buffIn = new BufferedInputStream(in)) {
                        Attributes dcm = GeneralAnonymizer.anonymize(buffIn, buffOut, deidentifyModel);
                        String sopIUID = dcm.getString(Tag.SOPInstanceUID);
                        anonymSOPIUID.set(sopIUID);
                        log.debug(" + + + StudyInstanceUID: {}", dcm.getString(Tag.StudyInstanceUID));
                        log.debug(" + + + SeriesInstanceUID:{}", dcm.getString(Tag.SeriesInstanceUID));
                        log.debug(" + + + SOPInstanceUID: {}", sopIUID);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception exception) {
                    throw exception;
                }
            });
            flowFile = session.putAttribute(flowFile, "AffectedSOPInstanceUID", anonymSOPIUID.get());
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            log.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }


}
