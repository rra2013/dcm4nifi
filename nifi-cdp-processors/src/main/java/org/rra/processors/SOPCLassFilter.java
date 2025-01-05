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
import org.dcm4che3.data.Attributes;
import org.rra.dcm.DcmObjectType;
import org.rra.dcm.SOPClassInfo;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.rra.dcm.DicomUtils.readDicomObjectUntilPixelData;

@Slf4j
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "filter", "SOP Class"})
@CapabilityDescription("A DICOM SOP Class Filter. Will route on Affected SOP Class UID during the NIFI Workflows")
@UseCase(description = "DICOM SOP Class Filter can be used for routing of selected SOP Classes to other processors.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class SOPCLassFilter extends AbstractProcessor {
    public final static String UNCOMPRESSED_SINGLE_FRAME_IMAGE = "UncompressedSingleFrameImage";
    public final static String COMPRESSED_SINGLE_FRAME_IMAGE = "CompressedSingleFrameImage";
    public final static String UNCOMPRESSED_MULTI_FRAME_IMAGE = "UncompressedMultiFrameImage";
    public final static String COMPRESSED_MULTI_FRAME_IMAGE = "CompressedMultiFrameImage";
    public final static String MPEG2VIDEO = "MPEG2Video";
    public final static String MPEG4VIDEO = "MPEG4Video";
    public final static String SR_DOCUMENT = "SRDocument";
    public final static String ENCAPSULATED_PDF = "EncapsulatedPDF";
    public final static String ENCAPSULATED_CDA = "EncapsulatedCDA";
    public final static String ENCAPSULATED_STL = "EncapsulatedSTL";
    public final static String ENCAPSULATED_OBJ = "EncapsulatedOBJ";
    public final static String ENCAPSULATED_MTL = "EncapsulatedMTL";
    public final static String ENCAPSULATED_GENOZIP = "EncapsulatedGenozip";
    public final static String ENCAPSULATED_VCFBZIP2 = "EncapsulatedVCFBzip2";
    public final static String ENCAPSULATED_BZIP2 = "EncapsulatedBzip2";
    public final static String OTHER = "Other";
    public final static String ALL = "*";
    public final static String VALUE = "Value";
    public static final PropertyDescriptor OBJECT_TYPE = new PropertyDescriptor
            .Builder()
            .name("ObjectType")
            .displayName("Object Type")
            .description("The Type of the DICOM Object that will be filtered. 'All' or Value '*' will bypass the object")
            .required(true)
            .allowableValues(ALL, VALUE, UNCOMPRESSED_SINGLE_FRAME_IMAGE, COMPRESSED_SINGLE_FRAME_IMAGE,
                    UNCOMPRESSED_MULTI_FRAME_IMAGE, COMPRESSED_MULTI_FRAME_IMAGE,
                    MPEG2VIDEO, MPEG4VIDEO, SR_DOCUMENT, ENCAPSULATED_PDF, ENCAPSULATED_CDA,
                    ENCAPSULATED_STL, ENCAPSULATED_OBJ, ENCAPSULATED_MTL,
                    ENCAPSULATED_GENOZIP, ENCAPSULATED_VCFBZIP2,
                    ENCAPSULATED_BZIP2, OTHER)
            .defaultValue(VALUE)
            .build();
    public static final PropertyDescriptor FILTER_SOP_CLASS = new PropertyDescriptor
            .Builder()
            .name("SOPClassUID")
            .displayName("SOP Class UID")
            .description("The The SOP Class that will be filtered. A '*' will bypass the object and is like the 'All' Selection. This property will be used when 'Value' is selected.")
            .required(true)
            .defaultValue(ALL)
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
        Attributes attr = null;
        try (InputStream read = session.read(flowFile)) {
            attr = readDicomObjectUntilPixelData(read);
        } catch (IOException e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
            log.error("Error", e);
            return;
        }


        if (context.getProperty(OBJECT_TYPE).isSet()){
            String selectedType = context.getProperty(OBJECT_TYPE).evaluateAttributeExpressions().getValue();
            if (selectedType.equals(ALL)){
                session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                session.transfer(flowFile, REL_SUCCESS);
            }
            else if (selectedType.equals(VALUE)){
                if (context.getProperty(FILTER_SOP_CLASS).isSet()) {
                    String filterSopIuid = context.getProperty(FILTER_SOP_CLASS).evaluateAttributeExpressions(flowFile).getValue();
                    if (filterSopIuid.equals("*")) {
                        session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                        session.transfer(flowFile, REL_SUCCESS);
                    } else {
                        String affectedSOPClassUID = flowFile.getAttribute("AffectedSOPClassUID");
                        if (affectedSOPClassUID.equalsIgnoreCase(filterSopIuid)) {
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
            }else {
                String tsuid = flowFile.getAttribute("TransferSyntax");
                SOPClassInfo sop = new SOPClassInfo(attr, tsuid);
                String typeOfObject = DcmObjectType.objectTypeOf(sop).toString();
                if (selectedType.equals(typeOfObject)) {
                    session.getProvenanceReporter().route(flowFile, REL_SUCCESS);
                    session.transfer(flowFile, REL_SUCCESS);
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
        descriptors = List.of(OBJECT_TYPE, FILTER_SOP_CLASS);
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
        String sopClass = context.getProperty(FILTER_SOP_CLASS).evaluateAttributeExpressions().getValue();
        if (null == sopClass || sopClass.equals("")) {
            String explanation = String.format("'%s' is unknown", FILTER_SOP_CLASS.getDisplayName());
            validationResults.add(createValidationResult(FILTER_SOP_CLASS.getDisplayName(), explanation));
        }
    }

    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }
}
