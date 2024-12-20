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
import org.rra.dcm.Dicom2JpegTransformer;
import org.rra.dcm.Dicom2XmlTransformer;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageOutputStream;
import java.io.BufferedInputStream;
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
@Tags({"CDP", "DICOM", "dcm2jpeg","jpg"})
@CapabilityDescription("A DICOM Jpeg Converter based on dcm4che. Will convert a DICOM object in Jpeg during the NIFI Workflows")
@UseCase(description = "Convert a DICOM Object in Jpeg",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class Dcm2Jpeg extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the DICOM 2 JPEG process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DICOM 2 JPEG Failed").build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try (ImageOutputStream imageOutputStream = ImageIO.createImageOutputStream(buffOut)){
                        Dicom2JpegTransformer transformer = new Dicom2JpegTransformer();
                        try(BufferedInputStream bis = new BufferedInputStream(in)){
                            transformer.transform(1, bis, imageOutputStream);
                        }
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".jpg";
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "image/jpeg");
            session.getProvenanceReporter().modifyContent(flowFile, "dcm2jpeg");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            log.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
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
