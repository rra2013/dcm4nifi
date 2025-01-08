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
import org.rra.dcm.Dicom2PdfTransformer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"CDP", "DICOM", "dcm2pdf", "pdf"})
@CapabilityDescription("Convert DICOM objects to PDF objets, CDA (object extension xml), STL (object extension stl), " +
        "MTL (object extension mtl), OBJ (object extension obj) or Genozip compressed genomic (object extension genozip) object(s). " +
        "Supported content types are application/pdf (for PDF objects), text/xml (for CDA objects), application/sla or model/stl or " +
        "model/x.stl-binary (for STL objects), model/mtl (for MTL objects), model/obj (for OBJ objects), application/vnd.genozip " +
        "(for Genozip compressed genomic objects), application/prs.vcfbzip2 (for Bzip2 compressed genomic data VCF objects) and " +
        "application/x-bzip2 (for Bzip2 compressed genomic data Document objects).")
@UseCase(description = "Convert a DICOM Object in other documents (pdf, xml, stl, mtl, obj, etc.)",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
public class Dcm2Pdf extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the DICOM 2 PDF process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DICOM 2 PDF Failed").build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        AtomicReference<String> fileExt = new AtomicReference<>(".unknown");
        try {
            flowFile = session.write(flowFile, (in, out) -> {
                try (OutputStream buffOut = new BufferedOutputStream(out)) {
                    try {
                        String ext = Dicom2PdfTransformer.transform(in, buffOut);
                        fileExt.set(ext);
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            String fileName = flowFile.getAttribute(CoreAttributes.UUID.key()) + fileExt.get();
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/" + fileExt.get());
            session.getProvenanceReporter().modifyContent(flowFile, "dcm2pdf");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            log.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
