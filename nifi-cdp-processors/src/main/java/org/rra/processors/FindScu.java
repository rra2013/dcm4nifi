package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.rra.cfind.NifiFindScu;
import org.rra.dcm.DicomUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.rra.cfind.NifiFindScu.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@Slf4j
@Tags({"DICOM", "Find-SCU", "CDP"})
@CapabilityDescription("Make C-Find query to remote SCP. There are three levels of query. patient/Study, Series and Image level. The input is the Flow File body as String. It will be interpreted in different level. In Pat/Study Level is the input the PatientID. In Series and Image level is the input the StudyIUID. The query result creates a flow file for each result item.")
@UseCase(description = "DICOM C-Find to query remote archives. Can be used for DICOM Query/Retrieve process.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)

public class FindScu extends AbstractProcessor {
    public static final String PATSTUDY_LEVEL = "PatientStudy";
    public static final String SERIES_LEVEL = "Series";
    public static final String IMAGE_LEVEL = "Image";

    public static final PropertyDescriptor REMOTE_HOST = new PropertyDescriptor.Builder()
            .name("remote-address")
            .displayName("Remote Address")
            .description("The address of the Remote Find-SCP server.")
            .required(true)
            .defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("remote-port")
            .displayName("Remote Port")
            .description("The Server Port of DICOM Find-SCP")
            .required(true)
            .defaultValue("11112")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLED_AET = new PropertyDescriptor.Builder()
            .name("called-AET")
            .displayName("Called AET")
            .description("The AE Title of the remote SCP")
            .defaultValue("DCM4CHEE")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor CALLING_AET = new PropertyDescriptor.Builder()
            .name("calling-AET")
            .displayName("Calling AET")
            .description("The AE Title of this SCU")
            .defaultValue("NIFI_SCU")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor QUERY_LEVEL = new PropertyDescriptor.Builder()
            .name("query-level")
            .displayName("Query Level")
            .description("The Level of C-Find")
            .required(true)
            .allowableValues(PATSTUDY_LEVEL, SERIES_LEVEL, IMAGE_LEVEL)
            .defaultValue(PATSTUDY_LEVEL)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Sending success relationship of the SCU")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to send DICOM Data.").build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;



    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(REMOTE_HOST, PORT, CALLED_AET, CALLING_AET, QUERY_LEVEL);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @OnScheduled
    protected void start(final ProcessContext context) {
        log.info("+ + + Start {} OK. + + +", getClass().getSimpleName());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String input = "";
        try (InputStream inputStream = session.read(flowFile)) {
            input = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            if (null == input || input.equals("")) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        String called_aet = context.getProperty(CALLED_AET).evaluateAttributeExpressions().getValue();
        String calling_aet = context.getProperty(CALLING_AET).evaluateAttributeExpressions().getValue();
        String remoteHost = context.getProperty(REMOTE_HOST).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();

        NifiFindScu findSCU;
        String level = context.getProperty(QUERY_LEVEL).evaluateAttributeExpressions().getValue();
        if (level.equals(PATSTUDY_LEVEL)) {
            findSCU = new NifiFindScu(calling_aet, called_aet, remoteHost, port, QUERY_LEVEL_PATIENT_STUDY);
            findSCU.getQueryFilter().setPatientID(input);
        } else if (level.equals(SERIES_LEVEL)) {
            findSCU = new NifiFindScu(calling_aet, called_aet, remoteHost, port, QUERY_LEVEL_SERIES);
            findSCU.getQueryFilter().setStudyInstanceUID(input);
        } else if (level.equals(IMAGE_LEVEL)) {
            findSCU = new NifiFindScu(calling_aet, called_aet, remoteHost, port, QUERY_LEVEL_IMAGE);
            findSCU.getQueryFilter().setStudyInstanceUID(input);
        } else {
            log.error("# # # No Level is set # # #");
            return;
        }

        try {
            findSCU.doQuery(remote -> {
                session.remove(flowFile);
            },attributes -> {
                try {
                    final long t1 = System.nanoTime();
                    FlowFile qResItem = session.create();
                    try (OutputStream outputStream = session.write(qResItem)) {
                        try (BufferedOutputStream bos = new BufferedOutputStream(outputStream)) {
                            DicomUtils.copyAttributesToOutput(attributes, bos);
                        }
                    } catch (SocketException socketException) {
                        log.error("Socket exception during data transfer", socketException);
                        session.rollback();
                        throw new IOException(socketException.getMessage());
                    } catch (IOException ioException) {
                        log.error("IOException during data transfer", ioException);
                        session.rollback();
                        throw new IOException(ioException.getMessage());
                    }
                    qResItem = session.putAttribute(qResItem, CoreAttributes.MIME_TYPE.key(), "application/dicom");
                    final long importNanos = System.nanoTime() - t1;
                    final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
                    session.getProvenanceReporter().receive(qResItem, called_aet, importMillis);
                    session.transfer(qResItem, REL_SUCCESS);
                    session.commitAsync(() -> {
                        log.info("Flow File Commit OK.");
                    });
                } catch (Exception e) {
                    session.rollback();
                    log.error("Process session error. ", e);
                }
            });
        } catch (Exception e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }

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
