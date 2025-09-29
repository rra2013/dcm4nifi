package org.rra.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.rra.deidentify.GeneralAnonymizer;
import org.rra.deidentify.PseudonymLookupData;

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@SystemResourceConsideration(resource = SystemResource.CPU)
@Tags({"CDP","DICOM", "deidentify", "pseudonymizer"})
@CapabilityDescription("A DICOM De-Identifier and pseudonymizer. Will deidentify DICOM Objects and replace PID"
        +" and Patient NAME via Database lookup during the NIFI Workflows."
        +"The character ? will be replaced with the PID of the flow file DIOCM Data for query of the pseudo IDs. (Tag.PatientID, VR.LO, prefix+\"-\"+postfix), "
        +"(Tag.PatientName, VR.PN, prefix+\"^\"+postfix). If a dateShift is selected [date_shift] then the AcquisitionDate will be retained and shifted by the value.")
@UseCase(description = "The pseudonymizer can be used for de-identify and pseudonymize DICOM Meta Data of DICOM 3 Objects.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED)
public class Pseudonymizer extends AbstractProcessor {
    public static final String RESULT_ERROR_MESSAGE = "executesql.error.message";

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL select query")
            .description("The SQL select query to execute. Like 'SELECT pid, prefix, postfix, [date_shift] FROM pseudonym_table WHERE pid=?'. The '?' will be replaced with the affected PID.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship of the pseudonymizing process")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to pseudonymize attributes.").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private DBCPService dbcpService;



    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(DBCP_SERVICE, SQL_SELECT_QUERY);
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

    @OnScheduled
    public void setup(ProcessContext context) {
        if (!context.getProperty(SQL_SELECT_QUERY).isSet()) {
            final String errorString = "The Select Query must be specified";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        getLogger().info("Setting up DBCP service successfully.");
    }
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null ) {
            return;
        }
        final ComponentLog log = getLogger();
        final String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(flowFile).getValue();
        }else{
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        log.info("+ + + On Data from AET: {} + + +", flowFile.getAttribute("CallingAET"));
        try (final Connection con = dbcpService.getConnection()) {
            try {
                AtomicReference<String> anonymStudyIUID = new AtomicReference("NO_UID");
                AtomicReference<String> anonymSeriesIUID = new AtomicReference("NO_UID");
                AtomicReference<String> anonymSOPIUID = new AtomicReference("NO_UID");
                flowFile = session.write(flowFile, (in, out) -> {
                    try (OutputStream buffOut = new BufferedOutputStream(out)) {
                        try (InputStream buffIn = new BufferedInputStream(in)) {
                            Attributes dcm = GeneralAnonymizer.pseudonymize(buffIn, buffOut, pid -> {
                                log.info("Got pid:{}", pid);
                                PseudonymLookupData lookup = lookupDB(con, selectQuery, pid);
                                log.info("Result Set query pid:{}, prefix:{}, postfix:{}, date_shift:{}", lookup.getPid(), lookup.getPrefix(), lookup.getPostfix(), lookup.getDateShift());
                                return lookup;
                            });
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

        } catch (ProcessException | SQLException e) {
            if (flowFile == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                log.error("Unable to execute SQL select query [{}]. No FlowFile to route to failure: {}", selectQuery, e.getMessage());
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    log.error("Execute SQL select query [{}] for {} routing to failure: {}", selectQuery, flowFile, e.getMessage());
                    flowFile = session.penalize(flowFile);
                } else {
                    log.error("Execute SQL select query [{}] routing to failure: {}", selectQuery, e.getMessage());
                    context.yield();
                }
                session.putAttribute(flowFile, RESULT_ERROR_MESSAGE, e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private PseudonymLookupData lookupDB(Connection con, final String selectQuery, String pid) throws SQLException {
        final boolean isAutoCommit = con.getAutoCommit();
        final boolean setAutoCommitValue = true;
        if (!isAutoCommit) {
            try {
                con.setAutoCommit(setAutoCommitValue);
            } catch (SQLFeatureNotSupportedException sfnse) {
                getLogger().info("setAutoCommit({}) not supported by this driver", setAutoCommitValue);
            }
        }
        PseudonymLookupData lookup;
        final String replaced = selectQuery.replace("?", "'"+pid+"'");
        try (final PreparedStatement st = con.prepareStatement(replaced)) {
            getLogger().info("Executing query {}", replaced);
            boolean hasResults = st.execute();
            if (hasResults) {
                ResultSet resultSet = st.getResultSet();
                lookup = new PseudonymLookupData(resultSet);
            }else{
                throw new Exception("No results found");
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
        return lookup;
    }
}
