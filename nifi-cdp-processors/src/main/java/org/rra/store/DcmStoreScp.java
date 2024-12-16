package org.rra.store;


import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomOutputStream;
import org.dcm4che3.net.*;
import org.dcm4che3.net.pdu.PresentationContext;
import org.dcm4che3.net.service.BasicCEchoSCP;
import org.dcm4che3.net.service.BasicCStoreSCP;
import org.dcm4che3.net.service.DicomServiceException;
import org.dcm4che3.net.service.DicomServiceRegistry;
import org.dcm4che3.util.SafeClose;
import org.dcm4che3.util.StreamUtils;
import org.dcm4che3.util.StringUtils;

import java.io.*;
import java.net.SocketException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class DcmStoreScp {

    private final Device device = new Device("store_scp");
    private final ApplicationEntity ae = new ApplicationEntity("*");
    private final Connection conn = new Connection();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private AtomicReference<ProcessSessionFactory> sessionFactory;
    private CountDownLatch sessionFactorySetSignal;
    private Relationship relationshipSuccess;

    public DcmStoreScp(String host, int port, String calledAET) {
        init(host, port, calledAET);
    }

    public boolean shutDown(){
        log.info("+ + + Shutting down Store SCP + + +");
        try {
            this.device.unbindConnections();
            scheduledExecutorService.shutdown();
            return executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        }
        return false;
    }

    public static String[] toUIDs(String s) {
        if (s.equals("*"))
            return new String[]{"*"};

        String[] uids = StringUtils.split(s, ',');
        for (int i = 0; i < uids.length; i++)
            uids[i] = toUID(uids[i]);
        return uids;
    }

    public static String toUID(String uid) {
        uid = uid.trim();
        return (uid.equals("*") || Character.isDigit(uid.charAt(0)))
                ? uid
                : UID.forName(uid);
    }

    public static Properties loadProperties(String url, Properties p)
            throws IOException {
        if (p == null)
            p = new Properties();
        InputStream in = StreamUtils.openFileOrURL(url);
        try {
            p.load(in);
        } finally {
            SafeClose.close(in);
        }
        return p;
    }

    private void init(String host, int port, String calledAET) {
        Properties p;
        try {
            p = loadProperties("resource:sop-classes.properties", null);
//            System.out.println("SOP Class Name" + ";" + "SOP Class UID" + "; SCU ; SCP" );
//            p.forEach((o, o2) -> {
//                String uid = UID.forName((String) o);
//                System.out.println(o + ";" + uid + "; Yes ; Yes" );
//            });
            log.info("+ + + Read store-scp properties ok. + + +");
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        log.info("+ + + " + calledAET + "@" + host + ":" + port + " + + +");
        device.setDimseRQHandler(createServiceRegistry());
        device.addConnection(conn);
        device.addApplicationEntity(ae);
        ae.setAssociationAcceptor(true);
        ae.addConnection(conn);
        ae.setAETitle(calledAET);
        //Bind to 0.0.0.0!!!
        //Dont set the host
        //conn.setHostname(host);
        conn.setPort(port);
        for (String cuid : p.stringPropertyNames()) {
            String ts = p.getProperty(cuid);
            TransferCapability tc = new TransferCapability(null,
                    toUID(cuid),
                    TransferCapability.Role.SCP,
                    toUIDs(ts));
            ae.addTransferCapability(tc);
        }



        device.setScheduledExecutor(scheduledExecutorService);
        device.setExecutor(executorService);
        try {
            device.bindConnections();

        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("+ + + Store-SCP instantiated + + +");
    }

    private DicomServiceRegistry createServiceRegistry(/*String storageDir*/) {
        DicomServiceRegistry serviceRegistry = new DicomServiceRegistry();
        serviceRegistry.addDicomService(new BasicCEchoSCP());
        serviceRegistry.addDicomService(new CStoreSCP());
        //serviceRegistry.addDicomService(new MppsSCP(storageDir));
        return serviceRegistry;
    }

    public void setSessionFactory(AtomicReference<ProcessSessionFactory> sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public void setSessionFactorySetSignal(CountDownLatch sessionFactorySetSignal) {
        this.sessionFactorySetSignal = sessionFactorySetSignal;
    }
    private ProcessSession createProcessSession() throws InterruptedException, TimeoutException {
        ProcessSessionFactory processSessionFactory = getProcessSessionFactory();
        return processSessionFactory.createSession();
    }

    private ProcessSessionFactory getProcessSessionFactory() throws InterruptedException, TimeoutException {
        if (sessionFactorySetSignal.await(10000, TimeUnit.MILLISECONDS)) {
            return sessionFactory.get();
        } else {
            throw new TimeoutException("Waiting period for sessionFactory is over.");
        }
    }

    public void setRelationshipSuccess(Relationship relationshipSuccess) {
        this.relationshipSuccess = relationshipSuccess;
    }

    private class CStoreSCP extends BasicCStoreSCP {

        public CStoreSCP() {
            super("*");
        }

        @Override
        protected void store(Association as, PresentationContext pc, Attributes rq, PDVInputStream data, Attributes rsp) throws IOException {
            //specifies status code in returned C-STORE RSPs, 0000H by default.
            rsp.setInt(Tag.Status, VR.US, 0);
            String cuid = rq.getString(Tag.AffectedSOPClassUID);
            String iuid = rq.getString(Tag.AffectedSOPInstanceUID);
            String tsuid = pc.getTransferSyntax();

            final ProcessSession processSession;
            try {
                processSession = createProcessSession();
            } catch (InterruptedException | TimeoutException exception) {
                log.error("ProcessSession could not be acquired, command STOR aborted.", exception);
                throw new IOException("File transfer failed.");
            }
            FlowFile flowFile = processSession.create();
            try {
                try (OutputStream flowFileOutputStream = processSession.write(flowFile)) {
                    try(BufferedOutputStream bos = new BufferedOutputStream(flowFileOutputStream)){
                        storeOnlyAttributesTo(bos, data);
                    }
                    log.info("+ + + DICOM Object received -> SOPIUID: {} + + +", iuid);
                } catch (SocketException socketException) {
                    log.error("Socket exception during data transfer", socketException);
                    processSession.rollback();
                    throw new IOException(socketException.getMessage());
                } catch (IOException ioException) {
                    log.error("IOException during data transfer", ioException);
                    processSession.rollback();
                    throw new IOException(ioException.getMessage());
                }
                try {
                    final String retrieveAET = as.getAAssociateAC().getCallingAET();
                    final String calledAET = as.getAAssociateAC().getCalledAET();
                    processSession.putAttribute(flowFile, "AffectedSOPClassUID", cuid);
                    processSession.putAttribute(flowFile, "AffectedSOPInstanceUID", iuid);
                    processSession.putAttribute(flowFile, "TransferSyntax", tsuid);
                    processSession.putAttribute(flowFile, "RetrieveAET", retrieveAET);
                    processSession.putAttribute(flowFile, "CalledAET", calledAET);
                    processSession.getProvenanceReporter().modifyContent(flowFile);
                    //Transfer
                    processSession.transfer(flowFile, relationshipSuccess);
                } catch (Exception exception) {
                    processSession.rollback();
                    log.error("Process session error. ", exception);
                }

                processSession.commitAsync(() -> {
                    // if data transfer ok - send transfer complete message
                    log.info("# # # Process Complete # # #");
                });

            } catch (Exception e) {
                throw new DicomServiceException(Status.ProcessingFailure, e);
            }
        }


        private void storeOnlyAttributesTo(OutputStream outputStream, PDVInputStream data){
            try {
                DicomOutputStream out = new DicomOutputStream(outputStream, UID.ExplicitVRLittleEndian);
                try {
                    data.copyTo(out);
                } finally {
                    SafeClose.close(out);
                }
            } catch (Exception e) {
            }
        }

    }
    /*
    private class MppsSCP extends BasicMPPSSCP {

        private final File storageDir;
        private IOD mppsNCreateIOD;
        private IOD mppsNSetIOD;

        public MppsSCP(String _storageDir) {

            storageDir = new File(_storageDir, "mpps");

            if (storageDir != null)
                storageDir.mkdirs();


            //Configure mpps IOD's
            try {
                mppsNCreateIOD = IOD.load("resource:mpps-ncreate-iod.xml");
                mppsNSetIOD = IOD.load("resource:mpps-nset-iod.xml");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        @Override
        protected Attributes create(Association as, Attributes rq,
                                    Attributes rqAttrs, Attributes rsp) throws DicomServiceException {
            return create(as, rq, rqAttrs);
        }

        @Override
        protected Attributes set(Association as, Attributes rq, Attributes rqAttrs,
                                 Attributes rsp) throws DicomServiceException {
            return set(as, rq, rqAttrs);
        }

        private Attributes create(Association as, Attributes rq, Attributes rqAttrs)
                throws DicomServiceException {
            if (mppsNCreateIOD != null) {
                ValidationResult result = rqAttrs.validate(mppsNCreateIOD);
                if (!result.isValid())
                    throw DicomServiceException.valueOf(result, rqAttrs);
            }
            if (storageDir == null)
                return null;
            String cuid = rq.getString(Tag.AffectedSOPClassUID);
            String iuid = rq.getString(Tag.AffectedSOPInstanceUID);
            File file = new File(storageDir, iuid);
            if (file.exists())
                throw new DicomServiceException(Status.DuplicateSOPinstance).
                        setUID(Tag.AffectedSOPInstanceUID, iuid);
            DicomOutputStream out = null;
            log.info("{}: M-WRITE {}", as, file);
            try {
                out = new DicomOutputStream(file);
                out.writeDataset(
                        Attributes.createFileMetaInformation(iuid, cuid,
                                UID.ExplicitVRLittleEndian),
                        rqAttrs);

            } catch (IOException e) {
                log.warn(as + ": Failed to store MPPS:", e);
                throw new DicomServiceException(Status.ProcessingFailure, e);
            } finally {
                SafeClose.close(out);
            }
            return null;
        }

        private Attributes set(Association as, Attributes rq, Attributes rqAttrs)
                throws DicomServiceException {
            if (mppsNSetIOD != null) {
                ValidationResult result = rqAttrs.validate(mppsNSetIOD);
                if (!result.isValid())
                    throw DicomServiceException.valueOf(result, rqAttrs);
            }
            if (storageDir == null)
                return null;
            String cuid = rq.getString(Tag.RequestedSOPClassUID);
            String iuid = rq.getString(Tag.RequestedSOPInstanceUID);
            File file = new File(storageDir, iuid);
            if (!file.exists())
                throw new DicomServiceException(Status.NoSuchObjectInstance).
                        setUID(Tag.AffectedSOPInstanceUID, iuid);
            log.info("{}: M-UPDATE {}", as, file);
            Attributes data;
            DicomInputStream in = null;
            try {
                in = new DicomInputStream(file);
                data = in.readDataset(-1, -1);
            } catch (IOException e) {
                log.warn(as + ": Failed to read MPPS:", e);
                throw new DicomServiceException(Status.ProcessingFailure, e);
            } finally {
                SafeClose.close(in);
            }
            if (!"IN PROGRESS".equals(data.getString(Tag.PerformedProcedureStepStatus)))
                BasicMPPSSCP.mayNoLongerBeUpdated();

            data.addAll(rqAttrs);
            DicomOutputStream out = null;
            try {
                out = new DicomOutputStream(file);
                out.writeDataset(
                        Attributes.createFileMetaInformation(iuid, cuid, UID.ExplicitVRLittleEndian),
                        data);
            } catch (IOException e) {
                log.warn(as + ": Failed to update MPPS:", e);
                throw new DicomServiceException(Status.ProcessingFailure, e);
            } finally {
                SafeClose.close(out);
            }
            return null;
        }

    }
     */
}
