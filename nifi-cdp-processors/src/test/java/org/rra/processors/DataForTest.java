package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.rra.processors.Utils.FileInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.rra.processors.Utils.readDicomFiles;

@Slf4j
public class DataForTest {
    private static final String DICOM_PATH_DEF = "src/test/resources/data/DICOM/ct_IVRLE/";
    private static final String DICOM_PATH_JPLL = "src/test/resources/data/DICOM/ct_JPLL/";
    private static final String DICOM_PATH_MR = "src/test/resources/data/DICOM/mr_rra/";

    private static final String DICOM_PATH_PDF = "src/test/resources/data/DICOM/pdfdcm/";
    private static final String DICOM_PATH_SR = "src/test/resources/data/DICOM/sr/";
    private static final String DICOM_PATH_RT = "src/test/resources/data/DICOM/rt/";

    public static final String DICOM_SERVER_HOST = "localhost";
    public static final String DICOM_SERVER_AET = "DCM4CHEE";
    public static final String DICOM_SERVER_MOVE_AET = "DCM4MOVE";
    public static final int DICOM_SERVER_PORT = 11112;
    public static final boolean DICOM_INTEGRATION_TESTS = false;

    public static final List<byte[]> DCMOBJECTS_IVRLE = new ArrayList<>();
    public static final List<byte[]> DCMOBJECTS_JPLL = new ArrayList<>();
    public static final List<byte[]> DCM_RT_OBJECTS = new ArrayList<>();
    public static final List<byte[]> DCM_MR_OBJECTS = new ArrayList<>();
    public static final List<byte[]> SR_OBJECTS = new ArrayList<>();
    public static final List<byte[]> PDF_OBJECTS = new ArrayList<>();
    public static final Map<FileInfo, byte[]> DCMOBJECTS_UNCOMPRESSED =new HashMap<>();

    static {
        readDicomFiles(DCMOBJECTS_IVRLE, DICOM_PATH_DEF);
        readDicomFiles(DCMOBJECTS_JPLL, DICOM_PATH_JPLL);
        readDicomFiles(DCM_MR_OBJECTS, DICOM_PATH_MR);
        readDicomFiles(DCM_RT_OBJECTS, DICOM_PATH_RT);
        readDicomFiles(PDF_OBJECTS, DICOM_PATH_PDF);
        readDicomFiles(SR_OBJECTS, DICOM_PATH_SR);
        readDicomFiles(DCMOBJECTS_UNCOMPRESSED, DICOM_PATH_DEF);
    }


}
