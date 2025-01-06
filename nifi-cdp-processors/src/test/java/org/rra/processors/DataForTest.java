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

    public static final String DICOM_PATH = "/mnt/f/DICOM/CT/ct_IVRLE";
    public static final String DICOM_PATH_DEF = "/mnt/f/DICOM/CT/ct_IVRLE/";
    public static final String DICOM_SERVER_HOST = "localhost";
    public static final String DICOM_SERVER_AET = "DCM4CHEE";
    public static final String DICOM_SERVER_MOVE_AET = "DCM4NIFI";
    public static final int DICOM_SERVER_PORT = 11112;
    public static final boolean DICOM_INTEGRATION_TESTS = false;
    public static final List<byte[]> DCMOBJECTS = new ArrayList<>();
    public static final Map<FileInfo, byte[]> DCMOBJECTS_UNCOMPRESSED =new HashMap<>();

    static {
        readDicomFiles(DCMOBJECTS, DICOM_PATH);
        readDicomFiles(DCMOBJECTS_UNCOMPRESSED, DICOM_PATH_DEF);
    }


}
