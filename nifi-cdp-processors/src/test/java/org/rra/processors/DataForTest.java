package org.rra.processors;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static org.rra.processors.Utils.readDicomFiles;

@Slf4j
public class DataForTest {

    public static final String DICOM_PATH = "/mnt/f/DICOM/CD_FUSS/";
    public static final String DICOM_SERVER_HOST = "localhost";
    public static final String DICOM_SERVER_AET = "DCM4CHEE";
    public static final String DICOM_SERVER_MOVE_AET = "DCM4NIFI";
    public static final int DICOM_SERVER_PORT = 11112;
    public static final boolean DICOM_INTEGRATION_TESTS = false;
    public static final List<byte[]> DCMOBJECTS = new ArrayList<>();

    static {
        readDicomFiles(DCMOBJECTS, DICOM_PATH);
    }
}
