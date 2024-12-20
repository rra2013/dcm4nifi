package org.rra.processors;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.rra.cmove.MoveSCU;

import static org.rra.processors.DataForTest.*;
@Slf4j
public class MoveScuTest {


    @Test
    public void testSCUMoveSeries() throws Exception {
        MoveSCU moveSCU = new MoveSCU(DICOM_SERVER_HOST, DICOM_SERVER_PORT, "MOVE_SCU", DICOM_SERVER_AET, DICOM_SERVER_MOVE_AET);
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";
        String seriesInstanceUID = "1.3.12.2.1107.5.2.19.45819.2016071811120879334462944.0.0.0";
        moveSCU.moveSeries(studyInstanceUID, seriesInstanceUID, () -> {
            log.info("studyInstanceUID:{}", studyInstanceUID);
            log.info("seriesInstanceUID:{}", seriesInstanceUID);
            log.info("Move complete");
        }, (cmd, status) -> {
            log.info("Incoming status :{} : {}", status);
        }, (staus, message) -> {
            log.error("Error:{}:{}", staus, message);
        });
    }
    @Test
    public void testSCUMoveStudy() throws Exception {
        MoveSCU moveSCU = new MoveSCU(DICOM_SERVER_HOST, DICOM_SERVER_PORT, "MOVE_SCU", DICOM_SERVER_AET, DICOM_SERVER_MOVE_AET);
        String studyInstanceUID = "1.2.840.113845.11.1000000001900555490.20160718102042.2434233";
        moveSCU.moveStudy(studyInstanceUID, () -> {
            log.info("studyInstanceUID:{}", studyInstanceUID);
            log.info("Move complete");
        }, (cmd, status) -> {
            log.info("Incoming status :{} : {}", status);
        }, (staus, message) -> {
            log.error("Error:{}:{}", staus, message);
        });
    }
}