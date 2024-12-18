package org.rra.cfind;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;

public class QueryFilter {

    private final Attributes keys;

    public QueryFilter(Attributes keys) {
        this.keys = keys;
        if (null == keys) {
            throw new NullPointerException("Attributes keys is null");
        }
    }


    public void setPatientName(String patientName) {
        keys.setString(Tag.PatientName, VR.PN, patientName);
    }

    public void setStudyInstanceUID(String studyIUID) {
        keys.setString(Tag.StudyInstanceUID, VR.UI, studyIUID);
    }

    public void setSeriesInstanceUID(String seriesUID) {
        keys.setString(Tag.SeriesInstanceUID, VR.UI, seriesUID);
    }

    public void setPatientID(String patientID) {
        keys.setString(Tag.PatientID, VR.LO, patientID);
    }
    public void setIssuerOfPatientID(String issuerOfPatientID) {
        keys.setString(Tag.IssuerOfPatientID, VR.LO, issuerOfPatientID);
    }

    public void setPatientBirthDate(String patientBirthDate) {
        keys.setString(Tag.PatientBirthDate, VR.DA, patientBirthDate);
    }

    public void setPatientSex(String patientSex) {
        keys.setString(Tag.PatientSex, VR.CS, patientSex);
    }

    public void setStudyDate(String studyDate) {
        keys.setString(Tag.StudyDate, VR.DA, studyDate);
    }

    public void setAccessionNumber(String accessionNumber) {
        keys.setString(Tag.AccessionNumber, VR.SH, accessionNumber);
    }

    public void setModality(String modality) {
        keys.setString(Tag.Modality, VR.CS, modality);
    }

}
