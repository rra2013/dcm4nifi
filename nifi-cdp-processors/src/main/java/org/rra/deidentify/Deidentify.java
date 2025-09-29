package org.rra.deidentify;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomEncodingOptions;

import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;

public class Deidentify {

    private final DcmDeIdentifier deidentifier;
    private final DicomEncodingOptions encOpts = DicomEncodingOptions.DEFAULT;

    public Deidentify() {
        EnumSet<DcmDeIdentifier.Option> options = EnumSet.noneOf(DcmDeIdentifier.Option.class);
        /*if (deidentModel.isRetain_date())
            options.add(DeIdentifier_dep.Option.RetainLongitudinalTemporalInformationFullDatesOption);
        if (deidentModel.isRetain_dev())
            options.add(DeIdentifier_dep.Option.RetainDeviceIdentityOption);
        if (deidentModel.isRetain_org())
            options.add(DeIdentifier_dep.Option.RetainInstitutionIdentityOption);
        if (deidentModel.isRetain_uid())
            options.add(DeIdentifier_dep.Option.RetainUIDsOption);
        if (deidentModel.isRetain_pid_hash())
            options.add(DeIdentifier_dep.Option.RetainPatientIDHashOption);*/
        DcmDeIdentifier.Option[] opt = options.toArray(new DcmDeIdentifier.Option[0]);
        deidentifier = new DcmDeIdentifier(opt);
    }

    /*public void setDummyValues(int tag, String value) {
        VR vr = ElementDictionary.getStandardElementDictionary().vrOf(tag);
        deidentifier.setDummyValue(tag, vr, value);
    }*/

    public Attributes deidentifyAttributes(Attributes dataset, boolean remapStSerUIDs, String prefix, String postfix, Integer dateShift) {
        dataset = deidentifyAttributes(dataset, remapStSerUIDs, dateShift);
        dataset.setString(Tag.PatientID, VR.LO, prefix+"-"+postfix);
        dataset.setString(Tag.PatientName, VR.PN, prefix+"^"+postfix);
        return dataset;
    }
    public Attributes deidentifyAttributes(Attributes dataset, boolean remapStSerUIDs, Integer dateShift) {
        //Remap Study IUID, Series IUID, SOP IUID and Frame Of Reference UID
        final String studyIUID = dataset.getString(Tag.StudyInstanceUID, null);
        final String seriesIUID = dataset.getString(Tag.SeriesInstanceUID, null);
        final String frameOfRefUID = dataset.getString(Tag.FrameOfReferenceUID, null);
        Date acqDate = dataset.getDate(Tag.AcquisitionDate);
        //------------------------------------------------------------------------------------------
        deidentifier.deidentify(dataset);
        //------------------------------------------------------------------------------------------
        if (remapStSerUIDs) {
            if (studyIUID != null && seriesIUID != null) {
                final String studyIUIDRemap = deidentifier.remapUID(studyIUID);
                final String seriesIUIDRemap = deidentifier.remapUID(seriesIUID);
                dataset.setString(Tag.StudyInstanceUID, VR.UI, studyIUIDRemap);
                dataset.setString(Tag.SeriesInstanceUID, VR.UI, seriesIUIDRemap);
            }
            if (null != frameOfRefUID) {
                final String frameOfRefUIDRemap = deidentifier.remapUID(frameOfRefUID);
                if (null != frameOfRefUIDRemap)
                    dataset.setString(Tag.FrameOfReferenceUID, VR.UI, frameOfRefUIDRemap);
            }

        }
        //------------------------------------------------------------------------------------------
        // Set date shift if already exists
        if (null != dateShift && null != acqDate) {
            if (dateShift != 0) {
                Calendar cal = Calendar.getInstance();
                cal.setTime(acqDate);
                cal.add(Calendar.DAY_OF_MONTH, dateShift);
                Date dateShifted = cal.getTime();
                dataset.setDate(Tag.AcquisitionDate, VR.DA, dateShifted);
            }
        }
        //------------------------------------------------------------------------------------------
        dataset.setString(Tag.IssuerOfPatientID, VR.LO,"IDSC_DCMA");
        //------------------------------------------------------------------------------------------

        return dataset;
    }

}
