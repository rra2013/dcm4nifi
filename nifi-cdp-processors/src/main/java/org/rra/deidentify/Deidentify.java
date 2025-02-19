package org.rra.deidentify;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomEncodingOptions;
import org.rra.deidentify.model.DeidentifyModel;

import java.util.EnumSet;

public class Deidentify {

    private final DeIdentifier deidentifier;
    private final DicomEncodingOptions encOpts = DicomEncodingOptions.DEFAULT;

    public Deidentify(DeidentifyModel deidentModel) {
        EnumSet<DeIdentifier.Option> options = EnumSet.noneOf(DeIdentifier.Option.class);
        if (deidentModel.isRetain_date())
            options.add(DeIdentifier.Option.RetainLongitudinalTemporalInformationFullDatesOption);
        if (deidentModel.isRetain_dev())
            options.add(DeIdentifier.Option.RetainDeviceIdentityOption);
        if (deidentModel.isRetain_org())
            options.add(DeIdentifier.Option.RetainInstitutionIdentityOption);
        if (deidentModel.isRetain_uid())
            options.add(DeIdentifier.Option.RetainUIDsOption);
        if (deidentModel.isRetain_pid_hash())
            options.add(DeIdentifier.Option.RetainPatientIDHashOption);
        DeIdentifier.Option[] opt = options.toArray(new DeIdentifier.Option[0]);
        deidentifier = new DeIdentifier(deidentModel, opt);
    }

    /*public void setDummyValues(int tag, String value) {
        VR vr = ElementDictionary.getStandardElementDictionary().vrOf(tag);
        deidentifier.setDummyValue(tag, vr, value);
    }*/

    public Attributes deidentifyAttributes(Attributes dataset, boolean remapStSerUIDs, String prefix, String postfix) {
        dataset = deidentifyAttributes(dataset, remapStSerUIDs);
        dataset.setString(Tag.PatientID, VR.LO, prefix+"-"+postfix);
        dataset.setString(Tag.PatientName, VR.PN, prefix+"^"+postfix);
        return dataset;
    }
    public Attributes deidentifyAttributes(Attributes dataset, boolean remapStSerUIDs) {
        //Remap Study IUID, Series IUID, SOP IUID and Frame Of Reference UID
        final String studyIUID = dataset.getString(Tag.StudyInstanceUID, null);
        final String seriesIUID = dataset.getString(Tag.SeriesInstanceUID, null);
        final String frameOfRefUID = dataset.getString(Tag.FrameOfReferenceUID, null);
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
        dataset.setString(Tag.IssuerOfPatientID, VR.LO,"DCM4NIFI");
        //------------------------------------------------------------------------------------------

        return dataset;
    }

}
