package org.rra.deidentify;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.ElementDictionary;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomEncodingOptions;

import java.util.*;

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

    public Attributes deidentifyAttributes(Attributes dataset, String retainTags, boolean remapStSerUIDs, String prefix, String postfix, Integer dateShift) {
        dataset = deidentifyAttributes(dataset, retainTags, remapStSerUIDs, dateShift);
        dataset.setString(Tag.PatientID, VR.LO, prefix+"-"+postfix);
        dataset.setString(Tag.PatientName, VR.PN, prefix+"^"+postfix);
        return dataset;
    }
    public Attributes deidentifyAttributes(Attributes dataset, String retainTags, boolean remapStSerUIDs, Integer dateShift) {
        //Remap Study IUID, Series IUID, SOP IUID and Frame Of Reference UID
        final String studyIUID = dataset.getString(Tag.StudyInstanceUID, null);
        final String seriesIUID = dataset.getString(Tag.SeriesInstanceUID, null);
        final String frameOfRefUID = dataset.getString(Tag.FrameOfReferenceUID, null);
        // Save for date shift
        final Date acqDate = dataset.getDate(Tag.AcquisitionDate);
        final Date acqDateTime = dataset.getDate(Tag.AcquisitionDateTime);
        final Date studyDate = dataset.getDate(Tag.StudyDate);
        final Date seriesDate = dataset.getDate(Tag.SeriesDate);
        final Date contentDate = dataset.getDate(Tag.ContentDate);
        //------------------------------------------------------------------------------------------
        final List<AuxTag> retainTagsList = getRetainTagsList(retainTags);
        //------------------------------------------------------------------------------------------
        deidentifier.deidentify(dataset, retainTagsList);
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
                dataset.setDate(Tag.AcquisitionDate, VR.DA, shiftDate(acqDate, dateShift));
            }
        }
        if (null != dateShift && null != acqDateTime) {
            if (dateShift != 0) {
                dataset.setDate(Tag.AcquisitionDateTime, VR.DT, shiftDate(acqDateTime, dateShift));
            }
        }
        if (null != dateShift && null != studyDate) {
            if (dateShift != 0) {
                dataset.setDate(Tag.StudyDate, VR.DA, shiftDate(studyDate, dateShift));
            }
        }
        if (null != dateShift && null != seriesDate) {
            if (dateShift != 0) {
                dataset.setDate(Tag.SeriesDate, VR.DA, shiftDate(seriesDate, dateShift));
            }
        }
        if (null != dateShift && null != contentDate) {
            if (dateShift != 0) {
                dataset.setDate(Tag.ContentDate, VR.DA, shiftDate(contentDate, dateShift));
            }
        }
        //------------------------------------------------------------------------------------------
        // Apply Retain Tags
       /* retainTagsList.forEach(aux -> {
            int tag = aux.tag;
            VR vr = aux.vr;
            String value = aux.value;
            dataset.setString(tag,vr, value);
        });*/
        //------------------------------------------------------------------------------------------
        dataset.setString(Tag.IssuerOfPatientID, VR.LO,"IDSC_DCMA");
        //------------------------------------------------------------------------------------------

        return dataset;
    }

    private Date shiftDate(Date date, int shift) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, shift);
        return cal.getTime();
    }
    List<AuxTag> getRetainTagsList(String retainTags) {
        if (null == retainTags || retainTags.isEmpty()) {
            return Collections.emptyList();
        }
        final List<AuxTag> list = new ArrayList<>();
        String[] split = retainTags.split(",");
        List<String> retainTagsList = Arrays.asList(split);
        retainTagsList.forEach(sTag -> {
            final String name = sTag.trim();
            final int tag = ElementDictionary.getStandardElementDictionary().tagForKeyword(name);
            if (tag >=0) { //Valid tag
                final VR vr = ElementDictionary.getStandardElementDictionary().vrOf(tag);
                AuxTag auxTag = new AuxTag(tag, vr, name);
                list.add(auxTag);
            }
        });
        return list;
    }

    @AllArgsConstructor
    @Data
    public static class AuxTag{
        public final int tag;
        public final VR vr;
        public final String name;
    }
}
