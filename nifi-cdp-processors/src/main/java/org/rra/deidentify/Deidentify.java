package org.rra.deidentify;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.ElementDictionary;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomEncodingOptions;

import java.util.*;

/**
 * De-identification of a DICOM data set (DICOM PS3.15 Basic Profile).
 *
 * <p>Date shift 0 or {@code null}: dates are removed or replaced by dummy values
 * as before. Date shift &ne; 0 (days, at most {@link DateShifter#MAX_DAYS}): dates
 * and times are kept, every DA and DT value, also in sequences and retained tags,
 * is shifted by that number of days, times stay unchanged, birth date and age are
 * removed as in the Basic Profile (unless retained), and the
 * data set is marked with LongitudinalTemporalInformationModified = MODIFIED and
 * code 113107 (Retain Longitudinal Temporal Information Modified Dates Option).</p>
 */
public class Deidentify {

    private final DcmDeIdentifier removingDates = new DcmDeIdentifier();
    private final DcmDeIdentifier shiftingDates =
            new DcmDeIdentifier(DcmDeIdentifier.Option.RetainLongitudinalTemporalInformationModifiedDatesOption);
    private final DicomEncodingOptions encOpts = DicomEncodingOptions.DEFAULT;

    public Attributes deidentifyAttributes(Attributes dataset, String retainTags, String pseudoPid, String pseudoName,Integer dateShift) {
        dataset = deidentifyAttributes(dataset, retainTags, true, dateShift);
        dataset.setString(Tag.PatientID, VR.LO, pseudoPid);
        dataset.setString(Tag.PatientName, VR.PN, pseudoName);
        return dataset;
    }
    public Attributes deidentifyAttributes(Attributes dataset, String retainTags, boolean remapStSerUIDs, String prefix, String postfix, Integer dateShift) {
        dataset = deidentifyAttributes(dataset, retainTags, remapStSerUIDs, dateShift);
        dataset.setString(Tag.PatientID, VR.LO, prefix+"-"+postfix);
        dataset.setString(Tag.PatientName, VR.PN, prefix+"^"+postfix);
        return dataset;
    }
    public Attributes deidentifyAttributes(Attributes dataset, String retainTags, boolean remapStSerUIDs, Integer dateShift) {
        final int days = dateShift == null ? 0 : dateShift;
        DateShifter.checkRange(days);
        final DcmDeIdentifier deidentifier = days == 0 ? removingDates : shiftingDates;
        //Remap Study IUID, Series IUID, SOP IUID and Frame Of Reference UID
        final String studyIUID = dataset.getString(Tag.StudyInstanceUID, null);
        final String seriesIUID = dataset.getString(Tag.SeriesInstanceUID, null);
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
        }
        //------------------------------------------------------------------------------------------
        // All remaining dates, including retained tags, by the same number of days
        DateShifter.shift(dataset, days);
        //------------------------------------------------------------------------------------------
        dataset.setString(Tag.IssuerOfPatientID, VR.LO,"IDSC_DCMA");
        //------------------------------------------------------------------------------------------

        return dataset;
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
